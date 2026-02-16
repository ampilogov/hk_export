import CryptoKit
import Foundation
import HealthKit

/// Utilities for persisting sensor bags and exporting heartbeats to HealthKit.
enum SensorBagPersistence {
    enum Profile: String, CaseIterable {
        case continuous
        case orthostatic
    }

    enum ImportResult {
        case imported
        case alreadyPresent
        case noData
        case failed(String)
    }

    struct BackfillSummary {
        let totalFiles: Int
        let pendingFiles: Int
        let skippedByMemoryFiles: Int
        let importedFiles: Int
        let unchangedFiles: Int
        let failedFiles: Int
    }

    private static let syncVersion = NSNumber(value: 1)
    private static let syncIdentifierRoot = "com.artemz.fitness_exporter.sensorbag"
    private static let backfillIndexFileName = ".sensorbag_hk_backfill_index.json"
    private static let backfillIndexQueue = DispatchQueue(
        label: "com.fitness_exporter.sensorbagBackfillIndexQueue")

    private struct BackfillIndexEntry: Codable {
        let fileSize: Int64
        let lastModifiedAt: Date
        let completedAt: Date
    }

    /// Save a sensor bag to the documents directory under the given subdirectory.
    /// - Returns: URL of the saved file.
    static func save(_ bag: SensorBag, subdir: String, maxAttempts: Int = 5) throws -> URL {
        let documents = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
        let dir = documents.appendingPathComponent(subdir, isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let timestamp = Int(Date().timeIntervalSince1970)

        for attempt in 0..<maxAttempts {
            let suffix = attempt == 0 ? "" : "-\(attempt)"
            let candidate = dir.appendingPathComponent("\(timestamp)\(suffix).bin")
            if !FileManager.default.fileExists(atPath: candidate.path) {
                try bag.saveBinary(to: candidate)
                return candidate
            }
        }

        throw NSError(
            domain: "SensorBagPersistence",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "Unable to generate unique filename after \(maxAttempts) attempts"]
        )
    }

    /// Import one previously-saved SensorBag file into HealthKit using idempotent sync identifiers.
    static func importSavedBagToHealthKit(
        fileURL: URL, profile: Profile, deviceName: String?,
        completion: ((ImportResult) -> Void)? = nil
    ) {
        func finish(_ result: ImportResult) {
            guard let completion else { return }
            if Thread.isMainThread {
                completion(result)
            } else {
                DispatchQueue.main.async {
                    completion(result)
                }
            }
        }

        DispatchQueue.global(qos: .utility).async {
            guard HKHealthStore.isHealthDataAvailable() else {
                finish(.failed("Health data is unavailable"))
                return
            }

            let fileData: Data
            do {
                fileData = try Data(contentsOf: fileURL)
            } catch {
                finish(.failed("Can't read \(fileURL.lastPathComponent): \(error.localizedDescription)"))
                return
            }

            let events: [SensorEvent]
            do {
                events = try decodeEventsFromBinary(fileData)
            } catch {
                finish(.failed("Can't decode \(fileURL.lastPathComponent): \(error.localizedDescription)"))
                return
            }

            let hrPoints = heartRatePoints(from: events)
            let rrEvents = profile == .orthostatic ? orthostaticRRWindowEvents(from: events) : events
            let beats = rrBeats(from: rrEvents)
            if hrPoints.isEmpty, beats.isEmpty {
                finish(.noData)
                return
            }

            let hash = sha256Hex(fileData)
            let hrSyncIdentifier = syncIdentifier(profile: profile, stream: "hr", fileHash: hash)
            let rrSyncPrefix = syncIdentifier(
                profile: profile,
                stream: profile == .orthostatic ? "rr_laying" : "rr",
                fileHash: hash
            )

            DispatchQueue.main.async {
                HealthKitManager.requestUnifiedAuthorization(startObservers: false) { success in
                    guard success else {
                        CustomLogger.log("[SensorBag][HK] Authorization failed for \(fileURL.lastPathComponent)")
                        finish(.failed("HealthKit authorization failed"))
                        return
                    }

                    let store = HKHealthStore()
                    var results: [ImportResult] = []
                    let resultsLock = NSLock()
                    let group = DispatchGroup()

                    func appendResult(_ result: ImportResult) {
                        resultsLock.lock()
                        results.append(result)
                        resultsLock.unlock()
                    }

                    if !hrPoints.isEmpty {
                        group.enter()
                        writeHeartRatesToHealthKitAuthorized(
                            hrPoints,
                            deviceName: deviceName,
                            store: store,
                            syncIdentifier: hrSyncIdentifier
                        ) { result in
                            appendResult(result)
                            group.leave()
                        }
                    }

                    if !beats.isEmpty {
                        group.enter()
                        writeBeatsToHealthKitAuthorized(
                            beats,
                            deviceName: deviceName,
                            store: store,
                            syncIdentifierPrefix: rrSyncPrefix
                        ) { result in
                            appendResult(result)
                            group.leave()
                        }
                    }

                    group.notify(queue: .main) {
                        let merged = mergeImportResults(results)
                        switch merged {
                        case .imported, .alreadyPresent, .noData:
                            markFileBackfilled(fileURL: fileURL, profile: profile)
                        case .failed:
                            break
                        }
                        finish(merged)
                    }
                }
            }
        }
    }

    /// Scan known SensorBag directories and import missing HealthKit data from saved files.
    /// By default, only files not marked as backfilled are processed.
    static func backfillSavedBagsToHealthKit(
        onlyPending: Bool = true,
        completion: @escaping (BackfillSummary) -> Void
    ) {
        let allFiles = listSavedFilesForBackfill()
        let memorySnapshot = backfillIndexQueue.sync { loadBackfillIndex() }
        let pendingFiles: [(URL, Profile)] =
            onlyPending
            ? allFiles.filter { !isFileMarkedBackfilled(fileURL: $0.0, profile: $0.1, index: memorySnapshot) }
            : allFiles
        let skippedByMemory = max(0, allFiles.count - pendingFiles.count)

        if pendingFiles.isEmpty {
            completion(
                BackfillSummary(
                    totalFiles: allFiles.count,
                    pendingFiles: 0,
                    skippedByMemoryFiles: skippedByMemory,
                    importedFiles: 0,
                    unchangedFiles: 0,
                    failedFiles: 0
                ))
            return
        }

        var imported = 0
        var unchanged = 0
        var failed = 0

        func process(_ idx: Int) {
            guard idx < pendingFiles.count else {
                completion(
                    BackfillSummary(
                        totalFiles: allFiles.count,
                        pendingFiles: pendingFiles.count,
                        skippedByMemoryFiles: skippedByMemory,
                        importedFiles: imported,
                        unchangedFiles: unchanged,
                        failedFiles: failed
                    ))
                return
            }

            let (fileURL, profile) = pendingFiles[idx]
            importSavedBagToHealthKit(fileURL: fileURL, profile: profile, deviceName: nil) { result in
                switch result {
                case .imported:
                    imported += 1
                case .alreadyPresent, .noData:
                    unchanged += 1
                case .failed(let reason):
                    failed += 1
                    CustomLogger.log("[SensorBag][HK] Backfill failed for \(fileURL.lastPathComponent): \(reason)")
                }
                DispatchQueue.main.async {
                    process(idx + 1)
                }
            }
        }

        process(0)
    }

    @discardableResult
    static func resetBackfillMemory() -> Int {
        backfillIndexQueue.sync {
            let current = loadBackfillIndex()
            let removed = current.count
            do {
                let url = backfillIndexURL()
                if FileManager.default.fileExists(atPath: url.path) {
                    try FileManager.default.removeItem(at: url)
                }
            } catch {
                CustomLogger.log("[SensorBag][HK] Failed to reset backfill memory: \(error.localizedDescription)")
            }
            return removed
        }
    }

    /// Write a list of heartbeats to HealthKit.
    static func getDevice(deviceName: String?) -> HKDevice {
        return
            deviceName.map {
                HKDevice(
                    name: $0, manufacturer: nil, model: nil, hardwareVersion: nil,
                    firmwareVersion: nil, softwareVersion: "v0",
                    localIdentifier: nil, udiDeviceIdentifier: nil)
            } ?? .local()
    }

    /// Write a list of heartbeats to HealthKit.
    static func writeBeatsToHealthKit(
        _ beats: [(Date, Bool)],
        deviceName: String?,
        syncIdentifierPrefix: String? = nil,
        completion: ((ImportResult) -> Void)? = nil
    ) {
        guard HKHealthStore.isHealthDataAvailable() else {
            completion?(.failed("Health data is unavailable"))
            return
        }
        let store = HKHealthStore()
        HealthKitManager.requestUnifiedAuthorization(startObservers: false) { success in
            guard success else {
                completion?(.failed("HealthKit authorization failed"))
                return
            }
            writeBeatsToHealthKitAuthorized(
                beats,
                deviceName: deviceName,
                store: store,
                syncIdentifierPrefix: syncIdentifierPrefix,
                completion: completion
            )
        }
    }

    /// Write instantaneous heart rate points (bpm) to HealthKit using HKQuantitySeriesSampleBuilder.
    static func writeHeartRatesToHealthKit(
        _ points: [(Date, Double)],
        deviceName: String?,
        syncIdentifier: String? = nil,
        completion: ((ImportResult) -> Void)? = nil
    ) {
        guard HKHealthStore.isHealthDataAvailable(), !points.isEmpty else {
            completion?(.noData)
            return
        }
        let store = HKHealthStore()
        HealthKitManager.requestUnifiedAuthorization(startObservers: false) { success in
            guard success else {
                completion?(.failed("HealthKit authorization failed"))
                return
            }
            writeHeartRatesToHealthKitAuthorized(
                points,
                deviceName: deviceName,
                store: store,
                syncIdentifier: syncIdentifier,
                completion: completion
            )
        }
    }

    /// Write only RR-derived heartbeat series to HealthKit from a list of events.
    static func writeRRIntervalsToHealthKit(
        from events: [SensorEvent],
        deviceName: String?,
        syncIdentifierPrefix: String? = nil,
        completion: ((ImportResult) -> Void)? = nil
    ) {
        let beats = rrBeats(from: events)
        guard !beats.isEmpty else {
            completion?(.noData)
            return
        }
        writeBeatsToHealthKit(
            beats,
            deviceName: deviceName,
            syncIdentifierPrefix: syncIdentifierPrefix,
            completion: completion
        )
    }

    /// Convenience overload: RR-derived heartbeat series from a bag.
    static func writeRRIntervalsToHealthKit(
        from bag: SensorBag,
        deviceName: String?,
        syncIdentifierPrefix: String? = nil,
        completion: ((ImportResult) -> Void)? = nil
    ) {
        writeRRIntervalsToHealthKit(
            from: bag.snapshot,
            deviceName: deviceName,
            syncIdentifierPrefix: syncIdentifierPrefix,
            completion: completion
        )
    }

    /// Write only instantaneous HR points to HealthKit from a list of events.
    static func writeHeartRatesToHealthKit(
        from events: [SensorEvent],
        deviceName: String?,
        syncIdentifier: String? = nil,
        completion: ((ImportResult) -> Void)? = nil
    ) {
        let hrPoints = heartRatePoints(from: events)
        guard !hrPoints.isEmpty else {
            completion?(.noData)
            return
        }
        writeHeartRatesToHealthKit(
            hrPoints,
            deviceName: deviceName,
            syncIdentifier: syncIdentifier,
            completion: completion
        )
    }

    /// Convenience overload: instantaneous HR points from a bag.
    static func writeHeartRatesToHealthKit(
        from bag: SensorBag,
        deviceName: String?,
        syncIdentifier: String? = nil,
        completion: ((ImportResult) -> Void)? = nil
    ) {
        writeHeartRatesToHealthKit(
            from: bag.snapshot,
            deviceName: deviceName,
            syncIdentifier: syncIdentifier,
            completion: completion
        )
    }

    private static func mergeImportResults(_ results: [ImportResult]) -> ImportResult {
        let errors: [String] = results.compactMap { result in
            if case .failed(let message) = result { return message }
            return nil
        }
        if !errors.isEmpty {
            return .failed(errors.joined(separator: " | "))
        }
        if results.contains(where: { if case .imported = $0 { return true }; return false }) {
            return .imported
        }
        if results.allSatisfy({ if case .noData = $0 { return true }; return false }) {
            return .noData
        }
        return .alreadyPresent
    }

    private static func listSavedFilesForBackfill() -> [(URL, Profile)] {
        let documents = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
        var files: [(URL, Profile)] = []

        for profile in Profile.allCases {
            let dir = documents.appendingPathComponent(profile.rawValue, isDirectory: true)
            guard let urls = try? FileManager.default.contentsOfDirectory(
                at: dir,
                includingPropertiesForKeys: [.isRegularFileKey],
                options: [.skipsHiddenFiles]
            )
            else { continue }

            let binFiles: [URL] = urls.filter { $0.pathExtension.lowercased() == "bin" }.filter { url in
                (try? url.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile) ?? false
            }.sorted { $0.lastPathComponent < $1.lastPathComponent }
            for file in binFiles {
                files.append((file, profile))
            }
        }

        return files
    }

    private static func backfillIndexURL() -> URL {
        let documents = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
        return documents.appendingPathComponent(backfillIndexFileName)
    }

    private static func backfillFileKey(fileURL: URL, profile: Profile) -> String {
        "\(profile.rawValue)/\(fileURL.lastPathComponent)"
    }

    private static func fileMetadata(fileURL: URL) -> (size: Int64, mtime: Date)? {
        let keys: Set<URLResourceKey> = [.fileSizeKey, .contentModificationDateKey]
        guard let values = try? fileURL.resourceValues(forKeys: keys) else { return nil }
        guard let size = values.fileSize.map({ Int64($0) }), let mtime = values.contentModificationDate else {
            return nil
        }
        return (size, mtime)
    }

    private static func loadBackfillIndex() -> [String: BackfillIndexEntry] {
        let url = backfillIndexURL()
        guard let data = try? Data(contentsOf: url) else { return [:] }
        return (try? JSONDecoder().decode([String: BackfillIndexEntry].self, from: data)) ?? [:]
    }

    private static func saveBackfillIndex(_ index: [String: BackfillIndexEntry]) {
        do {
            let data = try JSONEncoder().encode(index)
            try data.write(to: backfillIndexURL(), options: .atomic)
        } catch {
            CustomLogger.log("[SensorBag][HK] Failed to persist backfill memory: \(error.localizedDescription)")
        }
    }

    private static func isFileMarkedBackfilled(fileURL: URL, profile: Profile) -> Bool {
        let index = backfillIndexQueue.sync { loadBackfillIndex() }
        return isFileMarkedBackfilled(fileURL: fileURL, profile: profile, index: index)
    }

    private static func isFileMarkedBackfilled(
        fileURL: URL,
        profile: Profile,
        index: [String: BackfillIndexEntry]
    ) -> Bool {
        let key = backfillFileKey(fileURL: fileURL, profile: profile)
        guard let record = index[key] else { return false }
        guard let current = fileMetadata(fileURL: fileURL) else { return false }
        return record.fileSize == current.size && record.lastModifiedAt == current.mtime
    }

    private static func markFileBackfilled(fileURL: URL, profile: Profile) {
        backfillIndexQueue.sync {
            guard let current = fileMetadata(fileURL: fileURL) else { return }
            let key = backfillFileKey(fileURL: fileURL, profile: profile)
            var index = loadBackfillIndex()
            index[key] = BackfillIndexEntry(
                fileSize: current.size,
                lastModifiedAt: current.mtime,
                completedAt: Date()
            )
            saveBackfillIndex(index)
        }
    }

    private static func writeBeatsToHealthKitAuthorized(
        _ beats: [(Date, Bool)],
        deviceName: String?,
        store: HKHealthStore,
        syncIdentifierPrefix: String?,
        completion: ((ImportResult) -> Void)?
    ) {
        guard !beats.isEmpty else {
            completion?(.noData)
            return
        }

        let maxCount = HKHeartbeatSeriesBuilder.maximumCount
        let pages: [[(Date, Bool)]] = stride(from: 0, to: beats.count, by: maxCount).map {
            Array(beats[$0..<min($0 + maxCount, beats.count)])
        }
        let heartbeatType = HKSeriesType.heartbeat()
        var didImportAny = false
        var hadWritablePage = false

        func processPage(_ idx: Int) {
            guard idx < pages.count else {
                if didImportAny {
                    completion?(.imported)
                } else {
                    completion?(hadWritablePage ? .alreadyPresent : .noData)
                }
                return
            }

            var page = pages[idx]
            guard !page.isEmpty else {
                processPage(idx + 1)
                return
            }

            page[0].1 = false  // ensure first beat starts new sequence
            guard page.count >= HRVConstants.MIN_HEARTBEATS else {
                processPage(idx + 1)
                return
            }
            hadWritablePage = true

            let pageSyncIdentifier = syncIdentifierPrefix.map { "\($0).p\(idx)" }

            func writePage() {
                saveHeartbeatPage(
                    page,
                    store: store,
                    device: getDevice(deviceName: deviceName),
                    syncIdentifier: pageSyncIdentifier
                ) { result in
                    switch result {
                    case .imported:
                        didImportAny = true
                        processPage(idx + 1)
                    case .alreadyPresent, .noData:
                        processPage(idx + 1)
                    case .failed:
                        completion?(result)
                    }
                }
            }

            if let syncIdentifier = pageSyncIdentifier {
                hasSample(
                    withSyncIdentifier: syncIdentifier,
                    sampleType: heartbeatType,
                    store: store
                ) { exists in
                    if exists {
                        processPage(idx + 1)
                        return
                    }
                    hasLegacyHeartbeatSeries(page: page, deviceName: deviceName, store: store) { legacyExists in
                        if legacyExists {
                            processPage(idx + 1)
                        } else {
                            writePage()
                        }
                    }
                }
            } else {
                hasLegacyHeartbeatSeries(page: page, deviceName: deviceName, store: store) { legacyExists in
                    if legacyExists {
                        processPage(idx + 1)
                    } else {
                        writePage()
                    }
                }
            }
        }

        processPage(0)
    }

    private static func writeHeartRatesToHealthKitAuthorized(
        _ points: [(Date, Double)],
        deviceName: String?,
        store: HKHealthStore,
        syncIdentifier: String?,
        completion: ((ImportResult) -> Void)?
    ) {
        guard let hrType = HKQuantityType.quantityType(forIdentifier: .heartRate) else {
            completion?(.failed("Heart rate type is unavailable"))
            return
        }

        let sorted = points.sorted { $0.0 < $1.0 }.filter { $0.1.isFinite && $0.1 > 0 }
        guard let startDate = sorted.first?.0, let endDate = sorted.last?.0 else {
            completion?(.noData)
            return
        }

        let unit = HKUnit.count().unitDivided(by: .minute())
        let device = getDevice(deviceName: deviceName)

        func writeSeries() {
            let builder = HKQuantitySeriesSampleBuilder(
                healthStore: store,
                quantityType: hrType,
                startDate: startDate,
                device: device
            )

            for (ts, bpm) in sorted {
                let quantity = HKQuantity(unit: unit, doubleValue: bpm)
                do {
                    try builder.insert(quantity, at: ts)
                } catch {
                    builder.discard()
                    let message = "Can't insert HR point at \(ts): \(error.localizedDescription)"
                    CustomLogger.log("[SensorBag][HK] \(message)")
                    completion?(.failed(message))
                    return
                }
            }

            let metadata = syncIdentifier.map { syncMetadata(syncIdentifier: $0) }
            builder.finishSeries(metadata: metadata, endDate: endDate) { _, error in
                if let error {
                    let message = "Can't finish HR series: \(error.localizedDescription)"
                    CustomLogger.log("[SensorBag][HK] \(message)")
                    completion?(.failed(message))
                    return
                }
                completion?(.imported)
            }
        }

        func checkLegacyThenWrite() {
            hasLegacyHeartRateSeries(points: sorted, deviceName: deviceName, store: store) { legacyExists in
                if legacyExists {
                    completion?(.alreadyPresent)
                } else {
                    writeSeries()
                }
            }
        }

        if let syncIdentifier {
            hasSample(
                withSyncIdentifier: syncIdentifier,
                sampleType: hrType,
                store: store
            ) { exists in
                if exists {
                    completion?(.alreadyPresent)
                } else {
                    checkLegacyThenWrite()
                }
            }
        } else {
            checkLegacyThenWrite()
        }
    }

    /// Save a single heartbeat page to HealthKit.
    private static func saveHeartbeatPage(
        _ page: [(Date, Bool)],
        store: HKHealthStore,
        device: HKDevice,
        syncIdentifier: String?,
        completion: ((ImportResult) -> Void)?
    ) {
        guard page.count >= HRVConstants.MIN_HEARTBEATS else {
            completion?(.noData)
            return
        }
        let start = page[0].0
        let builder = HKHeartbeatSeriesBuilder(healthStore: store, device: device, start: start)

        func addBeat(at index: Int) {
            guard index < page.count else {
                builder.finishSeries { _, error in
                    if let error {
                        let message = "Can't finish heartbeat series: \(error.localizedDescription)"
                        CustomLogger.log("[SensorBag][HK] \(message)")
                        completion?(.failed(message))
                        return
                    }
                    completion?(.imported)
                }
                return
            }

            let (ts, hasPrev) = page[index]
            builder.addHeartbeatWithTimeInterval(
                sinceSeriesStartDate: ts.timeIntervalSince(start),
                precededByGap: !hasPrev
            ) { success, error in
                guard success else {
                    builder.discard()
                    let message = "Can't add heartbeat: \(error?.localizedDescription ?? "unknown error")"
                    CustomLogger.log("[SensorBag][HK] \(message)")
                    completion?(.failed(message))
                    return
                }
                addBeat(at: index + 1)
            }
        }

        if let syncIdentifier {
            builder.addMetadata(syncMetadata(syncIdentifier: syncIdentifier)) { success, error in
                guard success else {
                    builder.discard()
                    let message = "Can't set heartbeat metadata: \(error?.localizedDescription ?? "unknown error")"
                    CustomLogger.log("[SensorBag][HK] \(message)")
                    completion?(.failed(message))
                    return
                }
                addBeat(at: 0)
            }
        } else {
            addBeat(at: 0)
        }
    }

    private static func hasSample(
        withSyncIdentifier syncIdentifier: String,
        sampleType: HKSampleType,
        store: HKHealthStore,
        completion: @escaping (Bool) -> Void
    ) {
        let predicate = HKQuery.predicateForObjects(
            withMetadataKey: HKMetadataKeySyncIdentifier,
            operatorType: .equalTo,
            value: syncIdentifier
        )
        let query = HKSampleQuery(sampleType: sampleType, predicate: predicate, limit: 1, sortDescriptors: nil) {
            _, samples, error in
            if let error {
                CustomLogger.log("[SensorBag][HK] metadata existence check failed: \(error.localizedDescription)")
                completion(false)
                return
            }
            completion(!(samples?.isEmpty ?? true))
        }
        store.execute(query)
    }

    private static func hasLegacyHeartRateSeries(
        points: [(Date, Double)],
        deviceName: String?,
        store: HKHealthStore,
        completion: @escaping (Bool) -> Void
    ) {
        guard
            let hrType = HKQuantityType.quantityType(forIdentifier: .heartRate),
            let start = points.first?.0,
            let end = points.last?.0
        else {
            completion(false)
            return
        }
        let predicate = HKQuery.predicateForSamples(
            withStart: start.addingTimeInterval(-1),
            end: end.addingTimeInterval(1),
            options: []
        )
        let query = HKSampleQuery(sampleType: hrType, predicate: predicate, limit: 50, sortDescriptors: nil) {
            _, samples, _ in
            let expectedCount = points.count
            let bundleIdentifier = Bundle.main.bundleIdentifier
            let exists = (samples as? [HKQuantitySample])?.contains { sample in
                let sourceMatches = bundleIdentifier == nil
                    || sample.sourceRevision.source.bundleIdentifier == bundleIdentifier
                let deviceMatches = deviceName == nil || sample.device?.name == deviceName
                return sourceMatches
                    && deviceMatches
                    && sample.count == expectedCount
                    && abs(sample.startDate.timeIntervalSince(start)) < 1
                    && abs(sample.endDate.timeIntervalSince(end)) < 1
            } ?? false
            completion(exists)
        }
        store.execute(query)
    }

    private static func hasLegacyHeartbeatSeries(
        page: [(Date, Bool)],
        deviceName: String?,
        store: HKHealthStore,
        completion: @escaping (Bool) -> Void
    ) {
        guard let start = page.first?.0, let end = page.last?.0 else {
            completion(false)
            return
        }
        let predicate = HKQuery.predicateForSamples(
            withStart: start.addingTimeInterval(-1),
            end: end.addingTimeInterval(1),
            options: []
        )
        let query = HKSampleQuery(
            sampleType: HKSeriesType.heartbeat(),
            predicate: predicate,
            limit: 50,
            sortDescriptors: nil
        ) { _, samples, _ in
            let expectedCount = page.count
            let bundleIdentifier = Bundle.main.bundleIdentifier
            let exists = (samples as? [HKHeartbeatSeriesSample])?.contains { sample in
                let sourceMatches = bundleIdentifier == nil
                    || sample.sourceRevision.source.bundleIdentifier == bundleIdentifier
                let deviceMatches = deviceName == nil || sample.device?.name == deviceName
                return sourceMatches
                    && deviceMatches
                    && sample.count == expectedCount
                    && abs(sample.startDate.timeIntervalSince(start)) < 1
                    && abs(sample.endDate.timeIntervalSince(end)) < 1
            } ?? false
            completion(exists)
        }
        store.execute(query)
    }

    private static func heartRatePoints(from events: [SensorEvent]) -> [(Date, Double)] {
        let hrContainers: [(Date, HRSamples)] = events.compactMap { e in
            if case .hrSamples(let s) = e.data { return (e.timestamp, s) }
            return nil
        }
        var hrPoints: [(Date, Double)] = []
        for (ts, samples) in hrContainers {
            for s in samples.samples {
                hrPoints.append((ts, Double(s.value)))
            }
        }
        return hrPoints
    }

    private static func rrBeats(from events: [SensorEvent]) -> [(Date, Bool)] {
        let hrContainers: [(Date, HRSamples)] = events.compactMap { e in
            if case .hrSamples(let s) = e.data { return (e.timestamp, s) }
            return nil
        }
        return reconstructBeats(from: hrContainers)
    }

    private static func orthostaticRRWindowEvents(from events: [SensorEvent]) -> [SensorEvent] {
        var layingStart: Date?
        var waitingStart: Date?
        for e in events {
            guard case .hrvStage(let stage) = e.data else { continue }
            if stage == .laying {
                layingStart = e.timestamp
            } else if stage == .waitingForStanding {
                waitingStart = e.timestamp
            }
        }
        guard let layStartRaw = layingStart, let waitStartRaw = waitingStart else { return [] }
        let start = layStartRaw.addingTimeInterval(1)
        let end = waitStartRaw.addingTimeInterval(-1)
        return events.filter { $0.timestamp >= start && $0.timestamp <= end }
    }

    private static func syncMetadata(syncIdentifier: String) -> [String: Any] {
        [
            HKMetadataKeySyncIdentifier: syncIdentifier,
            HKMetadataKeySyncVersion: syncVersion,
        ]
    }

    private static func syncIdentifier(profile: Profile, stream: String, fileHash: String) -> String {
        "\(syncIdentifierRoot).\(profile.rawValue).\(stream).\(fileHash)"
    }

    private static func sha256Hex(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    private enum SensorBagDecodeError: Error, LocalizedError {
        case unsupportedVersion(UInt32)
        case unsupportedEventType(UInt32)
        case truncatedData
        case invalidLength

        var errorDescription: String? {
            switch self {
            case .unsupportedVersion(let version):
                return "Unsupported sensor bag version: \(version)"
            case .unsupportedEventType(let kind):
                return "Unsupported sensor event type: \(kind)"
            case .truncatedData:
                return "Truncated sensor bag data"
            case .invalidLength:
                return "Invalid sensor bag field length"
            }
        }
    }

    private struct BinaryReader {
        let data: Data
        private(set) var offset = 0

        mutating func readUInt32() throws -> UInt32 { try readInteger() }
        mutating func readUInt64() throws -> UInt64 { try readInteger() }

        mutating func readInt32() throws -> Int32 {
            Int32(bitPattern: try readUInt32())
        }

        mutating func readDouble() throws -> Double {
            let bits = try readUInt64()
            return Double(bitPattern: bits)
        }

        mutating func readString(length: Int) throws -> String {
            let bytes = try readBytes(length: length)
            return String(data: bytes, encoding: .utf8) ?? String(decoding: bytes, as: UTF8.self)
        }

        mutating func readBytes(length: Int) throws -> Data {
            guard length >= 0 else { throw SensorBagDecodeError.invalidLength }
            guard offset + length <= data.count else { throw SensorBagDecodeError.truncatedData }
            let slice = data[offset..<(offset + length)]
            offset += length
            return Data(slice)
        }

        mutating func skip(byteCount: Int) throws {
            guard byteCount >= 0 else { throw SensorBagDecodeError.invalidLength }
            guard offset + byteCount <= data.count else { throw SensorBagDecodeError.truncatedData }
            offset += byteCount
        }

        private mutating func readInteger<T: FixedWidthInteger>() throws -> T {
            let size = MemoryLayout<T>.size
            guard offset + size <= data.count else { throw SensorBagDecodeError.truncatedData }
            var value: T = 0
            _ = withUnsafeMutableBytes(of: &value) { dst in
                data.copyBytes(to: dst, from: offset..<(offset + size))
            }
            offset += size
            return T(littleEndian: value)
        }
    }

    private static func decodeEventsFromBinary(_ data: Data) throws -> [SensorEvent] {
        var reader = BinaryReader(data: data)
        let version = try reader.readUInt32()
        guard version == 0 || version == 1 else {
            throw SensorBagDecodeError.unsupportedVersion(version)
        }
        let eventCount = Int(try reader.readUInt32())
        var events: [SensorEvent] = []
        events.reserveCapacity(min(eventCount, 1024))

        for _ in 0..<eventCount {
            let timestamp = Date(timeIntervalSince1970: try reader.readDouble())
            let eventType = try reader.readUInt32()
            switch eventType {
            case 1:
                let sampleCount = Int(try reader.readUInt32())
                var samples: [HRSample] = []
                samples.reserveCapacity(sampleCount)
                for _ in 0..<sampleCount {
                    let value = Int(try reader.readInt32())
                    let rrCount = Int(try reader.readUInt32())
                    var rrIntervals: [Double] = []
                    rrIntervals.reserveCapacity(rrCount)
                    for _ in 0..<rrCount {
                        rrIntervals.append(try reader.readDouble())
                    }
                    samples.append(
                        HRSample(
                            value: value,
                            contactSupported: nil,
                            contactDetected: nil,
                            energyExpended: nil,
                            rrIntervals: rrIntervals
                        ))
                }
                events.append(SensorEvent(timestamp: timestamp, data: .hrSamples(HRSamples(samples: samples))))
            case 2:
                let sampleCount = Int(try reader.readUInt32())
                if version == 0 {
                    let bytes = try safeMultiply(
                        sampleCount,
                        MemoryLayout<UInt64>.size + MemoryLayout<Int32>.size
                    )
                    try reader.skip(byteCount: bytes)
                } else {
                    _ = try reader.readUInt64()  // first device timestamp
                    _ = try reader.readUInt64()  // last device timestamp
                    let bytes = try safeMultiply(sampleCount, MemoryLayout<Int16>.size)
                    try reader.skip(byteCount: bytes)
                }
            case 3:
                let sampleCount = Int(try reader.readUInt32())
                if version == 0 {
                    let bytes = try safeMultiply(
                        sampleCount,
                        MemoryLayout<UInt64>.size + (3 * MemoryLayout<Int32>.size)
                    )
                    try reader.skip(byteCount: bytes)
                } else {
                    _ = try reader.readUInt64()  // first device timestamp
                    _ = try reader.readUInt64()  // last device timestamp
                    let bytes = try safeMultiply(sampleCount, 3 * MemoryLayout<Int16>.size)
                    try reader.skip(byteCount: bytes)
                }
            case 4:
                _ = try reader.readInt32()
            case 5:
                let textLen = Int(try reader.readUInt32())
                let rawStage = try reader.readString(length: textLen)
                if let stage = HRVStage(rawValue: rawStage) {
                    events.append(SensorEvent(timestamp: timestamp, data: .hrvStage(stage)))
                }
            case 6:
                try reader.skip(byteCount: 5 * MemoryLayout<Double>.size)
            case 7:
                let textLen = Int(try reader.readUInt32())
                _ = try reader.readBytes(length: textLen)
            default:
                throw SensorBagDecodeError.unsupportedEventType(eventType)
            }
        }

        return events
    }

    private static func safeMultiply(_ a: Int, _ b: Int) throws -> Int {
        let (result, overflow) = a.multipliedReportingOverflow(by: b)
        if overflow || result < 0 { throw SensorBagDecodeError.invalidLength }
        return result
    }
}

/// Reconstruct individual heartbeats from raw HR samples.
func reconstructBeats(from samples: [(Date, HRSamples)], maxGap: Duration = .seconds(2)) -> [(
    Date, Bool
)] {
    var rr: [Double] = []
    var beatMaxTimes: [Date] = []
    for (arrival, container) in samples.sorted(by: { $0.0 < $1.0 }).reversed() {
        if beatMaxTimes.isEmpty { beatMaxTimes.append(arrival) }
        for sample in container.samples.reversed() {
            for interval in sample.rrIntervals.reversed() {
                let secondBeat = min(arrival, beatMaxTimes.last!)
                let firstBeat = secondBeat.addingTimeInterval(-interval)
                rr.append(interval)
                beatMaxTimes.append(firstBeat)
            }
        }
    }
    rr.reverse()
    beatMaxTimes.reverse()
    guard !rr.isEmpty else { return [] }
    var beats: [(Date, Bool)] = []
    var idx = 0
    while idx < rr.count {
        var current = beatMaxTimes[idx]
        beats.append((current, false))
        repeat {
            current = current.addingTimeInterval(rr[idx])
            beats.append((current, true))
            idx += 1
        } while idx < rr.count && current + Double(maxGap.components.seconds) >= beatMaxTimes[idx]
    }
    return beats
}
