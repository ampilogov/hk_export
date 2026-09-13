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

    enum ImportMode {
        /// The caller just created this file and has not attempted its import.
        /// Deterministic sync metadata is written without querying HealthKit.
        case newFile
        /// The file may have been imported fully or partially in an earlier run.
        /// HealthKit is queried before retrying any object.
        case recovery
    }

    struct BackfillSummary {
        let totalFiles: Int
        let pendingFiles: Int
        let skippedByMemoryFiles: Int
        let importedFiles: Int
        let unchangedFiles: Int
        let failedFiles: Int
        let errorMessage: String?
    }

    private static let syncVersion = NSNumber(value: 1)
    private static let syncIdentifierRoot = "com.artemz.fitness_exporter.sensorbag"
    private static let workStateLock = NSLock()
    private static let ecgFileIndexLock = NSLock()

    private struct ECGFileIndexEntry {
        let url: URL
        let finalizedAt: TimeInterval
    }

    private struct ECGFileIndexCache {
        let directoryPath: String
        let modificationDate: Date
        let entries: [ECGFileIndexEntry]
    }

    private static var ecgFileIndexCache: ECGFileIndexCache?

    private struct FileMetadata {
        let size: Int64
        let mtime: Date
        let resourceIdentifier: NSObject?
    }

    private struct SavedFileSnapshot {
        let data: Data
        let metadata: FileMetadata
    }

    private struct ActiveImport {
        var completions: [(ImportResult) -> Void]
    }

    private struct ActiveBackfill {
        let onlyPending: Bool
        var completions: [(BackfillSummary) -> Void]
    }

    private enum ImportCoordination {
        case start
        case joined
        case rejected(String)
    }

    private enum BackfillCoordination {
        case start
        case joined
        case rejected(String)
    }

    private static var activeImports: [String: ActiveImport] = [:]
    private static var activeBackfill: ActiveBackfill?
    private static var resetInProgress = false

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
                if subdir == Profile.continuous.rawValue {
                    invalidateECGFileIndex()
                }
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
        mode: ImportMode = .recovery,
        completion: ((ImportResult) -> Void)? = nil
    ) {
        let coordinationKey = backfillFileKey(fileURL: fileURL, profile: profile)
        switch beginImport(key: coordinationKey, completion: completion) {
        case .start:
            break
        case .joined:
            return
        case .rejected(let message):
            deliverImportResult(.failed(message), to: completion)
            return
        }

        func finish(_ result: ImportResult) {
            completeImport(key: coordinationKey, result: result)
        }

        DispatchQueue.global(qos: .utility).async {
            guard HKHealthStore.isHealthDataAvailable() else {
                finish(.failed("Health data is unavailable"))
                return
            }

            let fileSnapshot: SavedFileSnapshot
            do {
                fileSnapshot = try readStableFile(fileURL: fileURL)
            } catch {
                finish(.failed("Can't read \(fileURL.lastPathComponent): \(error.localizedDescription)"))
                return
            }
            let fileData = fileSnapshot.data

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
                do {
                    try verifyFileUnchanged(
                        fileURL: fileURL,
                        expected: fileSnapshot.metadata
                    )
                    try markFileBackfilled(
                        fileURL: fileURL,
                        profile: profile,
                        metadata: fileSnapshot.metadata
                    )
                    finish(.noData)
                } catch {
                    let message =
                        "No HealthKit data was found, but completion state could not be saved: "
                        + error.localizedDescription
                    CustomLogger.log("[SensorBag][HK] \(message)")
                    finish(.failed(message))
                }
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
                HealthKitManager.requestWriteAuthorization { success in
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
                            syncIdentifier: hrSyncIdentifier,
                            mode: mode
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
                            syncIdentifierPrefix: rrSyncPrefix,
                            mode: mode
                        ) { result in
                            appendResult(result)
                            group.leave()
                        }
                    }

                    group.notify(queue: .global(qos: .utility)) {
                        let merged = mergeImportResults(results)
                        switch merged {
                        case .imported, .alreadyPresent, .noData:
                            do {
                                try verifyFileUnchanged(
                                    fileURL: fileURL,
                                    expected: fileSnapshot.metadata
                                )
                                try markFileBackfilled(
                                    fileURL: fileURL,
                                    profile: profile,
                                    metadata: fileSnapshot.metadata
                                )
                            } catch {
                                let message =
                                    "HealthKit import finished, but completion state could not be saved: "
                                    + error.localizedDescription
                                CustomLogger.log("[SensorBag][HK] \(message)")
                                finish(.failed(message))
                                return
                            }
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
        switch beginBackfill(onlyPending: onlyPending, completion: completion) {
        case .start:
            break
        case .joined:
            return
        case .rejected(let message):
            deliverBackfillSummary(
                BackfillSummary(
                    totalFiles: 0,
                    pendingFiles: 0,
                    skippedByMemoryFiles: 0,
                    importedFiles: 0,
                    unchangedFiles: 0,
                    failedFiles: 0,
                    errorMessage: message
                ),
                to: completion
            )
            return
        }

        func finish(_ summary: BackfillSummary) {
            completeBackfill(summary)
        }

        DispatchQueue.global(qos: .utility).async {
            let allFiles: [(URL, Profile)]
            let pendingFiles: [(URL, Profile)]
            let skippedByMemory: Int
            do {
                allFiles = try listSavedFilesForBackfill()
                if onlyPending {
                    let memorySnapshot = try SensorBagBackfillIndex.records(
                        in: documentsDirectory()
                    )
                    pendingFiles = try allFiles.filter {
                        try !isFileMarkedBackfilled(
                            fileURL: $0.0,
                            profile: $0.1,
                            index: memorySnapshot
                        )
                    }
                } else {
                    pendingFiles = allFiles
                }
                skippedByMemory = max(0, allFiles.count - pendingFiles.count)
            } catch {
                let message = error.localizedDescription
                CustomLogger.log("[SensorBag][HK] Backfill setup failed: \(message)")
                finish(
                    BackfillSummary(
                        totalFiles: 0,
                        pendingFiles: 0,
                        skippedByMemoryFiles: 0,
                        importedFiles: 0,
                        unchangedFiles: 0,
                        failedFiles: 0,
                        errorMessage: message
                    )
                )
                return
            }

            guard !pendingFiles.isEmpty else {
                finish(
                    BackfillSummary(
                        totalFiles: allFiles.count,
                        pendingFiles: 0,
                        skippedByMemoryFiles: skippedByMemory,
                        importedFiles: 0,
                        unchangedFiles: 0,
                        failedFiles: 0,
                        errorMessage: nil
                    )
                )
                return
            }

            DispatchQueue.main.async {
                var imported = 0
                var unchanged = 0
                var failed = 0

                func process(_ idx: Int) {
                    guard idx < pendingFiles.count else {
                        finish(
                            BackfillSummary(
                                totalFiles: allFiles.count,
                                pendingFiles: pendingFiles.count,
                                skippedByMemoryFiles: skippedByMemory,
                                importedFiles: imported,
                                unchangedFiles: unchanged,
                                failedFiles: failed,
                                errorMessage: nil
                            )
                        )
                        return
                    }

                    let (fileURL, profile) = pendingFiles[idx]
                    importSavedBagToHealthKit(
                        fileURL: fileURL,
                        profile: profile,
                        deviceName: nil
                    ) { result in
                        switch result {
                        case .imported:
                            imported += 1
                        case .alreadyPresent, .noData:
                            unchanged += 1
                        case .failed(let reason):
                            failed += 1
                            CustomLogger.log(
                                "[SensorBag][HK] Backfill failed for "
                                    + "\(fileURL.lastPathComponent): \(reason)"
                            )
                        }
                        process(idx + 1)
                    }
                }

                process(0)
            }
        }
    }

    @discardableResult
    static func resetBackfillMemory() throws -> SensorBagBackfillResetResult {
        try beginReset()
        defer { endReset() }
        return try SensorBagBackfillIndex.reset(in: documentsDirectory())
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
        HealthKitManager.requestWriteAuthorization { success in
            guard success else {
                completion?(.failed("HealthKit authorization failed"))
                return
            }
            writeBeatsToHealthKitAuthorized(
                beats,
                deviceName: deviceName,
                store: store,
                syncIdentifierPrefix: syncIdentifierPrefix,
                mode: .recovery,
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
        HealthKitManager.requestWriteAuthorization { success in
            guard success else {
                completion?(.failed("HealthKit authorization failed"))
                return
            }
            writeHeartRatesToHealthKitAuthorized(
                points,
                deviceName: deviceName,
                store: store,
                syncIdentifier: syncIdentifier,
                mode: .recovery,
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

    private static func listSavedFilesForBackfill() throws -> [(URL, Profile)] {
        let documents = documentsDirectory()
        var files: [(URL, Profile)] = []

        for profile in Profile.allCases {
            let dir = documents.appendingPathComponent(profile.rawValue, isDirectory: true)
            guard FileManager.default.fileExists(atPath: dir.path) else { continue }
            let urls: [URL]
            do {
                urls = try FileManager.default.contentsOfDirectory(
                    at: dir,
                    includingPropertiesForKeys: [.isRegularFileKey],
                    options: [.skipsHiddenFiles]
                )
            } catch {
                throw NSError(
                    domain: "SensorBagPersistence",
                    code: 2,
                    userInfo: [
                        NSLocalizedDescriptionKey:
                            "Can't list \(profile.rawValue) recordings: "
                            + error.localizedDescription
                    ]
                )
            }

            var binFiles: [URL] = []
            for url in urls where url.pathExtension.lowercased() == "bin" {
                let values: URLResourceValues
                do {
                    values = try url.resourceValues(forKeys: [.isRegularFileKey])
                } catch {
                    throw NSError(
                        domain: "SensorBagPersistence",
                        code: 3,
                        userInfo: [
                            NSLocalizedDescriptionKey:
                                "Can't inspect \(url.lastPathComponent): "
                                + error.localizedDescription
                        ]
                    )
                }
                if values.isRegularFile == true {
                    binFiles.append(url)
                }
            }
            binFiles.sort { $0.lastPathComponent < $1.lastPathComponent }
            for file in binFiles {
                files.append((file, profile))
            }
        }

        return files
    }

    private static func documentsDirectory() -> URL {
        FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
    }

    private static func backfillFileKey(fileURL: URL, profile: Profile) -> String {
        "\(profile.rawValue)/\(fileURL.lastPathComponent)"
    }

    private static func fileMetadata(
        fileURL: URL,
        includeIdentity: Bool = false
    ) throws -> FileMetadata {
        var keys: Set<URLResourceKey> = [.fileSizeKey, .contentModificationDateKey]
        if includeIdentity {
            keys.insert(.fileResourceIdentifierKey)
        }
        let values: URLResourceValues
        do {
            values = try fileURL.resourceValues(forKeys: keys)
        } catch {
            throw NSError(
                domain: "SensorBagPersistence",
                code: 4,
                userInfo: [
                    NSLocalizedDescriptionKey:
                        "Can't read metadata for \(fileURL.lastPathComponent): "
                        + error.localizedDescription
                ]
            )
        }
        guard let size = values.fileSize.map({ Int64($0) }), let mtime = values.contentModificationDate else {
            throw NSError(
                domain: "SensorBagPersistence",
                code: 5,
                userInfo: [
                    NSLocalizedDescriptionKey:
                        "Missing size or modification date for \(fileURL.lastPathComponent)"
                ]
            )
        }
        return FileMetadata(
            size: size,
            mtime: mtime,
            resourceIdentifier: values.fileResourceIdentifier as? NSObject
        )
    }

    private static func isFileMarkedBackfilled(
        fileURL: URL,
        profile: Profile,
        index: [String: SensorBagBackfillRecord]
    ) throws -> Bool {
        let key = backfillFileKey(fileURL: fileURL, profile: profile)
        guard let record = index[key] else { return false }
        let current = try fileMetadata(fileURL: fileURL)
        return record.fileSize == current.size && record.lastModifiedAt == current.mtime
    }

    private static func markFileBackfilled(
        fileURL: URL,
        profile: Profile,
        metadata: FileMetadata
    ) throws {
        try SensorBagBackfillIndex.markCompleted(
            key: backfillFileKey(fileURL: fileURL, profile: profile),
            fileSize: metadata.size,
            lastModifiedAt: metadata.mtime,
            in: documentsDirectory()
        )
    }

    private static func readStableFile(fileURL: URL) throws -> SavedFileSnapshot {
        let before = try fileMetadata(fileURL: fileURL, includeIdentity: true)
        let data = try Data(contentsOf: fileURL)
        let after = try fileMetadata(fileURL: fileURL, includeIdentity: true)
        guard fileMetadataMatches(before, after),
              Int64(data.count) == before.size else {
            throw NSError(
                domain: "SensorBagPersistence",
                code: 6,
                userInfo: [
                    NSLocalizedDescriptionKey:
                        "\(fileURL.lastPathComponent) changed while it was being read"
                ]
            )
        }
        return SavedFileSnapshot(data: data, metadata: before)
    }

    private static func verifyFileUnchanged(
        fileURL: URL,
        expected: FileMetadata
    ) throws {
        let current = try fileMetadata(fileURL: fileURL, includeIdentity: true)
        guard fileMetadataMatches(expected, current) else {
            throw NSError(
                domain: "SensorBagPersistence",
                code: 7,
                userInfo: [
                    NSLocalizedDescriptionKey:
                        "\(fileURL.lastPathComponent) changed during its HealthKit import; "
                        + "the replacement remains pending"
                ]
            )
        }
    }

    private static func fileMetadataMatches(
        _ lhs: FileMetadata,
        _ rhs: FileMetadata
    ) -> Bool {
        guard lhs.size == rhs.size, lhs.mtime == rhs.mtime else { return false }
        switch (lhs.resourceIdentifier, rhs.resourceIdentifier) {
        case (nil, nil):
            return true
        case let (lhs?, rhs?):
            return lhs.isEqual(rhs)
        default:
            return false
        }
    }

    private static func beginImport(
        key: String,
        completion: ((ImportResult) -> Void)?
    ) -> ImportCoordination {
        workStateLock.lock()
        defer { workStateLock.unlock() }

        guard !resetInProgress else {
            return .rejected("HealthKit backfill memory is being reset; this file remains pending")
        }
        if var active = activeImports[key] {
            if let completion {
                active.completions.append(completion)
                activeImports[key] = active
            }
            return .joined
        }
        activeImports[key] = ActiveImport(
            completions: completion.map { [$0] } ?? []
        )
        return .start
    }

    private static func completeImport(key: String, result: ImportResult) {
        workStateLock.lock()
        let completions = activeImports.removeValue(forKey: key)?.completions ?? []
        workStateLock.unlock()

        for completion in completions {
            deliverImportResult(result, to: completion)
        }
    }

    private static func deliverImportResult(
        _ result: ImportResult,
        to completion: ((ImportResult) -> Void)?
    ) {
        guard let completion else { return }
        if Thread.isMainThread {
            completion(result)
        } else {
            DispatchQueue.main.async {
                completion(result)
            }
        }
    }

    private static func beginBackfill(
        onlyPending: Bool,
        completion: @escaping (BackfillSummary) -> Void
    ) -> BackfillCoordination {
        workStateLock.lock()
        defer { workStateLock.unlock() }

        guard !resetInProgress else {
            return .rejected("HealthKit backfill memory is being reset")
        }
        if var activeBackfill {
            guard activeBackfill.onlyPending == onlyPending else {
                return .rejected("A different HealthKit backfill is already running")
            }
            activeBackfill.completions.append(completion)
            self.activeBackfill = activeBackfill
            return .joined
        }
        activeBackfill = ActiveBackfill(
            onlyPending: onlyPending,
            completions: [completion]
        )
        return .start
    }

    private static func completeBackfill(_ summary: BackfillSummary) {
        workStateLock.lock()
        let completions = activeBackfill?.completions ?? []
        activeBackfill = nil
        workStateLock.unlock()

        for completion in completions {
            deliverBackfillSummary(summary, to: completion)
        }
    }

    private static func deliverBackfillSummary(
        _ summary: BackfillSummary,
        to completion: @escaping (BackfillSummary) -> Void
    ) {
        if Thread.isMainThread {
            completion(summary)
        } else {
            DispatchQueue.main.async {
                completion(summary)
            }
        }
    }

    private static func beginReset() throws {
        workStateLock.lock()
        defer { workStateLock.unlock() }

        guard !resetInProgress else {
            throw NSError(
                domain: "SensorBagPersistence",
                code: 8,
                userInfo: [NSLocalizedDescriptionKey: "A backfill reset is already running"]
            )
        }
        guard activeBackfill == nil, activeImports.isEmpty else {
            throw NSError(
                domain: "SensorBagPersistence",
                code: 9,
                userInfo: [
                    NSLocalizedDescriptionKey:
                        "Wait for the active HealthKit import or backfill to finish before resetting"
                ]
            )
        }
        resetInProgress = true
    }

    private static func endReset() {
        workStateLock.lock()
        resetInProgress = false
        workStateLock.unlock()
    }

    private static func writeBeatsToHealthKitAuthorized(
        _ beats: [(Date, Bool)],
        deviceName: String?,
        store: HKHealthStore,
        syncIdentifierPrefix: String?,
        mode: ImportMode,
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

            func checkLegacyThenWrite() {
                hasLegacyHeartbeatSeries(
                    page: page,
                    deviceName: deviceName,
                    store: store
                ) { result in
                    switch result {
                    case .success(true):
                        processPage(idx + 1)
                    case .success(false):
                        writePage()
                    case .failure(let error):
                        completion?(
                            .failed(
                                "Can't check legacy heartbeat series: "
                                    + error.localizedDescription
                            )
                        )
                    }
                }
            }

            switch mode {
            case .newFile:
                writePage()
            case .recovery:
                if let syncIdentifier = pageSyncIdentifier {
                    hasSample(
                        withSyncIdentifier: syncIdentifier,
                        sampleType: heartbeatType,
                        store: store
                    ) { result in
                        switch result {
                        case .success(true):
                            processPage(idx + 1)
                        case .success(false):
                            checkLegacyThenWrite()
                        case .failure(let error):
                            completion?(
                                .failed(
                                    "Can't check heartbeat sync identifier: "
                                        + error.localizedDescription
                                )
                            )
                        }
                    }
                } else {
                    checkLegacyThenWrite()
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
        mode: ImportMode,
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
            hasLegacyHeartRateSeries(
                points: sorted,
                deviceName: deviceName,
                store: store
            ) { result in
                switch result {
                case .success(true):
                    completion?(.alreadyPresent)
                case .success(false):
                    writeSeries()
                case .failure(let error):
                    completion?(
                        .failed(
                            "Can't check legacy heart-rate series: "
                                + error.localizedDescription
                        )
                    )
                }
            }
        }

        switch mode {
        case .newFile:
            writeSeries()
        case .recovery:
            if let syncIdentifier {
                hasSample(
                    withSyncIdentifier: syncIdentifier,
                    sampleType: hrType,
                    store: store
                ) { result in
                    switch result {
                    case .success(true):
                        completion?(.alreadyPresent)
                    case .success(false):
                        checkLegacyThenWrite()
                    case .failure(let error):
                        completion?(
                            .failed(
                                "Can't check heart-rate sync identifier: "
                                    + error.localizedDescription
                            )
                        )
                    }
                }
            } else {
                checkLegacyThenWrite()
            }
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
        completion: @escaping (Result<Bool, Error>) -> Void
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
                completion(.failure(error))
                return
            }
            completion(.success(!(samples?.isEmpty ?? true)))
        }
        store.execute(query)
    }

    private static func hasLegacyHeartRateSeries(
        points: [(Date, Double)],
        deviceName: String?,
        store: HKHealthStore,
        completion: @escaping (Result<Bool, Error>) -> Void
    ) {
        guard
            let hrType = HKQuantityType.quantityType(forIdentifier: .heartRate),
            let start = points.first?.0,
            let end = points.last?.0
        else {
            completion(.success(false))
            return
        }
        let predicate = HKQuery.predicateForSamples(
            withStart: start.addingTimeInterval(-1),
            end: end.addingTimeInterval(1),
            options: []
        )
        let query = HKSampleQuery(sampleType: hrType, predicate: predicate, limit: 50, sortDescriptors: nil) {
            _, samples, error in
            if let error {
                completion(.failure(error))
                return
            }
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
            completion(.success(exists))
        }
        store.execute(query)
    }

    private static func hasLegacyHeartbeatSeries(
        page: [(Date, Bool)],
        deviceName: String?,
        store: HKHealthStore,
        completion: @escaping (Result<Bool, Error>) -> Void
    ) {
        guard let start = page.first?.0, let end = page.last?.0 else {
            completion(.success(false))
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
        ) { _, samples, error in
            if let error {
                completion(.failure(error))
                return
            }
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
            completion(.success(exists))
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
        case invalidECGTimestampRange

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
            case .invalidECGTimestampRange:
                return "Invalid ECG timestamp range"
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

        mutating func readInt16() throws -> Int16 {
            Int16(bitPattern: try readInteger())
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

    static func loadECGWindow(
        centeredAt center: Date,
        halfWidth: TimeInterval,
        liveEvents: [SensorEvent] = [],
        recordingDirectory: URL? = nil
    ) throws -> [ECGPlotPoint] {
        let range = center.addingTimeInterval(-halfWidth)...center.addingTimeInterval(halfWidth)
        let livePoints = ecgPoints(from: liveEvents, in: range)
        var filePoints: [ECGPlotPoint] = []

        let liveCoversRange =
            (livePoints.first?.timestamp ?? .distantFuture) <= range.lowerBound
            && (livePoints.last?.timestamp ?? .distantPast) >= range.upperBound
        if !liveCoversRange {
            let directory = recordingDirectory ?? documentsDirectory().appendingPathComponent(
                Profile.continuous.rawValue,
                isDirectory: true
            )
            for entry in try candidateECGFiles(in: directory, covering: range) {
                let snapshot = try readStableFile(fileURL: entry.url)
                do {
                    filePoints.append(
                        contentsOf: try decodeECGPoints(from: snapshot.data, in: range)
                    )
                } catch {
                    throw NSError(
                        domain: "SensorBagPersistence",
                        code: 10,
                        userInfo: [
                            NSLocalizedDescriptionKey:
                                "Can't read ECG from \(entry.url.lastPathComponent): "
                                + error.localizedDescription
                        ]
                    )
                }
            }
        }

        if let firstLiveTimestamp = livePoints.first?.timestamp {
            filePoints.removeAll { $0.timestamp >= firstLiveTimestamp }
        }
        return (filePoints + livePoints).sorted { $0.timestamp < $1.timestamp }
    }

    static func decodeECGPoints(
        from data: Data,
        in range: ClosedRange<Date>
    ) throws -> [ECGPlotPoint] {
        var reader = BinaryReader(data: data)
        let version = try reader.readUInt32()
        guard version == 0 || version == 1 else {
            throw SensorBagDecodeError.unsupportedVersion(version)
        }
        let eventCount = Int(try reader.readUInt32())
        var points: [ECGPlotPoint] = []

        for _ in 0..<eventCount {
            let receivedAt = Date(timeIntervalSince1970: try reader.readDouble())
            let eventType = try reader.readUInt32()
            switch eventType {
            case 1:
                let sampleCount = Int(try reader.readUInt32())
                for _ in 0..<sampleCount {
                    _ = try reader.readInt32()
                    let rrCount = Int(try reader.readUInt32())
                    try reader.skip(
                        byteCount: try safeMultiply(rrCount, MemoryLayout<Double>.size)
                    )
                }
            case 2:
                let sampleCount = Int(try reader.readUInt32())
                if version == 0 {
                    var packet: [(UInt64, Int16)] = []
                    packet.reserveCapacity(sampleCount)
                    for _ in 0..<sampleCount {
                        let timestamp = try reader.readUInt64()
                        let rawVoltage = try reader.readInt32()
                        packet.append((timestamp, clampedInt16(rawVoltage)))
                    }
                    guard let lastTimestamp = packet.last?.0 else { continue }
                    for (timestamp, voltage) in packet {
                        let wallTime = wallTime(
                            receivedAt: receivedAt,
                            deviceTimestamp: timestamp,
                            lastDeviceTimestamp: lastTimestamp
                        )
                        if range.contains(wallTime) {
                            points.append(
                                ECGPlotPoint(timestamp: wallTime, voltage: voltage)
                            )
                        }
                    }
                } else {
                    let firstTimestamp = try reader.readUInt64()
                    let lastTimestamp = try reader.readUInt64()
                    guard lastTimestamp >= firstTimestamp else {
                        throw SensorBagDecodeError.invalidECGTimestampRange
                    }
                    let packetDuration = Double(lastTimestamp - firstTimestamp) / 1_000_000_000
                    for sampleIndex in 0..<sampleCount {
                        let voltage = try reader.readInt16()
                        let fraction = sampleCount > 1
                            ? Double(sampleIndex) / Double(sampleCount - 1)
                            : 1
                        let wallTime = receivedAt.addingTimeInterval(
                            -packetDuration * (1 - fraction)
                        )
                        if range.contains(wallTime) {
                            points.append(
                                ECGPlotPoint(timestamp: wallTime, voltage: voltage)
                            )
                        }
                    }
                }
            case 3:
                let sampleCount = Int(try reader.readUInt32())
                if version == 0 {
                    try reader.skip(
                        byteCount: try safeMultiply(
                            sampleCount,
                            MemoryLayout<UInt64>.size + 3 * MemoryLayout<Int32>.size
                        )
                    )
                } else {
                    _ = try reader.readUInt64()
                    _ = try reader.readUInt64()
                    try reader.skip(
                        byteCount: try safeMultiply(
                            sampleCount,
                            3 * MemoryLayout<Int16>.size
                        )
                    )
                }
            case 4:
                _ = try reader.readInt32()
            case 5, 7:
                let textLength = Int(try reader.readUInt32())
                try reader.skip(byteCount: textLength)
            case 6:
                try reader.skip(byteCount: 5 * MemoryLayout<Double>.size)
            default:
                throw SensorBagDecodeError.unsupportedEventType(eventType)
            }
        }
        return points
    }

    private static func ecgPoints(
        from events: [SensorEvent],
        in range: ClosedRange<Date>
    ) -> [ECGPlotPoint] {
        events
            .flatMap { ecgPoints(from: $0) }
            .filter { range.contains($0.timestamp) }
            .sorted { $0.timestamp < $1.timestamp }
    }

    /// Convert one received ECG packet to wall-clock points. The presentation
    /// layer uses the same conversion as persisted recordings so a live trace
    /// joins its saved counterpart without a timestamp discontinuity.
    static func ecgPoints(from event: SensorEvent) -> [ECGPlotPoint] {
        guard case .ecgSamples(let packet) = event.data,
              let lastTimestamp = packet.samples.last?.timestamp
        else { return [] }
        return packet.samples.map { sample in
            ECGPlotPoint(
                timestamp: wallTime(
                    receivedAt: event.timestamp,
                    deviceTimestamp: sample.timestamp,
                    lastDeviceTimestamp: lastTimestamp
                ),
                voltage: sample.voltage
            )
        }
    }

    private static func wallTime(
        receivedAt: Date,
        deviceTimestamp: UInt64,
        lastDeviceTimestamp: UInt64
    ) -> Date {
        if deviceTimestamp <= lastDeviceTimestamp {
            return receivedAt.addingTimeInterval(
                -Double(lastDeviceTimestamp - deviceTimestamp) / 1_000_000_000
            )
        }
        return receivedAt.addingTimeInterval(
            Double(deviceTimestamp - lastDeviceTimestamp) / 1_000_000_000
        )
    }

    private static func clampedInt16(_ value: Int32) -> Int16 {
        Int16(clamping: value)
    }

    private static func candidateECGFiles(
        in directory: URL,
        covering range: ClosedRange<Date>
    ) throws -> [ECGFileIndexEntry] {
        let entries = try indexedECGFiles(in: directory)
        guard !entries.isEmpty else { return [] }
        // Names record finalization time, not the start of the contained data.
        // Include the file finalized after the window ends, even when the
        // window crosses one or more boundaries. Keep the preceding neighbors
        // for packet overlap and the filename's whole-second rounding.
        let first = entries.firstIndex {
            $0.finalizedAt >= range.lowerBound.timeIntervalSince1970
        } ?? entries.count
        let last = entries.firstIndex {
            $0.finalizedAt > range.upperBound.timeIntervalSince1970
        } ?? entries.count
        let lower = max(0, first - 2)
        let upper = min(entries.count, last + 1)
        return Array(entries[lower..<upper])
    }

    private static func indexedECGFiles(in directory: URL) throws -> [ECGFileIndexEntry] {
        let fileManager = FileManager.default
        guard fileManager.fileExists(atPath: directory.path) else { return [] }
        let attributes = try fileManager.attributesOfItem(atPath: directory.path)
        let modificationDate = attributes[.modificationDate] as? Date ?? .distantPast

        ecgFileIndexLock.lock()
        if let cache = ecgFileIndexCache,
           cache.directoryPath == directory.path,
           cache.modificationDate == modificationDate {
            ecgFileIndexLock.unlock()
            return cache.entries
        }
        ecgFileIndexLock.unlock()

        let entries = try fileManager.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ).compactMap { url -> ECGFileIndexEntry? in
            guard url.pathExtension.lowercased() == "bin" else { return nil }
            let timestampText = url.deletingPathExtension().lastPathComponent
                .split(separator: "-", maxSplits: 1)
                .first
            guard let timestampText,
                  let timestamp = TimeInterval(timestampText)
            else { return nil }
            return ECGFileIndexEntry(url: url, finalizedAt: timestamp)
        }.sorted {
            if $0.finalizedAt == $1.finalizedAt {
                return $0.url.lastPathComponent < $1.url.lastPathComponent
            }
            return $0.finalizedAt < $1.finalizedAt
        }

        ecgFileIndexLock.lock()
        ecgFileIndexCache = ECGFileIndexCache(
            directoryPath: directory.path,
            modificationDate: modificationDate,
            entries: entries
        )
        ecgFileIndexLock.unlock()
        return entries
    }

    private static func invalidateECGFileIndex() {
        ecgFileIndexLock.lock()
        ecgFileIndexCache = nil
        ecgFileIndexLock.unlock()
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
