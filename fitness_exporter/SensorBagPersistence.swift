import CryptoKit
import Foundation
import HealthKit

/// Utilities for persisting sensor bags and exporting heartbeats to HealthKit.
enum SensorBagPersistence {
    /// Maps the Polar device's monotonic ECG clock onto wall time. A single
    /// anchor is intentionally reused across packets so Bluetooth delivery
    /// jitter cannot introduce artificial gaps or overlaps in the trace.
    struct ECGTimelineReconstructor {
        static let discontinuityTolerance: TimeInterval = 2

        private var anchorDeviceTimestamp: UInt64?
        private var anchorWallTime: Date?
        private var lastDeviceTimestamp: UInt64?

        mutating func reset() {
            anchorDeviceTimestamp = nil
            anchorWallTime = nil
            lastDeviceTimestamp = nil
        }

        mutating func wallTimes(
            receivedAt: Date,
            deviceTimestamps: [UInt64]
        ) -> [Date] {
            guard let packetLastTimestamp = deviceTimestamps.last else { return [] }

            let deviceClockDidNotAdvance = lastDeviceTimestamp.map {
                packetLastTimestamp <= $0
            } ?? false
            let projectedPacketEnd = projectedWallTime(for: packetLastTimestamp)
            let arrivalDrift = projectedPacketEnd.map {
                receivedAt.timeIntervalSince($0)
            }
            if anchorDeviceTimestamp == nil
                || deviceClockDidNotAdvance
                || arrivalDrift.map({ abs($0) > Self.discontinuityTolerance }) == true
            {
                anchorDeviceTimestamp = packetLastTimestamp
                anchorWallTime = receivedAt
            }

            lastDeviceTimestamp = packetLastTimestamp
            return deviceTimestamps.compactMap { projectedWallTime(for: $0) }
        }

        private func projectedWallTime(for deviceTimestamp: UInt64) -> Date? {
            guard let anchorDeviceTimestamp, let anchorWallTime else { return nil }
            let offset: TimeInterval
            if deviceTimestamp >= anchorDeviceTimestamp {
                offset = Double(deviceTimestamp - anchorDeviceTimestamp) / 1_000_000_000
            } else {
                offset = -Double(anchorDeviceTimestamp - deviceTimestamp) / 1_000_000_000
            }
            return anchorWallTime.addingTimeInterval(offset)
        }
    }

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
        var migratedHeartRateFiles: Int = 0
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
        let migrateLegacyHeartRates: Bool
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
        migrateLegacyHeartRates: Bool = false,
        completion: @escaping (BackfillSummary) -> Void
    ) {
        switch beginBackfill(
            onlyPending: onlyPending,
            migrateLegacyHeartRates: migrateLegacyHeartRates,
            completion: completion
        ) {
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
            let pendingFiles: [(URL, Profile, Bool)]
            let skippedByMemory: Int
            do {
                allFiles = try listSavedFilesForBackfill()
                let memorySnapshot = try SensorBagBackfillIndex.records(in: documentsDirectory())
                let migrationSnapshot = migrateLegacyHeartRates
                    ? try SensorBagBackfillIndex.heartRateMigrationRecords(in: documentsDirectory())
                    : [:]
                pendingFiles = try allFiles.compactMap { fileURL, profile in
                    let imported = try isFileMarkedBackfilled(
                            fileURL: fileURL,
                            profile: profile,
                            index: memorySnapshot
                    )
                    let migrated: Bool
                    if migrateLegacyHeartRates {
                        migrated = try isFileMarkedBackfilled(
                            fileURL: fileURL,
                            profile: profile,
                            index: migrationSnapshot
                        )
                    } else {
                        migrated = true
                    }
                    if onlyPending && imported && migrated {
                        return nil
                    }
                    return (fileURL, profile, imported)
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
                var migrated = 0

                func process(_ idx: Int) {
                    guard idx < pendingFiles.count else {
                        var summary = BackfillSummary(
                                totalFiles: allFiles.count,
                                pendingFiles: pendingFiles.count,
                                skippedByMemoryFiles: skippedByMemory,
                                importedFiles: imported,
                                unchangedFiles: unchanged,
                                failedFiles: failed,
                                errorMessage: nil
                            )
                        summary.migratedHeartRateFiles = migrated
                        finish(summary)
                        return
                    }

                    let (fileURL, profile, alreadyBackfilled) = pendingFiles[idx]
                    func finishFile(_ finalResult: ImportResult, migratedFile: Bool = false) {
                        switch finalResult {
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
                        if migratedFile { migrated += 1 }
                        process(idx + 1)
                    }
                    func afterImport(_ result: ImportResult) {
                        guard migrateLegacyHeartRates else {
                            finishFile(result)
                            return
                        }
                        if case .failed = result {
                            finishFile(result)
                            return
                        }
                        migrateLegacyHeartRatesForSavedBag(
                            fileURL: fileURL,
                            profile: profile
                        ) { migrationResult in
                            switch migrationResult {
                            case .failed:
                                finishFile(migrationResult)
                            case .imported:
                                finishFile(result, migratedFile: true)
                            case .alreadyPresent, .noData:
                                finishFile(result)
                            }
                        }
                    }
                    if migrateLegacyHeartRates && alreadyBackfilled {
                        // Existing backfill memory means the series was already imported.
                        // Query it before any write so denied read access cannot create duplicates.
                        afterImport(.alreadyPresent)
                    } else {
                        importSavedBagToHealthKit(
                            fileURL: fileURL,
                            profile: profile,
                            deviceName: nil,
                            completion: afterImport
                        )
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
        migrateLegacyHeartRates: Bool,
        completion: @escaping (BackfillSummary) -> Void
    ) -> BackfillCoordination {
        workStateLock.lock()
        defer { workStateLock.unlock() }

        guard !resetInProgress else {
            return .rejected("HealthKit backfill memory is being reset")
        }
        if var activeBackfill {
            guard activeBackfill.onlyPending == onlyPending,
                  activeBackfill.migrateLegacyHeartRates == migrateLegacyHeartRates else {
                return .rejected("A different HealthKit backfill is already running")
            }
            activeBackfill.completions.append(completion)
            self.activeBackfill = activeBackfill
            return .joined
        }
        activeBackfill = ActiveBackfill(
            onlyPending: onlyPending,
            migrateLegacyHeartRates: migrateLegacyHeartRates,
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

        let sorted = normalizedHeartRatePoints(points)
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
            builder.finishSeries(metadata: metadata, endDate: endDate) { samples, error in
                if let error {
                    let message = "Can't finish HR series: \(error.localizedDescription)"
                    CustomLogger.log("[SensorBag][HK] \(message)")
                    completion?(.failed(message))
                    return
                }
                guard !(samples?.isEmpty ?? true) else {
                    completion?(.failed("HR series builder returned no samples"))
                    return
                }
                completion?(.imported)
            }
        }

        func checkLegacyThenWrite() {
            hasLegacyHeartRateSeries(
                points: sorted,
                deviceName: deviceName,
                syncIdentifier: syncIdentifier,
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
        syncIdentifier: String?,
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
        let query = HKSampleQuery(
            sampleType: hrType,
            predicate: predicate,
            limit: HKObjectQueryNoLimit,
            sortDescriptors: nil
        ) {
            _, samples, error in
            if let error {
                completion(.failure(error))
                return
            }
            verifiedHeartRateSeriesIDs(
                points: points,
                samples: samples as? [HKQuantitySample] ?? [],
                deviceName: deviceName,
                syncIdentifier: syncIdentifier,
                store: store
            ) { result in
                completion(result.map { !$0.isEmpty })
            }
        }
        store.execute(query)
    }

    private struct LegacyHeartRateKey: Hashable {
        let millisecond: Int64
        let bpm: Int

        init(timestamp: Date, bpm: Double) {
            millisecond = Int64((timestamp.timeIntervalSinceReferenceDate * 1_000).rounded())
            self.bpm = Int(bpm.rounded())
        }
    }

    static func normalizedHeartRatePoints(_ points: [(Date, Double)]) -> [(Date, Double)] {
        // Keep the last reading for a timestamp, matching the series builder's
        // single-quantity-per-time behavior. Preserve raw points for legacy deletion.
        var byTimestamp: [Date: Double] = [:]
        for (timestamp, bpm) in points where bpm.isFinite && bpm > 0 {
            byTimestamp[timestamp] = bpm
        }
        return byTimestamp.map { ($0.key, $0.value) }.sorted { $0.0 < $1.0 }
    }

    struct HeartRateSeriesObservation {
        let id: UUID
        let syncIdentifier: String?
        let points: [(Date, Double)]
    }

    enum HeartRateSeriesVerificationError: LocalizedError {
        case incompleteSeries

        var errorDescription: String? {
            "A partial or ambiguous heart-rate series overlaps this recording; no legacy samples were deleted"
        }
    }

    /// The builder may return several HKQuantitySamples for one recording. A replacement
    /// is usable only when the combined quantities exactly equal the saved bag.
    static func matchingHeartRateSeriesIDs(
        points: [(Date, Double)],
        observations: [HeartRateSeriesObservation],
        syncIdentifier: String?
    ) throws -> Set<UUID> {
        let expected = Dictionary(
            points.map { (LegacyHeartRateKey(timestamp: $0.0, bpm: $0.1), 1) },
            uniquingKeysWith: +
        )
        func counts(_ series: [HeartRateSeriesObservation]) -> [LegacyHeartRateKey: Int] {
            Dictionary(
                series.flatMap(\.points).map { (LegacyHeartRateKey(timestamp: $0.0, bpm: $0.1), 1) },
                uniquingKeysWith: +
            )
        }
        let synced = observations.filter { syncIdentifier != nil && $0.syncIdentifier == syncIdentifier }
        let legacy = observations.filter { $0.syncIdentifier == nil }
        let syncedCounts = counts(synced)
        let legacyCounts = counts(legacy)
        let syncedMatches = !synced.isEmpty && syncedCounts == expected
        let legacyMatches = !legacy.isEmpty && legacyCounts == expected

        // A partial series may be a split recording whose other chunks are unreadable.
        // Do not create another series or delete the individual samples in that state.
        let partialLegacy = !legacyCounts.isEmpty && legacyCounts != expected
            && legacyCounts.contains { expected[$0.key] != nil }
        guard !partialLegacy, synced.isEmpty || syncedMatches, !(syncedMatches && legacyMatches) else {
            throw HeartRateSeriesVerificationError.incompleteSeries
        }
        if syncedMatches { return Set(synced.map(\.id)) }
        if legacyMatches { return Set(legacy.map(\.id)) }
        return []
    }

    private static func verifiedHeartRateSeriesIDs(
        points: [(Date, Double)],
        samples: [HKQuantitySample],
        deviceName: String?,
        syncIdentifier: String?,
        store: HKHealthStore,
        completion: @escaping (Result<Set<UUID>, Error>) -> Void
    ) {
        guard let hrType = HKQuantityType.quantityType(forIdentifier: .heartRate) else {
            completion(.success([]))
            return
        }
        guard let start = points.map(\.0).min(), let end = points.map(\.0).max() else {
            completion(.success([]))
            return
        }
        let candidates = samples.filter { sample in
            knownHeartRateSourceBundleIDs.contains(sample.sourceRevision.source.bundleIdentifier)
                && (deviceName == nil || sample.device?.name == deviceName)
                && sample.startDate >= start.addingTimeInterval(-1)
                && sample.endDate <= end.addingTimeInterval(1)
                && (sample.count > 1 || (syncIdentifier != nil
                    && sample.metadata?[HKMetadataKeySyncIdentifier] as? String == syncIdentifier))
        }
        let unit = HKUnit.count().unitDivided(by: .minute())
        var observations: [HeartRateSeriesObservation] = []

        func inspect(_ index: Int) {
            guard index < candidates.count else {
                completion(Result {
                    try matchingHeartRateSeriesIDs(
                        points: points,
                        observations: observations,
                        syncIdentifier: syncIdentifier
                    )
                })
                return
            }
            let sample = candidates[index]
            let predicate = HKQuery.predicateForObject(with: sample.uuid)
            var quantities: [(Date, Double)] = []
            let query = HKQuantitySeriesSampleQuery(quantityType: hrType, predicate: predicate) {
                _, quantity, interval, _, done, error in
                if let error {
                    completion(.failure(error))
                    return
                }
                if let quantity, let interval {
                    quantities.append((interval.start, quantity.doubleValue(for: unit)))
                }
                guard done else { return }
                guard quantities.count == sample.count else {
                    completion(.failure(HeartRateSeriesVerificationError.incompleteSeries))
                    return
                }
                observations.append(HeartRateSeriesObservation(
                    id: sample.uuid,
                    syncIdentifier: sample.metadata?[HKMetadataKeySyncIdentifier] as? String,
                    points: quantities
                ))
                inspect(index + 1)
            }
            store.execute(query)
        }
        inspect(0)
    }

    struct LegacyHeartRateObservation {
        let id: UUID
        let timestamp: Date
        let duration: TimeInterval
        let bpm: Double
        let count: Int
        let metadataEmpty: Bool
        let sourceBundleIdentifier: String
    }

    enum LegacyHeartRateMigrationError: LocalizedError {
        case unknownSource
        case duplicate
        case differentApp

        var errorDescription: String? {
            switch self {
            case .unknownSource:
                return "Matching HR samples have an unknown source; no samples were deleted"
            case .duplicate:
                return "Ambiguous duplicate legacy HR samples; no samples were deleted"
            case .differentApp:
                return "Legacy HR samples belong to another app bundle ID and cannot be deleted here"
            }
        }
    }

    private static let knownHeartRateSourceBundleIDs: Set<String> = [
        "com.artemz.fitness-exporter",
        "com.artemz.fitness-exporter-my"
    ]

    static func legacyHeartRateSampleIDs(
        points: [(Date, Double)],
        observations: [LegacyHeartRateObservation],
        currentBundleIdentifier: String?
    ) throws -> Set<UUID> {
        let expected = Dictionary(
            points.map { (LegacyHeartRateKey(timestamp: $0.0, bpm: $0.1), 1) },
            uniquingKeysWith: +
        )
        let matches = observations.filter { observation in
            guard observation.count == 1,
                  observation.metadataEmpty,
                  abs(observation.duration) < 0.001,
                  observation.bpm.isFinite,
                  abs(observation.bpm - observation.bpm.rounded()) < 0.001 else {
                return false
            }
            return expected[LegacyHeartRateKey(
                timestamp: observation.timestamp,
                bpm: observation.bpm
            )] != nil
        }
        guard matches.allSatisfy({
            knownHeartRateSourceBundleIDs.contains($0.sourceBundleIdentifier)
        }) else {
            throw LegacyHeartRateMigrationError.unknownSource
        }
        let foundCounts = Dictionary(
            matches.map { (LegacyHeartRateKey(timestamp: $0.timestamp, bpm: $0.bpm), 1) },
            uniquingKeysWith: +
        )
        guard foundCounts.allSatisfy({ $0.value <= (expected[$0.key] ?? 0) }) else {
            throw LegacyHeartRateMigrationError.duplicate
        }
        guard matches.allSatisfy({ $0.sourceBundleIdentifier == currentBundleIdentifier }) else {
            throw LegacyHeartRateMigrationError.differentApp
        }
        return Set(matches.map(\.id))
    }

    /// Previous releases saved one zero-duration HKQuantitySample per Polar HR point,
    /// with empty metadata. Only samples matching the saved bag's exact points and
    /// one of this project's historical bundle IDs can be migration candidates.
    private static func migrateLegacyHeartRatesForSavedBag(
        fileURL: URL,
        profile: Profile,
        completion: @escaping (ImportResult) -> Void
    ) {
        DispatchQueue.global(qos: .utility).async {
            let file: SavedFileSnapshot
            let rawPoints: [(Date, Double)]
            let points: [(Date, Double)]
            do {
                file = try readStableFile(fileURL: fileURL)
                rawPoints = heartRatePoints(from: try decodeEventsFromBinary(file.data))
                points = normalizedHeartRatePoints(rawPoints)
            } catch {
                completion(.failed("Can't prepare HR migration: \(error.localizedDescription)"))
                return
            }

            func markComplete(_ result: ImportResult) {
                do {
                    try verifyFileUnchanged(fileURL: fileURL, expected: file.metadata)
                    try SensorBagBackfillIndex.markHeartRateMigrationCompleted(
                        key: backfillFileKey(fileURL: fileURL, profile: profile),
                        fileSize: file.metadata.size,
                        lastModifiedAt: file.metadata.mtime,
                        in: documentsDirectory()
                    )
                    completion(result)
                } catch {
                    completion(.failed("Can't record HR migration: \(error.localizedDescription)"))
                }
            }

            guard !rawPoints.isEmpty else {
                markComplete(.noData)
                return
            }
            guard !points.isEmpty else {
                completion(.failed("No valid heart-rate points are available for a replacement series"))
                return
            }
            guard let hrType = HKQuantityType.quantityType(forIdentifier: .heartRate),
                  let start = rawPoints.map(\.0).min(), let end = rawPoints.map(\.0).max() else {
                completion(.failed("Heart rate type or sample timestamps are unavailable"))
                return
            }

            let store = HKHealthStore()
            DispatchQueue.main.async {
                store.requestAuthorization(toShare: [hrType], read: [hrType]) { authorized, error in
                    guard authorized, error == nil else {
                        completion(.failed("Heart rate read/write authorization failed: "
                            + (error?.localizedDescription ?? "not authorized")))
                        return
                    }

                    func query(_ done: @escaping (Result<[HKQuantitySample], Error>) -> Void) {
                        let predicate = HKQuery.predicateForSamples(
                            withStart: start.addingTimeInterval(-0.01),
                            end: end.addingTimeInterval(0.01),
                            options: []
                        )
                        let query = HKSampleQuery(
                            sampleType: hrType,
                            predicate: predicate,
                            limit: HKObjectQueryNoLimit,
                            sortDescriptors: nil
                        ) { _, samples, error in
                            if let error {
                                done(.failure(error))
                            } else {
                                done(.success(samples as? [HKQuantitySample] ?? []))
                            }
                        }
                        store.execute(query)
                    }

                    let syncID = syncIdentifier(
                        profile: profile,
                        stream: "hr",
                        fileHash: sha256Hex(file.data)
                    )
                    query { firstQuery in
                        let samples: [HKQuantitySample]
                        switch firstQuery {
                        case .success(let found): samples = found
                        case .failure(let error):
                            completion(.failed("Can't read HR samples: \(error.localizedDescription)"))
                            return
                        }
                        verifiedHeartRateSeriesIDs(
                            points: points,
                            samples: samples,
                            deviceName: nil,
                            syncIdentifier: syncID,
                            store: store
                        ) { seriesResult in
                            let seriesIDs: Set<UUID>
                            switch seriesResult {
                            case .success(let ids) where !ids.isEmpty: seriesIDs = ids
                            case .success:
                                completion(.failed(
                                    "No verified replacement HR series was found; check HealthKit read access"
                                ))
                                return
                            case .failure(let error):
                                completion(.failed("Can't verify replacement HR series: \(error.localizedDescription)"))
                                return
                            }

                            let unit = HKUnit.count().unitDivided(by: .minute())
                            let observations = samples.filter { !seriesIDs.contains($0.uuid) }.map { sample in
                                LegacyHeartRateObservation(
                                    id: sample.uuid,
                                    timestamp: sample.startDate,
                                    duration: sample.endDate.timeIntervalSince(sample.startDate),
                                    bpm: sample.quantity.doubleValue(for: unit),
                                    count: sample.count,
                                    metadataEmpty: sample.metadata?.isEmpty ?? true,
                                    sourceBundleIdentifier: sample.sourceRevision.source.bundleIdentifier
                                )
                            }
                            let legacyIDs: Set<UUID>
                            do {
                                legacyIDs = try legacyHeartRateSampleIDs(
                                    points: rawPoints,
                                    observations: observations,
                                    currentBundleIdentifier: Bundle.main.bundleIdentifier
                                )
                            } catch {
                                completion(.failed(error.localizedDescription))
                                return
                            }
                            let legacy = samples.filter { legacyIDs.contains($0.uuid) }
                            guard !legacy.isEmpty else {
                                markComplete(.alreadyPresent)
                                return
                            }
                            store.delete(legacy.map { $0 as HKObject }) { deleted, error in
                                guard deleted, error == nil else {
                                    completion(.failed("Can't delete legacy HR samples: "
                                        + (error?.localizedDescription ?? "unknown error")))
                                    return
                                }
                                let deletedIDs = Set(legacy.map(\.uuid))
                                query { secondQuery in
                                    switch secondQuery {
                                    case .failure(let error):
                                        completion(.failed("Can't verify HR deletion: \(error.localizedDescription)"))
                                    case .success(let remaining):
                                        guard seriesIDs.isSubset(of: Set(remaining.map(\.uuid))),
                                              remaining.allSatisfy({ !deletedIDs.contains($0.uuid) }) else {
                                            completion(.failed("Legacy HR samples remained after deletion"))
                                            return
                                        }
                                        markComplete(.imported)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
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
        var fileTimeline = ECGTimelineReconstructor()

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
                        contentsOf: try decodeECGPoints(
                            from: snapshot.data,
                            in: range,
                            timeline: &fileTimeline
                        )
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
        var timeline = ECGTimelineReconstructor()
        return try decodeECGPoints(from: data, in: range, timeline: &timeline)
    }

    private static func decodeECGPoints(
        from data: Data,
        in range: ClosedRange<Date>,
        timeline: inout ECGTimelineReconstructor
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
                    points.append(
                        contentsOf: mappedECGPoints(
                            deviceTimestamps: packet.map(\.0),
                            voltages: packet.map(\.1),
                            receivedAt: receivedAt,
                            timeline: &timeline
                        )
                        .filter { range.contains($0.timestamp) }
                    )
                } else {
                    let firstTimestamp = try reader.readUInt64()
                    let lastTimestamp = try reader.readUInt64()
                    guard lastTimestamp >= firstTimestamp else {
                        throw SensorBagDecodeError.invalidECGTimestampRange
                    }
                    var voltages: [Int16] = []
                    voltages.reserveCapacity(sampleCount)
                    for _ in 0..<sampleCount {
                        voltages.append(try reader.readInt16())
                    }
                    points.append(
                        contentsOf: mappedECGPoints(
                            deviceTimestamps: interpolatedTimestamps(
                                first: firstTimestamp,
                                last: lastTimestamp,
                                count: sampleCount
                            ),
                            voltages: voltages,
                            receivedAt: receivedAt,
                            timeline: &timeline
                        )
                        .filter { range.contains($0.timestamp) }
                    )
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
        var timeline = ECGTimelineReconstructor()
        return events
            .sorted { $0.timestamp < $1.timestamp }
            .flatMap { ecgPoints(from: $0, timeline: &timeline) }
            .filter { range.contains($0.timestamp) }
            .sorted { $0.timestamp < $1.timestamp }
    }

    /// Convert one received ECG packet to wall-clock points. The presentation
    /// layer uses the same conversion as persisted recordings so a live trace
    /// joins its saved counterpart without a timestamp discontinuity.
    static func ecgPoints(from event: SensorEvent) -> [ECGPlotPoint] {
        var timeline = ECGTimelineReconstructor()
        return ecgPoints(from: event, timeline: &timeline)
    }

    static func ecgPoints(
        from event: SensorEvent,
        timeline: inout ECGTimelineReconstructor
    ) -> [ECGPlotPoint] {
        guard case .ecgSamples(let packet) = event.data,
              !packet.samples.isEmpty
        else { return [] }
        return mappedECGPoints(
            deviceTimestamps: packet.samples.map(\.timestamp),
            voltages: packet.samples.map(\.voltage),
            receivedAt: event.timestamp,
            timeline: &timeline
        )
    }

    private static func mappedECGPoints(
        deviceTimestamps: [UInt64],
        voltages: [Int16],
        receivedAt: Date,
        timeline: inout ECGTimelineReconstructor
    ) -> [ECGPlotPoint] {
        let wallTimes = timeline.wallTimes(
            receivedAt: receivedAt,
            deviceTimestamps: deviceTimestamps
        )
        return zip(wallTimes, voltages).map { timestamp, voltage in
            ECGPlotPoint(timestamp: timestamp, voltage: voltage)
        }
    }

    private static func interpolatedTimestamps(
        first: UInt64,
        last: UInt64,
        count: Int
    ) -> [UInt64] {
        guard count > 0 else { return [] }
        guard count > 1 else { return [last] }
        let span = Double(last - first)
        return (0..<count).map { index in
            let fraction = Double(index) / Double(count - 1)
            return first + UInt64((span * fraction).rounded())
        }
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
