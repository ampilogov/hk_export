import Foundation

struct UploadDoneRecord: Codable, Equatable {
    let fileName: String
    let fileSize: Int64
    let lastModifiedAt: Date
}

enum UploadCoreError: Error, CustomStringConvertible, LocalizedError {
    case invalidBookmark
    case directoryListFailed(String)
    case fileReadFailed(String)
    case completionState(String)
    case network(String)
    case cancelled

    var description: String {
        switch self {
        case .invalidBookmark: return "Invalid directory bookmark"
        case .directoryListFailed(let message):
            return "Failed to list directory: \(message)"
        case .fileReadFailed(let name): return "Failed to read \(name)"
        case .completionState(let message):
            return "Failed to update upload completion state: \(message)"
        case .network(let msg): return msg
        case .cancelled: return "Upload cancelled"
        }
    }

    var errorDescription: String? { description }
}

enum UploadFailureScope: Equatable {
    /// This source file or its retained durable job needs attention, but later
    /// files may still upload successfully.
    case file
    /// The transport, server, configuration, or shared upload infrastructure
    /// is unavailable, so probing every file would create a retry storm.
    case collection
}

struct UploadFileFailure: Equatable {
    let message: String
    let scope: UploadFailureScope
}

private final class UploadQueueState {
    var nextIndex = 0
    var firstError: String?
    var clearedPersistedCooldown = false
}

/// Tracks one user/background backlog run across every configured directory.
/// A success resets the consecutive-failure streak, while deferred iCloud
/// files remain pending without being mistaken for a server outage.
final class UploadBatchState {
    static let defaultConsecutiveFailureLimit = 2

    private(set) var firstError: String?
    private(set) var succeededCount = 0
    private(set) var failedCount = 0
    private(set) var deferredCount = 0
    private(set) var consecutiveFailureCount = 0

    let consecutiveFailureLimit: Int

    init(
        consecutiveFailureLimit: Int = UploadBatchState.defaultConsecutiveFailureLimit
    ) {
        precondition(consecutiveFailureLimit > 0)
        self.consecutiveFailureLimit = consecutiveFailureLimit
    }

    var isCircuitOpen: Bool {
        consecutiveFailureCount >= consecutiveFailureLimit
    }

    var processedCount: Int {
        succeededCount + failedCount + deferredCount
    }

    func recordSuccess() {
        succeededCount += 1
        consecutiveFailureCount = 0
    }

    @discardableResult
    func recordFailure(
        _ message: String,
        consumesOutageBudget: Bool = true
    ) -> Bool {
        firstError = firstError ?? message
        failedCount += 1
        if consumesOutageBudget {
            consecutiveFailureCount += 1
        }
        return isCircuitOpen
    }

    func recordDeferred(_ message: String) {
        firstError = firstError ?? message
        deferredCount += 1
    }
}

final class UploadCollectionRetryStore {
    struct Entry: Codable, Equatable {
        let failureLevel: Int
        let nextAttemptAt: Date
    }

    static let shared = UploadCollectionRetryStore()
    static let initialDelay: TimeInterval = 60
    static let maximumDelay: TimeInterval = 60 * 60

    private static let defaultsKey = "UploadCollectionRetryCooldowns.v1"
    private let defaults: UserDefaults
    private let lock = NSLock()

    init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
    }

    func nextAttemptAt(
        server: String,
        sender: String,
        now: Date = Date()
    ) -> Date? {
        lock.withLock {
            guard let entry = load()[collectionKey(server: server, sender: sender)],
                  entry.nextAttemptAt > now else {
                return nil
            }
            return entry.nextAttemptAt
        }
    }

    @discardableResult
    func recordCircuitOpen(
        server: String,
        sender: String,
        now: Date = Date()
    ) -> Entry {
        lock.withLock {
            var entries = load()
            let key = collectionKey(server: server, sender: sender)
            let failureLevel = min((entries[key]?.failureLevel ?? 0) + 1, 7)
            let multiplier = pow(2.0, Double(failureLevel - 1))
            let delay = min(Self.initialDelay * multiplier, Self.maximumDelay)
            let entry = Entry(
                failureLevel: failureLevel,
                nextAttemptAt: now.addingTimeInterval(delay)
            )
            entries[key] = entry
            persist(entries)
            return entry
        }
    }

    func clear(server: String, sender: String) {
        lock.withLock {
            var entries = load()
            let key = collectionKey(server: server, sender: sender)
            guard entries.removeValue(forKey: key) != nil else { return }
            persist(entries)
        }
    }

    private func load() -> [String: Entry] {
        guard let data = defaults.data(forKey: Self.defaultsKey),
              let entries = try? JSONDecoder().decode(
                  [String: Entry].self,
                  from: data
              ) else {
            return [:]
        }
        return entries
    }

    private func persist(_ entries: [String: Entry]) {
        guard let data = try? JSONEncoder().encode(entries) else { return }
        defaults.set(data, forKey: Self.defaultsKey)
    }

    private func collectionKey(server: String, sender: String) -> String {
        "\(server.utf8.count):\(server)\(sender.utf8.count):\(sender)"
    }
}

struct UploadDirectoryInventory {
    let totalCount: Int
    let pendingFiles: [URL]

    var pendingCount: Int { pendingFiles.count }
    var uploadedCount: Int { max(0, totalCount - pendingCount) }

    var summary: UploadDirectorySummary {
        UploadDirectorySummary(
            totalCount: totalCount,
            pendingCount: pendingCount,
            uploadedCount: uploadedCount
        )
    }
}

struct UploadDirectorySummary: Codable, Equatable {
    let totalCount: Int
    let pendingCount: Int
    let uploadedCount: Int
}

enum UploadSummaryCache {
    private static let keyPrefix = "UploadDirectorySummary."

    static func load(
        directoryID: UUID,
        defaults: UserDefaults = .standard
    ) -> UploadDirectorySummary? {
        guard let data = defaults.data(forKey: keyPrefix + directoryID.uuidString) else {
            return nil
        }
        return try? JSONDecoder().decode(UploadDirectorySummary.self, from: data)
    }

    static func store(
        _ summary: UploadDirectorySummary,
        directoryID: UUID,
        defaults: UserDefaults = .standard
    ) {
        guard let data = try? JSONEncoder().encode(summary) else { return }
        defaults.set(data, forKey: keyPrefix + directoryID.uuidString)
    }

    static func remove(
        directoryID: UUID,
        defaults: UserDefaults = .standard
    ) {
        defaults.removeObject(forKey: keyPrefix + directoryID.uuidString)
    }
}

enum UploadJobPriority: Int, Comparable {
    case background
    case foreground
    case immediate

    static func < (lhs: UploadJobPriority, rhs: UploadJobPriority) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

struct UploadFileSnapshot {
    let data: Data
    let record: UploadDoneRecord
    let systemFileNumber: UInt64?
}

final class UploadCancellationToken {
    let id = UUID()

    private let lock = NSLock()
    private var cancelled = false
    private var activeTask: URLSessionTask?

    var isCancelled: Bool {
        lock.lock()
        defer { lock.unlock() }
        return cancelled
    }

    func cancel() {
        lock.lock()
        cancelled = true
        let task = activeTask
        lock.unlock()
        task?.cancel()
    }

    @discardableResult
    func register(_ task: URLSessionTask) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard !cancelled else { return false }
        activeTask = task
        return true
    }

    func clearActiveTask() {
        lock.lock()
        activeTask = nil
        lock.unlock()
    }
}

final class UploadInventoryRefreshCoordinator {
    static let shared = UploadInventoryRefreshCoordinator()

    typealias Completion = (Result<UploadDirectorySummary, Error>) -> Void
    typealias Scanner = (UploadDirectory) -> Result<UploadDirectorySummary, Error>

    private struct CachedResult {
        let result: Result<UploadDirectorySummary, Error>
        let completedAt: Date

        var isSuccess: Bool {
            if case .success = result { return true }
            return false
        }
    }

    private struct InFlightRefresh {
        let directory: UploadDirectory
        var completions: [Completion]
        var forcedFollowUpCompletions: [Completion]
    }

    private let stateQueue = DispatchQueue(
        label: "com.fitness_exporter.uploadInventory.state"
    )
    private let workerQueue = DispatchQueue(
        label: "com.fitness_exporter.uploadInventory.worker",
        qos: .userInitiated,
        attributes: .concurrent
    )
    private let scanner: Scanner
    private var cachedResults: [UUID: CachedResult] = [:]
    private var inFlight: [UUID: InFlightRefresh] = [:]

    convenience init() {
        self.init(scanner: Self.scan)
    }

    init(scanner: @escaping Scanner) {
        self.scanner = scanner
    }

    func refresh(
        directory: UploadDirectory,
        force: Bool = false,
        minimumInterval: TimeInterval = 60,
        completion: @escaping Completion
    ) {
        stateQueue.async { [self] in
            if !force, let cached = cachedResults[directory.id] {
                let allowedAge = cached.isSuccess ? minimumInterval : 5
                if Date().timeIntervalSince(cached.completedAt) < allowedAge {
                    DispatchQueue.main.async {
                        completion(cached.result)
                    }
                    return
                }
            }

            if var existing = inFlight[directory.id] {
                if force {
                    existing.forcedFollowUpCompletions.append(completion)
                } else {
                    existing.completions.append(completion)
                }
                inFlight[directory.id] = existing
                return
            }
            inFlight[directory.id] = InFlightRefresh(
                directory: directory,
                completions: [completion],
                forcedFollowUpCompletions: []
            )
            startScan(directory)
        }
    }

    func remove(directoryID: UUID) {
        stateQueue.async { [self] in
            cachedResults.removeValue(forKey: directoryID)
        }
    }

    private func finish(
        directoryID: UUID,
        result: Result<UploadDirectorySummary, Error>
    ) {
        stateQueue.async { [self] in
            cachedResults[directoryID] = CachedResult(
                result: result,
                completedAt: Date()
            )
            guard let completed = inFlight[directoryID] else { return }
            let completions = completed.completions
            if completed.forcedFollowUpCompletions.isEmpty {
                inFlight.removeValue(forKey: directoryID)
            } else {
                inFlight[directoryID] = InFlightRefresh(
                    directory: completed.directory,
                    completions: completed.forcedFollowUpCompletions,
                    forcedFollowUpCompletions: []
                )
                startScan(completed.directory)
            }
            DispatchQueue.main.async {
                for completion in completions {
                    completion(result)
                }
            }
        }
    }

    private func startScan(_ directory: UploadDirectory) {
        workerQueue.async { [self] in
            finish(directoryID: directory.id, result: scanner(directory))
        }
    }

    private static func scan(
        _ directory: UploadDirectory
    ) -> Result<UploadDirectorySummary, Error> {
        guard let url = UploadHelper.resolveURL(from: directory.bookmark) else {
            return .failure(UploadCoreError.invalidBookmark)
        }
        let hasAccess = url.startAccessingSecurityScopedResource()
        defer {
            if hasAccess {
                url.stopAccessingSecurityScopedResource()
            }
        }
        return Result {
            try UploadHelper.inventory(in: url).summary
        }
    }
}

final class UploadSingleFlightCoordinator {
    static let shared = UploadSingleFlightCoordinator()

    typealias Completion = (String?) -> Void
    typealias Operation = (@escaping Completion) -> Void

    private struct Job {
        let id = UUID()
        let key: String
        var priority: UploadJobPriority
        let operation: Operation
        let shouldRun: () -> Bool
        let cancel: (() -> Void)?
        var completions: [Completion]
    }

    private let stateQueue = DispatchQueue(
        label: "com.fitness_exporter.uploadCoordinator.state"
    )
    private let workerQueue = DispatchQueue(
        label: "com.fitness_exporter.uploadCoordinator.worker",
        qos: .utility
    )
    private var activeJob: Job?
    private var pendingJobs: [Job] = []

    func submit(
        key: String,
        priority: UploadJobPriority = .foreground,
        shouldRun: @escaping () -> Bool = { true },
        cancel: (() -> Void)? = nil,
        operation: @escaping Operation,
        completion: @escaping Completion
    ) {
        stateQueue.async { [self] in
            if activeJob?.key == key {
                let rerun = Job(
                    key: key,
                    priority: priority,
                    operation: operation,
                    shouldRun: shouldRun,
                    cancel: cancel,
                    completions: [completion]
                )
                enqueueOrCoalesce(rerun)
                preemptActiveJobIfNeeded(for: priority)
                return
            }
            if let index = pendingJobs.firstIndex(where: { $0.key == key }) {
                pendingJobs[index].completions.append(completion)
                if priority > pendingJobs[index].priority {
                    var promoted = pendingJobs.remove(at: index)
                    promoted.priority = priority
                    enqueue(promoted)
                }
                return
            }
            let job = Job(
                key: key,
                priority: priority,
                operation: operation,
                shouldRun: shouldRun,
                cancel: cancel,
                completions: [completion]
            )
            enqueue(job)
            preemptActiveJobIfNeeded(for: priority)
            startNextIfNeeded()
        }
    }

    private func enqueueOrCoalesce(_ job: Job) {
        if let index = pendingJobs.firstIndex(where: { $0.key == job.key }) {
            pendingJobs[index].completions.append(contentsOf: job.completions)
            if job.priority > pendingJobs[index].priority {
                var promoted = pendingJobs.remove(at: index)
                promoted.priority = job.priority
                enqueue(promoted)
            }
            return
        }
        enqueue(job)
    }

    private func enqueue(_ job: Job) {
        if let index = pendingJobs.firstIndex(
            where: { $0.priority < job.priority }
        ) {
            pendingJobs.insert(job, at: index)
        } else {
            pendingJobs.append(job)
        }
    }

    private func preemptActiveJobIfNeeded(for priority: UploadJobPriority) {
        guard let activeJob, priority > activeJob.priority else { return }
        activeJob.cancel?()
    }

    private func startNextIfNeeded() {
        guard activeJob == nil else { return }
        while !pendingJobs.isEmpty {
            let next = pendingJobs.removeFirst()
            guard next.shouldRun() else {
                for completion in next.completions {
                    completion("Upload cancelled")
                }
                continue
            }
            activeJob = next
            break
        }
        guard let job = activeJob else { return }
        workerQueue.async { [weak self] in
            job.operation { result in
                self?.finishActiveJob(id: job.id, result: result)
            }
        }
    }

    private func finishActiveJob(id: UUID, result: String?) {
        stateQueue.async { [self] in
            guard let completed = activeJob, completed.id == id else { return }
            activeJob = nil
            for completion in completed.completions {
                completion(result)
            }
            startNextIfNeeded()
        }
    }
}

/// Serializes individual transfers and coalesces duplicate requests for the
/// same immutable file. Immediate recording uploads are inserted ahead of
/// backlog work, but never interrupt a file already on the wire.
final class UploadFileCoordinator {
    static let shared = UploadFileCoordinator()

    typealias Completion = (UploadFileFailure?) -> Void
    typealias Operation = (@escaping Completion) -> Void

    private struct Job {
        let id = UUID()
        let key: String
        let priority: UploadJobPriority
        let operation: Operation
        var completions: [Completion]
    }

    private let stateQueue = DispatchQueue(
        label: "com.fitness_exporter.uploadFileCoordinator.state"
    )
    private let workerQueue = DispatchQueue(
        label: "com.fitness_exporter.uploadFileCoordinator.worker",
        qos: .utility
    )
    private var activeJob: Job?
    private var pendingJobs: [Job] = []

    func submit(
        key: String,
        priority: UploadJobPriority,
        operation: @escaping Operation,
        completion: @escaping Completion
    ) {
        stateQueue.async { [self] in
            if var activeJob, activeJob.key == key {
                activeJob.completions.append(completion)
                self.activeJob = activeJob
                return
            }
            if let index = pendingJobs.firstIndex(where: { $0.key == key }) {
                let existing = pendingJobs.remove(at: index)
                if priority > existing.priority {
                    enqueue(
                        Job(
                            key: key,
                            priority: priority,
                            operation: operation,
                            completions: existing.completions + [completion]
                        )
                    )
                } else {
                    var coalesced = existing
                    coalesced.completions.append(completion)
                    enqueue(coalesced)
                }
                return
            }
            enqueue(
                Job(
                    key: key,
                    priority: priority,
                    operation: operation,
                    completions: [completion]
                )
            )
            startNextIfNeeded()
        }
    }

    private func enqueue(_ job: Job) {
        if let index = pendingJobs.firstIndex(
            where: { $0.priority < job.priority }
        ) {
            pendingJobs.insert(job, at: index)
        } else {
            pendingJobs.append(job)
        }
    }

    private func startNextIfNeeded() {
        guard activeJob == nil, !pendingJobs.isEmpty else { return }
        let job = pendingJobs.removeFirst()
        activeJob = job
        workerQueue.async { [weak self] in
            job.operation { result in
                self?.finish(jobID: job.id, result: result)
            }
        }
    }

    private func finish(jobID: UUID, result: UploadFileFailure?) {
        stateQueue.async { [self] in
            guard let completed = activeJob, completed.id == jobID else { return }
            activeJob = nil
            startNextIfNeeded()
            workerQueue.async {
                for completion in completed.completions {
                    completion(result)
                }
            }
        }
    }
}

enum UploadHelper {
    static func resolveURL(from bookmark: Data) -> URL? {
        var isStale = false
        // iOS-only: resolve standard (non–security-scoped) bookmarks.
        let options: URL.BookmarkResolutionOptions = []
        return try? URL(resolvingBookmarkData: bookmark, options: options, relativeTo: nil, bookmarkDataIsStale: &isStale)
    }

    static func listFiles(
        in base: URL,
        shouldCancel: () -> Bool = { false }
    ) throws -> [URL] {
        let fm = FileManager.default
        do {
            if shouldCancel() { throw UploadCoreError.cancelled }
            let contents = try fm.contentsOfDirectory(
                at: base,
                includingPropertiesForKeys: [.isRegularFileKey, .isDirectoryKey],
                options: [.skipsHiddenFiles]
            )
            let filesOnly = try contents.filter { url in
                if shouldCancel() { throw UploadCoreError.cancelled }
                let values = try url.resourceValues(
                    forKeys: [.isRegularFileKey, .isDirectoryKey])
                return (values.isRegularFile ?? false)
                    && url.lastPathComponent != ".DS_Store"
            }
            return filesOnly.sorted {
                $0.lastPathComponent < $1.lastPathComponent
            }
        } catch {
            if let uploadError = error as? UploadCoreError {
                throw uploadError
            }
            throw UploadCoreError.directoryListFailed(
                "\(base.lastPathComponent): \(error.localizedDescription)"
            )
        }
    }

    /// Build one immutable snapshot for both the UI and uploader. Callers run
    /// this on a background queue; SwiftUI renders only the resulting counts.
    static func inventory(
        in base: URL,
        cancellationToken: UploadCancellationToken? = nil
    ) throws -> UploadDirectoryInventory {
        let shouldCancel = { cancellationToken?.isCancelled == true }
        let files = try listFiles(in: base, shouldCancel: shouldCancel)
        let doneMap = try UploadCompletionIndex.records(
            in: base,
            shouldCancel: shouldCancel
        )
        var pending: [URL] = []
        pending.reserveCapacity(files.count)
        for file in files {
            if shouldCancel() { throw UploadCoreError.cancelled }
            guard let record = doneMap[file.lastPathComponent] else {
                pending.append(file)
                continue
            }
            if !recordMatchesFile(record, fileURL: file) {
                pending.append(file)
            }
        }
        return UploadDirectoryInventory(
            totalCount: files.count,
            pendingFiles: pending
        )
    }

    static func isLocallyAvailable(_ url: URL) -> Bool {
        let keys: Set<URLResourceKey> = [.isUbiquitousItemKey, .ubiquitousItemDownloadingStatusKey]
        guard let vals = try? url.resourceValues(forKeys: keys) else { return false }
        if vals.isUbiquitousItem == true {
            return vals.ubiquitousItemDownloadingStatus == URLUbiquitousItemDownloadingStatus.current
        }
        return true
    }

    @discardableResult
    static func resetCompletionState(in base: URL) throws -> Int {
        try UploadCompletionIndex.reset(in: base)
    }

    @discardableResult
    static func markDone(file: URL, base: URL) throws -> UploadDoneRecord {
        try UploadCompletionIndex.markDone(file: file, in: base)
    }

    @discardableResult
    static func markDone(
        record: UploadDoneRecord,
        base: URL
    ) throws -> UploadDoneRecord {
        try UploadCompletionIndex.markDone(record: record, in: base)
    }

    static func doneRecord(fileName: String, base: URL) throws -> UploadDoneRecord? {
        try UploadCompletionIndex.record(fileName: fileName, in: base)
    }

    static func readStableFile(_ fileURL: URL) throws -> UploadFileSnapshot {
        let before = try fileIdentity(fileURL)
        let data: Data
        do {
            data = try Data(contentsOf: fileURL)
        } catch {
            throw UploadCoreError.fileReadFailed(
                "\(fileURL.lastPathComponent): \(error.localizedDescription)"
            )
        }
        let after = try fileIdentity(fileURL)
        guard identitiesMatch(before, after),
              Int64(data.count) == before.record.fileSize else {
            throw UploadCoreError.fileReadFailed(
                "\(fileURL.lastPathComponent): file changed while being read"
            )
        }
        return UploadFileSnapshot(
            data: data,
            record: before.record,
            systemFileNumber: before.systemFileNumber
        )
    }

    static func verifyUnchanged(
        _ snapshot: UploadFileSnapshot,
        fileURL: URL
    ) throws {
        let expected = FileIdentity(
            record: snapshot.record,
            systemFileNumber: snapshot.systemFileNumber
        )
        let current = try fileIdentity(fileURL)
        guard identitiesMatch(expected, current) else {
            throw UploadCoreError.fileReadFailed(
                "\(fileURL.lastPathComponent): file changed during upload"
            )
        }
    }

    /// Returns true when the on-disk file matches the recorded metadata.
    static func recordMatchesFile(_ record: UploadDoneRecord, fileURL: URL) -> Bool {
        guard let attributes = try? FileManager.default.attributesOfItem(
            atPath: fileURL.path
        ), attributes[.type] as? FileAttributeType == .typeRegular,
           let size = (attributes[.size] as? NSNumber)?.int64Value,
           size == record.fileSize,
           let curDate = attributes[.modificationDate] as? Date else {
            return false
        }
        // Strict match: require exact modification timestamp equality.
        return curDate == record.lastModifiedAt
    }

    static func sourceIdentityMatchesFile(
        record: UploadDoneRecord,
        systemFileNumber: UInt64?,
        fileURL: URL
    ) -> Bool {
        guard let current = try? fileIdentity(fileURL) else { return false }
        return identitiesMatch(
            FileIdentity(
                record: record,
                systemFileNumber: systemFileNumber
            ),
            current
        )
    }

    private struct FileIdentity {
        let record: UploadDoneRecord
        let systemFileNumber: UInt64?
    }

    private static func fileIdentity(_ fileURL: URL) throws -> FileIdentity {
        let attributes: [FileAttributeKey: Any]
        do {
            attributes = try FileManager.default.attributesOfItem(
                atPath: fileURL.path
            )
        } catch {
            throw UploadCoreError.fileReadFailed(
                "\(fileURL.lastPathComponent): \(error.localizedDescription)"
            )
        }
        guard attributes[.type] as? FileAttributeType == .typeRegular,
              let fileSize = (attributes[.size] as? NSNumber)?.int64Value,
              let modifiedAt = attributes[.modificationDate] as? Date else {
            throw UploadCoreError.fileReadFailed(
                "\(fileURL.lastPathComponent): not a regular file or missing metadata"
            )
        }
        return FileIdentity(
            record: UploadDoneRecord(
                fileName: fileURL.lastPathComponent,
                fileSize: fileSize,
                lastModifiedAt: modifiedAt
            ),
            systemFileNumber: (
                attributes[.systemFileNumber] as? NSNumber
            )?.uint64Value
        )
    }

    private static func identitiesMatch(
        _ lhs: FileIdentity,
        _ rhs: FileIdentity
    ) -> Bool {
        guard lhs.record == rhs.record else { return false }
        switch (lhs.systemFileNumber, rhs.systemFileNumber) {
        case (nil, nil):
            return true
        case let (lhs?, rhs?):
            return lhs == rhs
        default:
            return false
        }
    }
}

enum DirectoryUploader {
    struct ConfigError: Error {}

    static func getServerAndSender() -> (server: String, sender: String)? {
        let server = UserDefaults.standard.string(forKey: UserDefaultsKeys.SERVER_URL) ?? ""
        let sender = UserDefaults.standard.string(forKey: UserDefaultsKeys.SENDER) ?? ""
        guard !server.isEmpty, !sender.isEmpty else { return nil }
        return (server, sender)
    }

    static func uploadAll(
        dir: UploadDirectory,
        server: String,
        sender: String,
        stopOnError: Bool = true,
        priority: UploadJobPriority = .foreground,
        cancellationToken: UploadCancellationToken? = nil,
        completion: @escaping (String?) -> Void
    ) {
        var key =
            "directory:\(dir.id.uuidString):\(stopOnError):\(server):\(sender)"
        if let cancellationToken {
            key += ":request:\(cancellationToken.id.uuidString)"
        }
        UploadSingleFlightCoordinator.shared.submit(
            key: key,
            priority: priority,
            shouldRun: { cancellationToken?.isCancelled != true },
            cancel: cancellationToken.map { token in { token.cancel() } },
            operation: { finish in
                uploadAllNow(
                    dir: dir,
                    server: server,
                    sender: sender,
                    stopOnError: stopOnError,
                    priority: priority,
                    cancellationToken: cancellationToken,
                    completion: finish
                )
            },
            completion: completion
        )
    }

    private static func uploadAllNow(
        dir: UploadDirectory,
        server: String,
        sender: String,
        stopOnError: Bool,
        priority: UploadJobPriority,
        cancellationToken: UploadCancellationToken?,
        batchState: UploadBatchState? = nil,
        completion: @escaping (String?) -> Void
    ) {
        guard cancellationToken?.isCancelled != true else {
            return completion("Upload cancelled")
        }
        if priority == .background, batchState == nil,
           let nextAttemptAt = UploadCollectionRetryStore.shared.nextAttemptAt(
               server: server,
               sender: sender
           ) {
            let seconds = max(
                1,
                Int(nextAttemptAt.timeIntervalSinceNow.rounded(.up))
            )
            let message =
                "Backlog upload is cooling down after a server or transport failure; "
                + "retry after approximately \(seconds) second(s)."
            CustomLogger.log("[Upload][Batch][Deferred] \(message)")
            return completion(message)
        }
        guard let baseURL = UploadHelper.resolveURL(from: dir.bookmark) else {
            return completion(UploadCoreError.invalidBookmark.description)
        }
        let hasAccess = baseURL.startAccessingSecurityScopedResource()
        let completionLock = NSLock()
        var didFinish = false
        func finish(_ status: String?) {
            completionLock.lock()
            guard !didFinish else {
                completionLock.unlock()
                return
            }
            didFinish = true
            completionLock.unlock()
            completion(status)
        }

        let inventory: UploadDirectoryInventory
        do {
            inventory = try UploadHelper.inventory(
                in: baseURL,
                cancellationToken: cancellationToken
            )
        } catch {
            if hasAccess {
                baseURL.stopAccessingSecurityScopedResource()
            }
            CustomLogger.log("[Upload][Error] \(error.localizedDescription)")
            return finish(error.localizedDescription)
        }
        if hasAccess {
            baseURL.stopAccessingSecurityScopedResource()
        }
        guard !inventory.pendingFiles.isEmpty else { return finish(nil) }

        self.uploadQueue(
            inventory.pendingFiles,
            baseURL: baseURL,
            baseBookmark: dir.bookmark,
            dirName: dir.name,
            server: server,
            sender: sender,
            stopOnError: stopOnError,
            priority: priority,
            cancellationToken: cancellationToken,
            batchState: batchState ?? UploadBatchState(),
            completion: finish
        )
    }

    private static func uploadQueue(
        _ files: [URL],
        baseURL: URL,
        baseBookmark: Data,
        dirName: String,
        server: String,
        sender: String,
        stopOnError: Bool,
        priority: UploadJobPriority,
        cancellationToken: UploadCancellationToken?,
        batchState: UploadBatchState,
        completion: @escaping (String?) -> Void
    ) {
        let state = UploadQueueState()
        let startedProcessedCount = batchState.processedCount
        let startedSucceededCount = batchState.succeededCount
        let startedFailedCount = batchState.failedCount
        let startedDeferredCount = batchState.deferredCount
        CustomLogger.log(
            "[Upload][Batch][Start] dir=\(dirName) pending=\(files.count)"
        )

        @Sendable func finish(_ error: String?) {
            let processed = batchState.processedCount - startedProcessedCount
            CustomLogger.log(
                "[Upload][Batch][Finish] dir=\(dirName) processed=\(processed)/\(files.count) "
                    + "succeeded=\(batchState.succeededCount - startedSucceededCount) "
                    + "failed=\(batchState.failedCount - startedFailedCount) "
                    + "deferred=\(batchState.deferredCount - startedDeferredCount)"
            )
            completion(error)
        }

        func processNext() {
            guard cancellationToken?.isCancelled != true else {
                finish("Upload cancelled")
                return
            }
            guard !batchState.isCircuitOpen else {
                finish(batchState.firstError)
                return
            }
            while state.nextIndex < files.count {
                let file = files[state.nextIndex]
                state.nextIndex += 1

                // In background, do not hydrate iCloud files while the device
                // may be locked. Continue iteratively so a large backlog
                // cannot overflow the call stack.
                if !stopOnError && !UploadHelper.isLocallyAvailable(file) {
                    let message =
                        "Deferred \(file.lastPathComponent): iCloud file is not locally available"
                    state.firstError = state.firstError ?? message
                    batchState.recordDeferred(message)
                    continue
                }

                uploadFile(
                    file: file,
                    baseURL: baseURL,
                    baseBookmark: baseBookmark,
                    dirName: dirName,
                    server: server,
                    sender: sender,
                    priority: priority,
                    cancellationToken: cancellationToken,
                    logPerFile: false
                ) { failure in
                    if let failure {
                        if cancellationToken?.isCancelled == true {
                            finish("Upload cancelled")
                            return
                        }
                        let contextualError =
                            "\(file.lastPathComponent): \(failure.message)"
                        CustomLogger.log(
                            "[Upload][Batch][Error] dir=\(dirName) "
                                + "file=\(file.lastPathComponent) err=\(failure.message)"
                        )
                        let circuitOpened = batchState.recordFailure(
                            contextualError,
                            consumesOutageBudget: failure.scope == .collection
                        )
                        if stopOnError {
                            finish(failure.message)
                            return
                        }
                        state.firstError = state.firstError ?? failure.message
                        if circuitOpened {
                            let remaining = files.count - state.nextIndex
                            let cooldown = UploadCollectionRetryStore.shared
                                .recordCircuitOpen(server: server, sender: sender)
                            let seconds = max(
                                1,
                                Int(cooldown.nextAttemptAt.timeIntervalSinceNow.rounded(.up))
                            )
                            let message =
                                "Paused backlog upload after "
                                + "\(batchState.consecutiveFailureCount) consecutive failures; "
                                + "\(remaining) file(s) remain in \(dirName). "
                                + "Retry after approximately \(seconds) second(s). "
                                + "Last error for \(file.lastPathComponent): \(failure.message)"
                            CustomLogger.log("[Upload][Batch][Paused] \(message)")
                            finish(message)
                            return
                        }
                        DispatchQueue.global(qos: .utility).async {
                            processNext()
                        }
                        return
                    }
                    batchState.recordSuccess()
                    if !state.clearedPersistedCooldown {
                        state.clearedPersistedCooldown = true
                        UploadCollectionRetryStore.shared.clear(
                            server: server,
                            sender: sender
                        )
                    }
                    if batchState.succeededCount.isMultiple(of: 100) {
                        CustomLogger.log(
                            "[Upload][Batch][Progress] succeeded=\(batchState.succeededCount) "
                                + "failed=\(batchState.failedCount) "
                                + "deferred=\(batchState.deferredCount)"
                        )
                    }
                    DispatchQueue.global(qos: .utility).async {
                        processNext()
                    }
                }
                return
            }

            finish(state.firstError)
        }

        processNext()
    }

    /// Upload one immutable file snapshot. Every caller uses this path so an
    /// immediate recording upload and a backlog scan cannot send the same file
    /// concurrently or mark a replacement file as already accepted.
    static func uploadFile(
        file: URL,
        baseURL: URL,
        baseBookmark: Data,
        dirName: String,
        server: String,
        sender: String,
        priority: UploadJobPriority,
        cancellationToken: UploadCancellationToken? = nil,
        expectedRecord: UploadDoneRecord? = nil,
        immediateRelativePath: String? = nil,
        logPerFile: Bool = true,
        completion: @escaping (UploadFileFailure?) -> Void
    ) {
        // The coordinator key must describe the complete server-visible
        // destination. Length-prefixing keeps user-provided values from
        // colliding when they contain a separator.
        let key = [
            server,
            sender,
            dirName,
            baseURL.standardizedFileURL.path,
            file.standardizedFileURL.path,
        ].map { "\($0.utf8.count):\($0)" }.joined()
        UploadFileCoordinator.shared.submit(
            key: key,
            priority: priority,
            operation: { finish in
                guard cancellationToken?.isCancelled != true else {
                    return finish(
                        UploadFileFailure(
                            message: UploadCoreError.cancelled.description,
                            scope: .collection
                        ))
                }

                let hasAccess = baseURL.startAccessingSecurityScopedResource()
                let finishLock = NSLock()
                var didFinish = false
                var releasedAccess = false
                func releaseAccess() {
                    finishLock.lock()
                    guard !releasedAccess else {
                        finishLock.unlock()
                        return
                    }
                    releasedAccess = true
                    finishLock.unlock()
                    if hasAccess {
                        baseURL.stopAccessingSecurityScopedResource()
                    }
                }
                func finishOnce(_ failure: UploadFileFailure?) {
                    finishLock.lock()
                    guard !didFinish else {
                        finishLock.unlock()
                        return
                    }
                    didFinish = true
                    finishLock.unlock()
                    releaseAccess()
                    finish(failure)
                }

                do {
                    if let done = try UploadHelper.doneRecord(
                        fileName: file.lastPathComponent,
                        base: baseURL
                    ), UploadHelper.recordMatchesFile(done, fileURL: file) {
                        return finishOnce(nil)
                    }

                    let snapshot = try UploadHelper.readStableFile(file)
                    if let expectedRecord, snapshot.record != expectedRecord {
                        throw UploadCoreError.fileReadFailed(
                            "\(file.lastPathComponent): queued file identity changed"
                        )
                    }
                    let byteCount = snapshot.data.count
                    if logPerFile {
                        CustomLogger.log(
                            "[Upload][Start] dir=\(dirName) file=\(file.lastPathComponent) "
                                + "bytes=\(byteCount)"
                        )
                    }
                    try BackgroundFileUploadManager.shared.uploadFile(
                        deduplicationKey: key,
                        server: server,
                        sender: sender,
                        directoryName: dirName,
                        baseURL: baseURL,
                        baseBookmark: baseBookmark,
                        sourceFileURL: file,
                        fileBytes: snapshot.data,
                        record: snapshot.record,
                        sourceSystemFileNumber: snapshot.systemFileNumber,
                        immediateRelativePath: immediateRelativePath,
                        cancellationToken: cancellationToken,
                        validateSource: {
                            try UploadHelper.verifyUnchanged(
                                snapshot,
                                fileURL: file
                            )
                        },
                        completion: { failure in
                            if let failure {
                                if logPerFile {
                                    CustomLogger.log(
                                        "[Upload][Error] dir=\(dirName) "
                                            + "file=\(file.lastPathComponent) "
                                            + "err=\(failure.message)"
                                    )
                                }
                                return finishOnce(failure)
                            }
                            if logPerFile {
                                CustomLogger.log(
                                    "[Upload][Success] dir=\(dirName) "
                                        + "file=\(file.lastPathComponent) "
                                        + "bytes=\(byteCount)"
                                )
                            }
                            finishOnce(nil)
                        }
                    )
                    releaseAccess()
                } catch {
                    releaseAccess()
                    let message = error.localizedDescription
                    if logPerFile {
                        CustomLogger.log(
                            "[Upload][Error] dir=\(dirName) "
                                + "file=\(file.lastPathComponent) err=\(message)"
                        )
                    }
                    finishOnce(uploadFailure(for: error))
                }
            },
            completion: completion
        )
    }

    static func uploadFailure(for error: Error) -> UploadFileFailure {
        let scope: UploadFailureScope
        switch error {
        case UploadCoreError.fileReadFailed(_),
             UploadCoreError.completionState(_),
             BackgroundUploadError.invalidJob(_),
             BackgroundUploadError.bodyEncoding(_),
             BackgroundUploadError.transfer(_),
             BackgroundUploadError.acceptedFinalization(_):
            scope = .file
        case UploadCoreError.invalidBookmark,
             UploadCoreError.directoryListFailed(_),
             UploadCoreError.network(_),
             UploadCoreError.cancelled,
             BackgroundUploadError.invalidConfiguration(_),
             BackgroundUploadError.persistence(_):
            scope = .collection
        default:
            // Unknown local errors are isolated to the current file. A future
            // file must not be starved unless the transport explicitly reports
            // a collection-wide failure.
            scope = .file
        }
        return UploadFileFailure(
            message: error.localizedDescription,
            scope: scope
        )
    }
}

// MARK: - Directories storage and types

struct UploadDirectory: Identifiable, Codable, Equatable {
    let id: UUID
    var name: String
    var bookmark: Data

    static func == (lhs: UploadDirectory, rhs: UploadDirectory) -> Bool {
        lhs.id == rhs.id
    }
}

final class UploadDirectoriesStore: ObservableObject {
    @Published var dirs: [UploadDirectory] = [] {
        didSet { persist() }
    }

    private static let defaultsKey = "UploadDirectories"

    init() {
        load()
    }

    static func loadPersisted(
        defaults: UserDefaults = .standard
    ) throws -> [UploadDirectory] {
        guard let data = defaults.data(forKey: defaultsKey) else { return [] }
        return try JSONDecoder().decode([UploadDirectory].self, from: data)
    }

    func add(url: URL) {
        // iOS-only: create a standard bookmark. We still try to access the
        // resource during creation to stabilize file provider URLs.
        let hadAccess = url.startAccessingSecurityScopedResource()
        defer { if hadAccess { url.stopAccessingSecurityScopedResource() } }

        do {
            let options: URL.BookmarkCreationOptions = []
            let bookmark = try url.bookmarkData(options: options, includingResourceValuesForKeys: nil, relativeTo: nil)
            let name = (try? url.resourceValues(forKeys: [.localizedNameKey]).localizedName)
                ?? url.lastPathComponent
            let entry = UploadDirectory(id: UUID(), name: name, bookmark: bookmark)
            if !dirs.contains(where: { $0.bookmark == entry.bookmark }) {
                dirs.append(entry)
                ImmediateUploadService.shared.resume()
            }
        } catch {
            CustomLogger.log("[Upload] Failed to create bookmark: \(error)")
        }
    }

    func remove(_ dir: UploadDirectory) {
        dirs.removeAll { $0.id == dir.id }
        UploadSummaryCache.remove(directoryID: dir.id)
        UploadInventoryRefreshCoordinator.shared.remove(directoryID: dir.id)
    }

    private func persist() {
        do {
            let data = try JSONEncoder().encode(dirs)
            UserDefaults.standard.set(data, forKey: Self.defaultsKey)
        } catch {
            CustomLogger.log("[Upload] Persist failed: \(error)")
        }
    }

    private func load() {
        do {
            dirs = try Self.loadPersisted()
        } catch {
            CustomLogger.log("[Upload] Load failed: \(error)")
        }
    }
}

extension DirectoryUploader {
    static func uploadAllDirectories(
        _ dirs: [UploadDirectory],
        server: String,
        sender: String,
        stopOnError: Bool = true,
        priority: UploadJobPriority = .foreground,
        cancellationToken: UploadCancellationToken? = nil,
        completion: @escaping (String?) -> Void
    ) {
        let ids = dirs.map { $0.id.uuidString }.sorted().joined(separator: ",")
        var key = "directories:\(ids):\(stopOnError):\(server):\(sender)"
        if let cancellationToken {
            key += ":request:\(cancellationToken.id.uuidString)"
        }
        UploadSingleFlightCoordinator.shared.submit(
            key: key,
            priority: priority,
            shouldRun: { cancellationToken?.isCancelled != true },
            cancel: cancellationToken.map { token in { token.cancel() } },
            operation: { finish in
                uploadAllDirectoriesNow(
                    dirs,
                    server: server,
                    sender: sender,
                    stopOnError: stopOnError,
                    priority: priority,
                    cancellationToken: cancellationToken,
                    completion: finish
                )
            },
            completion: completion
        )
    }

    private static func uploadAllDirectoriesNow(
        _ dirs: [UploadDirectory],
        server: String,
        sender: String,
        stopOnError: Bool,
        priority: UploadJobPriority,
        cancellationToken: UploadCancellationToken?,
        completion: @escaping (String?) -> Void
    ) {
        let state = UploadQueueState()
        let batchState = UploadBatchState()
        if priority == .background,
           let nextAttemptAt = UploadCollectionRetryStore.shared.nextAttemptAt(
               server: server,
               sender: sender
           ) {
            let seconds = max(
                1,
                Int(nextAttemptAt.timeIntervalSinceNow.rounded(.up))
            )
            let message =
                "Backlog upload is cooling down after a server or transport failure; "
                + "retry after approximately \(seconds) second(s)."
            CustomLogger.log("[Upload][Batch][Deferred] \(message)")
            return completion(message)
        }
        func loop(_ index: Int) {
            guard cancellationToken?.isCancelled != true else {
                return completion("Upload cancelled")
            }
            if index >= dirs.count || batchState.isCircuitOpen {
                return completion(state.firstError ?? batchState.firstError)
            }
            uploadAllNow(
                dir: dirs[index],
                server: server,
                sender: sender,
                stopOnError: stopOnError,
                priority: priority,
                cancellationToken: cancellationToken,
                batchState: batchState
            ) { error in
                if let error {
                    if stopOnError { return completion(error) }
                    state.firstError = state.firstError ?? error
                    if batchState.isCircuitOpen {
                        return completion(error)
                    }
                }
                loop(index + 1)
            }
        }
        loop(0)
    }

    static func uploadAllFromStore(
        stopOnError: Bool = true,
        priority: UploadJobPriority = .foreground,
        cancellationToken: UploadCancellationToken? = nil,
        completion: @escaping (String?) -> Void
    ) {
        guard let cfg = getServerAndSender() else {
            return completion("Upload server or sender is not configured")
        }
        let dirs: [UploadDirectory]
        do {
            dirs = try UploadDirectoriesStore.loadPersisted()
        } catch {
            return completion(
                "Failed to load upload directories: \(error.localizedDescription)"
            )
        }
        guard !dirs.isEmpty else { return completion(nil) }
        uploadAllDirectories(
            dirs,
            server: cfg.server,
            sender: cfg.sender,
            stopOnError: stopOnError,
            priority: priority,
            cancellationToken: cancellationToken,
            completion: completion
        )
    }
}
