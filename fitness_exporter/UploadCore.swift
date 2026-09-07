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

private final class UploadQueueState {
    var nextIndex = 0
    var firstError: String?
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

    static func < (lhs: UploadJobPriority, rhs: UploadJobPriority) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
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

    /// Returns true when the on-disk file matches the recorded metadata.
    static func recordMatchesFile(_ record: UploadDoneRecord, fileURL: URL) -> Bool {
        let keys: Set<URLResourceKey> = [.fileSizeKey, .contentModificationDateKey]
        guard let vals = try? fileURL.resourceValues(forKeys: keys) else { return false }
        let sizeOK = (vals.fileSize.map { Int64($0) } ?? -1) == record.fileSize
        guard sizeOK else { return false }
        guard let curDate = vals.contentModificationDate else { return false }
        // Strict match: require exact modification timestamp equality.
        return curDate == record.lastModifiedAt
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
        cancellationToken: UploadCancellationToken?,
        completion: @escaping (String?) -> Void
    ) {
        guard cancellationToken?.isCancelled != true else {
            return completion("Upload cancelled")
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
            if hasAccess {
                baseURL.stopAccessingSecurityScopedResource()
            }
            completion(status)
        }

        let inventory: UploadDirectoryInventory
        do {
            inventory = try UploadHelper.inventory(
                in: baseURL,
                cancellationToken: cancellationToken
            )
        } catch {
            CustomLogger.log("[Upload][Error] \(error.localizedDescription)")
            return finish(error.localizedDescription)
        }
        guard !inventory.pendingFiles.isEmpty else { return finish(nil) }

        let session = ServerSession.getSession(server: server)
        self.uploadQueue(
            inventory.pendingFiles,
            baseURL: baseURL,
            dirName: dir.name,
            session: session,
            sender: sender,
            stopOnError: stopOnError,
            cancellationToken: cancellationToken,
            completion: finish
        )
    }

    private static func uploadQueue(
        _ files: [URL],
        baseURL: URL,
        dirName: String,
        session: ServerSession,
        sender: String,
        stopOnError: Bool,
        cancellationToken: UploadCancellationToken?,
        completion: @escaping (String?) -> Void
    ) {
        let state = UploadQueueState()

        func processNext() {
            guard cancellationToken?.isCancelled != true else {
                completion("Upload cancelled")
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
                    CustomLogger.log(
                        "[Upload][Skip] dir=\(dirName) file=\(file.lastPathComponent) reason=iCloud file not locally available"
                    )
                    continue
                }

                guard let data = try? Data(contentsOf: file) else {
                    let message = UploadCoreError.fileReadFailed(
                        file.lastPathComponent).description
                    CustomLogger.log(
                        "[Upload][Error] dir=\(dirName) file=\(file.lastPathComponent) err=\(message)"
                    )
                    if stopOnError {
                        completion(message)
                        return
                    }
                    state.firstError = state.firstError ?? message
                    continue
                }

                let size = data.count
                CustomLogger.log(
                    "[Upload][Start] dir=\(dirName) file=\(file.lastPathComponent) bytes=\(size)"
                )
                session.uploadFile(
                    dirName: dirName,
                    fileName: file.lastPathComponent,
                    fileBytes: data,
                    fullPath: file.path,
                    sender: sender,
                    cancellationToken: cancellationToken
                ) { error in
                    if let error {
                        CustomLogger.log(
                            "[Upload][Error] dir=\(dirName) file=\(file.lastPathComponent) err=\(error)"
                        )
                        if stopOnError {
                            completion(error)
                            return
                        }
                        state.firstError = state.firstError ?? error
                        DispatchQueue.global(qos: .utility).async {
                            processNext()
                        }
                        return
                    }

                    CustomLogger.log(
                        "[Upload][Success] dir=\(dirName) file=\(file.lastPathComponent) bytes=\(size)"
                    )
                    do {
                        try UploadHelper.markDone(file: file, base: baseURL)
                    } catch {
                        let message = error.localizedDescription
                        CustomLogger.log(
                            "[Upload][Error] dir=\(dirName) file=\(file.lastPathComponent) err=\(message)"
                        )
                        if stopOnError {
                            completion(message)
                            return
                        }
                        state.firstError = state.firstError ?? message
                        DispatchQueue.global(qos: .utility).async {
                            processNext()
                        }
                        return
                    }
                    DispatchQueue.global(qos: .utility).async {
                        processNext()
                    }
                }
                return
            }

            completion(state.firstError)
        }

        processNext()
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

    private let defaultsKey = "UploadDirectories"

    init() {
        load()
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
            UserDefaults.standard.set(data, forKey: defaultsKey)
        } catch {
            CustomLogger.log("[Upload] Persist failed: \(error)")
        }
    }

    private func load() {
        if let data = UserDefaults.standard.data(forKey: defaultsKey) {
            do {
                let decoded = try JSONDecoder().decode([UploadDirectory].self, from: data)
                self.dirs = decoded
            } catch {
                CustomLogger.log("[Upload] Load failed: \(error)")
            }
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
        cancellationToken: UploadCancellationToken?,
        completion: @escaping (String?) -> Void
    ) {
        let state = UploadQueueState()
        func loop(_ index: Int) {
            guard cancellationToken?.isCancelled != true else {
                return completion("Upload cancelled")
            }
            if index >= dirs.count { return completion(state.firstError) }
            uploadAllNow(
                dir: dirs[index],
                server: server,
                sender: sender,
                stopOnError: stopOnError,
                cancellationToken: cancellationToken
            ) { error in
                if let error {
                    if stopOnError { return completion(error) }
                    state.firstError = state.firstError ?? error
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
        let store = UploadDirectoriesStore()
        let dirs = store.dirs
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
