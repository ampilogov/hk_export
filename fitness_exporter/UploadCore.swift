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

    var description: String {
        switch self {
        case .invalidBookmark: return "Invalid directory bookmark"
        case .directoryListFailed(let message):
            return "Failed to list directory: \(message)"
        case .fileReadFailed(let name): return "Failed to read \(name)"
        case .completionState(let message):
            return "Failed to update upload completion state: \(message)"
        case .network(let msg): return msg
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

final class UploadSingleFlightCoordinator {
    static let shared = UploadSingleFlightCoordinator()

    typealias Completion = (String?) -> Void
    typealias Operation = (@escaping Completion) -> Void

    private struct Job {
        let id = UUID()
        let key: String
        let operation: Operation
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
        operation: @escaping Operation,
        completion: @escaping Completion
    ) {
        stateQueue.async { [self] in
            if activeJob?.key == key {
                activeJob?.completions.append(completion)
                return
            }
            if let index = pendingJobs.firstIndex(where: { $0.key == key }) {
                pendingJobs[index].completions.append(completion)
                return
            }
            pendingJobs.append(
                Job(key: key, operation: operation, completions: [completion])
            )
            startNextIfNeeded()
        }
    }

    private func startNextIfNeeded() {
        guard activeJob == nil, !pendingJobs.isEmpty else { return }
        activeJob = pendingJobs.removeFirst()
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

    static func listFiles(in base: URL) throws -> [URL] {
        let fm = FileManager.default
        do {
            let contents = try fm.contentsOfDirectory(
                at: base,
                includingPropertiesForKeys: [.isRegularFileKey, .isDirectoryKey],
                options: [.skipsHiddenFiles]
            )
            let filesOnly = try contents.filter { url in
                let values = try url.resourceValues(
                    forKeys: [.isRegularFileKey, .isDirectoryKey])
                return (values.isRegularFile ?? false)
                    && url.lastPathComponent != ".DS_Store"
            }
            return filesOnly.sorted {
                $0.lastPathComponent < $1.lastPathComponent
            }
        } catch {
            throw UploadCoreError.directoryListFailed(
                "\(base.lastPathComponent): \(error.localizedDescription)"
            )
        }
    }

    /// Build one immutable snapshot for both the UI and uploader. Callers run
    /// this on a background queue; SwiftUI renders only the resulting counts.
    static func inventory(in base: URL) throws -> UploadDirectoryInventory {
        let files = try listFiles(in: base)
        let doneMap = try UploadCompletionIndex.open(in: base).records()
        let pending = files.filter { file in
            guard let record = doneMap[file.lastPathComponent] else { return true }
            return !recordMatchesFile(record, fileURL: file)
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
        try UploadCompletionIndex.open(in: base).markDone(file: file)
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
        completion: @escaping (String?) -> Void
    ) {
        let key =
            "directory:\(dir.id.uuidString):\(stopOnError):\(server):\(sender)"
        UploadSingleFlightCoordinator.shared.submit(
            key: key,
            operation: { finish in
                uploadAllNow(
                    dir: dir,
                    server: server,
                    sender: sender,
                    stopOnError: stopOnError,
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
        completion: @escaping (String?) -> Void
    ) {
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
        let completionIndex: UploadCompletionIndex
        do {
            inventory = try UploadHelper.inventory(in: baseURL)
            completionIndex = try UploadCompletionIndex.open(in: baseURL)
        } catch {
            CustomLogger.log("[Upload][Error] \(error.localizedDescription)")
            return finish(error.localizedDescription)
        }
        guard !inventory.pendingFiles.isEmpty else { return finish(nil) }

        let session = ServerSession.getSession(server: server)
        self.uploadQueue(
            inventory.pendingFiles,
            dirName: dir.name,
            session: session,
            sender: sender,
            stopOnError: stopOnError,
            completionIndex: completionIndex,
            completion: finish
        )
    }

    private static func uploadQueue(
        _ files: [URL],
        dirName: String,
        session: ServerSession,
        sender: String,
        stopOnError: Bool,
        completionIndex: UploadCompletionIndex,
        completion: @escaping (String?) -> Void
    ) {
        let state = UploadQueueState()

        func processNext() {
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
                    sender: sender
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
                        try completionIndex.markDone(file: file)
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
        completion: @escaping (String?) -> Void
    ) {
        let ids = dirs.map { $0.id.uuidString }.sorted().joined(separator: ",")
        let key = "directories:\(ids):\(stopOnError):\(server):\(sender)"
        UploadSingleFlightCoordinator.shared.submit(
            key: key,
            operation: { finish in
                uploadAllDirectoriesNow(
                    dirs,
                    server: server,
                    sender: sender,
                    stopOnError: stopOnError,
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
        completion: @escaping (String?) -> Void
    ) {
        let state = UploadQueueState()
        func loop(_ index: Int) {
            if index >= dirs.count { return completion(state.firstError) }
            uploadAllNow(
                dir: dirs[index],
                server: server,
                sender: sender,
                stopOnError: stopOnError
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

    static func uploadAllFromStore(stopOnError: Bool = true, completion: @escaping (String?) -> Void) {
        guard let cfg = getServerAndSender() else {
            return completion("Upload server or sender is not configured")
        }
        let store = UploadDirectoriesStore()
        let dirs = store.dirs
        guard !dirs.isEmpty else { return completion(nil) }
        uploadAllDirectories(dirs, server: cfg.server, sender: cfg.sender, stopOnError: stopOnError, completion: completion)
    }
}
