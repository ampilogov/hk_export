import Foundation

struct UploadDoneRecord: Codable {
    let fileName: String
    let fileSize: Int64
    let lastModifiedAt: Date
}

enum UploadCoreError: Error, CustomStringConvertible {
    case invalidBookmark
    case directoryListFailed
    case fileReadFailed(String)
    case network(String)

    var description: String {
        switch self {
        case .invalidBookmark: return "Invalid directory bookmark"
        case .directoryListFailed: return "Failed to list directory"
        case .fileReadFailed(let name): return "Failed to read \(name)"
        case .network(let msg): return msg
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

    static func listFiles(in base: URL) -> [URL] {
        let fm = FileManager.default
        guard let contents = try? fm.contentsOfDirectory(at: base, includingPropertiesForKeys: [.isRegularFileKey, .isDirectoryKey], options: [.skipsHiddenFiles]) else { return [] }
        let filesOnly = (try? contents.filter { u in
            let vals = try u.resourceValues(forKeys: [.isRegularFileKey, .isDirectoryKey])
            return (vals.isRegularFile ?? false) && u.lastPathComponent != ".DS_Store"
        }) ?? []
        return filesOnly.sorted { $0.lastPathComponent < $1.lastPathComponent }
    }

    static func isLocallyAvailable(_ url: URL) -> Bool {
        let keys: Set<URLResourceKey> = [.isUbiquitousItemKey, .ubiquitousItemDownloadingStatusKey]
        guard let vals = try? url.resourceValues(forKeys: keys) else { return false }
        if vals.isUbiquitousItem == true {
            return vals.ubiquitousItemDownloadingStatus == URLUbiquitousItemDownloadingStatus.current
        }
        return true
    }

    static func loadDoneMap(for base: URL) -> [String: UploadDoneRecord] {
        var map: [String: UploadDoneRecord] = [:]
        let fm = FileManager.default
        let doneDir = base.appendingPathComponent(".done", isDirectory: true)

        // Ensure the directory exists locally. If it doesn't, there's nothing to load.
        var isDir: ObjCBool = false
        if !fm.fileExists(atPath: doneDir.path, isDirectory: &isDir) || !isDir.boolValue {
            CustomLogger.log("Empty dir!!!")
            return map
        }

        // For iCloud/File Provider-backed folders, coordinate the read to allow listing.
        // Avoid skipping hidden entries inside .done.
        let coordinator = NSFileCoordinator()
        var coordError: NSError?
        var entries: [URL] = []
        coordinator.coordinate(readingItemAt: doneDir, options: [], error: &coordError) { url in
            if let listed = try? fm.contentsOfDirectory(at: url, includingPropertiesForKeys: [.isRegularFileKey, .isUbiquitousItemKey, .ubiquitousItemDownloadingStatusKey], options: []) {
                entries = listed
            }
        }

        // Fallback to a direct listing if coordination didn't return anything
        if entries.isEmpty {
            entries = (try? fm.contentsOfDirectory(at: doneDir, includingPropertiesForKeys: [.isRegularFileKey], options: [])) ?? []
        }

        for e in entries where e.pathExtension == "json" {
            let values = try? e.resourceValues(forKeys: [.isUbiquitousItemKey, .ubiquitousItemDownloadingStatusKey])
            let isCloud = values?.isUbiquitousItem == true
            let isCurrent = values?.ubiquitousItemDownloadingStatus == URLUbiquitousItemDownloadingStatus.current

            // If the JSON is only a placeholder in the cloud, do not hydrate. Treat as missing/invalid.
            if isCloud && !isCurrent { continue }

            if let data = try? Data(contentsOf: e), let rec = try? JSONDecoder().decode(UploadDoneRecord.self, from: data) {
                map[rec.fileName] = rec
            }
        }
        return map
    }

    @discardableResult
    static func markDone(file: URL, base: URL) -> UploadDoneRecord? {
        let fm = FileManager.default
        let doneDir = base.appendingPathComponent(".done", isDirectory: true)
        do {
            try fm.createDirectory(at: doneDir, withIntermediateDirectories: true)
            let vals = try? file.resourceValues(forKeys: [.fileSizeKey, .contentModificationDateKey])
            let size = vals?.fileSize.map { Int64($0) } ?? 0
            let mtime = vals?.contentModificationDate ?? Date()
            let rec = UploadDoneRecord(fileName: file.lastPathComponent, fileSize: size, lastModifiedAt: mtime)
            let data = try JSONEncoder().encode(rec)
            let out = doneDir.appendingPathComponent("\(file.lastPathComponent).json")
            try data.write(to: out)
            return rec
        } catch {
            CustomLogger.log("[UploadCore] Failed to write .done: \(error.localizedDescription)")
            return nil
        }
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

    static func uploadAll(dir: UploadDirectory, server: String, sender: String, stopOnError: Bool = true, completion: @escaping (String?) -> Void) {
        guard let baseURL = UploadHelper.resolveURL(from: dir.bookmark) else {
            return completion(UploadCoreError.invalidBookmark.description)
        }
        let hasAccess = baseURL.startAccessingSecurityScopedResource()
        defer { if hasAccess { baseURL.stopAccessingSecurityScopedResource() } }

        let files = UploadHelper.listFiles(in: baseURL)
        guard !files.isEmpty else { return completion(nil) }
        let doneMap = UploadHelper.loadDoneMap(for: baseURL)
        let pending = files.filter { file in
            guard let rec = doneMap[file.lastPathComponent] else { return true }
            return !UploadHelper.recordMatchesFile(rec, fileURL: file)
        }
        guard !pending.isEmpty else { return completion(nil) }

        let session = ServerSession.getSession(server: server)
        self.uploadQueue(pending, index: 0, dirName: dir.name, base: baseURL, session: session, sender: sender, stopOnError: stopOnError, completion: completion)
    }

    private static func uploadQueue(_ files: [URL], index: Int, dirName: String, base: URL, session: ServerSession, sender: String, stopOnError: Bool, completion: @escaping (String?) -> Void) {
        guard index < files.count else { return completion(nil) }
        let file = files[index]
        // In background (stopOnError == false), skip files that are not locally available to avoid
        // iCloud hydration while the device may be locked. Log the skip.
        if !stopOnError && !UploadHelper.isLocallyAvailable(file) {
            CustomLogger.log("[Upload][Skip] dir=\(dirName) file=\(file.lastPathComponent) reason=iCloud file not locally available")
            return self.uploadQueue(files, index: index + 1, dirName: dirName, base: base, session: session, sender: sender, stopOnError: stopOnError, completion: completion)
        }

        guard let data = try? Data(contentsOf: file) else {
            let msg = UploadCoreError.fileReadFailed(file.lastPathComponent).description
            CustomLogger.log("[Upload][Error] dir=\(dirName) file=\(file.lastPathComponent) err=\(msg)")
            if stopOnError { return completion(msg) }
            // Background: continue with remaining files
            return self.uploadQueue(files, index: index + 1, dirName: dirName, base: base, session: session, sender: sender, stopOnError: stopOnError, completion: completion)
        }
        let size = data.count
        CustomLogger.log("[Upload][Start] dir=\(dirName) file=\(file.lastPathComponent) bytes=\(size)")
        session.uploadFile(dirName: dirName, fileName: file.lastPathComponent, fileBytes: data, fullPath: file.path, sender: sender) { err in
            if let err = err {
                CustomLogger.log("[Upload][Error] dir=\(dirName) file=\(file.lastPathComponent) err=\(err)")
                if stopOnError { return completion(err) }
                // If not stopping on error, proceed to next
                return self.uploadQueue(files, index: index + 1, dirName: dirName, base: base, session: session, sender: sender, stopOnError: stopOnError, completion: completion)
            }
            CustomLogger.log("[Upload][Success] dir=\(dirName) file=\(file.lastPathComponent) bytes=\(size)")
            UploadHelper.markDone(file: file, base: base)
            self.uploadQueue(files, index: index + 1, dirName: dirName, base: base, session: session, sender: sender, stopOnError: stopOnError, completion: completion)
        }
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
    static func uploadAllDirectories(_ dirs: [UploadDirectory], server: String, sender: String, stopOnError: Bool = true, completion: @escaping (String?) -> Void) {
        func loop(_ idx: Int) {
            if idx >= dirs.count { return completion(nil) }
            uploadAll(dir: dirs[idx], server: server, sender: sender, stopOnError: stopOnError) { err in
                if let err = err { return completion(err) }
                loop(idx + 1)
            }
        }
        loop(0)
    }

    static func uploadAllFromStore(stopOnError: Bool = true, completion: @escaping (String?) -> Void) {
        guard let cfg = getServerAndSender() else { return completion(nil) }
        let store = UploadDirectoriesStore()
        let dirs = store.dirs
        guard !dirs.isEmpty else { return completion(nil) }
        uploadAllDirectories(dirs, server: cfg.server, sender: cfg.sender, stopOnError: stopOnError, completion: completion)
    }
}
