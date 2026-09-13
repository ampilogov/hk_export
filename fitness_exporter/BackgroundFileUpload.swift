import CryptoKit
import Foundation
import UserNotifications
import zlib

struct BackgroundUploadJob: Codable, Equatable {
    enum State: String, Codable {
        case staged
        case submitted
        case accepted
        case unknown
    }

    static let schemaVersion = 1

    let version: Int
    let id: String
    let deduplicationKey: String
    var state: State
    var taskIdentifier: Int?
    let bodyFileName: String
    let basePath: String
    let baseBookmark: Data
    let sourceFilePath: String
    let record: UploadDoneRecord
    let sourceSystemFileNumber: UInt64?
    let immediateRelativePath: String?
    let createdAt: Date
    var lastError: String?
}

enum BackgroundUploadError: Error, LocalizedError {
    case invalidConfiguration(String)
    case invalidJob(String)
    case persistence(String)
    case bodyEncoding(String)
    case transfer(String)
    case acceptedFinalization(String)

    var errorDescription: String? {
        switch self {
        case .invalidConfiguration(let message):
            return "Background upload configuration is invalid: \(message)"
        case .invalidJob(let message):
            return "Background upload job is invalid: \(message)"
        case .persistence(let message):
            return "Background upload state failed: \(message)"
        case .bodyEncoding(let message):
            return "Background upload body failed: \(message)"
        case .transfer(let message):
            return "Background upload failed: \(message)"
        case .acceptedFinalization(let message):
            return "Server accepted the upload, but local completion is pending: \(message)"
        }
    }
}

/// Each job is an independent atomic JSON record. The directory stays small
/// because the app schedules one file at a time during normal operation.
final class BackgroundUploadJobStore {
    let rootURL: URL
    let jobsDirectoryURL: URL
    let bodiesDirectoryURL: URL

    private let lock = NSLock()

    convenience init() throws {
        let supportURL = try FileManager.default.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        try self.init(
            rootURL: supportURL.appendingPathComponent(
                "BackgroundUploads",
                isDirectory: true
            )
        )
    }

    init(rootURL: URL) throws {
        self.rootURL = rootURL.standardizedFileURL
        jobsDirectoryURL = self.rootURL.appendingPathComponent(
            "Jobs",
            isDirectory: true
        )
        bodiesDirectoryURL = self.rootURL.appendingPathComponent(
            "Bodies",
            isDirectory: true
        )
        do {
            try FileManager.default.createDirectory(
                at: jobsDirectoryURL,
                withIntermediateDirectories: true
            )
            try FileManager.default.createDirectory(
                at: bodiesDirectoryURL,
                withIntermediateDirectories: true
            )
            var values = URLResourceValues()
            values.isExcludedFromBackup = true
            var mutableRootURL = self.rootURL
            try mutableRootURL.setResourceValues(values)
        } catch {
            throw BackgroundUploadError.persistence(
                "create storage: \(error.localizedDescription)"
            )
        }
    }

    func insert(_ job: BackgroundUploadJob) throws {
        try validate(job)
        lock.lock()
        defer { lock.unlock() }
        let url = jobURL(job.id)
        guard !FileManager.default.fileExists(atPath: url.path) else {
            throw BackgroundUploadError.persistence(
                "job \(job.id) already exists"
            )
        }
        try write(job, to: url)
    }

    func update(_ job: BackgroundUploadJob) throws {
        try validate(job)
        lock.lock()
        defer { lock.unlock() }
        let url = jobURL(job.id)
        guard FileManager.default.fileExists(atPath: url.path) else {
            throw BackgroundUploadError.persistence(
                "job \(job.id) no longer exists"
            )
        }
        try write(job, to: url)
    }

    func job(id: String) throws -> BackgroundUploadJob? {
        guard UUID(uuidString: id) != nil else {
            throw BackgroundUploadError.invalidJob(id)
        }
        lock.lock()
        defer { lock.unlock() }
        let url = jobURL(id)
        guard FileManager.default.fileExists(atPath: url.path) else {
            return nil
        }
        return try read(url)
    }

    func job(deduplicationKey: String) throws -> BackgroundUploadJob? {
        try allJobs().first { $0.deduplicationKey == deduplicationKey }
    }

    func allJobs() throws -> [BackgroundUploadJob] {
        lock.lock()
        defer { lock.unlock() }
        let urls: [URL]
        do {
            urls = try FileManager.default.contentsOfDirectory(
                at: jobsDirectoryURL,
                includingPropertiesForKeys: [.isRegularFileKey],
                options: [.skipsHiddenFiles]
            ).filter { $0.pathExtension == "json" }
        } catch {
            throw BackgroundUploadError.persistence(
                "list jobs: \(error.localizedDescription)"
            )
        }
        return try urls.sorted { $0.lastPathComponent < $1.lastPathComponent }
            .map(read)
    }

    func remove(id: String) throws {
        guard UUID(uuidString: id) != nil else {
            throw BackgroundUploadError.invalidJob(id)
        }
        lock.lock()
        defer { lock.unlock() }
        let url = jobURL(id)
        guard FileManager.default.fileExists(atPath: url.path) else { return }
        do {
            try FileManager.default.removeItem(at: url)
        } catch {
            throw BackgroundUploadError.persistence(
                "remove job \(id): \(error.localizedDescription)"
            )
        }
    }

    func bodyURL(fileName: String) throws -> URL {
        guard !fileName.isEmpty,
              fileName == (fileName as NSString).lastPathComponent else {
            throw BackgroundUploadError.invalidJob(
                "invalid body filename \(fileName)"
            )
        }
        return bodiesDirectoryURL.appendingPathComponent(
            fileName,
            isDirectory: false
        )
    }

    func removeOrphanedBodies(referencedBy jobs: [BackgroundUploadJob]) {
        let referenced = Set(jobs.map(\.bodyFileName))
        guard let urls = try? FileManager.default.contentsOfDirectory(
            at: bodiesDirectoryURL,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ) else { return }
        for url in urls where !referenced.contains(url.lastPathComponent) {
            do {
                try FileManager.default.removeItem(at: url)
            } catch {
                CustomLogger.log(
                    "[Upload][Background][Error] Remove orphaned body "
                        + "\(url.lastPathComponent): \(error.localizedDescription)"
                )
            }
        }
    }

    private func jobURL(_ id: String) -> URL {
        jobsDirectoryURL.appendingPathComponent("\(id).json", isDirectory: false)
    }

    private func write(_ job: BackgroundUploadJob, to url: URL) throws {
        do {
            let data = try JSONEncoder().encode(job)
            try data.write(to: url, options: [.atomic])
            try FileManager.default.setAttributes(
                [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
                ofItemAtPath: url.path
            )
        } catch {
            if let backgroundError = error as? BackgroundUploadError {
                throw backgroundError
            }
            throw BackgroundUploadError.persistence(
                "write job \(job.id): \(error.localizedDescription)"
            )
        }
    }

    private func read(_ url: URL) throws -> BackgroundUploadJob {
        do {
            let job = try JSONDecoder().decode(
                BackgroundUploadJob.self,
                from: Data(contentsOf: url)
            )
            try validate(job)
            guard url.deletingPathExtension().lastPathComponent == job.id else {
                throw BackgroundUploadError.invalidJob(
                    "filename does not match job \(job.id)"
                )
            }
            return job
        } catch {
            if let backgroundError = error as? BackgroundUploadError {
                throw backgroundError
            }
            throw BackgroundUploadError.persistence(
                "read \(url.lastPathComponent): \(error.localizedDescription)"
            )
        }
    }

    private func validate(_ job: BackgroundUploadJob) throws {
        guard job.version == BackgroundUploadJob.schemaVersion,
              UUID(uuidString: job.id) != nil,
              !job.deduplicationKey.isEmpty,
              job.bodyFileName == (job.bodyFileName as NSString).lastPathComponent,
              job.bodyFileName == "\(job.id).body",
              job.record.fileSize >= 0,
              !job.record.fileName.isEmpty,
              job.record.fileName != ".",
              job.record.fileName != "..",
              job.record.fileName
                == (job.sourceFilePath as NSString).lastPathComponent,
              job.record.lastModifiedAt.timeIntervalSinceReferenceDate.isFinite,
              job.createdAt.timeIntervalSinceReferenceDate.isFinite else {
            throw BackgroundUploadError.invalidJob(job.id)
        }
        switch job.state {
        case .staged:
            guard job.taskIdentifier == nil else {
                throw BackgroundUploadError.invalidJob(
                    "staged job \(job.id) already has a task"
                )
            }
        case .submitted, .accepted, .unknown:
            guard job.taskIdentifier != nil else {
                throw BackgroundUploadError.invalidJob(
                    "\(job.state.rawValue) job \(job.id) has no task"
                )
            }
        }
    }
}

struct PreparedBackgroundUpload {
    let request: URLRequest
    let bodyURL: URL
}

enum BackgroundUploadBodyBuilder {
    private static let chunkSize = 64 * 1_024

    static func prepare(
        jobID: String,
        server: String,
        sender: String,
        directoryName: String,
        fileName: String,
        fullPath: String,
        fileBytes: Data,
        timeout: TimeInterval = 60,
        bodiesDirectoryURL: URL
    ) throws -> PreparedBackgroundUpload {
        guard let baseURL = URL(string: server) else {
            throw BackgroundUploadError.invalidConfiguration(
                "invalid server URL \(server)"
            )
        }
        try FileManager.default.createDirectory(
            at: bodiesDirectoryURL,
            withIntermediateDirectories: true
        )

        let senderHash = SHA256.hash(
            data: Data((sender + HealthDataExporter.SENDER_EXTRA_KEY).utf8)
        ).map { String(format: "%02x", $0) }.joined()
        let payload: [String: Any] = [
            "version": HealthDataExporter.VERSION,
            "sender_sha256": senderHash,
            "dir_name": directoryName,
            "file_name": fileName,
            "full_path": fullPath,
            "file_bytes": fileBytes,
        ]

        let propertyListURL = bodiesDirectoryURL.appendingPathComponent(
            "\(jobID).plist.partial"
        )
        let partialBodyURL = bodiesDirectoryURL.appendingPathComponent(
            "\(jobID).body.partial"
        )
        let bodyURL = bodiesDirectoryURL.appendingPathComponent(
            "\(jobID).body"
        )
        let temporaryURLs = [propertyListURL, partialBodyURL]
        defer {
            for url in temporaryURLs {
                try? FileManager.default.removeItem(at: url)
            }
        }

        do {
            try writePropertyList(payload, to: propertyListURL)
            try gzipFile(from: propertyListURL, to: partialBodyURL)
            guard !FileManager.default.fileExists(atPath: bodyURL.path) else {
                throw BackgroundUploadError.bodyEncoding(
                    "body \(bodyURL.lastPathComponent) already exists"
                )
            }
            try FileManager.default.moveItem(
                at: partialBodyURL,
                to: bodyURL
            )
            try FileManager.default.setAttributes(
                [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
                ofItemAtPath: bodyURL.path
            )
        } catch {
            try? FileManager.default.removeItem(at: bodyURL)
            if let backgroundError = error as? BackgroundUploadError {
                throw backgroundError
            }
            throw BackgroundUploadError.bodyEncoding(error.localizedDescription)
        }

        var request = URLRequest(url: baseURL.appendingPathComponent("file"))
        request.httpMethod = "POST"
        request.timeoutInterval = timeout
        request.setValue("application/x-plist", forHTTPHeaderField: "Content-Type")
        request.setValue("gzip", forHTTPHeaderField: "Content-Encoding")
        return PreparedBackgroundUpload(request: request, bodyURL: bodyURL)
    }

    private static func writePropertyList(
        _ payload: [String: Any],
        to url: URL
    ) throws {
        guard let stream = OutputStream(url: url, append: false) else {
            throw BackgroundUploadError.bodyEncoding(
                "could not create the property-list stream"
            )
        }
        stream.open()
        defer { stream.close() }
        var serializationError: NSError?
        let written = PropertyListSerialization.writePropertyList(
            payload,
            to: stream,
            format: .binary,
            options: 0,
            error: &serializationError
        )
        if let serializationError { throw serializationError }
        if let streamError = stream.streamError { throw streamError }
        guard written > 0 else {
            throw BackgroundUploadError.bodyEncoding(
                "property-list serialization wrote no data"
            )
        }
    }

    private static func gzipFile(from sourceURL: URL, to destinationURL: URL) throws {
        let input = try FileHandle(forReadingFrom: sourceURL)
        defer { try? input.close() }
        guard FileManager.default.createFile(
            atPath: destinationURL.path,
            contents: nil
        ) else {
            throw BackgroundUploadError.bodyEncoding(
                "could not create \(destinationURL.lastPathComponent)"
            )
        }
        let output = try FileHandle(forWritingTo: destinationURL)
        defer { try? output.close() }

        var zstream = z_stream()
        let initialization = deflateInit2_(
            &zstream,
            Z_DEFAULT_COMPRESSION,
            Z_DEFLATED,
            MAX_WBITS + 16,
            8,
            Z_DEFAULT_STRATEGY,
            ZLIB_VERSION,
            Int32(MemoryLayout<z_stream>.size)
        )
        guard initialization == Z_OK else {
            throw BackgroundUploadError.bodyEncoding(
                "gzip initialization returned \(initialization)"
            )
        }
        defer { deflateEnd(&zstream) }

        while true {
            let inputData = try input.read(upToCount: chunkSize) ?? Data()
            if inputData.isEmpty { break }
            try inputData.withUnsafeBytes { rawBuffer in
                zstream.next_in = UnsafeMutablePointer<Bytef>(
                    mutating: rawBuffer.bindMemory(to: Bytef.self).baseAddress
                )
                zstream.avail_in = uInt(rawBuffer.count)
                _ = try writeDeflated(
                    &zstream,
                    flush: Z_NO_FLUSH,
                    output: output
                )
            }
        }

        zstream.next_in = nil
        zstream.avail_in = 0
        let result = try writeDeflated(
            &zstream,
            flush: Z_FINISH,
            output: output
        )
        guard result == Z_STREAM_END else {
            throw BackgroundUploadError.bodyEncoding(
                "gzip finalization returned \(result)"
            )
        }
        try output.synchronize()
    }

    private static func writeDeflated(
        _ stream: inout z_stream,
        flush: Int32,
        output: FileHandle
    ) throws -> Int32 {
        var result = Int32(Z_OK)
        repeat {
            var outputData = Data(count: chunkSize)
            result = outputData.withUnsafeMutableBytes { rawBuffer in
                stream.next_out = rawBuffer.bindMemory(
                    to: Bytef.self
                ).baseAddress
                stream.avail_out = uInt(rawBuffer.count)
                return deflate(&stream, flush)
            }
            guard result == Z_OK || result == Z_STREAM_END else {
                let detail = stream.msg.map { String(cString: $0) }
                    ?? "zlib error \(result)"
                throw BackgroundUploadError.bodyEncoding(detail)
            }
            let produced = outputData.count - Int(stream.avail_out)
            if produced > 0 {
                try output.write(contentsOf: outputData.prefix(produced))
            }
        } while stream.avail_out == 0
            || (flush == Z_FINISH && result != Z_STREAM_END)
        return result
    }
}

final class BackgroundFileUploadManager: CustomSessionDelegate,
    URLSessionTaskDelegate
{
    static let shared = BackgroundFileUploadManager()
    static let sessionIdentifier =
        "com.artemz.fitness_exporter.background-file-upload.v1"

    typealias Completion = (UploadFileFailure?) -> Void

    private let store: BackgroundUploadJobStore?
    private let setupError: String?
    private let sessionConfiguration: URLSessionConfiguration
    private let submissionLock = NSLock()
    private let stateQueue = DispatchQueue(
        label: "com.fitness_exporter.backgroundUpload.state"
    )
    private let workerQueue = DispatchQueue(
        label: "com.fitness_exporter.backgroundUpload.worker",
        qos: .utility
    )
    private var activated = false
    private var callbacks: [String: [Completion]] = [:]
    private var eventsCompletionHandler: (() -> Void)?
    private var finishedEventsBeforeHandler = false

    private static func backgroundConfiguration() -> URLSessionConfiguration {
        let configuration = URLSessionConfiguration.background(
            withIdentifier: Self.sessionIdentifier
        )
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        configuration.urlCache = nil
        configuration.httpShouldSetCookies = true
        configuration.httpShouldUsePipelining = true
        configuration.isDiscretionary = false
        configuration.sessionSendsLaunchEvents = true
        configuration.timeoutIntervalForRequest = 60
        configuration.timeoutIntervalForResource = 24 * 60 * 60
        configuration.allowsCellularAccess = true
        configuration.allowsConstrainedNetworkAccess = true
        configuration.allowsExpensiveNetworkAccess = true
        return configuration
    }

    private lazy var session: URLSession = {
        let delegateQueue = OperationQueue()
        delegateQueue.name = "com.fitness_exporter.backgroundUpload.delegate"
        delegateQueue.maxConcurrentOperationCount = 1
        return URLSession(
            configuration: sessionConfiguration,
            delegate: self,
            delegateQueue: delegateQueue
        )
    }()

    override init() {
        sessionConfiguration = Self.backgroundConfiguration()
        do {
            store = try BackgroundUploadJobStore()
            setupError = nil
        } catch {
            store = nil
            setupError = error.localizedDescription
        }
        super.init()
    }

    init(store: BackgroundUploadJobStore, configuration: URLSessionConfiguration) {
        self.store = store
        setupError = nil
        sessionConfiguration = configuration
        super.init()
    }

    func activate() {
        stateQueue.sync {
            guard !activated else { return }
            activated = true
            _ = session
            reconcileOutstandingTasks()
        }
    }

    func handleEvents(completionHandler: @escaping () -> Void) {
        stateQueue.async { [self] in
            if finishedEventsBeforeHandler {
                finishedEventsBeforeHandler = false
                completionHandler()
            } else {
                eventsCompletionHandler = completionHandler
            }
        }
    }

    func uploadFile(
        deduplicationKey: String,
        server: String,
        sender: String,
        directoryName: String,
        baseURL: URL,
        baseBookmark: Data,
        sourceFileURL: URL,
        fileBytes: Data,
        record: UploadDoneRecord,
        sourceSystemFileNumber: UInt64?,
        immediateRelativePath: String?,
        cancellationToken: UploadCancellationToken?,
        validateSource: () throws -> Void,
        completion: @escaping Completion
    ) throws {
        activate()
        guard cancellationToken?.isCancelled != true else {
            throw UploadCoreError.cancelled
        }
        guard let store else {
            throw BackgroundUploadError.persistence(
                setupError ?? "storage is unavailable"
            )
        }

        submissionLock.lock()
        defer { submissionLock.unlock() }

        if let done = try UploadHelper.doneRecord(
            fileName: record.fileName,
            base: baseURL
        ), done == record,
           UploadHelper.sourceIdentityMatchesFile(
               record: record,
               systemFileNumber: sourceSystemFileNumber,
               fileURL: sourceFileURL
           ) {
            completion(nil)
            return
        }

        if let existing = try store.job(
            deduplicationKey: deduplicationKey
        ) {
            guard existing.record == record,
                  existing.sourceSystemFileNumber == sourceSystemFileNumber else {
                throw BackgroundUploadError.transfer(
                    "A previous transfer for \(record.fileName) still has "
                        + "durable state for a different file identity."
                )
            }
            switch existing.state {
            case .unknown:
                // Overfit identifies a file by content, directory, filename,
                // user and version. Repeating this unchanged payload is safe
                // even if the previous response was lost after acceptance.
                try validateSource()
                try store.remove(id: existing.id)
                if let bodyURL = try? store.bodyURL(fileName: existing.bodyFileName) {
                    try? FileManager.default.removeItem(at: bodyURL)
                }
                CustomLogger.log(
                    "[Upload][Background][Retry] file=\(record.fileName) "
                        + "retrying an uncertain outcome with the same file identity"
                )
            case .accepted:
                appendCallback(completion, jobID: existing.id)
                workerQueue.async { [weak self] in
                    guard let self else { return }
                    self.submissionLock.lock()
                    defer { self.submissionLock.unlock() }
                    self.finalizeAcceptedJob(existing)
                }
                return
            case .staged, .submitted:
                appendCallback(completion, jobID: existing.id)
                reconcileOutstandingTasks()
                return
            }
        }

        let jobID = UUID().uuidString
        let prepared = try BackgroundUploadBodyBuilder.prepare(
            jobID: jobID,
            server: server,
            sender: sender,
            directoryName: directoryName,
            fileName: record.fileName,
            fullPath: sourceFileURL.path,
            fileBytes: fileBytes,
            bodiesDirectoryURL: store.bodiesDirectoryURL
        )
        do {
            try validateSource()
        } catch {
            try? FileManager.default.removeItem(at: prepared.bodyURL)
            throw error
        }

        var job = BackgroundUploadJob(
            version: BackgroundUploadJob.schemaVersion,
            id: jobID,
            deduplicationKey: deduplicationKey,
            state: .staged,
            taskIdentifier: nil,
            bodyFileName: prepared.bodyURL.lastPathComponent,
            basePath: baseURL.standardizedFileURL.path,
            baseBookmark: baseBookmark,
            sourceFilePath: sourceFileURL.standardizedFileURL.path,
            record: record,
            sourceSystemFileNumber: sourceSystemFileNumber,
            immediateRelativePath: immediateRelativePath,
            createdAt: Date(),
            lastError: nil
        )

        do {
            try store.insert(job)
            let task = session.uploadTask(
                with: prepared.request,
                fromFile: prepared.bodyURL
            )
            task.taskDescription = jobID
            job.state = .submitted
            job.taskIdentifier = task.taskIdentifier
            try store.update(job)
            appendCallback(completion, jobID: jobID)
            task.resume()
        } catch {
            try? store.remove(id: jobID)
            try? FileManager.default.removeItem(at: prepared.bodyURL)
            throw error
        }
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didCompleteWithError error: Error?
    ) {
        guard let jobID = task.taskDescription, !jobID.isEmpty else {
            attention(
                "A background upload completed without durable task metadata."
            )
            return
        }
        workerQueue.async { [weak self] in
            self?.handleCompletion(
                jobID: jobID,
                response: task.response,
                error: error
            )
        }
    }

    func urlSessionDidFinishEvents(forBackgroundURLSession session: URLSession) {
        // didCompleteWithError queues durable work onto workerQueue. Hop
        // through that serial queue before telling iOS all events are handled.
        workerQueue.async { [weak self] in
            self?.stateQueue.async { [weak self] in
                guard let self else { return }
                if let completion = eventsCompletionHandler {
                    eventsCompletionHandler = nil
                    completion()
                } else {
                    finishedEventsBeforeHandler = true
                }
            }
        }
    }

    private func handleCompletion(
        jobID: String,
        response: URLResponse?,
        error: Error?
    ) {
        guard let store else { return }
        submissionLock.lock()
        defer { submissionLock.unlock() }
        let job: BackgroundUploadJob
        do {
            guard let stored = try store.job(id: jobID) else {
                let message = "Background upload \(jobID) completed without "
                    + "its durable job record."
                attention(message)
                completeCallbacks(
                    jobID: jobID,
                    failure: UploadFileFailure(
                        message: message,
                        scope: .collection
                    ),
                    recoveredJob: nil
                )
                return
            }
            job = stored
        } catch {
            let message = error.localizedDescription
            attention(message)
            completeCallbacks(
                jobID: jobID,
                failure: UploadFileFailure(
                    message: message,
                    scope: .collection
                ),
                recoveredJob: nil
            )
            return
        }

        if let error {
            failTransfer(
                job,
                message: "Client error: \(error.localizedDescription)",
                scope: .collection
            )
            return
        }
        guard let response = response as? HTTPURLResponse else {
            failTransfer(
                job,
                message: "Server error: \(String(describing: response))",
                scope: .collection
            )
            return
        }
        guard (200...299).contains(response.statusCode) else {
            failTransfer(
                job,
                message: "Server error: HTTP \(response.statusCode)",
                scope: Self.failureScope(forHTTPStatusCode: response.statusCode)
            )
            return
        }

        var accepted = job
        accepted.state = .accepted
        accepted.lastError = nil
        do {
            try store.update(accepted)
        } catch {
            let message = BackgroundUploadError.acceptedFinalization(
                "persist acceptance: \(error.localizedDescription)"
            ).localizedDescription
            attention(message)
            completeCallbacks(
                jobID: job.id,
                failure: UploadFileFailure(message: message, scope: .file),
                recoveredJob: job
            )
            return
        }
        finalizeAcceptedJob(accepted)
    }

    private func finalizeAcceptedJob(_ job: BackgroundUploadJob) {
        guard let store else { return }
        do {
            let baseURL = UploadHelper.resolveURL(from: job.baseBookmark)
                ?? URL(fileURLWithPath: job.basePath)
            let hasAccess = baseURL.startAccessingSecurityScopedResource()
            defer {
                if hasAccess { baseURL.stopAccessingSecurityScopedResource() }
            }
            let sourceURL = baseURL.appendingPathComponent(
                job.record.fileName,
                isDirectory: false
            )
            guard UploadHelper.sourceIdentityMatchesFile(
                record: job.record,
                systemFileNumber: job.sourceSystemFileNumber,
                fileURL: sourceURL
            ) else {
                throw BackgroundUploadError.acceptedFinalization(
                    "\(job.record.fileName) changed after submission"
                )
            }
            try UploadHelper.markDone(record: job.record, base: baseURL)
            guard try UploadHelper.doneRecord(
                fileName: job.record.fileName,
                base: baseURL
            ) == job.record else {
                throw BackgroundUploadError.acceptedFinalization(
                    "completion index did not retain \(job.record.fileName)"
                )
            }
            try store.remove(id: job.id)
            if let bodyURL = try? store.bodyURL(fileName: job.bodyFileName) {
                try? FileManager.default.removeItem(at: bodyURL)
            }
            completeCallbacks(jobID: job.id, failure: nil, recoveredJob: job)
        } catch {
            var retained = job
            retained.state = .accepted
            retained.lastError = error.localizedDescription
            do {
                try store.update(retained)
            } catch let persistenceError {
                attention(
                    error.localizedDescription + "; retain accepted state: "
                        + persistenceError.localizedDescription
                )
            }
            attention(error.localizedDescription)
            completeCallbacks(
                jobID: job.id,
                failure: UploadFileFailure(
                    message: error.localizedDescription,
                    scope: .file
                ),
                recoveredJob: job
            )
        }
    }

    private func failTransfer(
        _ job: BackgroundUploadJob,
        message: String,
        scope: UploadFailureScope
    ) {
        guard let store else { return }
        var finalMessage = message
        var removedJob = false
        do {
            try store.remove(id: job.id)
            removedJob = true
        } catch {
            finalMessage += "; remove failed task state: \(error.localizedDescription)"
        }
        if removedJob,
           let bodyURL = try? store.bodyURL(fileName: job.bodyFileName) {
            try? FileManager.default.removeItem(at: bodyURL)
        }
        completeCallbacks(
            jobID: job.id,
            failure: UploadFileFailure(
                message: finalMessage,
                scope: scope
            ),
            recoveredJob: job
        )
    }

    static func failureScope(
        forHTTPStatusCode statusCode: Int
    ) -> UploadFailureScope {
        guard (400...499).contains(statusCode) else { return .collection }
        switch statusCode {
        case 401, 403, 404, 408, 429:
            // Authentication, endpoint configuration, timeout, and throttling
            // affect the collection rather than the current source file.
            return .collection
        default:
            // Payload/file rejections must remain visible but cannot prevent
            // later valid recordings from being attempted.
            return .file
        }
    }

    private func reconcileOutstandingTasks() {
        let currentSession = session
        currentSession.getAllTasks { [weak self] tasks in
            self?.workerQueue.async {
                self?.reconcile(tasks: tasks)
            }
        }
    }

    private func reconcile(tasks: [URLSessionTask]) {
        guard let store else { return }
        submissionLock.lock()
        defer { submissionLock.unlock() }
        let jobs: [BackgroundUploadJob]
        do {
            jobs = try store.allJobs()
        } catch {
            attention(error.localizedDescription)
            return
        }

        let jobsByID = Dictionary(uniqueKeysWithValues: jobs.map { ($0.id, $0) })
        var tasksByJobID: [String: URLSessionTask] = [:]
        for task in tasks {
            guard let jobID = task.taskDescription,
                  let job = jobsByID[jobID] else {
                task.cancel()
                attention(
                    "Cancelled untracked background upload task "
                        + "\(task.taskIdentifier)."
                )
                continue
            }

            if let expectedIdentifier = job.taskIdentifier,
               task.taskIdentifier != expectedIdentifier {
                task.cancel()
                attention(
                    "Cancelled duplicate background task \(task.taskIdentifier) "
                        + "for job \(jobID)."
                )
                continue
            }
            if let existing = tasksByJobID[jobID] {
                task.cancel()
                attention(
                    "Cancelled duplicate background task \(task.taskIdentifier) "
                        + "for job \(jobID); retained \(existing.taskIdentifier)."
                )
                continue
            }
            tasksByJobID[jobID] = task
        }

        for var job in jobs {
            switch job.state {
            case .accepted:
                finalizeAcceptedJob(job)
            case .unknown:
                attention(
                    job.lastError
                        ?? "Background upload \(job.id) has an unknown outcome."
                )
            case .staged:
                if let task = tasksByJobID[job.id] {
                    job.state = .submitted
                    job.taskIdentifier = task.taskIdentifier
                    do {
                        try store.update(job)
                        task.resume()
                    } catch {
                        attention(error.localizedDescription)
                    }
                } else {
                    failTransfer(
                        job,
                        message: "Upload staging was interrupted before submission",
                        scope: .file
                    )
                }
            case .submitted:
                if let task = tasksByJobID[job.id] {
                    if task.state == .suspended { task.resume() }
                } else {
                    auditMissingSubmittedJob(job.id)
                }
            }
        }
        store.removeOrphanedBodies(referencedBy: jobs)
    }

    private func auditMissingSubmittedJob(_ jobID: String) {
        workerQueue.asyncAfter(deadline: .now() + 5) { [weak self] in
            guard let self else { return }
            self.session.getAllTasks { tasks in
                self.workerQueue.async {
                    guard !tasks.contains(where: {
                        $0.taskDescription == jobID
                    }), let store = self.store else { return }
                    self.submissionLock.lock()
                    defer { self.submissionLock.unlock() }
                    do {
                        guard var job = try store.job(id: jobID),
                              job.state == .submitted else { return }
                        job.state = .unknown
                        job.lastError =
                            "The system no longer reports background task "
                            + "\(job.taskIdentifier ?? -1), so its server outcome is unknown. "
                            + "The source remains pending for an idempotent retry."
                        try store.update(job)
                        let message = job.lastError ?? "Upload outcome is unknown"
                        self.attention(message)
                        self.completeCallbacks(
                            jobID: job.id,
                            failure: UploadFileFailure(
                                message: message,
                                scope: .file
                            ),
                            recoveredJob: job
                        )
                    } catch {
                        self.attention(error.localizedDescription)
                    }
                }
            }
        }
    }

    private func appendCallback(_ callback: @escaping Completion, jobID: String) {
        stateQueue.sync {
            callbacks[jobID, default: []].append(callback)
        }
    }

    private func completeCallbacks(
        jobID: String,
        failure: UploadFileFailure?,
        recoveredJob: BackgroundUploadJob?
    ) {
        let currentCallbacks: [Completion] = stateQueue.sync {
            callbacks.removeValue(forKey: jobID) ?? []
        }
        if currentCallbacks.isEmpty,
           let job = recoveredJob,
           let relativePath = job.immediateRelativePath {
            ImmediateUploadService.shared.handleRecoveredBackgroundResult(
                relativePath: relativePath,
                error: failure?.message
            )
            return
        }
        for callback in currentCallbacks {
            callback(failure)
        }
    }

    private func attention(_ message: String) {
        CustomLogger.log("[Upload][Background][Attention] \(message)")
        let content = UNMutableNotificationContent()
        content.title = "Recording upload needs attention"
        content.body = message
        content.sound = .default
        UNUserNotificationCenter.current().add(
            UNNotificationRequest(
                identifier: "ContinuousRecording.BackgroundUploadAttention",
                content: content,
                trigger: nil
            )
        ) { error in
            if let error {
                CustomLogger.log(
                    "[Upload][Background][Notification][Error] "
                        + error.localizedDescription
                )
            }
        }
    }
}
