import Foundation
import SQLite3

private let immediateUploadSQLiteTransient = unsafeBitCast(
    -1,
    to: sqlite3_destructor_type.self
)

struct PendingImmediateUpload: Equatable {
    let relativePath: String
    let record: UploadDoneRecord
    let enqueuedAt: Date
    let attemptCount: Int
    let nextAttemptAt: Date?
    let lastError: String?
}

enum ImmediateUploadQueueError: Error, LocalizedError {
    case invalidSourcePath(String)
    case invalidRecord(String)
    case database(String)
    case missingRecord(String)

    var errorDescription: String? {
        switch self {
        case .invalidSourcePath(let message):
            return "Invalid immediate-upload source path: \(message)"
        case .invalidRecord(let message):
            return "Invalid immediate-upload record: \(message)"
        case .database(let message):
            return "Immediate-upload queue failed: \(message)"
        case .missingRecord(let path):
            return "Immediate-upload queue no longer contains \(path)"
        }
    }
}

/// Small local durability journal for finalized recording files. It contains
/// metadata only; source bytes remain in the existing SensorBag files.
final class ImmediateUploadQueue {
    static let databaseFileName = "immediate-upload-queue.sqlite3"
    private static let operationLock = NSLock()

    let documentsURL: URL
    let databaseURL: URL
    private var database: OpaquePointer?

    convenience init(
        documentsURL: URL,
        databaseURL: URL
    ) throws {
        try self.init(
            documentsURL: documentsURL,
            databaseURL: databaseURL,
            createParentDirectory: true
        )
    }

    private init(
        documentsURL: URL,
        databaseURL: URL,
        createParentDirectory: Bool
    ) throws {
        self.documentsURL = documentsURL.standardizedFileURL
        self.databaseURL = databaseURL.standardizedFileURL

        do {
            if createParentDirectory {
                try FileManager.default.createDirectory(
                    at: self.databaseURL.deletingLastPathComponent(),
                    withIntermediateDirectories: true
                )
            }
            try openDatabase()
            try configureDatabase()
            try createSchema()
        } catch {
            closeDatabase()
            throw error
        }
    }

    deinit {
        closeDatabase()
    }

    static func withDefault<T>(
        _ operation: (ImmediateUploadQueue) throws -> T
    ) throws -> T {
        operationLock.lock()
        defer { operationLock.unlock() }

        let manager = FileManager.default
        guard let documentsURL = manager.urls(
            for: .documentDirectory,
            in: .userDomainMask
        ).first else {
            throw ImmediateUploadQueueError.invalidSourcePath(
                "the app Documents directory is unavailable"
            )
        }
        guard let supportURL = manager.urls(
            for: .applicationSupportDirectory,
            in: .userDomainMask
        ).first else {
            throw ImmediateUploadQueueError.database(
                "the app Application Support directory is unavailable"
            )
        }
        let queueDirectory = supportURL.appendingPathComponent(
            "ImmediateUploads",
            isDirectory: true
        )
        let queue = try ImmediateUploadQueue(
            documentsURL: documentsURL,
            databaseURL: queueDirectory.appendingPathComponent(
                databaseFileName,
                isDirectory: false
            )
        )
        return try operation(queue)
    }

    @discardableResult
    func enqueue(
        fileURL: URL,
        now: Date = Date()
    ) throws -> PendingImmediateUpload {
        let relativePath = try makeRelativePath(fileURL)
        let fileRecord = try currentRecord(fileURL)
        try validate(record: fileRecord, relativePath: relativePath)

        try transaction {
            let statement = try prepare(
                """
                INSERT INTO pending_uploads (
                    relative_path,
                    file_name,
                    file_size,
                    modified_at,
                    enqueued_at,
                    attempt_count,
                    next_attempt_at,
                    last_error
                ) VALUES (?, ?, ?, ?, ?, 0, NULL, NULL)
                ON CONFLICT(relative_path) DO UPDATE SET
                    file_name = excluded.file_name,
                    file_size = excluded.file_size,
                    modified_at = excluded.modified_at,
                    enqueued_at = CASE
                        WHEN pending_uploads.file_name != excluded.file_name
                          OR pending_uploads.file_size != excluded.file_size
                          OR pending_uploads.modified_at != excluded.modified_at
                        THEN excluded.enqueued_at
                        ELSE pending_uploads.enqueued_at
                    END,
                    attempt_count = CASE
                        WHEN pending_uploads.file_name != excluded.file_name
                          OR pending_uploads.file_size != excluded.file_size
                          OR pending_uploads.modified_at != excluded.modified_at
                        THEN 0
                        ELSE pending_uploads.attempt_count
                    END,
                    next_attempt_at = CASE
                        WHEN pending_uploads.file_name != excluded.file_name
                          OR pending_uploads.file_size != excluded.file_size
                          OR pending_uploads.modified_at != excluded.modified_at
                        THEN NULL
                        ELSE pending_uploads.next_attempt_at
                    END,
                    last_error = CASE
                        WHEN pending_uploads.file_name != excluded.file_name
                          OR pending_uploads.file_size != excluded.file_size
                          OR pending_uploads.modified_at != excluded.modified_at
                        THEN NULL
                        ELSE pending_uploads.last_error
                    END;
                """
            )
            defer { sqlite3_finalize(statement) }
            try bind(relativePath, to: 1, in: statement)
            try bind(fileRecord.fileName, to: 2, in: statement)
            guard sqlite3_bind_int64(statement, 3, fileRecord.fileSize) == SQLITE_OK,
                  sqlite3_bind_double(
                    statement,
                    4,
                    fileRecord.lastModifiedAt.timeIntervalSinceReferenceDate
                  ) == SQLITE_OK,
                  sqlite3_bind_double(
                    statement,
                    5,
                    now.timeIntervalSinceReferenceDate
                  ) == SQLITE_OK else {
                throw databaseError(operation: "bind queued recording")
            }
            guard sqlite3_step(statement) == SQLITE_DONE else {
                throw databaseError(operation: "enqueue recording")
            }
        }

        guard let pending = try record(relativePath: relativePath) else {
            throw ImmediateUploadQueueError.missingRecord(relativePath)
        }
        return pending
    }

    func nextDue(at now: Date = Date()) throws -> PendingImmediateUpload? {
        let statement = try prepare(
            """
            SELECT relative_path, file_name, file_size, modified_at,
                   enqueued_at, attempt_count, next_attempt_at, last_error
            FROM pending_uploads
            WHERE next_attempt_at IS NULL OR next_attempt_at <= ?
            -- A permanently bad older file must not consume every wake and
            -- starve newly finalized recordings. Fresh entries get their
            -- first attempt before retries, while FIFO order is retained
            -- within the same attempt count.
            ORDER BY attempt_count ASC, enqueued_at ASC, relative_path ASC
            LIMIT 1;
            """
        )
        defer { sqlite3_finalize(statement) }
        guard sqlite3_bind_double(
            statement,
            1,
            now.timeIntervalSinceReferenceDate
        ) == SQLITE_OK else {
            throw databaseError(operation: "bind immediate-upload due time")
        }
        let result = sqlite3_step(statement)
        if result == SQLITE_DONE { return nil }
        guard result == SQLITE_ROW else {
            throw databaseError(operation: "read next immediate upload")
        }
        return try decodeRecord(statement)
    }

    func allRecords() throws -> [PendingImmediateUpload] {
        let statement = try prepare(
            """
            SELECT relative_path, file_name, file_size, modified_at,
                   enqueued_at, attempt_count, next_attempt_at, last_error
            FROM pending_uploads
            ORDER BY enqueued_at ASC, relative_path ASC;
            """
        )
        defer { sqlite3_finalize(statement) }
        var records: [PendingImmediateUpload] = []
        while true {
            let result = sqlite3_step(statement)
            if result == SQLITE_DONE { return records }
            guard result == SQLITE_ROW else {
                throw databaseError(operation: "read immediate-upload queue")
            }
            records.append(try decodeRecord(statement))
        }
    }

    func nextRetryAt() throws -> Date? {
        let statement = try prepare("SELECT MIN(next_attempt_at) FROM pending_uploads;")
        defer { sqlite3_finalize(statement) }
        guard sqlite3_step(statement) == SQLITE_ROW else {
            throw databaseError(operation: "read next retry time")
        }
        guard sqlite3_column_type(statement, 0) != SQLITE_NULL else { return nil }
        return Date(timeIntervalSinceReferenceDate: sqlite3_column_double(statement, 0))
    }

    @discardableResult
    func markFailed(
        relativePath: String,
        error: String,
        now: Date = Date()
    ) throws -> PendingImmediateUpload {
        guard !error.isEmpty else {
            throw ImmediateUploadQueueError.invalidRecord(
                "failure text is empty for \(relativePath)"
            )
        }
        let existing = try requireRecord(relativePath: relativePath)
        let nextAttemptCount = existing.attemptCount + 1
        let exponent = min(6, max(0, nextAttemptCount - 1))
        let retryDelay = min(3_600, 60 * pow(2, Double(exponent)))
        let nextAttemptAt = now.addingTimeInterval(retryDelay)

        try transaction {
            let statement = try prepare(
                """
                UPDATE pending_uploads
                SET attempt_count = ?, next_attempt_at = ?, last_error = ?
                WHERE relative_path = ?;
                """
            )
            defer { sqlite3_finalize(statement) }
            guard sqlite3_bind_int64(
                statement,
                1,
                sqlite3_int64(nextAttemptCount)
            ) == SQLITE_OK,
                  sqlite3_bind_double(
                    statement,
                    2,
                    nextAttemptAt.timeIntervalSinceReferenceDate
                  ) == SQLITE_OK else {
                throw databaseError(operation: "bind immediate-upload failure")
            }
            try bind(error, to: 3, in: statement)
            try bind(relativePath, to: 4, in: statement)
            guard sqlite3_step(statement) == SQLITE_DONE else {
                throw databaseError(operation: "record immediate-upload failure")
            }
            guard sqlite3_changes(database) == 1 else {
                throw ImmediateUploadQueueError.missingRecord(relativePath)
            }
        }
        return try requireRecord(relativePath: relativePath)
    }

    func remove(relativePath: String) throws {
        try transaction {
            let statement = try prepare(
                "DELETE FROM pending_uploads WHERE relative_path = ?;"
            )
            defer { sqlite3_finalize(statement) }
            try bind(relativePath, to: 1, in: statement)
            guard sqlite3_step(statement) == SQLITE_DONE else {
                throw databaseError(operation: "remove completed immediate upload")
            }
            guard sqlite3_changes(database) == 1 else {
                throw ImmediateUploadQueueError.missingRecord(relativePath)
            }
        }
    }

    func fileURL(for pending: PendingImmediateUpload) throws -> URL {
        guard pending.relativePath == (pending.relativePath as NSString)
            .standardizingPath,
              !pending.relativePath.hasPrefix("/"),
              pending.relativePath != ".",
              !pending.relativePath.hasPrefix("../") else {
            throw ImmediateUploadQueueError.invalidSourcePath(
                pending.relativePath
            )
        }
        let result = documentsURL.appendingPathComponent(
            pending.relativePath,
            isDirectory: false
        ).standardizedFileURL
        _ = try makeRelativePath(result)
        return result
    }

    private func record(relativePath: String) throws -> PendingImmediateUpload? {
        let statement = try prepare(
            """
            SELECT relative_path, file_name, file_size, modified_at,
                   enqueued_at, attempt_count, next_attempt_at, last_error
            FROM pending_uploads
            WHERE relative_path = ?;
            """
        )
        defer { sqlite3_finalize(statement) }
        try bind(relativePath, to: 1, in: statement)
        let result = sqlite3_step(statement)
        if result == SQLITE_DONE { return nil }
        guard result == SQLITE_ROW else {
            throw databaseError(operation: "read immediate-upload record")
        }
        return try decodeRecord(statement)
    }

    private func requireRecord(
        relativePath: String
    ) throws -> PendingImmediateUpload {
        guard let record = try record(relativePath: relativePath) else {
            throw ImmediateUploadQueueError.missingRecord(relativePath)
        }
        return record
    }

    private func decodeRecord(
        _ statement: OpaquePointer?
    ) throws -> PendingImmediateUpload {
        guard let relativePathBytes = sqlite3_column_text(statement, 0),
              let fileNameBytes = sqlite3_column_text(statement, 1) else {
            throw ImmediateUploadQueueError.invalidRecord(
                "a queued path or filename is empty"
            )
        }
        let relativePath = String(cString: relativePathBytes)
        let fileName = String(cString: fileNameBytes)
        let pending = PendingImmediateUpload(
            relativePath: relativePath,
            record: UploadDoneRecord(
                fileName: fileName,
                fileSize: sqlite3_column_int64(statement, 2),
                lastModifiedAt: Date(
                    timeIntervalSinceReferenceDate: sqlite3_column_double(
                        statement,
                        3
                    )
                )
            ),
            enqueuedAt: Date(
                timeIntervalSinceReferenceDate: sqlite3_column_double(
                    statement,
                    4
                )
            ),
            attemptCount: Int(sqlite3_column_int64(statement, 5)),
            nextAttemptAt: sqlite3_column_type(statement, 6) == SQLITE_NULL
                ? nil
                : Date(
                    timeIntervalSinceReferenceDate: sqlite3_column_double(
                        statement,
                        6
                    )
                ),
            lastError: sqlite3_column_type(statement, 7) == SQLITE_NULL
                ? nil
                : sqlite3_column_text(statement, 7).map(String.init(cString:))
        )
        try validate(record: pending.record, relativePath: relativePath)
        guard pending.enqueuedAt.timeIntervalSinceReferenceDate.isFinite,
              pending.attemptCount >= 0,
              pending.attemptCount < 1_000_000 else {
            throw ImmediateUploadQueueError.invalidRecord(relativePath)
        }
        if let nextAttemptAt = pending.nextAttemptAt,
           !nextAttemptAt.timeIntervalSinceReferenceDate.isFinite {
            throw ImmediateUploadQueueError.invalidRecord(relativePath)
        }
        return pending
    }

    private func makeRelativePath(_ fileURL: URL) throws -> String {
        let source = fileURL.standardizedFileURL.resolvingSymlinksInPath()
        let resolvedDocuments = documentsURL.resolvingSymlinksInPath()
        let rootPath = resolvedDocuments.path.hasSuffix("/")
            ? resolvedDocuments.path
            : resolvedDocuments.path + "/"
        guard source.path.hasPrefix(rootPath) else {
            throw ImmediateUploadQueueError.invalidSourcePath(
                "\(source.path) is outside \(resolvedDocuments.path)"
            )
        }
        let relativePath = String(source.path.dropFirst(rootPath.count))
        guard !relativePath.isEmpty else {
            throw ImmediateUploadQueueError.invalidSourcePath(source.path)
        }
        return relativePath
    }

    private func currentRecord(_ fileURL: URL) throws -> UploadDoneRecord {
        let attributes: [FileAttributeKey: Any]
        do {
            attributes = try FileManager.default.attributesOfItem(
                atPath: fileURL.path
            )
        } catch {
            throw ImmediateUploadQueueError.invalidSourcePath(
                "\(fileURL.lastPathComponent): \(error.localizedDescription)"
            )
        }
        guard attributes[.type] as? FileAttributeType == .typeRegular,
              let size = (attributes[.size] as? NSNumber)?.int64Value,
              let modifiedAt = attributes[.modificationDate] as? Date else {
            throw ImmediateUploadQueueError.invalidSourcePath(
                "\(fileURL.lastPathComponent) is not a regular local file"
            )
        }
        return UploadDoneRecord(
            fileName: fileURL.lastPathComponent,
            fileSize: size,
            lastModifiedAt: modifiedAt
        )
    }

    private func validate(
        record: UploadDoneRecord,
        relativePath: String
    ) throws {
        guard !relativePath.isEmpty,
              !record.fileName.isEmpty,
              record.fileName == (relativePath as NSString).lastPathComponent,
              record.fileSize >= 0,
              record.lastModifiedAt.timeIntervalSinceReferenceDate.isFinite else {
            throw ImmediateUploadQueueError.invalidRecord(relativePath)
        }
    }

    private func openDatabase() throws {
        var handle: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        let result = sqlite3_open_v2(databaseURL.path, &handle, flags, nil)
        guard result == SQLITE_OK, let handle else {
            let message = handle.map { String(cString: sqlite3_errmsg($0)) }
                ?? "SQLite did not return a database handle"
            if let handle { sqlite3_close(handle) }
            throw ImmediateUploadQueueError.database(
                "open \(databaseURL.lastPathComponent): \(message)"
            )
        }
        database = handle
    }

    private func closeDatabase() {
        guard let database else { return }
        sqlite3_close(database)
        self.database = nil
    }

    private func configureDatabase() throws {
        guard let database else {
            throw ImmediateUploadQueueError.database("database is not open")
        }
        guard sqlite3_busy_timeout(database, 5_000) == SQLITE_OK else {
            throw databaseError(operation: "set busy timeout")
        }
        try execute("PRAGMA journal_mode = DELETE;")
        try execute("PRAGMA synchronous = FULL;")
        try execute("PRAGMA foreign_keys = ON;")
    }

    private func createSchema() throws {
        try execute(
            """
            CREATE TABLE IF NOT EXISTS pending_uploads (
                relative_path TEXT PRIMARY KEY NOT NULL,
                file_name TEXT NOT NULL,
                file_size INTEGER NOT NULL CHECK(file_size >= 0),
                modified_at REAL NOT NULL,
                enqueued_at REAL NOT NULL,
                attempt_count INTEGER NOT NULL CHECK(attempt_count >= 0),
                next_attempt_at REAL,
                last_error TEXT
            ) WITHOUT ROWID;
            """
        )
    }

    private func transaction(_ work: () throws -> Void) throws {
        try execute("BEGIN IMMEDIATE TRANSACTION;")
        do {
            try work()
            try execute("COMMIT;")
        } catch {
            try? execute("ROLLBACK;")
            throw error
        }
    }

    private func prepare(_ sql: String) throws -> OpaquePointer? {
        guard let database else {
            throw ImmediateUploadQueueError.database("database is not open")
        }
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(database, sql, -1, &statement, nil)
            == SQLITE_OK else {
            throw databaseError(operation: "prepare SQL")
        }
        return statement
    }

    private func bind(
        _ value: String,
        to index: Int32,
        in statement: OpaquePointer?
    ) throws {
        guard sqlite3_bind_text(
            statement,
            index,
            value,
            -1,
            immediateUploadSQLiteTransient
        ) == SQLITE_OK else {
            throw databaseError(operation: "bind text")
        }
    }

    private func execute(_ sql: String) throws {
        guard let database else {
            throw ImmediateUploadQueueError.database("database is not open")
        }
        var errorMessage: UnsafeMutablePointer<CChar>?
        let result = sqlite3_exec(database, sql, nil, nil, &errorMessage)
        guard result == SQLITE_OK else {
            let message = errorMessage.map { String(cString: $0) }
                ?? String(cString: sqlite3_errmsg(database))
            sqlite3_free(errorMessage)
            throw ImmediateUploadQueueError.database(message)
        }
    }

    private func databaseError(operation: String) -> ImmediateUploadQueueError {
        let detail = database.map { String(cString: sqlite3_errmsg($0)) }
            ?? "database is not open"
        return .database("\(operation): \(detail)")
    }
}

/// Drains the durable journal one file at a time. A failed transfer remains
/// pending with backoff; a later file finalization, launch, or background task
/// wakes the service again.
final class ImmediateUploadService {
    static let shared = ImmediateUploadService()

    static func retryDelay(until retryAt: Date, now: Date = Date()) -> TimeInterval {
        // If saving the next deadline fails, the old one can remain overdue.
        // Never turn that persistence failure into a tight network retry loop.
        max(60, retryAt.timeIntervalSince(now))
    }

    private let stateQueue = DispatchQueue(
        label: "com.fitness_exporter.immediateUpload.state"
    )
    private let workerQueue = DispatchQueue(
        label: "com.fitness_exporter.immediateUpload.worker",
        qos: .utility
    )
    private var isDraining = false
    private var restartRequested = false
    private var completions: [(String?) -> Void] = []
    private var retryWork: DispatchWorkItem?
    // Accessed only on workerQueue; shared cooldown survives process restarts.
    private var collectionRetryAt: Date?

    func enqueue(fileURL: URL) throws {
        _ = try ImmediateUploadQueue.withDefault { queue in
            try queue.enqueue(fileURL: fileURL)
        }
    }

    func resume(completion: ((String?) -> Void)? = nil) {
        stateQueue.async { [self] in
            retryWork?.cancel()
            retryWork = nil
            if let completion { completions.append(completion) }
            guard !isDraining else {
                restartRequested = true
                return
            }
            isDraining = true
            workerQueue.async { [weak self] in
                self?.processNext()
            }
        }
    }

    private func processNext() {
        let pending: PendingImmediateUpload
        let fileURL: URL
        do {
            var next: PendingImmediateUpload?
            var resolvedURL: URL?
            try ImmediateUploadQueue.withDefault { queue in
                next = try queue.nextDue()
                if let next {
                    resolvedURL = try queue.fileURL(for: next)
                }
            }
            guard let next, let resolvedURL else {
                return finish(nil)
            }
            pending = next
            fileURL = resolvedURL
        } catch {
            return finish(error.localizedDescription)
        }

        let config: (server: String, sender: String)
        guard let currentConfig = DirectoryUploader.getServerAndSender() else {
            return recordFailure(
                pending,
                message: "Upload server or sender is not configured"
            )
        }
        config = currentConfig
        collectionRetryAt = UploadCollectionRetryStore.shared.nextAttemptAt(
            server: config.server,
            sender: config.sender
        )
        if let collectionRetryAt {
            return finish(
                "Upload retry is scheduled after the server cooldown at \(collectionRetryAt)."
            )
        }

        let directory: UploadDirectory
        do {
            directory = try matchingDirectory(for: fileURL)
        } catch {
            return recordFailure(pending, message: error.localizedDescription)
        }

        let baseURL: URL
        guard let resolvedBase = UploadHelper.resolveURL(
            from: directory.bookmark
        ) else {
            return recordFailure(
                pending,
                message: UploadCoreError.invalidBookmark.description
            )
        }
        baseURL = resolvedBase

        DirectoryUploader.uploadFile(
            file: fileURL,
            baseURL: baseURL,
            baseBookmark: directory.bookmark,
            dirName: directory.name,
            server: config.server,
            sender: config.sender,
            priority: .immediate,
            expectedRecord: pending.record,
            immediateRelativePath: pending.relativePath
        ) { [weak self] failure in
            guard let self else { return }
            self.workerQueue.async {
                if let failure {
                    self.recordFailure(pending, message: failure.message, scope: failure.scope)
                    return
                }
                UploadCollectionRetryStore.shared.clear(server: config.server, sender: config.sender)
                do {
                    try ImmediateUploadQueue.withDefault { queue in
                        let done = try UploadHelper.doneRecord(
                            fileName: pending.record.fileName,
                            base: baseURL
                        )
                        guard done == pending.record else {
                            throw ImmediateUploadQueueError.invalidRecord(
                                "\(pending.relativePath) was accepted without matching completion state"
                            )
                        }
                        try queue.remove(relativePath: pending.relativePath)
                    }
                    self.processNext()
                } catch {
                    self.recordFailure(
                        pending,
                        message: error.localizedDescription
                    )
                }
            }
        }
    }

    /// Re-enters the durable immediate queue when iOS delivered a background
    /// session result after the process that scheduled it was terminated.
    func handleRecoveredBackgroundResult(
        relativePath: String,
        error: String?
    ) {
        workerQueue.async { [weak self] in
            guard let self else { return }
            if let error {
                do {
                    try ImmediateUploadQueue.withDefault { queue in
                        _ = try queue.markFailed(
                            relativePath: relativePath,
                            error: error
                        )
                    }
                } catch ImmediateUploadQueueError.missingRecord {
                    CustomLogger.log(
                        "[Upload][Immediate] Recovered task no longer has a "
                            + "queue row: \(relativePath)"
                    )
                } catch {
                    CustomLogger.log(
                        "[Upload][Immediate][Error] Could not retain recovered "
                            + "result for \(relativePath): \(error.localizedDescription)"
                    )
                }
            }
            self.resume()
        }
    }

    private func matchingDirectory(for fileURL: URL) throws -> UploadDirectory {
        let sourceDirectory = fileURL.deletingLastPathComponent()
            .standardizedFileURL
            .resolvingSymlinksInPath()
        let directories = try UploadDirectoriesStore.loadPersisted()
        for directory in directories {
            guard let candidate = UploadHelper.resolveURL(
                from: directory.bookmark
            ) else { continue }
            if candidate.standardizedFileURL.resolvingSymlinksInPath()
                == sourceDirectory {
                return directory
            }
        }
        throw ImmediateUploadQueueError.invalidSourcePath(
            "no configured upload directory matches \(sourceDirectory.lastPathComponent)"
        )
    }

    private func recordFailure(
        _ pending: PendingImmediateUpload,
        message: String,
        scope: UploadFailureScope = .file
    ) {
        if scope == .collection, let config = DirectoryUploader.getServerAndSender() {
            collectionRetryAt = UploadCollectionRetryStore.shared.recordCircuitOpen(
                server: config.server,
                sender: config.sender
            ).nextAttemptAt
        }
        let finalMessage: String
        do {
            try ImmediateUploadQueue.withDefault { queue in
                _ = try queue.markFailed(
                    relativePath: pending.relativePath,
                    error: message
                )
            }
            finalMessage = message
        } catch {
            finalMessage = message + "; could not retain retry state: "
                + error.localizedDescription
        }
        CustomLogger.log(
            "[Upload][Immediate][Error] file=\(pending.record.fileName) "
                + "err=\(finalMessage)"
        )
        finish(finalMessage)
    }

    private func finish(_ error: String?) {
        stateQueue.async { [self] in
            let callbacks = completions
            completions.removeAll()
            let shouldRestart = restartRequested
            restartRequested = false
            isDraining = shouldRestart
            if shouldRestart {
                workerQueue.async { [weak self] in
                    self?.processNext()
                }
            } else {
                workerQueue.async { [weak self] in
                    self?.scheduleNextRetry()
                }
            }
            DispatchQueue.main.async {
                for callback in callbacks {
                    callback(error)
                }
            }
        }
    }

    private func scheduleNextRetry() {
        do {
            let fileRetryAt = try ImmediateUploadQueue.withDefault { try $0.nextRetryAt() }
            guard let retryAt = [fileRetryAt, collectionRetryAt].compactMap({ $0 }).max() else { return }
            stateQueue.async { [weak self] in
                guard let self, !self.isDraining else { return }
                self.retryWork?.cancel()
                let work = DispatchWorkItem { [weak self] in self?.resume() }
                self.retryWork = work
                self.stateQueue.asyncAfter(
                    deadline: .now() + Self.retryDelay(until: retryAt),
                    execute: work
                )
            }
        } catch {
            CustomLogger.log("[Upload][Immediate][Error] Schedule retry: \(error.localizedDescription)")
        }
    }
}
