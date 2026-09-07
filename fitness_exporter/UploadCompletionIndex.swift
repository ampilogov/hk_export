import Foundation
import SQLite3

private let sqliteTransientDestructor = unsafeBitCast(
    -1,
    to: sqlite3_destructor_type.self
)

/// Transactional replacement for the per-recording JSON files formerly kept
/// in `.done`. The database lives beside those legacy records so completion
/// state follows the selected recording directory.
final class UploadCompletionIndex {
    static let databaseFileName = "upload-index.sqlite3"
    private static let resetMarkerFileName = "reset-in-progress"

    private static let operationLock = NSLock()

    private let doneDirectoryURL: URL
    private let databaseURL: URL
    private let shouldCancel: () -> Bool
    private var database: OpaquePointer?

    static func records(
        in baseURL: URL,
        shouldCancel: @escaping () -> Bool = { false }
    ) throws -> [String: UploadDoneRecord] {
        try withCoordinatedIndex(
            in: baseURL,
            migrateLegacyRecords: true,
            shouldCancel: shouldCancel
        ) {
            try $0.records()
        }
    }

    @discardableResult
    static func markDone(file: URL, in baseURL: URL) throws -> UploadDoneRecord {
        try withCoordinatedIndex(
            in: baseURL,
            migrateLegacyRecords: true,
            shouldCancel: { false }
        ) {
            try $0.markDone(file: file)
        }
    }

    static func reset(in baseURL: URL) throws -> Int {
        try withCoordinatedIndex(
            in: baseURL,
            migrateLegacyRecords: false,
            shouldCancel: { false }
        ) {
            try $0.reset()
        }
    }

    static func indexURL(in baseURL: URL) -> URL {
        baseURL
            .appendingPathComponent(".done", isDirectory: true)
            .appendingPathComponent(databaseFileName, isDirectory: false)
    }

    private static func withCoordinatedIndex<T>(
        in baseURL: URL,
        migrateLegacyRecords: Bool,
        shouldCancel: @escaping () -> Bool,
        operation: (UploadCompletionIndex) throws -> T
    ) throws -> T {
        operationLock.lock()
        defer { operationLock.unlock() }

        let requestedDoneURL = baseURL.appendingPathComponent(
            ".done",
            isDirectory: true
        )
        let coordinator = NSFileCoordinator(filePresenter: nil)
        var coordinationError: NSError?
        var result: Result<T, Error>?

        coordinator.coordinate(
            writingItemAt: requestedDoneURL,
            options: [],
            error: &coordinationError
        ) { coordinatedDoneURL in
            result = Result {
                try FileManager.default.createDirectory(
                    at: coordinatedDoneURL,
                    withIntermediateDirectories: true
                )
                let index = try UploadCompletionIndex(
                    doneDirectoryURL: coordinatedDoneURL,
                    migrateLegacyRecords: migrateLegacyRecords,
                    shouldCancel: shouldCancel
                )
                defer { index.closeDatabase() }
                return try operation(index)
            }
        }

        if let coordinationError {
            throw UploadCoreError.completionState(
                "Coordinate upload history: "
                    + coordinationError.localizedDescription
            )
        }
        guard let result else {
            throw UploadCoreError.completionState(
                "Coordinate upload history: no coordinated URL was provided"
            )
        }
        return try result.get()
    }

    private init(
        doneDirectoryURL: URL,
        migrateLegacyRecords: Bool,
        shouldCancel: @escaping () -> Bool
    ) throws {
        self.doneDirectoryURL = doneDirectoryURL
        self.shouldCancel = shouldCancel
        databaseURL = doneDirectoryURL.appendingPathComponent(
            Self.databaseFileName,
            isDirectory: false
        )

        do {
            try throwIfCancelled()
            try verifyDatabaseIsLocallyAvailable()
            try openDatabase()
            try configureDatabase()
            try createSchema()
            if FileManager.default.fileExists(atPath: resetMarkerURL.path) {
                _ = try finishReset()
            }
            if migrateLegacyRecords {
                try migrateLegacyRecordsIfNeeded()
            }
        } catch {
            closeDatabase()
            if let uploadError = error as? UploadCoreError {
                throw uploadError
            }
            throw UploadCoreError.completionState(error.localizedDescription)
        }
    }

    deinit {
        closeDatabase()
    }

    private var resetMarkerURL: URL {
        doneDirectoryURL.appendingPathComponent(
            Self.resetMarkerFileName,
            isDirectory: false
        )
    }

    private func verifyDatabaseIsLocallyAvailable() throws {
        guard FileManager.default.fileExists(atPath: databaseURL.path) else {
            return
        }
        let values: URLResourceValues
        do {
            values = try databaseURL.resourceValues(
                forKeys: [
                    .isRegularFileKey,
                    .isUbiquitousItemKey,
                    .ubiquitousItemDownloadingStatusKey,
                ]
            )
        } catch {
            throw UploadCoreError.completionState(
                "Read \(databaseURL.lastPathComponent) availability: "
                    + error.localizedDescription
            )
        }
        guard values.isRegularFile == true else {
            throw UploadCoreError.completionState(
                "\(databaseURL.lastPathComponent) is not a regular file"
            )
        }
        if values.isUbiquitousItem == true,
           values.ubiquitousItemDownloadingStatus
            != URLUbiquitousItemDownloadingStatus.current {
            throw UploadCoreError.completionState(
                "\(databaseURL.lastPathComponent) is not locally available"
            )
        }
    }

    func records() throws -> [String: UploadDoneRecord] {
        let sql = """
            SELECT file_name, file_size, modified_at
            FROM completed_uploads;
            """
        let statement = try prepare(sql)
        defer { sqlite3_finalize(statement) }

        var records: [String: UploadDoneRecord] = [:]
        while true {
            try throwIfCancelled()
            let result = sqlite3_step(statement)
            if result == SQLITE_DONE {
                return records
            }
            guard result == SQLITE_ROW else {
                throw databaseError(operation: "read completion index")
            }
            guard let fileNameBytes = sqlite3_column_text(statement, 0) else {
                throw UploadCoreError.completionState(
                    "Completion index contains an empty filename"
                )
            }

            let fileName = String(cString: fileNameBytes)
            let record = UploadDoneRecord(
                fileName: fileName,
                fileSize: sqlite3_column_int64(statement, 1),
                lastModifiedAt: Date(
                    timeIntervalSinceReferenceDate: sqlite3_column_double(
                        statement,
                        2
                    )
                )
            )
            records[fileName] = record
        }
    }

    @discardableResult
    func markDone(file: URL) throws -> UploadDoneRecord {
        let values: URLResourceValues
        do {
            values = try file.resourceValues(
                forKeys: [.fileSizeKey, .contentModificationDateKey]
            )
        } catch {
            throw UploadCoreError.completionState(
                "Read metadata for \(file.lastPathComponent): "
                    + error.localizedDescription
            )
        }
        guard let fileSize = values.fileSize,
              let modifiedAt = values.contentModificationDate else {
            throw UploadCoreError.completionState(
                "Missing size or modification date for \(file.lastPathComponent)"
            )
        }

        let record = UploadDoneRecord(
            fileName: file.lastPathComponent,
            fileSize: Int64(fileSize),
            lastModifiedAt: modifiedAt
        )
        try performTransaction {
            try upsert([record])
        }
        return record
    }

    private func openDatabase() throws {
        var handle: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        let result = sqlite3_open_v2(databaseURL.path, &handle, flags, nil)
        guard result == SQLITE_OK, let handle else {
            let message = handle.map { String(cString: sqlite3_errmsg($0)) }
                ?? "SQLite did not return a database handle"
            if let handle {
                sqlite3_close(handle)
            }
            throw UploadCoreError.completionState(
                "Open \(databaseURL.lastPathComponent): \(message)"
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
            throw UploadCoreError.completionState("Database is not open")
        }
        guard sqlite3_busy_timeout(database, 5_000) == SQLITE_OK else {
            throw databaseError(operation: "set completion-index busy timeout")
        }
        try execute("PRAGMA journal_mode = DELETE;")
        try execute("PRAGMA synchronous = FULL;")
        try execute("PRAGMA foreign_keys = ON;")
    }

    private func createSchema() throws {
        try execute(
            """
            CREATE TABLE IF NOT EXISTS completed_uploads (
                file_name TEXT PRIMARY KEY NOT NULL,
                file_size INTEGER NOT NULL CHECK(file_size >= 0),
                modified_at REAL NOT NULL
            ) WITHOUT ROWID;
            """
        )
    }

    /// Legacy sidecars are the source of truth until every record has been
    /// committed and verified. A failed read leaves every sidecar untouched.
    /// If deletion is interrupted, the remaining sidecars are safely imported
    /// and verified again on the next open.
    private func migrateLegacyRecordsIfNeeded() throws {
        let sidecars = try legacySidecarURLs()
        guard !sidecars.isEmpty else { return }

        var legacyRecords: [String: UploadDoneRecord] = [:]
        for sidecar in sidecars {
            try throwIfCancelled()
            let values: URLResourceValues
            do {
                values = try sidecar.resourceValues(
                    forKeys: [
                        .isRegularFileKey,
                        .isUbiquitousItemKey,
                        .ubiquitousItemDownloadingStatusKey,
                    ]
                )
            } catch {
                throw migrationError(
                    sidecar,
                    operation: "Read file availability",
                    underlying: error
                )
            }
            guard values.isRegularFile == true else {
                throw UploadCoreError.completionState(
                    "Migrate \(sidecar.lastPathComponent): not a regular file"
                )
            }
            if values.isUbiquitousItem == true,
               values.ubiquitousItemDownloadingStatus
                != URLUbiquitousItemDownloadingStatus.current {
                throw UploadCoreError.completionState(
                    "Migrate \(sidecar.lastPathComponent): iCloud file is not locally available"
                )
            }

            let data: Data
            do {
                data = try Data(contentsOf: sidecar)
            } catch {
                throw migrationError(
                    sidecar,
                    operation: "Read",
                    underlying: error
                )
            }

            let record: UploadDoneRecord
            do {
                record = try JSONDecoder().decode(
                    UploadDoneRecord.self,
                    from: data
                )
            } catch {
                throw migrationError(
                    sidecar,
                    operation: "Decode",
                    underlying: error
                )
            }

            guard sidecar.lastPathComponent == "\(record.fileName).json" else {
                throw UploadCoreError.completionState(
                    "Migrate \(sidecar.lastPathComponent): record names \(record.fileName)"
                )
            }
            guard !record.fileName.isEmpty,
                  record.fileSize >= 0,
                  record.lastModifiedAt.timeIntervalSinceReferenceDate.isFinite else {
                throw UploadCoreError.completionState(
                    "Migrate \(sidecar.lastPathComponent): invalid record values"
                )
            }
            if let duplicate = legacyRecords[record.fileName], duplicate != record {
                throw UploadCoreError.completionState(
                    "Migrate \(sidecar.lastPathComponent): conflicting duplicate record"
                )
            }
            legacyRecords[record.fileName] = record
        }

        try performTransaction {
            try upsert(Array(legacyRecords.values))
        }

        let committedRecords = try records()
        for (fileName, legacyRecord) in legacyRecords {
            guard committedRecords[fileName] == legacyRecord else {
                throw UploadCoreError.completionState(
                    "Verify migrated record for \(fileName): committed value differs"
                )
            }
        }

        for sidecar in sidecars {
            do {
                try FileManager.default.removeItem(at: sidecar)
            } catch {
                throw migrationError(
                    sidecar,
                    operation: "Remove verified legacy record",
                    underlying: error
                )
            }
        }
    }

    private func legacySidecarURLs() throws -> [URL] {
        let keys: [URLResourceKey] = [
            .isRegularFileKey,
            .isUbiquitousItemKey,
            .ubiquitousItemDownloadingStatusKey,
        ]
        do {
            return try FileManager.default.contentsOfDirectory(
                at: doneDirectoryURL,
                includingPropertiesForKeys: keys,
                options: []
            )
            .filter { $0.pathExtension.lowercased() == "json" }
            .sorted { $0.lastPathComponent < $1.lastPathComponent }
        } catch {
            throw UploadCoreError.completionState(
                "List legacy records: \(error.localizedDescription)"
            )
        }
    }

    private func reset() throws -> Int {
        do {
            try Data().write(to: resetMarkerURL, options: .atomic)
        } catch {
            throw UploadCoreError.completionState(
                "Begin upload-history reset: \(error.localizedDescription)"
            )
        }
        return try finishReset()
    }

    /// A durable marker makes an interrupted reset resume before legacy
    /// records can be imported again. Database rows remain authoritative until
    /// every legacy sidecar has been removed.
    private func finishReset() throws -> Int {
        let legacySidecars = try legacySidecarURLs()
        for sidecar in legacySidecars {
            do {
                try FileManager.default.removeItem(at: sidecar)
            } catch {
                throw migrationError(
                    sidecar,
                    operation: "Reset legacy completion record",
                    underlying: error
                )
            }
        }

        var removedDatabaseRecords = 0
        try performTransaction {
            try execute("DELETE FROM completed_uploads;")
            guard let database else {
                throw UploadCoreError.completionState("Database is not open")
            }
            removedDatabaseRecords = Int(sqlite3_changes(database))
        }
        do {
            try FileManager.default.removeItem(at: resetMarkerURL)
        } catch {
            throw UploadCoreError.completionState(
                "Finish upload-history reset: \(error.localizedDescription)"
            )
        }
        return removedDatabaseRecords + legacySidecars.count
    }

    private func upsert(_ records: [UploadDoneRecord]) throws {
        guard !records.isEmpty else { return }
        let sql = """
            INSERT INTO completed_uploads (file_name, file_size, modified_at)
            VALUES (?, ?, ?)
            ON CONFLICT(file_name) DO UPDATE SET
                file_size = excluded.file_size,
                modified_at = excluded.modified_at;
            """
        let statement = try prepare(sql)
        defer { sqlite3_finalize(statement) }

        for record in records {
            sqlite3_reset(statement)
            sqlite3_clear_bindings(statement)
            guard sqlite3_bind_text(
                statement,
                1,
                record.fileName,
                -1,
                sqliteTransientDestructor
            ) == SQLITE_OK,
            sqlite3_bind_int64(statement, 2, record.fileSize) == SQLITE_OK,
            sqlite3_bind_double(
                statement,
                3,
                record.lastModifiedAt.timeIntervalSinceReferenceDate
            ) == SQLITE_OK else {
                throw databaseError(operation: "bind completion record")
            }
            guard sqlite3_step(statement) == SQLITE_DONE else {
                throw databaseError(
                    operation: "write completion record for \(record.fileName)"
                )
            }
        }
    }

    private func performTransaction(_ body: () throws -> Void) throws {
        try execute("BEGIN IMMEDIATE;")
        do {
            try body()
            try execute("COMMIT;")
        } catch {
            try? execute("ROLLBACK;")
            throw error
        }
    }

    private func execute(_ sql: String) throws {
        guard let database else {
            throw UploadCoreError.completionState("Database is not open")
        }
        var errorMessage: UnsafeMutablePointer<CChar>?
        let result = sqlite3_exec(database, sql, nil, nil, &errorMessage)
        guard result == SQLITE_OK else {
            let message = errorMessage.map { String(cString: $0) }
                ?? String(cString: sqlite3_errmsg(database))
            sqlite3_free(errorMessage)
            throw UploadCoreError.completionState(message)
        }
    }

    private func prepare(_ sql: String) throws -> OpaquePointer {
        guard let database else {
            throw UploadCoreError.completionState("Database is not open")
        }
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(database, sql, -1, &statement, nil)
            == SQLITE_OK,
            let statement else {
            throw databaseError(operation: "prepare completion-index query")
        }
        return statement
    }

    private func databaseError(operation: String) -> UploadCoreError {
        let message = database.map { String(cString: sqlite3_errmsg($0)) }
            ?? "Database is not open"
        return .completionState("\(operation): \(message)")
    }

    private func migrationError(
        _ sidecar: URL,
        operation: String,
        underlying: Error
    ) -> UploadCoreError {
        .completionState(
            "\(operation) \(sidecar.lastPathComponent): "
                + underlying.localizedDescription
        )
    }

    private func throwIfCancelled() throws {
        if shouldCancel() {
            throw UploadCoreError.cancelled
        }
    }
}
