import Foundation
import SQLite3

private let backfillSQLiteTransientDestructor = unsafeBitCast(
    -1,
    to: sqlite3_destructor_type.self
)

struct SensorBagBackfillRecord: Codable, Equatable {
    let fileSize: Int64
    let lastModifiedAt: Date
    let completedAt: Date
}

struct SensorBagBackfillResetResult: Equatable {
    let removedRecords: Int
    let warningMessage: String?
}

enum SensorBagBackfillIndexError: LocalizedError {
    case operation(String)

    var errorDescription: String? {
        switch self {
        case .operation(let message):
            return "HealthKit backfill index: \(message)"
        }
    }
}

/// Transactional processing state for importing saved SensorBag files into
/// HealthKit. The database replaces the legacy whole-file JSON index without
/// changing any recording file or server-facing format.
final class SensorBagBackfillIndex {
    static let databaseFileName = ".sensorbag_hk_backfill_index.sqlite3"
    static let legacyFileName = ".sensorbag_hk_backfill_index.json"
    static let resetMarkerFileName = ".sensorbag_hk_backfill_reset_in_progress"

    private static let operationLock = NSLock()

    private let rootURL: URL
    private let databaseURL: URL
    private var database: OpaquePointer?

    static func records(in rootURL: URL) throws -> [String: SensorBagBackfillRecord] {
        try withIndex(in: rootURL, migrateLegacyRecords: true) {
            try $0.records()
        }
    }

    static func markCompleted(
        key: String,
        fileSize: Int64,
        lastModifiedAt: Date,
        completedAt: Date = Date(),
        in rootURL: URL
    ) throws {
        let record = SensorBagBackfillRecord(
            fileSize: fileSize,
            lastModifiedAt: lastModifiedAt,
            completedAt: completedAt
        )
        try withIndex(in: rootURL, migrateLegacyRecords: true) {
            try $0.upsert([key: record])
        }
    }

    static func reset(in rootURL: URL) throws -> SensorBagBackfillResetResult {
        operationLock.lock()
        defer { operationLock.unlock() }

        do {
            try FileManager.default.createDirectory(
                at: rootURL,
                withIntermediateDirectories: true
            )
            try Data().write(
                to: resetMarkerURL(in: rootURL),
                options: .atomic
            )
            return try finishResetLocked(
                in: rootURL,
                reportUnreadableState: true
            )
        } catch let error as SensorBagBackfillIndexError {
            throw error
        } catch {
            throw SensorBagBackfillIndexError.operation(error.localizedDescription)
        }
    }

    static func databaseURL(in rootURL: URL) -> URL {
        rootURL.appendingPathComponent(databaseFileName, isDirectory: false)
    }

    static func legacyURL(in rootURL: URL) -> URL {
        rootURL.appendingPathComponent(legacyFileName, isDirectory: false)
    }

    static func resetMarkerURL(in rootURL: URL) -> URL {
        rootURL.appendingPathComponent(resetMarkerFileName, isDirectory: false)
    }

    private static func withIndex<T>(
        in rootURL: URL,
        migrateLegacyRecords: Bool,
        operation: (SensorBagBackfillIndex) throws -> T
    ) throws -> T {
        operationLock.lock()
        defer { operationLock.unlock() }

        do {
            try FileManager.default.createDirectory(
                at: rootURL,
                withIntermediateDirectories: true
            )
            if FileManager.default.fileExists(
                atPath: resetMarkerURL(in: rootURL).path
            ) {
                _ = try finishResetLocked(
                    in: rootURL,
                    reportUnreadableState: false
                )
            }
            let index = try SensorBagBackfillIndex(
                rootURL: rootURL,
                migrateLegacyRecords: migrateLegacyRecords
            )
            defer { index.closeDatabase() }
            return try operation(index)
        } catch let error as SensorBagBackfillIndexError {
            throw error
        } catch {
            throw SensorBagBackfillIndexError.operation(error.localizedDescription)
        }
    }

    private init(rootURL: URL, migrateLegacyRecords: Bool) throws {
        self.rootURL = rootURL
        databaseURL = Self.databaseURL(in: rootURL)

        do {
            try openDatabase()
            try configureDatabase()
            try createSchema()
            if migrateLegacyRecords {
                try migrateLegacyRecordsIfNeeded()
            }
        } catch {
            closeDatabase()
            throw error
        }
    }

    deinit {
        closeDatabase()
    }

    private var legacyURL: URL {
        Self.legacyURL(in: rootURL)
    }

    private func records() throws -> [String: SensorBagBackfillRecord] {
        let statement = try prepare(
            """
            SELECT file_key, file_size, modified_at, completed_at
            FROM completed_backfills;
            """
        )
        defer { sqlite3_finalize(statement) }

        var result: [String: SensorBagBackfillRecord] = [:]
        while true {
            let step = sqlite3_step(statement)
            if step == SQLITE_DONE {
                return result
            }
            guard step == SQLITE_ROW else {
                throw databaseError(operation: "read records")
            }
            guard let keyBytes = sqlite3_column_text(statement, 0) else {
                throw SensorBagBackfillIndexError.operation(
                    "stored record has an empty key"
                )
            }
            let key = String(cString: keyBytes)
            let record = SensorBagBackfillRecord(
                fileSize: sqlite3_column_int64(statement, 1),
                lastModifiedAt: Date(
                    timeIntervalSinceReferenceDate: sqlite3_column_double(statement, 2)
                ),
                completedAt: Date(
                    timeIntervalSinceReferenceDate: sqlite3_column_double(statement, 3)
                )
            )
            try validate(key: key, record: record, operation: "read record")
            result[key] = record
        }
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
            throw SensorBagBackfillIndexError.operation(
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
            throw SensorBagBackfillIndexError.operation("database is not open")
        }
        guard sqlite3_busy_timeout(database, 5_000) == SQLITE_OK else {
            throw databaseError(operation: "set busy timeout")
        }
        try execute("PRAGMA journal_mode = DELETE;")
        try execute("PRAGMA synchronous = FULL;")
    }

    private func createSchema() throws {
        try execute(
            """
            CREATE TABLE IF NOT EXISTS completed_backfills (
                file_key TEXT PRIMARY KEY NOT NULL,
                file_size INTEGER NOT NULL CHECK(file_size >= 0),
                modified_at REAL NOT NULL,
                completed_at REAL NOT NULL
            ) WITHOUT ROWID;
            """
        )
    }

    /// The JSON file remains the source of truth until every decoded entry is
    /// transactionally committed and read back. An interruption after commit
    /// simply repeats the idempotent migration on the next open.
    private func migrateLegacyRecordsIfNeeded() throws {
        guard FileManager.default.fileExists(atPath: legacyURL.path) else { return }

        let values: URLResourceValues
        do {
            values = try legacyURL.resourceValues(forKeys: [.isRegularFileKey])
        } catch {
            throw operationError("read legacy index metadata", underlying: error)
        }
        guard values.isRegularFile == true else {
            throw SensorBagBackfillIndexError.operation(
                "legacy index is not a regular file"
            )
        }

        let data: Data
        do {
            data = try Data(contentsOf: legacyURL)
        } catch {
            throw operationError("read legacy index", underlying: error)
        }

        let legacyRecords: [String: SensorBagBackfillRecord]
        do {
            legacyRecords = try JSONDecoder().decode(
                [String: SensorBagBackfillRecord].self,
                from: data
            )
        } catch {
            throw operationError("decode legacy index", underlying: error)
        }
        for (key, record) in legacyRecords {
            try validate(key: key, record: record, operation: "migrate legacy record")
        }

        try performTransaction {
            try upsertWithoutTransaction(legacyRecords)
        }

        let committedRecords = try records()
        for (key, legacyRecord) in legacyRecords {
            guard committedRecords[key] == legacyRecord else {
                throw SensorBagBackfillIndexError.operation(
                    "verify migrated record \(key): committed value differs"
                )
            }
        }

        do {
            try FileManager.default.removeItem(at: legacyURL)
        } catch {
            throw operationError("remove verified legacy index", underlying: error)
        }
    }

    private func upsert(_ records: [String: SensorBagBackfillRecord]) throws {
        try performTransaction {
            try upsertWithoutTransaction(records)
        }
    }

    private func upsertWithoutTransaction(
        _ records: [String: SensorBagBackfillRecord]
    ) throws {
        guard !records.isEmpty else { return }
        let statement = try prepare(
            """
            INSERT INTO completed_backfills (
                file_key, file_size, modified_at, completed_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(file_key) DO UPDATE SET
                file_size = excluded.file_size,
                modified_at = excluded.modified_at,
                completed_at = excluded.completed_at;
            """
        )
        defer { sqlite3_finalize(statement) }

        for (key, record) in records {
            try validate(key: key, record: record, operation: "write record")
            sqlite3_reset(statement)
            sqlite3_clear_bindings(statement)
            guard sqlite3_bind_text(
                statement,
                1,
                key,
                -1,
                backfillSQLiteTransientDestructor
            ) == SQLITE_OK,
            sqlite3_bind_int64(statement, 2, record.fileSize) == SQLITE_OK,
            sqlite3_bind_double(
                statement,
                3,
                record.lastModifiedAt.timeIntervalSinceReferenceDate
            ) == SQLITE_OK,
            sqlite3_bind_double(
                statement,
                4,
                record.completedAt.timeIntervalSinceReferenceDate
            ) == SQLITE_OK else {
                throw databaseError(operation: "bind record \(key)")
            }
            guard sqlite3_step(statement) == SQLITE_DONE else {
                throw databaseError(operation: "write record \(key)")
            }
        }
    }

    private func validate(
        key: String,
        record: SensorBagBackfillRecord,
        operation: String
    ) throws {
        guard !key.isEmpty,
              record.fileSize >= 0,
              record.lastModifiedAt.timeIntervalSinceReferenceDate.isFinite,
              record.completedAt.timeIntervalSinceReferenceDate.isFinite else {
            throw SensorBagBackfillIndexError.operation(
                "\(operation): invalid values for \(key.isEmpty ? "<empty>" : key)"
            )
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
            throw SensorBagBackfillIndexError.operation("database is not open")
        }
        var errorMessage: UnsafeMutablePointer<CChar>?
        let result = sqlite3_exec(database, sql, nil, nil, &errorMessage)
        guard result == SQLITE_OK else {
            let message = errorMessage.map { String(cString: $0) }
                ?? String(cString: sqlite3_errmsg(database))
            sqlite3_free(errorMessage)
            throw SensorBagBackfillIndexError.operation(message)
        }
    }

    private func prepare(_ sql: String) throws -> OpaquePointer {
        guard let database else {
            throw SensorBagBackfillIndexError.operation("database is not open")
        }
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(database, sql, -1, &statement, nil) == SQLITE_OK,
              let statement else {
            throw databaseError(operation: "prepare query")
        }
        return statement
    }

    private func databaseError(operation: String) -> SensorBagBackfillIndexError {
        let message = database.map { String(cString: sqlite3_errmsg($0)) }
            ?? "database is not open"
        return .operation("\(operation): \(message)")
    }

    private func operationError(
        _ operation: String,
        underlying: Error
    ) -> SensorBagBackfillIndexError {
        .operation("\(operation): \(underlying.localizedDescription)")
    }

    /// Reset is file-based rather than schema-based so it remains a recovery
    /// operation even when the existing SQLite file cannot be opened. The
    /// marker is removed only after a fresh empty database is durable.
    private static func finishResetLocked(
        in rootURL: URL,
        reportUnreadableState: Bool
    ) throws -> SensorBagBackfillResetResult {
        let fileManager = FileManager.default
        let legacyURL = legacyURL(in: rootURL)
        let databaseURL = databaseURL(in: rootURL)
        let markerURL = resetMarkerURL(in: rootURL)
        var removedKeys: Set<String> = []
        var warnings: [String] = []

        if fileManager.fileExists(atPath: legacyURL.path) {
            do {
                let data = try Data(contentsOf: legacyURL)
                let records = try JSONDecoder().decode(
                    [String: SensorBagBackfillRecord].self,
                    from: data
                )
                for (key, record) in records {
                    try validateResetRecord(key: key, record: record)
                }
                removedKeys.formUnion(records.keys)
            } catch {
                if reportUnreadableState {
                    warnings.append(
                        "Unreadable legacy JSON was intentionally cleared: "
                            + error.localizedDescription
                    )
                }
            }
        }

        if fileManager.fileExists(atPath: databaseURL.path) {
            do {
                let index = try SensorBagBackfillIndex(
                    rootURL: rootURL,
                    migrateLegacyRecords: false
                )
                defer { index.closeDatabase() }
                removedKeys.formUnion(try index.records().keys)
            } catch {
                if reportUnreadableState {
                    warnings.append(
                        "Unreadable SQLite state was intentionally cleared: "
                            + error.localizedDescription
                    )
                }
            }
        }

        if fileManager.fileExists(atPath: legacyURL.path) {
            do {
                try fileManager.removeItem(at: legacyURL)
            } catch {
                throw SensorBagBackfillIndexError.operation(
                    "remove legacy index during reset: \(error.localizedDescription)"
                )
            }
        }

        for url in databaseArtifactURLs(databaseURL: databaseURL) {
            guard fileManager.fileExists(atPath: url.path) else { continue }
            do {
                try fileManager.removeItem(at: url)
            } catch {
                throw SensorBagBackfillIndexError.operation(
                    "remove \(url.lastPathComponent) during reset: "
                        + error.localizedDescription
                )
            }
        }

        let freshIndex = try SensorBagBackfillIndex(
            rootURL: rootURL,
            migrateLegacyRecords: false
        )
        freshIndex.closeDatabase()

        do {
            try fileManager.removeItem(at: markerURL)
        } catch {
            throw SensorBagBackfillIndexError.operation(
                "finish reset: \(error.localizedDescription)"
            )
        }
        return SensorBagBackfillResetResult(
            removedRecords: removedKeys.count,
            warningMessage: warnings.isEmpty
                ? nil
                : warnings.joined(separator: " | ")
        )
    }

    private static func databaseArtifactURLs(databaseURL: URL) -> [URL] {
        [
            databaseURL,
            URL(fileURLWithPath: databaseURL.path + "-journal"),
            URL(fileURLWithPath: databaseURL.path + "-wal"),
            URL(fileURLWithPath: databaseURL.path + "-shm"),
        ]
    }

    private static func validateResetRecord(
        key: String,
        record: SensorBagBackfillRecord
    ) throws {
        guard !key.isEmpty,
              record.fileSize >= 0,
              record.lastModifiedAt.timeIntervalSinceReferenceDate.isFinite,
              record.completedAt.timeIntervalSinceReferenceDate.isFinite else {
            throw SensorBagBackfillIndexError.operation(
                "legacy reset state contains invalid values"
            )
        }
    }
}
