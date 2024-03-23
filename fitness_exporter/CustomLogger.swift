import Foundation

final class CustomLogger {
    private static let PAGE_SIZE = 400
    private static let MAX_PAGES = 100

    private static let logsDirectoryURL: URL = {
        let urls = FileManager.default.urls(
            for: .documentDirectory, in: .userDomainMask)
        let logsDir = urls[0].appendingPathComponent("HKExportLogs")
        try? FileManager.default.createDirectory(
            at: logsDir, withIntermediateDirectories: true)
        return logsDir
    }()

    private struct LogEntry: Codable {
        let index: Int
        let date: Date
        let message: String
    }

    private static let loggerQueue = DispatchQueue(
        label: "com.yourorg.customlogger.queue",
        attributes: .concurrent
    )

    private static var inMemoryLogs: [LogEntry] = []

    public static func log(_ message: String) {
        let date = Date()
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
        print("\(formatter.string(from: date)): \(message)")

        loggerQueue.async(flags: .barrier) {
            loadStateIfNeeded()

            let newEntry = LogEntry(
                index: getNextLogIndex(), date: date, message: message)
            inMemoryLogs.append(newEntry)

            writeToDisk()
            writeToUserDefaults()
        }
    }

    public static func getNumberOfLogsAvailable() -> Int {
        loggerQueue.sync {
            loadStateIfNeeded()
            return min(MAX_PAGES * PAGE_SIZE, getNextLogIndex())
        }
    }

    public static func retrieveLogs(maxLogs: Int, skip: Int) -> [(Date, String)]
    {
        loggerQueue.sync {
            loadStateIfNeeded()

            if inMemoryLogs.isEmpty {
                return []
            }

            var maxLogs = min(maxLogs, PAGE_SIZE * MAX_PAGES, getNextLogIndex())
            var skip = skip

            var results: [LogEntry] = []
            results.reserveCapacity(maxLogs)

            let inMemoryPageIdx = inMemoryLogs.first!.index / PAGE_SIZE
            var pageIdx = inMemoryPageIdx
            while maxLogs > 0 {
                let pageSize =
                    pageIdx == inMemoryPageIdx ? inMemoryLogs.count : PAGE_SIZE
                if skip >= pageSize {
                    skip -= pageSize
                } else {
                    let page =
                        pageIdx == inMemoryPageIdx
                        ? inMemoryLogs : readLogsFromDiskSafe(page: pageIdx)

                    let startIndex = max(0, page.count - skip - maxLogs)
                    let endIndex = page.count - skip
                    results.append(
                        contentsOf: page[startIndex..<endIndex].reversed())
                    maxLogs -= endIndex - startIndex
                    skip = 0
                }
                pageIdx -= 1
            }

            return results.map { ($0.date, $0.message) }
        }
    }

    public static func clearLogs() {
        loggerQueue.async(flags: .barrier) {
            var numLogs = getNextLogIndex()

            UserDefaults.standard.removeObject(
                forKey: UserDefaultsKeys.CUSTOM_LOGGER_LOGS)
            inMemoryLogs.removeAll()

            for page in (0..<((numLogs - 1) / PAGE_SIZE)) {
                let url = fileURL(forPage: page)
                try? FileManager.default.removeItem(at: url)
                numLogs -= PAGE_SIZE
            }
        }
    }

    private static func writeToDisk() {
        while inMemoryLogs.count > PAGE_SIZE {
            let pageIndex = inMemoryLogs[0].index / PAGE_SIZE
            let pageFileURL = fileURL(forPage: pageIndex)

            let page = Array(inMemoryLogs[..<PAGE_SIZE])
            inMemoryLogs = Array(inMemoryLogs[PAGE_SIZE...])

            if let data = try? JSONEncoder().encode(page) {
                do {
                    try data.write(to: pageFileURL, options: .atomicWrite)
                } catch {
                    print("Can't write to file \(pageFileURL): \(error)")
                }
            } else {
                print("Can't encode logs to JSON: \(inMemoryLogs)")
            }

            writeToUserDefaults()
        }
    }

    private static func writeToUserDefaults() {
        let defaults = UserDefaults.standard
        let entries = inMemoryLogs.map {
            [
                "index": $0.index,
                "date": $0.date.timeIntervalSince1970,
                "message": $0.message,
            ]
        }
        defaults.set(entries, forKey: UserDefaultsKeys.CUSTOM_LOGGER_LOGS)
    }

    private static var didLoadState = false
    private static func loadStateIfNeeded() {
        guard !didLoadState else { return }
        didLoadState = true

        let defaults = UserDefaults.standard

        if let savedArray = defaults.array(
            forKey: UserDefaultsKeys.CUSTOM_LOGGER_LOGS) as? [[String: Any]]
        {
            inMemoryLogs = savedArray.compactMap { dict -> LogEntry? in
                guard
                    let index = dict["index"] as? Int,
                    let dateTS = dict["date"] as? TimeInterval,
                    let message = dict["message"] as? String
                else {
                    return nil
                }
                return LogEntry(
                    index: index, date: Date(timeIntervalSince1970: dateTS),
                    message: message)
            }
        }
    }

    private static func getNextLogIndex() -> Int {
        loadStateIfNeeded()

        return inMemoryLogs.isEmpty ? 0 : inMemoryLogs.last!.index + 1
    }

    private static func readLogsFromDiskSafe(page: Int) -> [LogEntry] {
        guard let logs = readLogsFromDisk(page: page) else {
            return (page * PAGE_SIZE..<(page + 1) * PAGE_SIZE).map {
                LogEntry(
                    index: $0, date: Date.distantPast, message: "Logging error")
            }
        }
        return logs

    }
    private static func readLogsFromDisk(page: Int) -> [LogEntry]? {
        let url = fileURL(forPage: page)
        guard let data = try? Data(contentsOf: url) else {
            print("Can't read logs from \(url)")
            return nil
        }
        do {
            return try JSONDecoder().decode([LogEntry].self, from: data)
        } catch {
            print("Cant decode logs from \(url): \(error)")
            return nil
        }
    }

    private static func fileURL(forPage page: Int) -> URL {
        let fileSuffix = page % (MAX_PAGES + 1)
        return logsDirectoryURL.appendingPathComponent(
            "logs_\(String(format: "%07d", fileSuffix)).json")
    }
}
