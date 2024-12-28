//
//  CustomLogger.swift
//  fitness_exporter
//
//  Created by Artem Zinchenko on 12/22/24.
//

import Foundation

class CustomLogger {
    // Maximum number of logs to keep
    private static let maxLogs = 500

    // Internal queue for thread safety
    private static let queue = DispatchQueue(label: "com.artemz.fitness-exporter.logger.queue", attributes: .concurrent)

    // Key for persistence
    private static let userDefaultsKey = "CustomLoggerLogs"

    // Circular buffer for in-memory logs
    private static var logs: [(timestamp: Date, message: String)] = loadPersistedLogs()

    // Log a message with a timestamp
    static func log(_ message: String) {
        let timestamp = Date()
        queue.async(flags: .barrier) {
            print(timestamp, message)
            // Add new log
            logs.append((timestamp: timestamp, message: message))

            // Trim logs to keep only the last maxLogs
            if logs.count > maxLogs {
                logs.removeFirst(logs.count - maxLogs)
            }

            // Persist logs
            persistLogs()
        }
    }

    // Retrieve all logs as [(timestamp, message)]
    static func retrieveLogs() -> [(Date, String)] {
        return queue.sync {
            return logs
        }
    }

    // Private helper to persist logs to UserDefaults
    private static func persistLogs() {
        let serializedLogs = logs.map { log in
            ["timestamp": log.timestamp.timeIntervalSince1970, "message": log.message]
        }
        UserDefaults.standard.set(serializedLogs, forKey: userDefaultsKey)
    }

    // Private helper to load persisted logs from UserDefaults
    private static func loadPersistedLogs() -> [(Date, String)] {
        guard let serializedLogs = UserDefaults.standard.array(forKey: userDefaultsKey) as? [[String: Any]] else {
            return []
        }
        return serializedLogs.compactMap { logDict in
            guard let timestamp = logDict["timestamp"] as? TimeInterval,
                  let message = logDict["message"] as? String else {
                return nil
            }
            return (timestamp: Date(timeIntervalSince1970: timestamp), message: message)
        }
    }
}
