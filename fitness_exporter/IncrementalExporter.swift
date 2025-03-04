import Foundation
import HealthKit

class KeyBasedLock {
    private protocol LockType {
        func canLock(lock: KeyBasedLock) -> Bool
        func setTTL(lock: KeyBasedLock, date: Date)
        func removeLocks(lock: KeyBasedLock)
    }

    private class TotalLock: LockType {
        func canLock(lock: KeyBasedLock) -> Bool {
            if lock.total != nil || !lock.locks.isEmpty {
                return false
            }
            return true
        }

        func setTTL(lock: KeyBasedLock, date: Date) {
            lock.total = date
        }

        func removeLocks(lock: KeyBasedLock) {
            lock.total = nil
        }
    }

    private class KeyLock: LockType {
        private let keys: [HKSampleType]

        init(keys: [HKSampleType]) {
            self.keys = keys
        }

        internal func canLock(lock: KeyBasedLock) -> Bool {
            if lock.total != nil {
                return false
            }
            for key in keys {
                if lock.locks.keys.contains(key) {
                    return false
                }
            }
            return true
        }

        internal func setTTL(lock: KeyBasedLock, date: Date) {
            for key in keys {
                lock.locks[key] = date
            }
        }

        internal func removeLocks(lock: KeyBasedLock) {
            for key in keys {
                lock.locks.removeValue(forKey: key)
            }
        }
    }

    protocol LockToken {
        func extendTTL()
        func unlock()
    }

    private class LockToken_: LockToken {
        private let owner: KeyBasedLock
        private let type: LockType

        init(owner: KeyBasedLock, type: LockType) {
            self.owner = owner
            self.type = type
        }

        func extendTTL() {
            owner.lock.lock()
            defer { owner.lock.unlock() }

            type.setTTL(lock: owner, date: Date())
        }

        func unlock() {
            owner.lock.lock()
            defer { owner.lock.unlock() }

            type.removeLocks(lock: owner)
        }
    }

    private var locks: [HKSampleType: Date] = [:]
    private var total: Date? = nil
    private let lock = NSLock()
    private let ttl: TimeInterval

    init(ttl: TimeInterval) {
        self.ttl = ttl
    }

    func tryLock() -> LockToken? {
        return tryLock_(TotalLock())
    }

    func tryLock(keys: [HKSampleType]) -> LockToken? {
        return tryLock_(KeyLock(keys: keys))
    }

    private func tryLock_(_ type: LockType) -> LockToken? {
        lock.lock()
        defer { lock.unlock() }

        let now = Date()

        cleanStaleLocks(now)

        if type.canLock(lock: self) {
            type.setTTL(lock: self, date: now)
            return LockToken_(owner: self, type: type)
        } else {
            return nil
        }
    }

    private func cleanStaleLocks(_ now: Date) {
        if let total_ = total, total_ + ttl < now {
            total = nil
        }
        for (key, value) in Array(locks) {
            if value + ttl < now {
                locks.removeValue(forKey: key)
            }
        }
    }
}

final class IncrementalExporter {
    private static let LOCK = KeyBasedLock(ttl: 60)

    private static let EPS: TimeInterval = 600
    private static let DEFAULT_START_DATE: Date = Calendar.current.date(
        from: DateComponents(year: 2001, month: 1, day: 1))!
    private static let TIME_TO_FINALIZE: TimeInterval = 3 * 24 * 60 * 60

    private static let USER_DEFAULTS_KEY_PREFIX =
        "IncrementalExporter_LastExportTime_" + HealthDataExporter.VERSION + "_"

    init() {}

    static func resetCursors() {
        if let token = IncrementalExporter.LOCK.tryLock() {
            defer { token.unlock() }

            CustomLogger.log("[IE][Warning] Resetting cursors")

            let keys = UserDefaults.standard.dictionaryRepresentation().keys

            for key in keys
            where key.hasPrefix(IncrementalExporter.USER_DEFAULTS_KEY_PREFIX) {
                UserDefaults.standard.removeObject(forKey: key)
            }

            UserDefaults.standard.synchronize()

        } else {
            CustomLogger.log("[IE][Error] Can't aquire lock for reset cursors")
        }
    }

    static func getCursors(
        sampleTypes: [HKSampleType]
    ) -> [HKSampleType: Date?]? {
        if let token = IncrementalExporter.LOCK.tryLock() {
            defer { token.unlock() }

            return Dictionary(
                uniqueKeysWithValues: sampleTypes.map {
                    ($0, IncrementalExporter.getLastExportTime($0))
                })
        } else {
            CustomLogger.log("[IE][Error] Can't aquire lock for get cursors")
            return nil
        }
    }

    func run(
        sampleTypes: [HKSampleType], batchSize: TimeInterval,
        completion: @escaping (String?) -> Void
    ) {
        let sampleTypesDescr =
            sampleTypes.count > 3
            ? "\(sampleTypes[0..<3])".replacingOccurrences(
                of: "]", with: "...]") : "\(sampleTypes)"
        if let token = IncrementalExporter.LOCK.tryLock(keys: sampleTypes) {
            CustomLogger.log(
                "[IE][Info] \(sampleTypesDescr), aquired the lock and starting export"
            )
            self.runUnlocked(
                sampleTypes: sampleTypes,
                batchSize: batchSize,
                token: token
            ) {
                result in
                token.unlock()
                CustomLogger.log(
                    "[IE][\(result == nil ? "Success" : "Error")] \(sampleTypesDescr), released the lock and finished export with status: \(result ?? "OK")"
                )
                return completion(result)
            }
        } else {
            return completion(
                "[IE][Error] Can't aquire lock for \(sampleTypesDescr)")
        }
    }

    private func runUnlocked(
        sampleTypes: [HKSampleType], batchSize: TimeInterval,
        token: KeyBasedLock.LockToken,
        completion: @escaping (String?) -> Void
    ) {
        IncrementalExporter.getServerURL {
            status, serverURL in
            if status != nil {
                return completion(status)
            }

            let exporter = HealthDataExporter(
                server: serverURL!,
                sender: UserDefaults.standard.string(
                    forKey: UserDefaultsKeys.SENDER) ?? ""
            )
            self.export(
                exporter: exporter, sampleTypes: sampleTypes,
                batchSize: batchSize, token: token, completion: completion)
        }
    }

    private func export(
        exporter: HealthDataExporter,
        sampleTypes: [HKSampleType], batchSize: TimeInterval,
        token: KeyBasedLock.LockToken,
        completion: @escaping (String?) -> Void
    ) {
        // CustomLogger.log("Running incremental export")

        let queue = DispatchQueue(
            label: "com.fitness_exporter.incrementalQueue")

        var index = 0

        func processNext() {
            queue.async {
                if index == sampleTypes.count {
                    //                    CustomLogger.log(
                    //                        "[IE] Finished running incremental export, success \(index)/\(sampleTypes.count)"
                    //                    )
                    return completion(nil)
                }

                self.exportSampleType(
                    exporter, sampleTypes[index], batchSize, token
                ) {
                    status in
                    if let status = status {
                        return completion(status)
                    }
                    index += 1
                    return processNext()
                }

            }
        }

        return processNext()
    }

    private func exportSampleType(
        _ exporter: HealthDataExporter,
        _ sampleType: HKSampleType, _ batchSize: TimeInterval,
        _ token: KeyBasedLock.LockToken,
        completion: @escaping (String?) -> Void
    ) {
        let queue = DispatchQueue(
            label: "com.fitness_exporter.incrementalSampleQueue")

        var lastExportTime =
            IncrementalExporter.getLastExportTime(sampleType)
            ?? IncrementalExporter.DEFAULT_START_DATE
        let now = Date()
        CustomLogger.log(
            "[IE][Info] \(sampleType), last export time: \(lastExportTime), now: \(now)"
        )

        func processNext() {
            queue.async {
                if lastExportTime >= now {
                    return completion(nil)
                }

                let from = lastExportTime - IncrementalExporter.EPS
                let to = lastExportTime + batchSize + IncrementalExporter.EPS
                exporter.export(sampleType: sampleType, from: from, to: to) {
                    status in
                    if let status = status {
                        return completion(status)
                    }
                    token.extendTTL()
                    lastExportTime += batchSize
                    let newLastExportTime = min(
                        lastExportTime,
                        now - IncrementalExporter.TIME_TO_FINALIZE)
                    CustomLogger.log(
                        "[IE][Success] \(sampleType), updating last export time: \(newLastExportTime)"
                    )
                    IncrementalExporter.setLastExportTime(
                        sampleType, newLastExportTime)
                    return processNext()
                }

            }
        }

        return processNext()
    }

    private static func getLastExportTime(_ sampleType: HKSampleType) -> Date? {
        let key = userDefaultsKey(for: sampleType)
        return UserDefaults.standard.object(forKey: key) as? Date
    }

    private static func setLastExportTime(
        _ sampleType: HKSampleType,
        _ date: Date
    ) {
        let key = userDefaultsKey(for: sampleType)
        UserDefaults.standard.set(date, forKey: key)
        UserDefaults.standard.synchronize()
    }

    private static func userDefaultsKey(for sampleType: HKSampleType) -> String
    {
        return
            "\(IncrementalExporter.USER_DEFAULTS_KEY_PREFIX)\(sampleType.identifier)"
    }

    private static func getServerURL(
        completion: @escaping (String?, String?) -> Void
    ) {
        let server =
            UserDefaults.standard.string(forKey: UserDefaultsKeys.SERVER_URL)
            ?? ""
        let serverSessionQuick = ServerSession.getSession(server: server)

        serverSessionQuick.testConnection(timeout: 1) { errMsg in
            if errMsg != nil {
                CustomLogger.log(
                    "[IE][Error] Failed to connect to server: \(errMsg!)"
                )
                if UserDefaults.standard.bool(
                    forKey: UserDefaultsKeys.AUTO_SERVER_DISCOVERY_ENABLED)
                {
                    CustomLogger.log(
                        "[IE][Info] Starting auto server discovery..."
                    )
                    AutoServerDiscovery.run {
                        url in
                        if let url = url {
                            return completion(nil, url.absoluteString)
                        } else {
                            return completion("Could not find server", nil)
                        }
                    }
                } else {
                    return completion(errMsg, nil)
                }
            }
            return completion(nil, server)
        }
    }
}
