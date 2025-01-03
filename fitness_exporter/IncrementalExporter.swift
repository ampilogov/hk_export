import Foundation
import HealthKit

final class IncrementalExporter {
    private static let LOCK = NSLock()

    private static let EPS: TimeInterval = 600
    private static let DEFAULT_START_DATE: Date = Calendar.current.date(
        from: DateComponents(year: 2001, month: 1, day: 1))!
    private static let TIME_TO_FINALIZE: TimeInterval = 2 * 24 * 60 * 60

    private static let USER_DEFAULTS_KEY_PREFIX =
        "IncrementalExporter_LastExportTime_"

    init() {
    }

    static func resetCursors() {
        if IncrementalExporter.LOCK.try() {
            defer { IncrementalExporter.LOCK.unlock() }

            CustomLogger.log("Resetting cursors")

            let keys = UserDefaults.standard.dictionaryRepresentation().keys

            for key in keys
            where key.hasPrefix(IncrementalExporter.USER_DEFAULTS_KEY_PREFIX) {
                UserDefaults.standard.removeObject(forKey: key)
            }

            UserDefaults.standard.synchronize()

        } else {
            CustomLogger.log("Can't aquire lock for reset cursors")
        }
    }

    static func getCursors(
        sampleTypes: [HKSampleType]
    ) -> [HKSampleType: Date?] {
        if IncrementalExporter.LOCK.try() {
            defer { IncrementalExporter.LOCK.unlock() }

            return Dictionary(
                uniqueKeysWithValues: sampleTypes.map {
                    ($0, IncrementalExporter.getLastExportTime($0))
                })
        } else {
            CustomLogger.log("Can't aquire lock for get cursors")
            return [:]
        }
    }

    func run(
        sampleTypes: [HKSampleType], batchSize: TimeInterval,
        completion: @escaping (String?) -> Void
    ) {
        if IncrementalExporter.LOCK.try() {
            CustomLogger.log("Run locked")
            self.runUnlocked(
                sampleTypes: sampleTypes,
                batchSize: batchSize
            ) {
                result in
                IncrementalExporter.LOCK.unlock()
                CustomLogger.log("Run unlocked")
                return completion(result)
            }
        } else {
            CustomLogger.log("Can't aquire lock for run")
            return completion("Can't aquire lock for run")
        }
    }

    private func runUnlocked(
        sampleTypes: [HKSampleType], batchSize: TimeInterval,
        completion: @escaping (String?) -> Void
    ) {
        IncrementalExporter.getServerURL {
            status, serverURL in
            if status != nil {
                return completion(status)
            }

            let exporter = HealthDataExporter(
                healthStore: HKHealthStore(),
                server: serverURL!,
                sender: UserDefaults.standard.string(
                    forKey: UserDefaultsKeys.SENDER) ?? ""
            )
            self.export(
                exporter: exporter, sampleTypes: sampleTypes,
                batchSize: batchSize, completion: completion)
        }
    }

    private func export(
        exporter: HealthDataExporter,
        sampleTypes: [HKSampleType], batchSize: TimeInterval,
        completion: @escaping (String?) -> Void
    ) {
        CustomLogger.log("Running incremental export")

        let queue = DispatchQueue(
            label: "com.fitness_exporter.incrementalQueue")

        var index = 0

        func processNext() {
            queue.async {
                if index == sampleTypes.count {
                    CustomLogger.log(
                        "Finished running incremental export, success \(index)/\(sampleTypes.count)"
                    )
                    return completion(nil)
                }

                self.exportSampleType(exporter, sampleTypes[index], batchSize) {
                    status in
                    if let status = status {
                        return completion(status)
                    }
                    index += 1
                    processNext()
                }

            }
        }

        processNext()
    }

    private func exportSampleType(
        _ exporter: HealthDataExporter,
        _ sampleType: HKSampleType, _ batchSize: TimeInterval,
        completion: @escaping (String?) -> Void
    ) {
        CustomLogger.log("Running incremental export for \(sampleType)")

        let queue = DispatchQueue(
            label: "com.fitness_exporter.incrementalSampleQueue")

        var lastExportTime =
            IncrementalExporter.getLastExportTime(sampleType)
            ?? IncrementalExporter.DEFAULT_START_DATE
        let now = Date()
        CustomLogger.log("Last export time: \(lastExportTime), now: \(now)")

        func processNext() {
            queue.async {
                if lastExportTime >= now {
                    CustomLogger.log(
                        "Finished running incremental export for \(sampleType), success"
                    )
                    return completion(nil)
                }

                let from = lastExportTime - IncrementalExporter.EPS
                let to = lastExportTime + batchSize + IncrementalExporter.EPS
                CustomLogger.log(
                    "Running incremental export for \(sampleType) from \(from) to \(to)"
                )
                exporter.export(sampleType: sampleType, from: from, to: to) {
                    status in
                    if let status = status {
                        return completion(status)
                    }
                    lastExportTime += batchSize
                    IncrementalExporter.setLastExportTime(
                        sampleType,
                        min(
                            lastExportTime,
                            now - IncrementalExporter.TIME_TO_FINALIZE))
                    processNext()
                }

            }
        }

        processNext()
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
        let serverSessionQuick = ServerSession(server: server)

        serverSessionQuick.testConnection(timeout: 1) { errMsg in
            if errMsg != nil {
                CustomLogger.log(
                    "IncrementalExporter: failed to connect to server: \(errMsg!)"
                )
                if UserDefaults.standard.bool(
                    forKey: UserDefaultsKeys.AUTO_SERVER_DISCOVERY_ENABLED)
                {
                    CustomLogger.log(
                        "IncrementalExporter: starting auto server discovery..."
                    )
                    AutoServerDiscovery.run {
                        url in
                        return completion(nil, url!.absoluteString)
                    }
                } else {
                    return completion(errMsg, nil)
                }
            }
            return completion(nil, server)
        }
    }
}
