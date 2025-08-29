import Foundation
import HealthKit

/// Utilities for persisting sensor bags and exporting heartbeats to HealthKit.
enum SensorBagPersistence {
    /// Save a sensor bag to the documents directory under the given subdirectory.
    /// - Returns: URL of the saved file.
    static func save(_ bag: SensorBag, subdir: String, maxAttempts: Int = 5) throws -> URL {
        let documents = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
        let dir = documents.appendingPathComponent(subdir, isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let timestamp = Int(Date().timeIntervalSince1970)

        for attempt in 0..<maxAttempts {
            let suffix = attempt == 0 ? "" : "-\(attempt)"
            let candidate = dir.appendingPathComponent("\(timestamp)\(suffix).bin")
            if !FileManager.default.fileExists(atPath: candidate.path) {
                try bag.saveBinary(to: candidate)
                return candidate
            }
        }

        throw NSError(
            domain: "SensorBagPersistence",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "Unable to generate unique filename after \(maxAttempts) attempts"]
        )
    }

    /// Write a list of heartbeats to HealthKit.
    static func getDevice(deviceName: String?) -> HKDevice {
        return
            deviceName.map {
                HKDevice(
                    name: $0, manufacturer: nil, model: nil, hardwareVersion: nil,
                    firmwareVersion: nil, softwareVersion: "v0",
                    localIdentifier: nil, udiDeviceIdentifier: nil)
            } ?? .local()
    }

    /// Write a list of heartbeats to HealthKit.
    static func writeBeatsToHealthKit(_ beats: [(Date, Bool)], deviceName: String?) {
        guard HKHealthStore.isHealthDataAvailable() else { return }
        let store = HKHealthStore()
        HealthKitManager.requestUnifiedAuthorization(startObservers: false) { success in
            guard success else { return }
            let maxCount = HKHeartbeatSeriesBuilder.maximumCount
            let pages: [[(Date, Bool)]] = stride(from: 0, to: beats.count, by: maxCount).map {
                Array(beats[$0..<min($0 + maxCount, beats.count)])
            }
            for page in pages {
                var pageCopy = page
                pageCopy[0].1 = false  // ensure first beat starts new sequence
                saveHeartbeatPage(pageCopy, store: store, device: getDevice(deviceName: deviceName))
            }
        }
    }

    /// Write instantaneous heart rate points (bpm) to HealthKit using HKQuantitySeriesSampleBuilder.
    static func writeHeartRatesToHealthKit(_ points: [(Date, Double)], deviceName: String?) {
        guard HKHealthStore.isHealthDataAvailable(), !points.isEmpty else { return }
        let store = HKHealthStore()
        guard let hrType = HKQuantityType.quantityType(forIdentifier: .heartRate) else { return }
        HealthKitManager.requestUnifiedAuthorization(startObservers: false) { success in
            guard success else {
                CustomLogger.log("Can't write to HK: authorization failed")
                return
            }
            let sorted = points.sorted { $0.0 < $1.0 }
            guard let startDate = sorted.first?.0, let endDate = sorted.last?.0 else { return }
            let unit = HKUnit.count().unitDivided(by: .minute())
            let device = getDevice(deviceName: deviceName)

            // Build a single HR quantity series sample for the provided points.
            let builder = HKQuantitySeriesSampleBuilder(
                healthStore: store, quantityType: hrType, startDate: startDate, device: device)

            for (ts, bpm) in sorted {
                let quantity = HKQuantity(unit: unit, doubleValue: bpm)
                do {
                    try builder.insert(quantity, at: ts)
                } catch {
                    CustomLogger.log("Can't insert HR point: \(error)")
                }
            }

            builder.finishSeries(metadata: nil, endDate: endDate) { _, _ in }
        }
    }

    /// Write only RR-derived heartbeat series to HealthKit from a list of events.
    static func writeRRIntervalsToHealthKit(from events: [SensorEvent], deviceName: String?) {
        let hrContainers: [(Date, HRSamples)] = events.compactMap { e in
            if case .hrSamples(let s) = e.data { return (e.timestamp, s) }
            return nil
        }
        let beats = reconstructBeats(from: hrContainers)
        if !beats.isEmpty {
            writeBeatsToHealthKit(beats, deviceName: deviceName)
        }
    }

    /// Convenience overload: RR-derived heartbeat series from a bag.
    static func writeRRIntervalsToHealthKit(from bag: SensorBag, deviceName: String?) {
        writeRRIntervalsToHealthKit(from: bag.snapshot, deviceName: deviceName)
    }

    /// Write only instantaneous HR points to HealthKit from a list of events.
    static func writeHeartRatesToHealthKit(from events: [SensorEvent], deviceName: String?) {
        let hrContainers: [(Date, HRSamples)] = events.compactMap { e in
            if case .hrSamples(let s) = e.data { return (e.timestamp, s) }
            return nil
        }
        var hrPoints: [(Date, Double)] = []
        for (ts, samples) in hrContainers {
            for s in samples.samples {
                hrPoints.append((ts, Double(s.value)))
            }
        }
        if !hrPoints.isEmpty {
            writeHeartRatesToHealthKit(hrPoints, deviceName: deviceName)
        }
    }

    /// Convenience overload: instantaneous HR points from a bag.
    static func writeHeartRatesToHealthKit(from bag: SensorBag, deviceName: String?) {
        writeHeartRatesToHealthKit(from: bag.snapshot, deviceName: deviceName)
    }

    /// Save a single heartbeat page to HealthKit.
    private static func saveHeartbeatPage(
        _ page: [(Date, Bool)], store: HKHealthStore, device: HKDevice
    ) {
        guard page.count >= HRVConstants.MIN_HEARTBEATS else { return }
        let start = page[0].0
        let builder = HKHeartbeatSeriesBuilder(healthStore: store, device: device, start: start)
        let group = DispatchGroup()
        for (ts, hasPrev) in page {
            group.enter()
            builder.addHeartbeatWithTimeInterval(
                sinceSeriesStartDate: ts.timeIntervalSince(start),
                precededByGap: !hasPrev
            ) { _, _ in
                group.leave()
            }
        }
        group.notify(queue: .main) {
            builder.finishSeries { _, _ in }
        }
    }
}

/// Reconstruct individual heartbeats from raw HR samples.
func reconstructBeats(from samples: [(Date, HRSamples)], maxGap: Duration = .seconds(2)) -> [(
    Date, Bool
)] {
    var rr: [Double] = []
    var beatMaxTimes: [Date] = []
    for (arrival, container) in samples.sorted(by: { $0.0 < $1.0 }).reversed() {
        if beatMaxTimes.isEmpty { beatMaxTimes.append(arrival) }
        for sample in container.samples.reversed() {
            for interval in sample.rrIntervals.reversed() {
                let secondBeat = min(arrival, beatMaxTimes.last!)
                let firstBeat = secondBeat.addingTimeInterval(-interval)
                rr.append(interval)
                beatMaxTimes.append(firstBeat)
            }
        }
    }
    rr.reverse()
    beatMaxTimes.reverse()
    guard !rr.isEmpty else { return [] }
    var beats: [(Date, Bool)] = []
    var idx = 0
    while idx < rr.count {
        var current = beatMaxTimes[idx]
        beats.append((current, false))
        repeat {
            current = current.addingTimeInterval(rr[idx])
            beats.append((current, true))
            idx += 1
        } while idx < rr.count && current + Double(maxGap.components.seconds) >= beatMaxTimes[idx]
    }
    return beats
}
