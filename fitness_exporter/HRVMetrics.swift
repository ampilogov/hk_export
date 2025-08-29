import Foundation

/// Stages of an orthostatic HRV session.
enum HRVStage: String, Codable {
    case preLaying
    case laying
    case waitingForStanding
    case standing
    case cooldown
    case done
}

/// Timestamp for when a stage started.
/// TODO: get rid of, replace with a pair.
struct HRVStageTimestamp: Codable {
    let stage: HRVStage
    let time: Date
}

/// Metrics calculated for a segment of RR intervals.
struct StageMetrics {
    let rmssd: Double
    let meanHR: Double
}

/// Utility for computing HRV metrics.
class HRVMetricsCalculator {
    static func rmssd(_ rr: [Double]) -> Double {
        guard rr.count > 1 else { return 0 }
        let diffs = zip(rr.dropFirst(), rr).map { (cur, prev) -> Double in
            let diff = (cur - prev) * 1000
            return diff * diff
        }
        let meanSquare = diffs.reduce(0, +) / Double(diffs.count)
        return sqrt(meanSquare)
    }

    static func meanHeartRate(_ rr: [Double]) -> Double {
        guard !rr.isEmpty else { return 0 }
        return 60.0 * Double(rr.count) / rr.reduce(0, +)
    }

    static func metrics(from rr: [Double]) -> StageMetrics {
        StageMetrics(rmssd: rmssd(rr), meanHR: meanHeartRate(rr))
    }
}

/// Processes events from a sensor data bag and returns metrics by stage.
func processSensorDataBag(_ bag: SensorBag) -> [HRVStage: StageMetrics] {
    let events = bag.snapshot
    let rrPairs: [(Double, Date)] = events.compactMap { event -> [(Double, Date)]? in
        switch event.data {
        case .hrSamples(let sample):
            return sample.samples.flatMap { s in s.rrIntervals.map { ($0, event.timestamp) } }
        case .ecgSamples, .accSamples, .battery, .hrvStage, .location, .custom:
            return nil
        }
    }.flatMap { $0 }

    guard !rrPairs.isEmpty else { return [:] }

    var timesByArrival = Array(repeating: rrPairs[0].1, count: rrPairs.count)
    var idx = 0
    while idx < rrPairs.count {
        let packetTime = rrPairs[idx].1
        let startIdx = idx
        while idx < rrPairs.count && rrPairs[idx].1 == packetTime { idx += 1 }
        var suffix: TimeInterval = 0
        for j in stride(from: idx - 1, through: startIdx, by: -1) {
            timesByArrival[j] = packetTime.addingTimeInterval(-suffix)
            suffix += rrPairs[j].0
        }
    }

    guard let anchor = timesByArrival.first else { return [:] }
    var timesByRR: [Date] = []
    timesByRR.reserveCapacity(rrPairs.count)
    var cumsum: TimeInterval = 0
    for i in 0..<rrPairs.count {
        if i == 0 {
            timesByRR.append(anchor)
        } else {
            cumsum += rrPairs[i].0
            timesByRR.append(anchor.addingTimeInterval(cumsum))
        }
    }
    let values = rrPairs.map { $0.0 }

    let stageEvents: [HRVStageTimestamp] = events.compactMap { event in
        switch event.data {
        case .hrvStage(let stage):
            return HRVStageTimestamp(stage: stage, time: event.timestamp)
        case .hrSamples, .ecgSamples, .accSamples, .battery, .location, .custom:
            return nil
        }
    }.sorted { $0.time < $1.time }

    var results: [HRVStage: StageMetrics] = [:]
    for (i, event) in stageEvents.enumerated() {
        guard event.stage == .laying || event.stage == .standing else { continue }
        let start = event.time
        let end = i + 1 < stageEvents.count ? stageEvents[i + 1].time : (timesByRR.last ?? start)
        let stageRR = zip(values, timesByRR)
            .filter { _, t in t >= start && t <= end }
            .map { $0.0 }
        results[event.stage] = HRVMetricsCalculator.metrics(from: stageRR)
    }
    return results
}
