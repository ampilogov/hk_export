import ActivityKit
import Foundation

/// Live activity attributes describing the continuous recording status.
struct ContinuousRecordingAttributes: ActivityAttributes {
    struct ContentState: Codable, Hashable {
        var lastRR: Date?
        var lastECG: Date?
        var lastACC: Date?
        var elapsedSeconds: Int
    }

    var name: String
}
