import ActivityKit
import WidgetKit
import SwiftUI

@available(iOS 16.1, *)
struct ContinuousRecordingLiveActivity: Widget {
    var body: some WidgetConfiguration {
        ActivityConfiguration(for: ContinuousRecordingAttributes.self) { context in
            // Lock Screen / Banner
            VStack(alignment: .leading, spacing: 4) {
                Text(context.attributes.name.isEmpty ? "HR Sensor" : context.attributes.name)
                    .font(.headline)
                HStack(spacing: 12) {
                    Label(Self.fmt(context.state.lastRR), systemImage: "waveform.path.ecg")
                    Label(Self.fmt(context.state.lastECG), systemImage: "bolt.heart")
                    Label(Self.fmt(context.state.lastACC), systemImage: "figure.run")
                    Label(Self.elapsed(context.state.elapsedSeconds), systemImage: "clock")
                }
                .font(.caption)
            }
            .padding(.vertical, 6)
            .padding(.horizontal, 10)
        } dynamicIsland: { context in
            DynamicIsland {
                DynamicIslandExpandedRegion(.leading) {
                    Label(Self.fmt(context.state.lastRR), systemImage: "waveform.path.ecg")
                }
                DynamicIslandExpandedRegion(.center) {
                    Label(Self.elapsed(context.state.elapsedSeconds), systemImage: "clock")
                }
                DynamicIslandExpandedRegion(.trailing) {
                    Label(Self.fmt(context.state.lastACC), systemImage: "figure.run")
                }
                DynamicIslandExpandedRegion(.bottom) {
                    Label("Updated \(Self.maxStaleness(context.state))", systemImage: "clock")
                        .font(.caption2)
                }
            } compactLeading: {
                Image(systemName: "waveform.path.ecg")
            } compactTrailing: {
                Text(Self.maxStalenessShort(context.state))
            } minimal: {
                Image(systemName: "figure.run")
            }
        }
    }

    private static func fmt(_ date: Date?) -> String {
        guard let d = date else { return "--" }
        let s = max(0, Int(Date().timeIntervalSince(d)))
        return "\(s)s"
    }

    private static func elapsed(_ seconds: Int) -> String {
        let s = max(0, seconds)
        let mm = s / 60
        let ss = s % 60
        return String(format: "%02d:%02d", mm, ss)
    }

    private static func maxStaleness(_ state: ContinuousRecordingAttributes.ContentState) -> String {
        let now = Date()
        let ages = [state.lastRR, state.lastECG, state.lastACC]
            .compactMap { $0 }
            .map { max(0, Int(now.timeIntervalSince($0))) }
        guard let maxAge = ages.max() else { return "--" }
        return "\(maxAge)s"
    }

    private static func maxStalenessShort(_ state: ContinuousRecordingAttributes.ContentState) -> String {
        let now = Date()
        let ages = [state.lastRR, state.lastECG, state.lastACC]
            .compactMap { $0 }
            .map { max(0, Int(now.timeIntervalSince($0))) }
        guard let maxAge = ages.max() else { return "--" }
        return "\(maxAge)s"
    }
}
