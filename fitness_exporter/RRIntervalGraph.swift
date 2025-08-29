import SwiftUI
import UIKit

final class RRIntervalGraphModel: ObservableObject {
    struct Sample {
        let rr: Double  // seconds
        let received: Date  // package arrival time
    }

    @Published var samples: [Sample] = []
    @Published var stageMarkers: [Date] = []
    private(set) var lastPackageTime: Date?
    /// Maintain only the last 5 minutes of data by received timestamp.
    let window: TimeInterval = 300

    func reset() {
        DispatchQueue.main.async {
            self.samples.removeAll()
            self.stageMarkers.removeAll()
            self.lastPackageTime = nil
        }
    }

    func append(intervals: [Double], packageTime: Date) {
        // print("new package: \(packageTime) \(intervals)")
        guard !intervals.isEmpty else { return }
        DispatchQueue.main.async {
            // Assert packages arrive with increasing received timestamp
            assert(
                self.lastPackageTime == nil || packageTime > self.lastPackageTime!,
                "packages must arrive in increasing received timestamp")

            var inferedPackageTime = packageTime
            for rr in intervals.reversed() {
                self.samples.append(Sample(rr: rr, received: inferedPackageTime))
                inferedPackageTime -= rr
            }
            self.lastPackageTime = packageTime
            self.pruneOld()
        }
    }

    func markStageChange(at date: Date) {
        DispatchQueue.main.async {
            self.stageMarkers.append(date)
            self.pruneOld()
        }
    }

    private func pruneOld() {
        guard let last = lastPackageTime else { return }
        let cutoff = last.addingTimeInterval(-window)
        samples.removeAll { $0.received < cutoff }
        stageMarkers = stageMarkers.filter { $0 >= cutoff }
    }
}

struct RRIntervalGraph: View {
    @ObservedObject var model: RRIntervalGraphModel

    var body: some View {
        TimelineView(.periodic(from: .now, by: 1)) { timeline in
            let now = Date()
            let samples = model.samples.sorted { $0.received < $1.received }

            let xFromRR: [Date] = samples.indices.reduce(into: [Date]()) { acc, i in
                if i == 0 {
                    acc.append(samples[i].received)
                } else {
                    acc.append(acc[i - 1].addingTimeInterval(samples[i - 1].rr))
                }
            }

            let xVals: [Date] = xFromRR + model.stageMarkers + [model.lastPackageTime ?? now, now]
            let xMin = xVals.min()!
            let xMax = xVals.max()!
            let yMin = samples.map { $0.rr }.min() ?? 0 + 1e-5
            let yMax = samples.map { $0.rr }.max() ?? 1 - 1e-5

            Canvas { context, size in
                func xPos(_ t: Date) -> CGFloat {
                    CGFloat((t.timeIntervalSince(xMin)) / xMax.timeIntervalSince(xMin)) * size.width
                }
                func yPos(_ value: Double) -> CGFloat {
                    CGFloat(1 - (value - yMin) / (yMax - yMin)) * size.height
                }

                // print("range: \(xMin) \(xMax) now=\(now)")
                // print("samples: \(samples)")
                // print("lastPackageTime: \(String(describing: model.lastPackageTime))")
                var path = Path()
                for i in 0..<samples.count {
                    let x = xPos(xFromRR[i])
                    let y = yPos(samples[i].rr)
                    //                    print("New pos: \(samples[i].rr) -- \(x), \(y)")
                    if i == 0 {
                        path.move(to: CGPoint(x: x, y: y))
                    } else {
                        path.addLine(to: CGPoint(x: x, y: y))
                    }
                }
                context.stroke(path, with: .color(.green), lineWidth: 1)

                for marker in model.stageMarkers {
                    let x = xPos(marker)
                    var line = Path()
                    line.move(to: CGPoint(x: x, y: 0))
                    line.addLine(to: CGPoint(x: x, y: size.height))
                    context.stroke(line, with: .color(.red), lineWidth: 1)
                }

                if model.lastPackageTime != nil {
                    let x = xPos(model.lastPackageTime!)
                    var drift = Path()
                    drift.move(to: CGPoint(x: x, y: 0))
                    drift.addLine(to: CGPoint(x: x, y: size.height))
                    context.stroke(drift, with: .color(.blue), lineWidth: 1)
                }
            }
            .overlay(alignment: .leading) {
                if !samples.isEmpty {
                    VStack {
                        Text(String(format: "%.0f ms", yMax * 1000.0))
                        Spacer()
                        Text(String(format: "%.0f ms", yMin * 1000.0))
                    }
                    .font(.caption2)
                    .padding(.leading, 2)
                }
            }
            .background(Color(UIColor.secondarySystemBackground))
        }
    }
}
