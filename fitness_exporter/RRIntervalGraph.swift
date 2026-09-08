import SwiftUI
import UIKit

final class RRIntervalGraphModel: ObservableObject {
    struct Sample {
        let rr: Double  // seconds
        let received: Date  // inferred timestamp for this RR interval
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

    /// Replace the visible RR window with a snapshot accumulated off the main
    /// thread while the graph was hidden or the app was inactive.
    func replace(samples: [Sample], lastPackageTime: Date?) {
        dispatchPrecondition(condition: .onQueue(.main))
        self.lastPackageTime = lastPackageTime
        self.samples = samplesInWindow(samples, relativeTo: lastPackageTime)
        pruneStageMarkers()
    }

    /// Append a coalesced group of samples with a single published mutation.
    func append(samples: [Sample], lastPackageTime: Date?) {
        dispatchPrecondition(condition: .onQueue(.main))
        if let lastPackageTime {
            self.lastPackageTime = lastPackageTime
        }
        self.samples = samplesInWindow(
            self.samples + samples,
            relativeTo: self.lastPackageTime
        )
        pruneStageMarkers()
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
        samples = samplesInWindow(samples, relativeTo: last)
        pruneStageMarkers()
    }

    private func samplesInWindow(_ samples: [Sample], relativeTo last: Date?) -> [Sample] {
        guard let last else { return samples }
        let cutoff = last.addingTimeInterval(-window)
        return samples.filter { $0.received >= cutoff }
    }

    private func pruneStageMarkers() {
        guard let last = lastPackageTime else { return }
        let cutoff = last.addingTimeInterval(-window)
        stageMarkers = stageMarkers.filter { $0 >= cutoff }
    }
}

struct RRIntervalGraph: View {
    @ObservedObject var model: RRIntervalGraphModel
    var onSelect: ((Date) -> Void)?

    init(
        model: RRIntervalGraphModel,
        onSelect: ((Date) -> Void)? = nil
    ) {
        self.model = model
        self.onSelect = onSelect
    }

    var body: some View {
        TimelineView(.periodic(from: .now, by: 1)) { timeline in
            let now = timeline.date
            let samples = model.samples.sorted { $0.received < $1.received }

            let xFromRR = samples.map(\.received)

            let xVals: [Date] = xFromRR + model.stageMarkers + [model.lastPackageTime ?? now, now]
            let xMin = xVals.min() ?? now
            let xMax = xVals.max() ?? now
            let xSpan = max(xMax.timeIntervalSince(xMin), 0.001)
            let rawYMin = samples.map(\.rr).min() ?? 0
            let rawYMax = samples.map(\.rr).max() ?? 1
            let yPadding = rawYMin == rawYMax ? max(abs(rawYMin) * 0.05, 0.001) : 0
            let yMin = rawYMin - yPadding
            let yMax = rawYMax + yPadding
            let ySpan = max(yMax - yMin, 0.001)

            GeometryReader { proxy in
                Canvas { context, size in
                    func xPos(_ t: Date) -> CGFloat {
                        CGFloat(t.timeIntervalSince(xMin) / xSpan) * size.width
                    }
                    func yPos(_ value: Double) -> CGFloat {
                        CGFloat(1 - (value - yMin) / ySpan) * size.height
                    }

                    var path = Path()
                    for i in samples.indices {
                        let x = xPos(xFromRR[i])
                        let y = yPos(samples[i].rr)
                        if i == samples.startIndex {
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

                    if let lastPackageTime = model.lastPackageTime {
                        let x = xPos(lastPackageTime)
                        var drift = Path()
                        drift.move(to: CGPoint(x: x, y: 0))
                        drift.addLine(to: CGPoint(x: x, y: size.height))
                        context.stroke(drift, with: .color(.blue), lineWidth: 1)
                    }
                }
                .contentShape(Rectangle())
                .gesture(
                    DragGesture(minimumDistance: 0)
                        .onEnded { value in
                            guard let onSelect, !xFromRR.isEmpty,
                                  proxy.size.width > 0
                            else { return }
                            let fraction = min(
                                max(Double(value.location.x / proxy.size.width), 0),
                                1
                            )
                            let tappedAt = xMin.addingTimeInterval(fraction * xSpan)
                            if let nearest = xFromRR.min(by: {
                                abs($0.timeIntervalSince(tappedAt))
                                    < abs($1.timeIntervalSince(tappedAt))
                            }) {
                                onSelect(nearest)
                            }
                        }
                )
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
}

struct ECGDetailView: View {
    let centeredAt: Date
    let load: (@escaping (Result<[ECGPlotPoint], Error>) -> Void) -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var points: [ECGPlotPoint] = []
    @State private var isLoading = true
    @State private var errorText: String?

    var body: some View {
        NavigationStack {
            Group {
                if isLoading {
                    ProgressView("Loading ECG…")
                } else if let errorText {
                    ContentUnavailableView(
                        "ECG unavailable",
                        systemImage: "waveform.path.ecg",
                        description: Text(errorText)
                    )
                } else if points.isEmpty {
                    ContentUnavailableView(
                        "No ECG in this window",
                        systemImage: "waveform.path.ecg",
                        description: Text(
                            "The selected data may still be in an unfinished recording window."
                        )
                    )
                } else {
                    VStack(alignment: .leading, spacing: 12) {
                        Text("Drag horizontally to pan. Pinch to zoom.")
                            .font(.caption)
                            .foregroundColor(.secondary)
                        ECGPlotCanvas(points: points, centeredAt: centeredAt)
                            .frame(minHeight: 260)
                    }
                    .padding()
                }
            }
            .navigationTitle(centeredAt.formatted(date: .omitted, time: .standard))
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { dismiss() }
                }
            }
        }
        .onAppear(perform: loadPoints)
    }

    private func loadPoints() {
        isLoading = true
        errorText = nil
        load { result in
            isLoading = false
            switch result {
            case .success(let loadedPoints):
                points = loadedPoints
            case .failure(let error):
                errorText = error.localizedDescription
                CustomLogger.log("[ECG][Error] \(error.localizedDescription)")
            }
        }
    }
}

private struct ECGPlotCanvas: View {
    let points: [ECGPlotPoint]
    let centeredAt: Date

    @State private var panOffset: TimeInterval = 0
    @State private var zoom: CGFloat = 1
    @GestureState private var dragTranslation: CGFloat = 0
    @GestureState private var magnification: CGFloat = 1

    private let baseWindow: TimeInterval = 10

    var body: some View {
        GeometryReader { proxy in
            let effectiveZoom = min(max(zoom * magnification, 1), 8)
            let duration = baseWindow / Double(effectiveZoom)
            let dragSeconds = proxy.size.width > 0
                ? Double(dragTranslation / proxy.size.width) * duration
                : 0
            let visibleCenter = centeredAt.addingTimeInterval(panOffset - dragSeconds)
            let start = visibleCenter.addingTimeInterval(-duration / 2)
            let end = visibleCenter.addingTimeInterval(duration / 2)
            let visible = points.filter { $0.timestamp >= start && $0.timestamp <= end }

            Canvas { context, size in
                guard !visible.isEmpty, size.width >= 1, size.height >= 1 else { return }
                let columnCount = max(1, Int(size.width.rounded(.up)))
                var minimums = Array(repeating: Double.infinity, count: columnCount)
                var maximums = Array(repeating: -Double.infinity, count: columnCount)
                var voltageMin = Double.infinity
                var voltageMax = -Double.infinity

                for point in visible {
                    let fraction = point.timestamp.timeIntervalSince(start) / duration
                    let column = min(
                        max(Int(fraction * Double(columnCount)), 0),
                        columnCount - 1
                    )
                    let voltage = Double(point.voltage)
                    minimums[column] = min(minimums[column], voltage)
                    maximums[column] = max(maximums[column], voltage)
                    voltageMin = min(voltageMin, voltage)
                    voltageMax = max(voltageMax, voltage)
                }

                let padding = voltageMin == voltageMax
                    ? max(abs(voltageMin) * 0.05, 1)
                    : 0
                voltageMin -= padding
                voltageMax += padding
                let voltageSpan = max(voltageMax - voltageMin, 1)
                func y(_ voltage: Double) -> CGFloat {
                    CGFloat(1 - (voltage - voltageMin) / voltageSpan) * size.height
                }

                var path = Path()
                for column in 0..<columnCount where minimums[column].isFinite {
                    let x = CGFloat(column) + 0.5
                    path.move(to: CGPoint(x: x, y: y(minimums[column])))
                    path.addLine(to: CGPoint(x: x, y: y(maximums[column])))
                }
                context.stroke(path, with: .color(.green), lineWidth: 1)
            }
            .background(Color(UIColor.secondarySystemBackground))
            .clipShape(RoundedRectangle(cornerRadius: 8))
            .overlay(alignment: .bottom) {
                HStack {
                    Text(start.formatted(date: .omitted, time: .standard))
                    Spacer()
                    Text(end.formatted(date: .omitted, time: .standard))
                }
                .font(.caption2.monospacedDigit())
                .padding(6)
            }
            .gesture(
                DragGesture()
                    .updating($dragTranslation) { value, state, _ in
                        state = value.translation.width
                    }
                    .onEnded { value in
                        guard proxy.size.width > 0 else { return }
                        panOffset -= Double(value.translation.width / proxy.size.width) * duration
                    }
            )
            .simultaneousGesture(
                MagnificationGesture()
                    .updating($magnification) { value, state, _ in
                        state = value
                    }
                    .onEnded { value in
                        zoom = min(max(zoom * value, 1), 8)
                    }
            )
        }
    }
}
