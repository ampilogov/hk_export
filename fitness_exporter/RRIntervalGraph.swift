import SwiftUI
import UIKit
import UniformTypeIdentifiers

enum HeartRateGraphScale {
    static func beatsPerMinute(forRR rrSeconds: Double) -> Double? {
        guard rrSeconds > 0, rrSeconds.isFinite else { return nil }
        return 60 / rrSeconds
    }

    static func axisTicks(
        minimum: Double,
        maximum: Double,
        targetCount: Int = 5
    ) -> [Double] {
        guard minimum.isFinite, maximum.isFinite, maximum > minimum,
              targetCount > 0
        else { return [] }
        let roughStep = (maximum - minimum) / Double(targetCount)
        let magnitude = pow(10, floor(log10(roughStep)))
        let normalizedStep = roughStep / magnitude
        let niceStep: Double
        if normalizedStep <= 1 {
            niceStep = magnitude
        } else if normalizedStep <= 2 {
            niceStep = 2 * magnitude
        } else if normalizedStep <= 5 {
            niceStep = 5 * magnitude
        } else {
            niceStep = 10 * magnitude
        }
        let first = ceil(minimum / niceStep) * niceStep
        var ticks: [Double] = []
        var value = first
        while value <= maximum, ticks.count <= targetCount + 2 {
            ticks.append(value)
            value += niceStep
        }
        return ticks
    }

    /// Include every sample in the axis domain so its plotted Y coordinate
    /// always represents its actual BPM value.
    static func displayRange(for values: [Double]) -> ClosedRange<Double> {
        let finite = values.filter(\.isFinite)
        guard let minimum = finite.min(),
              let maximum = finite.max()
        else { return 0...1 }

        let span = maximum - minimum
        let padding = max(span * 0.08, minimum == maximum ? 1 : 0.5)
        return max(0, minimum - padding)...(maximum + padding)
    }

    static func displayRange(
        for samples: [(timestamp: Date, bpm: Double)],
        in visibleRange: ClosedRange<Date>
    ) -> ClosedRange<Double> {
        displayRange(
            for: samples.compactMap { sample in
                visibleRange.contains(sample.timestamp) ? sample.bpm : nil
            }
        )
    }
}

enum SignalTimelineScale {
    static let historyDuration: TimeInterval = 5 * 60
    static let initialVisibleDuration: TimeInterval = 30
    static let minimumVisibleDuration: TimeInterval = 2
    static let maximumECGVisibleDuration: TimeInterval = 60

    static func clampedDuration(_ duration: TimeInterval) -> TimeInterval {
        min(max(duration, minimumVisibleDuration), historyDuration)
    }

    static func zoomedDuration(
        baseDuration: TimeInterval,
        magnification: Double
    ) -> TimeInterval {
        clampedDuration(baseDuration / max(magnification, 0.01))
    }

    static func shouldRenderECG(visibleDuration: TimeInterval) -> Bool {
        visibleDuration <= maximumECGVisibleDuration + 0.001
    }

    /// Treat a range endpoint very near the newest sample as being at the live
    /// edge when a pan begins from live mode.
    static func shouldFollowLatest(
        proposedEnd: Date,
        latest: Date,
        visibleDuration: TimeInterval
    ) -> Bool {
        let edgeTolerance = max(0.25, min(visibleDuration * 0.02, 1))
        return proposedEnd.timeIntervalSince(latest) >= -edgeTolerance
    }

    static func resolvedPausedEnd(
        proposedEnd: Date,
        latest: Date,
        visibleDuration: TimeInterval
    ) -> Date? {
        guard !shouldFollowLatest(
            proposedEnd: proposedEnd,
            latest: latest,
            visibleDuration: visibleDuration
        ) else { return nil }
        return min(proposedEnd, latest)
    }

    static func timeTicks(in range: ClosedRange<Date>) -> [Date] {
        let duration = range.upperBound.timeIntervalSince(range.lowerBound)
        guard duration > 0, duration.isFinite else { return [] }

        let step: TimeInterval
        switch duration {
        case ...5: step = 1
        case ...15: step = 3
        case ...30: step = 10
        case ...60: step = 15
        case ...120: step = 30
        default: step = 60
        }

        var ticks: [Date] = []
        var timestamp = ceil(range.lowerBound.timeIntervalSince1970 / step) * step
        while timestamp <= range.upperBound.timeIntervalSince1970, ticks.count < 7 {
            ticks.append(Date(timeIntervalSince1970: timestamp))
            timestamp += step
        }
        return ticks
    }

    static func timeLabel(for date: Date, visibleDuration: TimeInterval) -> String {
        let components = Calendar.current.dateComponents(
            [.hour, .minute, .second],
            from: date
        )
        let hour = components.hour ?? 0
        let minute = components.minute ?? 0
        if visibleDuration <= 60 {
            return String(
                format: "%02d:%02d:%02d",
                hour,
                minute,
                components.second ?? 0
            )
        }
        return String(format: "%02d:%02d", hour, minute)
    }
}

final class RRIntervalGraphModel: ObservableObject {
    struct Sample {
        let rr: Double  // seconds
        let received: Date  // inferred timestamp for this RR interval
        let beginsNewSegment: Bool

        init(rr: Double, received: Date, beginsNewSegment: Bool = false) {
            self.rr = rr
            self.received = received
            self.beginsNewSegment = beginsNewSegment
        }
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
    func replace(
        samples: [Sample],
        lastPackageTime: Date?,
        windowEnd: Date? = nil
    ) {
        dispatchPrecondition(condition: .onQueue(.main))
        self.lastPackageTime = lastPackageTime
        let referenceTime = windowEnd ?? lastPackageTime
        self.samples = samplesInWindow(samples, relativeTo: referenceTime)
        pruneStageMarkers(relativeTo: referenceTime)
    }

    /// Append a coalesced group of samples with a single published mutation.
    func append(
        samples: [Sample],
        lastPackageTime: Date?,
        windowEnd: Date? = nil
    ) {
        dispatchPrecondition(condition: .onQueue(.main))
        if let lastPackageTime {
            self.lastPackageTime = lastPackageTime
        }
        let referenceTime = windowEnd ?? self.lastPackageTime
        self.samples = samplesInWindow(
            self.samples + samples,
            relativeTo: referenceTime
        )
        pruneStageMarkers(relativeTo: referenceTime)
    }

    /// Advance the shared graph window even when no new RR sample arrived.
    /// This prevents a healthy ECG stream from retaining stale HR indefinitely.
    func prune(relativeTo windowEnd: Date) {
        dispatchPrecondition(condition: .onQueue(.main))
        samples = samplesInWindow(samples, relativeTo: windowEnd)
        pruneStageMarkers(relativeTo: windowEnd)
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
        pruneStageMarkers(relativeTo: last)
    }

    private func samplesInWindow(_ samples: [Sample], relativeTo last: Date?) -> [Sample] {
        let samplesInRange: [Sample]
        if let last {
            let cutoff = last.addingTimeInterval(-window)
            samplesInRange = samples.filter { $0.received >= cutoff }
        } else {
            samplesInRange = samples
        }
        return Self.chronologicalSamples(samplesInRange)
    }

    /// Keep the model defensive against duplicated or out-of-order delivery so
    /// the chart path can never travel backward across the time axis.
    static func chronologicalSamples(_ samples: [Sample]) -> [Sample] {
        let sorted = samples.enumerated().sorted { lhs, rhs in
            if lhs.element.received == rhs.element.received {
                return lhs.offset < rhs.offset
            }
            return lhs.element.received < rhs.element.received
        }

        var result: [Sample] = []
        result.reserveCapacity(sorted.count)
        for entry in sorted {
            if result.last?.received == entry.element.received {
                result[result.count - 1] = entry.element
            } else {
                result.append(entry.element)
            }
        }
        return result
    }

    private func pruneStageMarkers(relativeTo windowEnd: Date?) {
        guard let windowEnd else { return }
        let cutoff = windowEnd.addingTimeInterval(-window)
        stageMarkers = stageMarkers.filter { $0 >= cutoff }
    }
}

/// Builds a continuous beat clock from Polar RR values. Bluetooth callbacks
/// have variable delivery latency, so their wall-clock receipt time is useful
/// only as an anchor and for detecting a genuinely interrupted stream.
struct RRBeatTimelineReconstructor {
    static let discontinuityTolerance: TimeInterval = 2

    private(set) var lastBeatTime: Date?

    mutating func reset() {
        lastBeatTime = nil
    }

    mutating func append(
        intervals: [Double],
        receivedAt: Date
    ) -> [RRIntervalGraphModel.Sample] {
        let validIntervals = intervals.filter { $0 > 0 && $0.isFinite }
        guard !validIntervals.isEmpty else { return [] }

        // Polar documents RR values as oldest-to-newest. Reconstruct the
        // candidate end time of each interval by anchoring the newest beat to
        // this callback's receipt time.
        var reversedCandidates: [(rr: Double, timestamp: Date)] = []
        reversedCandidates.reserveCapacity(validIntervals.count)
        var offset: TimeInterval = 0
        for rr in validIntervals.reversed() {
            reversedCandidates.append(
                (rr: rr, timestamp: receivedAt.addingTimeInterval(-offset))
            )
            offset += rr
        }

        var result: [RRIntervalGraphModel.Sample] = []
        result.reserveCapacity(validIntervals.count)
        for candidate in reversedCandidates.reversed() {
            let timestamp: Date
            let beginsNewSegment: Bool
            if let lastBeatTime {
                let predicted = lastBeatTime.addingTimeInterval(candidate.rr)
                let arrivalDrift = candidate.timestamp.timeIntervalSince(predicted)
                if abs(arrivalDrift) > Self.discontinuityTolerance {
                    // A packet that maps wholly behind the current timeline is
                    // stale or duplicated. Do not create overlapping beats.
                    guard candidate.timestamp > lastBeatTime else { continue }
                    timestamp = candidate.timestamp
                    beginsNewSegment = true
                } else {
                    // RR is the actual beat-to-beat spacing; callback timing is
                    // transport jitter and must not move the point on the X-axis.
                    timestamp = predicted
                    beginsNewSegment = false
                }
            } else {
                timestamp = candidate.timestamp
                beginsNewSegment = true
            }

            result.append(
                RRIntervalGraphModel.Sample(
                    rr: candidate.rr,
                    received: timestamp,
                    beginsNewSegment: beginsNewSegment
                )
            )
            lastBeatTime = timestamp
        }
        return result
    }
}

struct HeartRatePlotPoint {
    let timestamp: Date
    let bpm: Double
    let beginsNewSegment: Bool
}

struct SignalGraphSnapshot {
    let heartRate: [HeartRatePlotPoint]
    let ecg: [ECGPlotPoint]
    let stageMarkers: [Date]
    let lastPackageTime: Date?
    let latestSignalTime: Date
    let range: ClosedRange<Date>
}

private enum SignalChartLayout {
    static let leading: CGFloat = 38
    static let trailing: CGFloat = 6
    static let top: CGFloat = 8
    static let timeAxisHeight: CGFloat = 22
    static let timeLabelHalfWidth: CGFloat = 22

    static func timeLabelX(for tickX: CGFloat, canvasWidth: CGFloat) -> CGFloat {
        min(
            max(tickX, timeLabelHalfWidth),
            max(timeLabelHalfWidth, canvasWidth - timeLabelHalfWidth)
        )
    }
}

struct SynchronizedSignalGraphs: View {
    @Environment(\.dismiss) private var dismiss
    @ObservedObject private var eventBridge: HRVEventBridge
    @ObservedObject private var model: RRIntervalGraphModel

    private let isExpanded: Bool

    @State private var visibleDuration = SignalTimelineScale.initialVisibleDuration
    @State private var pausedEnd: Date?
    @State private var panBaseEnd: Date?
    @State private var panOffset: TimeInterval = 0
    @State private var panStartedFromHistory = false
    @State private var zoomBaseDuration: TimeInterval?
    @State private var zoomBaseCenter: Date?
    @State private var zoomWasFollowingLatest = false
    @State private var zoomHeartRateRange: ClosedRange<Double>?
    @State private var zoomECGRange: ClosedRange<Double>?
    @State private var retainedHistory: SignalGraphSnapshot?
    @State private var frozenSnapshot: SignalGraphSnapshot?
    @State private var inspectionTime: Date?
    @State private var showExpanded = false
    @AppStorage(UserDefaultsKeys.ECG_EXPORT_DIRECTORY_BOOKMARK) private var ecgExportDirectoryBookmark = Data()
    @State private var choosingECGExportDirectory = false
    @State private var pendingExport: SignalGraphSnapshot?
    @State private var exportMessage: String?

    init(eventBridge: HRVEventBridge) {
        self.init(
            eventBridge: eventBridge,
            isExpanded: false,
            initialVisibleDuration: SignalTimelineScale.initialVisibleDuration,
            initialPausedEnd: nil,
            initialRetainedHistory: nil
        )
    }

    private init(
        eventBridge: HRVEventBridge,
        isExpanded: Bool,
        initialVisibleDuration: TimeInterval,
        initialPausedEnd: Date?,
        initialRetainedHistory: SignalGraphSnapshot?
    ) {
        self.eventBridge = eventBridge
        self.model = eventBridge.graphModel
        self.isExpanded = isExpanded
        self._visibleDuration = State(
            initialValue: SignalTimelineScale.clampedDuration(initialVisibleDuration)
        )
        self._pausedEnd = State(initialValue: initialPausedEnd)
        self._retainedHistory = State(initialValue: initialRetainedHistory)
    }

    var body: some View {
        let liveHeartRate = model.samples.compactMap { sample in
            HeartRateGraphScale.beatsPerMinute(forRR: sample.rr).map {
                HeartRatePlotPoint(
                    timestamp: sample.received,
                    bpm: $0,
                    beginsNewSegment: sample.beginsNewSegment
                )
            }
        }
        let liveECG = eventBridge.recentECGPoints
        let liveLatest = [liveHeartRate.last?.timestamp, liveECG.last?.timestamp]
            .compactMap { $0 }
            .max() ?? Date()
        let heartRate = retainedHistory?.heartRate ?? liveHeartRate
        let ecg = retainedHistory?.ecg ?? liveECG
        let stageMarkers = retainedHistory?.stageMarkers ?? model.stageMarkers
        let lastPackageTime = retainedHistory == nil
            ? model.lastPackageTime
            : retainedHistory?.lastPackageTime
        let latest = retainedHistory?.latestSignalTime ?? liveLatest
        let earliest = [heartRate.first?.timestamp, ecg.first?.timestamp]
            .compactMap { $0 }
            .min() ?? latest.addingTimeInterval(-SignalTimelineScale.historyDuration)
        let duration = visibleDuration
        let proposedEnd: Date = {
            if zoomBaseDuration != nil, zoomWasFollowingLatest {
                return latest
            } else if let zoomBaseCenter {
                return zoomBaseCenter.addingTimeInterval(duration / 2)
            } else {
                return (panBaseEnd ?? pausedEnd ?? latest)
                    .addingTimeInterval(-panOffset)
            }
        }()
        let liveRange = visibleRange(
            endingAt: proposedEnd,
            duration: duration,
            earliest: earliest,
            latest: latest
        )
        let navigationSnapshot = SignalGraphSnapshot(
            heartRate: heartRate,
            ecg: ecg,
            stageMarkers: stageMarkers,
            lastPackageTime: lastPackageTime,
            latestSignalTime: latest,
            range: liveRange
        )
        let snapshot = frozenSnapshot ?? navigationSnapshot

        Group {
            if isExpanded {
                expandedContent(
                    navigationSnapshot: navigationSnapshot,
                    snapshot: snapshot
                )
            } else {
                compactContent(
                    navigationSnapshot: navigationSnapshot,
                    snapshot: snapshot
                )
            }
        }
        .fullScreenCover(isPresented: $showExpanded, onDismiss: {
            SignalGraphOrientation.leaveFullScreen()
        }) {
            SynchronizedSignalGraphs(
                eventBridge: eventBridge,
                isExpanded: true,
                initialVisibleDuration: min(
                    visibleDuration,
                    SignalTimelineScale.maximumECGVisibleDuration
                ),
                initialPausedEnd: pausedEnd,
                initialRetainedHistory: retainedHistory
            )
            .onAppear {
                SignalGraphOrientation.enterFullScreen()
            }
        }
        .fileImporter(
            isPresented: $choosingECGExportDirectory,
            allowedContentTypes: [.folder]
        ) { result in
            switch result {
            case .success(let url):
                do {
                    ecgExportDirectoryBookmark = try ECGGraphExport.bookmark(for: url)
                    if let pendingExport {
                        self.pendingExport = nil
                        saveECGExport(pendingExport)
                    }
                } catch {
                    exportMessage = "Could not use the selected directory: \(error.localizedDescription)"
                }
            case .failure(let error):
                exportMessage = "Directory selection failed: \(error.localizedDescription)"
            }
        }
        .alert("ECG export", isPresented: Binding(
            get: { exportMessage != nil },
            set: { if !$0 { exportMessage = nil } }
        )) {
            Button("OK") { exportMessage = nil }
        } message: {
            Text(exportMessage ?? "")
        }
    }

    private func compactContent(
        navigationSnapshot: SignalGraphSnapshot,
        snapshot: SignalGraphSnapshot
    ) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text("Heart Rate")
                    .font(.subheadline.weight(.semibold))
                Spacer()
                if pausedEnd != nil {
                    Button("Latest · 5 min", systemImage: "arrow.right.to.line") {
                        resumeLatest()
                    }
                    .font(.caption)
                }
            }

            HeartRatePlot(
                points: snapshot.heartRate,
                stageMarkers: snapshot.stageMarkers,
                lastPackageTime: snapshot.lastPackageTime,
                range: snapshot.range,
                inspectionTime: inspectionTime,
                fixedYRange: zoomHeartRateRange
            )
            .frame(height: 170)
            .overlay {
                interactionLayer(
                    navigationSnapshot: navigationSnapshot,
                    displayedRange: snapshot.range
                )
            }

            HStack {
                Text("ECG")
                    .font(.subheadline.weight(.semibold))
                Spacer()
                Button("Export", systemImage: "square.and.arrow.up") {
                    exportVisibleECG(snapshot)
                }
                .font(.caption)
                .disabled(!snapshot.ecg.contains { snapshot.range.contains($0.timestamp) })
                Button("Full Screen", systemImage: "arrow.up.left.and.arrow.down.right") {
                    showExpanded = true
                }
                .font(.caption)
                .accessibilityHint("Opens ECG and heart rate graphs in landscape")
            }
            .padding(.top, 2)

            ECGTimelinePlot(
                points: snapshot.ecg,
                range: snapshot.range,
                inspectionTime: inspectionTime,
                fixedYRange: zoomECGRange
            )
            .frame(height: 150)
            .overlay {
                interactionLayer(
                    navigationSnapshot: navigationSnapshot,
                    displayedRange: snapshot.range
                )
            }
        }
    }

    private func expandedContent(
        navigationSnapshot: SignalGraphSnapshot,
        snapshot: SignalGraphSnapshot
    ) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(spacing: 12) {
                Text("ECG")
                    .font(.headline)
                Spacer()
                if pausedEnd != nil {
                    Button("Latest · 5 min", systemImage: "arrow.right.to.line") {
                        resumeLatest()
                    }
                    .font(.caption)
                }
                Button("Export", systemImage: "square.and.arrow.up") {
                    exportVisibleECG(snapshot)
                }
                .font(.caption)
                .disabled(!snapshot.ecg.contains { snapshot.range.contains($0.timestamp) })
                Button("Done") {
                    dismiss()
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.small)
            }

            ECGTimelinePlot(
                points: snapshot.ecg,
                range: snapshot.range,
                inspectionTime: inspectionTime,
                fixedYRange: zoomECGRange
            )
            .frame(maxHeight: .infinity)
            .layoutPriority(1)
            .overlay {
                interactionLayer(
                    navigationSnapshot: navigationSnapshot,
                    displayedRange: snapshot.range
                )
            }

            Text("Heart Rate")
                .font(.caption.weight(.semibold))

            HeartRatePlot(
                points: snapshot.heartRate,
                stageMarkers: snapshot.stageMarkers,
                lastPackageTime: snapshot.lastPackageTime,
                range: snapshot.range,
                inspectionTime: inspectionTime,
                fixedYRange: zoomHeartRateRange
            )
            .frame(height: 90)
            .overlay {
                interactionLayer(
                    navigationSnapshot: navigationSnapshot,
                    displayedRange: snapshot.range
                )
            }
        }
        .padding(10)
        .background(Color(UIColor.systemBackground).ignoresSafeArea())
        .statusBarHidden()
    }

    private func visibleRange(
        endingAt proposedEnd: Date,
        duration: TimeInterval,
        earliest: Date,
        latest: Date
    ) -> ClosedRange<Date> {
        let earliestFullEnd = earliest.addingTimeInterval(duration)
        let minimumEnd = min(earliestFullEnd, latest)
        let end = min(max(proposedEnd, minimumEnd), latest)
        return end.addingTimeInterval(-duration)...end
    }

    private func interactionLayer(
        navigationSnapshot: SignalGraphSnapshot,
        displayedRange: ClosedRange<Date>
    ) -> some View {
        SignalGraphInteractionLayer(
            range: displayedRange,
            onPanChanged: { offset in
                guard frozenSnapshot == nil, zoomBaseDuration == nil else { return }
                if panBaseEnd == nil {
                    panBaseEnd = displayedRange.upperBound
                    panStartedFromHistory = pausedEnd != nil || retainedHistory != nil
                }
                if retainedHistory == nil, (panStartedFromHistory || offset > 0) {
                    retainedHistory = navigationSnapshot
                }
                panOffset = offset
            },
            onPanEnded: { offset in
                guard frozenSnapshot == nil, zoomBaseDuration == nil else { return }
                let base = panBaseEnd ?? displayedRange.upperBound
                let proposedEnd = base.addingTimeInterval(-offset)
                if panStartedFromHistory {
                    pausedEnd = min(proposedEnd, navigationSnapshot.latestSignalTime)
                } else {
                    pausedEnd = SignalTimelineScale.resolvedPausedEnd(
                        proposedEnd: proposedEnd,
                        latest: navigationSnapshot.latestSignalTime,
                        visibleDuration: visibleDuration
                    )
                    if pausedEnd != nil, retainedHistory == nil {
                        retainedHistory = navigationSnapshot
                    } else if pausedEnd == nil {
                        retainedHistory = nil
                    }
                }
                panBaseEnd = nil
                panOffset = 0
                panStartedFromHistory = false
            },
            onZoomChanged: { magnification in
                guard frozenSnapshot == nil else { return }
                if zoomBaseDuration == nil {
                    panBaseEnd = nil
                    panOffset = 0
                    panStartedFromHistory = false
                    zoomWasFollowingLatest = pausedEnd == nil
                    zoomBaseDuration = visibleDuration
                    zoomBaseCenter = displayedRange.lowerBound.addingTimeInterval(
                        displayedRange.upperBound.timeIntervalSince(displayedRange.lowerBound) / 2
                    )
                    if pausedEnd != nil, retainedHistory == nil {
                        retainedHistory = navigationSnapshot
                    }
                    zoomHeartRateRange = HeartRateGraphScale.displayRange(
                        for: navigationSnapshot.heartRate.map {
                            (timestamp: $0.timestamp, bpm: $0.bpm)
                        },
                        in: displayedRange
                    )
                    zoomECGRange = ECGTraceGeometry.displayRange(
                        for: navigationSnapshot.ecg,
                        in: displayedRange
                    )
                }
                visibleDuration = SignalTimelineScale.zoomedDuration(
                    baseDuration: zoomBaseDuration ?? visibleDuration,
                    magnification: Double(magnification)
                )
            },
            onZoomEnded: { magnification in
                guard let baseDuration = zoomBaseDuration else { return }
                let duration = SignalTimelineScale.zoomedDuration(
                    baseDuration: baseDuration,
                    magnification: Double(magnification)
                )
                let proposedEnd: Date
                if zoomWasFollowingLatest {
                    proposedEnd = navigationSnapshot.latestSignalTime
                } else if let baseCenter = zoomBaseCenter {
                    proposedEnd = baseCenter.addingTimeInterval(duration / 2)
                } else {
                    proposedEnd = displayedRange.upperBound
                }
                visibleDuration = duration
                let resolvedPausedEnd = zoomWasFollowingLatest
                    ? nil
                    : min(proposedEnd, navigationSnapshot.latestSignalTime)
                pausedEnd = resolvedPausedEnd
                if resolvedPausedEnd == nil {
                    retainedHistory = nil
                } else if retainedHistory == nil {
                    retainedHistory = frozenSnapshot ?? navigationSnapshot
                }
                zoomBaseDuration = nil
                zoomBaseCenter = nil
                zoomWasFollowingLatest = false
                zoomHeartRateRange = nil
                zoomECGRange = nil
            },
            onInspectionChanged: { timestamp in
                if frozenSnapshot == nil {
                    // A small drift before the long press recognizes can start
                    // the simultaneous pan gesture. Drop that transient state
                    // before freezing so inspection cannot leave a hidden pan.
                    panBaseEnd = nil
                    panOffset = 0
                    panStartedFromHistory = false
                    zoomBaseDuration = nil
                    zoomBaseCenter = nil
                    zoomWasFollowingLatest = false
                    zoomHeartRateRange = nil
                    zoomECGRange = nil
                    if pausedEnd == nil {
                        retainedHistory = nil
                    }
                    frozenSnapshot = SignalGraphSnapshot(
                        heartRate: navigationSnapshot.heartRate,
                        ecg: navigationSnapshot.ecg,
                        stageMarkers: navigationSnapshot.stageMarkers,
                        lastPackageTime: navigationSnapshot.lastPackageTime,
                        latestSignalTime: navigationSnapshot.latestSignalTime,
                        range: displayedRange
                    )
                }
                inspectionTime = min(
                    max(timestamp, displayedRange.lowerBound),
                    displayedRange.upperBound
                )
            },
            onInspectionEnded: {
                inspectionTime = nil
                frozenSnapshot = nil
                panBaseEnd = nil
                panOffset = 0
                panStartedFromHistory = false
                if pausedEnd == nil {
                    retainedHistory = nil
                }
            }
        )
    }

    private func resumeLatest() {
        visibleDuration = SignalTimelineScale.historyDuration
        pausedEnd = nil
        panBaseEnd = nil
        panOffset = 0
        panStartedFromHistory = false
        zoomBaseDuration = nil
        zoomBaseCenter = nil
        zoomWasFollowingLatest = false
        zoomHeartRateRange = nil
        zoomECGRange = nil
        retainedHistory = nil
        frozenSnapshot = nil
    }

    private func exportVisibleECG(_ snapshot: SignalGraphSnapshot) {
        guard ECGGraphExport.hasDirectory(ecgExportDirectoryBookmark) else {
            pendingExport = snapshot
            choosingECGExportDirectory = true
            return
        }
        saveECGExport(snapshot)
    }

    @MainActor
    private func saveECGExport(_ snapshot: SignalGraphSnapshot) {
        let bookmark = ecgExportDirectoryBookmark
        Task { @MainActor in
            do {
                let files = try await ECGGraphExport.save(snapshot: snapshot, bookmark: bookmark)
                exportMessage = "Exported \(files.csv.lastPathComponent) and \(files.png.lastPathComponent)."
            } catch {
                exportMessage = "ECG export failed: \(error.localizedDescription)"
            }
        }
    }
}

@MainActor
private enum SignalGraphOrientation {
    private static var restorationGeneration = 0

    static func enterFullScreen() {
        restorationGeneration += 1
        apply(.landscapeRight)
    }

    static func leaveFullScreen() {
        restorationGeneration += 1
        let generation = restorationGeneration
        apply(.portrait)

        // Keep portrait locked through the dismissal animation, then restore
        // the app's normal orientation support without changing the geometry.
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
            guard restorationGeneration == generation else { return }
            AppDelegate.supportedOrientationMask = .all
            activeRootViewController()?.setNeedsUpdateOfSupportedInterfaceOrientations()
        }
    }

    private static func apply(_ orientations: UIInterfaceOrientationMask) {
        guard UIDevice.current.userInterfaceIdiom == .phone,
              let scene = UIApplication.shared.connectedScenes
                .compactMap({ $0 as? UIWindowScene })
                .first(where: { $0.activationState == .foregroundActive })
        else { return }

        AppDelegate.supportedOrientationMask = orientations
        activeRootViewController()?.setNeedsUpdateOfSupportedInterfaceOrientations()
        scene.requestGeometryUpdate(
            .iOS(interfaceOrientations: orientations)
        ) { error in
            CustomLogger.log("Could not rotate signal graphs: \(error.localizedDescription)")
        }
        UIViewController.attemptRotationToDeviceOrientation()
    }

    private static func activeRootViewController() -> UIViewController? {
        UIApplication.shared.connectedScenes
            .compactMap { $0 as? UIWindowScene }
            .first(where: { $0.activationState == .foregroundActive })?
            .windows
            .first(where: \.isKeyWindow)?
            .rootViewController
    }
}

private struct HeartRatePlot: View {
    let points: [HeartRatePlotPoint]
    let stageMarkers: [Date]
    let lastPackageTime: Date?
    let range: ClosedRange<Date>
    let inspectionTime: Date?
    var fixedYRange: ClosedRange<Double>? = nil

    private var visiblePoints: [HeartRatePlotPoint] {
        points.filter { range.contains($0.timestamp) }
    }

    private var inspectedPoint: HeartRatePlotPoint? {
        guard let inspectionTime else { return nil }
        return visiblePoints.min {
            abs($0.timestamp.timeIntervalSince(inspectionTime))
                < abs($1.timestamp.timeIntervalSince(inspectionTime))
        }
    }

    var body: some View {
        Canvas { context, size in
            let duration = max(range.upperBound.timeIntervalSince(range.lowerBound), 0.001)
            let plotWidth = max(
                size.width - SignalChartLayout.leading - SignalChartLayout.trailing,
                1
            )
            let plotHeight = max(
                size.height - SignalChartLayout.top - SignalChartLayout.timeAxisHeight,
                1
            )
            let displayRange = fixedYRange ?? HeartRateGraphScale.displayRange(
                // Scale to what is on screen so an outlier outside the current
                // time window cannot flatten the useful signal. displayRange
                // includes every supplied value, so visible points are never
                // percentile-clipped or drawn at a misleading Y coordinate.
                for: points.map { (timestamp: $0.timestamp, bpm: $0.bpm) },
                in: range
            )
            let ySpan = max(displayRange.upperBound - displayRange.lowerBound, 0.001)
            let yTicks = HeartRateGraphScale.axisTicks(
                minimum: displayRange.lowerBound,
                maximum: displayRange.upperBound
            )
            let timeTicks = SignalTimelineScale.timeTicks(in: range)

            func xPosition(_ date: Date) -> CGFloat {
                SignalChartLayout.leading
                    + CGFloat(date.timeIntervalSince(range.lowerBound) / duration) * plotWidth
            }
            func yPosition(_ bpm: Double) -> CGFloat {
                let clamped = min(max(bpm, displayRange.lowerBound), displayRange.upperBound)
                return SignalChartLayout.top
                    + CGFloat(1 - (clamped - displayRange.lowerBound) / ySpan) * plotHeight
            }

            for tick in timeTicks {
                let x = xPosition(tick)
                var line = Path()
                line.move(to: CGPoint(x: x, y: SignalChartLayout.top))
                line.addLine(to: CGPoint(x: x, y: SignalChartLayout.top + plotHeight))
                context.stroke(line, with: .color(.secondary.opacity(0.16)), lineWidth: 0.5)
                context.draw(
                    Text(
                        SignalTimelineScale.timeLabel(
                            for: tick,
                            visibleDuration: duration
                        )
                    )
                    .font(.system(size: 8).monospacedDigit())
                    .foregroundColor(.secondary),
                    at: CGPoint(
                        x: SignalChartLayout.timeLabelX(
                            for: x,
                            canvasWidth: size.width
                        ),
                        y: SignalChartLayout.top + plotHeight + 4
                    ),
                    anchor: .top
                )
            }

            for tick in yTicks {
                let y = yPosition(tick)
                var line = Path()
                line.move(to: CGPoint(x: SignalChartLayout.leading, y: y))
                line.addLine(to: CGPoint(x: size.width - SignalChartLayout.trailing, y: y))
                context.stroke(
                    line,
                    with: .color(.secondary.opacity(0.25)),
                    style: StrokeStyle(lineWidth: 0.5, dash: [3, 3])
                )
                context.draw(
                    Text(String(format: "%.0f", tick))
                        .font(.caption2)
                        .foregroundColor(.secondary),
                    at: CGPoint(x: SignalChartLayout.leading - 4, y: y),
                    anchor: .trailing
                )
            }

            var path = Path()
            for (index, point) in visiblePoints.enumerated() {
                let position = CGPoint(
                    x: xPosition(point.timestamp),
                    y: yPosition(point.bpm)
                )
                if index == 0 || point.beginsNewSegment {
                    path.move(to: position)
                } else {
                    path.addLine(to: position)
                }
            }
            context.stroke(path, with: .color(.green.opacity(0.65)), lineWidth: 1)

            for point in visiblePoints {
                let center = CGPoint(
                    x: xPosition(point.timestamp),
                    y: yPosition(point.bpm)
                )
                context.fill(
                    Path(
                        ellipseIn: CGRect(
                            x: center.x - 1.6,
                            y: center.y - 1.6,
                            width: 3.2,
                            height: 3.2
                        )
                    ),
                    with: .color(.green)
                )
            }

            for marker in stageMarkers where range.contains(marker) {
                let x = xPosition(marker)
                var line = Path()
                line.move(to: CGPoint(x: x, y: SignalChartLayout.top))
                line.addLine(to: CGPoint(x: x, y: SignalChartLayout.top + plotHeight))
                context.stroke(line, with: .color(.red), lineWidth: 1)
            }

            if let lastPackageTime, range.contains(lastPackageTime) {
                let x = xPosition(lastPackageTime)
                var line = Path()
                line.move(to: CGPoint(x: x, y: SignalChartLayout.top))
                line.addLine(to: CGPoint(x: x, y: SignalChartLayout.top + plotHeight))
                context.stroke(line, with: .color(.blue), lineWidth: 1)
            }

            if let inspectionTime {
                let x = xPosition(inspectionTime)
                var line = Path()
                line.move(to: CGPoint(x: x, y: SignalChartLayout.top))
                line.addLine(to: CGPoint(x: x, y: SignalChartLayout.top + plotHeight))
                context.stroke(line, with: .color(.primary), lineWidth: 1)
            }
        }
        .background(Color(UIColor.secondarySystemBackground))
        .clipShape(RoundedRectangle(cornerRadius: 8))
        .overlay(alignment: .topTrailing) {
            if let point = inspectedPoint {
                Text(
                    "\(Int(point.bpm.rounded())) bpm · "
                        + SignalTimelineScale.timeLabel(
                            for: point.timestamp,
                            visibleDuration: range.upperBound.timeIntervalSince(
                                range.lowerBound
                            )
                        )
                )
                .font(.caption2.monospacedDigit())
                .padding(.horizontal, 6)
                .padding(.vertical, 3)
                .background(.thinMaterial, in: Capsule())
                .padding(5)
            }
        }
    }
}

/// Drawing the individual samples preserves detail at every zoom level.
/// Never connect across a missing interval of more than 100 milliseconds.
enum ECGTraceGeometry {
    static let maximumConnectedInterval: TimeInterval = 0.1

    static func displayRange(
        for points: [ECGPlotPoint],
        in range: ClosedRange<Date>
    ) -> ClosedRange<Double> {
        let values = points.lazy.filter { range.contains($0.timestamp) }.map { Double($0.voltage) }
        guard let minimum = values.min(), let maximum = values.max() else { return -1...1 }
        guard minimum == maximum else { return minimum...maximum }
        let padding = max(abs(minimum) * 0.05, 1)
        return (minimum - padding)...(maximum + padding)
    }

    static func path(
        for points: [ECGPlotPoint],
        in range: ClosedRange<Date>,
        size: CGSize,
        fixedYRange: ClosedRange<Double>? = nil
    ) -> Path {
        let duration = range.upperBound.timeIntervalSince(range.lowerBound)
        var visible = points.filter { range.contains($0.timestamp) }
        if zip(visible, visible.dropFirst()).contains(where: { $0.0.timestamp > $0.1.timestamp }) {
            visible.sort { $0.timestamp < $1.timestamp }
        }
        guard duration > 0, duration.isFinite,
              size.width > 0, size.height > 0,
              !visible.isEmpty else { return Path() }
        let voltageRange = fixedYRange ?? displayRange(for: visible, in: range)
        let voltageMin = voltageRange.lowerBound
        let voltageSpan = max(voltageRange.upperBound - voltageMin, 1)
        var path = Path()
        var previousTimestamp: Date?

        for sample in visible {
            if let previousTimestamp, sample.timestamp == previousTimestamp { continue }
            let point = CGPoint(
                x: sample.timestamp.timeIntervalSince(range.lowerBound) / duration * size.width,
                y: (1 - (Double(sample.voltage) - voltageMin) / voltageSpan) * size.height
            )
            let separation = previousTimestamp.map { sample.timestamp.timeIntervalSince($0) }
            if let separation, separation >= 0, separation <= maximumConnectedInterval {
                path.addLine(to: point)
            } else {
                // A short mark also makes an isolated sample visible.
                path.move(to: CGPoint(x: point.x - 0.5, y: point.y))
                path.addLine(to: point)
            }
            previousTimestamp = sample.timestamp
        }
        return path
    }
}

private struct ECGTimelinePlot: View {
    let points: [ECGPlotPoint]
    let range: ClosedRange<Date>
    let inspectionTime: Date?
    var fixedYRange: ClosedRange<Double>? = nil
    var alwaysRender = false

    private var visiblePoints: [ECGPlotPoint] {
        points.filter { range.contains($0.timestamp) }
    }

    private var inspectedPoint: ECGPlotPoint? {
        guard let inspectionTime else { return nil }
        let point = visiblePoints.min {
            abs($0.timestamp.timeIntervalSince(inspectionTime))
                < abs($1.timestamp.timeIntervalSince(inspectionTime))
        }
        guard let point,
              abs(point.timestamp.timeIntervalSince(inspectionTime)) <= 0.1
        else { return nil }
        return point
    }

    var body: some View {
        Canvas { context, size in
            let duration = max(range.upperBound.timeIntervalSince(range.lowerBound), 0.001)
            let plotWidth = max(
                size.width - SignalChartLayout.leading - SignalChartLayout.trailing,
                1
            )
            let plotHeight = max(
                size.height - SignalChartLayout.top - SignalChartLayout.timeAxisHeight,
                1
            )
            let timeTicks = SignalTimelineScale.timeTicks(in: range)
            let visible = visiblePoints

            func xPosition(_ date: Date) -> CGFloat {
                SignalChartLayout.leading
                    + CGFloat(date.timeIntervalSince(range.lowerBound) / duration) * plotWidth
            }

            for tick in timeTicks {
                let x = xPosition(tick)
                var line = Path()
                line.move(to: CGPoint(x: x, y: SignalChartLayout.top))
                line.addLine(to: CGPoint(x: x, y: SignalChartLayout.top + plotHeight))
                context.stroke(line, with: .color(.secondary.opacity(0.16)), lineWidth: 0.5)
                context.draw(
                    Text(
                        SignalTimelineScale.timeLabel(
                            for: tick,
                            visibleDuration: duration
                        )
                    )
                    .font(.system(size: 8).monospacedDigit())
                    .foregroundColor(.secondary),
                    at: CGPoint(
                        x: SignalChartLayout.timeLabelX(
                            for: x,
                            canvasWidth: size.width
                        ),
                        y: SignalChartLayout.top + plotHeight + 4
                    ),
                    anchor: .top
                )
            }

            if alwaysRender || SignalTimelineScale.shouldRenderECG(visibleDuration: duration) {
                if visible.isEmpty {
                    context.draw(
                        Text("No ECG in this window")
                            .font(.caption)
                            .foregroundColor(.secondary),
                        at: CGPoint(
                            x: SignalChartLayout.leading + plotWidth / 2,
                            y: SignalChartLayout.top + plotHeight / 2
                        )
                    )
                } else {
                    var traceContext = context
                    traceContext.translateBy(
                        x: SignalChartLayout.leading,
                        y: SignalChartLayout.top
                    )
                    traceContext.clip(to: Path(CGRect(x: 0, y: 0, width: plotWidth, height: plotHeight)))
                    let path = ECGTraceGeometry.path(
                        for: visible,
                        in: range,
                        size: CGSize(width: plotWidth, height: plotHeight),
                        fixedYRange: fixedYRange
                    )
                    traceContext.stroke(
                        path,
                        with: .color(.green),
                        style: StrokeStyle(
                            lineWidth: 1,
                            lineCap: .round,
                            lineJoin: .round
                        )
                    )
                }
            } else {
                context.draw(
                    Text("Zoom in to view ECG")
                        .font(.caption)
                        .foregroundColor(.secondary),
                    at: CGPoint(
                        x: SignalChartLayout.leading + plotWidth / 2,
                        y: SignalChartLayout.top + plotHeight / 2
                    )
                )
            }

            if let inspectionTime {
                let x = xPosition(inspectionTime)
                var line = Path()
                line.move(to: CGPoint(x: x, y: SignalChartLayout.top))
                line.addLine(to: CGPoint(x: x, y: SignalChartLayout.top + plotHeight))
                context.stroke(line, with: .color(.primary), lineWidth: 1)
            }
        }
        .background(Color(UIColor.secondarySystemBackground))
        .clipShape(RoundedRectangle(cornerRadius: 8))
        .overlay(alignment: .topTrailing) {
            if let point = inspectedPoint,
               SignalTimelineScale.shouldRenderECG(
                visibleDuration: range.upperBound.timeIntervalSince(range.lowerBound)
               )
            {
                Text("\(point.voltage) µV")
                    .font(.caption2.monospacedDigit())
                    .padding(.horizontal, 6)
                    .padding(.vertical, 3)
                    .background(.thinMaterial, in: Capsule())
                    .padding(5)
            }
        }
    }
}

/// Saves the visible ECG samples and a rendered image of the same ECG/HR window.
enum ECGGraphExport {
    static func bookmark(for url: URL) throws -> Data {
        let hasAccess = url.startAccessingSecurityScopedResource()
        defer { if hasAccess { url.stopAccessingSecurityScopedResource() } }
        return try url.bookmarkData(options: .minimalBookmark, includingResourceValuesForKeys: nil, relativeTo: nil)
    }

    static func directoryName(for bookmark: Data) -> String {
        guard let url = resolve(bookmark) else { return "Not selected" }
        return url.lastPathComponent
    }

    static func hasDirectory(_ bookmark: Data) -> Bool {
        resolve(bookmark) != nil
    }

    static func fileStamp(for date: Date) -> String {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.dateFormat = "yyMMdd_HHmmss"
        return formatter.string(from: date)
    }

    static func csv(for points: [ECGPlotPoint], in range: ClosedRange<Date>) -> Data {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        var text = "timestamp,voltage_uV\n"
        text.reserveCapacity(points.count * 36)
        for point in points.filter({ range.contains($0.timestamp) }).sorted(by: { $0.timestamp < $1.timestamp }) {
            text += "\(formatter.string(from: point.timestamp)),\(point.voltage)\n"
        }
        return Data(text.utf8)
    }

    @MainActor
    static func save(
        snapshot: SignalGraphSnapshot,
        bookmark: Data
    ) async throws -> (csv: URL, png: URL) {
        guard snapshot.ecg.contains(where: { snapshot.range.contains($0.timestamp) }) else {
            throw exportError("There are no ECG samples in the visible window.")
        }
        let renderer = ImageRenderer(content: ECGGraphExportImage(snapshot: snapshot))
        renderer.scale = 2
        guard let pngData = renderer.uiImage?.pngData() else {
            throw exportError("Could not render the ECG image.")
        }
        let csvData = csv(for: snapshot.ecg, in: snapshot.range)
        let stampDate = snapshot.range.upperBound
        return try await Task.detached(priority: .utility) {
            try write(csvData: csvData, pngData: pngData, stampDate: stampDate, bookmark: bookmark)
        }.value
    }

    private static func write(
        csvData: Data,
        pngData: Data,
        stampDate: Date,
        bookmark: Data
    ) throws -> (csv: URL, png: URL) {
        guard let directory = resolve(bookmark) else {
            throw exportError("Choose an ECG export directory in Settings.")
        }
        let hasAccess = directory.startAccessingSecurityScopedResource()
        defer { if hasAccess { directory.stopAccessingSecurityScopedResource() } }
        let folder = directory.appendingPathComponent("ECG Exports", isDirectory: true)
        let coordinator = NSFileCoordinator(filePresenter: nil)
        var coordinationError: NSError?
        var result: Result<(csv: URL, png: URL), Error>?
        coordinator.coordinate(writingItemAt: folder, options: [], error: &coordinationError) { coordinatedFolder in
            result = Result {
                try FileManager.default.createDirectory(at: coordinatedFolder, withIntermediateDirectories: true)
                let stamp = fileStamp(for: stampDate)
                var suffix = 0
                var csvURL: URL
                var pngURL: URL
                repeat {
                    let base = "ECG_\(stamp)" + (suffix == 0 ? "" : "_\(suffix)")
                    csvURL = coordinatedFolder.appendingPathComponent(base + ".csv")
                    pngURL = coordinatedFolder.appendingPathComponent(base + ".png")
                    suffix += 1
                } while FileManager.default.fileExists(atPath: csvURL.path)
                    || FileManager.default.fileExists(atPath: pngURL.path)

                try csvData.write(to: csvURL, options: .atomic)
                do {
                    try pngData.write(to: pngURL, options: .atomic)
                } catch {
                    try? FileManager.default.removeItem(at: csvURL)
                    throw error
                }
                return (csvURL, pngURL)
            }
        }
        if let coordinationError { throw coordinationError }
        guard let result else { throw exportError("The export directory was unavailable.") }
        return try result.get()
    }

    private static func resolve(_ bookmark: Data) -> URL? {
        guard !bookmark.isEmpty else { return nil }
        var stale = false
        guard let url = try? URL(
            resolvingBookmarkData: bookmark,
            options: .withoutImplicitStartAccessing,
            relativeTo: nil,
            bookmarkDataIsStale: &stale
        ), !stale else { return nil }
        return url
    }

    private static func exportError(_ message: String) -> NSError {
        NSError(domain: "ECGGraphExport", code: 1, userInfo: [NSLocalizedDescriptionKey: message])
    }
}

private struct ECGGraphExportImage: View {
    let snapshot: SignalGraphSnapshot

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("ECG")
                .font(.headline)
            ECGTimelinePlot(
                points: snapshot.ecg,
                range: snapshot.range,
                inspectionTime: nil,
                alwaysRender: true
            )
            .frame(height: 440)
            Text("Heart Rate")
                .font(.headline)
            HeartRatePlot(
                points: snapshot.heartRate,
                stageMarkers: snapshot.stageMarkers,
                lastPackageTime: snapshot.lastPackageTime,
                range: snapshot.range,
                inspectionTime: nil
            )
            .frame(height: 170)
        }
        .padding(24)
        .frame(width: 1200, height: 720)
        .background(Color(UIColor.systemBackground))
    }
}

private struct SignalGraphInteractionLayer: View {
    let range: ClosedRange<Date>
    let onPanChanged: (TimeInterval) -> Void
    let onPanEnded: (TimeInterval) -> Void
    let onZoomChanged: (CGFloat) -> Void
    let onZoomEnded: (CGFloat) -> Void
    let onInspectionChanged: (Date) -> Void
    let onInspectionEnded: () -> Void

    @State private var isInspecting = false
    @State private var suppressPanEnd = false
    @State private var isPinching = false
    @State private var lastMagnification: CGFloat = 1
    @GestureState private var pinchGestureActive = false
    @GestureState private var dragGestureActive = false

    var body: some View {
        GeometryReader { proxy in
            let plotWidth = max(
                proxy.size.width - SignalChartLayout.leading - SignalChartLayout.trailing,
                1
            )
            let duration = range.upperBound.timeIntervalSince(range.lowerBound)

            Color.clear
                .contentShape(Rectangle())
                .onChange(of: pinchGestureActive) { _, active in
                    if !active { finishPinch(lastMagnification) }
                }
                .onChange(of: dragGestureActive) { _, active in
                    if !active { clearPanSuppressionWhenIdle() }
                }
                .highPriorityGesture(
                    LongPressGesture(minimumDuration: 0.25, maximumDistance: 12)
                        .sequenced(before: DragGesture(minimumDistance: 0))
                        .onChanged { value in
                            guard case .second(true, let drag?) = value else { return }
                            isInspecting = true
                            suppressPanEnd = true
                            onInspectionChanged(
                                timestamp(
                                    for: drag.location.x,
                                    plotWidth: plotWidth,
                                    duration: duration
                                )
                            )
                        }
                        .onEnded { _ in
                            guard isInspecting else { return }
                            isInspecting = false
                            onInspectionEnded()
                            DispatchQueue.main.async {
                                suppressPanEnd = false
                            }
                        }
                )
                .simultaneousGesture(
                    DragGesture(minimumDistance: 8)
                        .updating($dragGestureActive) { _, active, _ in active = true }
                        .onChanged { value in
                            guard !isInspecting, !isPinching, !suppressPanEnd else { return }
                            onPanChanged(
                                Double(value.translation.width / plotWidth) * duration
                            )
                        }
                        .onEnded { value in
                            let shouldSuppress = isInspecting || isPinching || suppressPanEnd
                            if !isPinching { suppressPanEnd = false }
                            guard !shouldSuppress else { return }
                            onPanEnded(
                                Double(value.translation.width / plotWidth) * duration
                            )
                        }
                )
                .simultaneousGesture(
                    MagnificationGesture()
                        .updating($pinchGestureActive) { _, active, _ in active = true }
                        .onChanged { value in
                            guard !isInspecting else { return }
                            isPinching = true
                            suppressPanEnd = true
                            lastMagnification = value
                            onZoomChanged(value)
                        }
                        .onEnded { value in
                            finishPinch(value)
                        }
                )
        }
    }

    private func finishPinch(_ magnification: CGFloat) {
        guard isPinching else { return }
        onZoomEnded(magnification)
        isPinching = false
        clearPanSuppressionWhenIdle()
    }

    private func clearPanSuppressionWhenIdle() {
        DispatchQueue.main.async {
            if !isPinching && !dragGestureActive { suppressPanEnd = false }
        }
    }

    private func timestamp(
        for locationX: CGFloat,
        plotWidth: CGFloat,
        duration: TimeInterval
    ) -> Date {
        let plotX = min(
            max(locationX - SignalChartLayout.leading, 0),
            plotWidth
        )
        return range.lowerBound.addingTimeInterval(
            Double(plotX / plotWidth) * duration
        )
    }
}
