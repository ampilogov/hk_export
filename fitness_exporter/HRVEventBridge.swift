import Combine
import Foundation

/// Bridges BluetoothManager sensor events to UI-friendly published values
/// and maintains the RR-interval graph model. Subscribes once.
final class HRVEventBridge: ObservableObject {
    private static let presentationUpdateInterval: TimeInterval = 1

    @Published var rawHR: Int = 0
    @Published var derivedHR: Int = 0
    @Published private(set) var lastRR: Date?
    @Published private(set) var lastECG: Date?
    @Published private(set) var lastACC: Date?
    @Published private(set) var recentECGPoints: [ECGPlotPoint] = []
    let graphModel = RRIntervalGraphModel()

    private var subscriptions = Set<AnyCancellable>()
    private let processingQueue = DispatchQueue(
        label: "com.fitness_exporter.hrvPresentation",
        qos: .utility
    )
    private var recentGraphSampleBuffer: [RRIntervalGraphModel.Sample] = []
    private var pendingGraphSamples: [RRIntervalGraphModel.Sample] = []
    private var beatTimeline = RRBeatTimelineReconstructor()
    private var recentECGPointBuffer: [ECGPlotPoint] = []
    private var ecgTimeline = SensorBagPersistence.ECGTimelineReconstructor()
    private var graphWindowEnd: Date?
    private var latestRawHR = 0
    private var latestDerivedHR = 0
    private var latestRR: Date?
    private var latestECG: Date?
    private var latestACC: Date?
    private var presentationActive = false
    private var needsFullSnapshot = true
    private var presentationUpdateScheduled = false

    init(manager: BluetoothManager) {
        manager.sensorPublisher
            .receive(on: processingQueue)
            .sink { [weak self] event in
                self?.process(event)
            }
            .store(in: &subscriptions)
    }

    func setPresentationActive(_ active: Bool) {
        processingQueue.async { [weak self] in
            guard let self else { return }
            guard self.presentationActive != active else { return }
            self.presentationActive = active
            self.pendingGraphSamples.removeAll(keepingCapacity: true)
            self.needsFullSnapshot = true
            if active {
                self.publishPresentationUpdate()
            }
        }
    }

    /// Clear only the plotted RR-derived heart-rate history. Sensor streaming,
    /// current heart rate, and the live ECG buffer continue uninterrupted.
    func clearHeartRateGraph() {
        processingQueue.async { [weak self] in
            guard let self else { return }
            self.recentGraphSampleBuffer.removeAll()
            self.pendingGraphSamples.removeAll()
            self.beatTimeline.reset()
            self.needsFullSnapshot = true
            DispatchQueue.main.async { [weak self] in
                self?.graphModel.reset()
            }
        }
    }

    private func process(_ event: SensorEvent) {
        switch event.data {
        case .hrSamples(let samples):
            if let sample = samples.samples.last {
                latestRawHR = sample.value
                if let last = sample.rrIntervals.last, last > 0 {
                    latestDerivedHR = Int((60.0 / last).rounded())
                }
            }

            let intervals = samples.samples.flatMap { $0.rrIntervals }
            if !intervals.isEmpty {
                latestRR = event.timestamp
                let newGraphSamples = beatTimeline.append(
                    intervals: intervals,
                    receivedAt: event.timestamp
                )
                recentGraphSampleBuffer.append(contentsOf: newGraphSamples)
                if presentationActive {
                    pendingGraphSamples.append(contentsOf: newGraphSamples)
                }
            }
            advanceGraphWindow(to: event.timestamp)

        case .ecgSamples(let samples):
            guard !samples.samples.isEmpty else { return }
            latestECG = event.timestamp
            let newPoints = SensorBagPersistence.ecgPoints(
                from: event,
                timeline: &ecgTimeline
            )
            guard !newPoints.isEmpty else { return }
            recentECGPointBuffer.append(contentsOf: newPoints)
            advanceGraphWindow(to: event.timestamp)

        case .accSamples(let samples):
            guard !samples.samples.isEmpty else { return }
            latestACC = event.timestamp
            advanceGraphWindow(to: event.timestamp)

        case .battery, .hrvStage, .location, .custom:
            return
        }

        if presentationActive {
            schedulePresentationUpdateIfNeeded()
        }
    }

    private func advanceGraphWindow(to timestamp: Date) {
        let windowEnd = max(graphWindowEnd ?? timestamp, timestamp)
        graphWindowEnd = windowEnd
        let cutoff = windowEnd.addingTimeInterval(-graphModel.window)
        recentGraphSampleBuffer.removeAll { $0.received < cutoff }
        pendingGraphSamples.removeAll { $0.received < cutoff }
        recentECGPointBuffer.removeAll { $0.timestamp < cutoff }
    }

    private func schedulePresentationUpdateIfNeeded() {
        guard !presentationUpdateScheduled else { return }
        presentationUpdateScheduled = true
        processingQueue.asyncAfter(
            deadline: .now() + Self.presentationUpdateInterval
        ) { [weak self] in
            guard let self else { return }
            self.presentationUpdateScheduled = false
            guard self.presentationActive else { return }
            self.publishPresentationUpdate()
        }
    }

    private func publishPresentationUpdate() {
        dispatchPrecondition(condition: .onQueue(processingQueue))
        guard presentationActive else { return }

        let replace = needsFullSnapshot
        let graphSamples = replace ? recentGraphSampleBuffer : pendingGraphSamples
        pendingGraphSamples.removeAll(keepingCapacity: true)
        needsFullSnapshot = false
        let lastPackageTime = latestRR
        let rawHR = latestRawHR
        let derivedHR = latestDerivedHR
        let lastRR = latestRR
        let lastECG = latestECG
        let lastACC = latestACC
        let recentECGPoints = recentECGPointBuffer
        let graphWindowEnd = graphWindowEnd

        DispatchQueue.main.async { [weak self] in
            guard let self else { return }
            self.rawHR = rawHR
            self.derivedHR = derivedHR
            self.lastRR = lastRR
            self.lastECG = lastECG
            self.lastACC = lastACC
            self.recentECGPoints = recentECGPoints
            if replace {
                self.graphModel.replace(
                    samples: graphSamples,
                    lastPackageTime: lastPackageTime,
                    windowEnd: graphWindowEnd
                )
            } else if !graphSamples.isEmpty {
                self.graphModel.append(
                    samples: graphSamples,
                    lastPackageTime: lastPackageTime,
                    windowEnd: graphWindowEnd
                )
            } else if let graphWindowEnd {
                self.graphModel.prune(relativeTo: graphWindowEnd)
            }
        }
    }

}
