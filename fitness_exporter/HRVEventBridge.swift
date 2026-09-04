import Combine
import Foundation

/// Bridges BluetoothManager sensor events to UI-friendly published values
/// and maintains the RR-interval graph model. Subscribes once.
final class HRVEventBridge: ObservableObject {
    private struct RRPacket {
        let intervals: [Double]
        let packageTime: Date
    }

    private static let presentationUpdateInterval: TimeInterval = 1

    @Published var rawHR: Int = 0
    @Published var derivedHR: Int = 0
    let graphModel = RRIntervalGraphModel()

    private var subscriptions = Set<AnyCancellable>()
    private let processingQueue = DispatchQueue(
        label: "com.fitness_exporter.hrvPresentation",
        qos: .utility
    )
    private var recentPackets: [RRPacket] = []
    private var pendingPackets: [RRPacket] = []
    private var latestRawHR = 0
    private var latestDerivedHR = 0
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
            self.pendingPackets.removeAll(keepingCapacity: true)
            self.needsFullSnapshot = true
            if active {
                self.schedulePresentationUpdateIfNeeded()
            }
        }
    }

    func resetGraph() {
        processingQueue.async { [weak self] in
            guard let self else { return }
            self.recentPackets.removeAll()
            self.pendingPackets.removeAll()
            self.latestRawHR = 0
            self.latestDerivedHR = 0
            self.needsFullSnapshot = true
            DispatchQueue.main.async { [weak self] in
                self?.rawHR = 0
                self?.derivedHR = 0
                self?.graphModel.reset()
            }
        }
    }

    private func process(_ event: SensorEvent) {
        guard case .hrSamples(let samples) = event.data else { return }

        if let sample = samples.samples.last {
            latestRawHR = sample.value
            if let last = sample.rrIntervals.last, last > 0 {
                latestDerivedHR = Int((60.0 / last).rounded())
            }
        }

        let intervals = samples.samples.flatMap { $0.rrIntervals }
        if !intervals.isEmpty {
            let packet = RRPacket(intervals: intervals, packageTime: event.timestamp)
            recentPackets.append(packet)
            if presentationActive {
                pendingPackets.append(packet)
            }
            let cutoff = event.timestamp.addingTimeInterval(-graphModel.window)
            recentPackets.removeAll { $0.packageTime < cutoff }
        }

        if presentationActive {
            schedulePresentationUpdateIfNeeded()
        }
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

            let replace = self.needsFullSnapshot
            let packets = replace ? self.recentPackets : self.pendingPackets
            self.pendingPackets.removeAll(keepingCapacity: true)
            self.needsFullSnapshot = false
            let graphSamples = Self.makeGraphSamples(from: packets)
            let lastPackageTime = packets.last?.packageTime
            let rawHR = self.latestRawHR
            let derivedHR = self.latestDerivedHR

            DispatchQueue.main.async { [weak self] in
                guard let self else { return }
                self.rawHR = rawHR
                self.derivedHR = derivedHR
                if replace {
                    self.graphModel.replace(
                        samples: graphSamples,
                        lastPackageTime: lastPackageTime
                    )
                } else if !graphSamples.isEmpty {
                    self.graphModel.append(
                        samples: graphSamples,
                        lastPackageTime: lastPackageTime
                    )
                }
            }
        }
    }

    private static func makeGraphSamples(
        from packets: [RRPacket]
    ) -> [RRIntervalGraphModel.Sample] {
        var graphSamples: [RRIntervalGraphModel.Sample] = []
        graphSamples.reserveCapacity(
            packets.reduce(0) { $0 + $1.intervals.count }
        )
        for packet in packets {
            var inferredPackageTime = packet.packageTime
            for rr in packet.intervals.reversed() {
                graphSamples.append(
                    RRIntervalGraphModel.Sample(
                        rr: rr,
                        received: inferredPackageTime
                    )
                )
                inferredPackageTime -= rr
            }
        }
        return graphSamples
    }
}
