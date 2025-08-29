import Combine
import Foundation

/// Bridges BluetoothManager sensor events to UI-friendly published values
/// and maintains the RR-interval graph model. Subscribes once.
final class HRVEventBridge: ObservableObject {
    @Published var rawHR: Int = 0
    @Published var derivedHR: Int = 0
    let graphModel = RRIntervalGraphModel()

    private var subscriptions = Set<AnyCancellable>()

    init(manager: BluetoothManager) {
        manager.sensorPublisher
            .receive(on: DispatchQueue.main)
            .sink { [weak self] event in
                guard let self = self else { return }
                switch event.data {
                case .hrSamples(let samples):
                    if let sample = samples.samples.last {
                        self.rawHR = sample.value
                        if let last = sample.rrIntervals.last {
                            self.derivedHR = Int((60.0 / last).rounded())
                        }
                    }
                    let rrs = samples.samples.flatMap { $0.rrIntervals }
                    self.graphModel.append(intervals: rrs, packageTime: event.timestamp)
                default:
                    break
                }
            }
            .store(in: &subscriptions)
    }

    func resetGraph() {
        graphModel.reset()
    }
}

