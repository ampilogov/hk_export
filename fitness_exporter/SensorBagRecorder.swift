import Combine
import Foundation

/// Collection of timestamped sensor events.
final class SensorBag {
    private var events: [SensorEvent] = []
    private let queue = DispatchQueue(label: "SensorBag.queue")

    func addEvent(_ event: SensorEvent) {
        //        print("New event: \(event)")
        queue.sync { events.append(event) }
    }

    var snapshot: [SensorEvent] { queue.sync { events } }

    func reset() {
        queue.sync { events.removeAll() }
    }
}

extension SensorBag {
    /// Saves collected sensor data into a binary file.
    func saveBinary(to fileURL: URL) throws {
        let data = _serializeV1(snapshot)
        try data.write(to: fileURL)
    }

    func _serializeV1(_ events: [SensorEvent]) -> Data {
        var data = Data()

        func _append<T>(_ value: T) {
            var v = value
            withUnsafeBytes(of: &v) { data.append(contentsOf: $0) }
        }

        func appendDouble(_ value: Double) {
            _append(value)
        }

        func appendInt16(_ value: Int16) {
            _append(value)
        }

        func appendInt32(_ value: Int32) {
            _append(value)
        }

        func appendUInt32(_ value: UInt32) {
            _append(value)
        }

        func appendUInt64(_ value: UInt64) {
            _append(value)
        }

        appendUInt32(1) // version
        appendUInt32(UInt32(events.count))
        for event in events {
            appendDouble(event.timestamp.timeIntervalSince1970)
            switch event.data {
            case .hrSamples(let sample):
                appendUInt32(1)
                appendUInt32(UInt32(sample.samples.count))
                for s in sample.samples {
                    appendInt32(Int32(s.value))
                    appendUInt32(UInt32(s.rrIntervals.count))
                    for r in s.rrIntervals { appendDouble(r) }
                }
            case .ecgSamples(let sample):
                appendUInt32(2)
                appendUInt32(UInt32(sample.samples.count))
                appendUInt64(sample.samples.first?.timestamp ?? UInt64.max)
                appendUInt64(sample.samples.last?.timestamp ?? UInt64.max)
                for s in sample.samples {
                    appendInt16(s.voltage)
                }
            case .accSamples(let sample):
                appendUInt32(3)
                appendUInt32(UInt32(sample.samples.count))
                appendUInt64(sample.samples.first?.timestamp ?? UInt64.max)
                appendUInt64(sample.samples.last?.timestamp ?? UInt64.max)
                for s in sample.samples {
                    appendInt16(s.x)
                    appendInt16(s.y)
                    appendInt16(s.z)
                }
            case .battery(let sample):
                appendUInt32(4)
                appendInt32(Int32(sample.level))
            case .hrvStage(let stage):
                appendUInt32(5)
                let raw = stage.rawValue
                appendUInt32(UInt32(raw.utf8.count))
                data.append(raw.data(using: .utf8) ?? Data())
            case .location(let loc):
                appendUInt32(6)
                appendDouble(loc.latitude)
                appendDouble(loc.longitude)
                appendDouble(loc.altitude)
                appendDouble(loc.horizontalAccuracy)
                appendDouble(loc.verticalAccuracy)
            case .custom(let message):
                appendUInt32(7)
                appendUInt32(UInt32(message.utf8.count))
                data.append(message.data(using: .utf8) ?? Data())
            }
        }

        return data
    }

    func _serializeV0(_ events: [SensorEvent]) -> Data {
        var data = Data()

        func _append<T>(_ value: T) {
            var v = value
            withUnsafeBytes(of: &v) { data.append(contentsOf: $0) }
        }

        func appendDouble(_ value: Double) {
            _append(value)
        }

        func appendInt16(_ value: Int16) {
            _append(value)
        }

        func appendInt32(_ value: Int32) {
            _append(value)
        }

        func appendUInt32(_ value: UInt32) {
            _append(value)
        }

        func appendUInt64(_ value: UInt64) {
            _append(value)
        }

        appendUInt32(0) // version
        appendUInt32(UInt32(events.count))
        for event in events {
            appendDouble(event.timestamp.timeIntervalSince1970)
            switch event.data {
            case .hrSamples(let sample):
                appendUInt32(1)
                appendUInt32(UInt32(sample.samples.count))
                for s in sample.samples {
                    appendInt32(Int32(s.value))
                    appendUInt32(UInt32(s.rrIntervals.count))
                    for r in s.rrIntervals { appendDouble(r) }
                }
            case .ecgSamples(let sample):
                appendUInt32(2)
                appendUInt32(UInt32(sample.samples.count))
                for s in sample.samples {
                    appendUInt64(s.timestamp)
                    appendInt32(Int32(s.voltage))
                }
            case .accSamples(let sample):
                appendUInt32(3)
                appendUInt32(UInt32(sample.samples.count))
                for s in sample.samples {
                    appendUInt64(s.timestamp)
                    appendInt32(Int32(s.x))
                    appendInt32(Int32(s.y))
                    appendInt32(Int32(s.z))
                }
            case .battery(let sample):
                appendUInt32(4)
                appendInt32(Int32(sample.level))
            case .hrvStage(let stage):
                appendUInt32(5)
                let raw = stage.rawValue
                appendUInt32(UInt32(raw.utf8.count))
                data.append(raw.data(using: .utf8) ?? Data())
            case .location(let loc):
                appendUInt32(6)
                appendDouble(loc.latitude)
                appendDouble(loc.longitude)
                appendDouble(loc.altitude)
                appendDouble(loc.horizontalAccuracy)
                appendDouble(loc.verticalAccuracy)
            case .custom(let message):
                appendUInt32(7)
                appendUInt32(UInt32(message.utf8.count))
                data.append(message.data(using: .utf8) ?? Data())
            }
        }

        return data
    }
}

/// Records events from a manager into a SensorBag.
class SensorBagRecorder: ObservableObject {
    private var cancellables = Set<AnyCancellable>()
    @Published private(set) var bag = SensorBag()
    @Published private(set) var isRecording = false

    func start(with manager: BluetoothManager) {
        guard !isRecording else { return }
        isRecording = true
        manager.sensorPublisher
            .sink { [weak self] event in
                self?.bag.addEvent(event)
            }
            .store(in: &cancellables)
    }

    func stop() -> SensorBag {
        guard isRecording else { return bag }
        isRecording = false
        cancellables.removeAll()
        return bag
    }

    func reset() {
        bag = SensorBag()
    }

    /// Atomically return the current bag and replace it with a fresh one
    /// without interrupting the recording subscription.
    func takeAndReset() -> SensorBag {
        let current = bag
        bag = SensorBag()
        return current
    }

    func markHRVProtocolStage(_ stage: HRVStage, at timestamp: Date = Date()) {
        let event = SensorEvent(timestamp: timestamp, data: .hrvStage(stage))
        bag.addEvent(event)
    }

    func recordLocation(_ sample: LocationSample, at timestamp: Date = Date()) {
        let event = SensorEvent(timestamp: timestamp, data: .location(sample))
        bag.addEvent(event)
    }

    func markCustomEvent(_ message: String, at timestamp: Date = Date()) {
        let event = SensorEvent(timestamp: timestamp, data: .custom(message))
        bag.addEvent(event)
    }
}
