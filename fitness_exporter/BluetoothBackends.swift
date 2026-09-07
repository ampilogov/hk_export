import Combine
import CoreBluetooth
import Foundation
import PolarBleSdk
import RxSwift

enum SensorStreamKind: String, CaseIterable, Hashable {
    case hr
    case ecg
    case acc
}

enum BluetoothLifecycleEvent: Equatable {
    case connecting
    case connected(generation: Int)
    case disconnected(generation: Int, pairingError: Bool)
    case streamReady(SensorStreamKind, generation: Int)
    case streamFailed(SensorStreamKind, generation: Int, message: String)
    case connectionFailed(message: String)
}

struct BluetoothConnectionStateMachine: Equatable {
    enum Phase: Equatable {
        case idle
        case connecting
        case connected
        case degraded
        case ready
    }

    private(set) var phase: Phase = .idle
    private(set) var wantsConnection = false
    private(set) var generation: Int?
    private(set) var requiredStreams: Set<SensorStreamKind> = []
    private(set) var readyStreams: Set<SensorStreamKind> = []

    var isConnected: Bool {
        switch phase {
        case .connected, .degraded, .ready:
            return true
        case .idle, .connecting:
            return false
        }
    }

    var isReady: Bool {
        phase == .ready
    }

    mutating func requestConnection(requiredStreams: Set<SensorStreamKind>) {
        wantsConnection = true
        generation = nil
        self.requiredStreams = requiredStreams
        readyStreams.removeAll()
        phase = .connecting
    }

    mutating func requestDisconnect() {
        wantsConnection = false
        generation = nil
        readyStreams.removeAll()
        phase = .idle
    }

    @discardableResult
    mutating func handle(_ event: BluetoothLifecycleEvent) -> Bool {
        switch event {
        case .connecting:
            guard wantsConnection else { return false }
            generation = nil
            readyStreams.removeAll()
            phase = .connecting
            return true

        case .connected(let newGeneration):
            guard wantsConnection else { return false }
            generation = newGeneration
            readyStreams.removeAll()
            phase = requiredStreams.isEmpty ? .ready : .connected
            return true

        case .disconnected(let disconnectedGeneration, _):
            guard generation == disconnectedGeneration else { return false }
            generation = nil
            readyStreams.removeAll()
            phase = wantsConnection ? .connecting : .idle
            return true

        case .streamReady(let stream, let eventGeneration):
            guard wantsConnection, generation == eventGeneration else { return false }
            readyStreams.insert(stream)
            phase = requiredStreams.isSubset(of: readyStreams) ? .ready : .connected
            return true

        case .streamFailed(let stream, let eventGeneration, _):
            guard wantsConnection, generation == eventGeneration else { return false }
            readyStreams.remove(stream)
            phase = .degraded
            return true

        case .connectionFailed:
            guard wantsConnection else { return false }
            generation = nil
            readyStreams.removeAll()
            phase = .connecting
            return true
        }
    }
}

/// Unified backend interface used by the BluetoothManager to consume sensor
/// data from different sources. Default implementations are provided for
/// optional functionality so individual backends can implement only what
/// they need.
protocol BluetoothBackend {
    init(deviceId: String)
    var eventPublisher: AnyPublisher<SensorEvent, Never> { get }
    var lifecyclePublisher: AnyPublisher<BluetoothLifecycleEvent, Never> { get }
    var requiredStreams: Set<SensorStreamKind> { get }

    /// Connect to the underlying device. Publishers may not emit until
    /// the returned publisher completes.
    func connect() -> AnyPublisher<Void, Error>
    /// Clean up internal streams and detach from the device.
    func disconnect()
    /// Wait until sensor packets already accepted by the backend have been
    /// published. Used before a recording detaches at Stop.
    func drainPendingEvents()
    /// Restart one unhealthy stream without rebuilding a healthy connection.
    func restartStream(_ stream: SensorStreamKind, reason: String)

    /// List of CoreBluetooth services this backend needs discovered.
    /// Return an empty array to skip CoreBluetooth service discovery entirely.
    func requiredServices() -> [CBUUID]
    /// Allow backend to handle service discovery if needed.
    func didDiscoverServices(peripheral: CBPeripheral, error: Error?)
    /// Allow backend to handle characteristic discovery and set notifications.
    func didDiscoverCharacteristics(peripheral: CBPeripheral, service: CBService, error: Error?)

    func process(
        peripheral: CBPeripheral,
        didUpdateValueFor characteristic: CBCharacteristic,
        error: Error?
    )
}

extension BluetoothBackend {
    var eventPublisher: AnyPublisher<SensorEvent, Never> {
        Empty().eraseToAnyPublisher()
    }
    var lifecyclePublisher: AnyPublisher<BluetoothLifecycleEvent, Never> {
        Empty().eraseToAnyPublisher()
    }
    var requiredStreams: Set<SensorStreamKind> { [.hr] }
    func connect() -> AnyPublisher<Void, Error> {
        Just(()).setFailureType(to: Error.self).eraseToAnyPublisher()
    }
    func disconnect() {}
    func drainPendingEvents() {}
    func restartStream(_ stream: SensorStreamKind, reason: String) {}
    func requiredServices() -> [CBUUID] { [] }
    func didDiscoverServices(peripheral: CBPeripheral, error: Error?) {}
    func didDiscoverCharacteristics(peripheral: CBPeripheral, service: CBService, error: Error?) {}
    func process(
        peripheral: CBPeripheral,
        didUpdateValueFor characteristic: CBCharacteristic,
        error: Error?
    ) {}
}

// Common sensor samples live in `SensorTypes.swift`.

/// Generic backend that parses standard Bluetooth heart-rate service measurements.
final class GenericBackend: NSObject, CBPeripheralDelegate, BluetoothBackend {
    private let eventSubject = PassthroughSubject<SensorEvent, Never>()

    var eventPublisher: AnyPublisher<SensorEvent, Never> { eventSubject.eraseToAnyPublisher() }

    required init(deviceId: String) {}

    func connect() -> AnyPublisher<Void, Error> {
        Just(()).setFailureType(to: Error.self).eraseToAnyPublisher()
    }

    func disconnect() {
        eventSubject.send(completion: .finished)
    }

    func requiredServices() -> [CBUUID] {
        [BluetoothUUID.heartRateService, BluetoothUUID.batteryService]
    }

    func didDiscoverCharacteristics(peripheral: CBPeripheral, service: CBService, error: Error?) {
        guard let characteristics = service.characteristics else { return }
        for characteristic in characteristics {
            if characteristic.uuid == BluetoothUUID.heartRateMeasurement {
                peripheral.setNotifyValue(true, for: characteristic)
            } else if characteristic.uuid == BluetoothUUID.batteryLevel {
                peripheral.readValue(for: characteristic)
            }
        }
    }

    func process(
        peripheral: CBPeripheral,
        didUpdateValueFor characteristic: CBCharacteristic,
        error: Error?
    ) {
        self.peripheral(peripheral, didUpdateValueFor: characteristic, error: error)
    }

    func peripheral(
        _ peripheral: CBPeripheral,
        didUpdateValueFor characteristic: CBCharacteristic,
        error: Error?
    ) {
        guard let data = characteristic.value else { return }
        let timestamp = Date()
        switch characteristic.uuid {
        case BluetoothUUID.batteryLevel:
            if let level = data.first {
                let event = SensorEvent(
                    timestamp: timestamp,
                    data: .battery(BatterySample(level: Int(level)))
                )
                eventSubject.send(event)
            }
        case BluetoothUUID.heartRateMeasurement:
            let bytes = [UInt8](data)
            guard !bytes.isEmpty else { return }
            let flags = bytes[0]
            var index = 1
            let heartRate: Int
            if flags & 0x01 != 0 {
                let raw = UInt16(bytes[index]) | UInt16(bytes[index + 1]) << 8
                heartRate = Int(raw)
                index += 2
            } else {
                heartRate = Int(bytes[index])
                index += 1
            }
            let contactSupported = (flags & 0x02) != 0
            let contactDetected = (flags & 0x04) != 0
            var energyExpended: UInt? = nil
            if flags & 0x08 != 0 {
                let raw = UInt16(bytes[index]) | UInt16(bytes[index + 1]) << 8
                energyExpended = UInt(raw)
                index += 2
            }
            var rrList: [Double] = []
            if flags & 0x10 != 0 {
                while index + 1 < bytes.count {
                    let rrRaw = UInt16(bytes[index]) | UInt16(bytes[index + 1]) << 8
                    rrList.append(Double(rrRaw) / 1024.0)
                    index += 2
                }
            }
            let hr = HRSample(
                value: heartRate,
                contactSupported: contactSupported,
                contactDetected: contactDetected,
                energyExpended: energyExpended,
                rrIntervals: rrList
            )
            let event = SensorEvent(
                timestamp: timestamp,
                data: .hrSamples(HRSamples(samples: [hr]))
            )
            eventSubject.send(event)
        default:
            let bytes = [UInt8](data)
            let hex = bytes.map { String(format: "%02hhx", $0) }.joined(separator: " ")
            CustomLogger.log(
                "GenericBackend received unparsed characteristic \(characteristic.uuid): [\(hex)]")
        }
    }
}

/// Handles streaming of additional data via Polar SDK.
final class PolarSDKBackend: NSObject, PolarBleApiObserver, PolarBleApiPowerStateObserver,
    PolarBleApiDeviceInfoObserver, PolarBleApiDeviceFeaturesObserver, BluetoothBackend
{
    private let eventSubject = PassthroughSubject<SensorEvent, Never>()
    private let lifecycleSubject = PassthroughSubject<BluetoothLifecycleEvent, Never>()

    var eventPublisher: AnyPublisher<SensorEvent, Never> { eventSubject.eraseToAnyPublisher() }
    var lifecyclePublisher: AnyPublisher<BluetoothLifecycleEvent, Never> {
        lifecycleSubject.eraseToAnyPublisher()
    }
    let requiredStreams: Set<SensorStreamKind> = [.hr, .ecg, .acc]

    func process(
        peripheral: CBPeripheral,
        didUpdateValueFor characteristic: CBCharacteristic,
        error: Error?
    ) {
        // Polar SDK uses its own streaming API; no CoreBluetooth data handled.
    }

    private let deviceId: String
    private var api: PolarBleApi?
    private var settingsDisposeBag = DisposeBag()
    private var hrDisposable: Disposable?
    private var ecgDisposable: Disposable?
    private var accDisposable: Disposable?
    private var startedStreams = Set<SensorStreamKind>()
    private var pendingStreams = Set<SensorStreamKind>()
    private var retryAttempts: [SensorStreamKind: Int] = [:]
    private var retryWorkItems: [SensorStreamKind: DispatchWorkItem] = [:]
    private var generationCounter = 0
    private var activeGeneration: Int?
    private var healthyGenerationByStream: [SensorStreamKind: Int] = [:]
    private let generationLock = NSLock()
    private let streamSubmissionLock = NSLock()
    private let streamQueue = DispatchQueue(
        label: "com.fitness_exporter.polarStreamProcessing",
        qos: .utility
    )
    private let streamQueueKey = DispatchSpecificKey<Void>()

    required init(deviceId: String) {
        self.deviceId = deviceId
        super.init()
        streamQueue.setSpecific(key: streamQueueKey, value: ())
        api = PolarBleApiDefaultImpl.polarImplementation(
            DispatchQueue.main,
            features: [
                .feature_hr,
                .feature_polar_sdk_mode,
                .feature_battery_info,
                .feature_polar_activity_data,
                .feature_device_info,
                .feature_polar_online_streaming,
                .feature_polar_offline_recording,
                .feature_polar_device_time_setup,
                .feature_polar_h10_exercise_recording,
            ]
        )
        api?.observer = self
        api?.deviceInfoObserver = self
        api?.powerStateObserver = self
        api?.deviceFeaturesObserver = self
        api?.automaticReconnection = true
    }

    func connect() -> AnyPublisher<Void, Error> {
        do {
            api?.automaticReconnection = true
            try api?.connectToDevice(deviceId)
            return Just(()).setFailureType(to: Error.self).eraseToAnyPublisher()
        } catch {
            return Fail(error: error).eraseToAnyPublisher()
        }
    }

    func disconnect() {
        _ = invalidateGenerationAfterDraining()
        resetStreams()
        try? api?.disconnectFromDevice(deviceId)
    }

    func drainPendingEvents() {
        guard DispatchQueue.getSpecific(key: streamQueueKey) == nil else { return }
        streamSubmissionLock.lock()
        streamQueue.sync {}
        streamSubmissionLock.unlock()
    }

    func restartStream(_ stream: SensorStreamKind, reason: String) {
        DispatchQueue.main.async { [weak self] in
            guard let self, let generation = self.currentGeneration else { return }
            self.clearStream(stream)
            self.lifecycleSubject.send(
                .streamFailed(stream, generation: generation, message: reason)
            )
            self.scheduleRestart(stream, generation: generation)
        }
    }

    func requiredServices() -> [CBUUID] {
        // Polar SDK streams HR/ECG/ACC and provides battery via SDK callback; no CB services needed.
        return []
    }

    deinit {
        _ = invalidateGenerationAfterDraining()
        resetStreams()
        try? api?.disconnectFromDevice(deviceId)
        api = nil
    }

    func deviceConnecting(_ polarDeviceInfo: PolarDeviceInfo) {
        DispatchQueue.main.async { [weak self] in
            guard let self else { return }
            self.reportConnectionLossIfNeeded(pairingError: false)
            self.lifecycleSubject.send(.connecting)
        }
    }

    func deviceConnected(_ polarDeviceInfo: PolarDeviceInfo) {
        DispatchQueue.main.async { [weak self] in
            guard let self else { return }
            guard self.currentGeneration == nil else { return }
            self.resetStreams()
            let generation = self.activateNextGeneration()
            self.lifecycleSubject.send(.connected(generation: generation))
        }
    }

    func deviceDisconnected(_ polarDeviceInfo: PolarDeviceInfo, pairingError: Bool) {
        DispatchQueue.main.async { [weak self] in
            guard let self else { return }
            self.reportConnectionLossIfNeeded(pairingError: pairingError)
            // Polar's automatic reconnection remains enabled. Calling
            // disconnectFromDevice here would cancel the SDK's reconnect attempt.
        }
    }

    func disInformationReceived(_ identifier: String, uuid: CBUUID, value: String) {}
    func disInformationReceivedWithKeysAsStrings(_ identifier: String, key: String, value: String) {
    }
    func batteryLevelReceived(_ identifier: String, batteryLevel: UInt) {
        guard let generation = currentGeneration else { return }
        let timestamp = Date()
        let event = SensorEvent(
            timestamp: timestamp,
            data: .battery(BatterySample(level: Int(batteryLevel)))
        )
        enqueueStreamEvent(generation: generation) { [weak self] in
            self?.eventSubject.send(event)
        }
    }
    func batteryChargingStatusReceived(
        _ identifier: String, chargingStatus: BleBasClient.ChargeState
    ) {}
    func blePowerOn() {}
    func blePowerOff() {
        DispatchQueue.main.async { [weak self] in
            self?.reportConnectionLossIfNeeded(pairingError: false)
        }
    }

    func bleSdkFeatureReady(_ identifier: String, feature: PolarBleSdkFeature) {
        DispatchQueue.main.async { [weak self] in
            guard let self, let generation = self.currentGeneration else { return }
            if feature == .feature_hr {
                self.startHrStreaming(generation: generation)
            }
            if feature == .feature_polar_online_streaming {
                self.requestAndStart(.ecg, generation: generation)
                self.requestAndStart(.acc, generation: generation)
            }
        }
    }

    private static func clampInt32To16(_ value: Int32) -> Int16 {
        if value > Int32(Int16.max) {
            return Int16.max
        } else if value < Int32(Int16.min) {
            return Int16.min
        } else {
            return Int16(value)
        }
    }

    private func startEcgStreaming(settings: PolarSensorSetting, generation: Int) {
        guard isGenerationActive(generation), !startedStreams.contains(.ecg) else { return }
        guard let api else {
            handleStreamTermination(.ecg, generation: generation, message: "Polar API unavailable")
            return
        }
        startedStreams.insert(.ecg)
        ecgDisposable = api.startEcgStreaming(deviceId, settings: settings)
            .subscribe { [weak self] event in
                guard let self else { return }
                switch event {
                case .next(let data):
                    let receivedAt = Date()
                    self.enqueueStreamEvent(generation: generation) { [weak self] in
                        guard let self else { return }
                        let samples = data.map {
                            ECGSample(
                                timestamp: $0.timeStamp,
                                voltage: PolarSDKBackend.clampInt32To16($0.voltage)
                            )
                        }
                        let event = SensorEvent(
                            timestamp: receivedAt,
                            data: .ecgSamples(ECGSamples(samples: samples))
                        )
                        self.eventSubject.send(event)
                        self.markStreamHealthy(.ecg, generation: generation)
                    }
                case .error(let err):
                    self.handleStreamTermination(
                        .ecg,
                        generation: generation,
                        message: err.localizedDescription
                    )
                case .completed:
                    self.handleStreamTermination(
                        .ecg,
                        generation: generation,
                        message: "ECG stream completed unexpectedly"
                    )
                }
            }
    }

    private func startAccStreaming(settings: PolarSensorSetting, generation: Int) {
        guard isGenerationActive(generation), !startedStreams.contains(.acc) else { return }
        guard let api else {
            handleStreamTermination(.acc, generation: generation, message: "Polar API unavailable")
            return
        }
        startedStreams.insert(.acc)
        accDisposable = api.startAccStreaming(deviceId, settings: settings)
            .subscribe { [weak self] event in
                guard let self else { return }
                switch event {
                case .next(let data):
                    let receivedAt = Date()
                    self.enqueueStreamEvent(generation: generation) { [weak self] in
                        guard let self else { return }
                        let samples = data.map {
                            AccSample(
                                timestamp: $0.timeStamp,
                                x: PolarSDKBackend.clampInt32To16($0.x),
                                y: PolarSDKBackend.clampInt32To16($0.y),
                                z: PolarSDKBackend.clampInt32To16($0.z)
                            )
                        }
                        let event = SensorEvent(
                            timestamp: receivedAt,
                            data: .accSamples(AccSamples(samples: samples))
                        )
                        self.eventSubject.send(event)
                        self.markStreamHealthy(.acc, generation: generation)
                    }
                case .error(let err):
                    self.handleStreamTermination(
                        .acc,
                        generation: generation,
                        message: err.localizedDescription
                    )
                case .completed:
                    self.handleStreamTermination(
                        .acc,
                        generation: generation,
                        message: "ACC stream completed unexpectedly"
                    )
                }
            }
    }

    private func startHrStreaming(generation: Int) {
        guard isGenerationActive(generation), !startedStreams.contains(.hr) else { return }
        guard let api else {
            handleStreamTermination(.hr, generation: generation, message: "Polar API unavailable")
            return
        }
        startedStreams.insert(.hr)
        hrDisposable = api.startHrStreaming(deviceId)
            .subscribe { [weak self] event in
                guard let self else { return }
                switch event {
                case .next(let data):
                    let receivedAt = Date()
                    self.enqueueStreamEvent(generation: generation) { [weak self] in
                        guard let self else { return }
                        let samples = data.map { item -> HRSample in
                            let rrs = item.rrsMs.map { Double($0) / 1000.0 }
                            return HRSample(
                                value: Int(item.hr),
                                contactSupported: item.contactStatusSupported,
                                contactDetected: item.contactStatus,
                                energyExpended: nil,
                                rrIntervals: rrs
                            )
                        }
                        let event = SensorEvent(
                            timestamp: receivedAt,
                            data: .hrSamples(HRSamples(samples: samples))
                        )
                        self.eventSubject.send(event)
                        self.markStreamHealthy(.hr, generation: generation)
                    }
                case .error(let err):
                    self.handleStreamTermination(
                        .hr,
                        generation: generation,
                        message: err.localizedDescription
                    )
                case .completed:
                    self.handleStreamTermination(
                        .hr,
                        generation: generation,
                        message: "HR stream completed unexpectedly"
                    )
                }
            }
    }

    private func requestAndStart(_ stream: SensorStreamKind, generation: Int) {
        guard stream != .hr, isGenerationActive(generation) else { return }
        guard !startedStreams.contains(stream), !pendingStreams.contains(stream) else { return }
        guard let api else {
            handleStreamTermination(stream, generation: generation, message: "Polar API unavailable")
            return
        }

        pendingStreams.insert(stream)
        let feature: PolarDeviceDataType = stream == .ecg ? .ecg : .acc
        api.requestStreamSettings(deviceId, feature: feature)
            .observe(on: MainScheduler.instance)
            .subscribe(
                onSuccess: { [weak self] settings in
                    guard let self, self.isGenerationActive(generation) else { return }
                    self.pendingStreams.remove(stream)
                    if stream == .ecg {
                        self.startEcgStreaming(
                            settings: settings.maxSettings(),
                            generation: generation
                        )
                    } else {
                        let selectedSettings = PolarSensorSetting(
                            settings.settings.reduce(into: [:]) { result, setting in
                                let (key, values) = setting
                                result[key] =
                                    (key == PolarSensorSetting.SettingType.sampleRate
                                        ? values.min() : values.max()) ?? 0
                            }
                        )
                        self.startAccStreaming(
                            settings: selectedSettings,
                            generation: generation
                        )
                    }
                },
                onFailure: { [weak self] error in
                    guard let self else { return }
                    self.pendingStreams.remove(stream)
                    self.handleStreamTermination(
                        stream,
                        generation: generation,
                        message: "Could not load stream settings: \(error.localizedDescription)"
                    )
                }
            )
            .disposed(by: settingsDisposeBag)
    }

    private func handleStreamTermination(
        _ stream: SensorStreamKind,
        generation: Int,
        message: String
    ) {
        DispatchQueue.main.async { [weak self] in
            guard let self, self.isGenerationActive(generation) else { return }
            self.clearStream(stream)
            CustomLogger.log(
                "[Polar][\(stream.rawValue.uppercased())] \(message); scheduling restart"
            )
            self.lifecycleSubject.send(
                .streamFailed(stream, generation: generation, message: message)
            )
            self.scheduleRestart(stream, generation: generation)
        }
    }

    private func scheduleRestart(_ stream: SensorStreamKind, generation: Int) {
        guard isGenerationActive(generation), retryWorkItems[stream] == nil else { return }
        let attempt = (retryAttempts[stream] ?? 0) + 1
        retryAttempts[stream] = attempt
        let delays: [TimeInterval] = [1, 2, 5, 10, 30]
        let delay = delays[min(attempt - 1, delays.count - 1)]
        let workItem = DispatchWorkItem { [weak self] in
            guard let self else { return }
            self.retryWorkItems.removeValue(forKey: stream)
            guard self.isGenerationActive(generation) else { return }
            if stream == .hr {
                self.startHrStreaming(generation: generation)
            } else {
                self.requestAndStart(stream, generation: generation)
            }
        }
        retryWorkItems[stream] = workItem
        DispatchQueue.main.asyncAfter(deadline: .now() + delay, execute: workItem)
    }

    private func markStreamHealthy(_ stream: SensorStreamKind, generation: Int) {
        generationLock.lock()
        guard
            activeGeneration == generation,
            healthyGenerationByStream[stream] != generation
        else {
            generationLock.unlock()
            return
        }
        healthyGenerationByStream[stream] = generation
        generationLock.unlock()
        DispatchQueue.main.async { [weak self] in
            guard let self, self.isGenerationActive(generation) else { return }
            self.retryAttempts.removeValue(forKey: stream)
            self.retryWorkItems.removeValue(forKey: stream)?.cancel()
        }
    }

    private func clearStream(_ stream: SensorStreamKind) {
        generationLock.lock()
        healthyGenerationByStream.removeValue(forKey: stream)
        generationLock.unlock()
        startedStreams.remove(stream)
        pendingStreams.remove(stream)
        retryWorkItems.removeValue(forKey: stream)?.cancel()
        switch stream {
        case .hr:
            hrDisposable?.dispose()
            hrDisposable = nil
        case .ecg:
            ecgDisposable?.dispose()
            ecgDisposable = nil
        case .acc:
            accDisposable?.dispose()
            accDisposable = nil
        }
    }

    private func resetStreams() {
        for workItem in retryWorkItems.values {
            workItem.cancel()
        }
        retryWorkItems.removeAll()
        retryAttempts.removeAll()
        settingsDisposeBag = DisposeBag()
        clearStream(.hr)
        clearStream(.ecg)
        clearStream(.acc)
        startedStreams.removeAll()
        pendingStreams.removeAll()
    }

    private func reportConnectionLossIfNeeded(pairingError: Bool) {
        guard let generation = invalidateGenerationAfterDraining() else { return }
        resetStreams()
        lifecycleSubject.send(
            .disconnected(generation: generation, pairingError: pairingError)
        )
    }

    private func enqueueStreamEvent(
        generation: Int,
        _ operation: @escaping () -> Void
    ) {
        streamSubmissionLock.lock()
        guard isGenerationActive(generation) else {
            streamSubmissionLock.unlock()
            return
        }
        streamQueue.async { [weak self] in
            guard let self, self.isGenerationActive(generation) else { return }
            operation()
        }
        streamSubmissionLock.unlock()
    }

    private var currentGeneration: Int? {
        generationLock.lock()
        defer { generationLock.unlock() }
        return activeGeneration
    }

    private func activateNextGeneration() -> Int {
        generationLock.lock()
        defer { generationLock.unlock() }
        generationCounter += 1
        activeGeneration = generationCounter
        healthyGenerationByStream.removeAll()
        return generationCounter
    }

    private func isGenerationActive(_ generation: Int) -> Bool {
        generationLock.lock()
        defer { generationLock.unlock() }
        return activeGeneration == generation
    }

    private func invalidateGenerationAfterDraining() -> Int? {
        streamSubmissionLock.lock()
        if DispatchQueue.getSpecific(key: streamQueueKey) == nil {
            streamQueue.sync {}
        }
        generationLock.lock()
        let generation = activeGeneration
        activeGeneration = nil
        healthyGenerationByStream.removeAll()
        generationLock.unlock()
        streamSubmissionLock.unlock()
        return generation
    }
}
