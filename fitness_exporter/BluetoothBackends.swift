import Combine
import CoreBluetooth
import Foundation
import PolarBleSdk
import RxSwift

/// Unified backend interface used by the BluetoothManager to consume sensor
/// data from different sources. Default implementations are provided for
/// optional functionality so individual backends can implement only what
/// they need.
protocol BluetoothBackend {
    init(deviceId: String)
    var eventPublisher: AnyPublisher<SensorEvent, Never> { get }
    /// Signals when the backend disconnects from the device.
    var disconnectPublisher: AnyPublisher<Void, Never> { get }

    /// Connect to the underlying device. Publishers may not emit until
    /// the returned publisher completes.
    func connect() -> AnyPublisher<Void, Error>
    /// Clean up internal streams and detach from the device.
    func disconnect()

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
    var disconnectPublisher: AnyPublisher<Void, Never> {
        Empty().eraseToAnyPublisher()
    }
    func connect() -> AnyPublisher<Void, Error> {
        Just(()).setFailureType(to: Error.self).eraseToAnyPublisher()
    }
    func disconnect() {}
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
    private let disconnectSubject = PassthroughSubject<Void, Never>()

    var eventPublisher: AnyPublisher<SensorEvent, Never> { eventSubject.eraseToAnyPublisher() }
    var disconnectPublisher: AnyPublisher<Void, Never> { disconnectSubject.eraseToAnyPublisher() }

    required init(deviceId: String) {}

    func connect() -> AnyPublisher<Void, Error> {
        Just(()).setFailureType(to: Error.self).eraseToAnyPublisher()
    }

    func disconnect() {
        eventSubject.send(completion: .finished)
        disconnectSubject.send()
        disconnectSubject.send(completion: .finished)
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
    func deviceDisconnected(_ identifier: PolarBleSdk.PolarDeviceInfo, pairingError: Bool) {
        print("deviceDisconnected \(identifier) \(pairingError)")
        disconnect()
    }

    private var hrStarted = false
    private var ecgStarted = false
    private var accStarted = false

    private let eventSubject = PassthroughSubject<SensorEvent, Never>()
    private let disconnectSubject = PassthroughSubject<Void, Never>()

    var eventPublisher: AnyPublisher<SensorEvent, Never> { eventSubject.eraseToAnyPublisher() }
    var disconnectPublisher: AnyPublisher<Void, Never> { disconnectSubject.eraseToAnyPublisher() }

    func process(
        peripheral: CBPeripheral,
        didUpdateValueFor characteristic: CBCharacteristic,
        error: Error?
    ) {
        // Polar SDK uses its own streaming API; no CoreBluetooth data handled.
    }

    private let deviceId: String
    private var api: PolarBleApi?
    private var disposeBag = DisposeBag()

    required init(deviceId: String) {
        self.deviceId = deviceId
        super.init()
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
    }

    func connect() -> AnyPublisher<Void, Error> {
        do {
            try api?.connectToDevice(deviceId)
            return Just(()).setFailureType(to: Error.self).eraseToAnyPublisher()
        } catch {
            return Fail(error: error).eraseToAnyPublisher()
        }
    }

    func disconnect() {
        eventSubject.send(completion: .finished)
        disconnectSubject.send()
        disconnectSubject.send(completion: .finished)
        hrStarted = false
        ecgStarted = false
        accStarted = false
        try? api?.disconnectFromDevice(deviceId)
    }

    func requiredServices() -> [CBUUID] {
        // Polar SDK streams HR/ECG/ACC and provides battery via SDK callback; no CB services needed.
        return []
    }

    deinit {
        disconnect()
        api = nil
    }

    func deviceConnected(_ polarDeviceInfo: PolarDeviceInfo) {}
    func deviceConnecting(_ polarDeviceInfo: PolarDeviceInfo) {}
    func deviceDisconnected(_ polarDeviceInfo: PolarDeviceInfo) {}
    func disInformationReceived(_ identifier: String, uuid: CBUUID, value: String) {}
    func disInformationReceivedWithKeysAsStrings(_ identifier: String, key: String, value: String) {
    }
    func batteryLevelReceived(_ identifier: String, batteryLevel: UInt) {
        let timestamp = Date()
        let event = SensorEvent(
            timestamp: timestamp,
            data: .battery(BatterySample(level: Int(batteryLevel)))
        )
        eventSubject.send(event)
    }
    func batteryChargingStatusReceived(
        _ identifier: String, chargingStatus: BleBasClient.ChargeState
    ) {}
    func blePowerOn() {}
    func blePowerOff() {}

    func bleSdkFeatureReady(_ identifier: String, feature: PolarBleSdkFeature) {
        print("bleSdkFeatureReady \(identifier) \(feature)")
        if feature == .feature_hr {
            startHrStreaming()
        }
        if feature == .feature_polar_online_streaming {
            api?.requestStreamSettings(identifier, feature: .ecg)
                .observe(on: MainScheduler.instance)
                .subscribe(
                    onSuccess: { [weak self] settings in
                        // Resolution: 14
                        // print("ECG settings: \(settings)")
                        self?.startEcgStreaming(settings: settings.maxSettings())
                    },
                    onFailure: { error in
                        CustomLogger.log("Failed to fetch ECG stream settings: \(error)")
                    }
                )
                .disposed(by: disposeBag)
            api?.requestStreamSettings(identifier, feature: .acc)
                .observe(on: MainScheduler.instance)
                .subscribe(
                    onSuccess: { [weak self] settings in
                        // Resolution: 16
                        // print("ACC settings: \(settings)")
                        let selectedSettings = PolarSensorSetting(
                            settings.settings.reduce(into: [:]) { (result, arg1) in
                                let (key, value) = arg1
                                result[key] =
                                    (key == PolarSensorSetting.SettingType.sampleRate
                                        ? value.min() : value.max()) ?? 0
                            })
                        // print("ACC selected: \(selectedSettings)")
                        self?.startAccStreaming(settings: selectedSettings)
                    },
                    onFailure: { error in
                        CustomLogger.log("Failed to fetch ACC stream settings: \(error)")
                    }
                )
                .disposed(by: disposeBag)
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

    private func startEcgStreaming(settings: PolarSensorSetting) {
        if ecgStarted { return }
        ecgStarted = true
        api?.startEcgStreaming(deviceId, settings: settings)
            .subscribe { [weak self] e in
                switch e {
                case .next(let data):
                    let timestamp = Date()
                    let samples = data.map {
                        ECGSample(
                            timestamp: $0.timeStamp,
                            voltage: PolarSDKBackend.clampInt32To16($0.voltage)
                        )
                    }
                    let event = SensorEvent(
                        timestamp: timestamp,
                        data: .ecgSamples(ECGSamples(samples: samples))
                    )
                    self?.eventSubject.send(event)
                //                    if data.count > 0 {
                //                        print("ECG cnt \(data.count)")
                //                        print(
                //                            "ECG Min/max \(data.map{$0.voltage}.min()!) \(data.map{$0.voltage}.max()!)"
                //                        )
                //                        print("ECG TS offsets \(data.map{$0.timeStamp - data[0].timeStamp})")
                //                    }
                //                    print("ECG: \(data.count) \(String(describing: data.first))")
                case .error(let err):
                    CustomLogger.log("ECG streaming error: \(err)")
                case .completed:
                    CustomLogger.log("ECG streaming completed")
                }
            }.disposed(by: disposeBag)
    }

    private func startAccStreaming(settings: PolarSensorSetting) {
        if accStarted { return }
        accStarted = true
        api?.startAccStreaming(deviceId, settings: settings)
            .subscribe { [weak self] e in
                switch e {
                case .next(let data):
                    let timestamp = Date()
                    let samples = data.map {
                        AccSample(
                            timestamp: $0.timeStamp,
                            x: PolarSDKBackend.clampInt32To16($0.x),
                            y: PolarSDKBackend.clampInt32To16($0.y),
                            z: PolarSDKBackend.clampInt32To16($0.z)
                        )
                    }
                    let event = SensorEvent(
                        timestamp: timestamp,
                        data: .accSamples(AccSamples(samples: samples))
                    )
                    self?.eventSubject.send(event)
                //                    print("ACC cnt \(data.count)")
                //                    print(
                //                        "ACC Min/max \(data.map{min($0.x, $0.y, $0.z)}.min()!) \(data.map{max($0.x, $0.y, $0.z)}.max()!)"
                //                    )
                //                    print("ACC TS offsets \(data.map{$0.timeStamp - data[0].timeStamp})")
                //                    print("ACC: \(data.count) \(String(describing: data.first))")
                case .error(let err):
                    CustomLogger.log("ACC streaming error: \(err)")
                case .completed:
                    CustomLogger.log("ACC streaming completed")
                }
            }.disposed(by: disposeBag)
    }

    private func startHrStreaming() {
        if hrStarted { return }
        hrStarted = true
        api?.startHrStreaming(deviceId)
            .subscribe { [weak self] e in
                switch e {
                case .next(let data):
                    let timestamp = Date()
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
                        timestamp: timestamp,
                        data: .hrSamples(HRSamples(samples: samples))
                    )
                    self?.eventSubject.send(event)
                //                    print("HR: \(data.count) \(String(describing: data.first))")
                case .error(let err):
                    CustomLogger.log("HR streaming error: \(err)")
                case .completed:
                    CustomLogger.log("HR streaming completed")
                }
            }.disposed(by: disposeBag)
    }
}
