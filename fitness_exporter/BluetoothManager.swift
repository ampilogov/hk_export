import Combine
import CoreBluetooth
import Foundation

/// Handles CoreBluetooth discovery and connection and forwards raw
/// characteristic updates to a backend that knows how to parse them into
/// ``SensorEvent`` values.
class BluetoothManager: NSObject, ObservableObject, CBCentralManagerDelegate, CBPeripheralDelegate {
    @Published var isConnected: Bool = false
    /// Battery level (%) of the connected device, if available.
    @Published var batteryLevel: Int? = nil
    @Published var discoveredDevices: [CBPeripheral] = []
    @Published private(set) var peripheral: CBPeripheral?
    @Published var isScanning: Bool = false
    /// If true, automatically scan when Bluetooth powers on (then reset).
    var autoScanOnPowerOn: Bool = false

    private let disconnectSubject = PassthroughSubject<Void, Never>()
    private let eventSubject = PassthroughSubject<SensorEvent, Never>()

    /// Currently active backend used to parse sensor data from the connected device.
    private var backend: BluetoothBackend?
    /// Subscriptions to the backend publishers.
    private var backendCancellables = Set<AnyCancellable>()

    /// Publisher that emits when the peripheral disconnects.
    var disconnectPublisher: AnyPublisher<Void, Never> {
        disconnectSubject.eraseToAnyPublisher()
    }

    /// Stream of all sensor events from any backend.
    var sensorPublisher: AnyPublisher<SensorEvent, Never> {
        eventSubject.eraseToAnyPublisher()
    }

    private var central: CBCentralManager!

    override init() {
        super.init()
        central = CBCentralManager(delegate: self, queue: nil)
    }

    func centralManagerDidUpdateState(_ central: CBCentralManager) {
        if central.state != .poweredOn {
            discoveredDevices.removeAll()
            isConnected = false
            isScanning = false
        } else if autoScanOnPowerOn {
            autoScanOnPowerOn = false
            scanForDevices()
        }
    }

    func centralManager(
        _ central: CBCentralManager,
        didDiscover peripheral: CBPeripheral,
        advertisementData: [String: Any],
        rssi RSSI: NSNumber
    ) {
        if !discoveredDevices.contains(where: {
            $0.identifier == peripheral.identifier
        }) {
            discoveredDevices.append(peripheral)
        }
    }

    func centralManager(
        _ central: CBCentralManager, didConnect peripheral: CBPeripheral
    ) {
        peripheral.delegate = self
        isConnected = true
        // Instantiate appropriate backend once the connection succeeds
        let backend: BluetoothBackend
        if let name = peripheral.name, name.contains("Polar H10") {
            backend = PolarSDKBackend(deviceId: peripheral.identifier.uuidString)
        } else {
            backend = GenericBackend(deviceId: peripheral.identifier.uuidString)
        }
        subscribe(to: backend)
        // Ask backend which services to discover (if any)
        let services = backend.requiredServices()
        if !services.isEmpty {
            peripheral.discoverServices(services)
        }
    }

    func peripheral(
        _ peripheral: CBPeripheral, didDiscoverServices error: Error?
    ) {
        backend?.didDiscoverServices(peripheral: peripheral, error: error)
        guard let services = peripheral.services else { return }
        for service in services {
            // Discover all characteristics; backend decides what to use.
            peripheral.discoverCharacteristics(nil, for: service)
        }
    }

    func peripheral(
        _ peripheral: CBPeripheral,
        didDiscoverCharacteristicsFor service: CBService,
        error: Error?
    ) {
        backend?.didDiscoverCharacteristics(peripheral: peripheral, service: service, error: error)
    }

    func centralManager(
        _ central: CBCentralManager,
        didDisconnectPeripheral peripheral: CBPeripheral,
        error: Error?
    ) {
        guard peripheral == self.peripheral else {
            return
        }
        backend?.disconnect()
        disconnectSubject.send()
        self.peripheral = nil
        isConnected = false
        isScanning = false
        batteryLevel = nil
    }

    func peripheral(
        _ peripheral: CBPeripheral,
        didUpdateValueFor characteristic: CBCharacteristic, error: Error?
    ) {
        backend?.process(
            peripheral: peripheral,
            didUpdateValueFor: characteristic,
            error: error
        )
    }

    func scanForDevices() {
        discoveredDevices.removeAll()
        if central.state == .poweredOn {
            isScanning = true
            central.scanForPeripherals(
                withServices: [BluetoothUUID.heartRateService], options: nil)
        }
    }

    func stopScan() {
        isScanning = false
        central.stopScan()
    }

    private func subscribe(to backend: BluetoothBackend) {
        backendCancellables.removeAll()
        self.backend = backend
        backend.disconnectPublisher
            .sink { [weak self] in self?.disconnectSubject.send() }
            .store(in: &backendCancellables)
        backend.eventPublisher
            .sink { [weak self] event in
                if case .battery(let sample) = event.data {
                    self?.batteryLevel = sample.level
                }
                self?.eventSubject.send(event)
            }
            .store(in: &backendCancellables)
        backend.connect()
            .sink(
                receiveCompletion: { [weak self] completion in
                    if case .failure(let error) = completion {
                        CustomLogger.log("Backend connect error: \(error)")
                        self?.disconnectSubject.send()
                    }
                },
                receiveValue: { }
            )
            .store(in: &backendCancellables)
    }

    func connect(to peripheral: CBPeripheral) {
        stopScan()
        backend?.disconnect()
        backend = nil
        backendCancellables.removeAll()

        self.peripheral = peripheral
        central.connect(peripheral, options: nil)
    }

    func disconnect() {
        backend?.disconnect()
        if let p = peripheral {
            central.cancelPeripheralConnection(p)
        }
        backend = nil
        backendCancellables.removeAll()
    }
}
