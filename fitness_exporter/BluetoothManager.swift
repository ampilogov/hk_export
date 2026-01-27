import Combine
import CoreBluetooth
import Foundation

/// Handles CoreBluetooth discovery and connection and forwards raw
/// characteristic updates to a backend that knows how to parse them into
/// ``SensorEvent`` values.
class BluetoothManager: NSObject, ObservableObject, CBCentralManagerDelegate, CBPeripheralDelegate {
    struct RememberedDevice: Codable, Identifiable, Equatable {
        /// CoreBluetooth peripheral identifier UUID string.
        let id: String
        /// Last known peripheral name, if available.
        var name: String?
        /// Last time we saw or connected to this device.
        var lastSeen: Date
    }

    @Published var isConnected: Bool = false
    /// Battery level (%) of the connected device, if available.
    @Published var batteryLevel: Int? = nil
    @Published var discoveredDevices: [CBPeripheral] = []
    /// Devices we have connected to before and persist across launches.
    @Published private(set) var rememberedDevices: [RememberedDevice] = []
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

    /// If set, we will attempt to connect to this identifier when we can resolve it.
    private var pendingReconnectUUID: UUID?

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
        rememberedDevices = Self.loadRememberedDevices()
        super.init()
        central = CBCentralManager(delegate: self, queue: nil)
    }

    func centralManagerDidUpdateState(_ central: CBCentralManager) {
        if central.state != .poweredOn {
            discoveredDevices.removeAll()
            isConnected = false
            isScanning = false
        } else if let pending = pendingReconnectUUID {
            // Bluetooth just became available; try resolving the pending device.
            pendingReconnectUUID = nil
            connect(toIdentifier: pending)
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
        updateRememberedDeviceName(id: peripheral.identifier.uuidString, name: peripheral.name)
        if !discoveredDevices.contains(where: {
            $0.identifier == peripheral.identifier
        }) {
            discoveredDevices.append(peripheral)
        }
        if pendingReconnectUUID == peripheral.identifier {
            pendingReconnectUUID = nil
            connect(to: peripheral)
        }
    }

    func centralManager(
        _ central: CBCentralManager, didConnect peripheral: CBPeripheral
    ) {
        peripheral.delegate = self
        isConnected = true
        remember(peripheral: peripheral)
        UserDefaults.standard.set(peripheral.identifier.uuidString, forKey: UserDefaultsKeys.LAST_HRV_DEVICE)
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
        } else {
            autoScanOnPowerOn = true
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
        pendingReconnectUUID = nil

        self.peripheral = peripheral
        central.connect(peripheral, options: nil)
    }

    /// Attempt to connect to a previously remembered device, even if it is not
    /// currently discoverable via scanning.
    func connect(to rememberedDevice: RememberedDevice) {
        guard let uuid = UUID(uuidString: rememberedDevice.id) else {
            CustomLogger.log("Invalid remembered device UUID: \(rememberedDevice.id)")
            return
        }
        connect(toIdentifier: uuid)
    }

    /// Remove a remembered device from persistent storage.
    func forgetRememberedDevice(id: String) {
        rememberedDevices.removeAll { $0.id == id }
        persistRememberedDevices()
        if pendingReconnectUUID?.uuidString == id {
            pendingReconnectUUID = nil
        }
    }

    func disconnect() {
        backend?.disconnect()
        if let p = peripheral {
            central.cancelPeripheralConnection(p)
        }
        backend = nil
        backendCancellables.removeAll()
    }

    // MARK: - Remembered devices persistence and reconnect

    private func connect(toIdentifier identifier: UUID) {
        stopScan()
        backend?.disconnect()
        backend = nil
        backendCancellables.removeAll()

        guard central.state == .poweredOn else {
            pendingReconnectUUID = identifier
            autoScanOnPowerOn = true
            return
        }

        if let resolved = resolvePeripheral(with: identifier) {
            connect(to: resolved)
            return
        }

        // Fall back to scanning and connect when discovered.
        pendingReconnectUUID = identifier
        scanForDevices()
    }

    private func resolvePeripheral(with identifier: UUID) -> CBPeripheral? {
        let retrieved = central.retrievePeripherals(withIdentifiers: [identifier])
        if let peripheral = retrieved.first {
            return peripheral
        }
        let connected = central.retrieveConnectedPeripherals(withServices: [BluetoothUUID.heartRateService])
        return connected.first(where: { $0.identifier == identifier })
    }

    private func remember(peripheral: CBPeripheral) {
        let id = peripheral.identifier.uuidString
        let now = Date()
        if let index = rememberedDevices.firstIndex(where: { $0.id == id }) {
            rememberedDevices[index].name = peripheral.name ?? rememberedDevices[index].name
            rememberedDevices[index].lastSeen = now
        } else {
            rememberedDevices.append(RememberedDevice(id: id, name: peripheral.name, lastSeen: now))
        }
        sortRememberedDevices()
        persistRememberedDevices()
    }

    private func updateRememberedDeviceName(id: String, name: String?) {
        guard let name, !name.isEmpty else { return }
        guard let index = rememberedDevices.firstIndex(where: { $0.id == id }) else { return }
        if rememberedDevices[index].name != name {
            rememberedDevices[index].name = name
            rememberedDevices[index].lastSeen = Date()
            sortRememberedDevices()
            persistRememberedDevices()
        }
    }

    private func sortRememberedDevices() {
        rememberedDevices.sort { lhs, rhs in
            let lhsName = (lhs.name ?? lhs.id).lowercased()
            let rhsName = (rhs.name ?? rhs.id).lowercased()
            if lhsName == rhsName {
                return lhs.lastSeen > rhs.lastSeen
            }
            return lhsName < rhsName
        }
    }

    private func persistRememberedDevices() {
        do {
            let data = try JSONEncoder().encode(rememberedDevices)
            UserDefaults.standard.set(data, forKey: UserDefaultsKeys.HRV_REMEMBERED_DEVICES)
        } catch {
            CustomLogger.log("Failed to persist remembered devices: \(error)")
        }
    }

    private static func loadRememberedDevices() -> [RememberedDevice] {
        guard let data = UserDefaults.standard.data(forKey: UserDefaultsKeys.HRV_REMEMBERED_DEVICES) else {
            return []
        }
        do {
            let devices = try JSONDecoder().decode([RememberedDevice].self, from: data)
            return devices
        } catch {
            CustomLogger.log("Failed to load remembered devices: \(error)")
            return []
        }
    }
}
