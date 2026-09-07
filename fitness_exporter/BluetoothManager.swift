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
    @Published private(set) var deviceName: String?
    @Published var isScanning: Bool = false
    @Published private(set) var connectionState = BluetoothConnectionStateMachine()
    /// If true, automatically scan when Bluetooth powers on (then reset).
    var autoScanOnPowerOn: Bool = false

    private let disconnectSubject = PassthroughSubject<Void, Never>()
    private let eventSubject = PassthroughSubject<SensorEvent, Never>()
    private let lifecycleSubject = PassthroughSubject<BluetoothLifecycleEvent, Never>()
    private let readinessLock = NSLock()
    private var readinessGeneration: Int?
    private var streamsReportedReady = Set<SensorStreamKind>()

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

    var lifecyclePublisher: AnyPublisher<BluetoothLifecycleEvent, Never> {
        lifecycleSubject.eraseToAnyPublisher()
    }

    var isReadyForRecording: Bool {
        connectionState.isReady
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
            isScanning = false
            if !(backend is PolarSDKBackend), let generation = connectionState.generation {
                applyLifecycleEvent(
                    .disconnected(generation: generation, pairingError: false)
                )
                disconnectSubject.send()
            }
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
        deviceName = peripheral.name ?? peripheral.identifier.uuidString
        remember(peripheral: peripheral)
        UserDefaults.standard.set(peripheral.identifier.uuidString, forKey: UserDefaultsKeys.LAST_HRV_DEVICE)
        let backend = GenericBackend(deviceId: peripheral.identifier.uuidString)
        subscribe(to: backend)
        let generation = nextGenericGeneration()
        applyLifecycleEvent(.connected(generation: generation))
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
        let generation = connectionState.generation
        disconnectSubject.send()
        if let generation {
            applyLifecycleEvent(
                .disconnected(generation: generation, pairingError: false)
            )
        }
        backend = nil
        backendCancellables.removeAll()
        self.peripheral = nil
        deviceName = nil
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
        backend.lifecyclePublisher
            .receive(on: DispatchQueue.main)
            .sink { [weak self] event in
                self?.handleBackendLifecycleEvent(event)
            }
            .store(in: &backendCancellables)
        backend.eventPublisher
            .sink { [weak self] event in
                if case .battery(let sample) = event.data {
                    DispatchQueue.main.async { [weak self] in
                        self?.batteryLevel = sample.level
                    }
                }
                self?.eventSubject.send(event)
                if
                    let stream = Self.streamKind(for: event),
                    self?.shouldPublishReady(stream: stream) == true
                {
                    DispatchQueue.main.async { [weak self] in
                        self?.markStreamReady(stream)
                    }
                }
            }
            .store(in: &backendCancellables)
        backend.connect()
            .sink(
                receiveCompletion: { [weak self] completion in
                    if case .failure(let error) = completion {
                        CustomLogger.log("Backend connect error: \(error)")
                        DispatchQueue.main.async { [weak self] in
                            self?.applyLifecycleEvent(
                                .connectionFailed(message: error.localizedDescription)
                            )
                            self?.disconnectSubject.send()
                        }
                    }
                },
                receiveValue: { }
            )
            .store(in: &backendCancellables)
    }

    func connect(to peripheral: CBPeripheral) {
        stopScan()
        tearDownCurrentConnection()
        pendingReconnectUUID = nil

        self.peripheral = peripheral
        deviceName = peripheral.name ?? peripheral.identifier.uuidString
        if Self.isPolarH10(name: peripheral.name) {
            connectPolar(
                identifier: peripheral.identifier,
                name: peripheral.name
            )
        } else {
            connectionState.requestConnection(requiredStreams: [.hr])
            publishConnectionState()
            central.connect(peripheral, options: nil)
        }
    }

    /// Attempt to connect to a previously remembered device, even if it is not
    /// currently discoverable via scanning.
    func connect(to rememberedDevice: RememberedDevice) {
        guard let uuid = UUID(uuidString: rememberedDevice.id) else {
            CustomLogger.log("Invalid remembered device UUID: \(rememberedDevice.id)")
            return
        }
        if Self.isPolarH10(name: rememberedDevice.name) {
            stopScan()
            tearDownCurrentConnection()
            pendingReconnectUUID = nil
            peripheral = resolvePeripheral(with: uuid)
            deviceName = rememberedDevice.name ?? uuid.uuidString
            connectPolar(identifier: uuid, name: rememberedDevice.name)
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
        let generation = connectionState.generation
        let shouldCancelCoreBluetooth = !(backend is PolarSDKBackend)
        connectionState.requestDisconnect()
        resetReadinessTracking(generation: nil)
        publishConnectionState()
        backend?.disconnect()
        if shouldCancelCoreBluetooth, let p = peripheral {
            central.cancelPeripheralConnection(p)
        }
        backend = nil
        backendCancellables.removeAll()
        peripheral = nil
        deviceName = nil
        batteryLevel = nil
        if let generation {
            lifecycleSubject.send(
                .disconnected(generation: generation, pairingError: false)
            )
            disconnectSubject.send()
        }
    }

    /// Ensure packets already accepted by the active backend have reached
    /// `sensorPublisher` before a recorder detaches.
    func drainPendingSensorEvents() {
        backend?.drainPendingEvents()
    }

    func restartStream(_ stream: SensorStreamKind, reason: String) {
        guard connectionState.requiredStreams.contains(stream) else { return }
        backend?.restartStream(stream, reason: reason)
    }

    private var genericGenerationCounter = 0

    private func connectPolar(identifier: UUID, name: String?) {
        deviceName = name ?? identifier.uuidString
        remember(id: identifier.uuidString, name: name)
        UserDefaults.standard.set(
            identifier.uuidString,
            forKey: UserDefaultsKeys.LAST_HRV_DEVICE
        )
        let backend = PolarSDKBackend(deviceId: identifier.uuidString)
        connectionState.requestConnection(requiredStreams: backend.requiredStreams)
        publishConnectionState()
        subscribe(to: backend)
    }

    private func tearDownCurrentConnection() {
        let currentBackend = backend
        let shouldCancelCoreBluetooth =
            !(currentBackend is PolarSDKBackend)
            && !Self.isPolarH10(name: deviceName)
        connectionState.requestDisconnect()
        resetReadinessTracking(generation: nil)
        publishConnectionState()
        currentBackend?.disconnect()
        if shouldCancelCoreBluetooth, let peripheral {
            central.cancelPeripheralConnection(peripheral)
        }
        backend = nil
        backendCancellables.removeAll()
        peripheral = nil
        deviceName = nil
        batteryLevel = nil
    }

    private func nextGenericGeneration() -> Int {
        genericGenerationCounter += 1
        return genericGenerationCounter
    }

    private func handleBackendLifecycleEvent(_ event: BluetoothLifecycleEvent) {
        if case .disconnected = event {
            batteryLevel = nil
        }
        guard connectionState.handle(event) else { return }
        updateReadinessTracking(for: event)
        publishConnectionState()
        lifecycleSubject.send(event)
        if case .disconnected = event {
            disconnectSubject.send()
        }
    }

    private func applyLifecycleEvent(_ event: BluetoothLifecycleEvent) {
        guard connectionState.handle(event) else { return }
        updateReadinessTracking(for: event)
        publishConnectionState()
        lifecycleSubject.send(event)
    }

    private func markStreamReady(_ stream: SensorStreamKind) {
        guard
            let generation = connectionState.generation,
            connectionState.requiredStreams.contains(stream),
            !connectionState.readyStreams.contains(stream)
        else {
            return
        }
        applyLifecycleEvent(.streamReady(stream, generation: generation))
    }

    private func publishConnectionState() {
        isConnected = connectionState.isConnected
    }

    private func shouldPublishReady(stream: SensorStreamKind) -> Bool {
        readinessLock.lock()
        defer { readinessLock.unlock() }
        guard readinessGeneration != nil else { return false }
        return streamsReportedReady.insert(stream).inserted
    }

    private func updateReadinessTracking(for event: BluetoothLifecycleEvent) {
        switch event {
        case .connected(let generation):
            resetReadinessTracking(generation: generation)
        case .connecting, .disconnected, .connectionFailed:
            resetReadinessTracking(generation: nil)
        case .streamFailed(let stream, let generation, _):
            readinessLock.lock()
            if readinessGeneration == generation {
                streamsReportedReady.remove(stream)
            }
            readinessLock.unlock()
        case .streamReady:
            break
        }
    }

    private func resetReadinessTracking(generation: Int?) {
        readinessLock.lock()
        readinessGeneration = generation
        streamsReportedReady.removeAll()
        readinessLock.unlock()
    }

    private static func streamKind(for event: SensorEvent) -> SensorStreamKind? {
        switch event.data {
        case .hrSamples:
            return .hr
        case .ecgSamples:
            return .ecg
        case .accSamples:
            return .acc
        case .battery, .hrvStage, .location, .custom:
            return nil
        }
    }

    private static func isPolarH10(name: String?) -> Bool {
        name?.localizedCaseInsensitiveContains("Polar H10") == true
    }

    // MARK: - Remembered devices persistence and reconnect

    private func connect(toIdentifier identifier: UUID) {
        stopScan()
        tearDownCurrentConnection()

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
        remember(
            id: peripheral.identifier.uuidString,
            name: peripheral.name
        )
    }

    private func remember(id: String, name: String?) {
        let now = Date()
        if let index = rememberedDevices.firstIndex(where: { $0.id == id }) {
            rememberedDevices[index].name = name ?? rememberedDevices[index].name
            rememberedDevices[index].lastSeen = now
        } else {
            rememberedDevices.append(RememberedDevice(id: id, name: name, lastSeen: now))
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
