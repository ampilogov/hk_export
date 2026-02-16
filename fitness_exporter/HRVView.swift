import SwiftUI
import CoreBluetooth
import Combine
import UIKit
import Dispatch
import UserNotifications
import AVFoundation
import HealthKit
import ActivityKit

// Additional utilities for recording and processing HRV data
import Foundation

// Delegate to handle speech completion and restore audio session
private class SpeechDelegate: NSObject, AVSpeechSynthesizerDelegate {
    func speechSynthesizer(_ synthesizer: AVSpeechSynthesizer, didFinish utterance: AVSpeechUtterance) {
        let session = AVAudioSession.sharedInstance()
        do {
            try session.setActive(false, options: .notifyOthersOnDeactivation)
        } catch {
            CustomLogger.log("Audio session deactivation error: \(error)")
        }
    }
}

struct HRVView: View {
    @Binding var isProcessing: Bool
    @StateObject private var manager: BluetoothManager
    @StateObject private var continuousRecorder: ContinuousRecorder
    @State private var subscriptions = Set<AnyCancellable>()
    @State private var derivedHR: Int = 0
    @State private var rawHR: Int = 0
    @State private var orthoTester: OrthostaticHRV?
    @State private var timer: Timer?
    /// Timer for repeating haptic reminders to stand until user action
    @State private var standReminderTimer: Timer?
    @State private var startDate: Date?
    @State private var elapsedSeconds: Int = 0
    /// Flag to present a volume-up reminder before the stand-up alert
    @State private var showVolumeAlert: Bool = false
    /// Text to display in the console after orthostatic test completion
    @State private var consoleText: String = ""
    /// Speech synthesizer for TTS prompts.
    @State private var speechSynthesizer = AVSpeechSynthesizer()
    @State private var speechDelegate = SpeechDelegate()
    /// Confirmation sheet for cancelling/stopping an active recording
    @State private var showStopRecordingConfirm: Bool = false
    /// Sheet for logging a custom user event
    @State private var showCustomEventSheet: Bool = false
    @State private var customEventText: String = ""
    @State private var customEventTimestamp: Date?
    
    // Bridge that subscribes once to manager events and updates UI + graph
    @StateObject private var eventBridge: HRVEventBridge
    
    
    init(isProcessing: Binding<Bool>) {
        self._isProcessing = isProcessing
        let manager = BluetoothManager()
        _manager = StateObject(wrappedValue: manager)
        _continuousRecorder = StateObject(wrappedValue: ContinuousRecorder(manager: manager))
        _eventBridge = StateObject(wrappedValue: HRVEventBridge(manager: manager))
    }
    
    private enum Mode: String, CaseIterable, Identifiable {
        case orthostatic = "Orthostatic HRV"
        case continuous = "Continuous Recording"
        var id: String { self.rawValue }
    }
    
    @State private var selectedMode: Mode = .orthostatic
    /// Allowed durations for orthostatic test (seconds).
    private let durationOptions: [Int] = [0, 2, 5, 10, 30] + Array(stride(from: 60, through: 3600, by: 60))
    @AppStorage(UserDefaultsKeys.HRV_WARMUP_DURATION) private var warmupDurationSeconds: Int = 2
    @AppStorage(UserDefaultsKeys.HRV_LAYING_DURATION) private var layingDurationSeconds: Int = 60
    @AppStorage(UserDefaultsKeys.HRV_STANDING_DURATION) private var standingDurationSeconds: Int = 60
    @AppStorage(UserDefaultsKeys.HRV_RECORDING_DURATION) private var recordingDurationSeconds: Int = 300
    @AppStorage(UserDefaultsKeys.HRV_RECORDING_INTERVAL) private var recordingIntervalSeconds: Int = 300
    
    private enum ConnectionPhase {
        case notConnected
        case connected
        case recording
    }
    
    @State private var connectionPhase: ConnectionPhase = .notConnected
    
    private enum OrthoUIPhase {
        case preLaying, laying, transition, standing, cooldown, done
    }
    
    @State private var orthoUIPhase: OrthoUIPhase = .preLaying
    
    private var orthoPhaseName: String {
        switch orthoUIPhase {
        case .preLaying: return "Warmup"
        case .laying: return "Laying"
        case .transition: return "Ready to Stand"
        case .standing: return "Standing"
        case .cooldown: return "Cooldown"
        case .done: return "Done"
        }
    }
    
    var body: some View {
        VStack(spacing: 16) {
            if connectionPhase == .notConnected {
                VStack(spacing: 12) {
                    if !manager.rememberedDevices.isEmpty {
                        VStack(spacing: 8) {
                            HStack {
                                Text("Remembered Devices")
                                    .font(.headline)
                                Spacer()
                            }
                            ScrollView {
                                VStack(spacing: 8) {
                                    ForEach(manager.rememberedDevices) { device in
                                        HStack(spacing: 12) {
                                            Text(device.name ?? device.id)
                                            Spacer()
                                            Button("Connect") {
                                                manager.connect(to: device)
                                            }
                                            Button("Delete", role: .destructive) {
                                                deleteRememberedDevice(device)
                                            }
                                        }
                                    }
                                }
                            }
                            .frame(maxHeight: 160)
                        }
                    }

                    if manager.isScanning {
                        VStack(spacing: 8) {
                            HStack {
                                Button("Stop Scan") { manager.stopScan() }
                                Spacer()
                            }
                            ScrollView {
                                VStack(spacing: 8) {
                                    ForEach(unrememberedDiscoveredDevices, id: \.identifier) { device in
                                        HStack {
                                            Text(device.name ?? device.identifier.uuidString)
                                            Spacer()
                                            Button("Connect") {
                                                manager.connect(to: device)
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        Button("Scan Devices") { manager.scanForDevices() }
                    }
                }
            } else if connectionPhase == .connected {
                VStack(spacing: 8) {
                    Text("\(manager.peripheral?.name ?? manager.peripheral?.identifier.uuidString ?? "") \(eventBridge.rawHR) (\(eventBridge.derivedHR) RR) bpm 🔋\(manager.batteryLevel.map { "\($0)%" } ?? "--")")
                    HStack {
                        Spacer()
                        Button("Start Recording") {
                            connectionPhase = .recording
                            isProcessing = true
                            startRecording()
                        }
                        Button("Disconnect") {
                            manager.disconnect()
                            connectionPhase = .notConnected
                            UserDefaults.standard.removeObject(forKey: UserDefaultsKeys.LAST_HRV_DEVICE)
                        }
                    }
                }
            } else if connectionPhase == .recording {
                VStack(spacing: 8) {
                    Text("\(manager.peripheral?.name ?? manager.peripheral?.identifier.uuidString ?? "") \(eventBridge.rawHR) (\(eventBridge.derivedHR) RR) bpm 🔋\(manager.batteryLevel.map { "\($0)%" } ?? "--")")
                    HStack {
                        Spacer()
                        Button("Stop Recording") { showStopRecordingConfirm = true }
                    }
                    if selectedMode == .orthostatic {
                        if orthoUIPhase == .transition {
                            Button("Start Standing") {
                                orthoTester?.startStanding()
                            }
                        } else {
                            Text("\(orthoPhaseName): \(formatElapsed(elapsedSeconds))")
                                .font(.headline)
                        }
                    } else {
                        VStack {
                            Text("RR: \(formatLast(continuousRecorder.lastRR))")
                            Text("ECG: \(formatLast(continuousRecorder.lastECG))")
                            Text("ACC: \(formatLast(continuousRecorder.lastACC))")
                            Text("Elapsed: \(formatElapsed(elapsedSeconds))")
                                .font(.headline)
                            HStack {
                                Spacer()
                                Button("Add Event") {
                                    // Capture click time for accurate timestamping
                                    customEventTimestamp = Date()
                                    showCustomEventSheet = true
                                }
                            }
                            // Surface Live Activity availability for troubleshooting
                            if !liveActivityEnabledOnDevice {
                                Text("Live Activity unavailable — enable capability and widget")
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                            }
                        }
                    }
                }
            }
            if connectionPhase != .notConnected {
                Picker("Mode", selection: $selectedMode) {
                    ForEach(Mode.allCases) { mode in
                        Text(mode.rawValue).tag(mode)
                    }
                }
                .pickerStyle(SegmentedPickerStyle())
                .disabled(connectionPhase == .recording)
                
                if selectedMode == .orthostatic {
                    VStack(spacing: 8) {
                        Stepper(
                            onIncrement: { stepDuration(&warmupDurationSeconds, up: true) },
                            onDecrement: { stepDuration(&warmupDurationSeconds, up: false) },
                            label: { Text("Warmup Duration: \(formatDuration(warmupDurationSeconds))") }
                        )
                        Stepper(
                            onIncrement: { stepDuration(&layingDurationSeconds, up: true) },
                            onDecrement: { stepDuration(&layingDurationSeconds, up: false) },
                            label: { Text("Laying Duration: \(formatDuration(layingDurationSeconds))") }
                        )
                        Stepper(
                            onIncrement: { stepDuration(&standingDurationSeconds, up: true) },
                            onDecrement: { stepDuration(&standingDurationSeconds, up: false) },
                            label: { Text("Standing Duration: \(formatDuration(standingDurationSeconds))") }
                        )
                    }
                    .disabled(connectionPhase == .recording)
                } else {
                    VStack(spacing: 8) {
                        Stepper(
                            onIncrement: {
                                stepDuration(&recordingDurationSeconds, up: true)
                                if recordingDurationSeconds > recordingIntervalSeconds {
                                    recordingIntervalSeconds = recordingDurationSeconds
                                }
                            },
                            onDecrement: {
                                stepDuration(&recordingDurationSeconds, up: false)
                            },
                            label: { Text("Recording Duration: \(formatDuration(recordingDurationSeconds))") }
                        )
                        Stepper(
                            onIncrement: { stepDuration(&recordingIntervalSeconds, up: true) },
                            onDecrement: {
                                stepDuration(&recordingIntervalSeconds, up: false)
                                if recordingIntervalSeconds < recordingDurationSeconds {
                                    recordingDurationSeconds = recordingIntervalSeconds
                                }
                            },
                            label: { Text("Record Every: \(formatDuration(recordingIntervalSeconds))") }
                        )
                    }
                    .disabled(connectionPhase == .recording)
                }
            }
            
            // Console output area for orthostatic results
            if selectedMode == .orthostatic {
                ScrollView([.vertical, .horizontal]) {
                    Text(consoleText)
                        .font(.system(.footnote, design: .monospaced))
                        .padding()
                        .frame(maxWidth: .infinity, alignment: .topLeading)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(Color(UIColor.secondarySystemBackground))
                .cornerRadius(8)
            }
            Spacer()
            if connectionPhase != .notConnected {
                RRIntervalGraph(model: eventBridge.graphModel)
                    .frame(height: 200)
            }
        }
        .padding()
        .navigationTitle("HRV")
        .onAppear {
            // Ensure any persisted durations fall within our supported range so the steppers work
            validateDurations()
            UIApplication.shared.isIdleTimerDisabled = true
            UNUserNotificationCenter.current().requestAuthorization(options: [.alert, .sound, .badge]) { granted, error in
                if let error = error {
                    CustomLogger.log("Notification authorization error: \(error)")
                }
            }
            manager.disconnectPublisher
                .sink { _ in
                    connectionPhase = .notConnected
                    eventBridge.resetGraph()
                }
                .store(in: &subscriptions)
            manager.$isConnected
                .receive(on: DispatchQueue.main)
                .sink { connected in
                    if connected {
                        if connectionPhase == .notConnected {
                            connectionPhase = .connected
                        }
                    } else if connectionPhase != .recording {
                        connectionPhase = .notConnected
                    }
                }
                .store(in: &subscriptions)
            manager.$discoveredDevices
                .receive(on: DispatchQueue.main)
                .sink { devices in
                    guard connectionPhase == .notConnected else { return }
                    guard let lastUUID = UserDefaults.standard.string(forKey: UserDefaultsKeys.LAST_HRV_DEVICE) else { return }
                    if let device = devices.first(where: { $0.identifier.uuidString == lastUUID }) {
                        manager.connect(to: device)
                    }
                }
                .store(in: &subscriptions)
            if let lastUUID = UserDefaults.standard.string(forKey: UserDefaultsKeys.LAST_HRV_DEVICE) {
                let remembered =
                    manager.rememberedDevices.first(where: { $0.id == lastUUID })
                    ?? BluetoothManager.RememberedDevice(id: lastUUID, name: nil, lastSeen: .distantPast)
                manager.connect(to: remembered)
            } else {
                manager.autoScanOnPowerOn = true
                manager.scanForDevices()
            }
        }
        .onDisappear {
            subscriptions.removeAll()
            UIApplication.shared.isIdleTimerDisabled = false
        }
        .confirmationDialog(
            "Stop recording?",
            isPresented: $showStopRecordingConfirm,
            titleVisibility: .visible
        ) {
            Button("Stop Recording", role: .destructive) {
                stopRecording()
                connectionPhase = .connected
                isProcessing = false
            }
            Button("Continue", role: .cancel) { }
        } message: {
            Text("This will end the current session.")
        }
        .alert("Volume Check", isPresented: $showVolumeAlert) {
            Button("OK", role: .cancel) { }
        } message: {
            Text("Please make sure your volume is turned up so you can hear the stand-up alert.")
        }
        .sheet(isPresented: $showCustomEventSheet) {
            VStack(spacing: 16) {
                Text("Log Custom Event")
                    .font(.headline)
                TextField("Enter note…", text: $customEventText)
                    .textFieldStyle(RoundedBorderTextFieldStyle())
                    .padding(.horizontal)
                HStack {
                    Button("Cancel") {
                        customEventText = ""
                        customEventTimestamp = nil
                        showCustomEventSheet = false
                    }
                    Spacer()
                    Button("Save") {
                        if let ts = customEventTimestamp, !customEventText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                            continuousRecorder.logCustomEvent(customEventText, at: ts)
                        }
                        customEventText = ""
                        customEventTimestamp = nil
                        showCustomEventSheet = false
                    }
                    .disabled(customEventText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                }
                .padding(.horizontal)
            }
            .padding()
            .presentationDetents([.medium])
        }
    }
    private var liveActivityEnabledOnDevice: Bool {
        if #available(iOS 16.1, *) {
            return ActivityAuthorizationInfo().areActivitiesEnabled
        } else {
            return false
        }
    }

    private var rememberedDeviceIDs: Set<String> {
        Set(manager.rememberedDevices.map(\.id))
    }

    private var unrememberedDiscoveredDevices: [CBPeripheral] {
        manager.discoveredDevices.filter { !rememberedDeviceIDs.contains($0.identifier.uuidString) }
    }

    private func deleteRememberedDevice(_ device: BluetoothManager.RememberedDevice) {
        manager.forgetRememberedDevice(id: device.id)
        if UserDefaults.standard.string(forKey: UserDefaultsKeys.LAST_HRV_DEVICE) == device.id {
            UserDefaults.standard.removeObject(forKey: UserDefaultsKeys.LAST_HRV_DEVICE)
        }
    }

    private func startRecording() {
        startDate = Date()
        elapsedSeconds = 0
        eventBridge.resetGraph()
        timer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true) { _ in
            if let start = startDate {
                elapsedSeconds = Int(Date().timeIntervalSince(start))
            }
        }
        if selectedMode == .orthostatic {
            orthoTester = OrthostaticHRV(manager: manager,
                                         warmupDuration: TimeInterval(warmupDurationSeconds),
                                         layingDuration: TimeInterval(layingDurationSeconds),
                                         standingDuration: TimeInterval(standingDurationSeconds))
            DispatchQueue.main.async {
                let volume = AVAudioSession.sharedInstance().outputVolume
                if volume < 0.5 {
                    self.showVolumeAlert = true
                }
            }
            // Kick off an incremental HealthKit export when the test starts
            kickOffIncrementalExport()
            orthoTester?
                .stagePublisher
                .receive(on: DispatchQueue.main)
                .sink { stage in
                    eventBridge.graphModel.markStageChange(at: Date())
                    switch stage {
                    case .preLaying:
                        orthoUIPhase = .preLaying
                    case .laying:
                        orthoUIPhase = .laying
                    case .waitingForStanding:
                        orthoUIPhase = .transition
                        standingDurationSeconds == 0 ? orthoTester?.startStanding() : notifyReadyToStand()
                    case .standing:
                        orthoUIPhase = .standing
                        // stop haptic reminders when user starts standing
                        standReminderTimer?.invalidate()
                        standReminderTimer = nil
                    case .cooldown:
                        orthoUIPhase = .cooldown
                    case .done:
                        orthoUIPhase = .done
                    }
                }
                .store(in: &subscriptions)
            orthoTester?
                .resultsPublisher
                .receive(on: DispatchQueue.main)
                .sink { metrics in
                    if let laying = metrics[.laying], let standing = metrics[.standing] {
                        let index = laying.rmssd > 0 ? standing.rmssd / laying.rmssd : 0
                        consoleText = [
                            "Orthostatic HRV Results:",
                            "  Laying: RMSSD = \(laying.rmssd) ms, Mean HR = \(laying.meanHR) bpm",
                            "  Standing: RMSSD = \(standing.rmssd) ms, Mean HR = \(standing.meanHR) bpm",
                            "  Orthostatic Index (Standing/Laying RMSSD): \(index)"
                        ].joined(separator: "\n")
                    }
                    // After finishing, trigger uploads for any configured directories
                    triggerDirectoryUploads()
                    finalizeOrthostaticRecording()
                }
                .store(in: &subscriptions)
            orthoTester?.start()
        } else {
            continuousRecorder.start(durationSeconds: recordingDurationSeconds, intervalSeconds: recordingIntervalSeconds)
        }
    }
    
    private func stopRecording() {
        timer?.invalidate()
        timer = nil
        startDate = nil
        // stop any standing reminders
        standReminderTimer?.invalidate()
        standReminderTimer = nil
        if selectedMode == .orthostatic {
            orthoTester?.cancel()
        } else {
            continuousRecorder.stop()
            CustomLogger.log("Continuous recording stopped after \(formatElapsed(elapsedSeconds))")
            connectionPhase = .connected
            isProcessing = false
            // After continuous session ends, auto-run incremental HK export and directory uploads
            kickOffIncrementalExport()
            triggerDirectoryUploads()
        }
    }
    
    private func finalizeOrthostaticRecording() {
        timer?.invalidate()
        timer = nil
        startDate = nil
        standReminderTimer?.invalidate()
        standReminderTimer = nil
        connectionPhase = .connected
        isProcessing = false
        orthoTester = nil
    }
    
    private func stepDuration(_ value: inout Int, up: Bool) {
        if let idx = durationOptions.firstIndex(of: value) {
            let newIdx = max(0, min(durationOptions.count - 1, idx + (up ? 1 : -1)))
            value = durationOptions[newIdx]
        } else {
            // Snap legacy or out-of-range values (e.g. 1s) to the nearest valid option
            if up {
                value = durationOptions.first(where: { $0 > value }) ?? durationOptions.last!
            } else {
                value = durationOptions.last(where: { $0 < value }) ?? durationOptions.first!
            }
        }
    }
    
    private func validateDurations() {
        if !durationOptions.contains(layingDurationSeconds) {
            layingDurationSeconds = durationOptions.first(where: { $0 == 60 }) ?? durationOptions.first!
        }
        if !durationOptions.contains(standingDurationSeconds) {
            standingDurationSeconds = durationOptions.first(where: { $0 == 60 }) ?? durationOptions.first!
        }
        if !durationOptions.contains(recordingDurationSeconds) {
            recordingDurationSeconds = durationOptions.first(where: { $0 == 300 }) ?? durationOptions.first!
            if !durationOptions.contains(warmupDurationSeconds) {
                warmupDurationSeconds = durationOptions.first(where: { $0 == 2 }) ?? durationOptions.first!
            }
            if !durationOptions.contains(recordingIntervalSeconds) {
                recordingIntervalSeconds = max(recordingDurationSeconds, durationOptions.first(where: { $0 == 300 }) ?? durationOptions.first!)
            }
            if recordingIntervalSeconds < recordingDurationSeconds {
                recordingIntervalSeconds = recordingDurationSeconds
            }
        }
    }
        
        private func formatDuration(_ seconds: Int) -> String {
            seconds < 60 ? "\(seconds)s" : "\(seconds / 60)m"
        }
        
        private func formatElapsed(_ seconds: Int) -> String {
            let minutes = seconds / 60
            let secs = seconds % 60
            return String(format: "%02d:%02d", minutes, secs)
        }
        
        private func formatLast(_ date: Date?) -> String {
            guard let date = date else { return "--" }
            let diff = Int(Date().timeIntervalSince(date))
            return "\(diff)s ago"
        }
        
        /// Schedules notifications, audio, and haptic feedback to prompt user to stand.
        private func notifyReadyToStand() {
            let center = UNUserNotificationCenter.current()
            let title = "Time to Stand Up"
            let body = "Please stand now for your HRV measurement."
            for i in 0..<1 {
                let content = UNMutableNotificationContent()
                content.title = title
                content.body = body
                content.sound = UNNotificationSound.default
                let rawInterval = TimeInterval(i) * 2.0
                let triggerInterval = rawInterval > 0 ? rawInterval : 0.1
                let trigger = UNTimeIntervalNotificationTrigger(timeInterval: triggerInterval, repeats: false)
                let request = UNNotificationRequest(identifier: "OrthostaticStandNotification\(i)", content: content, trigger: trigger)
                center.add(request) { error in
                    if let error = error {
                        CustomLogger.log("Error scheduling standing notification: \(error)")
                    }
                }
            }
            let utterance = AVSpeechUtterance(string: body)
            utterance.rate = AVSpeechUtteranceDefaultSpeechRate
            utterance.pitchMultiplier = 1.1
            utterance.volume = 1.0
            do {
                let session = AVAudioSession.sharedInstance()
                try session.setCategory(.playback, options: [.duckOthers])
                try session.setActive(true)
            } catch {
                CustomLogger.log("Audio session error: \(error)")
            }
            speechSynthesizer.delegate = speechDelegate
            speechSynthesizer.speak(utterance)
            let notifGen = UINotificationFeedbackGenerator()
            let impactGen = UIImpactFeedbackGenerator(style: .heavy)
            // start haptic reminders immediately and repeat until user starts standing
            DispatchQueue.main.async {
                notifGen.prepare()
                impactGen.prepare()
                notifGen.notificationOccurred(.error)
                impactGen.impactOccurred()
                standReminderTimer?.invalidate()
                standReminderTimer = Timer.scheduledTimer(withTimeInterval: 1.0, repeats: true) { _ in
                    notifGen.notificationOccurred(.error)
                    impactGen.impactOccurred()
                }
            }
        }
        
        // MARK: - Auto export + upload hooks

        private func kickOffIncrementalExport() {
            // Ensure HK permissions are in place, then run a wide incremental export
            HealthKitManager.initialize(startObservers: false) { success in
                guard success else {
                    CustomLogger.log("[HRV][IE] HealthKit init failed; skipping incremental export")
                    return
                }
                let types = ExportConstants.getSampleTypesOfInterest()
                IncrementalExporter().run(sampleTypes: types, batchSize: 60 * 60 * 24 * 10) { status in
                    if let status = status {
                        CustomLogger.log("[HRV][IE][Error] \(status)")
                    } else {
                        CustomLogger.log("[HRV][IE][Success] Completed incremental export at test start")
                    }
                    kickOffSensorBagBackfill()
                }
            }
        }

        private func kickOffSensorBagBackfill() {
            SensorBagPersistence.backfillSavedBagsToHealthKit(onlyPending: true) { summary in
                CustomLogger.log(
                    "[HRV][Backfill] total=\(summary.totalFiles) pending=\(summary.pendingFiles) skipped=\(summary.skippedByMemoryFiles) imported=\(summary.importedFiles) unchanged=\(summary.unchangedFiles) failed=\(summary.failedFiles)"
                )
            }
        }

        private func triggerDirectoryUploads() {
            DispatchQueue.global(qos: .utility).async {
                DirectoryUploader.uploadAllFromStore(stopOnError: true) { err in
                    if let err = err {
                        CustomLogger.log("[HRV][Upload][Error] \(err)")
                    } else {
                        CustomLogger.log("[HRV][Upload][Success] Completed uploads after orthostatic test")
                    }
                }
            }
        }
        
    }
    
