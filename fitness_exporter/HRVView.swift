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
    @Environment(\.scenePhase) private var scenePhase
    @Binding var isProcessing: Bool
    @Binding var activeRecording: AppRecordingStatus?
    @ObservedObject private var manager: BluetoothManager
    @ObservedObject private var continuousRecorder: ContinuousRecorder
    @State private var subscriptions = Set<AnyCancellable>()
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
    @State private var orthostaticMetrics: [HRVStage: StageMetrics]?
    @State private var showModeSettings = false
    @State private var showResults = false
    @State private var statusNotice: String?
    @State private var isViewVisible = false
    @State private var hasInitializedView = false
    
    // Bridge that subscribes once to manager events and updates UI + graph
    @ObservedObject private var eventBridge: HRVEventBridge
    
    
    init(
        isProcessing: Binding<Bool>,
        activeRecording: Binding<AppRecordingStatus?>,
        manager: BluetoothManager,
        continuousRecorder: ContinuousRecorder,
        eventBridge: HRVEventBridge
    ) {
        self._isProcessing = isProcessing
        self._activeRecording = activeRecording
        self.manager = manager
        self.continuousRecorder = continuousRecorder
        self.eventBridge = eventBridge
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

    private var deviceSummary: String {
        "\(manager.deviceName ?? "") \(eventBridge.rawHR) "
            + "(\(eventBridge.derivedHR) RR) bpm "
            + "🔋\(manager.batteryLevel.map { "\($0)%" } ?? "--")"
    }
    
    var body: some View {
        ZStack {
            recordingBackground.ignoresSafeArea()
            ScrollView {
                VStack(spacing: 12) {
                    if connectionPhase == .notConnected {
                        disconnectedContent
                    } else {
                        connectedHeader
                        modeControls
                        recordingStatusContent
                        orthostaticResultContent
                        SynchronizedSignalGraphs(eventBridge: eventBridge)
                    }
                }
                .padding()
                .frame(maxWidth: .infinity, alignment: .top)
            }
            .scrollBounceBehavior(.basedOnSize)
        }
        .navigationTitle("HRV")
        .onAppear {
            if let activeRecording {
                connectionPhase = .recording
                startDate = activeRecording.startedAt
                selectedMode = activeRecording.modeName == Mode.continuous.rawValue
                    ? .continuous : .orthostatic
            } else if manager.isConnected {
                connectionPhase = .connected
            }
            isViewVisible = true
            updateEventPresentationState()
            updateElapsedTimer()
            if !hasInitializedView {
                hasInitializedView = true
                // Ensure persisted durations fall within the supported stepper values.
                validateDurations()
                UNUserNotificationCenter.current().requestAuthorization(
                    options: [.alert, .sound, .badge]
                ) { _, error in
                    if let error = error {
                        CustomLogger.log("Notification authorization error: \(error)")
                    }
                }
                manager.disconnectPublisher
                    .sink { _ in
                        if connectionPhase == .recording {
                            return
                        }
                        connectionPhase = .notConnected
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
                        guard let lastUUID = UserDefaults.standard.string(
                            forKey: UserDefaultsKeys.LAST_HRV_DEVICE
                        ) else { return }
                        if let device = devices.first(where: {
                            $0.identifier.uuidString == lastUUID
                        }) {
                            manager.connect(to: device)
                        }
                    }
                    .store(in: &subscriptions)
                if activeRecording != nil || manager.isConnected {
                    // The app-scoped sensor owner is already active.
                } else if let lastUUID = UserDefaults.standard.string(
                    forKey: UserDefaultsKeys.LAST_HRV_DEVICE
                ) {
                    let remembered =
                        manager.rememberedDevices.first(where: { $0.id == lastUUID })
                        ?? BluetoothManager.RememberedDevice(
                            id: lastUUID,
                            name: nil,
                            lastSeen: .distantPast
                        )
                    manager.connect(to: remembered)
                } else {
                    manager.autoScanOnPowerOn = true
                    manager.scanForDevices()
                }
            }
        }
        .onDisappear {
            isViewVisible = false
            updateEventPresentationState()
            timer?.invalidate()
            timer = nil
        }
        .onChange(of: scenePhase) {
            updateEventPresentationState()
            updateElapsedTimer()
        }
        .onReceive(continuousRecorder.$attention.compactMap { $0 }) { attention in
            guard attention.endedRecording else { return }
            timer?.invalidate()
            timer = nil
            startDate = nil
            connectionPhase = manager.isConnected ? .connected : .notConnected
            isProcessing = false
            activeRecording = nil
            UIApplication.shared.isIdleTimerDisabled = false
        }
        .confirmationDialog(
            "Stop recording?",
            isPresented: $showStopRecordingConfirm,
            titleVisibility: .visible
        ) {
            Button("Stop Recording", role: .destructive) {
                stopRecording()
                connectionPhase = manager.isConnected ? .connected : .notConnected
                isProcessing = false
                activeRecording = nil
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
        .alert(
            item: Binding(
                get: { continuousRecorder.attention },
                set: { value in
                    if value == nil {
                        continuousRecorder.dismissAttention()
                    }
                }
            )
        ) { attention in
            if attention.offersWriteRetry {
                return Alert(
                    title: Text(attention.title),
                    message: Text(attention.message),
                    primaryButton: .default(Text("Retry Save")) {
                        continuousRecorder.retryFailedWrites()
                    },
                    secondaryButton: .cancel(Text("Keep for Later"))
                )
            }
            return Alert(
                title: Text(attention.title),
                message: Text(attention.message),
                dismissButton: .default(Text("OK"))
            )
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
        .sheet(isPresented: $showModeSettings) {
            NavigationStack {
                Form {
                    if selectedMode == .orthostatic {
                        Section("Orthostatic Protocol") {
                            Stepper(
                                "Warmup: \(formatDuration(warmupDurationSeconds))",
                                onIncrement: {
                                    stepDuration(&warmupDurationSeconds, up: true)
                                },
                                onDecrement: {
                                    stepDuration(&warmupDurationSeconds, up: false)
                                }
                            )
                            Stepper(
                                "Laying: \(formatDuration(layingDurationSeconds))",
                                onIncrement: {
                                    stepDuration(&layingDurationSeconds, up: true)
                                },
                                onDecrement: {
                                    stepDuration(&layingDurationSeconds, up: false)
                                }
                            )
                            Stepper(
                                "Standing: \(formatDuration(standingDurationSeconds))",
                                onIncrement: {
                                    stepDuration(&standingDurationSeconds, up: true)
                                },
                                onDecrement: {
                                    stepDuration(&standingDurationSeconds, up: false)
                                }
                            )
                        }
                    } else {
                        Section("Continuous Schedule") {
                            Stepper(
                                "Record: \(formatDuration(recordingDurationSeconds))",
                                onIncrement: {
                                    stepDuration(&recordingDurationSeconds, up: true)
                                    if recordingDurationSeconds > recordingIntervalSeconds {
                                        recordingIntervalSeconds = recordingDurationSeconds
                                    }
                                },
                                onDecrement: {
                                    stepDuration(&recordingDurationSeconds, up: false)
                                }
                            )
                            Stepper(
                                "Every: \(formatDuration(recordingIntervalSeconds))",
                                onIncrement: {
                                    stepDuration(&recordingIntervalSeconds, up: true)
                                },
                                onDecrement: {
                                    stepDuration(&recordingIntervalSeconds, up: false)
                                    if recordingIntervalSeconds < recordingDurationSeconds {
                                        recordingDurationSeconds = recordingIntervalSeconds
                                    }
                                }
                            )
                        }
                    }
                }
                .navigationTitle("Recording Settings")
                .toolbar {
                    ToolbarItem(placement: .confirmationAction) {
                        Button("Done") { showModeSettings = false }
                    }
                }
            }
            .presentationDetents([.medium])
        }
        .sheet(isPresented: $showResults) {
            NavigationStack {
                ScrollView([.vertical, .horizontal]) {
                    Text(consoleText)
                        .font(.system(.body, design: .monospaced))
                        .textSelection(.enabled)
                        .padding()
                        .frame(maxWidth: .infinity, alignment: .topLeading)
                }
                .navigationTitle("Orthostatic Results")
                .toolbar {
                    ToolbarItem(placement: .topBarLeading) {
                        Button("Copy") {
                            UIPasteboard.general.string = consoleText
                        }
                    }
                    ToolbarItem(placement: .confirmationAction) {
                        Button("Done") { showResults = false }
                    }
                }
            }
            .presentationDetents([.medium, .large])
        }
    }

    @ViewBuilder
    private var disconnectedContent: some View {
        VStack(spacing: 12) {
            if !manager.rememberedDevices.isEmpty {
                VStack(spacing: 8) {
                    HStack {
                        Text("Remembered Devices")
                            .font(.headline)
                        Spacer()
                    }
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

            if manager.isScanning {
                HStack {
                    Button("Stop Scan") { manager.stopScan() }
                    Spacer()
                }
                ForEach(unrememberedDiscoveredDevices, id: \.identifier) { device in
                    HStack {
                        Text(device.name ?? device.identifier.uuidString)
                        Spacer()
                        Button("Connect") {
                            manager.connect(to: device)
                        }
                    }
                }
            } else {
                Button("Scan Devices") { manager.scanForDevices() }
            }
        }
    }

    private var connectedHeader: some View {
        VStack(spacing: 6) {
            HStack {
                Text(deviceSummary)
                    .lineLimit(1)
                    .minimumScaleFactor(0.75)
                Spacer()
                if connectionPhase != .recording {
                    Button(role: .destructive) {
                        manager.disconnect()
                        connectionPhase = .notConnected
                        UserDefaults.standard.removeObject(
                            forKey: UserDefaultsKeys.LAST_HRV_DEVICE
                        )
                    } label: {
                        Image(systemName: "bolt.slash")
                    }
                    .accessibilityLabel("Disconnect")
                }
            }

            TimelineView(.periodic(from: .now, by: 1)) { timeline in
                HStack(spacing: 9) {
                    streamStatusLabel(
                        "RR",
                        lastReceivedAt: eventBridge.lastRR,
                        now: timeline.date
                    )
                    streamStatusLabel(
                        "ECG",
                        lastReceivedAt: eventBridge.lastECG,
                        now: timeline.date
                    )
                    streamStatusLabel(
                        "ACC",
                        lastReceivedAt: eventBridge.lastACC,
                        now: timeline.date
                    )
                    Spacer(minLength: 4)
                    if connectionPhase == .recording {
                        Button("Stop Recording") {
                            showStopRecordingConfirm = true
                        }
                        .buttonStyle(.borderedProminent)
                        .tint(.red)
                        .controlSize(.small)
                    } else {
                        Button("Start Recording") {
                            connectionPhase = .recording
                            isProcessing = true
                            startRecording()
                        }
                        .buttonStyle(.borderedProminent)
                        .controlSize(.small)
                        .disabled(!manager.isReadyForRecording || isProcessing)
                    }
                }
            }

            if connectionPhase != .recording, !manager.isReadyForRecording {
                Text("Waiting for required sensor streams…")
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
    }

    private var modeControls: some View {
        VStack(spacing: 6) {
            Picker("Mode", selection: $selectedMode) {
                ForEach(Mode.allCases) { mode in
                    Text(mode.rawValue).tag(mode)
                }
            }
            .pickerStyle(.segmented)
            .disabled(connectionPhase == .recording)

            Button {
                showModeSettings = true
            } label: {
                HStack {
                    Text(modeConfigurationSummary)
                        .font(.caption)
                        .foregroundColor(.primary)
                    Spacer()
                    Label("Edit", systemImage: "slider.horizontal.3")
                        .font(.caption)
                }
                .padding(.horizontal, 10)
                .padding(.vertical, 7)
                .background(
                    Color(UIColor.secondarySystemBackground),
                    in: RoundedRectangle(cornerRadius: 8)
                )
            }
            .buttonStyle(.plain)
            .disabled(connectionPhase == .recording)
            .opacity(connectionPhase == .recording ? 0.65 : 1)
        }
    }

    @ViewBuilder
    private var recordingStatusContent: some View {
        if connectionPhase == .recording {
            HStack {
                Text(
                    selectedMode == .orthostatic
                        ? "\(orthoPhaseName) · \(formatElapsed(elapsedSeconds))"
                        : "Continuous · \(formatElapsed(elapsedSeconds))"
                )
                .font(.headline)
                Spacer()
                if selectedMode == .orthostatic, orthoUIPhase == .transition {
                    Button("Start Standing") {
                        orthoTester?.startStanding()
                    }
                    .buttonStyle(.borderedProminent)
                } else if selectedMode == .continuous {
                    Button("Add Event", systemImage: "plus") {
                        customEventTimestamp = Date()
                        showCustomEventSheet = true
                    }
                    .buttonStyle(.bordered)
                }
            }

            if let interruption = continuousRecorder.interruptionMessage,
               selectedMode == .continuous
            {
                Text("Recovering: \(interruption)")
                    .font(.caption)
                    .foregroundColor(.orange)
                    .frame(maxWidth: .infinity, alignment: .leading)
            } else if !manager.isReadyForRecording {
                Text(
                    manager.isConnected
                        ? "Recovering Polar sensor streams…"
                        : "Polar disconnected — recording remains active while reconnecting…"
                )
                .font(.caption)
                .foregroundColor(.orange)
                .frame(maxWidth: .infinity, alignment: .leading)
            }

            if selectedMode == .continuous, !liveActivityEnabledOnDevice {
                Text("Live Activity unavailable — enable capability and widget")
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }

        if let statusNotice {
            Text(statusNotice)
                .font(.caption)
                .foregroundColor(.secondary)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    @ViewBuilder
    private var orthostaticResultContent: some View {
        if selectedMode == .orthostatic,
           let summary = orthostaticResultSummary
        {
            Button {
                showResults = true
            } label: {
                HStack {
                    Text(summary)
                        .font(.caption.monospacedDigit())
                        .foregroundColor(.primary)
                    Spacer()
                    Label("Details", systemImage: "chevron.right")
                        .font(.caption)
                }
                .padding(9)
                .background(
                    Color(UIColor.secondarySystemBackground),
                    in: RoundedRectangle(cornerRadius: 8)
                )
            }
            .buttonStyle(.plain)
        }
    }

    private var recordingBackground: Color {
        if connectionPhase == .recording, !manager.isReadyForRecording {
            return .orange.opacity(0.10)
        }
        switch (selectedMode, connectionPhase) {
        case (.orthostatic, .recording):
            return .green.opacity(0.10)
        case (.continuous, .notConnected), (.continuous, .connected):
            return .red.opacity(0.08)
        default:
            return .clear
        }
    }

    private var modeConfigurationSummary: String {
        switch selectedMode {
        case .orthostatic:
            return "Warmup \(formatDuration(warmupDurationSeconds)) · "
                + "Laying \(formatDuration(layingDurationSeconds)) · "
                + "Standing \(formatDuration(standingDurationSeconds))"
        case .continuous:
            return "Record \(formatDuration(recordingDurationSeconds)) · "
                + "Every \(formatDuration(recordingIntervalSeconds))"
        }
    }

    private var orthostaticResultSummary: String? {
        guard let laying = orthostaticMetrics?[.laying],
              let standing = orthostaticMetrics?[.standing]
        else { return nil }
        let index = laying.rmssd > 0 ? standing.rmssd / laying.rmssd : 0
        return String(
            format: "Laying %.0f ms · Standing %.0f ms · Index %.2f",
            laying.rmssd,
            standing.rmssd,
            index
        )
    }

    private func updateEventPresentationState() {
        eventBridge.setPresentationActive(isViewVisible && scenePhase == .active)
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
        let startedAt = Date()
        statusNotice = nil
        startDate = startedAt
        activeRecording = AppRecordingStatus(
            startedAt: startedAt,
            modeName: selectedMode.rawValue
        )
        UIApplication.shared.isIdleTimerDisabled = selectedMode == .orthostatic
        elapsedSeconds = 0
        updateElapsedTimer()
        if selectedMode == .orthostatic {
            orthostaticMetrics = nil
            consoleText = ""
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
                .completionPublisher
                .receive(on: DispatchQueue.main)
                .sink { completion in
                    switch completion {
                    case .completed(let metrics):
                        orthostaticMetrics = metrics
                        if let laying = metrics[.laying],
                           let standing = metrics[.standing]
                        {
                            let index = laying.rmssd > 0
                                ? standing.rmssd / laying.rmssd : 0
                            consoleText = [
                                "Orthostatic HRV Results:",
                                "  Laying: RMSSD = \(laying.rmssd) ms, Mean HR = \(laying.meanHR) bpm",
                                "  Standing: RMSSD = \(standing.rmssd) ms, Mean HR = \(standing.meanHR) bpm",
                                "  Orthostatic Index (Standing/Laying RMSSD): \(index)"
                            ].joined(separator: "\n")
                        }
                        // Upload only a completed, retained recording.
                        triggerDirectoryUploads()
                    case .discardedShortRecording:
                        orthostaticMetrics = nil
                        consoleText = ""
                        showTemporaryStatus("Short recording discarded")
                    }
                    finalizeOrthostaticRecording()
                }
                .store(in: &subscriptions)
            orthoTester?.start()
        } else {
            continuousRecorder.start(durationSeconds: recordingDurationSeconds, intervalSeconds: recordingIntervalSeconds)
        }
    }
    
    private func stopRecording() {
        if let startDate {
            elapsedSeconds = max(0, Int(Date().timeIntervalSince(startDate)))
        }
        timer?.invalidate()
        timer = nil
        startDate = nil
        activeRecording = nil
        UIApplication.shared.isIdleTimerDisabled = false
        // stop any standing reminders
        standReminderTimer?.invalidate()
        standReminderTimer = nil
        if selectedMode == .orthostatic {
            orthoTester?.cancel()
        } else {
            continuousRecorder.stop()
            CustomLogger.log("Continuous recording stopped after \(formatElapsed(elapsedSeconds))")
            connectionPhase = manager.isConnected ? .connected : .notConnected
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
        connectionPhase = manager.isConnected ? .connected : .notConnected
        isProcessing = false
        activeRecording = nil
        UIApplication.shared.isIdleTimerDisabled = false
        orthoTester = nil
    }

    private func showTemporaryStatus(_ message: String) {
        statusNotice = message
        DispatchQueue.main.asyncAfter(deadline: .now() + 3) {
            if statusNotice == message {
                statusNotice = nil
            }
        }
    }

    private func updateElapsedTimer() {
        timer?.invalidate()
        timer = nil
        guard isViewVisible, scenePhase == .active,
              connectionPhase == .recording,
              let startDate
        else { return }
        elapsedSeconds = max(0, Int(Date().timeIntervalSince(startDate)))
        timer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true) { _ in
            elapsedSeconds = max(0, Int(Date().timeIntervalSince(startDate)))
        }
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
        
        private func streamStatusLabel(
            _ name: String,
            lastReceivedAt: Date?,
            now: Date
        ) -> some View {
            let age = lastReceivedAt.map {
                max(0, Int(now.timeIntervalSince($0)))
            }
            let isLive = age.map { $0 <= 10 } ?? false
            let detail = age.map { "\($0)s" } ?? "--"
            return HStack(spacing: 4) {
                Image(systemName: "circle.fill")
                    .font(.system(size: 6))
                    .foregroundColor(isLive ? .green : .orange)
                Text("\(name) \(detail)")
                    .monospacedDigit()
            }
            .font(.caption)
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
                if let errorMessage = summary.errorMessage {
                    CustomLogger.log("[HRV][Backfill][Error] \(errorMessage)")
                } else {
                    let message =
                        "[HRV][Backfill] total=\(summary.totalFiles) "
                        + "pending=\(summary.pendingFiles) "
                        + "skipped=\(summary.skippedByMemoryFiles) "
                        + "imported=\(summary.importedFiles) "
                        + "unchanged=\(summary.unchangedFiles) "
                        + "failed=\(summary.failedFiles)"
                    CustomLogger.log(message)
                }
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
    
