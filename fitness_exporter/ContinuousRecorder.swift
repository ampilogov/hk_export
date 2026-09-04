import ActivityKit
import Combine
import Foundation
import SwiftUI
import UserNotifications
import UIKit

// Live Activity attributes moved to ContinuousRecordingAttributes.swift shared by app and widget.

private final class BackgroundTaskToken {
    var identifier: UIBackgroundTaskIdentifier = .invalid

    func end() {
        guard Thread.isMainThread else {
            DispatchQueue.main.async { [self] in self.end() }
            return
        }
        guard identifier != .invalid else { return }
        UIApplication.shared.endBackgroundTask(identifier)
        identifier = .invalid
    }
}

/// Handles periodic recording of raw sensor data and updates a Live Activity
/// with the latest reception timestamps for RR/ECG/ACC streams.
final class ContinuousRecorder: ObservableObject {
    private struct PendingHealthKitImport {
        let fileURL: URL
        let deviceName: String?
    }

    private struct IngestionState {
        var recordingID: UUID?
        var lastRR: Date?
        var lastECG: Date?
        var lastACC: Date?
        var uiPublishScheduled = false
    }

    private struct IngestionSnapshot {
        let recordingID: UUID
        let lastRR: Date?
        let lastECG: Date?
        let lastACC: Date?
    }

    private static let liveActivityMinimumUpdateInterval: TimeInterval = 30
    private static let uiTimestampMinimumUpdateInterval: TimeInterval = 1

    private let manager: BluetoothManager
    private let recorder = SensorBagRecorder()
    private let ingestionQueue = DispatchQueue(
        label: "com.fitness_exporter.continuousIngestion",
        qos: .utility
    )
    private let fileWriteQueue = DispatchQueue(
        label: "com.fitness_exporter.continuousFileWriter",
        qos: .utility
    )
    private var subscriptions = Set<AnyCancellable>()
    /// Drives switching between write-on and write-off windows
    private var sessionTimer: Timer?
    private var staleTimer: Timer?
    private var activity: Activity<ContinuousRecordingAttributes>?
    private var activityStateTask: Task<Void, Never>?
    private var isRunning = false
    private var isWriteWindow = false
    private var durationSeconds = 0
    private var intervalSeconds = 0
    private var captureStart: Date?
    private var lastRRReceivedAt: Date?
    private var lastECGReceivedAt: Date?
    private var lastACCReceivedAt: Date?
    private var lastLiveActivityUpdateAt: Date?
    private var activeRecordingID: UUID?
    private var healthKitImports: [PendingHealthKitImport] = []
    private var healthKitImportInFlight = false
    private var ingestionState = IngestionState()
    private var uiPublishWorkItem: DispatchWorkItem?
    private var discardBufferedEventsAtNextWindowStart = false

    /// Last reception timestamps for each sensor stream.
    @Published var lastRR: Date?
    @Published var lastECG: Date?
    @Published var lastACC: Date?

    init(manager: BluetoothManager) {
        self.manager = manager
    }

    /// Start continuous capture; timers only control batch writes.
    func start(durationSeconds: Int, intervalSeconds: Int) {
        self.durationSeconds = durationSeconds
        self.intervalSeconds = intervalSeconds
        guard !isRunning else {
            // If already running, just reschedule windows with new timings
            rescheduleWindows()
            return
        }
        isRunning = true
        beginContinuousCapture()
        startWriteWindow()
    }

    /// Stop timers and continuous capture. Optionally flush current write window.
    func stop() {
        guard isRunning else { return }
        manager.drainPendingSensorEvents()
        let finalSnapshot = drainIngestion(cancelScheduledPublish: true)
        subscriptions.removeAll()
        if let finalSnapshot {
            applyIngestionSnapshot(finalSnapshot, requireRunning: false)
        }
        isRunning = false
        sessionTimer?.invalidate()
        sessionTimer = nil
        staleTimer?.invalidate()
        staleTimer = nil
        // If we are in an active write window, flush what's collected.
        if isWriteWindow {
            flushCurrentBatch()
        }
        endContinuousCapture()
    }

    // MARK: - Continuous capture lifecycle
    private func beginContinuousCapture() {
        let start = Date()
        captureStart = start
        lastRR = nil
        lastECG = nil
        lastACC = nil
        lastRRReceivedAt = nil
        lastECGReceivedAt = nil
        lastACCReceivedAt = nil
        lastLiveActivityUpdateAt = nil
        let recordingID = UUID()
        activeRecordingID = recordingID
        recorder.reset()
        resetIngestion(for: recordingID)
        discardBufferedEventsAtNextWindowStart = false
        subscribe(recordingID: recordingID)
        startLiveActivityIfNeeded()
        startStaleTimer()
    }

    private func endContinuousCapture() {
        subscriptions.removeAll()
        endLiveActivity()
        activeRecordingID = nil
    }

    private func startStaleTimer() {
        staleTimer?.invalidate()
        staleTimer = Timer.scheduledTimer(withTimeInterval: 5, repeats: true) { _ in
            self.checkStaleness()
        }
    }

    private func checkStaleness() {
        let now = Date()
        if let last = lastRRReceivedAt, now.timeIntervalSince(last) > 10 {
            notify(title: "RR data stale", body: "No RR data for 10s")
            staleTimer?.invalidate()
        }
        if let last = lastECGReceivedAt, now.timeIntervalSince(last) > 10 {
            notify(title: "ECG data stale", body: "No ECG data for 10s")
            staleTimer?.invalidate()
        }
        if let last = lastACCReceivedAt, now.timeIntervalSince(last) > 10 {
            notify(title: "ACC data stale", body: "No ACC data for 10s")
            staleTimer?.invalidate()
        }
    }

    private func subscribe(recordingID: UUID) {
        subscriptions.removeAll()
        manager.sensorPublisher
            .receive(on: ingestionQueue)
            .sink { [weak self] event in
                self?.ingest(event, recordingID: recordingID)
            }
            .store(in: &subscriptions)

        manager.disconnectPublisher
            .receive(on: DispatchQueue.main)
            .sink { [weak self] _ in
                self?.notify(title: "Connection lost", body: "Peripheral disconnected")
            }
            .store(in: &subscriptions)
    }

    private func resetIngestion(for recordingID: UUID) {
        ingestionQueue.sync {
            uiPublishWorkItem?.cancel()
            uiPublishWorkItem = nil
            ingestionState = IngestionState(recordingID: recordingID)
        }
    }

    private func ingest(_ event: SensorEvent, recordingID: UUID) {
        guard ingestionState.recordingID == recordingID else { return }

        // This is the only continuous-recording append path. It runs on the
        // serial ingestion queue, preserving the publisher's packet order.
        recorder.record(event)

        var shouldPublish = false
        switch event.data {
        case .hrSamples:
            ingestionState.lastRR = event.timestamp
            shouldPublish = true
        case .ecgSamples:
            ingestionState.lastECG = event.timestamp
            shouldPublish = true
        case .accSamples:
            ingestionState.lastACC = event.timestamp
            shouldPublish = true
        default:
            break
        }

        if shouldPublish {
            scheduleIngestionPublishIfNeeded()
        }
    }

    private func scheduleIngestionPublishIfNeeded() {
        guard !ingestionState.uiPublishScheduled else { return }
        ingestionState.uiPublishScheduled = true

        let workItem = DispatchWorkItem { [weak self] in
            guard let self else { return }
            self.ingestionState.uiPublishScheduled = false
            self.uiPublishWorkItem = nil
            guard let snapshot = self.makeIngestionSnapshot() else { return }
            DispatchQueue.main.async { [weak self] in
                self?.applyIngestionSnapshot(snapshot)
            }
        }
        uiPublishWorkItem = workItem
        ingestionQueue.asyncAfter(
            deadline: .now() + Self.uiTimestampMinimumUpdateInterval,
            execute: workItem
        )
    }

    private func makeIngestionSnapshot() -> IngestionSnapshot? {
        guard let recordingID = ingestionState.recordingID else { return nil }
        return IngestionSnapshot(
            recordingID: recordingID,
            lastRR: ingestionState.lastRR,
            lastECG: ingestionState.lastECG,
            lastACC: ingestionState.lastACC
        )
    }

    @discardableResult
    private func drainIngestion(cancelScheduledPublish: Bool = false) -> IngestionSnapshot? {
        ingestionQueue.sync {
            if cancelScheduledPublish {
                uiPublishWorkItem?.cancel()
                uiPublishWorkItem = nil
                ingestionState.uiPublishScheduled = false
            }
            return makeIngestionSnapshot()
        }
    }

    private func applyIngestionSnapshot(
        _ snapshot: IngestionSnapshot,
        requireRunning: Bool = true
    ) {
        guard activeRecordingID == snapshot.recordingID else { return }
        guard !requireRunning || isRunning else { return }

        lastRRReceivedAt = snapshot.lastRR
        lastECGReceivedAt = snapshot.lastECG
        lastACCReceivedAt = snapshot.lastACC
        lastRR = snapshot.lastRR
        lastECG = snapshot.lastECG
        lastACC = snapshot.lastACC

        if snapshot.lastRR != nil || snapshot.lastECG != nil || snapshot.lastACC != nil {
            updateLiveActivityIfDue(at: Date())
        }
    }

    // MARK: - Write window scheduling
    private func rescheduleWindows() {
        guard isRunning else { return }
        sessionTimer?.invalidate()
        // Restart from beginning of a write window using current timings
        startWriteWindow()
    }

    private func startWriteWindow() {
        isWriteWindow = true
        if discardBufferedEventsAtNextWindowStart {
            // Put the reset in the same ordering domain as incoming packets so
            // a packet cannot race across the start of the requested window.
            ingestionQueue.sync {
                recorder.reset()
            }
            discardBufferedEventsAtNextWindowStart = false
        }
        sessionTimer = Timer.scheduledTimer(withTimeInterval: TimeInterval(durationSeconds), repeats: false) { [weak self] _ in
            self?.endWriteWindow()
        }
    }

    private func endWriteWindow() {
        isWriteWindow = false
        flushCurrentBatch()
        let off = max(0, intervalSeconds - durationSeconds)
        discardBufferedEventsAtNextWindowStart = off > 0
        sessionTimer = Timer.scheduledTimer(withTimeInterval: TimeInterval(off), repeats: false) { [weak self] _ in
            self?.startWriteWindow()
        }
    }

    private func flushCurrentBatch() {
        // Rotate the bag on the ingestion queue. Every packet accepted before
        // this boundary lands in this file; every later packet lands in the
        // next one.
        let bag = ingestionQueue.sync {
            recorder.takeAndReset()
        }
        let deviceName = manager.peripheral?.name
        let finalizationTask = beginFileFinalizationTask()

        fileWriteQueue.async { [weak self] in
            let result: Result<URL, Error>
            do {
                let fileURL = try SensorBagPersistence.save(bag, subdir: "continuous")
                result = .success(fileURL)
            } catch {
                result = .failure(error)
            }

            DispatchQueue.main.async {
                finalizationTask.end()
                guard let self else { return }
                switch result {
                case .success(let fileURL):
                    self.enqueueHealthKitImport(
                        fileURL: fileURL,
                        deviceName: deviceName
                    )
                case .failure(let error):
                    CustomLogger.log(
                        "[Continuous] File write failed: \(error.localizedDescription)")
                }
            }
        }
    }

    private func beginFileFinalizationTask() -> BackgroundTaskToken {
        let token = BackgroundTaskToken()
        token.identifier = UIApplication.shared.beginBackgroundTask(
            withName: "ContinuousRecording.FileFinalization"
        ) { [weak token] in
            CustomLogger.log(
                "[Continuous] File-finalization background time expired"
            )
            token?.end()
        }
        return token
    }

    private func enqueueHealthKitImport(fileURL: URL, deviceName: String?) {
        healthKitImports.append(
            PendingHealthKitImport(
                fileURL: fileURL,
                deviceName: deviceName
            ))
        startNextHealthKitImportIfNeeded()
    }

    private func startNextHealthKitImportIfNeeded() {
        guard !healthKitImportInFlight, !healthKitImports.isEmpty else { return }
        healthKitImportInFlight = true
        let pending = healthKitImports.removeFirst()

        SensorBagPersistence.importSavedBagToHealthKit(
            fileURL: pending.fileURL,
            profile: .continuous,
            deviceName: pending.deviceName,
            mode: .newFile
        ) { [weak self] result in
            guard let self else { return }
            if case .failed(let message) = result {
                CustomLogger.log(
                    "[SensorBag][HK] Continuous import failed for "
                        + "\(pending.fileURL.lastPathComponent): \(message)")
            }
            self.healthKitImportInFlight = false
            self.startNextHealthKitImportIfNeeded()
        }
    }

    private func notify(title: String, body: String) {
        let content = UNMutableNotificationContent()
        content.title = title
        content.body = body
        content.sound = .default
        UNUserNotificationCenter.current().add(UNNotificationRequest(identifier: UUID().uuidString, content: content, trigger: nil))
    }

    // MARK: - Live Activity
    @available(iOS 16.1, *)
    private func startLiveActivity() {
        guard ActivityAuthorizationInfo().areActivitiesEnabled else {
            CustomLogger.log("Live Activities are not enabled (capability or widget missing)")
            return
        }
        let attributes = ContinuousRecordingAttributes(name: manager.peripheral?.name ?? "")
        let state = ContinuousRecordingAttributes.ContentState(lastRR: nil, lastECG: nil, lastACC: nil, elapsedSeconds: 0)
        do {
            activity = try Activity.request(attributes: attributes, contentState: state)
            // Observe state and re-request if user dismisses while running
            if let activity = activity {
                activityStateTask?.cancel()
                activityStateTask = Task { [weak self] in
                    guard let self = self else { return }
                    for await st in activity.activityStateUpdates {
                        if !self.isRunning { break }
                        switch st {
                        case .dismissed:
                            // User dismissed; recreate to keep persistent presence
                            self.activity = nil
                            if self.isRunning { self.startLiveActivityIfNeeded() }
                        case .ended:
                            break
                        default:
                            break
                        }
                    }
                }
            }
        } catch {
            CustomLogger.log("Failed to start activity: \(error)")
        }
    }

    private func startLiveActivityIfNeeded() {
        if #available(iOS 16.1, *) {
            if activity == nil { startLiveActivity() }
        }
    }

    private func updateLiveActivityIfDue(at now: Date) {
        if let lastLiveActivityUpdateAt,
            now.timeIntervalSince(lastLiveActivityUpdateAt)
                < Self.liveActivityMinimumUpdateInterval
        {
            return
        }
        lastLiveActivityUpdateAt = now
        updateLiveActivity()
    }

    @available(iOS 16.1, *)
    private func updateLiveActivity() {
        if activity == nil { startLiveActivityIfNeeded() }
        guard let activity = activity else { return }
        let elapsed = captureStart.map { max(0, Int(Date().timeIntervalSince($0))) } ?? 0
        let state = ContinuousRecordingAttributes.ContentState(lastRR: lastRR, lastECG: lastECG, lastACC: lastACC, elapsedSeconds: elapsed)
        Task { await activity.update(using: state) }
    }

    @available(iOS 16.1, *)
    private func endLiveActivity() {
        activityStateTask?.cancel()
        activityStateTask = nil
        guard let activity = activity else { return }
        Task { await activity.end(dismissalPolicy: .immediate) }
        self.activity = nil
    }

    /// Log a custom user event into the current recording bag.
    /// - Parameters:
    ///   - message: User-provided string to record with the event.
    ///   - timestamp: Timestamp to associate with the event (defaults to now).
    func logCustomEvent(_ message: String, at timestamp: Date = Date()) {
        ingestionQueue.async { [weak self] in
            self?.recorder.markCustomEvent(message, at: timestamp)
        }
    }
}
