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
    struct RecordingAttention: Identifiable {
        let id = UUID()
        let title: String
        let message: String
        let offersWriteRetry: Bool
        let endedRecording: Bool
        let interruptedSessionID: UUID?

        init(
            title: String,
            message: String,
            offersWriteRetry: Bool,
            endedRecording: Bool,
            interruptedSessionID: UUID? = nil
        ) {
            self.title = title
            self.message = message
            self.offersWriteRetry = offersWriteRetry
            self.endedRecording = endedRecording
            self.interruptedSessionID = interruptedSessionID
        }
    }

    private struct PendingHealthKitImport {
        let fileURL: URL
        let deviceName: String?
    }

    private struct PendingBatch {
        let recordingID: UUID
        let bag: SensorBag
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
    private static let watchdogRefreshInterval: TimeInterval = 60
    private static let healthyStreamMaximumAge: TimeInterval = 15

    private let manager: BluetoothManager
    private let recorder = SensorBagRecorder()
    private let watchdog = RecordingWatchdog()
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
    private var lastWatchdogRefreshAt: Date?
    private var lastWatchdogRR: Date?
    private var lastWatchdogECG: Date?
    private var lastWatchdogACC: Date?
    private var pendingWriteCounts: [UUID: Int] = [:]
    private var retainedBatches: [PendingBatch] = []
    private var sessionsAwaitingFinalization = Set<UUID>()

    /// Last reception timestamps for each sensor stream.
    @Published var lastRR: Date?
    @Published var lastECG: Date?
    @Published var lastACC: Date?
    @Published private(set) var attention: RecordingAttention?

    init(manager: BluetoothManager) {
        self.manager = manager
        if let interrupted = RecordingSessionJournal.interruptedSessionForCurrentProcess() {
            let savedDescription = interrupted.lastSavedFileName.map {
                " The last completed file was \($0)."
            } ?? " No completed-file checkpoint was recorded."
            attention = RecordingAttention(
                title: "Recording ended unexpectedly",
                message:
                    "A recording started at \(interrupted.startedAt.formatted()) did not stop normally."
                    + savedDescription
                    + " The unfinished in-memory window may be incomplete.",
                offersWriteRetry: false,
                endedRecording: true,
                interruptedSessionID: interrupted.sessionID
            )
            watchdog.cancelObsoleteAlert()
        }
    }

    /// Start continuous capture; timers only control batch writes.
    func start(durationSeconds: Int, intervalSeconds: Int) {
        guard retainedBatches.isEmpty else {
            attention = RecordingAttention(
                title: "Unsaved recording data",
                message:
                    "Retry saving the retained recording batches before starting another session.",
                offersWriteRetry: true,
                endedRecording: true
            )
            return
        }
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
        guard let recordingID = activeRecordingID else { return }
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
        sessionsAwaitingFinalization.insert(recordingID)
        watchdog.stop(sessionID: recordingID, clearJournal: false)
        endContinuousCapture()
        finishSessionIfPossible(recordingID)
    }

    func dismissAttention() {
        if let sessionID = attention?.interruptedSessionID {
            RecordingSessionJournal.acknowledgeLaunchInterruption(
                sessionID: sessionID)
        }
        attention = nil
    }

    func retryFailedWrites() {
        guard !retainedBatches.isEmpty else { return }
        let batches = retainedBatches
        retainedBatches.removeAll()
        attention = nil
        for batch in batches {
            enqueuePersistence(batch)
        }
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
        lastWatchdogRefreshAt = nil
        lastWatchdogRR = nil
        lastWatchdogECG = nil
        lastWatchdogACC = nil
        let recordingID = UUID()
        activeRecordingID = recordingID
        recorder.reset()
        resetIngestion(for: recordingID)
        discardBufferedEventsAtNextWindowStart = false
        subscribe(recordingID: recordingID)
        watchdog.start(sessionID: recordingID, startedAt: start) { [weak self] message in
            self?.attention = RecordingAttention(
                title: "Recording alerts unavailable",
                message: message,
                offersWriteRetry: false,
                endedRecording: false
            )
        }
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
            let now = Date()
            refreshWatchdogIfHealthy(snapshot: snapshot, at: now)
            updateLiveActivityIfDue(at: now)
        }
    }

    private func refreshWatchdogIfHealthy(
        snapshot: IngestionSnapshot,
        at now: Date
    ) {
        guard isRunning else { return }
        guard
            let rr = snapshot.lastRR,
            let ecg = snapshot.lastECG,
            let acc = snapshot.lastACC,
            now.timeIntervalSince(rr) <= Self.healthyStreamMaximumAge,
            now.timeIntervalSince(ecg) <= Self.healthyStreamMaximumAge,
            now.timeIntervalSince(acc) <= Self.healthyStreamMaximumAge
        else {
            return
        }

        if let lastWatchdogRefreshAt,
            now.timeIntervalSince(lastWatchdogRefreshAt)
                < Self.watchdogRefreshInterval
        {
            return
        }

        if let previousRR = lastWatchdogRR,
            let previousECG = lastWatchdogECG,
            let previousACC = lastWatchdogACC,
            (rr <= previousRR || ecg <= previousECG || acc <= previousACC)
        {
            return
        }

        lastWatchdogRefreshAt = now
        lastWatchdogRR = rr
        lastWatchdogECG = ecg
        lastWatchdogACC = acc
        watchdog.refresh(sessionID: snapshot.recordingID)
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
        guard let recordingID = activeRecordingID else { return }
        // Rotate the bag on the ingestion queue. Every packet accepted before
        // this boundary lands in this file; every later packet lands in the
        // next one.
        let bag = ingestionQueue.sync {
            recorder.takeAndReset()
        }
        guard !bag.isEmpty else { return }
        enqueuePersistence(
            PendingBatch(
                recordingID: recordingID,
                bag: bag,
                deviceName: manager.peripheral?.name
            ))
    }

    private func enqueuePersistence(_ batch: PendingBatch) {
        pendingWriteCounts[batch.recordingID, default: 0] += 1
        let finalizationTask = beginFileFinalizationTask()

        fileWriteQueue.async { [weak self] in
            let result: Result<URL, Error>
            do {
                let fileURL = try SensorBagPersistence.save(
                    batch.bag,
                    subdir: "continuous"
                )
                result = .success(fileURL)
            } catch {
                result = .failure(error)
            }

            DispatchQueue.main.async {
                finalizationTask.end()
                guard let self else { return }
                self.completePersistence(batch: batch, result: result)
            }
        }
    }

    private func completePersistence(
        batch: PendingBatch,
        result: Result<URL, Error>
    ) {
        let remaining = max(0, (pendingWriteCounts[batch.recordingID] ?? 1) - 1)
        if remaining == 0 {
            pendingWriteCounts.removeValue(forKey: batch.recordingID)
        } else {
            pendingWriteCounts[batch.recordingID] = remaining
        }

        switch result {
        case .success(let fileURL):
            watchdog.checkpoint(sessionID: batch.recordingID, fileURL: fileURL)
            enqueueHealthKitImport(
                fileURL: fileURL,
                deviceName: batch.deviceName
            )
        case .failure(let error):
            retainedBatches.append(batch)
            sessionsAwaitingFinalization.insert(batch.recordingID)
            let endedCurrentRecording = stopAfterPersistenceFailure(
                recordingID: batch.recordingID)
            let message =
                "Could not save a continuous-recording batch: \(error.localizedDescription). "
                + (endedCurrentRecording
                    ? "Recording was stopped and the unsaved batches are retained in memory for retry."
                    : "The unsaved batch is retained in memory for retry.")
            CustomLogger.log("[Continuous][Error] \(message)")
            notify(
                title: "Recording save failed",
                body: "Open the app and retry saving the retained recording data.",
                identifier: "ContinuousRecording.FileWriteFailure"
            )
            attention = RecordingAttention(
                title: "Recording save failed",
                message: message,
                offersWriteRetry: true,
                endedRecording: endedCurrentRecording
            )
        }

        finishSessionIfPossible(batch.recordingID)
    }

    private func stopAfterPersistenceFailure(recordingID: UUID) -> Bool {
        guard activeRecordingID == recordingID, isRunning else { return false }
        manager.drainPendingSensorEvents()
        _ = drainIngestion(cancelScheduledPublish: true)
        subscriptions.removeAll()
        isRunning = false
        isWriteWindow = false
        sessionTimer?.invalidate()
        sessionTimer = nil
        staleTimer?.invalidate()
        staleTimer = nil

        let currentBag = ingestionQueue.sync {
            recorder.takeAndReset()
        }
        if !currentBag.isEmpty {
            retainedBatches.append(
                PendingBatch(
                    recordingID: recordingID,
                    bag: currentBag,
                    deviceName: manager.peripheral?.name
                ))
        }
        endContinuousCapture()
        return true
    }

    private func finishSessionIfPossible(_ recordingID: UUID) {
        guard sessionsAwaitingFinalization.contains(recordingID) else { return }
        guard pendingWriteCounts[recordingID] == nil else { return }
        guard !retainedBatches.contains(where: { $0.recordingID == recordingID }) else {
            return
        }
        sessionsAwaitingFinalization.remove(recordingID)
        RecordingSessionJournal.end(sessionID: recordingID)
        watchdog.stop(sessionID: recordingID, clearJournal: false)
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

    private func notify(
        title: String,
        body: String,
        identifier: String = UUID().uuidString
    ) {
        let content = UNMutableNotificationContent()
        content.title = title
        content.body = body
        content.sound = .default
        UNUserNotificationCenter.current().add(
            UNNotificationRequest(
                identifier: identifier,
                content: content,
                trigger: nil
            ))
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
            activity = try Activity.request(
                attributes: attributes,
                content: ActivityContent(
                    state: state,
                    staleDate: Date().addingTimeInterval(watchdog.delay)
                )
            )
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
        let content = ActivityContent(
            state: state,
            staleDate: Date().addingTimeInterval(watchdog.delay)
        )
        Task { await activity.update(content) }
    }

    @available(iOS 16.1, *)
    private func endLiveActivity() {
        activityStateTask?.cancel()
        activityStateTask = nil
        guard let activity = activity else { return }
        let elapsed = captureStart.map {
            max(0, Int(Date().timeIntervalSince($0)))
        } ?? 0
        let state = ContinuousRecordingAttributes.ContentState(
            lastRR: lastRR,
            lastECG: lastECG,
            lastACC: lastACC,
            elapsedSeconds: elapsed
        )
        Task {
            await activity.end(
                ActivityContent(state: state, staleDate: nil),
                dismissalPolicy: .immediate
            )
        }
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
