import ActivityKit
import Combine
import Foundation
import SwiftUI
import UserNotifications
import UIKit
import HealthKit

// Live Activity attributes moved to ContinuousRecordingAttributes.swift shared by app and widget.

private struct ContinuousRecordingDiagnostics: Codable {
    let schemaVersion: Int
    let sessionID: String
    let startedAt: Date
    var stoppedAt: Date? = nil
    let configuredDurationSeconds: Int
    let configuredIntervalSeconds: Int
    let deviceName: String?
    let h10BatteryStartPercent: Int?
    let iPhoneBatteryStartPercent: Int?
    var h10BatteryLatestPercent: Int?
    var iPhoneBatteryLatestPercent: Int?
    var hrPacketCount = 0
    var hrSampleCount = 0
    var rrIntervalCount = 0
    var ecgPacketCount = 0
    var ecgSampleCount = 0
    var accPacketCount = 0
    var accSampleCount = 0
    var disconnectCount = 0
    var liveActivityUpdateCount = 0
    var liveActivityThrottledCount = 0
    var fileWriteAttemptCount = 0
    var fileWriteSuccessCount = 0
    var fileWriteFailureCount = 0
    var pendingFileWriteCount = 0
    var maximumPendingFileWriteCount = 0
    var bytesWritten: Int64 = 0
    var totalFileWriteMilliseconds: Int64 = 0
    var maximumFileWriteMilliseconds: Int64 = 0
    var healthKitEnqueuedCount = 0
    var healthKitCompletedCount = 0
    var healthKitImportedCount = 0
    var healthKitUnchangedCount = 0
    var healthKitFailureCount = 0
    var healthKitQueueDepth = 0
    var maximumHealthKitQueueDepth = 0
    var totalHealthKitMilliseconds: Int64 = 0
    var maximumHealthKitMilliseconds: Int64 = 0
    var generatedAt: Date
}

/// Handles periodic recording of raw sensor data and updates a Live Activity
/// with the latest reception timestamps for RR/ECG/ACC streams.
final class ContinuousRecorder: ObservableObject {
    private struct PendingHealthKitImport {
        let fileURL: URL
        let deviceName: String?
        let sessionID: String
        let enqueuedAt: Date
    }

    private static let liveActivityMinimumUpdateInterval: TimeInterval = 30
    private static let uiTimestampMinimumUpdateInterval: TimeInterval = 1

    private let manager: BluetoothManager
    private let recorder = SensorBagRecorder()
    private let fileWriteQueue = DispatchQueue(
        label: "com.fitness_exporter.continuousFileWriter",
        qos: .utility
    )
    private let diagnosticsWriteQueue = DispatchQueue(
        label: "com.fitness_exporter.continuousDiagnostics",
        qos: .utility
    )
    private var subscriptions = Set<AnyCancellable>()
    /// Drives switching between write-on and write-off windows
    private var sessionTimer: Timer?
    private var staleTimer: Timer?
    private var bgTask: UIBackgroundTaskIdentifier = .invalid
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
    private var diagnostics: ContinuousRecordingDiagnostics?
    private var healthKitImports: [PendingHealthKitImport] = []
    private var healthKitImportInFlight = false
    private var wasBatteryMonitoringEnabled = false

    /// Last reception timestamps for each sensor stream.
    @Published var lastRR: Date?
    @Published var lastECG: Date?
    @Published var lastACC: Date?
    @Published private(set) var diagnosticsReportURL: URL?

    init(manager: BluetoothManager) {
        self.manager = manager
        let reportURL = Self.diagnosticsFileURL()
        if FileManager.default.fileExists(atPath: reportURL.path) {
            diagnosticsReportURL = reportURL
        }
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
        startDiagnostics(at: start)
        recorder.reset()
        recorder.start(with: manager)
        bgTask = UIApplication.shared.beginBackgroundTask(withName: "ContinuousRecording")
        subscribe()
        startLiveActivityIfNeeded()
        startStaleTimer()
    }

    private func endContinuousCapture() {
        subscriptions.removeAll()
        let _ = recorder.stop()
        if bgTask != .invalid {
            UIApplication.shared.endBackgroundTask(bgTask)
            bgTask = .invalid
        }
        endLiveActivity()
        stopDiagnostics()
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

    private func subscribe() {
        subscriptions.removeAll()
        manager.sensorPublisher
            .receive(on: DispatchQueue.main)
            .sink { [weak self] event in
                guard let self = self else { return }
                let now = Date()
                var didUpdate = false
                switch event.data {
                case .hrSamples(let samples):
                    self.diagnostics?.hrPacketCount += 1
                    self.diagnostics?.hrSampleCount += samples.samples.count
                    self.diagnostics?.rrIntervalCount += samples.samples.reduce(0) {
                        $0 + $1.rrIntervals.count
                    }
                    self.lastRRReceivedAt = now
                    if self.shouldPublishTimestamp(now, previous: self.lastRR) {
                        self.lastRR = now
                    }
                    didUpdate = true
                case .ecgSamples(let samples):
                    self.diagnostics?.ecgPacketCount += 1
                    self.diagnostics?.ecgSampleCount += samples.samples.count
                    self.lastECGReceivedAt = now
                    if self.shouldPublishTimestamp(now, previous: self.lastECG) {
                        self.lastECG = now
                    }
                    didUpdate = true
                case .accSamples(let samples):
                    self.diagnostics?.accPacketCount += 1
                    self.diagnostics?.accSampleCount += samples.samples.count
                    self.lastACCReceivedAt = now
                    if self.shouldPublishTimestamp(now, previous: self.lastACC) {
                        self.lastACC = now
                    }
                    didUpdate = true
                case .battery(let sample):
                    self.diagnostics?.h10BatteryLatestPercent = sample.level
                default:
                    break
                }
                if didUpdate { self.updateLiveActivityIfDue(at: now) }
            }
            .store(in: &subscriptions)

        manager.disconnectPublisher
            .receive(on: DispatchQueue.main)
            .sink { [weak self] _ in
                self?.diagnostics?.disconnectCount += 1
                self?.persistDiagnostics()
                self?.notify(title: "Connection lost", body: "Peripheral disconnected")
            }
            .store(in: &subscriptions)
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
        // Start a fresh batch for this window
        recorder.reset()
        sessionTimer = Timer.scheduledTimer(withTimeInterval: TimeInterval(durationSeconds), repeats: false) { [weak self] _ in
            self?.endWriteWindow()
        }
    }

    private func endWriteWindow() {
        isWriteWindow = false
        flushCurrentBatch()
        let off = max(0, intervalSeconds - durationSeconds)
        sessionTimer = Timer.scheduledTimer(withTimeInterval: TimeInterval(off), repeats: false) { [weak self] _ in
            self?.startWriteWindow()
        }
    }

    private func flushCurrentBatch() {
        let bag = recorder.takeAndReset()
        let writeStartedAt = Date()
        let deviceName = manager.peripheral?.name
        let sessionID = diagnostics?.sessionID
        diagnostics?.fileWriteAttemptCount += 1
        diagnostics?.pendingFileWriteCount += 1
        let pendingFileWriteCount = diagnostics?.pendingFileWriteCount ?? 0
        let maximumPendingFileWriteCount = diagnostics?.maximumPendingFileWriteCount ?? 0
        diagnostics?.maximumPendingFileWriteCount = max(
            maximumPendingFileWriteCount,
            pendingFileWriteCount
        )

        fileWriteQueue.async { [weak self] in
            let result: Result<(URL, Int64), Error>
            do {
                let fileURL = try SensorBagPersistence.save(bag, subdir: "continuous")
                let attributes = try FileManager.default.attributesOfItem(atPath: fileURL.path)
                let bytes = (attributes[.size] as? NSNumber)?.int64Value ?? 0
                result = .success((fileURL, bytes))
            } catch {
                result = .failure(error)
            }

            let elapsedMilliseconds = Int64(Date().timeIntervalSince(writeStartedAt) * 1_000)
            DispatchQueue.main.async {
                guard let self else { return }
                if self.diagnostics?.sessionID == sessionID {
                    let pendingCount = self.diagnostics?.pendingFileWriteCount ?? 1
                    self.diagnostics?.pendingFileWriteCount = max(0, pendingCount - 1)
                    self.diagnostics?.totalFileWriteMilliseconds += elapsedMilliseconds
                    let maximumMilliseconds =
                        self.diagnostics?.maximumFileWriteMilliseconds ?? 0
                    self.diagnostics?.maximumFileWriteMilliseconds = max(
                        maximumMilliseconds,
                        elapsedMilliseconds
                    )
                }

                switch result {
                case .success(let (fileURL, bytes)):
                    if self.diagnostics?.sessionID == sessionID {
                        self.diagnostics?.fileWriteSuccessCount += 1
                        self.diagnostics?.bytesWritten += bytes
                    }
                    if let sessionID {
                        self.enqueueHealthKitImport(
                            fileURL: fileURL,
                            deviceName: deviceName,
                            sessionID: sessionID
                        )
                    }
                case .failure(let error):
                    if self.diagnostics?.sessionID == sessionID {
                        self.diagnostics?.fileWriteFailureCount += 1
                    }
                    CustomLogger.log(
                        "[Continuous] File write failed: \(error.localizedDescription)")
                }
                self.persistDiagnostics()
            }
        }
    }

    private func enqueueHealthKitImport(fileURL: URL, deviceName: String?, sessionID: String) {
        healthKitImports.append(
            PendingHealthKitImport(
                fileURL: fileURL,
                deviceName: deviceName,
                sessionID: sessionID,
                enqueuedAt: Date()
            ))
        if diagnostics?.sessionID == sessionID {
            diagnostics?.healthKitEnqueuedCount += 1
            updateHealthKitQueueMetrics(for: sessionID)
        }
        startNextHealthKitImportIfNeeded()
    }

    private func startNextHealthKitImportIfNeeded() {
        guard !healthKitImportInFlight, !healthKitImports.isEmpty else { return }
        healthKitImportInFlight = true
        let pending = healthKitImports.removeFirst()
        updateHealthKitQueueMetrics(for: pending.sessionID)

        SensorBagPersistence.importSavedBagToHealthKit(
            fileURL: pending.fileURL,
            profile: .continuous,
            deviceName: pending.deviceName
        ) { [weak self] result in
            guard let self else { return }
            let elapsedMilliseconds = Int64(
                Date().timeIntervalSince(pending.enqueuedAt) * 1_000)
            if self.diagnostics?.sessionID == pending.sessionID {
                self.diagnostics?.healthKitCompletedCount += 1
                self.diagnostics?.totalHealthKitMilliseconds += elapsedMilliseconds
                let maximumMilliseconds =
                    self.diagnostics?.maximumHealthKitMilliseconds ?? 0
                self.diagnostics?.maximumHealthKitMilliseconds = max(
                    maximumMilliseconds,
                    elapsedMilliseconds
                )
                switch result {
                case .imported:
                    self.diagnostics?.healthKitImportedCount += 1
                case .alreadyPresent, .noData:
                    self.diagnostics?.healthKitUnchangedCount += 1
                case .failed:
                    self.diagnostics?.healthKitFailureCount += 1
                }
            }
            if case .failed(let message) = result {
                CustomLogger.log(
                    "[SensorBag][HK] Continuous import failed for "
                        + "\(pending.fileURL.lastPathComponent): \(message)")
            }
            self.healthKitImportInFlight = false
            self.updateHealthKitQueueMetrics(for: pending.sessionID)
            self.persistDiagnostics()
            self.startNextHealthKitImportIfNeeded()
        }
    }

    private func updateHealthKitQueueMetrics(for sessionID: String) {
        guard diagnostics?.sessionID == sessionID else { return }
        let depth = healthKitImports.filter { $0.sessionID == sessionID }.count
            + (healthKitImportInFlight ? 1 : 0)
        diagnostics?.healthKitQueueDepth = depth
        let maximumDepth = diagnostics?.maximumHealthKitQueueDepth ?? 0
        diagnostics?.maximumHealthKitQueueDepth = max(
            maximumDepth,
            depth
        )
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

    private func shouldPublishTimestamp(_ now: Date, previous: Date?) -> Bool {
        guard let previous else { return true }
        return now.timeIntervalSince(previous) >= Self.uiTimestampMinimumUpdateInterval
    }

    private func updateLiveActivityIfDue(at now: Date) {
        if let lastLiveActivityUpdateAt,
            now.timeIntervalSince(lastLiveActivityUpdateAt)
                < Self.liveActivityMinimumUpdateInterval
        {
            diagnostics?.liveActivityThrottledCount += 1
            return
        }
        lastLiveActivityUpdateAt = now
        diagnostics?.liveActivityUpdateCount += 1
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
        // print("Custom message: \(message) \(timestamp)")
        recorder.markCustomEvent(message, at: timestamp)
    }

    // MARK: - Lightweight diagnostics

    private func startDiagnostics(at start: Date) {
        wasBatteryMonitoringEnabled = UIDevice.current.isBatteryMonitoringEnabled
        UIDevice.current.isBatteryMonitoringEnabled = true
        diagnostics = ContinuousRecordingDiagnostics(
            schemaVersion: 1,
            sessionID: UUID().uuidString,
            startedAt: start,
            configuredDurationSeconds: durationSeconds,
            configuredIntervalSeconds: intervalSeconds,
            deviceName: manager.peripheral?.name,
            h10BatteryStartPercent: manager.batteryLevel,
            iPhoneBatteryStartPercent: Self.iPhoneBatteryPercent(),
            h10BatteryLatestPercent: manager.batteryLevel,
            iPhoneBatteryLatestPercent: Self.iPhoneBatteryPercent(),
            generatedAt: start
        )
        persistDiagnostics()
    }

    private func stopDiagnostics() {
        guard diagnostics != nil else { return }
        diagnostics?.stoppedAt = Date()
        if let batteryLevel = manager.batteryLevel {
            diagnostics?.h10BatteryLatestPercent = batteryLevel
        }
        if let batteryPercent = Self.iPhoneBatteryPercent() {
            diagnostics?.iPhoneBatteryLatestPercent = batteryPercent
        }
        persistDiagnostics()
        if let diagnostics {
            CustomLogger.log(
                "[Continuous][Diagnostics] session=\(diagnostics.sessionID) "
                    + "packets(hr/ecg/acc)=\(diagnostics.hrPacketCount)/"
                    + "\(diagnostics.ecgPacketCount)/\(diagnostics.accPacketCount) "
                    + "files=\(diagnostics.fileWriteSuccessCount)/"
                    + "\(diagnostics.fileWriteAttemptCount) bytes=\(diagnostics.bytesWritten) "
                    + "hk=\(diagnostics.healthKitCompletedCount)/"
                    + "\(diagnostics.healthKitEnqueuedCount)")
        }
        UIDevice.current.isBatteryMonitoringEnabled = wasBatteryMonitoringEnabled
    }

    private func persistDiagnostics() {
        guard var snapshot = diagnostics else { return }
        if let batteryLevel = manager.batteryLevel {
            snapshot.h10BatteryLatestPercent = batteryLevel
        }
        if let batteryPercent = Self.iPhoneBatteryPercent() {
            snapshot.iPhoneBatteryLatestPercent = batteryPercent
        }
        snapshot.generatedAt = Date()
        diagnostics = snapshot
        let reportURL = Self.diagnosticsFileURL()

        diagnosticsWriteQueue.async { [weak self] in
            do {
                let encoder = JSONEncoder()
                encoder.dateEncodingStrategy = .iso8601
                encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
                let data = try encoder.encode(snapshot)
                try FileManager.default.createDirectory(
                    at: reportURL.deletingLastPathComponent(),
                    withIntermediateDirectories: true
                )
                try data.write(to: reportURL, options: .atomic)
                DispatchQueue.main.async {
                    self?.diagnosticsReportURL = reportURL
                }
            } catch {
                CustomLogger.log(
                    "[Continuous][Diagnostics] Failed to save report: "
                        + error.localizedDescription)
            }
        }
    }

    private static func diagnosticsFileURL() -> URL {
        FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("diagnostics", isDirectory: true)
            .appendingPathComponent("latest-continuous-recording.json")
    }

    private static func iPhoneBatteryPercent() -> Int? {
        let level = UIDevice.current.batteryLevel
        guard level >= 0 else { return nil }
        return Int((level * 100).rounded())
    }
}
