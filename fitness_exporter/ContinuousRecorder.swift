import ActivityKit
import Combine
import Foundation
import SwiftUI
import UserNotifications
import UIKit
import HealthKit

// Live Activity attributes moved to ContinuousRecordingAttributes.swift shared by app and widget.

/// Handles periodic recording of raw sensor data and updates a Live Activity
/// with the latest reception timestamps for RR/ECG/ACC streams.
final class ContinuousRecorder: ObservableObject {
    private let manager: BluetoothManager
    private let recorder = SensorBagRecorder()
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
        captureStart = Date()
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
    }

    private func startStaleTimer() {
        staleTimer?.invalidate()
        staleTimer = Timer.scheduledTimer(withTimeInterval: 5, repeats: true) { _ in
            self.checkStaleness()
        }
    }

    private func checkStaleness() {
        let now = Date()
        if let last = lastRR, now.timeIntervalSince(last) > 10 {
            notify(title: "RR data stale", body: "No RR data for 10s")
            staleTimer?.invalidate()
        }
        if let last = lastECG, now.timeIntervalSince(last) > 10 {
            notify(title: "ECG data stale", body: "No ECG data for 10s")
            staleTimer?.invalidate()
        }
        if let last = lastACC, now.timeIntervalSince(last) > 10 {
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
                case .hrSamples:
                    self.lastRR = now
                    didUpdate = true
                case .ecgSamples:
                    self.lastECG = now
                    didUpdate = true
                case .accSamples:
                    self.lastACC = now
                    didUpdate = true
                default:
                    break
                }
                if didUpdate { self.updateLiveActivity() }
            }
            .store(in: &subscriptions)

        manager.disconnectPublisher
            .receive(on: DispatchQueue.main)
            .sink { [weak self] _ in
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
        if let _ = try? SensorBagPersistence.save(bag, subdir: "continuous") {
            // Write heartbeat series and HR directly from events
            SensorBagPersistence.writeRRIntervalsToHealthKit(
                from: bag, deviceName: manager.peripheral?.name
            )
            SensorBagPersistence.writeHeartRatesToHealthKit(
                from: bag, deviceName: manager.peripheral?.name
            )
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
}
