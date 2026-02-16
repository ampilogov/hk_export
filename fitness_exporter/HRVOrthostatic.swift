import AVFoundation
import Combine
import CoreBluetooth
import CoreLocation
import Dispatch
import HealthKit
import SwiftUI
import UIKit
import UserNotifications

/// Handles the lay‑then‑stand orthostatic HRV test, capturing sensor data and publishing results.
class OrthostaticHRV: NSObject {
    enum FinishReason { case user, timer }

    enum Stage { case preLaying, laying, waitingForStanding, standing, cooldown, done }

    private let manager: BluetoothManager
    /// Recorder for raw sensor events.
    private let recorder = SensorBagRecorder()
    private let warmupDuration: TimeInterval
    private let cooldownDuration: TimeInterval = 2
    private let layingDuration: TimeInterval
    private let standingDuration: TimeInterval
    private let locationManager = CLLocationManager()
    private var recordedLocation: LocationSample?
    private var timerSource: DispatchSourceTimer?
    private let timerQueue = DispatchQueue(label: "OrthostaticHRV.timer")
    private var isFinished = false
    private(set) var finishReason: FinishReason?

    private var stage: Stage = .preLaying
    private let stageSubject = PassthroughSubject<Stage, Never>()
    var stagePublisher: AnyPublisher<Stage, Never> { stageSubject.eraseToAnyPublisher() }

    private let resultsSubject = PassthroughSubject<[HRVStage: StageMetrics], Never>()
    var resultsPublisher: AnyPublisher<[HRVStage: StageMetrics], Never> {
        resultsSubject.eraseToAnyPublisher()
    }

    init(
        manager: BluetoothManager, warmupDuration: TimeInterval, layingDuration: TimeInterval,
        standingDuration: TimeInterval
    ) {
        self.manager = manager
        self.warmupDuration = warmupDuration
        self.layingDuration = layingDuration
        self.standingDuration = standingDuration
        super.init()
        locationManager.delegate = self
    }

    /// Starts the test; automatically transitions from laying to standing, then finishes.
    func start() {
        CustomLogger.log(
            "Starting Orthostatic HRV test: warmup for \(warmupDuration) seconds, laying for \(layingDuration) seconds, standing for \(standingDuration) seconds, cooldown for \(cooldownDuration) seconds"
        )
        recorder.start(with: manager)
        locationManager.requestWhenInUseAuthorization()
        locationManager.requestLocation()

        stage = .preLaying
        stageSubject.send(.preLaying)
        recorder.markHRVProtocolStage(.preLaying)
        timerSource = DispatchSource.makeTimerSource(queue: timerQueue)
        timerSource?.schedule(deadline: .now() + warmupDuration, leeway: .milliseconds(50))
        timerSource?.setEventHandler { [weak self] in
            guard let self = self else { return }
            // End of pre-laying stage
            self.stage = .laying
            self.stageSubject.send(.laying)
            self.recorder.markHRVProtocolStage(.laying)
            self.timerSource?.schedule(
                deadline: .now() + self.layingDuration, leeway: .milliseconds(50))
            self.timerSource?.setEventHandler { [weak self] in
                guard let self = self else { return }
                // End of laying stage
                self.stage = .waitingForStanding
                self.stageSubject.send(.waitingForStanding)
                self.recorder.markHRVProtocolStage(.waitingForStanding)
            }
        }
        timerSource?.resume()
    }

    /// Cancels the test as a user stop.
    func cancel() {
        timerSource?.cancel()
        finish(reason: .user)
    }

    /// Starts the standing phase after user confirmation.
    func startStanding() {
        // End of waiting-for-standing stage
        stage = .standing
        stageSubject.send(.standing)
        recorder.markHRVProtocolStage(.standing)
        timerSource?.schedule(deadline: .now() + standingDuration, leeway: .milliseconds(50))
        timerSource?.setEventHandler { [weak self] in
            guard let self = self else { return }
            // End of standing stage
            self.stage = .cooldown
            self.stageSubject.send(.cooldown)
            self.recorder.markHRVProtocolStage(.cooldown)
            self.timerSource?.schedule(
                deadline: .now() + self.cooldownDuration, leeway: .milliseconds(50))
            self.timerSource?.setEventHandler { [weak self] in
                guard let self = self else { return }
                self.finish(reason: .timer)
            }
        }
    }

    private func finish(reason: FinishReason) {
        self.timerSource?.cancel()
        guard !isFinished else { return }
        isFinished = true
        finishReason = reason
        timerSource?.cancel()

        stage = .done
        stageSubject.send(.done)
        recorder.markHRVProtocolStage(.done)
        if let loc = recordedLocation {
            recorder.recordLocation(loc)
        }

        let bag = recorder.stop()
        do {
            let fileURL = try SensorBagPersistence.save(bag, subdir: "orthostatic")
            CustomLogger.log("Saved orthostatic data to \(fileURL.path)")
            SensorBagPersistence.importSavedBagToHealthKit(
                fileURL: fileURL,
                profile: .orthostatic,
                deviceName: manager.peripheral?.name
            ) { result in
                switch result {
                case .imported, .alreadyPresent:
                    CustomLogger.log("Wrote orthostatic data to HK")
                case .noData:
                    CustomLogger.log("Orthostatic bag had no HK-importable HR/RR data")
                case .failed(let message):
                    CustomLogger.log("[SensorBag][HK] Orthostatic import failed for \(fileURL.lastPathComponent): \(message)")
                }
            }
        } catch {
            CustomLogger.log("Failed to save orthostatic data: \(error)")
        }
        let metrics = processSensorDataBag(bag)
        resultsSubject.send(metrics)
    }

    // Stage-specific filtering is performed locally as required.
}

extension OrthostaticHRV: CLLocationManagerDelegate {
    func locationManager(_ manager: CLLocationManager, didUpdateLocations locations: [CLLocation]) {
        guard let last = locations.last else { return }
        recordedLocation = LocationSample(location: last)
    }

    func locationManager(_ manager: CLLocationManager, didFailWithError error: Error) {
        CustomLogger.log("Location manager failed: \(error)")
    }
}
