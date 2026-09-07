import Foundation
import MetricKit
import UserNotifications

struct ActiveRecordingMarker: Codable {
    let sessionID: UUID
    let startedAt: Date
    var lastSavedFileName: String?
    var lastSavedAt: Date?
}

enum RecordingSessionJournal {
    private static var sessionIDsStartedInThisProcess = Set<UUID>()
    private static var didCheckForLaunchInterruption = false
    private static var launchInterruption: ActiveRecordingMarker?

    static var hasActiveSession: Bool {
        marker != nil
    }

    static func begin(sessionID: UUID, at date: Date) {
        sessionIDsStartedInThisProcess.insert(sessionID)
        write(
            ActiveRecordingMarker(
                sessionID: sessionID,
                startedAt: date,
                lastSavedFileName: nil,
                lastSavedAt: nil
            ))
    }

    static func checkpoint(sessionID: UUID, fileURL: URL, at date: Date) {
        guard var current = marker, current.sessionID == sessionID else { return }
        current.lastSavedFileName = fileURL.lastPathComponent
        current.lastSavedAt = date
        write(current)
    }

    static func end(sessionID: UUID) {
        guard marker?.sessionID == sessionID else { return }
        UserDefaults.standard.removeObject(forKey: UserDefaultsKeys.ACTIVE_CONTINUOUS_RECORDING)
    }

    static func consumeInterruptedSession() -> ActiveRecordingMarker? {
        guard let current = marker else { return nil }
        UserDefaults.standard.removeObject(forKey: UserDefaultsKeys.ACTIVE_CONTINUOUS_RECORDING)
        return current
    }

    static func interruptedSessionForCurrentProcess() -> ActiveRecordingMarker? {
        if !didCheckForLaunchInterruption {
            didCheckForLaunchInterruption = true
            if let current = marker,
                !sessionIDsStartedInThisProcess.contains(current.sessionID)
            {
                launchInterruption = current
            }
        }
        return launchInterruption
    }

    static func acknowledgeLaunchInterruption(sessionID: UUID) {
        guard launchInterruption?.sessionID == sessionID else { return }
        if marker?.sessionID == sessionID {
            UserDefaults.standard.removeObject(
                forKey: UserDefaultsKeys.ACTIVE_CONTINUOUS_RECORDING)
        }
        launchInterruption = nil
    }

    private static var marker: ActiveRecordingMarker? {
        guard
            let data = UserDefaults.standard.data(
                forKey: UserDefaultsKeys.ACTIVE_CONTINUOUS_RECORDING)
        else {
            return nil
        }
        do {
            return try JSONDecoder().decode(ActiveRecordingMarker.self, from: data)
        } catch {
            CustomLogger.log(
                "[Continuous][Journal] Invalid active-session marker: \(error.localizedDescription)"
            )
            return nil
        }
    }

    private static func write(_ marker: ActiveRecordingMarker) {
        do {
            let data = try JSONEncoder().encode(marker)
            UserDefaults.standard.set(
                data,
                forKey: UserDefaultsKeys.ACTIVE_CONTINUOUS_RECORDING)
        } catch {
            CustomLogger.log(
                "[Continuous][Journal] Could not persist session marker: \(error.localizedDescription)"
            )
        }
    }
}

final class RecordingWatchdog {
    static let notificationIdentifier = "ContinuousRecording.Watchdog"

    private let center = UNUserNotificationCenter.current()
    private var activeSessionID: UUID?
    private var generation = 0
    private var notificationOperation: Task<Void, Never>?

    var delay: TimeInterval {
        let configured = UserDefaults.standard.integer(
            forKey: UserDefaultsKeys.RECORDING_WATCHDOG_DELAY_SECONDS)
        return TimeInterval(configured > 0 ? configured : 180)
    }

    func start(
        sessionID: UUID,
        startedAt: Date,
        warning: @escaping (String) -> Void
    ) {
        activeSessionID = sessionID
        RecordingSessionJournal.begin(sessionID: sessionID, at: startedAt)
        schedule(sessionID: sessionID)

        Task { [weak self] in
            guard let self else { return }
            let settings = await center.notificationSettings()
            await MainActor.run {
                guard self.activeSessionID == sessionID else { return }
                switch settings.authorizationStatus {
                case .authorized, .provisional, .ephemeral:
                    if settings.timeSensitiveSetting != .enabled {
                        warning(
                            "Recording alerts are allowed, but Time Sensitive alerts are disabled. Focus may delay a recording warning."
                        )
                    }
                default:
                    warning(
                        "Recording alerts are disabled. Enable notifications in Settings so the app can warn you if recording stops."
                    )
                }
            }
        }
    }

    func refresh(sessionID: UUID) {
        guard activeSessionID == sessionID else { return }
        schedule(sessionID: sessionID)
    }

    func checkpoint(sessionID: UUID, fileURL: URL) {
        guard activeSessionID == sessionID else { return }
        RecordingSessionJournal.checkpoint(
            sessionID: sessionID,
            fileURL: fileURL,
            at: Date()
        )
    }

    func stop(sessionID: UUID, clearJournal: Bool) {
        guard activeSessionID == sessionID else { return }
        activeSessionID = nil
        generation += 1
        if clearJournal {
            RecordingSessionJournal.end(sessionID: sessionID)
        }
        enqueueNotificationOperation { watchdog in
            watchdog.center.removePendingNotificationRequests(
                withIdentifiers: [Self.notificationIdentifier])
            watchdog.center.removeDeliveredNotifications(
                withIdentifiers: [Self.notificationIdentifier])
        }
    }

    func cancelObsoleteAlert() {
        center.removePendingNotificationRequests(
            withIdentifiers: [Self.notificationIdentifier])
        center.removeDeliveredNotifications(
            withIdentifiers: [Self.notificationIdentifier])
    }

    private func schedule(sessionID: UUID) {
        generation += 1
        let scheduledGeneration = generation
        let scheduledDelay = delay

        enqueueNotificationOperation { watchdog in
            guard
                watchdog.activeSessionID == sessionID,
                watchdog.generation == scheduledGeneration
            else {
                return
            }

            let content = UNMutableNotificationContent()
            content.title = "Recording needs attention"
            content.body =
                "The app has not confirmed healthy RR, ECG, and ACC recording. Open it to check the session."
            content.sound = .default
            content.interruptionLevel = .timeSensitive
            content.userInfo = ["recordingSessionID": sessionID.uuidString]

            let trigger = UNTimeIntervalNotificationTrigger(
                timeInterval: scheduledDelay,
                repeats: false
            )
            let request = UNNotificationRequest(
                identifier: Self.notificationIdentifier,
                content: content,
                trigger: trigger
            )

            do {
                try await watchdog.center.add(request)
                guard
                    watchdog.activeSessionID == sessionID,
                    watchdog.generation == scheduledGeneration
                else {
                    // A queued refresh or stop operation will replace or
                    // cancel this request. Keep it in place until then so a
                    // process exit cannot create a no-watchdog interval.
                    return
                }
                watchdog.center.removeDeliveredNotifications(
                    withIdentifiers: [Self.notificationIdentifier])
            } catch {
                CustomLogger.log(
                    "[Continuous][Watchdog] Could not schedule alert: \(error.localizedDescription)"
                )
            }
        }
    }

    private func enqueueNotificationOperation(
        _ operation: @escaping (RecordingWatchdog) async -> Void
    ) {
        let previous = notificationOperation
        notificationOperation = Task { @MainActor [weak self] in
            await previous?.value
            guard let self else { return }
            await operation(self)
        }
    }
}

final class CrashDiagnosticsReporter: NSObject, MXMetricManagerSubscriber {
    static let shared = CrashDiagnosticsReporter()

    private let queue = DispatchQueue(
        label: "com.fitness_exporter.crashDiagnostics",
        qos: .utility
    )
    private let maximumPayloadCount = 5
    private var started = false

    func start() {
        guard !started else { return }
        started = true
        MXMetricManager.shared.add(self)
    }

    func didReceive(_ payloads: [MXDiagnosticPayload]) {
        queue.async { [weak self] in
            self?.persistDiagnostics(payloads)
        }
    }

    func didReceive(_ payloads: [MXMetricPayload]) {
        queue.async { [weak self] in
            self?.persistExitMetrics(payloads)
        }
    }

    private func persistDiagnostics(_ payloads: [MXDiagnosticPayload]) {
        persistJSON(
            payloads.map { (prefix: "metric-diagnostic", data: $0.jsonRepresentation()) }
        )
    }

    private func persistExitMetrics(_ payloads: [MXMetricPayload]) {
        let exitPayloads = payloads.compactMap { payload -> (prefix: String, data: Data)? in
            guard payload.applicationExitMetrics != nil else { return nil }
            return (prefix: "metric-exit", data: payload.jsonRepresentation())
        }
        persistJSON(exitPayloads)
    }

    private func persistJSON(_ payloads: [(prefix: String, data: Data)]) {
        guard !payloads.isEmpty else { return }
        do {
            let directory = try diagnosticsDirectory()
            for payload in payloads {
                let milliseconds = Int64(Date().timeIntervalSince1970 * 1_000)
                let fileURL = directory.appendingPathComponent(
                    "\(payload.prefix)-\(milliseconds)-\(UUID().uuidString).json")
                try payload.data.write(to: fileURL, options: .atomic)
                CustomLogger.log(
                    "[Diagnostics] Saved MetricKit report \(fileURL.lastPathComponent)"
                )
            }
            try prune(directory: directory)
        } catch {
            CustomLogger.log(
                "[Diagnostics] Could not persist MetricKit report: \(error.localizedDescription)"
            )
        }
    }

    private func diagnosticsDirectory() throws -> URL {
        let applicationSupport = try FileManager.default.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        let directory = applicationSupport.appendingPathComponent(
            "CrashDiagnostics",
            isDirectory: true
        )
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )
        return directory
    }

    private func prune(directory: URL) throws {
        let keys: Set<URLResourceKey> = [.contentModificationDateKey]
        let files = try FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: Array(keys),
            options: [.skipsHiddenFiles]
        )
        let sorted = files.sorted {
            let lhs = try? $0.resourceValues(forKeys: keys).contentModificationDate
            let rhs = try? $1.resourceValues(forKeys: keys).contentModificationDate
            return (lhs ?? .distantPast) > (rhs ?? .distantPast)
        }
        for fileURL in sorted.dropFirst(maximumPayloadCount) {
            try FileManager.default.removeItem(at: fileURL)
        }
    }
}
