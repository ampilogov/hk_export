//
//  AppDelegate.swift
//  fitness_exporter
//
//  Created by Artem Zinchenko on 12/11/24.
//

import BackgroundTasks
import OSLog
import UIKit
import UserNotifications

private final class BackgroundTaskCompletionGate {
    private let lock = NSLock()
    private let task: BGTask
    private var completed = false

    init(task: BGTask) {
        self.task = task
    }

    var isCompleted: Bool {
        lock.lock()
        defer { lock.unlock() }
        return completed
    }

    func complete(success: Bool) {
        lock.lock()
        guard !completed else {
            lock.unlock()
            return
        }
        completed = true
        lock.unlock()
        task.setTaskCompleted(success: success)
    }
}

class AppDelegate: UIResponder, UIApplicationDelegate {
    static let BG_APP_REFRESH_IDENTIFIER =
        "com.artemz.fitness_exporter.app_refresh"
    static let BG_PROCESSING_IDENTIFIER =
        "com.artemz.fitness_exporter.processing"

    func scheduleAppRefreshTask() {
        let request = BGAppRefreshTaskRequest(
            identifier: AppDelegate.BG_APP_REFRESH_IDENTIFIER)
        request.earliestBeginDate = Date(timeIntervalSinceNow: 15 * 60)

        do {
            try BGTaskScheduler.shared.submit(request)
            CustomLogger.log("[App] Scheduled app refresh task")
        } catch {
            CustomLogger.log("[App] Could not schedule app refresh task: \(error)")
        }
    }

    func scheduleProcessingTask() {
        do {
            let request = BGProcessingTaskRequest(
                identifier: AppDelegate.BG_PROCESSING_IDENTIFIER)
            request.requiresNetworkConnectivity = true
            request.requiresExternalPower = false
            try BGTaskScheduler.shared.submit(request)
            CustomLogger.log("[App] Scheduled processing task")
        } catch {
            CustomLogger.log("[App] Failed to schedule processing task: \(error)")
        }
    }

    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication
            .LaunchOptionsKey: Any]?
    ) -> Bool {
        let defaults = UserDefaults.standard
        if defaults.string(forKey: UserDefaultsKeys.SERVER_URL) == nil {
            defaults.set(
                "https://192.168.1.67:8000/upload/",
                forKey: UserDefaultsKeys.SERVER_URL)
        }

        os_log(
            "App launched with background fetch enabled", log: OSLog.default,
            type: .info)
        CustomLogger.log("[App] App launched with background fetch enabled")
        CrashDiagnosticsReporter.shared.start()
        BackgroundFileUploadManager.shared.activate()

        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: AppDelegate.BG_APP_REFRESH_IDENTIFIER,
            using: nil
        ) { task in
            self.handleAppRefreshTask(task: task as! BGAppRefreshTask)
        }

        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: AppDelegate.BG_PROCESSING_IDENTIFIER,
            using: nil
        ) { task in
            self.handleProcessingTask(task: task as! BGProcessingTask)
        }

        scheduleAppRefreshTask()
        scheduleProcessingTask()

        ImmediateUploadService.shared.resume { error in
            if let error {
                CustomLogger.log(
                    "[App][ImmediateUpload][Error] Resume failed: \(error)"
                )
            }
        }

        UNUserNotificationCenter.current().delegate = self

        return true
    }

    func application(
        _ application: UIApplication,
        handleEventsForBackgroundURLSession identifier: String,
        completionHandler: @escaping () -> Void
    ) {
        guard identifier == BackgroundFileUploadManager.sessionIdentifier else {
            CustomLogger.log(
                "[Upload][Background][Error] Unknown session identifier: \(identifier)"
            )
            completionHandler()
            return
        }
        BackgroundFileUploadManager.shared.handleEvents(
            completionHandler: completionHandler
        )
    }

    func handleAppRefreshTask(task: BGAppRefreshTask) {
        CustomLogger.log("[App] App refresh task started")

        // Schedule the next task
        scheduleAppRefreshTask()

        runBackgroundExportAndUpload(
            task: task,
            batchSize: 60 * 60 * 24 * 3,
            label: "app refresh"
        )
    }

    func handleProcessingTask(task: BGProcessingTask) {
        CustomLogger.log("[App] Processing task started")

        // Schedule the next processing task
        scheduleProcessingTask()

        runBackgroundExportAndUpload(
            task: task,
            batchSize: 60 * 60 * 24 * 10,
            label: "processing"
        )
    }

    private func runBackgroundExportAndUpload(
        task: BGTask,
        batchSize: TimeInterval,
        label: String
    ) {
        let completionGate = BackgroundTaskCompletionGate(task: task)
        let uploadCancellation = UploadCancellationToken()
        task.expirationHandler = {
            CustomLogger.log("[App] Background \(label) task is about to expire")
            uploadCancellation.cancel()
            completionGate.complete(success: false)
        }

        if RecordingSessionJournal.hasActiveSession {
            CustomLogger.log(
                "[App] Continuous recording is active; deferring HealthKit reads during \(label)"
            )
            runBackgroundUpload(
                completionGate: completionGate,
                cancellationToken: uploadCancellation,
                label: label
            )
            return
        }

        let exporter = IncrementalExporter()
        exporter.run(
            sampleTypes: ExportConstants.getSampleTypesOfInterest(),
            batchSize: batchSize
        ) { status in
            guard !completionGate.isCompleted else { return }
            CustomLogger.log(
                "[App] HK \(label) export finished: \(status ?? "nil")"
            )
            if let status {
                CustomLogger.log("[App][Error] HK \(label) export failed: \(status)")
            }
            self.runBackgroundUpload(
                completionGate: completionGate,
                cancellationToken: uploadCancellation,
                label: label,
                priorError: status
            )
        }
    }

    private func runBackgroundUpload(
        completionGate: BackgroundTaskCompletionGate,
        cancellationToken: UploadCancellationToken,
        label: String,
        priorError: String? = nil
    ) {
        DirectoryUploader.uploadAllFromStore(
            stopOnError: false,
            priority: .background,
            cancellationToken: cancellationToken
        ) { status in
            guard !completionGate.isCompleted else { return }
            CustomLogger.log(
                "[App] Background \(label) file upload finished: \(status ?? "nil")"
            )
            completionGate.complete(success: priorError == nil && status == nil)
        }
    }
}

// MARK: - UNUserNotificationCenterDelegate
extension AppDelegate: UNUserNotificationCenterDelegate {
    func userNotificationCenter(_ center: UNUserNotificationCenter,
                                willPresent notification: UNNotification,
                                withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void) {
        completionHandler([.banner, .list, .sound, .badge])
    }
}
