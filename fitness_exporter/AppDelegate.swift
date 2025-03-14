//
//  AppDelegate.swift
//  fitness_exporter
//
//  Created by Artem Zinchenko on 12/11/24.
//

import BackgroundTasks
import OSLog
import UIKit

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

        return true
    }

    func handleAppRefreshTask(task: BGAppRefreshTask) {
        CustomLogger.log("[App] App refresh task started")

        // Schedule the next task
        scheduleAppRefreshTask()

        task.expirationHandler = {
            CustomLogger.log("[App] App refresh task is about to expire")
            task.setTaskCompleted(success: true)
        }

        let exporter = IncrementalExporter()
        exporter.run(
            sampleTypes: ExportConstants.getSampleTypesOfInterest(),
            batchSize: 60 * 60 * 24 * 3
        ) {
            status in
            CustomLogger.log("[App] App refresh task finished: \(status ?? "nil")")
            task.setTaskCompleted(success: true)
        }
    }

    func handleProcessingTask(task: BGProcessingTask) {
        CustomLogger.log("[App] Processing task started")

        // Schedule the next processing task
        scheduleProcessingTask()

        task.expirationHandler = {
            CustomLogger.log("[App] Processing task is about to expire")
            task.setTaskCompleted(success: true)
        }

        let exporter = IncrementalExporter()
        exporter.run(
            sampleTypes: ExportConstants.getSampleTypesOfInterest(),
            batchSize: 60 * 60 * 24 * 10
        ) {
            status in
            CustomLogger.log("[App] Processing task finished: \(status ?? "nil")")
            task.setTaskCompleted(success: true)
        }
    }

}
