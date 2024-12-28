//
//  AppDelegate.swift
//  fitness_exporter
//
//  Created by Artem Zinchenko on 12/11/24.
//

import UIKit
import BackgroundTasks
import OSLog


class AppRefreshOperation: Operation {
    override func main() {
        if isCancelled {
            return
        }
        
        CustomLogger.log("AppRefreshOperation started")

        Thread.sleep(forTimeInterval: 5)

        if isCancelled {
            return
        }

        CustomLogger.log("AppRefreshOperation completed")
    }
}

class ProcessingOperation: Operation {
    override func main() {
        if isCancelled {
            return
        }
        
        CustomLogger.log("ProcessingOperation started")

        Thread.sleep(forTimeInterval: 5)

        if isCancelled {
            return
        }

        CustomLogger.log("ProcessingOperation completed")
    }
}

class AppDelegate:  UIResponder, UIApplicationDelegate {
    static let BG_APP_REFRESH_IDENTIFIER = "com.artemz.fitness_exporter.app_refresh"
    static let BG_PROCESSING_IDENTIFIER = "com.artemz.fitness_exporter.processing"
    
    func scheduleAppRefreshTask() {
        let request = BGAppRefreshTaskRequest(identifier: AppDelegate.BG_APP_REFRESH_IDENTIFIER)
        request.earliestBeginDate = Date(timeIntervalSinceNow: 15 * 60)

        do {
            try BGTaskScheduler.shared.submit(request)
            CustomLogger.log("Scheduled app refresh task")
        } catch {
            CustomLogger.log("Could not schedule app refresh task: \(error)")
        }
    }
    
    func scheduleProcessingTask() {
        do {
            let request = BGProcessingTaskRequest(identifier: AppDelegate.BG_PROCESSING_IDENTIFIER)
            request.requiresNetworkConnectivity = true
            request.requiresExternalPower = false
            try BGTaskScheduler.shared.submit(request)
            CustomLogger.log("Scheduled processing task")
        } catch {
            CustomLogger.log("Failed to schedule processing task: \(error)")
        }
    }

    
    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?
    ) -> Bool {
        os_log("App launched with background fetch enabled", log: OSLog.default, type: .info)
        CustomLogger.log("App launched with background fetch enabled")
        
        BGTaskScheduler.shared.register(forTaskWithIdentifier: AppDelegate.BG_APP_REFRESH_IDENTIFIER, using: nil) { task in
                self.handleAppRefreshTask(task: task as! BGAppRefreshTask)
            }
        
        BGTaskScheduler.shared.register(forTaskWithIdentifier: AppDelegate.BG_PROCESSING_IDENTIFIER, using: nil) { task in
                self.handleProcessingTask(task: task as! BGProcessingTask)
            }

        scheduleAppRefreshTask()
        scheduleProcessingTask()

        return true
    }
    
    func handleAppRefreshTask(task: BGAppRefreshTask) {
        os_log("App refresh task started", log: OSLog.default, type: .info)
        CustomLogger.log("App refresh task started")
        
        // Schedule the next task
        scheduleAppRefreshTask()

        // Perform your work here
        let operation = AppRefreshOperation()
        operation.completionBlock = {
            task.setTaskCompleted(success: !operation.isCancelled)
        }

        let queue = OperationQueue()
        queue.addOperation(operation)

        // If the task is running too long, request extra time or cancel
        task.expirationHandler = {
            queue.cancelAllOperations()
        }
    }
    
    func handleProcessingTask(task: BGProcessingTask) {
        os_log("Processing task started", log: OSLog.default, type: .info)
        CustomLogger.log("Processing task started")
        
        // Schedule the next processing task
        scheduleProcessingTask()
        
        let operation = ProcessingOperation()
        operation.completionBlock = {
            task.setTaskCompleted(success: !operation.isCancelled)
        }

        let queue = OperationQueue()
        queue.addOperation(operation)

        // If the task is running too long, request extra time or cancel
        task.expirationHandler = {
            queue.cancelAllOperations()
        }
    }

}
