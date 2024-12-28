import HealthKit
import SwiftUI

struct DateRangeExporterView: View {
    private static let batchSizeDays = 30

    struct ExportTask {
        var healthStore: HKHealthStore
        var sampleType: HKSampleType
        var start: Date
        var end: Date
        var server: String
        var sender: String
    }

    @State private var exportTasks: [ExportTask] = []
    @State private var nextExportTaskIndex = 0

    @State private var startDate: Date =
        Calendar.current.date(byAdding: .day, value: -7, to: Date()) ?? Date()
    @State private var endDate: Date = Date()
    @State private var showAlert = false
    @State private var alertMessage = ""
    @State private var progress: Double = 0
    @AppStorage("serverURL") private var server: String =
        "https://192.168.1.67:8000/upload/"
    @AppStorage("sender") private var sender: String = ""
    @Binding var isExporting: Bool

    var body: some View {
        Form {
            Section(header: Text("Server")) {
                TextField("server", text: $server)
                    .textFieldStyle(RoundedBorderTextFieldStyle())
                    .padding()
            }
            Section(header: Text("Sender")) {
                TextField("Sender", text: $sender)
                    .textFieldStyle(RoundedBorderTextFieldStyle())
                    .padding()
            }

            Section(header: Text("Select Date Range")) {
                DatePicker(
                    "Start Date:", selection: $startDate,
                    displayedComponents: .date
                )
                .datePickerStyle(CompactDatePickerStyle())
                DatePicker(
                    "End Date:", selection: $endDate, displayedComponents: .date
                )
                .datePickerStyle(CompactDatePickerStyle())
            }

            Section {
                Button("Export Data") {
                    exportDataInRange(
                        from: startDate, to: endDate, server: server,
                        sender: sender)
                }
                .disabled(isExporting)  // Disable button while exporting
                .padding()
                .background(isExporting ? Color.gray : Color.blue)
                .foregroundColor(.white)
                .cornerRadius(8)
                .alert(isPresented: $showAlert) {
                    Alert(
                        title: Text("Error"),
                        message: Text(alertMessage),
                        dismissButton: .default(Text("OK")) {
                            continueExport()
                        }
                    )
                }

                ProgressView(value: progress, total: 1.0)
                    .progressViewStyle(LinearProgressViewStyle())
                    .padding()

            }
        }
    }

    func exportDataInRange(
        from start: Date, to end: Date, server: String, sender: String
    ) {
        //            let allQuantityTypeIdentifiers: [HKQuantityTypeIdentifier] = [
        //                .heartRate, .activeEnergyBurned, .flightsClimbed,
        //            ]
        let quantityTypeIdentifiersToExport = HealthDataExporter.QUANTITY_TYPES
        var quantityTypeToExport: [HKQuantityType] = []
        for quantityTypeIdentifier in quantityTypeIdentifiersToExport {
            guard
                let quantityType = HKQuantityType.quantityType(
                    forIdentifier: quantityTypeIdentifier)
            else {
                CustomLogger.log(
                    "Quantity Type \(quantityTypeIdentifier) is not available in HealthKit"
                )
                continue
            }
            quantityTypeToExport.append(quantityType)
        }

        let sampleTypesOfInterest =
            [
                HKObjectType.workoutType(),
                HKObjectType.categoryType(forIdentifier: .mindfulSession)!,
                HKSeriesType.heartbeat(),
                HKSeriesType.workoutRoute(),
            ] + quantityTypeToExport

        let healthStore = HKHealthStore()
        // TODO: utilize this.
        healthStore.enableBackgroundDelivery(
            for: HKQuantityType.quantityType(forIdentifier: .stepCount)!,
            frequency: .immediate
        ) { success, error in
            if !success {
                CustomLogger.log(
                    "Failed to enable background delivery: \(String(describing: error))"
                )
                abort()
            }
        }
        healthStore.requestAuthorization(
            toShare: Set([]),
            read: Set(sampleTypesOfInterest)
        ) { (okay, error) in
            if let error = error {
                CustomLogger.log("Error requesting authorization: \(error)")
                return
            }
            if !okay {
                CustomLogger.log("Don't have permissions")
                return
            }

            //            let sampleTypesOfInterest_ = [
            //                HKSampleType.quantityType(forIdentifier: HKQuantityTypeIdentifier.distanceWalkingRunning)!,
            //            ]
            //            let sampleTypesOfInterest_ = [
            //                HKSeriesType.workoutRoute(),
            //                HKSeriesType.heartbeat(),
            //                HKObjectType.categoryType(forIdentifier: .mindfulSession)!,
            //            ]
            exportDataInRangeForTypes(
                healthStore: healthStore, sampleTypes: sampleTypesOfInterest,
                from: start, to: end, server: server, sender: sender)
        }
    }

    func exportDataInRangeForTypes(
        healthStore: HKHealthStore, sampleTypes: [HKSampleType],
        from start: Date, to end: Date, server: String, sender: String
    ) {
        exportTasks = []
        nextExportTaskIndex = 0

        var currentDate = start
        while currentDate <= end {
            let nextDate =
                Calendar.current.date(
                    byAdding: .day, value: DateRangeExporterView.batchSizeDays,
                    to: currentDate) ?? currentDate
            for sampleType in sampleTypes {
                exportTasks.append(
                    ExportTask(
                        healthStore: healthStore,
                        sampleType: sampleType,
                        start: currentDate,
                        end: min(nextDate, end),
                        server: server,
                        sender: sender)
                )
            }
            currentDate = nextDate
        }

        continueExport()
    }

    func continueExport() {
        if nextExportTaskIndex == exportTasks.count {
            DispatchQueue.main.async {
                self.progress = 1.0
                self.isExporting = false
                UIApplication.shared.isIdleTimerDisabled = false
            }
            return
        }

        let done = Double(nextExportTaskIndex) / Double(exportTasks.count)
        let task = exportTasks[nextExportTaskIndex]

        DispatchQueue.main.async {
            self.isExporting = true
            self.nextExportTaskIndex += 1
            UIApplication.shared.isIdleTimerDisabled = false
        }

        DispatchQueue.global(qos: .userInitiated).async {
            DispatchQueue.main.async {  // Update UI on main thread
                self.progress = done
            }

            let exporter = HealthDataExporter(
                healthStore: task.healthStore,
                server: server,
                sender: task.sender)
            exporter.export(
                sampleType: task.sampleType, from: task.start, to: task.end
            ) { error in
                //                    CustomLogger.log("Export callback: \(error ?? "Success!")")
                alertMessage = error ?? "Success!"
                let success = error == nil
                showAlert = !success
                if success {
                    continueExport()
                }
            }
        }
    }
}
