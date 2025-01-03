import HealthKit
import SwiftUI

struct DateRangeExporterView: View {
    private static let batchSizeDays = 30

    struct ExportTask {
        var sampleType: HKSampleType
        var start: Date
        var end: Date
        var server: String
        var sender: String
    }

    @State private var exportTasks: [ExportTask] = []
    @State private var nextExportTaskIndex = 0

    @State private var startDate: Date =
        Calendar.current.date(byAdding: .day, value: -31, to: Date()) ?? Date()
    @State private var endDate: Date = Date()
    @State private var showAlert = false
    @State private var alertMessage = ""
    @State private var progress: Double = 0
    @AppStorage(UserDefaultsKeys.SERVER_URL) private var server: String =
        ""
    @AppStorage(UserDefaultsKeys.SENDER) private var sender: String = ""
    @Binding var isExporting: Bool

    var body: some View {
        Form {
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

                HStack {
                    Button("7d") {
                        updateDates(for: 7)
                    }
                    .buttonStyle(BorderlessButtonStyle())

                    Button("35d") {
                        updateDates(for: 35)
                    }
                    .buttonStyle(BorderlessButtonStyle())

                    Button("100d") {
                        updateDates(for: 100)
                    }
                    .buttonStyle(BorderlessButtonStyle())

                    Button("1y") {
                        updateDates(for: 365)
                    }
                    .buttonStyle(BorderlessButtonStyle())

                    Button("20y") {
                        updateDates(for: 3650 * 2)
                    }
                    .buttonStyle(BorderlessButtonStyle())
                }
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

    private func updateDates(for days: Int) {
        let today = Date()
        endDate = today
        startDate =
            Calendar.current.date(byAdding: .day, value: -days, to: today)
            ?? today
    }

    func exportDataInRange(
        from start: Date, to end: Date, server: String, sender: String
    ) {
        //        let exporter = IncrementalExporter()
        //        exporter.runExport(
        //            sampleTypes: HealthDataExporter.getSampleTypesOfInterest(),
        //            batchSize: 60 * 60 * 24 * 31
        //        ) {
        //            status in
        //            CustomLogger.log("Processing task finished: \(status ?? "nil")")
        //        }
        //        return
        //
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
            read: Set(ExportConstants.getSampleTypesOfInterest())
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
            DispatchQueue.main.async {
                self.isExporting = true
                UIApplication.shared.isIdleTimerDisabled = true
            }

            exportDataInRangeForTypes(
                sampleTypes:
                    ExportConstants.getSampleTypesOfInterest(),
                from: start, to: end, server: server, sender: sender)
        }
    }

    func exportDataInRangeForTypes(
        sampleTypes: [HKSampleType],
        from start: Date, to end: Date, server: String, sender: String
    ) {
        exportTasks = []
        nextExportTaskIndex = 0

        for sampleType in sampleTypes {
            var currentDate = start
            while currentDate <= end {
                let nextDate =
                    Calendar.current.date(
                        byAdding: .day,
                        value: DateRangeExporterView.batchSizeDays,
                        to: currentDate) ?? currentDate
                exportTasks.append(
                    ExportTask(
                        sampleType: sampleType,
                        start: currentDate,
                        end: min(nextDate, end),
                        server: server,
                        sender: sender)
                )
                currentDate = nextDate
            }
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
            UIApplication.shared.isIdleTimerDisabled = true
        }

        DispatchQueue.global(qos: .userInitiated).async {
            DispatchQueue.main.async {  // Update UI on main thread
                self.progress = done
            }

            let exporter = HealthDataExporter(
                server: server, sender: task.sender)
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
