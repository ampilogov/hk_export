import HealthKit
import SwiftUI

struct DateRangeExporterView: View {
    private static let batchSizeDays = 30

    typealias ProcessingTask = (@escaping (String?) -> Void) -> Void

    @State private var processingTasks: [ProcessingTask] = []
    @State private var nextProcessingTaskIndex = 0

    @State private var startDate: Date =
        Calendar.current.date(byAdding: .day, value: -31, to: Date()) ?? Date()
    @State private var endDate: Date = Date()
    @State private var cursorsText: String = ""
    @State private var showAlert = false
    @State private var alertMessage = ""
    @State private var progress: Double = 0
    @AppStorage(UserDefaultsKeys.SERVER_URL) private var server: String =
        ""
    @AppStorage(UserDefaultsKeys.SENDER) private var sender: String = ""
    @Binding var isProcessing: Bool

    var body: some View {
        Form {
            Section(header: Text("Manual export")) {
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

                Button("Export Data") {
                    exportDataInRange(
                        from: startDate, to: endDate, server: server,
                        sender: sender)
                }
                .disabled(isProcessing)
                .padding()
                .background(isProcessing ? Color.gray : Color.blue)
                .foregroundColor(.white)
                .cornerRadius(8)
                .alert(isPresented: $showAlert) {
                    Alert(
                        title: Text("Error"),
                        message: Text(alertMessage),
                        dismissButton: .default(Text("OK")) {
                            continueProcessing()
                        }
                    )
                }
            }

            Section(header: Text("Auto export")) {
                Button("Run Auto Export") {
                    cursorsText = ""
                    runIncrementalExporterAndHKObserver(
                        server: server, sender: sender)
                }
                .disabled(isProcessing)
                .padding()
                .background(isProcessing ? Color.gray : Color.blue)
                .foregroundColor(.white)
                .cornerRadius(8)
                .alert(isPresented: $showAlert) {
                    Alert(
                        title: Text("Error"),
                        message: Text(alertMessage),
                        dismissButton: .default(Text("OK")) {
                            continueProcessing()
                        }
                    )
                }

                if !cursorsText.isEmpty {
                    Text(cursorsText)
                }
            }

            Section {
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
        HealthKitManager.initialize(startObservers: false) {
            success in
            if !success {
                alertMessage = "HK Store initialization failed, see logs"
                showAlert = true
                return
            }

            startProcessing(
                tasks:
                    getExportDataInRangeTasks(
                        sampleTypes:
                            ExportConstants.getSampleTypesOfInterest(),
                        from: start, to: end, server: server, sender: sender))
        }
    }

    func runIncrementalExporterAndHKObserver(server: String, sender: String) {
        HealthKitManager.initialize(startObservers: false) {
            success in
            if !success {
                alertMessage = "HK Store initialization failed, see logs"
                showAlert = true
                return
            }

            startProcessing(
                tasks:
                    ExportConstants.getSampleTypesOfInterest().map {
                        { [sampleType = $0] completion in
                            IncrementalExporter().run(
                                sampleTypes: [sampleType],
                                batchSize: 60 * 60 * 24 * 31,
                                completion: completion
                            )
                        }
                    }
                    + [
                        {
                            completion in
                            DispatchQueue.main.sync {
                                cursorsText =
                                    SettingsView.getBgRefreshCursorsText()
                            }
                            HealthKitManager.initialize(startObservers: true) {
                                success in
                                return completion(
                                    success
                                        ? nil
                                        : "HK Store initialization failed, see logs"
                                )
                            }
                        }
                    ]
            )
        }
    }

    func getExportDataInRangeTasks(
        sampleTypes: [HKSampleType],
        from start: Date, to end: Date, server: String, sender: String
    ) -> [ProcessingTask] {
        var tasks: [ProcessingTask] = []

        for sampleType in sampleTypes {
            var currentDate = start
            while currentDate <= end {
                let nextDate =
                    Calendar.current.date(
                        byAdding: .day,
                        value: DateRangeExporterView.batchSizeDays,
                        to: currentDate) ?? currentDate
                tasks.append(
                    {
                        [
                            server, sender, sampleType, from = currentDate,
                            to = min(nextDate, end)
                        ]
                        completion in
                        HealthDataExporter(server: server, sender: sender)
                            .export(
                                sampleType: sampleType, from: from, to: to,
                                completion: completion)
                    }
                )
                currentDate = nextDate
            }
        }

        return tasks
    }

    func startProcessing(tasks: [ProcessingTask]) {
        DispatchQueue.main.sync {
            self.nextProcessingTaskIndex = 0
            self.processingTasks = tasks
            self.isProcessing = true
            UIApplication.shared.isIdleTimerDisabled = true
        }

        continueProcessing()
    }

    func continueProcessing() {
        DispatchQueue.main.async {
            if nextProcessingTaskIndex == processingTasks.count {
                self.progress = 1.0
                self.isProcessing = false
                UIApplication.shared.isIdleTimerDisabled = false
                return
            }

            let done =
                Double(nextProcessingTaskIndex) / Double(processingTasks.count)
            let task = processingTasks[nextProcessingTaskIndex]

            self.isProcessing = true
            self.nextProcessingTaskIndex += 1
            self.progress = done

            DispatchQueue.global(qos: .userInitiated).async {
                task { error in
                    alertMessage = error ?? "Success!"
                    let success = error == nil
                    showAlert = !success
                    if success {
                        continueProcessing()
                    }
                }
            }
        }
    }
}
