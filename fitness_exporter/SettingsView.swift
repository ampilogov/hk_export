import HealthKit
import SwiftUI
import UniformTypeIdentifiers

struct SettingsView: View {
    let isRecordingActive: Bool
    let isAppProcessing: Bool

    @AppStorage(UserDefaultsKeys.SERVER_URL) private var server: String =
        "https://192.168.1.67:8000/upload/"
    @AppStorage(UserDefaultsKeys.SENDER) private var sender: String = ""
    @AppStorage(UserDefaultsKeys.AUTO_SERVER_DISCOVERY_ENABLED) private
        var autoServerDiscovery: Bool =
            false
    @AppStorage(UserDefaultsKeys.RECORDING_WATCHDOG_DELAY_SECONDS) private
        var recordingWatchdogDelaySeconds: Int = 180
    @State private var bgRefreshCursorsText: String = ""
    @State private var hkBackfillStatusText: String = ""
    @State private var isHKBackfillRunning: Bool = false
    @State private var showCursorResetConfirmation = false
    @State private var showBackfillResetConfirmation = false
    @AppStorage(UserDefaultsKeys.ECG_EXPORT_DIRECTORY_BOOKMARK) private var ecgExportDirectoryBookmark = Data()
    @AppStorage(UserDefaultsKeys.MIGRATE_LEGACY_HEART_RATES) private var migrateLegacyHeartRates = false
    @State private var choosingECGExportDirectory = false
    @State private var ecgExportDirectoryName = "Not selected"
    @State private var ecgExportDirectoryError: String?

    init(isRecordingActive: Bool = false, isAppProcessing: Bool = false) {
        self.isRecordingActive = isRecordingActive
        self.isAppProcessing = isAppProcessing
    }

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

            Section(header: Text("Continuous recording safety")) {
                Picker(
                    "Alert if recording stops",
                    selection: $recordingWatchdogDelaySeconds
                ) {
                    Text("2 minutes").tag(120)
                    Text("3 minutes").tag(180)
                    Text("5 minutes").tag(300)
                    Text("8 minutes").tag(480)
                }
                Text(
                    "This alert is independent of the five-minute recording file interval."
                )
                .font(.caption)
                .foregroundColor(.secondary)
            }

            Section(header: Text("Background refresh")) {
                Button("HK register observers") {
                    HealthKitManager.initialize(startObservers: true) {
                        success in
                        CustomLogger.log(
                            "Request authorization and start observers success: \(success)"
                        )
                    }
                }
                .padding()
                .background(Color.blue)
                .foregroundColor(.white)
                .cornerRadius(8)

                Button("Reset background refresh cursors", role: .destructive) {
                    showCursorResetConfirmation = true
                }
                .disabled(resetControlsDisabled)
                .padding()
                .background(resetControlsDisabled ? Color.gray : Color.red)
                .foregroundColor(.white)
                .cornerRadius(8)
                .confirmationDialog(
                    "Reset all background export cursors?",
                    isPresented: $showCursorResetConfirmation,
                    titleVisibility: .visible
                ) {
                    Button("Reset Cursors", role: .destructive) {
                        resetBackgroundRefreshCursors()
                    }
                    Button("Cancel", role: .cancel) { }
                } message: {
                    Text(
                        "The next incremental export may reprocess HealthKit data from 2001. "
                            + "No export starts automatically."
                    )
                }

                Button("Check background refresh cursors") {
                    bgRefreshCursorsText =
                        SettingsView.getBgRefreshCursorsText()
                }
                .padding()
                .background(Color.blue)
                .foregroundColor(.white)
                .cornerRadius(8)

                if !bgRefreshCursorsText.isEmpty {
                    Text(bgRefreshCursorsText)
                }

                Toggle(isOn: $autoServerDiscovery) {
                    Text("Enable auto server discovery")
                }
            }

            Section(header: Text("Auto server discovery")) {
                Button("Start auto server discovery") {
                    AutoServerDiscovery.run {
                        _ in
                    }
                }
                .padding()
                .background(Color.blue)
                .foregroundColor(.white)
                .cornerRadius(8)
            }

            Section(header: Text("SensorBag HealthKit")) {
                Toggle("Migrate legacy heart rate samples", isOn: $migrateLegacyHeartRates)
                    .disabled(resetControlsDisabled)
                if migrateLegacyHeartRates {
                    Text(
                        "Backfill checks saved bags for old individual heart rate samples. "
                            + "It removes only verified samples from this app after a replacement "
                            + "series is readable. The switch turns off after a successful migration."
                    )
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                Button(isHKBackfillRunning ? "Backfilling..." : "Backfill missing HK data") {
                    runSensorBagBackfill()
                }
                .disabled(resetControlsDisabled)
                .font(.footnote)

                Button("Reset backfill memory", role: .destructive) {
                    showBackfillResetConfirmation = true
                }
                .disabled(resetControlsDisabled)
                .font(.footnote)
                .confirmationDialog(
                    "Reset SensorBag HealthKit backfill memory?",
                    isPresented: $showBackfillResetConfirmation,
                    titleVisibility: .visible
                ) {
                    Button("Reset Backfill Memory", role: .destructive) {
                        resetSensorBagBackfillMemory()
                    }
                    Button("Cancel", role: .cancel) { }
                } message: {
                    Text(
                        "Previously imported files and heart rate migrations will become pending again. "
                            + "No backfill starts automatically."
                    )
                }

                if !hkBackfillStatusText.isEmpty {
                    Text(hkBackfillStatusText)
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }

            Section(header: Text("ECG export")) {
                LabeledContent("Destination", value: ecgExportDirectoryName)
                Button("Choose ECG export directory") {
                    choosingECGExportDirectory = true
                }
                Text("Visible ECG samples and a PNG of both graphs are saved in an ECG Exports folder.")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }

            Section(header: Text("Logs")) {
                Button("Clear logs") {
                    CustomLogger.clearLogs()
                }
                .padding()
                .background(Color.blue)
                .foregroundColor(.white)
                .cornerRadius(8)
            }
        }
        .onAppear {
            ecgExportDirectoryName = ECGGraphExport.directoryName(for: ecgExportDirectoryBookmark)
        }
        .fileImporter(
            isPresented: $choosingECGExportDirectory,
            allowedContentTypes: [.folder]
        ) { result in
            switch result {
            case .success(let url):
                do {
                    ecgExportDirectoryBookmark = try ECGGraphExport.bookmark(for: url)
                    ecgExportDirectoryName = url.lastPathComponent
                } catch {
                    ecgExportDirectoryError = error.localizedDescription
                }
            case .failure(let error):
                ecgExportDirectoryError = error.localizedDescription
            }
        }
        .alert("ECG export directory", isPresented: Binding(
            get: { ecgExportDirectoryError != nil },
            set: { if !$0 { ecgExportDirectoryError = nil } }
        )) {
            Button("OK") { ecgExportDirectoryError = nil }
        } message: {
            Text(ecgExportDirectoryError ?? "")
        }
    }

    private var resetControlsDisabled: Bool {
        isRecordingActive || isAppProcessing || isHKBackfillRunning
    }

    private func resetBackgroundRefreshCursors() {
        guard !isRecordingActive, !isAppProcessing else {
            bgRefreshCursorsText =
                "Cursor reset refused: stop the active recording or export first."
            CustomLogger.log("[IE][Error] Cursor reset refused while app work is active")
            return
        }
        do {
            let result = try IncrementalExporter.resetCursors()
            bgRefreshCursorsText =
                "Reset \(result.removedEntries) background refresh cursor entries."
        } catch {
            bgRefreshCursorsText = "Cursor reset failed: \(error.localizedDescription)"
            CustomLogger.log("[IE][Error] Cursor reset failed: \(error.localizedDescription)")
        }
    }

    private func runSensorBagBackfill() {
        guard !isRecordingActive, !isAppProcessing else {
            hkBackfillStatusText =
                "Backfill refused: stop the active recording or export first."
            return
        }
        isHKBackfillRunning = true
        hkBackfillStatusText = "Scanning saved files..."
        SensorBagPersistence.backfillSavedBagsToHealthKit(
            migrateLegacyHeartRates: migrateLegacyHeartRates
        ) { summary in
            isHKBackfillRunning = false
            if let errorMessage = summary.errorMessage {
                hkBackfillStatusText = "Backfill failed: \(errorMessage)"
            } else {
                hkBackfillStatusText =
                    "Total \(summary.totalFiles), pending \(summary.pendingFiles), "
                    + "skipped \(summary.skippedByMemoryFiles), "
                    + "imported \(summary.importedFiles), "
                    + "HR migrated \(summary.migratedHeartRateFiles), "
                    + "unchanged \(summary.unchangedFiles), "
                    + "failed \(summary.failedFiles)."
                if migrateLegacyHeartRates && summary.totalFiles > 0 && summary.failedFiles == 0 {
                    migrateLegacyHeartRates = false
                    hkBackfillStatusText += " Legacy migration turned off."
                }
            }
        }
    }

    private func resetSensorBagBackfillMemory() {
        guard !isRecordingActive, !isAppProcessing else {
            hkBackfillStatusText =
                "Backfill reset refused: stop the active recording or export first."
            CustomLogger.log(
                "[SensorBag][HK] Backfill reset refused while app work is active"
            )
            return
        }
        isHKBackfillRunning = true
        hkBackfillStatusText = "Resetting backfill memory..."
        DispatchQueue.global(qos: .utility).async {
            let result = Result {
                try SensorBagPersistence.resetBackfillMemory()
            }
            DispatchQueue.main.async {
                isHKBackfillRunning = false
                switch result {
                case .success(let resetResult):
                    hkBackfillStatusText =
                        "Reset backfill memory for \(resetResult.removedRecords) files."
                    if let warning = resetResult.warningMessage {
                        hkBackfillStatusText += " Warning: \(warning)"
                        CustomLogger.log("[SensorBag][HK] Backfill reset warning: \(warning)")
                    }
                case .failure(let error):
                    hkBackfillStatusText =
                        "Backfill reset failed: \(error.localizedDescription)"
                    CustomLogger.log(
                        "[SensorBag][HK] Backfill reset failed: "
                            + error.localizedDescription
                    )
                }
            }
        }
    }

    public static func getBgRefreshCursorsText() -> String {
        guard
            let dates =
                (IncrementalExporter.getCursors(
                    sampleTypes:
                        ExportConstants.getSampleTypesOfInterest()
                )?.values.map { $0 })
        else {
            return "Can't aquire the lock"
        }

        let setDates = dates.compactMap { $0 }
        let minDate: Date? =
            (dates.isEmpty || dates.contains(nil))
            ? nil : setDates.min()
        let maxDate: Date? = setDates.max()

        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
        formatter.timeZone = TimeZone.current

        return "Background refresh cursors: ["
            + (minDate == nil ? "nil" : formatter.string(from: minDate!)) + ".."
            + (maxDate == nil ? "nil" : formatter.string(from: maxDate!)) + "]"
    }
}
