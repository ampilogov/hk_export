import HealthKit
import SwiftUI

struct SettingsView: View {
    @AppStorage(UserDefaultsKeys.SERVER_URL) private var server: String =
        "https://192.168.1.67:8000/upload/"
    @AppStorage(UserDefaultsKeys.SENDER) private var sender: String = ""
    @AppStorage(UserDefaultsKeys.AUTO_SERVER_DISCOVERY_ENABLED) private
        var autoServerDiscovery: Bool =
            false
    @State private var bgRefreshCursorsText: String = ""

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

                Button("Reset background refresh cursors") {
                    IncrementalExporter.resetCursors()
                }
                .padding()
                .background(Color.blue)
                .foregroundColor(.white)
                .cornerRadius(8)

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
