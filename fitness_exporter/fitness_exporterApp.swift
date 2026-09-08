//
//  fitness_exporterApp.swift
//  fitness_exporter
//
//  Created by Artem Zinchenko on 3/23/24.
//

import SwiftUI
import UIKit

struct AppRecordingStatus: Equatable {
    let startedAt: Date
    let modeName: String
}

final class HRVSessionOwner: ObservableObject {
    let manager: BluetoothManager
    let continuousRecorder: ContinuousRecorder
    let eventBridge: HRVEventBridge

    init() {
        let manager = BluetoothManager()
        self.manager = manager
        continuousRecorder = ContinuousRecorder(manager: manager)
        eventBridge = HRVEventBridge(manager: manager)
    }
}

@main
struct fitness_exporterApp: App {
    @UIApplicationDelegateAdaptor(AppDelegate.self) var appDelegate

    var body: some Scene {
        WindowGroup {
            NavigationView {
                ContentView()
            }
        }
    }
}

struct ContentView: View {
    @State private var isProcessing: Bool = false
    @State private var activeRecording: AppRecordingStatus?
    @StateObject private var hrvSession = HRVSessionOwner()

    private enum Tab: Hashable {
        case export, upload, logs, settings, hrv
    }
    @State private var selection: Tab = .export

    var body: some View {
        TabView(selection: $selection) {
            DateRangeExporterView(isProcessing: $isProcessing)
                .tabItem {
                    Label("Export", systemImage: "house")
                }
                .tag(Tab.export)

            UploadView()
                .tabItem {
                    Label("Upload", systemImage: "tray.and.arrow.up")
                }
                .tag(Tab.upload)

            LogView()
                .tabItem {
                    Label("Logs", systemImage: "list.bullet.rectangle")
                }
                .tag(Tab.logs)

            SettingsView(
                isRecordingActive: activeRecording != nil,
                isAppProcessing: isProcessing
            )
                .tabItem {
                    Label("Settings", systemImage: "gear")
                }
                .tag(Tab.settings)

            HRVView(
                isProcessing: $isProcessing,
                activeRecording: $activeRecording,
                manager: hrvSession.manager,
                continuousRecorder: hrvSession.continuousRecorder,
                eventBridge: hrvSession.eventBridge
            )
                .tabItem {
                    Label("HRV", systemImage: "waveform.path.ecg")
                }
                .tag(Tab.hrv)
        }
        .safeAreaInset(edge: .bottom, spacing: 0) {
            if let activeRecording {
                Button {
                    selection = .hrv
                } label: {
                    HStack(spacing: 8) {
                        Image(systemName: "record.circle.fill")
                            .foregroundColor(.red)
                        Text(activeRecording.modeName)
                            .lineLimit(1)
                        Spacer()
                        Text(activeRecording.startedAt, style: .timer)
                            .monospacedDigit()
                        Image(systemName: "chevron.right")
                    }
                    .font(.footnote.weight(.semibold))
                    .padding(.horizontal)
                    .padding(.vertical, 10)
                    .background(.bar)
                }
                .buttonStyle(.plain)
                .accessibilityLabel("Return to active recording")
            }
        }
    }
}
