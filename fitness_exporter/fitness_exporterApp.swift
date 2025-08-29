//
//  fitness_exporterApp.swift
//  fitness_exporter
//
//  Created by Artem Zinchenko on 3/23/24.
//

import SwiftUI
import UIKit

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

            SettingsView()
                .tabItem {
                    Label("Settings", systemImage: "gear")
                }
                .tag(Tab.settings)

            HRVView(isProcessing: $isProcessing)
                .tabItem {
                    Label("HRV", systemImage: "waveform.path.ecg")
                }
                .tag(Tab.hrv)
        }
        .blur(radius: (isProcessing && selection != .hrv) ? 1.0 : 0)
        .onChange(of: selection) { newSelection in
            if isProcessing && newSelection != .hrv {
                selection = .hrv
            }
        }
    }
}
