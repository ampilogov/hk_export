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

    var body: some View {
        TabView {
            DateRangeExporterView(isProcessing: $isProcessing)
                .tabItem {
                    Label("Export", systemImage: "house")
                }

            LogView()
                .tabItem {
                    Label("Logs", systemImage: "list.bullet.rectangle")
                }

            SettingsView()
                .tabItem {
                    Label("Settings", systemImage: "gear")
                }
        }
        .disabled(isProcessing)
        .blur(radius: isProcessing ? 1.0 : 0)
    }
}
