//
//  fitness_exporterApp.swift
//  fitness_exporter
//
//  Created by Artem Zinchenko on 3/23/24.
//

import UIKit

import SwiftUI

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
    @State private var isExporting: Bool = false

    var body: some View {
        TabView {
            DateRangeExporterView(isExporting: $isExporting)
                .tabItem {
                    Label("Export", systemImage: "house")
                }

            LogView()
                .tabItem {
                    Label("Logs", systemImage: "gear")
                }
        }
        .disabled(isExporting)
    }
}
