//
//  fitness_exporterApp.swift
//  fitness_exporter
//
//  Created by Artem Zinchenko on 3/23/24.
//

import SwiftUI

@main
struct fitness_exporterApp: App {
    var body: some Scene {
        WindowGroup {
            NavigationView {
                DateRangeExporterView()
            }
        }
    }
}
