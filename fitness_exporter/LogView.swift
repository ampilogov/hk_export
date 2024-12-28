//
//  LogView.swift
//  fitness_exporter
//
//  Created by Artem Zinchenko on 12/22/24.
//

import SwiftUI

struct LogView: View {
    @State private var displayedText: String = ""
    @State private var lastLoaded: Date? = nil

    var body: some View {
        VStack(spacing: 20) {
            // Display the last loaded timestamp
            if let timestamp = lastLoaded {
                Text("Last Loaded: \(formattedDate(timestamp))")
                    .font(.footnote)
                    .foregroundColor(.gray)
            }

            // Scrollable text content
            ScrollView([.vertical, .horizontal]) {
                Text(displayedText)
                    .font(.footnote)  // Smaller font size
                    .padding()
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(Color(UIColor.secondarySystemBackground))
                    .cornerRadius(8)
            }

            // Refresh button
            Button(action: {
                loadData()
            }) {
                Text("Refresh")
                    .padding()
                    .frame(maxWidth: .infinity)
                    .background(Color.blue)
                    .foregroundColor(.white)
                    .cornerRadius(8)
            }
        }
        .padding()
        .onAppear {
            loadData()
        }
    }

    // Function to fetch data and update the state
    private func loadData() {
        // Simulate fetching data from a function call
        displayedText = fetchText()
        lastLoaded = Date()
    }

    // Mock function to simulate fetching data
    private func fetchText() -> String {
        let logs = CustomLogger.retrieveLogs()
        let formatter = DateFormatter()
        // formatter.dateStyle = .medium
        // formatter.timeStyle = .medium
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
        formatter.timeZone = TimeZone.current

        let formattedStrings = logs.map { date, string in
            let localDate = formatter.string(from: date)
            return "\(localDate): \(string)"
        }

        return formattedStrings.reversed().joined(separator: "\n")
    }

    // Helper function to format dates
    private func formattedDate(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .short
        return formatter.string(from: date)
    }
}
