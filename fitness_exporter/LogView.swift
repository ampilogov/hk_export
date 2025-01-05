import SwiftUI

struct LogView: View {
    @State private var displayedText: String = ""
    @State private var lastLoaded: Date? = nil
    @State private var currentPage: Int = 1
    private let logsPerPage: Int = 50  // Number of logs per page

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

            // Pagination controls
            HStack {
                Button(action: {
                    if currentPage > 1 {
                        currentPage -= 1
                        loadData()
                    }
                }) {
                    Text("Previous")
                        .padding()
                        .frame(maxWidth: .infinity)
                        .background(currentPage > 1 ? Color.blue : Color.gray)
                        .foregroundColor(.white)
                        .cornerRadius(8)
                }
                .disabled(currentPage <= 1)

                Button(action: {
                    if hasNextPage() {
                        currentPage += 1
                        loadData()
                    }
                }) {
                    Text("Next")
                        .padding()
                        .frame(maxWidth: .infinity)
                        .background(hasNextPage() ? Color.blue : Color.gray)
                        .foregroundColor(.white)
                        .cornerRadius(8)
                }
                .disabled(!hasNextPage())
            }

            // Refresh button
            Button(action: {
                currentPage = 1
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
        let logs = fetchLogsForPage(page: currentPage)
        displayedText = formatLogs(logs: logs)
        lastLoaded = Date()
    }

    // Function to fetch logs for a specific page
    private func fetchLogsForPage(page: Int) -> [(Date, String)] {
        let skip = (page - 1) * logsPerPage
        let take = min(
            logsPerPage, CustomLogger.getNumberOfLogsAvailable() - skip)
        return CustomLogger.retrieveLogs(maxLogs: take, skip: skip)
    }

    // Function to format logs for display
    private func formatLogs(logs: [(Date, String)]) -> String {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
        formatter.timeZone = TimeZone.current

        let formattedStrings = logs.map { date, string in
            let localDate = formatter.string(from: date)
            return "\(localDate): \(string)"
        }

        return formattedStrings.joined(separator: "\n")
    }

    // Helper function to format dates
    private func formattedDate(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .short
        return formatter.string(from: date)
    }

    // Helper function to check if there is a next page
    private func hasNextPage() -> Bool {
        let totalLogs = CustomLogger.getNumberOfLogsAvailable()
        return currentPage * logsPerPage < totalLogs
    }
}
