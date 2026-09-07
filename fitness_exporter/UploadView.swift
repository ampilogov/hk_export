import SwiftUI
import UniformTypeIdentifiers

// UploadDirectory and UploadDirectoriesStore moved to UploadCore.swift

struct UploadView: View {
    @StateObject private var store = UploadDirectoriesStore()
    @State private var showingImporter = false
    @State private var bulkUploadTrigger = 0

    var body: some View {
        List {
            Section(header: Text("Actions")) {
                Button(action: { showingImporter = true }) {
                    Label("Add Directory", systemImage: "folder.badge.plus")
                }
                if !store.dirs.isEmpty {
                    Button(action: { bulkUploadTrigger += 1 }) {
                        Label("Upload All", systemImage: "arrow.up.circle")
                    }
                }
            }

            if store.dirs.isEmpty {
                Section(header: Text("Directories")) {
                    Text("No directories added yet").foregroundColor(.secondary)
                }
            } else {
                ForEach(store.dirs) { dir in
                    Section(header: Text(dir.name)) {
                        UploadDirectorySection(dir: dir, trigger: bulkUploadTrigger, onRemove: {
                            store.remove(dir)
                        })
                    }
                }
            }
        }
        .navigationTitle("Upload")
        .fileImporter(
            isPresented: $showingImporter,
            allowedContentTypes: [.folder],
            allowsMultipleSelection: true
        ) { result in
            switch result {
            case .success(let urls):
                urls.forEach { store.add(url: $0) }
            case .failure(let error):
                CustomLogger.log("[Upload] Import failed: \(error)")
            }
        }
    }
}

private struct UploadDirectorySection: View {
    let dir: UploadDirectory
    let trigger: Int
    let onRemove: () -> Void
    @State private var summary: UploadDirectorySummary?
    @State private var isRefreshing = false
    @State private var refreshID = UUID()
    @State private var inventoryErrorText: String?
    @State private var actionErrorText: String?
    @State private var isUploading: Bool = false
    @State private var confirmRemoveDir: Bool = false
    @State private var confirmRemoveDone: Bool = false

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            if let actionErrorText {
                Text(actionErrorText).foregroundColor(.red)
            }
            if let inventoryErrorText,
               inventoryErrorText != actionErrorText {
                Text(inventoryErrorText).foregroundColor(.red)
            }

            HStack {
                if let summary {
                    Text("Pending \(summary.pendingCount) / Total \(summary.totalCount)")
                    if isRefreshing {
                        ProgressView()
                            .controlSize(.small)
                    }
                } else if isRefreshing {
                    ProgressView("Scanning…")
                        .controlSize(.small)
                } else {
                    Text("Counts unavailable")
                        .foregroundColor(.secondary)
                }
                Spacer()
                Button("Upload", action: uploadAll)
                    .buttonStyle(.borderedProminent)
                    .controlSize(.small)
                    .disabled(
                        isUploading || isRefreshing || summary?.pendingCount == 0
                            || summary == nil
                    )
            }

            HStack {
                Button { confirmRemoveDir = true } label: {
                    Label("Remove Dir", systemImage: "trash")
                        .lineLimit(1)
                        .fixedSize(horizontal: true, vertical: false)
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
                .tint(.red)

                Spacer()

                Button { confirmRemoveDone = true } label: {
                    Label("Reset Upload History", systemImage: "trash.slash")
                        .lineLimit(1)
                        .fixedSize(horizontal: true, vertical: false)
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
                .tint(.orange)
                .disabled(
                    isUploading || isRefreshing
                        || ((summary?.uploadedCount ?? 0) == 0
                            && inventoryErrorText == nil)
                )
            }
        }
        .onAppear(perform: loadCachedSummaryAndRefresh)
        .onChange(of: trigger) { _, _ in
            uploadAll()
        }
        .alert("Remove directory?", isPresented: $confirmRemoveDir) {
            Button("Remove", role: .destructive) { onRemove() }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("This only removes the directory from the list. Files are not deleted.")
        }
        .alert("Reset upload history?", isPresented: $confirmRemoveDone) {
            Button("Reset", role: .destructive) { resetUploadHistory() }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text(
                "All recordings in this directory will become pending and may "
                    + "upload again. Recording files are not deleted."
            )
        }
    }

    private func loadCachedSummaryAndRefresh() {
        if summary == nil {
            summary = UploadSummaryCache.load(directoryID: dir.id)
        }
        refreshInventory()
    }

    private func refreshInventory(force: Bool = false) {
        let requestID = UUID()
        refreshID = requestID
        isRefreshing = true
        let directory = dir

        UploadInventoryRefreshCoordinator.shared.refresh(
            directory: directory,
            force: force
        ) { result in
            guard self.refreshID == requestID else { return }
            self.isRefreshing = false
            switch result {
            case .success(let newSummary):
                self.summary = newSummary
                UploadSummaryCache.store(
                    newSummary,
                    directoryID: directory.id
                )
                self.inventoryErrorText = nil
            case .failure(let error):
                self.summary = nil
                self.inventoryErrorText = error.localizedDescription
                CustomLogger.log("[Upload][Error] \(error.localizedDescription)")
            }
        }
    }

    private func resetUploadHistory() {
        isRefreshing = true
        let directory = dir
        DispatchQueue.global(qos: .utility).async {
            let error: Error?
            if let base = UploadHelper.resolveURL(from: directory.bookmark) {
                let hasAccess = base.startAccessingSecurityScopedResource()
                do {
                    try UploadHelper.resetCompletionState(in: base)
                    error = nil
                } catch let removalError {
                    error = removalError
                }
                if hasAccess {
                    base.stopAccessingSecurityScopedResource()
                }
            } else {
                error = UploadCoreError.invalidBookmark
            }

            DispatchQueue.main.async {
                if let error {
                    self.isRefreshing = false
                    self.actionErrorText = error.localizedDescription
                    CustomLogger.log("[Upload][Error] \(error.localizedDescription)")
                } else {
                    self.actionErrorText = nil
                    self.refreshInventory(force: true)
                }
            }
        }
    }

    // Upload handled via shared DirectoryUploader now

    private func uploadAll() {
        guard !isUploading else { return }
        guard let cfg = DirectoryUploader.getServerAndSender() else {
            actionErrorText = "Server URL or Sender is empty"
            return
        }

        // Prepare list of pending files
        isUploading = true
        actionErrorText = nil
        DirectoryUploader.uploadAll(dir: dir, server: cfg.server, sender: cfg.sender, stopOnError: true) { err in
            DispatchQueue.main.async {
                self.actionErrorText = err
                self.isUploading = false
                self.refreshInventory(force: true)
            }
        }
    }
}
