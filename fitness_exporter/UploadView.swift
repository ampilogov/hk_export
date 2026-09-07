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
    @State private var errorText: String?
    @State private var isUploading: Bool = false
    @State private var confirmRemoveDir: Bool = false
    @State private var confirmRemoveDone: Bool = false

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            if let errorText {
                Text(errorText).foregroundColor(.red)
            }

            HStack {
                if let summary {
                    Text("Pending \(summary.pendingCount) / Total \(summary.totalCount)")
                    if isRefreshing {
                        ProgressView()
                            .controlSize(.small)
                    }
                } else {
                    ProgressView("Scanning…")
                        .controlSize(.small)
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
                    Label("Clear .done", systemImage: "trash.slash")
                        .lineLimit(1)
                        .fixedSize(horizontal: true, vertical: false)
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
                .tint(.orange)
                .disabled(isRefreshing || summary?.uploadedCount == 0 || summary == nil)
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
        .alert("Remove .done files?", isPresented: $confirmRemoveDone) {
            Button("Remove", role: .destructive) { removeDoneFiles() }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("Deletes only the metadata JSON files under .done.")
        }
    }

    private func loadCachedSummaryAndRefresh() {
        if summary == nil {
            summary = UploadSummaryCache.load(directoryID: dir.id)
        }
        refreshInventory()
    }

    private func refreshInventory() {
        let requestID = UUID()
        refreshID = requestID
        isRefreshing = true
        let directory = dir

        DispatchQueue.global(qos: .userInitiated).async {
            let result: Result<UploadDirectorySummary, Error>
            if let url = UploadHelper.resolveURL(from: directory.bookmark) {
                let hasAccess = url.startAccessingSecurityScopedResource()
                result = Result {
                    try UploadHelper.inventory(in: url).summary
                }
                if hasAccess {
                    url.stopAccessingSecurityScopedResource()
                }
            } else {
                result = .failure(UploadCoreError.invalidBookmark)
            }

            DispatchQueue.main.async {
                guard self.refreshID == requestID else { return }
                self.isRefreshing = false
                switch result {
                case .success(let newSummary):
                    self.summary = newSummary
                    UploadSummaryCache.store(
                        newSummary,
                        directoryID: directory.id
                    )
                    self.errorText = nil
                case .failure(let error):
                    self.summary = nil
                    self.errorText = error.localizedDescription
                    CustomLogger.log("[Upload][Error] \(error.localizedDescription)")
                }
            }
        }
    }

    private func removeDoneFiles() {
        isRefreshing = true
        let directory = dir
        DispatchQueue.global(qos: .utility).async {
            let error: Error?
            if let base = UploadHelper.resolveURL(from: directory.bookmark) {
                let hasAccess = base.startAccessingSecurityScopedResource()
                do {
                    try UploadHelper.removeLegacyDoneRecords(in: base)
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
                    self.errorText = error.localizedDescription
                    CustomLogger.log("[Upload][Error] \(error.localizedDescription)")
                } else {
                    self.refreshInventory()
                }
            }
        }
    }

    // Upload handled via shared DirectoryUploader now

    private func uploadAll() {
        guard !isUploading else { return }
        guard let cfg = DirectoryUploader.getServerAndSender() else { self.errorText = "Server URL or Sender is empty"; return }

        // Prepare list of pending files
        isUploading = true
        errorText = nil
        DirectoryUploader.uploadAll(dir: dir, server: cfg.server, sender: cfg.sender, stopOnError: true) { err in
            DispatchQueue.main.async {
                if let err = err { self.errorText = err }
                self.refreshInventory()
                self.isUploading = false
            }
        }
    }
}
