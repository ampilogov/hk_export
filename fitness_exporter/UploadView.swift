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
    @State private var resolvedURL: URL?
    @State private var securityScopeActive = false
    @State private var files: [URL] = []
    @State private var doneMap: [String: UploadDoneRecord] = [:]
    @State private var isLoadingDoneMap: Bool = false
    @State private var errorText: String?
    @State private var isUploading: Bool = false
    @State private var confirmRemoveDir: Bool = false
    @State private var confirmRemoveDone: Bool = false

    private var totalCount: Int { files.count }
    private var pendingCount: Int {
        files.filter { !isFileUploaded($0) }.count
    }
    private var uploadedCount: Int { max(0, totalCount - pendingCount) }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            if let errorText {
                Text(errorText).foregroundColor(.red)
            }

            HStack {
                if isLoadingDoneMap {
                    Text("Pending ? / Total \(totalCount)")
                } else {
                    Text("Pending \(pendingCount) / Total \(totalCount)")
                }
                Spacer()
                Button("Upload", action: uploadAll)
                    .buttonStyle(.borderedProminent)
                    .controlSize(.small)
                    .disabled(isUploading || isLoadingDoneMap || pendingCount == 0)
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
                .disabled(isLoadingDoneMap || uploadedCount == 0)
            }
        }
        .onAppear(perform: resolveAndScan)
        .onDisappear(perform: stopAccessIfNeeded)
        .onChange(of: trigger) { _ in
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

    private func resolveAndScan() {
        if let url = UploadHelper.resolveURL(from: dir.bookmark) {
            resolvedURL = url
            securityScopeActive = url.startAccessingSecurityScopedResource()
            refreshFileList()
        } else {
            errorText = "Cannot access directory"
        }
    }

    private func stopAccessIfNeeded() {
        if securityScopeActive, let url = resolvedURL {
            url.stopAccessingSecurityScopedResource()
            securityScopeActive = false
        }
    }

    private func refreshFileList() {
        guard let url = resolvedURL else { return }
        self.files = UploadHelper.listFiles(in: url)
        self.isLoadingDoneMap = true
        DispatchQueue.global(qos: .userInitiated).async {
            let map = UploadHelper.loadDoneMap(for: url)
            DispatchQueue.main.async {
                self.doneMap = map
                self.isLoadingDoneMap = false
            }
        }
    }

    private func isFileUploaded(_ url: URL) -> Bool {
        guard let rec = doneMap[url.lastPathComponent] else { return false }
        return UploadHelper.recordMatchesFile(rec, fileURL: url)
    }

    private func removeDoneFiles() {
        guard let base = resolvedURL else { return }
        let doneDir = base.appendingPathComponent(".done", isDirectory: true)
        let fm = FileManager.default
        if let entries = try? fm.contentsOfDirectory(at: doneDir, includingPropertiesForKeys: nil, options: []) {
            for e in entries {
                try? fm.removeItem(at: e)
            }
        }
        self.doneMap.removeAll()
    }

    // Upload handled via shared DirectoryUploader now

    private func uploadAll() {
        guard !isUploading else { return }
        guard let _ = resolvedURL else { return }
        guard let cfg = DirectoryUploader.getServerAndSender() else { self.errorText = "Server URL or Sender is empty"; return }

        // Prepare list of pending files
        isUploading = true
        errorText = nil
        DirectoryUploader.uploadAll(dir: dir, server: cfg.server, sender: cfg.sender, stopOnError: true) { err in
            DispatchQueue.main.async {
                if let err = err { self.errorText = err }
                self.refreshFileList()
                self.isUploading = false
            }
        }
    }
}
