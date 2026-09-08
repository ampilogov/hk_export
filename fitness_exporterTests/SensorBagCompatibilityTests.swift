import XCTest
import UserNotifications
@testable import fitness_exporter

final class SensorBagCompatibilityTests: XCTestCase {
    func test_v1Serialization_matchesGoldenBytes() throws {
        let events: [SensorEvent] = [
            SensorEvent(
                timestamp: Date(timeIntervalSince1970: 1.5),
                data: .hrSamples(
                    HRSamples(
                        samples: [
                            HRSample(
                                value: 61,
                                contactSupported: true,
                                contactDetected: true,
                                energyExpended: 7,
                                rrIntervals: [0.98, 1.02]
                            )
                        ]
                    )
                )
            ),
            SensorEvent(
                timestamp: Date(timeIntervalSince1970: 2.5),
                data: .ecgSamples(
                    ECGSamples(
                        samples: [
                            ECGSample(timestamp: 100, voltage: -12),
                            ECGSample(timestamp: 104, voltage: 34),
                        ]
                    )
                )
            ),
            SensorEvent(
                timestamp: Date(timeIntervalSince1970: 3.5),
                data: .accSamples(
                    AccSamples(
                        samples: [
                            AccSample(timestamp: 200, x: 1, y: -2, z: 3),
                            AccSample(timestamp: 204, x: -4, y: 5, z: -6),
                        ]
                    )
                )
            ),
            SensorEvent(
                timestamp: Date(timeIntervalSince1970: 4.5),
                data: .battery(BatterySample(level: 87))
            ),
            SensorEvent(
                timestamp: Date(timeIntervalSince1970: 5.5),
                data: .custom("checkpoint")
            ),
        ]

        let actual = SensorBag()._serializeV1(events)
        let expected = try XCTUnwrap(
            Data(
                base64Encoded:
                    "AQAAAAUAAAAAAAAAAAD4PwEAAAABAAAAPQAAAAIAAABcj8L1KFzvP1K4HoXrUfA/AAAAAAAABEACAAAAAgAAAGQAAAAAAAAAaAAAAAAAAAD0/yIAAAAAAAAADEADAAAAAgAAAMgAAAAAAAAAzAAAAAAAAAABAP7/AwD8/wUA+v8AAAAAAAASQAQAAABXAAAAAAAAAAAAFkAHAAAACgAAAGNoZWNrcG9pbnQ="
            ))
        XCTAssertEqual(actual, expected, actual.base64EncodedString())
    }

    func test_highVolumeRotation_preservesEveryDeliveredEventExactlyOnce() {
        let recorder = SensorBagRecorder()
        var completedBags: [SensorBag] = []
        let eventCount = 100_000

        for sequence in 0..<eventCount {
            recorder.record(
                SensorEvent(
                    timestamp: Date(timeIntervalSince1970: TimeInterval(sequence)),
                    data: .custom(String(sequence))
                ))
            if sequence % 137 == 136 {
                completedBags.append(recorder.takeAndReset())
            }
        }
        completedBags.append(recorder.takeAndReset())

        let recovered: [Int] = completedBags.flatMap { bag in
            bag.snapshot.compactMap { event in
                guard case .custom(let value) = event.data else { return nil }
                return Int(value)
            }
        }
        XCTAssertEqual(recovered, Array(0..<eventCount))
    }

    func test_sessionJournal_ignoresLateCompletionFromOlderSession() throws {
        let olderSessionID = UUID()
        let currentSessionID = UUID()
        defer { _ = RecordingSessionJournal.consumeInterruptedSession() }

        RecordingSessionJournal.begin(sessionID: olderSessionID, at: .distantPast)
        RecordingSessionJournal.begin(sessionID: currentSessionID, at: Date())
        RecordingSessionJournal.checkpoint(
            sessionID: olderSessionID,
            fileURL: URL(fileURLWithPath: "/tmp/old.bin"),
            at: Date()
        )
        RecordingSessionJournal.end(sessionID: olderSessionID)

        XCTAssertTrue(RecordingSessionJournal.hasActiveSession)
        let marker = try XCTUnwrap(
            RecordingSessionJournal.consumeInterruptedSession())
        XCTAssertEqual(marker.sessionID, currentSessionID)
        XCTAssertNil(marker.lastSavedFileName)
    }

    func test_fileListing_missingDirectoryReturnsExplicitError() {
        let missingDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)

        XCTAssertThrowsError(
            try UploadHelper.listFiles(in: missingDirectory)
        ) { error in
            guard case UploadCoreError.directoryListFailed(_) = error else {
                XCTFail("Unexpected error: \(error)")
                return
            }
        }
    }

    func test_connectionState_requiresEveryStreamAndRecoversAfterFailure() {
        var state = BluetoothConnectionStateMachine()
        state.requestConnection(requiredStreams: [.hr, .ecg, .acc])

        XCTAssertTrue(state.handle(.connected(generation: 1)))
        XCTAssertTrue(state.handle(.streamReady(.hr, generation: 1)))
        XCTAssertTrue(state.handle(.streamReady(.ecg, generation: 1)))
        XCTAssertFalse(state.isReady)

        XCTAssertTrue(state.handle(.streamReady(.acc, generation: 1)))
        XCTAssertTrue(state.isReady)

        XCTAssertTrue(
            state.handle(
                .streamFailed(.ecg, generation: 1, message: "test failure")
            )
        )
        XCTAssertEqual(state.phase, .degraded)
        XCTAssertFalse(state.isReady)

        XCTAssertTrue(state.handle(.streamReady(.ecg, generation: 1)))
        XCTAssertTrue(state.isReady)
    }

    func test_connectionState_ignoresCallbacksFromObsoleteGeneration() {
        var state = BluetoothConnectionStateMachine()
        state.requestConnection(requiredStreams: [.hr, .ecg, .acc])
        state.handle(.connected(generation: 1))
        state.handle(.disconnected(generation: 1, pairingError: false))
        state.handle(.connected(generation: 2))

        XCTAssertFalse(state.handle(.streamReady(.hr, generation: 1)))
        XCTAssertFalse(
            state.handle(
                .streamFailed(.acc, generation: 1, message: "late callback")
            )
        )
        XCTAssertEqual(state.generation, 2)
        XCTAssertTrue(state.readyStreams.isEmpty)

        state.requestDisconnect()
        XCTAssertFalse(state.handle(.connected(generation: 3)))
        XCTAssertEqual(state.phase, .idle)
        XCTAssertFalse(state.wantsConnection)
    }

    func test_interruptionNotifications_haveStableStagedEscalation() throws {
        let sessionID = UUID()
        let requests = RecordingInterruptionNotifier.makeRequests(
            sessionID: sessionID,
            reason: "Polar disconnected"
        )

        XCTAssertEqual(
            requests.map(\.identifier),
            [
                RecordingInterruptionNotifier.reconnectingIdentifier,
                RecordingInterruptionNotifier.attentionIdentifier,
            ]
        )
        let reconnectingTrigger = try XCTUnwrap(
            requests[0].trigger as? UNTimeIntervalNotificationTrigger
        )
        let attentionTrigger = try XCTUnwrap(
            requests[1].trigger as? UNTimeIntervalNotificationTrigger
        )
        XCTAssertEqual(
            reconnectingTrigger.timeInterval,
            RecordingInterruptionNotifier.reconnectingDelay
        )
        XCTAssertEqual(
            attentionTrigger.timeInterval,
            RecordingInterruptionNotifier.attentionDelay
        )
        XCTAssertNil(requests[0].content.sound)
        XCTAssertNotNil(requests[1].content.sound)
        XCTAssertEqual(requests[0].content.interruptionLevel, .active)
        XCTAssertEqual(requests[1].content.interruptionLevel, .timeSensitive)

        let alreadyStale = RecordingInterruptionNotifier.makeRequests(
            sessionID: sessionID,
            reason: "ECG stale",
            elapsed: 12
        )
        XCTAssertNil(alreadyStale[0].trigger)
        let remainingAttentionTrigger = try XCTUnwrap(
            alreadyStale[1].trigger as? UNTimeIntervalNotificationTrigger
        )
        XCTAssertEqual(remainingAttentionTrigger.timeInterval, 48)
    }

    func test_recordingHealth_requiresActualSamples() {
        let timestamp = Date()
        let emptyRR = SensorEvent(
            timestamp: timestamp,
            data: .hrSamples(
                HRSamples(
                    samples: [
                        HRSample(
                            value: 60,
                            contactSupported: true,
                            contactDetected: false,
                            energyExpended: nil,
                            rrIntervals: []
                        )
                    ]
                )
            )
        )
        let actualRR = SensorEvent(
            timestamp: timestamp,
            data: .hrSamples(
                HRSamples(
                    samples: [
                        HRSample(
                            value: 60,
                            contactSupported: true,
                            contactDetected: true,
                            energyExpended: nil,
                            rrIntervals: [1.0]
                        )
                    ]
                )
            )
        )
        let emptyECG = SensorEvent(
            timestamp: timestamp,
            data: .ecgSamples(ECGSamples(samples: []))
        )
        let emptyACC = SensorEvent(
            timestamp: timestamp,
            data: .accSamples(AccSamples(samples: []))
        )

        XCTAssertNil(emptyRR.recordingHealthStream)
        XCTAssertEqual(actualRR.recordingHealthStream, .hr)
        XCTAssertNil(emptyECG.recordingHealthStream)
        XCTAssertNil(emptyACC.recordingHealthStream)
    }

    func test_interruptionTiming_includesSilenceBeforeDetection() {
        let detectedAt = Date(timeIntervalSince1970: 100)
        let missingSince = RecordingInterruptionTiming.estimatedStart(
            detectedAt: detectedAt,
            alreadyMissingFor: 15
        )

        XCTAssertEqual(missingSince, Date(timeIntervalSince1970: 85))
        XCTAssertEqual(
            RecordingInterruptionTiming.durationMilliseconds(
                from: missingSince,
                to: Date(timeIntervalSince1970: 102)
            ),
            17_000
        )
    }

    func test_interruptionNotificationSchedulingFailure_isSurfaced() async {
        let center = FailingRecordingNotificationCenter()
        let notifier = RecordingInterruptionNotifier(center: center)
        let failureReported = expectation(description: "notification failure surfaced")

        notifier.begin(sessionID: UUID(), reason: "test") { message in
            XCTAssertTrue(message.contains("ContinuousRecording.Reconnecting"))
            XCTAssertTrue(message.contains("ContinuousRecording.ConnectionAttention"))
            failureReported.fulfill()
        }

        await fulfillment(of: [failureReported], timeout: 2)
        XCTAssertEqual(
            center.attemptedIdentifiers,
            [
                RecordingInterruptionNotifier.reconnectingIdentifier,
                RecordingInterruptionNotifier.attentionIdentifier,
            ]
        )
    }

    func test_uploadInventory_countsEachFileOnceAgainstDoneState() throws {
        let fm = FileManager.default
        let base = fm.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try fm.createDirectory(at: base, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: base) }

        let fileCount = 500
        let uploadedCount = 125
        var files: [URL] = []
        for index in 0..<fileCount {
            let file = base.appendingPathComponent(String(format: "%05d.bin", index))
            XCTAssertTrue(fm.createFile(atPath: file.path, contents: Data([UInt8(index % 255)])))
            files.append(file)
        }
        for file in files.prefix(uploadedCount) {
            try UploadHelper.markDone(file: file, base: base)
        }
        let doneDirectory = base.appendingPathComponent(".done", isDirectory: true)
        let legacySidecars = try fm.contentsOfDirectory(
            at: doneDirectory,
            includingPropertiesForKeys: nil
        ).filter { $0.pathExtension == "json" }
        XCTAssertTrue(legacySidecars.isEmpty)

        let initial = try UploadHelper.inventory(in: base)
        XCTAssertEqual(initial.totalCount, fileCount)
        XCTAssertEqual(initial.uploadedCount, uploadedCount)
        XCTAssertEqual(initial.pendingCount, fileCount - uploadedCount)

        try Data([1, 2]).write(to: files[0], options: .atomic)
        let afterMutation = try UploadHelper.inventory(in: base)
        XCTAssertEqual(afterMutation.uploadedCount, uploadedCount - 1)
        XCTAssertEqual(afterMutation.pendingCount, fileCount - uploadedCount + 1)
    }

    func test_uploadInventory_cancelledFailsExplicitly() throws {
        let fm = FileManager.default
        let base = fm.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try fm.createDirectory(at: base, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: base) }
        let token = UploadCancellationToken()
        token.cancel()

        XCTAssertThrowsError(
            try UploadHelper.inventory(in: base, cancellationToken: token)
        ) { error in
            guard case UploadCoreError.cancelled = error else {
                return XCTFail("Unexpected error: \(error)")
            }
        }
    }

    func test_uploadCompletionIndex_migratesAndRemovesVerifiedLegacySidecars() throws {
        let fm = FileManager.default
        let base = fm.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let doneDirectory = base.appendingPathComponent(".done", isDirectory: true)
        try fm.createDirectory(at: doneDirectory, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: base) }

        let files = try (0..<3).map { index -> URL in
            let file = base.appendingPathComponent("legacy-\(index).bin")
            try Data([UInt8(index), 42]).write(to: file, options: .atomic)
            return file
        }
        let originalBytes = try files.map { try Data(contentsOf: $0) }
        let sidecars = try files.map { file -> URL in
            let values = try file.resourceValues(
                forKeys: [.fileSizeKey, .contentModificationDateKey]
            )
            let record = UploadDoneRecord(
                fileName: file.lastPathComponent,
                fileSize: Int64(try XCTUnwrap(values.fileSize)),
                lastModifiedAt: try XCTUnwrap(values.contentModificationDate)
            )
            let sidecar = doneDirectory.appendingPathComponent(
                "\(file.lastPathComponent).json"
            )
            try JSONEncoder().encode(record).write(to: sidecar, options: .atomic)
            return sidecar
        }

        let inventory = try UploadHelper.inventory(in: base)

        XCTAssertEqual(inventory.totalCount, files.count)
        XCTAssertEqual(inventory.uploadedCount, files.count)
        XCTAssertEqual(inventory.pendingCount, 0)
        XCTAssertTrue(
            fm.fileExists(atPath: UploadCompletionIndex.indexURL(in: base).path)
        )
        for sidecar in sidecars {
            XCTAssertFalse(fm.fileExists(atPath: sidecar.path))
        }
        for (index, file) in files.enumerated() {
            XCTAssertEqual(try Data(contentsOf: file), originalBytes[index])
        }

        let reopenedInventory = try UploadHelper.inventory(in: base)
        XCTAssertEqual(reopenedInventory.uploadedCount, files.count)
    }

    func test_uploadCompletionIndex_invalidLegacyRecordFailsWithoutDeletingSources() throws {
        let fm = FileManager.default
        let base = fm.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let doneDirectory = base.appendingPathComponent(".done", isDirectory: true)
        try fm.createDirectory(at: doneDirectory, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: base) }

        let recording = base.appendingPathComponent("recording.bin")
        let recordingBytes = Data([1, 2, 3, 4])
        try recordingBytes.write(to: recording, options: .atomic)
        let invalidSidecar = doneDirectory.appendingPathComponent(
            "z-invalid.bin.json"
        )
        try Data("not-json".utf8).write(to: invalidSidecar, options: .atomic)
        let recordingValues = try recording.resourceValues(
            forKeys: [.fileSizeKey, .contentModificationDateKey]
        )
        let validRecord = UploadDoneRecord(
            fileName: recording.lastPathComponent,
            fileSize: Int64(try XCTUnwrap(recordingValues.fileSize)),
            lastModifiedAt: try XCTUnwrap(recordingValues.contentModificationDate)
        )
        let validSidecar = doneDirectory.appendingPathComponent(
            "\(recording.lastPathComponent).json"
        )
        try JSONEncoder().encode(validRecord).write(
            to: validSidecar,
            options: .atomic
        )

        XCTAssertThrowsError(try UploadHelper.inventory(in: base)) { error in
            guard case UploadCoreError.completionState(let message) = error else {
                return XCTFail("Unexpected error: \(error)")
            }
            XCTAssertTrue(message.contains("z-invalid.bin.json"))
            XCTAssertTrue(message.contains("Decode"))
        }
        XCTAssertTrue(fm.fileExists(atPath: invalidSidecar.path))
        XCTAssertTrue(fm.fileExists(atPath: validSidecar.path))
        XCTAssertEqual(try Data(contentsOf: recording), recordingBytes)

        XCTAssertEqual(try UploadHelper.resetCompletionState(in: base), 2)
        let recoveredInventory = try UploadHelper.inventory(in: base)
        XCTAssertEqual(recoveredInventory.uploadedCount, 0)
        XCTAssertEqual(recoveredInventory.pendingCount, 1)
        XCTAssertFalse(fm.fileExists(atPath: invalidSidecar.path))
        XCTAssertFalse(fm.fileExists(atPath: validSidecar.path))
    }

    func test_uploadCompletionIndex_resetMakesFilesPendingWithoutDeletingRecordings() throws {
        let fm = FileManager.default
        let base = fm.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try fm.createDirectory(at: base, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: base) }

        let files = try (0..<2).map { index -> URL in
            let file = base.appendingPathComponent("reset-\(index).bin")
            try Data([UInt8(index)]).write(to: file, options: .atomic)
            try UploadHelper.markDone(file: file, base: base)
            return file
        }
        XCTAssertEqual(try UploadHelper.inventory(in: base).uploadedCount, 2)

        XCTAssertEqual(try UploadHelper.resetCompletionState(in: base), 2)

        let resetInventory = try UploadHelper.inventory(in: base)
        XCTAssertEqual(resetInventory.uploadedCount, 0)
        XCTAssertEqual(resetInventory.pendingCount, 2)
        XCTAssertTrue(
            fm.fileExists(atPath: UploadCompletionIndex.indexURL(in: base).path)
        )
        for file in files {
            XCTAssertTrue(fm.fileExists(atPath: file.path))
        }
    }

    func test_uploadCompletionIndex_interruptedResetResumesBeforeMigration() throws {
        let fm = FileManager.default
        let base = fm.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try fm.createDirectory(at: base, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: base) }

        let indexedFile = base.appendingPathComponent("indexed.bin")
        let legacyFile = base.appendingPathComponent("legacy.bin")
        try Data([1]).write(to: indexedFile)
        try Data([2]).write(to: legacyFile)
        try UploadHelper.markDone(file: indexedFile, base: base)

        let legacyValues = try legacyFile.resourceValues(
            forKeys: [.fileSizeKey, .contentModificationDateKey]
        )
        let legacyRecord = UploadDoneRecord(
            fileName: legacyFile.lastPathComponent,
            fileSize: Int64(try XCTUnwrap(legacyValues.fileSize)),
            lastModifiedAt: try XCTUnwrap(legacyValues.contentModificationDate)
        )
        let doneDirectory = base.appendingPathComponent(".done", isDirectory: true)
        let sidecar = doneDirectory.appendingPathComponent("legacy.bin.json")
        try JSONEncoder().encode(legacyRecord).write(to: sidecar)
        let resetMarker = doneDirectory.appendingPathComponent("reset-in-progress")
        try Data().write(to: resetMarker)

        let inventory = try UploadHelper.inventory(in: base)

        XCTAssertEqual(inventory.uploadedCount, 0)
        XCTAssertEqual(inventory.pendingCount, 2)
        XCTAssertFalse(fm.fileExists(atPath: sidecar.path))
        XCTAssertFalse(fm.fileExists(atPath: resetMarker.path))
    }

    func test_uploadCoordinator_activeMatchingJobsScheduleOneSerializedRerun() {
        let coordinator = UploadSingleFlightCoordinator()
        let allCompletions = expectation(description: "all callers completed")
        allCompletions.expectedFulfillmentCount = 4
        let stateLock = NSLock()
        var operationStarts = 0
        var runningOperations = 0
        var maximumConcurrentOperations = 0

        func operation() -> UploadSingleFlightCoordinator.Operation {
            { finish in
                stateLock.lock()
                operationStarts += 1
                runningOperations += 1
                maximumConcurrentOperations = max(
                    maximumConcurrentOperations,
                    runningOperations
                )
                stateLock.unlock()

                DispatchQueue.global(qos: .utility).asyncAfter(
                    deadline: .now() + 0.05
                ) {
                    stateLock.lock()
                    runningOperations -= 1
                    stateLock.unlock()
                    finish(nil)
                }
            }
        }

        coordinator.submit(
            key: "same",
            operation: operation()
        ) { _ in allCompletions.fulfill() }
        coordinator.submit(
            key: "same",
            operation: operation()
        ) { _ in allCompletions.fulfill() }
        coordinator.submit(
            key: "same",
            operation: operation()
        ) { _ in allCompletions.fulfill() }
        coordinator.submit(
            key: "same",
            operation: operation()
        ) { _ in allCompletions.fulfill() }

        wait(for: [allCompletions], timeout: 2)
        stateLock.lock()
        let finalStarts = operationStarts
        let finalMaximum = maximumConcurrentOperations
        stateLock.unlock()
        XCTAssertEqual(finalStarts, 2)
        XCTAssertEqual(finalMaximum, 1)
    }

    func test_uploadCoordinator_foregroundPreemptsCancellableBackgroundJob() {
        let coordinator = UploadSingleFlightCoordinator()
        let backgroundStarted = expectation(description: "background started")
        let backgroundCancelled = expectation(description: "background cancelled")
        let foregroundStarted = expectation(description: "foreground started")
        let allCompletions = expectation(description: "both callers completed")
        allCompletions.expectedFulfillmentCount = 2
        let stateLock = NSLock()
        var finishBackground: UploadSingleFlightCoordinator.Completion?

        coordinator.submit(
            key: "background",
            priority: .background,
            cancel: { backgroundCancelled.fulfill() },
            operation: { finish in
                stateLock.lock()
                finishBackground = finish
                stateLock.unlock()
                backgroundStarted.fulfill()
            }
        ) { _ in allCompletions.fulfill() }
        wait(for: [backgroundStarted], timeout: 1)

        coordinator.submit(
            key: "foreground",
            priority: .foreground,
            operation: { finish in
                foregroundStarted.fulfill()
                finish(nil)
            }
        ) { _ in allCompletions.fulfill() }

        wait(for: [backgroundCancelled], timeout: 1)
        stateLock.lock()
        let finish = finishBackground
        stateLock.unlock()
        finish?("Upload cancelled")

        wait(for: [foregroundStarted, allCompletions], timeout: 2)
    }

    func test_uploadCoordinator_skipsCancelledPendingJob() {
        let coordinator = UploadSingleFlightCoordinator()
        let completionCalled = expectation(description: "completion called")
        let token = UploadCancellationToken()
        token.cancel()

        coordinator.submit(
            key: "cancelled",
            shouldRun: { !token.isCancelled },
            operation: { _ in
                XCTFail("Cancelled operation must not start")
            }
        ) { status in
            XCTAssertEqual(status, "Upload cancelled")
            completionCalled.fulfill()
        }

        wait(for: [completionCalled], timeout: 1)
    }

    func test_uploadInventoryRefresh_coalescesAndCachesScans() {
        let scanStarted = expectation(description: "scan started")
        let coalescedCompletions = expectation(
            description: "coalesced refresh completions"
        )
        coalescedCompletions.expectedFulfillmentCount = 2
        let releaseScan = DispatchSemaphore(value: 0)
        let stateLock = NSLock()
        var scanCount = 0
        let expected = UploadDirectorySummary(
            totalCount: 50_000,
            pendingCount: 20,
            uploadedCount: 49_980
        )
        let coordinator = UploadInventoryRefreshCoordinator { _ in
            stateLock.lock()
            scanCount += 1
            stateLock.unlock()
            scanStarted.fulfill()
            releaseScan.wait()
            return .success(expected)
        }
        let directory = UploadDirectory(
            id: UUID(),
            name: "Test",
            bookmark: Data()
        )

        coordinator.refresh(directory: directory) { result in
            XCTAssertEqual(try? result.get(), expected)
            coalescedCompletions.fulfill()
        }
        coordinator.refresh(directory: directory) { result in
            XCTAssertEqual(try? result.get(), expected)
            coalescedCompletions.fulfill()
        }
        wait(for: [scanStarted], timeout: 1)
        releaseScan.signal()
        wait(for: [coalescedCompletions], timeout: 2)

        let cachedCompletion = expectation(description: "cached completion")
        coordinator.refresh(directory: directory) { result in
            XCTAssertEqual(try? result.get(), expected)
            cachedCompletion.fulfill()
        }
        wait(for: [cachedCompletion], timeout: 1)

        stateLock.lock()
        let finalScanCount = scanCount
        stateLock.unlock()
        XCTAssertEqual(finalScanCount, 1)
    }

    func test_uploadInventoryRefresh_forceDuringScanRunsFreshFollowUp() {
        let firstScanStarted = expectation(description: "first scan started")
        let firstCompletion = expectation(description: "first completion")
        let forcedCompletion = expectation(description: "forced completion")
        let releaseFirstScan = DispatchSemaphore(value: 0)
        let stateLock = NSLock()
        var scanCount = 0
        let oldSummary = UploadDirectorySummary(
            totalCount: 1,
            pendingCount: 1,
            uploadedCount: 0
        )
        let freshSummary = UploadDirectorySummary(
            totalCount: 2,
            pendingCount: 0,
            uploadedCount: 2
        )
        let coordinator = UploadInventoryRefreshCoordinator { _ in
            stateLock.lock()
            scanCount += 1
            let currentScan = scanCount
            stateLock.unlock()
            if currentScan == 1 {
                firstScanStarted.fulfill()
                releaseFirstScan.wait()
                return .success(oldSummary)
            }
            return .success(freshSummary)
        }
        let directory = UploadDirectory(
            id: UUID(),
            name: "Test",
            bookmark: Data()
        )

        coordinator.refresh(directory: directory) { result in
            XCTAssertEqual(try? result.get(), oldSummary)
            firstCompletion.fulfill()
        }
        wait(for: [firstScanStarted], timeout: 1)
        coordinator.refresh(directory: directory, force: true) { result in
            XCTAssertEqual(try? result.get(), freshSummary)
            forcedCompletion.fulfill()
        }
        releaseFirstScan.signal()

        wait(for: [firstCompletion, forcedCompletion], timeout: 2)
        stateLock.lock()
        let finalScanCount = scanCount
        stateLock.unlock()
        XCTAssertEqual(finalScanCount, 2)
    }

    func test_uploadSummaryCache_roundTripsByDirectory() throws {
        let suiteName = "UploadSummaryCacheTests.\(UUID().uuidString)"
        let defaults = try XCTUnwrap(UserDefaults(suiteName: suiteName))
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let directoryID = UUID()
        let summary = UploadDirectorySummary(
            totalCount: 50_000,
            pendingCount: 123,
            uploadedCount: 49_877
        )

        UploadSummaryCache.store(
            summary,
            directoryID: directoryID,
            defaults: defaults
        )
        XCTAssertEqual(
            UploadSummaryCache.load(
                directoryID: directoryID,
                defaults: defaults
            ),
            summary
        )

        UploadSummaryCache.remove(
            directoryID: directoryID,
            defaults: defaults
        )
        XCTAssertNil(
            UploadSummaryCache.load(
                directoryID: directoryID,
                defaults: defaults
            )
        )
    }

    func test_sensorBagBackfillIndex_migratesVerifiedLegacyJSON() throws {
        let fm = FileManager.default
        let root = fm.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try fm.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: root) }

        let expected = [
            "continuous/100.bin": SensorBagBackfillRecord(
                fileSize: 123,
                lastModifiedAt: Date(timeIntervalSinceReferenceDate: 456),
                completedAt: Date(timeIntervalSinceReferenceDate: 789)
            ),
            "orthostatic/200.bin": SensorBagBackfillRecord(
                fileSize: 321,
                lastModifiedAt: Date(timeIntervalSinceReferenceDate: 654),
                completedAt: Date(timeIntervalSinceReferenceDate: 987)
            ),
        ]
        let legacyURL = SensorBagBackfillIndex.legacyURL(in: root)
        try JSONEncoder().encode(expected).write(to: legacyURL, options: .atomic)

        let migrated = try SensorBagBackfillIndex.records(in: root)

        XCTAssertEqual(migrated, expected)
        XCTAssertTrue(
            fm.fileExists(
                atPath: SensorBagBackfillIndex.databaseURL(in: root).path
            )
        )
        XCTAssertFalse(fm.fileExists(atPath: legacyURL.path))
        XCTAssertEqual(try SensorBagBackfillIndex.records(in: root), expected)
    }

    func test_sensorBagBackfillIndex_invalidLegacyJSONFailsWithoutDeletion() throws {
        let fm = FileManager.default
        let root = fm.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try fm.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: root) }

        let legacyURL = SensorBagBackfillIndex.legacyURL(in: root)
        let invalidData = Data("not-json".utf8)
        try invalidData.write(to: legacyURL, options: .atomic)

        XCTAssertThrowsError(try SensorBagBackfillIndex.records(in: root)) { error in
            XCTAssertTrue(error.localizedDescription.contains("decode legacy index"))
        }
        XCTAssertEqual(try Data(contentsOf: legacyURL), invalidData)
    }

    func test_sensorBagBackfillIndex_upsertAndResetAreTransactional() throws {
        let fm = FileManager.default
        let root = fm.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try fm.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: root) }

        let key = "continuous/recording.bin"
        try SensorBagBackfillIndex.markCompleted(
            key: key,
            fileSize: 10,
            lastModifiedAt: Date(timeIntervalSinceReferenceDate: 20),
            completedAt: Date(timeIntervalSinceReferenceDate: 30),
            in: root
        )
        let replacement = SensorBagBackfillRecord(
            fileSize: 40,
            lastModifiedAt: Date(timeIntervalSinceReferenceDate: 50),
            completedAt: Date(timeIntervalSinceReferenceDate: 60)
        )
        try SensorBagBackfillIndex.markCompleted(
            key: key,
            fileSize: replacement.fileSize,
            lastModifiedAt: replacement.lastModifiedAt,
            completedAt: replacement.completedAt,
            in: root
        )

        XCTAssertEqual(
            try SensorBagBackfillIndex.records(in: root),
            [key: replacement]
        )
        XCTAssertEqual(
            try SensorBagBackfillIndex.reset(in: root),
            SensorBagBackfillResetResult(
                removedRecords: 1,
                warningMessage: nil
            )
        )
        XCTAssertEqual(try SensorBagBackfillIndex.records(in: root), [:])
    }

    func test_sensorBagBackfillIndex_interruptedResetFinishesBeforeMigration() throws {
        let fm = FileManager.default
        let root = fm.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try fm.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: root) }

        try SensorBagBackfillIndex.markCompleted(
            key: "continuous/indexed.bin",
            fileSize: 1,
            lastModifiedAt: Date(timeIntervalSinceReferenceDate: 2),
            completedAt: Date(timeIntervalSinceReferenceDate: 3),
            in: root
        )
        let legacy = [
            "continuous/legacy.bin": SensorBagBackfillRecord(
                fileSize: 4,
                lastModifiedAt: Date(timeIntervalSinceReferenceDate: 5),
                completedAt: Date(timeIntervalSinceReferenceDate: 6)
            )
        ]
        let legacyURL = SensorBagBackfillIndex.legacyURL(in: root)
        try JSONEncoder().encode(legacy).write(to: legacyURL, options: .atomic)
        let markerURL = SensorBagBackfillIndex.resetMarkerURL(in: root)
        try Data().write(to: markerURL, options: .atomic)

        XCTAssertEqual(try SensorBagBackfillIndex.records(in: root), [:])
        XCTAssertFalse(fm.fileExists(atPath: legacyURL.path))
        XCTAssertFalse(fm.fileExists(atPath: markerURL.path))
    }

    func test_sensorBagBackfillIndex_resetRecoversCorruptSQLiteAndReportsIt() throws {
        let fm = FileManager.default
        let root = fm.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try fm.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: root) }

        let databaseURL = SensorBagBackfillIndex.databaseURL(in: root)
        try Data("not-sqlite".utf8).write(to: databaseURL, options: .atomic)

        let result = try SensorBagBackfillIndex.reset(in: root)

        XCTAssertEqual(result.removedRecords, 0)
        XCTAssertTrue(
            try XCTUnwrap(result.warningMessage).contains(
                "Unreadable SQLite state was intentionally cleared"
            )
        )
        XCTAssertEqual(try SensorBagBackfillIndex.records(in: root), [:])
    }

    func test_sensorBagBackfillIndex_resetReportsClearedInvalidLegacyJSON() throws {
        let fm = FileManager.default
        let root = fm.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try fm.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: root) }

        let legacyURL = SensorBagBackfillIndex.legacyURL(in: root)
        try Data("not-json".utf8).write(to: legacyURL, options: .atomic)

        let result = try SensorBagBackfillIndex.reset(in: root)

        XCTAssertEqual(result.removedRecords, 0)
        XCTAssertTrue(
            try XCTUnwrap(result.warningMessage).contains(
                "Unreadable legacy JSON was intentionally cleared"
            )
        )
        XCTAssertFalse(fm.fileExists(atPath: legacyURL.path))
        XCTAssertEqual(try SensorBagBackfillIndex.records(in: root), [:])
    }
}

private final class FailingRecordingNotificationCenter: RecordingNotificationScheduling {
    private(set) var attemptedIdentifiers: [String] = []

    func add(_ request: UNNotificationRequest) async throws {
        attemptedIdentifiers.append(request.identifier)
        throw TestError.failed
    }

    func removePendingNotificationRequests(withIdentifiers identifiers: [String]) {}

    func removeDeliveredNotifications(withIdentifiers identifiers: [String]) {}

    private enum TestError: LocalizedError {
        case failed

        var errorDescription: String? { "Deliberate test failure" }
    }
}
