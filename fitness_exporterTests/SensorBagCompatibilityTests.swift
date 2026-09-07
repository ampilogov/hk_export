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
            XCTAssertNotNil(UploadHelper.markDone(file: file, base: base))
        }

        let initial = try UploadHelper.inventory(in: base)
        XCTAssertEqual(initial.totalCount, fileCount)
        XCTAssertEqual(initial.uploadedCount, uploadedCount)
        XCTAssertEqual(initial.pendingCount, fileCount - uploadedCount)

        try Data([1, 2]).write(to: files[0], options: .atomic)
        let afterMutation = try UploadHelper.inventory(in: base)
        XCTAssertEqual(afterMutation.uploadedCount, uploadedCount - 1)
        XCTAssertEqual(afterMutation.pendingCount, fileCount - uploadedCount + 1)
    }

    func test_uploadCoordinator_coalescesMatchingJobsAndSerializesDifferentJobs() {
        let coordinator = UploadSingleFlightCoordinator()
        let allCompletions = expectation(description: "all callers completed")
        allCompletions.expectedFulfillmentCount = 3
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
            key: "different",
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
