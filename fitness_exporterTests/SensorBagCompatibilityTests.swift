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
}
