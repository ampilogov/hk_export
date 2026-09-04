# My Requirements

Last updated: September 3, 2026

This document tracks improvements to continuous Polar H10 recording. Requirements are kept separate from implementation considerations so that each item can be discussed, implemented, and verified independently.

## Status legend

- **Proposed** — captured, but not yet ready to implement.
- **Ready** — sufficiently understood to start implementation.
- **In progress** — investigation or implementation has started.
- **Blocked** — waiting on a decision, dependency, or external information.
- **Done** — implemented and verified against its acceptance criteria.

## Requirements

### R1 — Measure and reduce battery drain

Determine how much battery continuous recording consumes on both the iPhone and Polar H10, identify the largest sources of energy use, and reduce them without compromising the selected recording quality. Evaluate the real usage pattern: recording remains active for approximately 24 hours with the screen normally off, and the user disconnects the H10 after stopping. Once-daily morning HRV measurements did not exhibit the battery problem.

**Status:** In progress — static energy audit complete; physical-device measurements pending

### R2 — Prevent and diagnose crashes

Find the causes of crashes or unexpected app termination during long recordings. Preserve as much recorded data as possible and make failures diagnosable after they occur.

**Status:** Ready — its recording-lifecycle fixes overlap with active R1 work

### R3 — Make large recording collections and uploads efficient

The app must remain responsive with tens of thousands of existing recordings. Opening the Upload screen, determining pending work, and uploading recordings should not require long waits. While continuous recording is active, each finalized package should be durably queued and sent immediately over either Wi-Fi or cellular; it must not wait for the user to stop recording or for an opportunistic background task. Future recordings should produce substantially fewer permanent local files.

**Status:** Proposed

### R4 — Automatically reconnect and resume recording

When the Polar H10 disconnects unexpectedly, the app should reconnect automatically and resume recording without requiring manual intervention. The resulting data should clearly represent any gap.

**Status:** Proposed

### R5 — Make notifications actionable without causing noise

Short, self-healing interruptions should not immediately notify the user. Persistent interruptions, exhausted reconnection attempts, recording failures, and unexpected app termination should reliably request attention.

**Status:** Proposed

### R6 — Add fast, interactive ECG access

Make ECG data easy to inspect without continuously rendering a large graph or creating significant additional battery use. Prefer an RR overview where selecting a timestamp opens a short ECG window around that time.

**Status:** Proposed

### R7 — Address other continuous-recording inefficiencies

Review memory use, disk writes, HealthKit imports, background execution, UI updates, and data durability for any additional problems that become important during long recordings.

**Status:** Proposed

## Recommended implementation order

1. **R1 and R2 — Energy, stream lifecycle, and stability.** Stop work that continues outside the requested recording lifecycle, remove known termination risks, and establish physical-device evidence.
2. **R3 — Storage and upload scalability.** Fix the existing backlog experience, then change the future file format and upload pipeline.
3. **R4 and R5 — Reconnection and notifications.** Implement these together around one recording state machine.
4. **R6 — ECG inspection.** Build the viewer after the storage format supports efficient time-range reads.
5. **R7 — Remaining improvements.** Reassess using the metrics gathered by the earlier work.

## Status tracker

| ID | Requirement | Status | Next milestone |
| --- | --- | --- | --- |
| R1 | Battery drain | In progress | Review audit, fix the recording lifecycle, and run physical-device A/B sessions |
| R2 | Crash prevention and diagnostics | Ready | Remove unsafe background-task lifetime and add session diagnostics |
| R3 | File and upload efficiency | Proposed | Design one durable upload coordinator and background transfer queue |
| R4 | Auto reconnect and resume | Proposed | Define and test the recording connection state machine |
| R5 | Notification behavior | Proposed | Implement reconnect grace and escalation rules |
| R6 | Interactive ECG | Proposed | Add time-range ECG decoding for existing recordings |
| R7 | Other inefficiencies | Proposed | Re-profile after R1–R3 |

## Considerations by requirement

### R1 considerations — Battery drain

Current observations:

- **Observed usage:** continuous recording normally remains active for approximately 24 hours, the screen is normally off, and the H10 is explicitly disconnected after Stop. The user does not leave the device connected while idle. Therefore, post-recording streaming and a lit display are correctness concerns but are unlikely explanations for the reported battery drain.
- The earlier once-daily morning HRV workflow did not cause noticeable battery drain. The relevant difference is sustained 24-hour acquisition plus the repeated persistence, HealthKit, indexing, and upload work that accompanies it.
- **Lower priority for this usage, but still a defect:** connecting the device starts HR, ECG, and ACC immediately, before the user starts a recording. Stopping a recording does not itself dispose the Polar SDK's raw-stream subscriptions, although the user's explicit disconnect does terminate the session.
- The H10 ECG rate is fixed at 130 Hz, so the call to `maxSettings()` is not raising it beyond the device's supported rate. ACC is already selected at its minimum supported sample rate, 25 Hz. The avoidable issue is when these streams run, not an unusually high configured H10 rate.
- **Lower priority for this usage, but still a defect:** opening the HRV tab disables automatic screen sleep. Because the screen is normally turned off and the app is seldom reopened during a recording, this is unlikely to explain the reported background drain.
- Every HR, ECG, and ACC packet is delivered and mapped on the main thread. Each packet also publishes UI state and starts a new asynchronous Live Activity update, producing several tasks per second throughout a long connection.
- With the actual five-minute duration and five-minute interval, the implementation computes `max(0, interval - duration)`, so the off-window is zero seconds and the next write window starts immediately. This is nonstop recording, as intended.
- A UIKit background task is held for the complete recording with no expiration handler. This is not a supported way to keep an iOS app alive indefinitely and can contribute to system termination.
- The raw compact binary payload is probably not the principal sustained cost. The expensive lifecycle around every file includes serialization, rereading, decoding, hashing, HealthKit authorization and writes, and rereading/re-encoding the complete HealthKit JSON index.

The continuous controls reuse duration choices intended for short tests. When duration and interval are equal, the current timer creates this many files and independent HealthKit import jobs:

| Duration and interval | Files/import jobs per day |
| --- | ---: |
| 0 seconds | Unbounded hot timer loop; saves may fail silently |
| 2 seconds | 43,200 |
| 5 seconds | 17,280 |
| 10 seconds | 8,640 |
| 30 seconds | 2,880 |
| 5 minutes (current default) | 288 |

The configured five-minute cadence is reasonable as a durability or upload checkpoint. It is inefficient in the current architecture because it creates 288 permanent files and 288 independent HealthKit jobs per day—approximately 8,640 of each in a 30-day month. Imports have no single in-flight gate, and completion of each one reloads and rewrites the full HealthKit backfill index. With a large collection, this becomes effectively quadratic cumulative work. It is a strong candidate for battery drain, hangs, and memory or watchdog terminations. The zero-second choice remains an unrelated edge-case defect because it can create a tight timer loop and save errors are ignored.

Current suspected-drain ranking:

1. Per-packet main-thread mapping/publication and unthrottled Live Activity updates sustained for 24 hours.
2. The five-minute file lifecycle: 288 daily serialization/read/decode/hash/HealthKit jobs plus full-index rewrites.
3. Upload scan, retry, and logging storms when a large backlog exists or the server is unreachable.
4. Expected H10 cost of continuous 130 Hz ECG and 25 Hz ACC acquisition; measure this separately from avoidable app overhead.
5. The session-long UIKit background assertion and competing background work.
6. Post-Stop streaming and forced screen wakefulness are lower-priority explanations under the documented usage pattern.

Proposed approach:

- Make Polar stream subscriptions recording- and profile-scoped. Start only the requested streams, retain a separately disposable handle for each stream, and explicitly stop them before disconnect or recording completion.
- Use one authoritative Bluetooth/Polar connection owner. The current outer CoreBluetooth central connects first and then the Polar SDK creates its own central and connection, producing redundant ownership and conflicting connection state.
- Remove the session-long UIKit background assertion. Use Bluetooth background delivery plus short, expiration-safe assertions only while finalizing durable data.
- Separate the five-minute durability/upload checkpoint from permanent file rotation. Append through one serialized writer, make each five-minute checkpoint eligible for immediate transfer, and rotate permanent local storage into hourly segments rather than creating one independent HealthKit/import transaction per checkpoint.
- Move serialization and indexing off the main thread. Serialize HealthKit work through a bounded queue and update one transactional index in batches.
- Add explicit recording profiles. ACC should be opt-in. Historical ECG requires ECG to be captured continuously; an RR-only profile can only start ECG prospectively after the user requests it.
- Limit visible status updates to roughly once per second and Live Activity updates to state changes or approximately every 15–30 seconds.
- Allow the screen to sleep during continuous recording.
- Measure the actual 24-hour, screen-off workflow first, then isolate `recording only`, `recording + HealthKit`, and `recording + immediate upload` variants. Compare `RR only`, `RR + ECG`, and `RR + ECG + ACC` separately, and use long H10 runs because its reported battery percentage is coarse.
- Record iPhone energy impact, CPU, memory, disk writes, wakeups, packets, bytes, file finalization latency, HealthKit queue depth, and upload activity. Record H10 battery separately.

Measurement status:

- Static code and dependency audit completed on September 3, 2026.
- A physical Power Profiler trace has not been captured because the paired iPhone is currently offline. No precise battery-per-hour claim should be made until the same-device A/B sessions are collected.
- The project currently pins Polar BLE SDK 6.4.0. Later 6.x and 8.x releases contain crash and streaming-lifecycle changes, but an upgrade should be evaluated as a controlled soak-test experiment rather than assumed to fix energy use.

Acceptance criteria:

- Battery consumption is reported separately for every supported capture profile.
- Memory remains bounded throughout a long recording.
- No sensor stream runs before recording, after Stop, or while it is disabled by the active profile.
- Selecting zero or an unsafe short continuous window cannot create a hot loop or thousands of file/import transactions.
- Optimization results are compared against the recorded baseline before changing defaults.

Evidence references:

- [Polar H10 streaming capabilities](https://github.com/polarofficial/polar-ble-sdk/blob/master/documentation/products/PolarH10.md)
- [Polar SDK known issues, including explicit H10 stream termination](https://github.com/polarofficial/polar-ble-sdk/blob/master/documentation/KnownIssues.md)
- [Apple energy-efficiency fundamentals](https://developer.apple.com/library/archive/documentation/Performance/Conceptual/EnergyGuide-iOS/FundamentalConcepts.html)
- [Apple guidance for finite background tasks](https://developer.apple.com/documentation/uikit/uiapplication/beginbackgroundtask%28expirationhandler%3A%29?language=objc)

### R2 considerations — Crashes and unexpected termination

Current observations:

- A UIKit background task is opened for the entire recording and has no expiration handler. Background tasks are finite; failing to end one before expiration can cause iOS to terminate the app.
- Current recording data lives in memory until a complete window is serialized, so a crash can lose the active window.
- Some HealthKit export paths use `fatalError` for unexpected data, and the RR graph assumes ordered timestamps and non-zero plot ranges.
- Large directory scans and HealthKit index work can run from the main thread, creating watchdog risk.
- There is no test target or integrated crash, hang, memory, and disk-write diagnostic capture.

Proposed approach:

- Remove the session-long background task. Use short, balanced, expiration-safe tasks only around critical file finalization.
- Add an append-only, crash-recoverable writer with a small bounded flush interval.
- Persist a lightweight active-session journal containing the session ID, selected device and streams, start time, last sensor packet, and last successful disk write.
- Add MetricKit reporting and preserve symbolicated build archives. Detect an unfinished journal on launch and report that the previous recording ended unexpectedly.
- Replace production assertions and `fatalError` calls on asynchronous data paths with logged, recoverable errors.
- Harden plotting against duplicate timestamps, empty data, and constant ranges.
- Add unit tests and interruption tests before making larger recording changes.

Acceptance criteria:

- No background task remains open for the lifetime of a recording.
- Force-terminating the app leaves a readable file with only a small, explicitly bounded tail at risk.
- The next launch identifies an interrupted recording and preserves relevant diagnostic context.
- Crash, hang, watchdog, and memory-termination reports can be distinguished.
- Recording and graph edge cases are covered by automated tests.

### R3 considerations — Files and uploads

Current observations:

- Each recording window creates a separate `.bin` file.
- Each uploaded file creates a separate `.done` JSON sidecar, further increasing the number of filesystem entries.
- The Upload screen enumerates and sorts the directory synchronously, reads every sidecar, and repeatedly checks file metadata while SwiftUI renders.
- Uploads are serial, and each file is fully loaded, wrapped in a property list, and gzip-compressed in memory.
- HealthKit backfill reloads and rewrites its complete JSON index after each processed recording, which scales especially poorly for a large backlog.
- Continuous recording does not currently upload a segment when it is finalized. Upload is triggered only after the user stops recording or later by opportunistic background jobs, which iOS may delay substantially.
- The core uploader starts security-scoped directory access and then releases it as soon as the asynchronous upload is launched. Later file reads and completion-marker writes may therefore lose access. The Upload screen can accidentally mask this bug while it remains visible because the view holds a second access lease.
- There is no global upload coordinator. Manual upload, recording-stop upload, app refresh, and background processing can concurrently scan and upload the same pending files.
- Background mode suppresses individual failures and can report overall success after every file failed. An unreachable server can perform four attempts per file without a collection-level circuit breaker.
- Consecutive unavailable/read-failed files are skipped through synchronous recursion. With tens of thousands of files this creates a stack-overflow crash risk.
- Upload logging magnifies backlog work: each file emits multiple persistent logs, and each log rebuilds and rewrites up to 400 records in `UserDefaults`.
- Upload uses a default foreground `URLSession`, so the operating system does not own continuation of a transfer after suspension or termination.

Proposed approach:

- Immediate client-only improvement: display cached aggregate status immediately and scan asynchronously on a dedicated actor. Never perform file metadata reads from a SwiftUI computed property.
- Replace per-file `.done` records and the HealthKit JSON index with one transactional SQLite index.
- Store future continuous data as one logical session with crash-recoverable hourly local segment files. Preserve a five-minute durability/upload checkpoint without requiring a permanent standalone local file for every checkpoint.
- Introduce one upload actor/coordinator with a durable pending queue, bounded concurrency, and deduplication across UI, recording, and background triggers.
- Mark every finalized five-minute package pending immediately and begin its transfer while recording continues. Permit both Wi-Fi and cellular, including expensive-network access; do not wait for recording Stop.
- Hold security-scoped access for the entire asynchronous operation and test access loss explicitly.
- Use a background `URLSession` and file-backed request bodies so the system can continue eligible transfers while the app is suspended and upload memory remains bounded.
- Add a collection-level circuit breaker and retry budget. Immediate sending over cellular is required, but a confirmed server/network outage must pause and reschedule the batch instead of retrying every pending file continuously.
- Replace recursive file traversal with iteration and aggregate repetitive logs into periodic progress summaries.
- Preferred server-assisted improvement: upload size-limited bundles with a manifest, content hashes, idempotent acknowledgement, and resumable progress.
- Provide an explicit migration/compaction tool for the existing backlog. Never delete originals until the compacted output is verified and the configured retention rule permits deletion.

Acceptance criteria:

- The Upload screen presents useful cached information in under 0.5 seconds with 50,000 indexed recordings.
- UI rendering does not trigger repeated directory scans or metadata reads.
- Future uninterrupted capture creates no more than 24 permanent local recording segments per day while retaining five-minute durability/upload checkpoints.
- Every finalized five-minute package is immediately represented in the durable pending queue and starts an upload attempt without ending the recording.
- Upload is allowed over both Wi-Fi and cellular.
- A collection-wide network failure performs a bounded number of attempts and cannot generate one retry storm per pending file.
- Background completion reports failure when the requested work did not actually upload.
- Upload progress survives relaunch and retries without duplicating accepted content.
- Upload memory use does not scale with the total backlog.

Dependencies and decisions:

- Bundled or resumable upload may require a compatible server endpoint. The asynchronous UI, single index, hourly files, and bounded-memory client can be completed without that server change.
- Retention policy must remain conservative by default: keep source data unless deletion has been explicitly enabled.

### R4 considerations — Auto reconnect and recording resume

Current observations:

- There is no explicit reconnect loop after an unexpected disconnect.
- The Polar SDK enables automatic reconnection by default, but the current disconnect callback explicitly disconnects the SDK session, likely cancelling that behavior.
- CoreBluetooth and Polar SDK paths can both emit disconnect events and do not share one authoritative readiness state.
- The UI can consider the device connected before RR, ECG, and ACC streams are actually ready.

Proposed approach:

- Introduce a state machine such as `idle → connecting → recording → degraded → reconnecting → recording/attentionNeeded`.
- Track user intent separately from connection state. An unexpected disconnect must not clear the intent to keep recording; an explicit Stop must.
- On loss, safely finalize the current segment, record a structured gap, and retry immediately with bounded exponential backoff.
- Use one authoritative Polar connection path and deduplicate disconnect events.
- Resume automatically only after all streams required by the selected profile have produced readiness or data signals.
- Preserve enough session intent to recover after a system relaunch and adopt supported Bluetooth state preservation/restoration behavior.

Acceptance criteria:

- A brief disconnect reconnects and resumes without user interaction.
- Reconnection creates a new recoverable segment under the same logical session and records the gap duration.
- Intentional Stop never triggers reconnection.
- Duplicate callbacks cannot create duplicate sessions, alerts, or writers.
- Reconnection behavior is covered with deterministic simulated-device tests.

### R5 considerations — Notifications

Current observations:

- A disconnect notification is sent immediately.
- Stream-stale checks can send several notifications together and then invalidate their own timer, preventing continued monitoring.
- The app cannot create a notification after its process has already crashed. Crash awareness therefore needs either a notification scheduled in advance or an external server watchdog.

Proposed behavior:

- **0–10 seconds:** reconnect silently; show status only in the app and Live Activity.
- **At 10 seconds:** send one “Reconnecting” notification if the interruption is still active.
- **At 60 seconds or after repeated failed attempts:** send one audible “Recording needs attention” notification.
- Cancel pending disconnect notifications immediately after recovery and avoid a noisy “recovered” notification for very short gaps.
- Monitor only streams enabled in the current recording profile and use stable notification identifiers for replacement and cancellation.
- Maintain a low-frequency local watchdog notification scheduled in advance and refreshed while recording is healthy. If the process dies or data stops, the already-scheduled notification can still fire.
- Consider a server-side missing-heartbeat alert later if immediate failure notification must work across app termination, phone failure, or loss of local execution.

Acceptance criteria:

- A disconnect shorter than 10 seconds produces no notification.
- A persistent interruption produces one notification at each configured escalation level, not one per stream or callback.
- Recovery cancels all obsolete pending alerts.
- A simulated dead process results in the pre-scheduled watchdog notification.
- Explicitly stopping a recording cancels the watchdog and all reconnect alerts.

### R6 considerations — Interactive ECG

Current observations:

- The existing UI plots only RR intervals.
- Existing recording files contain ECG voltages, but the current HealthKit-oriented decoder deliberately skips ECG and ACC payloads.
- Publishing and rendering the full ECG continuously would waste CPU and memory without improving the normal recording screen.

Proposed approach:

- Keep RR as the lightweight overview.
- Make an RR point or timestamp selectable and open an ECG detail centered on approximately five seconds before and after it.
- Decode only the requested time range on a background queue.
- Draw one fixed-size viewport using `Canvas`, with min/max envelope downsampling, drag-to-pan, and optional pinch zoom. Do not create a SwiftUI view per ECG sample.
- Maintain only a small live ECG ring buffer while the detail view is visible.
- Add a range reader compatible with existing version-1 files.
- Include explicit device-clock-to-wall-clock alignment and byte-range indexing in the next recording format so selections remain accurate across segments and reconnects.

Acceptance criteria:

- Selecting an RR timestamp opens its ten-second ECG neighborhood quickly without loading the complete recording.
- Panning and zooming remain responsive on long sessions.
- Keeping the ECG detail closed adds no continuous rendering workload.
- Existing recordings remain readable.

### R7 considerations — Other continuous-recording inefficiencies

Areas to include in ongoing review:

- Serialize sensor ownership and mutable recording state to remove race conditions.
- Keep all queues and buffers bounded and expose high-water marks in diagnostics.
- Batch HealthKit authorization and imports rather than initializing the complete flow for every small file.
- Make background jobs cancellable and report expiration accurately instead of allowing late callbacks to mark expired work successful.
- Record structured session telemetry: packet counts, gaps, reconnect attempts, write latency, write failures, pending uploads, upload throughput, and battery samples.
- Avoid logging or persisting high-frequency per-packet details unless diagnostic mode is explicitly enabled.
- Exercise 12-hour and 24-hour physical-device soak tests after each major recording-engine change.

Acceptance criteria:

- A 24-hour soak test shows stable memory and bounded queues.
- No synchronous disk or network work blocks the main thread.
- Background expiration, disk-full, corrupt-tail, permission-loss, and network-loss cases fail safely without silently losing the whole session.

## Decision log

Record decisions here as requirements are refined.

| Date | Requirement | Decision | Reason |
| --- | --- | --- | --- |
| 2026-09-03 | All | Begin with R2 before larger feature work (superseded later the same day) | Stability and diagnostic evidence reduce risk for every later change |
| 2026-09-03 | R1 | Make R1 the first active investigation and include upload-as-you-go energy behavior | Current long recordings indicate that energy and lifecycle work need immediate evidence and correction |
| 2026-09-03 | R1 | Evaluate battery drain against a 24-hour, normally screen-off recording that ends with an explicit disconnect | Post-Stop streaming and foreground display use do not match the user's actual usage and should not dominate the diagnosis |
| 2026-09-03 | R3 | Immediately send each finalized five-minute continuous package over Wi-Fi or cellular | Prompt server delivery is preferred over waiting for recording completion, opportunistic scheduling, or Wi-Fi |

## Progress notes

Add dated implementation and verification notes here as work proceeds.

- **2026-09-03:** Initial requirements and code-review considerations captured. The existing project builds successfully for the iPhone 16 / iOS 18.1 simulator. No implementation changes have been made yet.
- **2026-09-03:** Initial R1 static audit completed. It found Polar streams that outlive recording Stop, forced screen wakefulness, per-packet Live Activity updates, unsafe short/zero recording windows, unbounded per-file HealthKit work, and upload retry/logging storms. The later usage clarification below de-prioritizes the first two as explanations for the reported drain. Upload-as-you-go is not currently implemented, and the existing uploader has security-scope, coordination, recursion, and error-reporting defects. Physical-device energy traces remain pending; production behavior has not yet been changed.
- **2026-09-03:** Usage assumptions corrected. The real case is a roughly 24-hour continuous recording with a normally off display and an explicit disconnect after Stop; once-daily morning HRV did not show the drain. Five-minute duration plus five-minute interval is confirmed to be nonstop capture with no off-window. The five-minute cadence is acceptable, but its current per-file HealthKit/index lifecycle remains a likely inefficiency. R3 now requires immediate upload of each completed five-minute package over either Wi-Fi or cellular.
