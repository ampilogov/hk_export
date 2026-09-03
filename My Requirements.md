# My Requirements

Last updated: September 3, 2026

This document tracks improvements to continuous Polar H10 recording. Requirements are kept separate from implementation considerations so that each item can be discussed, implemented, and verified independently.

## Status legend

- **Proposed** — captured, but not yet ready to implement.
- **Ready** — sufficiently understood to start implementation.
- **In progress** — implementation has started.
- **Blocked** — waiting on a decision, dependency, or external information.
- **Done** — implemented and verified against its acceptance criteria.

## Requirements

### R1 — Measure and reduce battery drain

Determine how much battery continuous recording consumes on both the iPhone and Polar H10, identify the largest sources of energy use, and reduce them without compromising the selected recording quality.

**Status:** Proposed

### R2 — Prevent and diagnose crashes

Find the causes of crashes or unexpected app termination during long recordings. Preserve as much recorded data as possible and make failures diagnosable after they occur.

**Status:** Ready — recommended first item

### R3 — Make large recording collections and uploads efficient

The app must remain responsive with tens of thousands of existing recordings. Opening the Upload screen, determining pending work, and uploading recordings should not require long waits. Future recordings should produce substantially fewer files.

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

1. **R2 — Stability and diagnostics.** Fix known termination risks and establish evidence about future failures.
2. **R1 — Battery measurement and recording profiles.** Establish physical-device baselines, then optimize the largest costs.
3. **R3 — Storage and upload scalability.** Fix the existing backlog experience, then change the future file format and upload pipeline.
4. **R4 and R5 — Reconnection and notifications.** Implement these together around one recording state machine.
5. **R6 — ECG inspection.** Build the viewer after the storage format supports efficient time-range reads.
6. **R7 — Remaining improvements.** Reassess using the metrics gathered by the earlier work.

## Status tracker

| ID | Requirement | Status | Next milestone |
| --- | --- | --- | --- |
| R1 | Battery drain | Proposed | Run physical-device baseline sessions |
| R2 | Crash prevention and diagnostics | Ready | Remove unsafe background-task lifetime and add session diagnostics |
| R3 | File and upload efficiency | Proposed | Make Upload load cached counts asynchronously |
| R4 | Auto reconnect and resume | Proposed | Define and test the recording connection state machine |
| R5 | Notification behavior | Proposed | Implement reconnect grace and escalation rules |
| R6 | Interactive ECG | Proposed | Add time-range ECG decoding for existing recordings |
| R7 | Other inefficiencies | Proposed | Re-profile after R1–R3 |

## Considerations by requirement

### R1 considerations — Battery drain

Current observations:

- The Polar backend starts HR, ECG at maximum available settings, and ACC automatically.
- High-rate streams continue during configured recording “off” windows; their data is accumulated and later discarded rather than preventing the work.
- Sensor packets cause frequent main-thread state changes and Live Activity updates.
- Opening the HRV tab disables screen sleep even when recording does not require the display.

Proposed approach:

- Measure separate one-hour physical-device sessions for `RR only`, `RR + ECG`, and `RR + ECG + ACC`, with the screen off and on.
- Record iPhone battery percentage per hour, Polar battery percentage per hour, CPU, memory, disk writes, wakeups, packets, and bytes.
- Add explicit recording profiles. ACC should be opt-in. Historical ECG requires ECG to be captured continuously; an RR-only profile can only start ECG prospectively after the user requests it.
- Stop unnecessary Polar streams during off-windows instead of discarding their output.
- Limit visible status updates to roughly once per second and Live Activity updates to state changes or a much lower cadence.
- Allow the screen to sleep during continuous recording.

Acceptance criteria:

- Battery consumption is reported separately for every supported capture profile.
- Memory remains bounded throughout a long recording.
- No sensor stream runs when it is disabled by the active profile.
- Optimization results are compared against the recorded baseline before changing defaults.

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

Proposed approach:

- Immediate client-only improvement: display cached aggregate status immediately and scan asynchronously on a dedicated actor. Never perform file metadata reads from a SwiftUI computed property.
- Replace per-file `.done` records and the HealthKit JSON index with one transactional SQLite index.
- Store future continuous data as one logical session with crash-recoverable hourly segment files. This caps uninterrupted recording at approximately 24 recording files per day.
- Stream request bodies from disk and keep upload memory bounded.
- Preferred server-assisted improvement: upload size-limited bundles with a manifest, content hashes, idempotent acknowledgement, and resumable progress.
- Provide an explicit migration/compaction tool for the existing backlog. Never delete originals until the compacted output is verified and the configured retention rule permits deletion.

Acceptance criteria:

- The Upload screen presents useful cached information in under 0.5 seconds with 50,000 indexed recordings.
- UI rendering does not trigger repeated directory scans or metadata reads.
- Future uninterrupted capture creates no more than 24 normal recording segments per day.
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
| 2026-09-03 | All | Begin with R2 before larger feature work | Stability and diagnostic evidence reduce risk for every later change |

## Progress notes

Add dated implementation and verification notes here as work proceeds.

- **2026-09-03:** Initial requirements and code-review considerations captured. The existing project builds successfully for the iPhone 16 / iOS 18.1 simulator. No implementation changes have been made yet.
