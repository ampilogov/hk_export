# My Requirements

Last updated: September 6, 2026

This document tracks improvements to continuous Polar H10 recording. Requirements are kept separate from implementation considerations so that each item can be discussed, implemented, and verified independently.

Completed items are removed from this active tracker instead of accumulating under a Done status. Git history remains the record of completed work and past implementation notes.

## Status legend

- **Proposed** — captured, but not yet ready to implement.
- **Ready** — sufficiently understood to start implementation.
- **In progress** — investigation or implementation has started.
- **Blocked** — waiting on a decision, dependency, or external information.

## Global data-safety and error policy

- Preserving source recordings takes precedence over availability or apparent progress.
- An unhandled error must stop the affected persistence, HealthKit, export, or upload pipeline. It must not advance a cursor, mark data complete, delete a source file, or silently skip the failed unit.
- Show the exact failed operation, data type/file or time range, and underlying error to the user. Record the same context in the application log.
- A pipeline failure should be explicit and retryable rather than an intentional process crash. Raw recording may continue when its data is already durable and only a downstream HealthKit/upload stage failed; the failed downstream stage must remain pending.
- Automatic retries must be bounded and visible. Never convert failure into success merely because all remaining items were traversed.
- The SensorBag v1 binary bytes, visible `.bin` filename scheme, and server-facing upload/export contract are compatibility requirements. Internal durability work may use temporary files, but the final filename, payload metadata, endpoint-visible type/version, and source bytes must remain unchanged unless a separate server migration is explicitly approved.

## Platform compatibility

- Only the latest iOS release needs to be supported. New work may use current iOS APIs directly and does not need compatibility branches or fallback behavior for older iOS versions.

## Requirements

### R2 — Prevent and diagnose crashes

Find the causes of crashes or unexpected app termination during long recordings. Preserve as much recorded data as possible and make failures diagnosable after they occur.

**Status:** In progress — silent termination after roughly 12 hours of continuous recording remains unresolved

### R3 — Make large recording collections and uploads efficient

The app must remain responsive with tens of thousands of existing recordings. Opening the Upload screen, determining pending work, and uploading recordings should not require long waits. While continuous recording is active, each finalized package should be durably queued and sent immediately over either Wi-Fi or cellular; it must not wait for the user to stop recording or for an opportunistic background task. Future recordings should produce substantially fewer permanent local files.

**Status:** Proposed

### R4 — Automatically reconnect and resume recording

Use the Polar SDK with one repeatable connection and stream lifecycle. Repeated Stop/Start, disconnect/reconnect, and unrelated upload activity must not leave HR/RR, ECG, or ACC partially stopped. When the Polar H10 disconnects unexpectedly, the app should reconnect automatically and resume every required stream without manual intervention. The resulting data should clearly represent any unavoidable gap.

**Status:** Proposed

### R5 — Make notifications actionable without causing noise

Short, self-healing interruptions should not immediately notify the user. Persistent interruptions, exhausted reconnection attempts, recording failures, and unexpected app termination should reliably request attention.

**Status:** Ready — implement an independent recording watchdog alongside the long-duration crash investigation

### R6 — Add fast, interactive ECG access

Make ECG data easy to inspect without continuously rendering a large graph or creating significant additional battery use. Prefer an RR overview where selecting a timestamp opens a short ECG window around that time.

**Status:** Proposed

### R7 — Address other continuous-recording inefficiencies

Review memory use, disk writes, HealthKit imports, background execution, UI updates, and data durability for any additional problems that become important during long recordings.

**Status:** Proposed

### R8 — Protect cursor and backfill reset controls

Prevent accidental cursor or HealthKit-backfill resets from launching unexpectedly large re-exports or duplicate work. Reset actions must explain their consequences, require confirmation, and refuse to race an active recording, export, or backfill operation.

**Status:** Proposed

### R9 — Keep the rest of the app usable during continuous recording

Allow navigation to other tabs while continuous recording continues. Keep recording state and controls visible, while disabling only operations that genuinely conflict with active recording.

**Status:** Proposed

## Recommended implementation order

1. **R2 and R5 — Long-run stability and independent failure notification.** Diagnose the reported approximately 12-hour termination and add a low-frequency dead-man alert that survives the app process.
2. **R3 — Storage and upload scalability.** Fix the existing backlog experience, then compact local storage without changing server-visible files or payloads.
3. **R4 — Reconnection and recording resume.** Build this around the same health state and escalation rules used by R5.
4. **R6 — ECG inspection.** Build the viewer after the storage format supports efficient time-range reads.
5. **R8 — Reset safety.** Protect destructive cursor and backfill reset controls before making the rest of the UI available during recording.
6. **R9 — Recording-time UI usability.** Remove the global tab lock after recording state and conflicting actions are safely app-scoped.
7. **R7 — Remaining improvements.** Reassess after the earlier work.

## Status tracker

| ID | Requirement | Status | Next milestone |
| --- | --- | --- | --- |
| R2 | Crash prevention and diagnostics | In progress | Add post-termination diagnostics and reproduce or classify the failure with a 12-hour soak |
| R3 | File and upload efficiency | Proposed | Design one durable upload coordinator and background transfer queue |
| R4 | Auto reconnect and resume | Proposed | Define and test the recording connection state machine |
| R5 | Notification behavior | Ready | Add a rolling local dead-man notification and stale Live Activity state; verify delivery on the paired Apple Watch |
| R6 | Interactive ECG | Proposed | Add time-range ECG decoding for existing recordings |
| R7 | Other inefficiencies | Proposed | Reassess after R2–R3 |
| R8 | Reset-button safety | Proposed | Define confirmation, busy-state, and error-result behavior for both reset controls |
| R9 | UI during active recording | Proposed | Move recording status to app scope and inventory actions that must remain disabled |

## Considerations by requirement

### R2 considerations — Crashes and unexpected termination

Current observations:

- The app reportedly terminates silently after roughly 12 hours of continuous recording. The user normally discovers this only after unlocking the phone. No crash, Jetsam, watchdog, or hang artifact has yet identified the cause.
- The recording invariant is that every `SensorEvent` delivered by the SDK is written exactly once, in per-stream order, to exactly one adjacent v1 file. The current format cannot recover a packet that never reached the app, and a process crash can still lose the unfinished in-memory window of up to the configured recording duration.
- A rotation swaps bags synchronously but saves the old bag asynchronously. Completion from an earlier recording must never update the session marker or watchdog for a later recording.
- Current final-file writes are not atomic, so process termination during a write can leave a truncated file with a normal `.bin` name. A save failure is logged but currently releases the rotated bag.
- Some HealthKit export paths use `fatalError` for unexpected data, and the RR graph assumes ordered timestamps and non-zero plot ranges. These remain possible crash paths to harden if termination evidence implicates them.
- Large directory scans and HealthKit index work can run from the main thread, creating watchdog risk.
- There is no test target or integrated crash, hang, or memory-termination reporting.

Proposed approach:

- Defer unfinished-window crash protection until the termination cause is understood. If implemented, prefer one active staging file rather than more frequent permanent file rotation.
- Add MetricKit reporting and preserve symbolicated build archives so crash, watchdog, hang, and memory-termination evidence survives the process.
- Keep MetricKit artifacts bounded and outside the recording directory. Do not restore packet counters, battery sampling, or a continuously rewritten diagnostic report.
- Use only a tiny active-session marker outside SensorBag to tell the next launch that recording ended unexpectedly. Associate all asynchronous completions with the recording ID.
- Preserve the exact v1 bytes and final filename while using atomic final-file replacement. Advance durable state only after the final write succeeds. Retain and visibly report a failed rotated bag rather than silently discarding it.
- Add byte-for-byte v1 compatibility tests and concurrent rotation tests that prove SDK-delivered packets are neither dropped nor duplicated across boundaries.
- Run a 12-hour physical-device soak first, followed by a 24-hour confirmation after fixing the identified cause. Classify the event as a crash, watchdog termination, Jetsam/memory termination, or recording stall before treating it as resolved.
- Harden plotting against duplicate timestamps, empty data, and constant ranges.
- Add unit tests and interruption tests before making larger recording changes.

Acceptance criteria:

- Previously completed files remain readable after force termination; only the unfinished current window remains at risk, bounded by the configured recording duration.
- The binary bytes, final filename, and server-visible upload/export contract remain unchanged.
- Any reduction in the crash-loss window must not materially increase battery drain or create an iCloud/file-synchronization storm.
- The next launch identifies an interrupted recording and preserves relevant diagnostic context.
- Crash, hang, watchdog, and memory-termination reports can be distinguished.
- A 12-hour reproduction or 24-hour confirmation run cannot silently stop without leaving both a durable diagnostic trail and a user-facing alert.
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
- Continue producing the same five-minute `.bin` upload artifacts with the same filename scheme and bytes. Reduce local top-level file count only through reversible local indexing or post-upload archival that can reproduce every original file exactly.
- Introduce one upload actor/coordinator with a durable pending queue, bounded concurrency, and deduplication across UI, recording, and background triggers.
- Mark every finalized five-minute package pending immediately and begin its transfer while recording continues. Permit both Wi-Fi and cellular, including expensive-network access; do not wait for recording Stop.
- Hold security-scoped access for the entire asynchronous operation and test access loss explicitly.
- Use a background `URLSession` and file-backed request bodies so the system can continue eligible transfers while the app is suspended and upload memory remains bounded.
- Add a collection-level circuit breaker and retry budget. Immediate sending over cellular is required, but a confirmed server/network outage must pause and reschedule the batch instead of retrying every pending file continuously.
- Replace recursive file traversal with iteration and aggregate repetitive logs into periodic progress summaries.
- Any future bundled or resumable transport must remain outside scope unless the server migration and compatibility contract are explicitly approved.
- Provide an explicit migration/compaction tool for the existing backlog. Never delete originals until the compacted output is verified and the configured retention rule permits deletion.

Acceptance criteria:

- The Upload screen presents useful cached information in under 0.5 seconds with 50,000 indexed recordings.
- UI rendering does not trigger repeated directory scans or metadata reads.
- Local compaction can reproduce every original five-minute filename and byte sequence exactly; until compaction is verified and reversible, the original files remain untouched.
- Every finalized five-minute package is immediately represented in the durable pending queue and starts an upload attempt without ending the recording.
- Upload is allowed over both Wi-Fi and cellular.
- A collection-wide network failure performs a bounded number of attempts and cannot generate one retry storm per pending file.
- Background completion reports failure when the requested work did not actually upload.
- Upload progress survives relaunch and retries without duplicating accepted content.
- Upload memory use does not scale with the total backlog.

Dependencies and decisions:

- Server protocol changes are not part of the current plan. Asynchronous UI, a single local index, bounded-memory uploads, and reversible local archival can be completed while preserving the existing server contract.
- Retention policy must remain conservative by default: keep source data unless deletion has been explicitly enabled.

### R4 considerations — Auto reconnect and recording resume

Current observations:

- There is no explicit reconnect loop after an unexpected disconnect.
- The Polar SDK enables automatic reconnection by default, but the current disconnect callback explicitly disconnects the SDK session, likely cancelling that behavior.
- CoreBluetooth and Polar SDK paths can both emit disconnect events and do not share one authoritative readiness state.
- The UI can consider the device connected before RR, ECG, and ACC streams are actually ready.
- The app first connects through its own `CBCentralManager`, then creates a separate Polar SDK API and asks the SDK to connect to the same identifier. This gives connection ownership to two independent Bluetooth stacks.
- Polar stream-start flags are set before streaming succeeds and are not cleared when ECG, ACC, or HR emits an error or completion. That backend instance therefore cannot restart a failed stream.
- The Polar disconnect callback calls the backend's explicit `disconnect()`, completing its event publishers and asking the SDK to disconnect even though SDK automatic reconnection is enabled by default.
- Stop/Start of continuous recording only detaches and reattaches the recorder subscription; it does not verify that all three underlying SDK streams are still healthy.

Proposed approach:

- Introduce a state machine such as `idle → connecting → recording → degraded → reconnecting → recording/attentionNeeded`.
- Track user intent separately from connection state. An unexpected disconnect must not clear the intent to keep recording; an explicit Stop must.
- On loss, safely finalize the current segment, record a structured gap, and retry immediately with bounded exponential backoff.
- Use one authoritative Polar connection path and deduplicate disconnect events.
- Give each connection generation fresh, individually disposable stream subscriptions. Clear stream state on error, completion, and disconnect; ignore callbacks belonging to an obsolete generation.
- Resume automatically only after all streams required by the selected profile have produced readiness or data signals.
- Preserve enough session intent to recover after a system relaunch and adopt supported Bluetooth state preservation/restoration behavior.

Acceptance criteria:

- A brief disconnect reconnects and resumes without user interaction.
- Reconnection creates a new recoverable segment under the same logical session and records the gap duration.
- Intentional Stop never triggers reconnection.
- Duplicate callbacks cannot create duplicate sessions, alerts, or writers.
- Repeating Stop/Start and disconnect/reconnect cycles cannot leave ECG or ACC absent while HR continues, and upload work cannot mutate Bluetooth stream state.
- The UI reports `recording` only after RR, ECG, and ACC have each delivered data for the current connection generation.
- Reconnection behavior is covered with deterministic simulated-device tests.

### R5 considerations — Notifications

Current observations:

- A disconnect notification is sent immediately.
- Stream-stale checks can send several notifications together and then invalidate their own timer, preventing continued monitoring.
- The app cannot create a notification after its process has already crashed. Crash awareness therefore needs either a notification scheduled in advance or an external server watchdog.
- The project has an iPhone app and Live Activity extension, but no watchOS app target. A separate watch app is therefore not available as an independent monitor today.
- The current Live Activity is updated by the phone process. It can provide glanceable state, including on supported Apple Watch surfaces, but by itself cannot emit a new failure alert after the phone app dies.

Proposed behavior:

- **0–10 seconds:** reconnect silently; show status only in the app and Live Activity.
- **At 10 seconds:** send one “Reconnecting” notification if the interruption is still active.
- **At 60 seconds or after repeated failed attempts:** send one audible “Recording needs attention” notification.
- Cancel pending disconnect notifications immediately after recovery and avoid a noisy “recovered” notification for very short gaps.
- Monitor only streams enabled in the current recording profile and use stable notification identifiers for replacement and cancellation.
- Keep the crash/data watchdog independent of the file-rotation interval. Refresh one stable, pre-scheduled local notification at a bounded cadence only while RR, ECG, and ACC are all advancing. If the process dies or data stops, the already-scheduled notification can still fire.
- Make the watchdog deadline configurable in Settings. The initial proposal is a three-minute default refreshed at most once per minute, producing an expected trigger roughly two to three minutes after process death. Longer options should remain available if physical-device use shows false alerts.
- Schedule the first watchdog when recording starts. Explicit Stop cancels both pending and already-delivered watchdog alerts. Session-ID guards must prevent a late save or callback from an older recording from rearming it.
- Treat raw file-write failure separately: show an immediate attention alert and do not refresh durable progress. The watchdog must never be required for the recording write path and must never change, insert events into, or delay a SensorBag.
- Give each Live Activity update a future stale date and render a conspicuous stale state. Treat this as a secondary visual indicator, not the only alert mechanism.
- Use normal notification delivery to reach the paired Apple Watch when the iPhone is locked or asleep; do not add a watchOS target solely for the first watchdog implementation.
- Consider a server-side missing-heartbeat alert later if immediate failure notification must work across app termination, phone failure, or loss of local execution.

Acceptance criteria:

- A disconnect shorter than 10 seconds produces no notification.
- A persistent interruption produces one notification at each configured escalation level, not one per stream or callback.
- Recovery cancels all obsolete pending alerts.
- A simulated dead process results in the pre-scheduled watchdog notification.
- The configured watchdog can request delivery in less than five minutes without changing the five-minute recording cadence.
- Explicitly stopping a recording cancels the watchdog and all reconnect alerts.
- A healthy 12-hour run produces no false watchdog alert, while terminating the app after a checkpoint produces one alert within the documented grace period.
- Watch delivery is verified with the iPhone locked and the paired Apple Watch unlocked; failure to mirror must remain visible on the iPhone.

Evidence references:

- [Apple local notification scheduling](https://developer.apple.com/documentation/usernotifications/scheduling-a-notification-locally-from-your-app)
- [Apple Live Activity stale dates](https://developer.apple.com/documentation/activitykit/activitycontent/staledate)
- [Apple Watch notification routing](https://support.apple.com/en-gb/108369)

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
- Build device-clock-to-wall-clock alignment and byte-range indexes as sidecar metadata so existing v1 files remain unchanged. Consider a new recording format only as a separately approved migration if sidecars prove insufficient.

Acceptance criteria:

- Selecting an RR timestamp opens its ten-second ECG neighborhood quickly without loading the complete recording.
- Panning and zooming remain responsive on long sessions.
- Keeping the ECG detail closed adds no continuous rendering workload.
- Existing recordings remain readable.

### R7 considerations — Other continuous-recording inefficiencies

Areas to include in ongoing review:

- Serialize sensor ownership and mutable recording state to remove race conditions.
- Keep all queues and buffers bounded.
- Batch HealthKit authorization and imports rather than initializing the complete flow for every small file.
- Make background jobs cancellable and report expiration accurately instead of allowing late callbacks to mark expired work successful.
- Record only telemetry that directly answers an active reliability question; do not persist high-frequency per-packet details.
- Exercise 12-hour and 24-hour physical-device soak tests after each major recording-engine change.

Acceptance criteria:

- A 24-hour soak test shows stable memory and bounded queues.
- No synchronous disk or network work blocks the main thread.
- Background expiration, disk-full, corrupt-tail, permission-loss, and network-loss cases fail safely without silently losing the whole session.

### R8 considerations — Reset-button safety

Current observations:

- “Reset background refresh cursors” immediately deletes all incremental-export cursor values with one tap. A later export can consequently begin from the default 2001 start date.
- “Reset backfill memory” immediately forgets which SensorBag files were imported. It is disabled only for a backfill started from the current Settings view and does not protect against other active work.
- A lock can reject some cursor resets internally, but the button does not explain the failure or confirm what was changed.

Proposed behavior:

- Put both reset controls in a visually destructive section and require a confirmation that names the resulting reprocessing work.
- Refuse reset while the corresponding exporter/backfill is active or while continuous recording is active; return and display an explicit result rather than relying only on a log.
- Report how many cursor or index entries were removed. Do not automatically start the resulting re-export.

Acceptance criteria:

- Neither reset can occur with one accidental tap.
- A busy-state race cannot partially reset state.
- Success and failure are visible, including the number of removed entries.

### R9 considerations — UI during active recording

Current observations:

- The root tab view currently redirects every attempted tab change back to HRV while `isProcessing` is true and blurs other tabs.
- This prevents viewing logs, upload state, and safe settings during a long recording.

Proposed behavior:

- Move continuous-recording status to app-scoped state rather than using a global UI lock.
- Permit normal tab navigation while recording and show a persistent recording indicator with elapsed time and a path back to Stop.
- Disable or guard only actions that conflict with recording, such as destructive state resets or starting a second recording/export with unsafe resource overlap.

Acceptance criteria:

- Switching tabs does not stop, restart, duplicate, or interrupt sensor ingestion.
- Logs and upload status remain readable during recording.
- Returning to HRV shows the same session and controls.

## Decision log

Record decisions here as requirements are refined.

| Date | Requirement | Decision | Reason |
| --- | --- | --- | --- |
| 2026-09-03 | R3 | Immediately send each finalized five-minute continuous package over Wi-Fi or cellular | Prompt server delivery is preferred over waiting for recording completion, opportunistic scheduling, or Wi-Fi |
| 2026-09-03 | All | Prefer an explicit, retryable pipeline failure over skipping or losing data | Source data and failed work must remain pending; errors must be shown with actionable context rather than swallowed |
| 2026-09-04 | R2/R5 | Restore the approximately 12-hour silent termination as a high-priority issue and add an independent dead-man alert | The first-use HealthKit fix did not address the original long-recording failure, and an app cannot notify after its own process has already died unless the alert was scheduled in advance or sent externally |
| 2026-09-04 | R5 | Start with a rolling local notification plus stale Live Activity rather than a new watchOS app | This provides process-independent alerting with low update frequency and can reach a paired Apple Watch through normal notification routing |
| 2026-09-04 | All | Remove completed items from this active tracker | Keep the document focused on unresolved requirements; use Git history for completed work |
| 2026-09-06 | R2/R3 | Preserve v1 bytes, final filenames, and the server-facing upload/export contract | Crash protection and scalability work must not silently break offline reconstruction or backend compatibility |
| 2026-09-06 | R5 | Decouple the configurable crash/data watchdog from five-minute file rotation | Notification latency may be shorter than the persistence interval, while file-write failures remain an immediate and separate error path |
| 2026-09-06 | R8/R9 | Capture reset-button protection and recording-time tab navigation as separate work | These concerns should not be forgotten or silently expand the current crash slice |
| 2026-09-06 | R4 | Treat repeatable Polar SDK stream lifecycle as a prerequisite for reconnect/resume | Current dual connection ownership, terminal publisher teardown, and sticky stream-start flags can explain HR continuing while ECG or ACC fails after repeated lifecycle operations |

## Progress notes

Add dated implementation and verification notes here as work proceeds.

- **2026-09-04:** Clarified that the original crash concern remains: continuous recording may terminate silently after roughly 12 hours and is noticed only when the phone is unlocked. R2 is active again and now pairs with R5. The project contains no watchOS target; the first monitoring design will use a rolling system-delivered local notification and a stale Live Activity, with ordinary Apple Watch notification routing where available.
