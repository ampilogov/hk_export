# My Requirements

Last updated: September 7, 2026

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

**Status:** In progress — observe normal use with retained lightweight diagnostics; no dedicated long-duration test is planned

### R3 — Make large recording collections and uploads efficient

The app must remain responsive with tens of thousands of existing recordings. Opening the Upload screen, determining pending work, and uploading recordings should not require long waits. While continuous recording is active, each finalized package should be durably queued and sent immediately over either Wi-Fi or cellular; it must not wait for the user to stop recording or for an opportunistic background task. Future recordings should produce substantially fewer permanent local files.

**Status:** In progress — the HealthKit JSON processing index is the next scalability bottleneck

### R4 — Automatically reconnect and resume recording

Use the Polar SDK with one repeatable connection and stream lifecycle. Repeated Stop/Start, disconnect/reconnect, and unrelated upload activity must not leave HR/RR, ECG, or ACC partially stopped. When the Polar H10 disconnects unexpectedly, the app should reconnect automatically and resume every required stream without manual intervention. The resulting data should clearly represent any unavoidable gap.

**Status:** In progress — implementation complete; real-device verification is deferred to one bundled validation pass after the next implementation items

### R5 — Make notifications actionable without causing noise

Short, self-healing interruptions should not immediately notify the user. Persistent interruptions, exhausted reconnection attempts, recording failures, and unexpected app termination should reliably request attention.

**Status:** In progress — staged alerts are implemented; iPhone and paired-Watch delivery are deferred to the bundled real-device validation pass

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

1. **R4 and R5 — Reconnection, recording resume, and failure notification.** Build one connection-health state machine and use it for silent recovery followed by actionable escalation.
2. **R3 — Storage and upload scalability.** Fix the existing backlog experience, then compact local storage without changing server-visible files or payloads.
3. **R2 — Passive stability observation.** Keep lightweight diagnostics available and investigate only if an unexpected termination recurs during normal use.
4. **R6 — ECG inspection.** Build the viewer after the storage format supports efficient time-range reads.
5. **R8 — Reset safety.** Protect destructive cursor and backfill reset controls before making the rest of the UI available during recording.
6. **R9 — Recording-time UI usability.** Remove the global tab lock after recording state and conflicting actions are safely app-scoped.
7. **R7 — Remaining improvements.** Reassess after the earlier work.

## Status tracker

| ID | Requirement | Status | Next milestone |
| --- | --- | --- | --- |
| R2 | Crash prevention and diagnostics | In progress | Review retained diagnostics only if termination or recording stall recurs during normal use |
| R3 | File and upload efficiency | In progress | Replace the HealthKit JSON processing index with transactional SQLite state |
| R4 | Auto reconnect and resume | In progress | Include reconnect and all-stream recovery in the bundled real-device validation pass |
| R5 | Notification behavior | In progress | Include the 10-second and 60-second iPhone/Watch alerts in the same validation pass |
| R6 | Interactive ECG | Proposed | Add time-range ECG decoding for existing recordings |
| R7 | Other inefficiencies | Proposed | Reassess after R2–R3 |
| R8 | Reset-button safety | Proposed | Define confirmation, busy-state, and error-result behavior for both reset controls |
| R9 | UI during active recording | Proposed | Move recording status to app scope and inventory actions that must remain disabled |

## Considerations by requirement

### R2 considerations — Crashes and unexpected termination

Current observations:

- The app reportedly terminates silently after roughly 12 hours of continuous recording. The user normally discovers this only after unlocking the phone. The cause is still unknown until a physical-device run produces MetricKit, interruption-marker, or watchdog evidence.
- The recording invariant is that every `SensorEvent` delivered by the SDK is written exactly once, in per-stream order, to exactly one adjacent v1 file. The current format cannot recover a packet that never reached the app, and a process crash can still lose the unfinished in-memory window of up to the configured recording duration.
- The RR graph still assumes ordered timestamps and non-zero plot ranges. Harden it if the captured evidence implicates plotting or if interactive ECG work reuses that path.
- Large directory scans and HealthKit index work can still run from the main thread, creating watchdog risk; this belongs to R3.

Remaining approach:

- Do not schedule dedicated 12-hour or 24-hour soak tests. Observe stability during normal recording use.
- Keep the existing bounded session journal and MetricKit crash/application-exit payloads. Add no higher-frequency telemetry unless a recurring failure provides a specific question that requires it.
- If termination recurs, classify it from the retained evidence before adding unfinished-window protection or more instrumentation.
- Harden plotting against duplicate timestamps, empty data, and constant ranges when the graph path is next changed or if retained evidence implicates it.

Acceptance criteria:

- Previously completed files remain readable after force termination; only the unfinished current window remains at risk, bounded by the configured recording duration.
- The binary bytes, final filename, and server-visible upload/export contract remain unchanged.
- Any reduction in the crash-loss window must not materially increase battery drain or create an iCloud/file-synchronization storm.
- If a termination occurs during normal use, the next launch exposes the retained session marker and any available MetricKit evidence; the independently scheduled watchdog remains the immediate user-facing alert.
- Graph edge cases are covered by automated tests before the graph path is expanded.

### R3 considerations — Files and uploads

Current observations:

- Each recording window creates a separate `.bin` file.
- Uploads are serial, and each file is fully loaded, wrapped in a property list, and gzip-compressed in memory.
- HealthKit backfill reloads and rewrites its complete JSON index after each processed recording, which scales especially poorly for a large backlog.
- Continuous recording does not currently upload a segment when it is finalized. Upload is triggered only after the user stops recording or later by opportunistic background jobs, which iOS may delay substantially.
- An unreachable server can perform four attempts per file without a collection-level circuit breaker.
- Upload logging magnifies backlog work: each file emits multiple persistent logs, and each log rebuilds and rewrites up to 400 records in `UserDefaults`.
- Upload uses a default foreground `URLSession`, so the operating system does not own continuation of a transfer after suspension or termination.

Proposed approach:

- Replace the HealthKit JSON index with transactional SQLite state.
- Continue producing the same five-minute `.bin` upload artifacts with the same filename scheme and bytes. Reduce local top-level file count only through reversible local indexing or post-upload archival that can reproduce every original file exactly.
- Extend the single-flight upload coordinator with a durable pending queue while retaining deduplication across UI, recording, and background triggers.
- Mark every finalized five-minute package pending immediately and begin its transfer while recording continues. Permit both Wi-Fi and cellular, including expensive-network access; do not wait for recording Stop.
- Use a background `URLSession` and file-backed request bodies so the system can continue eligible transfers while the app is suspended and upload memory remains bounded.
- Add a collection-level circuit breaker and retry budget. Immediate sending over cellular is required, but a confirmed server/network outage must pause and reschedule the batch instead of retrying every pending file continuously.
- Aggregate repetitive logs into periodic progress summaries.
- Any future bundled or resumable transport must remain outside scope unless the server migration and compatibility contract are explicitly approved.
- Provide an explicit migration/compaction tool for the existing backlog. Never delete originals until the compacted output is verified and the configured retention rule permits deletion.

Acceptance criteria:

- The Upload screen presents useful cached information in under 0.5 seconds with 50,000 indexed recordings.
- UI rendering does not trigger repeated directory scans or metadata reads.
- Local compaction can reproduce every original five-minute filename and byte sequence exactly; until compaction is verified and reversible, the original files remain untouched.
- Every finalized five-minute package is immediately represented in the durable pending queue and starts an upload attempt without ending the recording.
- Upload is allowed over both Wi-Fi and cellular.
- A collection-wide network failure performs a bounded number of attempts and cannot generate one retry storm per pending file.
- Upload progress survives relaunch and retries without duplicating accepted content.
- Upload memory use does not scale with the total backlog.

Dependencies and decisions:

- Server protocol changes are not part of the current plan. Asynchronous UI, a single local index, bounded-memory uploads, and reversible local archival can be completed while preserving the existing server contract.
- Retention policy must remain conservative by default: keep source data unless deletion has been explicitly enabled.

### R4 considerations — Auto reconnect and recording resume

Remaining verification:

- During one continuous recording, interrupt the H10 connection briefly and confirm recording remains active and RR, ECG, and ACC all resume.
- Confirm that stopping while recovery is in progress prevents recording from resuming, even if the SDK later reconnects the device.
- Automatic recording recovery after an app-process termination is not part of this slice; the independent watchdog requests attention instead.

Acceptance criteria:

- A brief disconnect reconnects and resumes without user interaction.
- Reconnection creates a new recoverable segment under the same logical session and records the gap duration from the last actual sample, not from delayed detection.
- Intentional Stop never resumes or starts recording after a later connection recovery.
- Duplicate callbacks cannot create duplicate sessions, alerts, or writers.
- Repeating Stop/Start and disconnect/reconnect cycles cannot leave ECG or ACC absent while HR continues, and upload work cannot mutate Bluetooth stream state.
- Empty SDK packets remain in the source recording but do not count as healthy RR, ECG, or ACC data.
- The UI reports `recording` only after RR, ECG, and ACC have each delivered data for the current connection generation.
- Reconnection behavior is covered with deterministic simulated-device tests.

### R5 considerations — Notifications

Remaining verification:

- **0–10 seconds:** reconnect silently; show status only in the app and Live Activity.
- **At 10 seconds:** send one “Reconnecting” notification if the interruption is still active.
- **At 60 seconds or after repeated failed attempts:** send one audible “Recording needs attention” notification.
- Disconnect and SDK stream-failure callbacks schedule the 10-second and 60-second alerts immediately and therefore remain effective after suspension. A stream that becomes silently stale can only be identified while the app is executing; if iOS suspends it first, the independently pre-scheduled configurable watchdog is the process-independent fallback.
- Verify delivery to the paired Apple Watch with the iPhone locked before considering a dedicated watchOS target.

Acceptance criteria:

- A disconnect shorter than 10 seconds produces no notification.
- A persistent interruption produces one notification at each configured escalation level, not one per stream or callback.
- Recovery cancels all obsolete pending alerts.
- Failure to schedule an interruption alert is shown in the app with the failed notification identifier and underlying error.
- A simulated dead process results in the pre-scheduled watchdog notification.
- A healthy normal-use recording produces no false watchdog alert, while a deliberate notification sanity check produces one alert within the documented grace period.
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
- Make underlying background exporter and upload work cancellable when its task expires.
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
| 2026-09-06 | R2 | Use passive observation instead of dedicated long-duration soak testing | Existing bounded MetricKit and session-journal evidence is sufficient unless a failure recurs; manual effort should focus on notification sanity checks |
