# My Requirements

Last updated: September 4, 2026

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
- Show the exact failed operation, data type/file or time range, and underlying error to the user. Record the same context in diagnostics.
- A pipeline failure should be explicit and retryable rather than an intentional process crash. Raw recording may continue when its data is already durable and only a downstream HealthKit/upload stage failed; the failed downstream stage must remain pending.
- Automatic retries must be bounded and visible. Never convert failure into success merely because all remaining items were traversed.

## Platform compatibility

- Only the latest iOS release needs to be supported. New work may use current iOS APIs directly and does not need compatibility branches or fallback behavior for older iOS versions.

## Requirements

### R1 — Measure and reduce battery drain

Determine how much battery continuous recording consumes on both the iPhone and Polar H10, identify the largest sources of energy use, and reduce them without compromising the selected recording quality. Evaluate the real usage pattern: recording remains active for approximately 24 hours with the screen normally off, and the user disconnects the H10 after stopping. Once-daily morning HRV measurements did not exhibit the battery problem.

All currently captured HR/RR, ECG, and ACC data is required for offline server analysis. R1 must not disable a stream, reduce its sampling rate, discard samples, or otherwise trade data completeness for battery life.

**Status:** In progress — agreed low-risk implementation is locally verified; one basic H10 sanity recording and a normal-use unplugged battery observation remain

### R2 — Prevent and diagnose crashes

Find the causes of crashes or unexpected app termination during long recordings. Preserve as much recorded data as possible and make failures diagnosable after they occur.

**Status:** In progress — silent termination after roughly 12 hours of continuous recording remains unresolved

### R3 — Make large recording collections and uploads efficient

The app must remain responsive with tens of thousands of existing recordings. Opening the Upload screen, determining pending work, and uploading recordings should not require long waits. While continuous recording is active, each finalized package should be durably queued and sent immediately over either Wi-Fi or cellular; it must not wait for the user to stop recording or for an opportunistic background task. Future recordings should produce substantially fewer permanent local files.

**Status:** Proposed

### R4 — Automatically reconnect and resume recording

When the Polar H10 disconnects unexpectedly, the app should reconnect automatically and resume recording without requiring manual intervention. The resulting data should clearly represent any gap.

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

## Recommended implementation order

1. **R1 — Energy and stream lifecycle.** Complete one basic H10 sanity recording, then collect battery evidence during normal use rather than a dedicated profiling campaign.
2. **R2 and R5 — Long-run stability and independent failure notification.** Diagnose the reported approximately 12-hour termination and add a low-frequency dead-man alert that survives the app process.
3. **R3 — Storage and upload scalability.** Fix the existing backlog experience, then change the future file format and upload pipeline.
4. **R4 — Reconnection and recording resume.** Build this around the same health state and escalation rules used by R5.
5. **R6 — ECG inspection.** Build the viewer after the storage format supports efficient time-range reads.
6. **R7 — Remaining improvements.** Reassess using the metrics gathered by the earlier work.

## Status tracker

| ID | Requirement | Status | Next milestone |
| --- | --- | --- | --- |
| R1 | Battery drain | In progress | Run one basic H10 sanity recording; then observe battery during normal unplugged use when convenient |
| R2 | Crash prevention and diagnostics | In progress | Add post-termination diagnostics and reproduce or classify the failure with a 12-hour soak |
| R3 | File and upload efficiency | Proposed | Design one durable upload coordinator and background transfer queue |
| R4 | Auto reconnect and resume | Proposed | Define and test the recording connection state machine |
| R5 | Notification behavior | Ready | Add a rolling local dead-man notification and stale Live Activity state; verify delivery on the paired Apple Watch |
| R6 | Interactive ECG | Proposed | Add time-range ECG decoding for existing recordings |
| R7 | Other inefficiencies | Proposed | Re-profile after R1–R3 |

## Considerations by requirement

### R1 considerations — Battery drain

Current considerations:

- The real workflow is approximately 24 hours of continuous recording with the display normally off, followed by Stop and an explicit H10 disconnect. The earlier once-daily morning HRV workflow did not cause noticeable drain.
- Every currently captured HR/RR, ECG, and ACC sample is required. The sampling configuration, compact binary format, five-minute cadence, and downstream server data must remain unchanged.
- Five-minute packages still create 288 files and HealthKit write jobs per day. Reducing the permanent file count and redesigning upload scheduling belong to R3 so that R1 does not mix a storage migration into the low-risk energy changes.
- Continuous 130 Hz ECG and 25 Hz ACC have an unavoidable device and phone cost. H10 battery reporting is coarse, so meaningful H10 comparisons are best collected during normal long recordings.
- The earlier 53-minute field run occurred while the phone was charging and cannot establish battery consumption. No dedicated Power Profiler session is required before using this build.

Remaining work:

1. Run one short H10 sanity recording and confirm that RR, ECG, and ACC timestamps advance, stopping produces a normal recording file, and no new error appears.
2. Use the build normally with the phone unplugged and screen off. Compare the saved start/end battery values when a convenient run is available; this does not need to delay ordinary use.
3. Reopen the implementation only if the sanity recording shows a data regression or normal-use diagnostics still show unexpectedly high app overhead.

Acceptance criteria:

- RR, ECG, and ACC remain present at their existing sampling rates with no intentional sample loss.
- Five-minute file rotation, binary compatibility, HealthKit writes, post-Stop incremental export, and upload behavior do not regress.
- The app starts normally and a short physical recording completes without a new error.
- An unplugged normal-use observation is available before making any stronger claim about battery savings.

Evidence references:

- [Polar H10 streaming capabilities](https://github.com/polarofficial/polar-ble-sdk/blob/master/documentation/products/PolarH10.md)
- [Polar SDK known issues, including explicit H10 stream termination](https://github.com/polarofficial/polar-ble-sdk/blob/master/documentation/KnownIssues.md)
- [Apple energy-efficiency fundamentals](https://developer.apple.com/library/archive/documentation/Performance/Conceptual/EnergyGuide-iOS/FundamentalConcepts.html)
- [Apple guidance for finite background tasks](https://developer.apple.com/documentation/uikit/uiapplication/beginbackgroundtask%28expirationhandler%3A%29?language=objc)

### R2 considerations — Crashes and unexpected termination

Current observations:

- The app reportedly terminates silently after roughly 12 hours of continuous recording. The user normally discovers this only after unlocking the phone. No crash, Jetsam, watchdog, or hang artifact has yet identified the cause.
- **Lower-priority durability risk:** current recording data lives in memory until the five-minute window is serialized, so a crash can lose the unfinished window. Completed `.bin` files remain available to the existing HealthKit backfill and do not require a more elaborate recovery design now.
- Some HealthKit export paths use `fatalError` for unexpected data, and the RR graph assumes ordered timestamps and non-zero plot ranges. These remain possible crash paths to harden if termination evidence implicates them.
- Large directory scans and HealthKit index work can run from the main thread, creating watchdog risk.
- There is no test target or integrated crash, hang, memory, and disk-write diagnostic capture.

Proposed approach:

- Defer unfinished-window crash protection until the higher-priority crash and battery work is understood. If implemented, prefer checkpointing or appending to one active local staging file rather than shortening the permanent file-rotation interval or creating many additional files. Measure disk activity, battery impact, and any backup/iCloud synchronization activity before enabling it by default.
- Persist a lightweight active-session journal containing the session ID, selected device and streams, start time, last sensor packet, and last successful disk write.
- Add MetricKit reporting and preserve symbolicated build archives. Detect an unfinished journal on launch and report that the previous recording ended unexpectedly.
- Persist bounded memory, queue-depth, file-write, HealthKit, and upload telemetry at each five-minute checkpoint so the final pre-termination state survives the process.
- Run a 12-hour physical-device soak first, followed by a 24-hour confirmation after fixing the identified cause. Classify the event as a crash, watchdog termination, Jetsam/memory termination, or recording stall before treating it as resolved.
- Harden plotting against duplicate timestamps, empty data, and constant ranges.
- Add unit tests and interruption tests before making larger recording changes.

Acceptance criteria:

- Force-terminating the app leaves a readable file with only a small, explicitly bounded tail at risk.
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
- The project has an iPhone app and Live Activity extension, but no watchOS app target. A separate watch app is therefore not available as an independent monitor today.
- The current Live Activity is updated by the phone process. It can provide glanceable state, including on supported Apple Watch surfaces, but by itself cannot emit a new failure alert after the phone app dies.

Proposed behavior:

- **0–10 seconds:** reconnect silently; show status only in the app and Live Activity.
- **At 10 seconds:** send one “Reconnecting” notification if the interruption is still active.
- **At 60 seconds or after repeated failed attempts:** send one audible “Recording needs attention” notification.
- Cancel pending disconnect notifications immediately after recovery and avoid a noisy “recovered” notification for very short gaps.
- Monitor only streams enabled in the current recording profile and use stable notification identifiers for replacement and cancellation.
- Maintain a low-frequency local watchdog notification scheduled in advance and refreshed while recording is healthy. If the process dies or data stops, the already-scheduled notification can still fire.
- Initially refresh one stable watchdog notification identifier after each successful five-minute recording checkpoint, scheduling it far enough ahead to tolerate one delayed checkpoint. Explicit Stop cancels it. This avoids a high-frequency timer and should add negligible steady-state work.
- Give each Live Activity update a future stale date and render a conspicuous stale state. Treat this as a secondary visual indicator, not the only alert mechanism.
- Use normal notification delivery to reach the paired Apple Watch when the iPhone is locked or asleep; do not add a watchOS target solely for the first watchdog implementation.
- Consider a server-side missing-heartbeat alert later if immediate failure notification must work across app termination, phone failure, or loss of local execution.

Acceptance criteria:

- A disconnect shorter than 10 seconds produces no notification.
- A persistent interruption produces one notification at each configured escalation level, not one per stream or callback.
- Recovery cancels all obsolete pending alerts.
- A simulated dead process results in the pre-scheduled watchdog notification.
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
| 2026-09-03 | All | Prefer an explicit, retryable pipeline failure over skipping or losing data | Source data and failed work must remain pending; errors must be shown with actionable context rather than swallowed |
| 2026-09-04 | R2/R5 | Restore the approximately 12-hour silent termination as a high-priority issue and add an independent dead-man alert | The first-use HealthKit fix did not address the original long-recording failure, and an app cannot notify after its own process has already died unless the alert was scheduled in advance or sent externally |
| 2026-09-04 | R5 | Start with a rolling local notification plus stale Live Activity rather than a new watchOS app | This provides process-independent alerting with low update frequency and can reach a paired Apple Watch through normal notification routing |
| 2026-09-04 | All | Remove completed items from this active tracker | Keep the document focused on unresolved requirements; use Git history for completed work |

## Progress notes

Add dated implementation and verification notes here as work proceeds.

- **2026-09-03:** Initial requirements and code-review considerations captured. The existing project builds successfully for the iPhone 16 / iOS 18.1 simulator. No implementation changes have been made yet.
- **2026-09-03:** Initial R1 static audit completed. It found Polar streams that outlive recording Stop, forced screen wakefulness, per-packet Live Activity updates, unsafe short/zero recording windows, unbounded per-file HealthKit work, and upload retry/logging storms. The later usage clarification below de-prioritizes the first two as explanations for the reported drain. Upload-as-you-go is not currently implemented, and the existing uploader has security-scope, coordination, recursion, and error-reporting defects. Physical-device energy traces remain pending; production behavior has not yet been changed.
- **2026-09-03:** Usage assumptions corrected. The real case is a roughly 24-hour continuous recording with a normally off display and an explicit disconnect after Stop; once-daily morning HRV did not show the drain. Five-minute duration plus five-minute interval is confirmed to be nonstop capture with no off-window. The five-minute cadence is acceptable, but its current per-file HealthKit/index lifecycle remains a likely inefficiency. R3 now requires immediate upload of each completed five-minute package over either Wi-Fi or cellular.
- **2026-09-03:** Added a low-overhead diagnostic JSON report and Share action. The recorder now measures packet/sample counts, battery readings, file writes, HealthKit work/backlog, disconnects, and Live Activity activity without logging each packet. UI timestamps are limited to 1 Hz, Live Activity updates to one per 30 seconds, file writes moved off the main thread, and continuous HealthKit imports serialized. The iPhone 16 / iOS 18.1 simulator build succeeds. Immediate per-package upload is intentionally deferred to the next R3 slice so its battery effect can be measured separately.
- **2026-09-04:** Clarified that the original crash concern remains: continuous recording may terminate silently after roughly 12 hours and is noticed only when the phone is unlocked. R2 is active again and now pairs with R5. The project contains no watchOS target; the first monitoring design will use a rolling system-delivered local notification and a stale Live Activity, with ordinary Apple Watch notification routing where available.
