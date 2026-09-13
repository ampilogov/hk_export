# My Requirements

Last updated: September 12, 2026

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
- Automatic retries must be bounded in rate and visible, without abandoning pending files after a finite lifetime retry budget. Never convert failure into success merely because all remaining items were traversed.
- Finalized source files remain pending until durable server acceptance is confirmed. Retry an uncertain result with the same file bytes, directory, filename, user, and version so Overfit deduplicates the upload. Local HealthKit import and server file upload are independent; success in one must not imply success in the other.
- The SensorBag v1 binary bytes, visible `.bin` filename scheme, and server-facing upload/export contract are compatibility requirements. Internal durability work may use temporary files, but the final filename, payload metadata, endpoint-visible type/version, and source bytes must remain unchanged unless a separate server migration is explicitly approved.

## Platform compatibility

- Only the latest iOS release needs to be supported. New work may use current iOS APIs directly and does not need compatibility branches or fallback behavior for older iOS versions.

## Requirements

### R2 — Prevent and diagnose crashes

Find the causes of crashes or unexpected app termination during long recordings. Preserve as much recorded data as possible and make failures diagnosable after they occur.

**Status:** In progress — implementation complete; observe normal use with retained lightweight diagnostics after the final device pass

### R4 — Automatically reconnect and resume recording

Use the Polar SDK with one repeatable connection and stream lifecycle. Repeated Stop/Start, disconnect/reconnect, and unrelated upload activity must not leave HR/RR, ECG, or ACC partially stopped. When the Polar H10 disconnects unexpectedly, the app should reconnect automatically and resume every required stream without manual intervention. The resulting data should clearly represent any unavoidable gap.

**Status:** In progress — implementation complete; real-device verification is deferred to the final bundled validation pass

### R5 — Make notifications actionable without causing noise

Short, self-healing interruptions should not immediately notify the user. Persistent interruptions, exhausted reconnection attempts, recording failures, and unexpected app termination should reliably request attention.

**Status:** In progress — staged alerts are implemented; iPhone and paired-Watch delivery are deferred to the bundled real-device validation pass

## Recommended implementation order

1. **R2, R4, and R5 — Final bundled device validation.** Verify H10 recovery, all three streams, navigation, ECG access, staged notifications, the watchdog, and diagnostic visibility in one pass. Continue passive crash observation during normal use afterward.

## Status tracker

| ID | Requirement | Status | Next milestone |
| --- | --- | --- | --- |
| R2 | Crash prevention and diagnostics | In progress | Review retained diagnostics only if termination or recording stall recurs during normal use |
| R4 | Auto reconnect and resume | In progress | Include reconnect and all-stream recovery in the bundled real-device validation pass |
| R5 | Notification behavior | In progress | Include the 10-second and 60-second iPhone/Watch alerts in the same validation pass |

## Considerations by requirement

### R2 considerations — Crashes and unexpected termination

Current observations:

- The app reportedly terminates silently after roughly 12 hours of continuous recording. The user normally discovers this only after unlocking the phone. The cause is still unknown until a physical-device run produces MetricKit, interruption-marker, or watchdog evidence.
- The recording invariant is that every `SensorEvent` delivered by the SDK is written exactly once, in per-stream order, to exactly one adjacent v1 file. The current format cannot recover a packet that never reached the app, and a process crash can still lose the unfinished in-memory window of up to the configured recording duration.

Remaining approach:

- Do not schedule dedicated 12-hour or 24-hour soak tests. Observe stability during normal recording use.
- Keep the existing bounded session journal and MetricKit crash/application-exit payloads. Add no higher-frequency telemetry unless a recurring failure provides a specific question that requires it.
- If termination recurs, classify it from the retained evidence before adding unfinished-window protection or more instrumentation.

Acceptance criteria:

- Previously completed files remain readable after force termination; only the unfinished current window remains at risk, bounded by the configured recording duration.
- The binary bytes, final filename, and server-visible upload/export contract remain unchanged.
- Any reduction in the crash-loss window must not materially increase battery drain or create an iCloud/file-synchronization storm.
- If a termination occurs during normal use, the next launch exposes the retained session marker and any available MetricKit evidence; the independently scheduled watchdog remains the immediate user-facing alert.

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
- A simulated dead process results in the pre-scheduled watchdog notification.
- A healthy normal-use recording produces no false watchdog alert, while a deliberate notification sanity check produces one alert within the documented grace period.
- Watch delivery is verified with the iPhone locked and the paired Apple Watch unlocked; failure to mirror must remain visible on the iPhone.

Evidence references:

- [Apple local notification scheduling](https://developer.apple.com/documentation/usernotifications/scheduling-a-notification-locally-from-your-app)
- [Apple Live Activity stale dates](https://developer.apple.com/documentation/activitykit/activitycontent/staledate)
- [Apple Watch notification routing](https://support.apple.com/en-gb/108369)

## Final bundled device pass

Run this once after installing the completed build:

1. Start continuous recording and confirm RR, ECG, and ACC become current. Lock the phone and wait for at least one five-minute file boundary.
2. While recording, visit Upload, Logs, and Settings. Confirm the recording strip remains visible, returns to the same session, and destructive reset/backfill controls are disabled.
3. Tap an RR point and confirm a ten-second ECG view loads and pans smoothly without interrupting recording.
4. Briefly interrupt the H10 connection for less than ten seconds. Confirm no notification is delivered and all three streams recover automatically.
5. Interrupt it for longer than ten seconds and confirm the reconnecting notification. Leave it interrupted through sixty seconds and confirm the time-sensitive attention notification reaches the expected iPhone/Watch destination.
6. Recover once more, then stop normally. Confirm the latest logs contain a consistent session ID across `Started`, `File`, any `Interrupted`/`Recovered` events, upload outcome, and `Finalized`.
7. In a separate short run, force-terminate the app and wait for the configurable watchdog. Relaunch and confirm the unexpected-termination message identifies the last completed file. Do not perform a dedicated long soak.

If anything fails, capture the latest in-app log pages before retrying. The retained diagnostics intentionally contain lifecycle transitions, file/upload outcomes, recovery gaps, notification errors, and MetricKit reports only—no per-packet or battery telemetry.

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
| 2026-09-08 | R2–R7 | Defer physical-device testing until all remaining implementation work is complete | One diagnostic-friendly bundled pass minimizes scarce device-testing time and avoids repeated setup churn |
| 2026-09-08 | R2/R7 | Keep device diagnostics limited to lifecycle transitions, durable-file/upload outcomes, recovery gaps, notification outcomes, and explicit failures | These events can distinguish the likely device-only failure modes without packet-level logs, battery sampling, or continuous telemetry |
| 2026-09-08 | R3 | Keep every existing recording file, binary format, filename, and server contract unchanged; reconsider lossless local compression only if a representative real recording demonstrates a significant size reduction | Large-collection performance is addressed through non-destructive indexing, caching, and durable upload queues, while deterministic HealthKit sync identifiers preserve idempotent writes; speculative format complexity is not justified |
