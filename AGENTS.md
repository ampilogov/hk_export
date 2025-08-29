# Repository Guidelines

## Project Structure & Module Organization
- `fitness_exporter/`: Swift source (SwiftUI app, HealthKit, Bluetooth, export logic). Key files: `HealthDataExporter.swift`, `IncrementalExporter.swift`, `BluetoothManager.swift`, `HRVView.swift`.
- `fitness_exporter.xcodeproj/`: Xcode project files and workspace.
- Assets and configs: `fitness_exporter/Assets.xcassets/`, `fitness_exporter/fitness_exporter.entitlements`, `fitness-exporter-Info.plist`.
- Certificates (development): `client_cert.p12`, `server_cert.der` — treat as sensitive.

## Build, Test, and Development Commands
- Open in Xcode: `open fitness_exporter.xcodeproj`
- Build (Debug, iOS Simulator):
  - `xcodebuild -scheme fitness_exporter -configuration Debug -destination 'platform=iOS Simulator,name=iPhone 15' build`
- Run unit tests (if/when a test target exists):
  - `xcodebuild -scheme fitness_exporterTests -destination 'platform=iOS Simulator,name=iPhone 15' test`
- Logging: app uses `CustomLogger`. View logs via in‑app Log view or Xcode console.

## Coding Style & Naming Conventions
- Swift 5, 4‑space indentation, 120‑col soft wrap.
- Follow Swift API Design Guidelines; use camelCase for variables/functions, UpperCamelCase for types.
- File names match primary type/view (e.g., `HRVView.swift`, `BluetoothManager.swift`).
- Prefer structs and value semantics; keep UI on the main thread (notably in graphs/HRV code).

## Testing Guidelines
- Framework: XCTest. Create a test target `fitness_exporterTests` and name files `ThingTests.swift`.
- Test names: `test_<behavior>_<condition>()` with Arrange‑Act‑Assert comments where helpful.
- Focus: exporters (incremental boundaries), Bluetooth parsing, HealthKit query filters. Use dependency injection/mocks.
- Aim for meaningful coverage of core data paths; snapshot/UI tests are optional.

## Commit & Pull Request Guidelines
- Commits: imperative mood and scoped prefixes when useful (e.g., `HRVView: Fix warmup duration handling`).
- Keep changesets focused; include rationale in the body.
- PRs: clear description, linked issues, before/after screenshots for UI, and test notes. Mention any background task or permission impacts.

## Security & Configuration Tips
- Default server URL is set in `AppDelegate` via `UserDefaultsKeys.SERVER_URL`. Update via Settings in‑app or override in code for dev.
- Do not commit real secrets. Treat `client_cert.p12` and `server_cert.der` as placeholders; rotate for production.
- Background tasks (`BGTaskScheduler`) require matching identifiers in capabilities and server reachability during tests.
