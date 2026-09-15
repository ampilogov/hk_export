# Fitness Exporter

An iOS app for recording Polar H10 heart-rate, RR-interval, ECG, and accelerometer data, running orthostatic HRV tests, and exporting collected health data.

## Build variants

Both developer identities are supported from the same source branch:

| Use | Xcode scheme | Configuration | Bundle identifier | Notification behavior |
| --- | --- | --- | --- | --- |
| Production/deployment | `fitness_exporter` | `Release` | `com.artemz.fitness-exporter` | Includes Time Sensitive Notifications |
| Artem local testing | `fitness_exporter Artem` | `Artem Release` | `com.artemz.fitness-exporter-my` | Uses the personal-team-compatible capability set |

`Artem Release` is optimized like the production build and is the preferred configuration for behavior and performance testing. `Artem Debug` is available when debugger support is more important.

The `ARTEM_BUILD` compilation condition and Artem-specific entitlements keep unsupported capabilities out of personal-team builds. Do not change the production configuration merely to make personal signing work.

## Local development

Open the project in Xcode:

```sh
open fitness_exporter.xcodeproj
```

Select the `fitness_exporter Artem` scheme and run `Artem Release` on the device for release-like testing.

Run the simulator test suite with:

```sh
xcodebuild \
  -project fitness_exporter.xcodeproj \
  -scheme fitness_exporter \
  -configuration Debug \
  -destination 'platform=iOS Simulator,OS=18.1,name=iPhone 16' \
  test
```

## Repositories and deployment

There is one source branch: `main`. The old `main-vitalik-squashed` deployment branch is no longer needed because bundle identifiers, signing teams, entitlements, and compilation conditions are selected by Xcode configuration.

The local checkout uses two remotes:

- `origin`: the primary source repository.
- `new-origin`: the deployment repository connected to the production build service.

If the deployment remote is missing, add it once:

```sh
git remote add new-origin git@github.com:ampilogov/hk_export.git
```

For an ordinary source-only push:

```sh
git push origin main
```

For a production deployment:

1. Test and commit everything on `main`.
2. Increment the integer in `BUILD_VERSION` and commit that change. This is the build-service trigger.
3. Push the exact same `main` commit to both repositories:

```sh
git push origin main
git push new-origin main
```

Do not maintain deployment-only source changes and do not force-push either `main`. If a remote rejects a push, fetch it and inspect the divergence before merging.

