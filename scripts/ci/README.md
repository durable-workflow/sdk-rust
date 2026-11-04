# CI support

These scripts support four public repository checks:

- replay and codec regression-corpus validation;
- Apache Avro interoperability and performance checks;
- publishable crate and fresh-consumer verification;
- API documentation, analytics, navigation, and base-URL checks.

The release workflow uses the crates.io publishing scripts after an immutable
tag already exists. A version that is not yet on crates.io is published only
when `cargo package` of the tag reproduces the reviewed pre-publication record
`release/approved-packages/<version>.json` exactly. That record must be the
regular file tracked at that path in the maintained-tooling checkout. Its
acceptance receipt must be complete and passing, and must equal the run's
committed `<version>.summary.json`. The release runs as four jobs (`gate`,
`publish`, `verify` and `release`). Only `publish` has `id-token` and the
crates.io credential, and it runs no crate build or test. The record must match the
SHA-256, file set, version, VCS commit and Cargo release. After publication, the
crates.io checksum, the downloaded archive and the tag read back from `origin`
must still match it. See `scripts/acceptance/installed-native/README.md`
(pre-publication mode) for how a passing acceptance run produces the record. Ordinary pull requests run the product tests and package
checks directly from `.github/workflows/ci.yml`.

GitHub release notes come from the exact versioned CHANGELOG section selected by
`release_package.py --release-notes VERSION`. The existing CommonMark validator
ignores apparent headings inside code blocks/quotes and refuses missing, empty
or ambiguous entries. It also requires the requested version to match Cargo.
Notes mode reads only the current checkout's regular `Cargo.toml` and
`CHANGELOG.md`, refusing custom paths and symlinks. The maintained workflow
prepares these notes before obtaining publication
credentials and passes the temporary file to `gh release create --notes-file`.
Qualified tags select reviewed source before merge, so merged-PR auto-generated
notes would describe the preceding release. Package source is never modified to
produce release text.

## Local activity qualification

Dispatch `ci.yml` with `local_qualification=true`. The nine ignored
`local_activity_server` cases run actual SDK workers against the published
Server 2.5.0 image in `docker-compose.local-activities.yml`. The task lease is
10 seconds and the real repair loop runs every two seconds. The disposable
Node proxy forwards one completion to Server, consumes its successful response,
then closes the SDK connection to prove lost-acknowledgement replay. Another
case sends SIGKILL to a real child Worker and awaits natural lease reclamation.
No task or lease rows are edited to force either result.

Leave `local_sdk_version` empty for source qualification. Supply an exact
crates.io version for a fresh installed consumer, with no source SDK override.
The run retains scenario results, package provenance, image identities, consumer
metadata and the downloaded crate's SHA-256 for 90 days. The runtime and payload
volume are removed when the job finishes. These checks do not qualify Cloud.

## Stable API documentation

Normal CI documents the current source. The separate `API Documentation`
workflow builds the latest stable public crate for the stable Pages site.
`resolve-docs-release.py` checks crates.io's version and checksum against the
maintained tracked approved-package record and its complete passing acceptance
summary, the public source tag, the archive's clean
VCS identity, manifest and exact file inventory. It extracts that verified
archive into a fresh build directory. Missing or mismatched publication evidence
refuses the build; current SDK source is never a fallback.

The maintained site navigation, assets and analytics are applied to that API
build. `__build.json` records the package version, checksum, source revision and
documentation tooling revision. Example links bind to the published source.
Pull requests exercise the published build without Pages write permissions.

After publishing a new stable crate and accepting its public identity, dispatch
the existing `API Documentation` workflow on `main` once. Verify the protected
Pages deployment, live `__build.json`, API version and representative changed
pages before recording documentation follow-through. A source or version commit
alone does not publish the crate or change the stable API selection.

Run the focused refusal controls with:

```bash
python3 scripts/ci/test-resolve-docs-release.py
```
