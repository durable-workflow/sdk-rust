# CI support

These scripts support four public repository checks:

- replay and codec regression-corpus validation;
- Apache Avro interoperability and performance checks;
- publishable crate and fresh-consumer verification;
- API documentation, analytics, navigation, and base-URL checks.

The release workflow uses the crates.io publishing scripts after an immutable
tag already exists. Ordinary pull requests run the product tests and package
checks directly from `.github/workflows/ci.yml`.

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
