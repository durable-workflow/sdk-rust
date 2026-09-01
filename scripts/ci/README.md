# CI support

These scripts support four public repository checks:

- replay and codec regression-corpus validation;
- Apache Avro interoperability and performance checks;
- publishable crate and fresh-consumer verification;
- API documentation, analytics, navigation, and base-URL checks.

The release workflow uses the crates.io publishing scripts after an immutable
tag already exists. Ordinary pull requests run the product tests and package
checks directly from `.github/workflows/ci.yml`.
