//! Worker answers without repeated `server_capabilities` (server#1262).
//!
//! A worker request sends the digest of the capabilities this Client holds,
//! or `none`. A Server that supports the digest answers with each copy of its
//! current capabilities either tagged with a `digest` member, which is kept
//! here for later requests, or replaced by the digest string, which is
//! restored here before any reader sees the answer. Older Servers ignore the
//! header and answer in full, so readers always observe complete capabilities.
//!
//! The Server only ever replaces copies with the digest that request sent, so
//! an answer is resolved against the pair captured when its request was sent,
//! not the pair held when it arrives. Overlapping requests across a change of
//! capabilities (a rolling deploy) each restore their own object.

use crate::{Error, ProtocolFailure};
use serde_json::{Map, Value};
use std::sync::{Arc, Mutex};

pub(crate) const HEADER: &str = "X-Durable-Workflow-Server-Capabilities-Digest";
const FIELD: &str = "server_capabilities";
const PREFIX: &str = "sha256:";
/// The top level and up to two objects deep, as in a carried poll's `answer`.
const MAX_DEPTH: usize = 3;

/// One digest and the capabilities object it names.
#[derive(Clone, Debug)]
pub(crate) struct Pair {
    digest: String,
    capabilities: Arc<Map<String, Value>>,
}

/// The pair a request was sent with (`None` before any tagged copy).
pub(crate) type Captured = Option<Pair>;

/// The newest pair this Client learned, shared by its clones.
#[derive(Clone, Debug, Default)]
pub(crate) struct Digest(Arc<Mutex<Option<Pair>>>);

impl Digest {
    /// The pair a request is sent with; pass it to [`Digest::restore`].
    pub(crate) fn capture(&self) -> Captured {
        self.0.lock().ok().and_then(|pair| pair.clone())
    }

    /// Forget the held pair, so the next request asks for full capabilities.
    pub(crate) fn forget(&self) {
        if let Ok(mut pair) = self.0.lock() {
            *pair = None;
        }
    }

    pub(crate) fn header(captured: &Captured) -> &str {
        captured
            .as_ref()
            .map_or("none", |pair| pair.digest.as_str())
    }

    /// Restore a request's answer body against the pair captured when it was
    /// sent. Returns the rewritten body, or `None` when it needs no change:
    /// no digest string or tagged copy, or not a JSON object (left to its
    /// reader).
    pub(crate) fn restore(
        &self,
        status: u16,
        bytes: &[u8],
        captured: &Captured,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.restore_with(status, bytes, captured, None)
    }

    /// Like [`Digest::restore`], but a digest string that matches nothing is
    /// replaced by `fallback`: full capabilities read separately (from
    /// cluster info) for an answer that must not be requested again.
    pub(crate) fn restore_with(
        &self,
        status: u16,
        bytes: &[u8],
        captured: &Captured,
        fallback: Option<&Map<String, Value>>,
    ) -> Result<Option<Vec<u8>>, Error> {
        if !contains(bytes, FIELD.as_bytes()) {
            return Ok(None);
        }
        let Ok(Value::Object(mut answer)) = serde_json::from_slice::<Value>(bytes) else {
            return Ok(None);
        };
        // A tagged copy anywhere in this answer can name a digest string in it.
        let mut learned = None;
        collect_tagged(&mut answer, 1, &mut learned);
        let mut changed = learned.is_some();
        if let Err(digest) = replace(&mut answer, 1, captured, &learned, fallback, &mut changed) {
            return Err(Error::Protocol(ProtocolFailure {
                status,
                reason: "server_capabilities_digest_unknown".to_owned(),
                message: format!(
                    "the server replaced server_capabilities with digest {digest}, \
                     which this worker did not send and the answer does not carry"
                ),
                supported_version: None,
                requested_version: None,
                body: Value::Object(answer),
            }));
        }
        if let Some(pair) = learned {
            // Later requests send the newest digest this Client has seen.
            if let Ok(mut current) = self.0.lock() {
                *current = Some(pair);
            }
        }
        if !changed {
            return Ok(None);
        }
        Ok(Some(serde_json::to_vec(&Value::Object(answer))?))
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Strip the `digest` member from tagged copies and remember the pair.
fn collect_tagged(answer: &mut Map<String, Value>, depth: usize, learned: &mut Option<Pair>) {
    for (key, value) in answer.iter_mut() {
        if key == FIELD {
            let Value::Object(capabilities) = value else {
                continue;
            };
            let tagged = capabilities
                .get("digest")
                .and_then(Value::as_str)
                .filter(|digest| digest.starts_with(PREFIX))
                .map(str::to_owned);
            if let Some(digest) = tagged {
                capabilities.remove("digest");
                *learned = Some(Pair {
                    digest,
                    capabilities: Arc::new(capabilities.clone()),
                });
            }
        } else if depth < MAX_DEPTH {
            if let Value::Object(nested) = value {
                collect_tagged(nested, depth + 1, learned);
            }
        }
    }
}

/// Replace digest strings with the captured pair or a tagged copy in this
/// answer. Returns the unknown digest on a protocol failure.
fn replace(
    answer: &mut Map<String, Value>,
    depth: usize,
    captured: &Captured,
    learned: &Option<Pair>,
    fallback: Option<&Map<String, Value>>,
    changed: &mut bool,
) -> Result<(), String> {
    for (key, value) in answer.iter_mut() {
        if key == FIELD {
            let Value::String(digest) = value else {
                continue;
            };
            if !digest.starts_with(PREFIX) {
                continue;
            }
            let capabilities = [captured.as_ref(), learned.as_ref()]
                .into_iter()
                .flatten()
                .find(|pair| pair.digest == *digest)
                .map(|pair| &*pair.capabilities)
                .or(fallback)
                .ok_or_else(|| digest.clone())?;
            *value = Value::Object(capabilities.clone());
            *changed = true;
        } else if depth < MAX_DEPTH {
            if let Value::Object(nested) = value {
                replace(nested, depth + 1, captured, learned, fallback, changed)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn caps(version: u64) -> Value {
        json!({"activity_lease_renewal": true, "version": version})
    }

    fn tagged(version: u64, digest: &str) -> Value {
        let mut value = caps(version);
        value["digest"] = json!(digest);
        value
    }

    fn body(value: Value) -> Vec<u8> {
        serde_json::to_vec(&value).unwrap()
    }

    fn parse(bytes: &Option<Vec<u8>>) -> Value {
        serde_json::from_slice(bytes.as_ref().expect("rewritten body")).unwrap()
    }

    #[test]
    fn full_answers_from_older_servers_are_unchanged() {
        let digest = Digest::default();
        let captured = digest.capture();
        assert_eq!(Digest::header(&captured), "none");
        let original = body(json!({"task": null, "server_capabilities": caps(1)}));
        let restored = digest.restore(200, &original, &captured).unwrap();
        assert_eq!(restored, None, "no rewrite without a digest");
        assert_eq!(Digest::header(&digest.capture()), "none");
    }

    #[test]
    fn tagged_copy_is_stripped_and_used_by_later_requests() {
        let digest = Digest::default();
        let first = digest.capture();
        let answer = digest
            .restore(
                200,
                &body(json!({"server_capabilities": tagged(1, "sha256:d1")})),
                &first,
            )
            .unwrap();
        assert_eq!(parse(&answer)["server_capabilities"], caps(1));
        let second = digest.capture();
        assert_eq!(Digest::header(&second), "sha256:d1");
        let answer = digest
            .restore(
                200,
                &body(json!({"server_capabilities": "sha256:d1"})),
                &second,
            )
            .unwrap();
        assert_eq!(parse(&answer)["server_capabilities"], caps(1));
    }

    #[test]
    fn nested_copies_up_to_depth_three_and_error_bodies_are_restored() {
        let digest = Digest::default();
        digest
            .restore(
                200,
                &body(json!({"server_capabilities": tagged(1, "sha256:d1")})),
                &None,
            )
            .unwrap();
        let captured = digest.capture();
        let answer = json!({
            "server_capabilities": "sha256:d1",
            "activity_poll": {"answer": {"server_capabilities": "sha256:d1", "task": null}},
            "deeper": {"a": {"b": {"server_capabilities": "sha256:d1"}}},
            "list": [{"server_capabilities": "sha256:d1"}],
        });
        let restored = parse(&digest.restore(200, &body(answer), &captured).unwrap());
        assert_eq!(restored["server_capabilities"], caps(1));
        assert_eq!(
            restored["activity_poll"]["answer"]["server_capabilities"],
            caps(1)
        );
        // Beyond depth three and inside lists the server never replaces copies.
        assert_eq!(
            restored["deeper"]["a"]["b"]["server_capabilities"],
            "sha256:d1"
        );
        assert_eq!(restored["list"][0]["server_capabilities"], "sha256:d1");

        let refusal = json!({"reason": "lease_expired", "server_capabilities": "sha256:d1"});
        let restored = parse(&digest.restore(409, &body(refusal), &captured).unwrap());
        assert_eq!(restored["server_capabilities"], caps(1));
        assert_eq!(restored["reason"], "lease_expired");
    }

    #[test]
    fn rolling_deploy_resolves_each_answer_against_its_own_request() {
        let digest = Digest::default();
        digest
            .restore(
                200,
                &body(json!({"server_capabilities": tagged(1, "sha256:d1")})),
                &None,
            )
            .unwrap();
        // A completion is sent under D1 and is still in flight...
        let completion = digest.capture();
        // ...while a poll, also sent under D1, reaches an upgraded server.
        let poll = digest.capture();
        let answer = digest
            .restore(
                200,
                &body(json!({"server_capabilities": tagged(2, "sha256:d2")})),
                &poll,
            )
            .unwrap();
        assert_eq!(parse(&answer)["server_capabilities"], caps(2));
        assert_eq!(Digest::header(&digest.capture()), "sha256:d2");
        // The old server's D1 answer to the completion arrives afterwards.
        let answer = digest
            .restore(
                200,
                &body(json!({"recorded": true, "server_capabilities": "sha256:d1"})),
                &completion,
            )
            .unwrap();
        assert_eq!(parse(&answer)["server_capabilities"], caps(1));
        // A request sent after the switch resolves D2.
        let next = digest.capture();
        let answer = digest
            .restore(
                200,
                &body(json!({"server_capabilities": "sha256:d2"})),
                &next,
            )
            .unwrap();
        assert_eq!(parse(&answer)["server_capabilities"], caps(2));
        // An answer tagging D2 can also name D2 elsewhere in the same body.
        let answer = digest
            .restore(
                200,
                &body(json!({
                    "server_capabilities": tagged(2, "sha256:d2"),
                    "workflow_poll": {"answer": {"server_capabilities": "sha256:d2"}},
                })),
                &completion,
            )
            .unwrap();
        assert_eq!(
            parse(&answer)["workflow_poll"]["answer"]["server_capabilities"],
            caps(2)
        );
    }

    #[test]
    fn unknown_digest_is_a_protocol_failure() {
        let digest = Digest::default();
        digest
            .restore(
                200,
                &body(json!({"server_capabilities": tagged(1, "sha256:d1")})),
                &None,
            )
            .unwrap();
        let captured = digest.capture();
        let error = digest
            .restore(
                200,
                &body(json!({"server_capabilities": "sha256:other"})),
                &captured,
            )
            .unwrap_err();
        let Error::Protocol(failure) = error else {
            panic!("expected a protocol failure, got {error:?}");
        };
        assert_eq!(failure.reason, "server_capabilities_digest_unknown");
        // Before any tagged copy, every digest string is unknown.
        assert!(digest
            .restore(
                200,
                &body(json!({"server_capabilities": "sha256:d1"})),
                &None
            )
            .is_err());
    }

    #[test]
    fn non_digest_strings_and_non_object_bodies_are_left_alone() {
        let digest = Digest::default();
        let original = body(json!({"server_capabilities": "unavailable"}));
        assert_eq!(digest.restore(200, &original, &None).unwrap(), None);
        assert_eq!(
            digest
                .restore(200, b"[\"server_capabilities\"]", &None)
                .unwrap(),
            None
        );
        let html = b"<html>server_capabilities</html>";
        assert_eq!(digest.restore(502, html, &None).unwrap(), None);
    }
}
