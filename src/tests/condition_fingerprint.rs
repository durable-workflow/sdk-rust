use super::*;

fn checked_predicate() -> Result<bool> {
    Ok(true)
}

#[test]
fn macro_predicate_fingerprints_match_the_v1_contract() {
    let ctx = workflow_context(Vec::new());
    // These literals guard durable identities in both supported CI toolchains.
    // A changed compiler expansion needs a compatibility decision, not a blind refresh.
    let cases = [
        (
            "simple",
            wait_condition!(ctx, "simple", || Ok(true)),
            "sha256:0a73d57d2d47028c6035ce87dc1b7e3c0c8f891d0bf40bcd340510ace052a869",
        ),
        (
            "fallible",
            wait_condition!(ctx, "fallible", || Ok(checked_predicate()?)),
            "sha256:b63dd90e172358b7f2d873f209004e06b44d6b5d795ea053874e9ec56971648b",
        ),
        (
            "method_chain",
            wait_condition!(ctx, "method_chain", || Ok([1_u8, 2, 3]
                .iter()
                .any(|n| *n == 2))),
            "sha256:fe495a6005c06264582cb9c9ee7e83c59b4a504b79c47d8f602eb5f5d00e44fe",
        ),
        (
            "pattern",
            wait_condition!(ctx, "pattern", || Ok(
                matches!(Some(7_u32), Some(n) if n > 3)
            )),
            "sha256:8f28f7046a63e558cd9b6d079d04ff235e71a4130534c1e921f8bfd02480e227",
        ),
        (
            "slice",
            wait_condition!(ctx, "slice", || Ok([1_u8, 2, 3][..2] == [1, 2])),
            "sha256:e7236cf6d60adce33f36c958a8ce4b53a65befbe03379fdc9169af9b150fc964",
        ),
        (
            "cast_and_path",
            wait_condition!(ctx, "cast_and_path", || Ok((7_u8 as u32) < u32::MAX)),
            "sha256:1555c20f6cacde9a79c911a788114a3ae398a61e71d30f69e6315acf57f54b9b",
        ),
    ];
    for (name, call, expected) in cases {
        assert_eq!(
            call.options.predicate_identity, expected,
            "{name}: wait_condition! identity changed; inspect toolchain and module identity",
        );
    }
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn keys_and_timeouts_do_not_change_the_predicate_identity() {
    let ctx = workflow_context(Vec::new());
    let plain = wait_condition!(ctx, "first-key", || Ok(false));
    let timed = wait_condition!(
        ctx,
        "another-key",
        timeout: Duration::from_secs(17),
        || Ok(false),
    );
    assert_eq!(
        plain.options.predicate_identity,
        timed.options.predicate_identity
    );
    assert_ne!(plain.options.condition_key, timed.options.condition_key);
    assert_eq!(plain.options.timeout, None);
    assert_eq!(timed.options.timeout, Some(Duration::from_secs(17)));
}

mod original {
    use super::*;

    pub(super) fn automatic_identity() -> String {
        let ctx = workflow_context(Vec::new());
        wait_condition!(ctx, "ready", || Ok(false))
            .options
            .predicate_identity
    }

    pub(super) fn explicit_identity() -> String {
        let ctx = workflow_context(Vec::new());
        ctx.wait_condition(
            ConditionWaitOptions::new("ready", "approval-rule-v1"),
            || Ok(false),
        )
        .options
        .predicate_identity
    }
}

mod relocated {
    use super::*;

    pub(super) fn automatic_identity() -> String {
        let ctx = workflow_context(Vec::new());
        wait_condition!(ctx, "ready", || Ok(false))
            .options
            .predicate_identity
    }

    pub(super) fn explicit_identity() -> String {
        let ctx = workflow_context(Vec::new());
        ctx.wait_condition(
            ConditionWaitOptions::new("ready", "approval-rule-v1"),
            || Ok(false),
        )
        .options
        .predicate_identity
    }
}

#[test]
fn module_paths_change_macro_identity_but_not_explicit_identity() {
    assert_ne!(
        original::automatic_identity(),
        relocated::automatic_identity()
    );
    assert_eq!(
        original::explicit_identity(),
        relocated::explicit_identity()
    );
    assert_eq!(original::explicit_identity(), "approval-rule-v1");
}
