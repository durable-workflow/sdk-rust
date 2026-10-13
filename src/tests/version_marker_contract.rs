use super::*;

#[test]
fn numeric_zero_remains_frozen_across_patch_and_deprecation_helpers() {
    let ctx = workflow_context(Vec::new());
    assert_eq!(ctx.get_version("frozen-zero", -7, 0).unwrap(), 0);
    assert!(!ctx.patched("frozen-zero").unwrap());
    ctx.deprecate_patch("frozen-zero").unwrap();
    assert_eq!(ctx.get_version("frozen-zero", 0, 10).unwrap(), 0);
    assert!(!ctx.patched("frozen-zero").unwrap());
    assert!(matches!(ctx.get_version("frozen-zero", 1, 10),
        Err(Error::NonDeterministicReplay(ReplayFailure { ref reason, .. }))
            if reason == "version_marker_incompatible_range"));
    assert_eq!(
        ctx.get_version("frozen-zero", i32::MIN, i32::MAX).unwrap(),
        0
    );
    assert_eq!(
        ctx.take_commands().unwrap(),
        vec![json!({
            "type":"record_version_marker", "change_id":"frozen-zero", "version":0,
            "min_supported":-7, "max_supported":0,
        })]
    );
}

#[test]
fn deprecation_first_preserves_aliases_with_different_supported_ranges() {
    let mut first = retained_patch_marker(1, 1, "2026-10-13T03:00:00.123456Z");
    first.payload["min_supported"] = json!(0);
    let mut second = retained_patch_marker(2, 1, "2026-10-13T03:00:00.234567Z");
    second.payload["min_supported"] = json!(1);
    second.payload["max_supported"] = json!(7);
    let history = vec![first, second];
    for _cold_reader in 0..2 {
        let ctx = workflow_context(history.clone());
        ctx.deprecate_patch("retained-patch").unwrap();
        assert_eq!(ctx.state.lock().unwrap().command_cursor, 1);
        assert!(ctx.patched("retained-patch").unwrap());
        assert_eq!(ctx.state.lock().unwrap().command_cursor, 2);
        assert_eq!(ctx.get_version("retained-patch", 0, 10).unwrap(), 1);
        ctx.ensure_history_consumed().unwrap();
        assert!(ctx.take_commands().unwrap().is_empty());
        let state = ctx.state.lock().unwrap();
        for (actual, original) in state.history_events.iter().zip(&history) {
            assert_eq!(actual.payload, original.payload);
            assert_eq!(actual.raw, original.raw);
        }
    }
}

#[test]
fn arbitrary_ranges_freeze_one_decision_and_validate_every_current_range() {
    for (min, max) in [(-3, 7), (-8, -4), (i32::MIN, i32::MAX)] {
        let ctx = workflow_context(Vec::new());
        assert_eq!(ctx.get_version("arbitrary", min, max).unwrap(), max);
        assert_eq!(ctx.get_version("arbitrary", max, max).unwrap(), max);
        assert!(matches!(ctx.get_version("arbitrary", min, max - 1),
            Err(Error::NonDeterministicReplay(ReplayFailure { ref reason, .. }))
                if reason == "version_marker_incompatible_range"));
        assert!(matches!(ctx.patched("arbitrary"),
            Err(Error::NonDeterministicReplay(ReplayFailure { ref reason, .. }))
                if reason == "version_marker_incompatible_range"));
        assert!(matches!(ctx.deprecate_patch("arbitrary"),
            Err(Error::NonDeterministicReplay(ReplayFailure { ref reason, .. }))
                if reason == "version_marker_incompatible_range"));
        assert_eq!(
            ctx.get_version("arbitrary", i32::MIN, i32::MAX).unwrap(),
            max
        );
        assert_eq!(
            ctx.take_commands().unwrap(),
            vec![json!({
                "type":"record_version_marker", "change_id":"arbitrary", "version":max,
                "min_supported":min, "max_supported":max,
            })]
        );
    }
}
