use super::*;
use sha2::{Digest, Sha256};

const FIXTURE: &[u8] =
    include_bytes!("../../tests/fixtures/auth-configuration/static-credentials-v1.json");

fn fixture() -> Value {
    serde_json::from_slice(FIXTURE).unwrap()
}

fn text(points: &Value) -> String {
    points
        .as_array()
        .unwrap()
        .iter()
        .map(|point| char::from_u32(point.as_u64().unwrap().try_into().unwrap()).unwrap())
        .collect()
}

fn configure(builder: ClientBuilder, option: &str, value: String) -> ClientBuilder {
    match option {
        "token" => builder.token(Some(value)),
        "control_token" => builder.control_token(Some(value)),
        "worker_token" => builder.worker_token(Some(value)),
        _ => panic!("unknown fixture option"),
    }
}

fn invalid(result: Result<Client>, option: &str) {
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("malformed {option} was accepted"),
    };
    assert_eq!(
        error.to_string(),
        format!("invalid static credential option {option}: leading or trailing Unicode whitespace or control character")
    );
    assert_eq!(
        format!("{error:?}"),
        format!("InvalidStaticCredential {{ option: {option:?} }}")
    );
}

#[test]
fn frozen_static_credential_fixture_identity_and_rust_mapping() {
    assert_eq!(FIXTURE.len(), 60_716);
    assert_eq!(
        format!("{:x}", Sha256::digest(FIXTURE)),
        "0d4f01b763d72850943acbd9a74a0cc5da1ca6db5bc1b6fa5ca37fda11810acb"
    );
    let fixture = fixture();
    assert_eq!(
        fixture["language_mappings"]["rust"],
        json!({
            "logical_setting_names": {"shared": "token", "control": "control_token", "worker": "worker_token"},
            "none": "existing-absence",
            "some_literal_empty": "preserve-supplied-empty-and-fallback-precedence",
            "opposite_only": "existing-role-refusal"
        })
    );
}

#[test]
fn static_credential_common_scalar_vectors_validate_every_option() {
    let fixture = fixture();
    let rows = fixture["common_scalar_cases"].as_array().unwrap();
    assert_eq!(rows.len(), 286);
    for row in rows {
        let value = text(&row["input_code_points"]);
        for option in ["token", "control_token", "worker_token"] {
            let result = configure(Client::builder("http://127.0.0.1:1"), option, value.clone()).build();
            match row["expected"].as_str().unwrap() {
                "preserve" => {
                    let client = result.unwrap();
                    let protocol = if option == "worker_token" {
                        RequestProtocol::DefaultWorker
                    } else {
                        RequestProtocol::ControlPlane
                    };
                    let selected = client.auth_token(protocol).unwrap().unwrap();
                    let hex: String = selected.bytes().map(|byte| format!("{byte:02x}")).collect();
                    assert_eq!(hex, row["expected_utf8_hex"].as_str().unwrap());
                    assert_eq!(selected, value);
                }
                "reject" => invalid(result, option),
                _ => panic!("unknown fixture verdict"),
            }
        }
    }
}

#[test]
fn static_credential_unused_and_masked_options_refuse_before_selection() {
    let fixture = fixture();
    let rows = fixture["pre_selection_cases"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    let names = &fixture["language_mappings"]["rust"]["logical_setting_names"];
    for row in rows {
        let mut builder = Client::builder("http://127.0.0.1:1");
        for (logical, points) in row["options_code_points"].as_object().unwrap() {
            builder = configure(builder, names[logical].as_str().unwrap(), text(points));
        }
        let offending = row["offending_option"].as_str().unwrap();
        invalid(builder.build(), names[offending].as_str().unwrap());
    }
}

#[test]
fn static_credential_refusal_precedes_other_client_construction_errors() {
    for option in ["token", "control_token", "worker_token"] {
        invalid(
            configure(
                Client::builder("http://127.0.0.1:1/api"),
                option,
                "fixture-value\n".to_owned(),
            )
            .build(),
            option,
        );
    }
}

#[test]
fn static_credential_absence_empty_and_role_precedence_stay_distinct() {
    let anonymous = Client::builder("http://127.0.0.1:1").build().unwrap();
    assert_eq!(anonymous.auth_token(RequestProtocol::ControlPlane).unwrap(), None);
    assert_eq!(anonymous.auth_token(RequestProtocol::DefaultWorker).unwrap(), None);
    for shared in ["fixture-shared", ""] {
        let client = Client::builder("http://127.0.0.1:1")
            .token(Some(shared.into()))
            .control_token(None)
            .worker_token(None)
            .build()
            .unwrap();
        assert_eq!(client.auth_token(RequestProtocol::ControlPlane).unwrap(), Some(shared));
        assert_eq!(client.auth_token(RequestProtocol::DefaultWorker).unwrap(), Some(shared));
    }
    for scoped in ["fixture-scoped", ""] {
        let client = Client::builder("http://127.0.0.1:1")
            .token(Some("fixture-shared".into()))
            .control_token(Some(scoped.into()))
            .worker_token(Some(scoped.into()))
            .build()
            .unwrap();
        assert_eq!(client.auth_token(RequestProtocol::ControlPlane).unwrap(), Some(scoped));
        assert_eq!(client.auth_token(RequestProtocol::DefaultWorker).unwrap(), Some(scoped));
    }
    let control_only = Client::builder("http://127.0.0.1:1")
        .control_token(Some("fixture-control".into()))
        .build()
        .unwrap();
    assert!(matches!(
        control_only.auth_token(RequestProtocol::DefaultWorker),
        Err(Error::MissingRoleCredentials { role: "worker", .. })
    ));
    let worker_only = Client::builder("http://127.0.0.1:1")
        .worker_token(Some("fixture-worker".into()))
        .build()
        .unwrap();
    assert!(matches!(
        worker_only.auth_token(RequestProtocol::ControlPlane),
        Err(Error::MissingRoleCredentials { role: "control", .. })
    ));
}
