fn exact_prerelease_pin(content: &str) -> Option<&str> {
    content
        .split(|character: char| {
            character.is_whitespace()
                || matches!(
                    character,
                    '`' | '"' | '\'' | '(' | ')' | '[' | ']' | '<' | '>'
                )
        })
        .find(|token| {
            let token = token.trim_matches(|character: char| {
                !character.is_ascii_alphanumeric() && character != '.' && character != '-'
            });
            ["-alpha.", "-beta.", "-rc."].iter().any(|marker| {
                token.contains(marker)
                    && token.rsplit('.').next().is_some_and(|value| {
                        value.chars().all(|character| character.is_ascii_digit())
                    })
            })
        })
}

fn visible_html_text(content: &str) -> String {
    let mut visible = String::with_capacity(content.len());
    let mut in_tag = false;

    for character in content.chars() {
        match character {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                visible.push(' ');
            }
            _ if !in_tag => visible.push(character),
            _ => {}
        }
    }

    visible
}

#[test]
fn onboarding_uses_the_prerelease_channel() {
    for (path, content) in [
        ("README.md", include_str!("../README.md")),
        ("docs/index.html", include_str!("../docs/index.html")),
    ] {
        let visible_onboarding = if path.ends_with(".html") {
            visible_html_text(content)
        } else {
            content.to_owned()
        };

        assert_eq!(exact_prerelease_pin(&visible_onboarding), None, "{path}");
        assert!(
            visible_onboarding.contains("https://durable-workflow.com/install-sdk.sh"),
            "{path} must use the qualified versionless resolver"
        );
        assert!(
            !visible_onboarding.contains("cargo add durable-workflow@2.0.0-rc"),
            "{path} must not float to the newest published crate"
        );
    }
}
