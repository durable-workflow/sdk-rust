# One Identity built-page review

The screenshots bind source `863a1b1ee9fc67bc56b9a4844fc254fd1ab773a1` to the published SDK 4.12.0 API. [Checks and input hashes](checks.json) record the exact identities.

The starting build is [the normal stable documentation check](https://github.com/durable-workflow/sdk-rust/actions/runs/42120452486), artifact `13457709462`. Its API/examples come from published source `4b2440a5618ccc1c98960ea2e1e62127042c3590`. Only current `layout.css` and the final canonical shared status module were overlaid for the affected preview; the generated API content was preserved. The following evidence commit adds these review images and the generated social-card PNG without changing the checked HTML, CSS or JavaScript.

The optional browser check passed 126 assertions in Chrome 156.0.8078.80 at 1440, 1024, 390 and 320 pixels in both themes. It covers light default despite a dark OS, explicit theme persistence, all three local fonts, page overflow, action and API command contrast, API search/sidebar, SDK keyboard dismissal/focus, clipboard content, status states/expiry and resource errors. The browser, profile and loopback server were cleaned up and independently absent afterward.

**The footer screenshots use synthetic unknown status.** Functional status cases use synthetic operational/degraded/incident/maintenance/stale/unknown/unavailable responses. These images do not establish live service health or cross-origin acceptance. Existing generated API badges are public image responses and were not replaced with fixtures.

| Page | Width | Light | Dark |
| --- | --- | --- | --- |
| Landing | 1440 | [Screenshot](landing-1440-light.png) | [Screenshot](landing-1440-dark.png) |
| Landing | 390 | [Screenshot](landing-390-light.png) | [Screenshot](landing-390-dark.png) |
| API reference | 1440 | [Screenshot](api-1440-light.png) | [Screenshot](api-1440-dark.png) |
| API reference | 390 | [Screenshot](api-390-light.png) | [Screenshot](api-390-dark.png) |

The social card is rendered from [the editable SVG](../../assets/social-card.svg) with the unchanged canonical logo and licensed local fonts. It is [1200 × 630 pixels](../../assets/social-card.png).

Run the optional check using the [documentation preview instructions](../../README.md). Normal current-head required checks and the actual protected Pages deployment/live receipt are recorded on [PR #217](https://github.com/durable-workflow/sdk-rust/pull/217).
