# Static credential configuration fixture

`static-credentials-v1.json` is copied byte for byte from
[`durable-workflow/server` at `23972b1d12fcaa64788480eda0a31de7a5347422`](https://github.com/durable-workflow/server/blob/23972b1d12fcaa64788480eda0a31de7a5347422/tests/Fixtures/AuthConfiguration/static-credentials-v1.json).
It has 60,716 bytes and SHA-256
`0d4f01b763d72850943acbd9a74a0cc5da1ca6db5bc1b6fa5ca37fda11810acb`.
Server issue [#554](https://github.com/durable-workflow/server/issues/554)
records the shared policy and exact-row review.

The Rust consumer applies the 286 common scalar rows to each typed token option
and the three pre-selection rows to client construction. It preserves the Rust
mapping for `None`, supplied empty strings and role selection. PHP byte-loader,
Server setting and decoded binary key rows are outside the Rust token surface.
All strings are synthetic. Passing these checks establishes constructor/text
behavior, without making an authentication or HTTP-header acceptance claim.
