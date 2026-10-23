These synthetic certificates are used only by the localhost HTTPS fixture in
`tests/private_ca.rs`. The leaf is valid for `localhost`, excluding IP addresses
so the same fixture can verify hostname refusal. The CA signing key was discarded.

`server-key.der` is the fixture leaf's PKCS#8 private key. The certificate and
key must be replaced together when regenerating this fixture.
