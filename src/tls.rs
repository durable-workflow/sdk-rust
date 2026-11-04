use crate::{ClientBuilder, Error, Result};

impl ClientBuilder {
    /// Trust additional PEM root certificates, preserving the bundled public roots.
    ///
    /// Accepts one or more `CERTIFICATE` entries, and can be called repeatedly.
    /// Invalid PEM and bundles without certificates are rejected. Certificate
    /// chains and hostnames are still verified for every request, including
    /// worker polling and external payload transfers.
    pub fn additional_root_certificates_pem(mut self, pem: impl AsRef<[u8]>) -> Result<Self> {
        let certificates = reqwest::Certificate::from_pem_bundle(pem.as_ref())
            .map_err(|error| Error::InvalidRootCertificates(error.to_string()))?;
        if certificates.is_empty() {
            return Err(Error::InvalidRootCertificates(
                "PEM bundle must contain at least one CERTIFICATE entry".into(),
            ));
        }
        self.additional_root_certificates.extend(certificates);
        Ok(self)
    }

    /// Disable TLS certificate and hostname verification for every request
    /// this client makes, including worker polling and external payload
    /// transfers. The default is `false` (verified).
    ///
    /// Any party on the network path can then read and change traffic,
    /// including bearer tokens. Prefer
    /// [`ClientBuilder::additional_root_certificates_pem`] for a private CA.
    pub fn danger_accept_invalid_certs(mut self, accept: bool) -> Self {
        self.danger_accept_invalid_certs = accept;
        self
    }
}
