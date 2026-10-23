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
}
