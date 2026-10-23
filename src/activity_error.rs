use crate::Error;

/// An application failure returned by an activity handler.
///
/// The exception type is matched by [`crate::ActivityRetryPolicy::non_retryable_error_types()`].
/// Local and remote activities preserve the same type, message and retry marker.
/// Convert this error into [`enum@Error`] with `.into()` when returning it from a handler.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("{message}")]
pub struct ActivityError {
    pub exception_type: String,
    pub message: String,
    pub non_retryable: bool,
}

impl ActivityError {
    /// Create a retryable application failure with a stable application-defined type.
    pub fn new(exception_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            exception_type: exception_type.into(),
            message: message.into(),
            non_retryable: false,
        }
    }

    /// Stop retries regardless of the activity's retry policy when set to `true`.
    pub fn non_retryable(mut self, non_retryable: bool) -> Self {
        self.non_retryable = non_retryable;
        self
    }

    pub(crate) fn from_error(error: &Error, fallback_type: &str) -> Self {
        match error {
            Error::ActivityError(failure) => failure.clone(),
            Error::ActivityFailed(failure) => Self::new(
                failure.exception_type.as_deref().unwrap_or(fallback_type),
                &failure.message,
            )
            .non_retryable(failure.non_retryable),
            Error::ActivityNotRegistered(_) => {
                Self::new("ActivityNotRegistered", error.to_string()).non_retryable(true)
            }
            Error::Codec(_) | Error::HandlerType { .. } => {
                Self::new("ActivityCodecError", error.to_string()).non_retryable(true)
            }
            _ => Self::new(fallback_type, error.to_string()),
        }
        .validated()
    }

    // The runtime bounds exception types. Publish a terminal diagnostic rather
    // than losing a leased task to an invalid failure report.
    pub(crate) fn validated(&self) -> Self {
        if self.exception_type.trim().is_empty() || self.exception_type.len() > 255 {
            Self::new(
                "ActivityFailureMetadataError",
                "activity exception type must contain 1 to 255 bytes and not be blank",
            )
            .non_retryable(true)
        } else {
            self.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_activity_errors_preserve_application_metadata_in_both_execution_modes() {
        for non_retryable in [false, true] {
            let error: Error = ActivityError::new("PaymentDeclined", "card declined")
                .non_retryable(non_retryable)
                .into();
            for fallback in ["RustActivityFailure", "RustActivityError"] {
                let failure = ActivityError::from_error(&error, fallback);
                assert_eq!(failure.exception_type, "PaymentDeclined");
                assert_eq!(failure.message, "card declined");
                assert_eq!(failure.non_retryable, non_retryable);
            }
        }
    }

    #[test]
    fn invalid_activity_error_metadata_becomes_a_terminal_failure() {
        for exception_type in [String::new(), " \t".into(), "x".repeat(256)] {
            let failure = ActivityError::new(exception_type, "original message").validated();
            assert_eq!(failure.exception_type, "ActivityFailureMetadataError");
            assert!(failure.non_retryable);
        }
        assert_eq!(
            ActivityError::new("x".repeat(255), "message")
                .validated()
                .exception_type
                .len(),
            255
        );
    }
}
