// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

/// Provider-specific error with retry classification
///
/// Providers return this error type to indicate whether an error should be retried.
/// The runtime uses `is_retryable()` to decide whether to retry the operation.
///
/// # Error Classification
///
/// **Retryable (is_retryable = true)**:
/// - Database busy/locked
/// - Connection timeouts
/// - Network failures
/// - Temporary resource exhaustion
///
/// **Non-retryable (is_retryable = false)**:
/// - Data corruption (missing instance, invalid format)
/// - Duplicate events (indicates bug)
/// - Invalid input (malformed work item)
/// - Configuration errors
/// - Invalid lock tokens (idempotent - already processed)
///
/// # Example Usage
///
/// ```rust,no_run
/// use duroxide::providers::ProviderError;
///
/// // Transient error - retryable
/// # fn example() -> Result<(), ProviderError> {
/// return Err(ProviderError::retryable("ack_orchestration_item", "Database is busy"));
/// # }
///
/// // Permanent error - not retryable
/// # fn example2() -> Result<(), ProviderError> {
/// return Err(ProviderError::permanent("ack_orchestration_item", "Duplicate event detected"));
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderError {
    /// Operation that failed (e.g., "ack_orchestration_item", "fetch_orchestration_item")
    pub operation: String,
    /// Human-readable error message
    pub message: String,
    /// Whether this error should be retried
    pub retryable: bool,
}

impl ProviderError {
    /// Create a retryable (transient) error
    ///
    /// Use for errors that might succeed on retry:
    /// - Database busy/locked
    /// - Connection timeouts
    /// - Network failures
    pub fn retryable(operation: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            operation: operation.into(),
            message: message.into(),
            retryable: true,
        }
    }

    /// Create a non-retryable (permanent) error
    ///
    /// Use for errors that won't succeed on retry:
    /// - Data corruption
    /// - Duplicate events
    /// - Invalid input
    /// - Configuration errors
    /// - Invalid lock tokens (idempotent)
    pub fn permanent(operation: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            operation: operation.into(),
            message: message.into(),
            retryable: false,
        }
    }

    /// Check if error is retryable
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    /// Convert to ErrorDetails::Infrastructure for runtime
    pub fn to_infrastructure_error(&self) -> crate::ErrorDetails {
        crate::ErrorDetails::Infrastructure {
            operation: self.operation.clone(),
            message: self.message.clone(),
            retryable: self.retryable,
        }
    }
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.operation, self.message)
    }
}

impl std::error::Error for ProviderError {}

/// Conversion from String for backward compatibility
impl From<String> for ProviderError {
    /// Convert String error to retryable ProviderError
    ///
    /// This allows existing code that returns `Err(String)` to work.
    /// By default, String errors are treated as retryable (conservative approach).
    fn from(s: String) -> Self {
        Self {
            operation: "unknown".to_string(),
            message: s,
            retryable: true, // Default to retryable for backward compatibility
        }
    }
}

impl From<&str> for ProviderError {
    fn from(s: &str) -> Self {
        s.to_string().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test: ProviderError retryable vs permanent classification
    #[test]
    fn test_provider_error_classification() {
        // Test retryable error
        let retryable = ProviderError::retryable("fetch_orchestration_item", "Database is busy");
        assert!(retryable.is_retryable(), "Retryable error should be retryable");
        assert_eq!(retryable.operation, "fetch_orchestration_item");
        assert!(retryable.message.contains("busy"));

        // Test permanent error
        let permanent = ProviderError::permanent("ack_orchestration_item", "Duplicate event detected");
        assert!(!permanent.is_retryable(), "Permanent error should not be retryable");
        assert_eq!(permanent.operation, "ack_orchestration_item");
        assert!(permanent.message.contains("Duplicate"));

        // Test Display trait
        let display = format!("{permanent}");
        assert!(display.contains("ack_orchestration_item"));
        assert!(display.contains("Duplicate"));

        // Test Error trait
        let _err: Box<dyn std::error::Error> = Box::new(permanent.clone());
    }

    /// Test: ProviderError conversion from String (backward compatibility)
    #[test]
    fn test_provider_error_from_string() {
        // String errors should be retryable by default (conservative approach)
        let from_string: ProviderError = "Some error message".into();
        assert!(
            from_string.is_retryable(),
            "String errors should be retryable by default"
        );
        assert_eq!(from_string.operation, "unknown");
        assert_eq!(from_string.message, "Some error message");

        // From owned String
        let from_owned: ProviderError = String::from("Another error").into();
        assert!(from_owned.is_retryable());
        assert_eq!(from_owned.message, "Another error");
    }

    /// Test: ProviderError to_infrastructure_error conversion
    #[test]
    fn test_provider_error_to_infrastructure() {
        let retryable = ProviderError::retryable("read", "Connection timeout");
        let infra = retryable.to_infrastructure_error();

        match infra {
            crate::ErrorDetails::Infrastructure {
                operation,
                message,
                retryable,
            } => {
                assert_eq!(operation, "read");
                assert!(message.contains("timeout"));
                assert!(retryable);
            }
            _ => panic!("Expected Infrastructure error"),
        }

        let permanent = ProviderError::permanent("write", "Data corruption");
        let infra = permanent.to_infrastructure_error();

        match infra {
            crate::ErrorDetails::Infrastructure {
                operation,
                message,
                retryable,
            } => {
                assert_eq!(operation, "write");
                assert!(message.contains("corruption"));
                assert!(!retryable);
            }
            _ => panic!("Expected Infrastructure error"),
        }
    }

    /// Test: ProviderError equality
    #[test]
    fn test_provider_error_equality() {
        let err1 = ProviderError::retryable("op", "msg");
        let err2 = ProviderError::retryable("op", "msg");
        let err3 = ProviderError::permanent("op", "msg");

        assert_eq!(err1, err2);
        assert_ne!(err1, err3); // Different retryable flag
    }
}
