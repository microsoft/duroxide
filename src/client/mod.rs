use std::sync::Arc;

use tracing::info;

use crate::_typed_codec::{Codec, Json};
use crate::providers::{
    DeleteInstanceResult, ExecutionInfo, InstanceFilter, InstanceInfo, InstanceTree, Provider, ProviderAdmin,
    ProviderError, PruneOptions, PruneResult, QueueDepths, SystemMetrics, WorkItem,
};
use crate::{EventKind, OrchestrationStatus};
use serde::Serialize;

/// Client-specific error type that wraps provider errors and adds client-specific errors.
///
/// This enum allows callers to distinguish between:
/// - Provider errors (storage failures, can be retryable or permanent)
/// - Client-specific errors (validation, capability not available, etc.)
#[derive(Debug, Clone)]
pub enum ClientError {
    /// Provider operation failed (wraps ProviderError)
    Provider(ProviderError),

    /// Management capability not available
    ManagementNotAvailable,

    /// Invalid input (client validation)
    InvalidInput { message: String },

    /// Operation timed out
    Timeout,

    /// Instance is still running (for delete without force)
    InstanceStillRunning { instance_id: String },

    /// Cannot delete a sub-orchestration directly (must delete root)
    CannotDeleteSubOrchestration { instance_id: String },

    /// Instance not found
    InstanceNotFound { instance_id: String },
}

impl ClientError {
    /// Check if this error is retryable (only applies to Provider errors)
    pub fn is_retryable(&self) -> bool {
        match self {
            ClientError::Provider(e) => e.is_retryable(),
            ClientError::ManagementNotAvailable => false,
            ClientError::InvalidInput { .. } => false,
            ClientError::Timeout => true,
            ClientError::InstanceStillRunning { .. } => false,
            ClientError::CannotDeleteSubOrchestration { .. } => false,
            ClientError::InstanceNotFound { .. } => false,
        }
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Provider(e) => write!(f, "{e}"),
            ClientError::ManagementNotAvailable => write!(
                f,
                "Management features not available - provider doesn't implement ProviderAdmin"
            ),
            ClientError::InvalidInput { message } => write!(f, "Invalid input: {message}"),
            ClientError::Timeout => write!(f, "Operation timed out"),
            ClientError::InstanceStillRunning { instance_id } => write!(
                f,
                "Instance {instance_id} is still running. Use force=true or cancel first."
            ),
            ClientError::CannotDeleteSubOrchestration { instance_id } => write!(
                f,
                "Cannot delete sub-orchestration {instance_id} directly. Delete the root orchestration instead."
            ),
            ClientError::InstanceNotFound { instance_id } => {
                write!(f, "Instance {instance_id} not found")
            }
        }
    }
}

impl std::error::Error for ClientError {}

impl From<ProviderError> for ClientError {
    fn from(e: ProviderError) -> Self {
        ClientError::Provider(e)
    }
}

// Constants for polling behavior in wait_for_orchestration
/// Initial delay between status polls (5ms)
const INITIAL_POLL_DELAY_MS: u64 = 5;

/// Maximum delay between status polls (100ms)
const MAX_POLL_DELAY_MS: u64 = 100;

/// Multiplier for exponential backoff
const POLL_DELAY_MULTIPLIER: u64 = 2;

/// Client for orchestration control-plane operations with automatic capability discovery.
///
/// The Client provides APIs for managing orchestration instances:
/// - Starting orchestrations
/// - Raising external events
/// - Cancelling instances
/// - Checking status
/// - Waiting for completion
/// - Rich management features (when available)
///
/// # Automatic Capability Discovery
///
/// The Client automatically discovers provider capabilities through the `Provider::as_management_capability()` method.
/// When a provider implements `ProviderAdmin`, rich management features become available:
///
/// ```ignore
/// let client = Client::new(provider);
///
/// // Control plane (always available)
/// client.start_orchestration("order-1", "ProcessOrder", "{}").await?;
///
/// // Management (automatically discovered)
/// if client.has_management_capability() {
///     let instances = client.list_all_instances().await?;
///     let metrics = client.get_system_metrics().await?;
/// } else {
///     println!("Management features not available");
/// }
/// ```
///
/// # Design
///
/// The Client communicates with the Runtime **only through the shared Provider** (no direct coupling).
/// This allows the Client to be used from any process, even one without a running Runtime.
///
/// # Thread Safety
///
/// Client is `Clone` and can be safely shared across threads.
///
/// # Example Usage
///
/// ```ignore
/// use duroxide::{Client, OrchestrationStatus};
/// use duroxide::providers::sqlite::SqliteProvider;
/// use std::sync::Arc;
///
/// use duroxide::ClientError;
/// let store = Arc::new(SqliteProvider::new("sqlite:./data.db").await?);
/// let client = Client::new(store);
///
/// // Start an orchestration
/// client.start_orchestration("order-123", "ProcessOrder", r#"{"customer_id": "c1"}"#).await?;
///
/// // Check status
/// let status = client.get_orchestration_status("order-123").await?;
/// println!("Status: {:?}", status);
///
/// // Wait for completion
/// let result = client.wait_for_orchestration("order-123", std::time::Duration::from_secs(30)).await.unwrap();
/// match result {
///     OrchestrationStatus::Completed { output, .. } => println!("Done: {}", output),
///     OrchestrationStatus::Failed { details, .. } => {
///         eprintln!("Failed ({}): {}", details.category(), details.display_message());
///     }
///     _ => {}
/// }
/// ```
pub struct Client {
    store: Arc<dyn Provider>,
}

impl Client {
    /// Create a client bound to a Provider instance.
    ///
    /// # Parameters
    ///
    /// * `store` - Arc-wrapped Provider (same instance used by Runtime)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let store = Arc::new(SqliteProvider::new("sqlite::memory:").await.unwrap());
    /// let client = Client::new(store.clone());
    /// // Multiple clients can share the same store
    /// let client2 = client.clone();
    /// ```
    pub fn new(store: Arc<dyn Provider>) -> Self {
        Self { store }
    }

    /// Start an orchestration instance with string input.
    ///
    /// # Parameters
    ///
    /// * `instance` - Unique instance ID (e.g., "order-123", "user-payment-456")
    /// * `orchestration` - Name of registered orchestration (e.g., "ProcessOrder")
    /// * `input` - JSON string input (will be passed to orchestration)
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Instance was enqueued for processing
    /// * `Err(msg)` - Failed to enqueue (storage error)
    ///
    /// # Behavior
    ///
    /// - Enqueues a StartOrchestration work item
    /// - Returns immediately (doesn't wait for orchestration to start/complete)
    /// - Use `wait_for_orchestration()` to wait for completion
    ///
    /// # Instance ID Requirements
    ///
    /// - Must be unique across all orchestrations
    /// - Can be any string (alphanumeric + hyphens recommended)
    /// - Reusing an instance ID that already exists will fail
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::{Client, ClientError};
    /// # use std::sync::Arc;
    /// # async fn example(client: Client) -> Result<(), ClientError> {
    /// // Start with JSON string input
    /// client.start_orchestration(
    ///     "order-123",
    ///     "ProcessOrder",
    ///     r#"{"customer_id": "c1", "items": ["item1", "item2"]}"#
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `ClientError::Provider` if the provider fails to enqueue the orchestration.
    pub async fn start_orchestration(
        &self,
        instance: impl Into<String>,
        orchestration: impl Into<String>,
        input: impl Into<String>,
    ) -> Result<(), ClientError> {
        let item = WorkItem::StartOrchestration {
            instance: instance.into(),
            orchestration: orchestration.into(),
            input: input.into(),
            version: None,
            parent_instance: None,
            parent_id: None,
            execution_id: crate::INITIAL_EXECUTION_ID,
        };
        self.store
            .enqueue_for_orchestrator(item, None)
            .await
            .map_err(ClientError::from)
    }

    /// Start an orchestration instance pinned to a specific version.
    ///
    /// # Errors
    ///
    /// Returns `ClientError::Provider` if the provider fails to enqueue the orchestration.
    pub async fn start_orchestration_versioned(
        &self,
        instance: impl Into<String>,
        orchestration: impl Into<String>,
        version: impl Into<String>,
        input: impl Into<String>,
    ) -> Result<(), ClientError> {
        let item = WorkItem::StartOrchestration {
            instance: instance.into(),
            orchestration: orchestration.into(),
            input: input.into(),
            version: Some(version.into()),
            parent_instance: None,
            parent_id: None,
            execution_id: crate::INITIAL_EXECUTION_ID,
        };
        self.store
            .enqueue_for_orchestrator(item, None)
            .await
            .map_err(ClientError::from)
    }

    // Note: No delayed scheduling API. Clients should use normal start APIs.

    /// Start an orchestration with typed input (serialized to JSON).
    ///
    /// # Errors
    ///
    /// Returns `ClientError::InvalidInput` if serialization fails.
    /// Returns `ClientError::Provider` if the provider fails to enqueue the orchestration.
    pub async fn start_orchestration_typed<In: Serialize>(
        &self,
        instance: impl Into<String>,
        orchestration: impl Into<String>,
        input: In,
    ) -> Result<(), ClientError> {
        let payload = Json::encode(&input).map_err(|e| ClientError::InvalidInput {
            message: format!("encode: {e}"),
        })?;
        self.start_orchestration(instance, orchestration, payload).await
    }

    /// Start a versioned orchestration with typed input (serialized to JSON).
    ///
    /// # Errors
    ///
    /// Returns `ClientError::InvalidInput` if serialization fails.
    /// Returns `ClientError::Provider` if the provider fails to enqueue the orchestration.
    pub async fn start_orchestration_versioned_typed<In: Serialize>(
        &self,
        instance: impl Into<String>,
        orchestration: impl Into<String>,
        version: impl Into<String>,
        input: In,
    ) -> Result<(), ClientError> {
        let payload = Json::encode(&input).map_err(|e| ClientError::InvalidInput {
            message: format!("encode: {e}"),
        })?;
        self.start_orchestration_versioned(instance, orchestration, version, payload)
            .await
    }

    /// Raise an external event into a running orchestration instance.
    ///
    /// # Purpose
    ///
    /// Send a signal/message to a running orchestration that is waiting for an external event.
    /// The orchestration must have called `ctx.schedule_wait(event_name)` to receive the event.
    ///
    /// # Parameters
    ///
    /// * `instance` - Instance ID of the running orchestration
    /// * `event_name` - Name of the event (must match `schedule_wait` name)
    /// * `data` - Payload data (JSON string, passed to orchestration)
    ///
    /// # Behavior
    ///
    /// - Enqueues ExternalRaised work item to orchestrator queue
    /// - If instance isn't waiting for this event (yet), it's buffered
    /// - Event is matched by NAME (not correlation ID)
    /// - Multiple events with same name can be raised
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::{Client, ClientError};
    /// # async fn example(client: Client) -> Result<(), ClientError> {
    /// // Orchestration waiting for approval
    /// // ctx.schedule_wait("ApprovalEvent").await
    ///
    /// // External system/human approves
    /// client.raise_event(
    ///     "order-123",
    ///     "ApprovalEvent",
    ///     r#"{"approved": true, "by": "manager@company.com"}"#
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Use Cases
    ///
    /// - Human approval workflows
    /// - Webhook callbacks
    /// - Inter-orchestration communication
    /// - External system integration
    ///
    /// # Error Cases
    ///
    /// - Instance doesn't exist: Event is buffered, orchestration processes when started
    /// - Instance already completed: Event is ignored gracefully
    ///
    /// # Errors
    ///
    /// Returns `ClientError::Provider` if the provider fails to enqueue the event.
    pub async fn raise_event(
        &self,
        instance: impl Into<String>,
        event_name: impl Into<String>,
        data: impl Into<String>,
    ) -> Result<(), ClientError> {
        let item = WorkItem::ExternalRaised {
            instance: instance.into(),
            name: event_name.into(),
            data: data.into(),
        };
        self.store
            .enqueue_for_orchestrator(item, None)
            .await
            .map_err(ClientError::from)
    }

    /// Raise a positional external event with typed data.
    ///
    /// Serializes `data` as JSON before sending. Same semantics as [`Self::raise_event`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the provider fails to enqueue the event.
    pub async fn raise_event_typed<T: serde::Serialize>(
        &self,
        instance: impl Into<String>,
        event_name: impl Into<String>,
        data: &T,
    ) -> Result<(), ClientError> {
        let payload = crate::_typed_codec::Json::encode(data).expect("Serialization should not fail");
        self.raise_event(instance, event_name, payload).await
    }

    /// Enqueue a message into a named queue for an orchestration instance.
    ///
    /// Queue messages use FIFO mailbox semantics:
    /// - Matched to [`OrchestrationContext::dequeue_event`] subscriptions in order
    /// - Stick around until consumed (even if no subscription exists yet)
    /// - Survive `continue_as_new` boundaries
    /// - Not affected by subscription cancellation
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the provider fails to enqueue the message.
    pub async fn enqueue_event(
        &self,
        instance: impl Into<String>,
        queue: impl Into<String>,
        data: impl Into<String>,
    ) -> Result<(), ClientError> {
        let item = WorkItem::QueueMessage {
            instance: instance.into(),
            name: queue.into(),
            data: data.into(),
        };
        self.store
            .enqueue_for_orchestrator(item, None)
            .await
            .map_err(ClientError::from)
    }

    /// Enqueue a typed message into a named queue for an orchestration instance.
    ///
    /// Serializes `data` as JSON before enqueuing. Same semantics as [`Self::enqueue_event`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the provider fails to enqueue the message.
    pub async fn enqueue_event_typed<T: serde::Serialize>(
        &self,
        instance: impl Into<String>,
        queue: impl Into<String>,
        data: &T,
    ) -> Result<(), ClientError> {
        let payload = crate::_typed_codec::Json::encode(data).expect("Serialization should not fail");
        self.enqueue_event(instance, queue, payload).await
    }

    /// Raise a persistent external event that uses mailbox semantics.
    ///
    /// Prefer [`Self::enqueue_event`] — this is a deprecated alias.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the provider fails to enqueue the event.
    #[deprecated(note = "Use enqueue_event() instead")]
    pub async fn raise_event_persistent(
        &self,
        instance: impl Into<String>,
        event_name: impl Into<String>,
        data: impl Into<String>,
    ) -> Result<(), ClientError> {
        self.enqueue_event(instance, event_name, data).await
    }

    /// V2: Raise an external event with topic-based pub/sub matching.
    ///
    /// Same as `raise_event`, but includes a `topic` for pub/sub matching.
    /// The orchestration must have called `ctx.schedule_wait2(name, topic)` to receive the event.
    /// Feature-gated for replay engine extensibility verification.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the provider fails to enqueue the event.
    #[cfg(feature = "replay-version-test")]
    pub async fn raise_event2(
        &self,
        instance: impl Into<String>,
        event_name: impl Into<String>,
        topic: impl Into<String>,
        data: impl Into<String>,
    ) -> Result<(), ClientError> {
        let item = WorkItem::ExternalRaised2 {
            instance: instance.into(),
            name: event_name.into(),
            topic: topic.into(),
            data: data.into(),
        };
        self.store
            .enqueue_for_orchestrator(item, None)
            .await
            .map_err(ClientError::from)
    }

    /// Request cancellation of an orchestration instance.
    ///
    /// # Purpose
    ///
    /// Gracefully cancel a running orchestration. The orchestration will complete its current
    /// turn and then fail deterministically with a "canceled: {reason}" error.
    ///
    /// # Parameters
    ///
    /// * `instance` - Instance ID to cancel
    /// * `reason` - Reason for cancellation (included in error message)
    ///
    /// # Behavior
    ///
    /// 1. Enqueues CancelInstance work item
    /// 2. Runtime appends OrchestrationCancelRequested event
    /// 3. Next turn, orchestration sees cancellation and fails deterministically
    /// 4. Final status: `OrchestrationStatus::Failed { details: Application::Cancelled }`
    ///
    /// # Deterministic Cancellation
    ///
    /// Cancellation is **deterministic** - the orchestration fails at a well-defined point:
    /// - Not mid-activity (activities complete)
    /// - Not mid-turn (current turn finishes)
    /// - Failure is recorded in history (replays consistently)
    ///
    /// # Propagation
    ///
    /// If the orchestration has child sub-orchestrations, they are also cancelled.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Cancel a long-running order
    /// client.cancel_instance("order-123", "Customer requested cancellation").await?;
    ///
    /// // Wait for cancellation to complete
    /// let status = client.wait_for_orchestration("order-123", std::time::Duration::from_secs(5)).await?;
    /// match status {
    ///     OrchestrationStatus::Failed { details, .. } if matches!(
    ///         details,
    ///         duroxide::ErrorDetails::Application {
    ///             kind: duroxide::AppErrorKind::Cancelled { .. },
    ///             ..
    ///         }
    ///     ) => {
    ///         println!("Successfully cancelled");
    ///     }
    ///     _ => {}
    /// }
    /// ```
    ///
    /// # Error Cases
    ///
    /// - Instance already completed: Cancellation is no-op
    /// - Instance doesn't exist: Cancellation is no-op
    ///
    /// # Errors
    ///
    /// Returns `ClientError::Provider` if the provider fails to enqueue the cancellation.
    pub async fn cancel_instance(
        &self,
        instance: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<(), ClientError> {
        let item = WorkItem::CancelInstance {
            instance: instance.into(),
            reason: reason.into(),
        };
        self.store
            .enqueue_for_orchestrator(item, None)
            .await
            .map_err(ClientError::from)
    }

    /// Get the current status of an orchestration by inspecting its history.
    ///
    /// # Purpose
    ///
    /// Query the current state of an orchestration instance without waiting.
    ///
    /// # Parameters
    ///
    /// * `instance` - Instance ID to query
    ///
    /// # Returns
    ///
    /// * `OrchestrationStatus::NotFound` - Instance doesn't exist
    /// * `OrchestrationStatus::Running` - Instance is still executing
    /// * `OrchestrationStatus::Completed { output, .. }` - Instance completed successfully
    /// * `OrchestrationStatus::Failed { error }` - Instance failed (includes cancellations)
    ///
    /// # Behavior
    ///
    /// - Reads instance history from provider
    /// - Scans for terminal events (Completed/Failed)
    /// - For multi-execution instances (ContinueAsNew), returns status of LATEST execution
    ///
    /// # Performance
    ///
    /// This method reads from storage (not cached). For polling, use `wait_for_orchestration` instead.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::{Client, ClientError, OrchestrationStatus};
    /// # async fn example(client: Client) -> Result<(), ClientError> {
    /// let status = client.get_orchestration_status("order-123").await?;
    ///
    /// match status {
    ///     OrchestrationStatus::NotFound => println!("Instance not found"),
    ///     OrchestrationStatus::Running { .. } => println!("Still processing"),
    ///     OrchestrationStatus::Completed { output, .. } => println!("Done: {}", output),
    ///     OrchestrationStatus::Failed { details, .. } => eprintln!("Error: {}", details.display_message()),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `ClientError::Provider` if the provider fails to read the orchestration history.
    pub async fn get_orchestration_status(&self, instance: &str) -> Result<OrchestrationStatus, ClientError> {
        let hist = self.store.read(instance).await.map_err(ClientError::from)?;

        // Query custom status (lightweight, always available)
        let (custom_status, custom_status_version) = match self.store.get_custom_status(instance, 0).await {
            Ok(Some((cs, v))) => (cs, v),
            Ok(None) => (None, 0),
            Err(_) => (None, 0), // Best-effort: don't fail status query for custom_status errors
        };

        // Find terminal events first
        for e in hist.iter().rev() {
            match &e.kind {
                EventKind::OrchestrationCompleted { output } => {
                    return Ok(OrchestrationStatus::Completed {
                        output: output.clone(),
                        custom_status,
                        custom_status_version,
                    });
                }
                EventKind::OrchestrationFailed { details } => {
                    return Ok(OrchestrationStatus::Failed {
                        details: details.clone(),
                        custom_status,
                        custom_status_version,
                    });
                }
                _ => {}
            }
        }
        // If we ever saw a start, it's running
        if hist
            .iter()
            .any(|e| matches!(&e.kind, EventKind::OrchestrationStarted { .. }))
        {
            Ok(OrchestrationStatus::Running {
                custom_status,
                custom_status_version,
            })
        } else {
            Ok(OrchestrationStatus::NotFound)
        }
    }

    /// Wait until terminal state or timeout using provider reads.
    ///
    /// # Purpose
    ///
    /// Poll for orchestration completion with exponential backoff, returning when terminal or timeout.
    ///
    /// # Parameters
    ///
    /// * `instance` - Instance ID to wait for
    /// * `timeout` - Maximum time to wait before returning timeout error
    ///
    /// # Returns
    ///
    /// * `Ok(OrchestrationStatus::Completed { output, .. })` - Orchestration completed successfully
    /// * `Ok(OrchestrationStatus::Failed { details, .. })` - Orchestration failed (includes cancellations)
    /// * `Err(ClientError::Timeout)` - Timeout elapsed while still Running
    /// * `Err(ClientError::Provider(e))` - Provider/Storage error
    ///
    /// **Note:** Never returns `NotFound` or `Running` - only terminal states or timeout.
    ///
    /// # Polling Behavior
    ///
    /// - First check: Immediate (no delay)
    /// - Subsequent checks: Exponential backoff starting at 5ms, doubling each iteration, max 100ms
    /// - Continues until terminal state or timeout
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Start orchestration
    /// client.start_orchestration("order-123", "ProcessOrder", "{}").await?;
    ///
    /// // Wait up to 30 seconds
    /// match client.wait_for_orchestration("order-123", std::time::Duration::from_secs(30)).await {
    ///     Ok(OrchestrationStatus::Completed { output, .. }) => {
    ///         println!("Success: {}", output);
    ///     }
    ///     Ok(OrchestrationStatus::Failed { details, .. }) => {
    ///         eprintln!("Failed ({}): {}", details.category(), details.display_message());
    ///     }
    ///     Err(ClientError::Timeout) => {
    ///         println!("Still running after 30s, instance: order-123");
    ///         // Instance is still running - can wait more or cancel
    ///     }
    ///     _ => unreachable!("wait_for_orchestration only returns terminal or timeout"),
    /// }
    /// ```
    ///
    /// # Use Cases
    ///
    /// - Synchronous request/response workflows
    /// - Testing (wait for workflow to complete)
    /// - CLI tools (block until done)
    /// - Health checks
    ///
    /// # For Long-Running Workflows
    ///
    /// Don't wait for hours/days:
    /// ```ignore
    /// // Start workflow
    /// client.start_orchestration("batch-job", "ProcessBatch", "{}").await.unwrap();
    ///
    /// // DON'T wait for hours
    /// // let status = client.wait_for_orchestration("batch-job", Duration::from_hours(24)).await;
    ///
    /// // DO poll periodically
    /// loop {
    ///     match client.get_orchestration_status("batch-job").await {
    ///         OrchestrationStatus::Completed { .. } => break,
    ///         OrchestrationStatus::Failed { .. } => break,
    ///         _ => tokio::time::sleep(std::time::Duration::from_secs(60)).await,
    ///     }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `ClientError::Provider` if the provider fails to read the orchestration status.
    /// Returns `ClientError::Timeout` if the orchestration doesn't complete within the timeout.
    pub async fn wait_for_orchestration(
        &self,
        instance: &str,
        timeout: std::time::Duration,
    ) -> Result<OrchestrationStatus, ClientError> {
        let deadline = std::time::Instant::now() + timeout;
        // quick path
        match self.get_orchestration_status(instance).await {
            Ok(s @ OrchestrationStatus::Completed { .. }) => return Ok(s),
            Ok(s @ OrchestrationStatus::Failed { .. }) => return Ok(s),
            Err(e) => return Err(e),
            _ => {}
        }
        // poll with backoff
        let mut delay_ms: u64 = INITIAL_POLL_DELAY_MS;
        while std::time::Instant::now() < deadline {
            match self.get_orchestration_status(instance).await {
                Ok(s @ OrchestrationStatus::Completed { .. }) => return Ok(s),
                Ok(s @ OrchestrationStatus::Failed { .. }) => return Ok(s),
                Err(e) => return Err(e),
                _ => {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms.saturating_mul(POLL_DELAY_MULTIPLIER)).min(MAX_POLL_DELAY_MS);
                }
            }
        }
        Err(ClientError::Timeout)
    }

    /// Typed wait helper: decodes output on Completed, returns Err(String) on Failed.
    ///
    /// # Errors
    ///
    /// Returns `ClientError::Provider` if the provider fails to read the orchestration status.
    /// Returns `ClientError::Timeout` if the orchestration doesn't complete within the timeout.
    /// Returns `ClientError::InvalidInput` if deserialization of the output fails.
    pub async fn wait_for_orchestration_typed<Out: serde::de::DeserializeOwned>(
        &self,
        instance: &str,
        timeout: std::time::Duration,
    ) -> Result<Result<Out, String>, ClientError> {
        match self.wait_for_orchestration(instance, timeout).await? {
            OrchestrationStatus::Completed { output, .. } => match Json::decode::<Out>(&output) {
                Ok(v) => Ok(Ok(v)),
                Err(e) => Err(ClientError::InvalidInput {
                    message: format!("decode failed: {e}"),
                }),
            },
            OrchestrationStatus::Failed { details, .. } => Ok(Err(details.display_message())),
            _ => unreachable!("wait_for_orchestration returns only terminal or timeout"),
        }
    }

    /// Wait for custom_status to change, polling the provider at `poll_interval`.
    ///
    /// Returns the full `OrchestrationStatus` when:
    /// - The `custom_status_version` exceeds `last_seen_version`
    /// - The orchestration reaches a terminal state (Completed/Failed)
    /// - The timeout elapses (returns `Err(ClientError::Timeout)`)
    ///
    /// # Parameters
    ///
    /// * `instance` - Instance ID to monitor
    /// * `last_seen_version` - The version the caller last observed (0 to get any status)
    /// * `poll_interval` - How often to check the provider
    /// * `timeout` - Maximum time to wait
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut version = 0u64;
    /// loop {
    ///     match client.wait_for_status_change("order-123", version, Duration::from_millis(200), Duration::from_secs(30)).await {
    ///         Ok(OrchestrationStatus::Running { custom_status, custom_status_version }) => {
    ///             println!("Progress: {:?}", custom_status);
    ///             version = custom_status_version;
    ///         }
    ///         Ok(OrchestrationStatus::Completed { output, .. }) => {
    ///             println!("Done: {output}");
    ///             break;
    ///         }
    ///         _ => break,
    ///     }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the provider fails or the instance doesn't exist.
    pub async fn wait_for_status_change(
        &self,
        instance: &str,
        last_seen_version: u64,
        poll_interval: std::time::Duration,
        timeout: std::time::Duration,
    ) -> Result<OrchestrationStatus, ClientError> {
        let deadline = std::time::Instant::now() + timeout;

        while std::time::Instant::now() < deadline {
            // Lightweight check: just custom_status + version
            match self.store.get_custom_status(instance, last_seen_version).await {
                Ok(Some(_)) => {
                    // Version changed — return full status
                    return self.get_orchestration_status(instance).await;
                }
                Ok(None) => {
                    // No change — check if terminal before sleeping
                    // (get_custom_status returns None if version hasn't changed,
                    //  but also if the instance doesn't exist or is terminal)
                }
                Err(e) => return Err(ClientError::from(e)),
            }

            // Also check for terminal state (in case orchestration completed
            // without ever updating custom_status)
            match self.get_orchestration_status(instance).await? {
                OrchestrationStatus::Running { .. } | OrchestrationStatus::NotFound => {
                    // Still running — sleep and retry
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    tokio::time::sleep(poll_interval.min(remaining)).await;
                }
                terminal => return Ok(terminal),
            }
        }

        Err(ClientError::Timeout)
    }

    // ===== Key-Value Store =====

    /// Read a single KV entry for a given instance.
    ///
    /// Reads directly from the provider's materialized `kv_store` table.
    /// Returns `Ok(None)` if the key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `ClientError` if the provider fails to read.
    pub async fn get_value(&self, instance: &str, key: &str) -> Result<Option<String>, ClientError> {
        self.store.get_kv_value(instance, key).await.map_err(ClientError::from)
    }

    /// Read a typed KV entry for a given instance.
    ///
    /// Deserializes the stored JSON value into `T`.
    /// Returns `Ok(None)` if the key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `ClientError` if the provider fails or deserialization fails.
    pub async fn get_value_typed<T: serde::de::DeserializeOwned>(
        &self,
        instance: &str,
        key: &str,
    ) -> Result<Option<T>, ClientError> {
        match self.get_value(instance, key).await? {
            None => Ok(None),
            Some(s) => serde_json::from_str(&s)
                .map(Some)
                .map_err(|e| ClientError::InvalidInput {
                    message: format!("KV deserialization error: {e}"),
                }),
        }
    }

    /// Poll a KV key until it becomes `Some`, or timeout.
    ///
    /// Uses exponential backoff (same as [`wait_for_orchestration`](Self::wait_for_orchestration)).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let value = client.wait_for_value(
    ///     "my-instance", "response:op-1",
    ///     Duration::from_secs(5),
    /// ).await?;
    /// println!("Got: {value}");
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `ClientError::Timeout` if the key is still `None` after `timeout`.
    /// Returns `ClientError` if a provider read fails.
    pub async fn wait_for_value(
        &self,
        instance: &str,
        key: &str,
        timeout: std::time::Duration,
    ) -> Result<String, ClientError> {
        let deadline = std::time::Instant::now() + timeout;
        // quick path
        if let Some(val) = self.get_value(instance, key).await? {
            return Ok(val);
        }
        // poll with backoff
        let mut delay_ms: u64 = INITIAL_POLL_DELAY_MS;
        while std::time::Instant::now() < deadline {
            if let Some(val) = self.get_value(instance, key).await? {
                return Ok(val);
            }
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            delay_ms = (delay_ms.saturating_mul(POLL_DELAY_MULTIPLIER)).min(MAX_POLL_DELAY_MS);
        }
        Err(ClientError::Timeout)
    }

    /// Poll a typed KV key until it becomes `Some`, or timeout.
    ///
    /// Same as [`wait_for_value`](Self::wait_for_value) but deserializes
    /// the JSON value into `T`.
    ///
    /// # Errors
    ///
    /// Returns `ClientError::Timeout` if the key is still `None` after `timeout`.
    /// Returns `ClientError` if a provider read or deserialization fails.
    pub async fn wait_for_value_typed<T: serde::de::DeserializeOwned>(
        &self,
        instance: &str,
        key: &str,
        timeout: std::time::Duration,
    ) -> Result<T, ClientError> {
        let raw = self.wait_for_value(instance, key, timeout).await?;
        serde_json::from_str(&raw).map_err(|e| ClientError::InvalidInput {
            message: format!("KV deserialization error: {e}"),
        })
    }

    // ===== Capability Discovery =====

    /// Check if management capabilities are available.
    ///
    /// # Returns
    ///
    /// `true` if the provider implements `ProviderAdmin`, `false` otherwise.
    ///
    /// # Usage
    ///
    /// ```ignore
    /// let client = Client::new(provider);
    /// if client.has_management_capability() {
    ///     let instances = client.list_all_instances().await?;
    /// } else {
    ///     println!("Management features not available");
    /// }
    /// ```
    pub fn has_management_capability(&self) -> bool {
        self.discover_management().is_ok()
    }

    /// Automatically discover management capabilities from the provider.
    ///
    /// # Returns
    ///
    /// `Ok(&dyn ManagementCapability)` if available, `Err(String)` if not.
    ///
    /// # Internal Use
    ///
    /// This method is used internally by management methods to access capabilities.
    fn discover_management(&self) -> Result<&dyn ProviderAdmin, ClientError> {
        self.store
            .as_management_capability()
            .ok_or(ClientError::ManagementNotAvailable)
    }

    // ===== Rich Management Methods =====

    /// List all orchestration instances.
    ///
    /// # Returns
    ///
    /// Vector of instance IDs, typically sorted by creation time (newest first).
    ///
    /// # Errors
    ///
    /// Returns `Err("Management features not available")` if the provider doesn't implement `ProviderAdmin`.
    ///
    /// # Usage
    ///
    /// ```ignore
    /// let client = Client::new(provider);
    /// if client.has_management_capability() {
    ///     let instances = client.list_all_instances().await?;
    ///     for instance in instances {
    ///         println!("Instance: {}", instance);
    ///     }
    /// }
    /// ```
    pub async fn list_all_instances(&self) -> Result<Vec<String>, ClientError> {
        self.discover_management()?
            .list_instances()
            .await
            .map_err(ClientError::from)
    }

    /// List instances matching a status filter.
    ///
    /// # Parameters
    ///
    /// * `status` - Filter by execution status: "Running", "Completed", "Failed", "ContinuedAsNew"
    ///
    /// # Returns
    ///
    /// Vector of instance IDs with the specified status.
    ///
    /// # Errors
    ///
    /// Returns `Err("Management features not available")` if the provider doesn't implement `ProviderAdmin`.
    ///
    /// # Usage
    ///
    /// ```ignore
    /// let client = Client::new(provider);
    /// if client.has_management_capability() {
    ///     let running = client.list_instances_by_status("Running").await?;
    ///     let completed = client.list_instances_by_status("Completed").await?;
    ///     println!("Running: {}, Completed: {}", running.len(), completed.len());
    /// }
    /// ```
    pub async fn list_instances_by_status(&self, status: &str) -> Result<Vec<String>, ClientError> {
        self.discover_management()?
            .list_instances_by_status(status)
            .await
            .map_err(ClientError::from)
    }

    /// Get comprehensive information about an instance.
    ///
    /// # Parameters
    ///
    /// * `instance` - The ID of the orchestration instance.
    ///
    /// # Returns
    ///
    /// Detailed instance information including status, output, and metadata.
    ///
    /// # Errors
    ///
    /// Returns `Err("Management features not available")` if the provider doesn't implement `ProviderAdmin`.
    ///
    /// # Usage
    ///
    /// ```ignore
    /// let client = Client::new(provider);
    /// if client.has_management_capability() {
    ///     let info = client.get_instance_info("order-123").await?;
    ///     println!("Instance {}: {} ({})", info.instance_id, info.orchestration_name, info.status);
    /// }
    /// ```
    pub async fn get_instance_info(&self, instance: &str) -> Result<InstanceInfo, ClientError> {
        self.discover_management()?
            .get_instance_info(instance)
            .await
            .map_err(ClientError::from)
    }

    /// Get detailed information about a specific execution.
    ///
    /// # Parameters
    ///
    /// * `instance` - The ID of the orchestration instance.
    /// * `execution_id` - The specific execution ID.
    ///
    /// # Returns
    ///
    /// Detailed execution information including status, output, and event count.
    ///
    /// # Errors
    ///
    /// Returns `Err("Management features not available")` if the provider doesn't implement `ProviderAdmin`.
    ///
    /// # Usage
    ///
    /// ```ignore
    /// let client = Client::new(provider);
    /// if client.has_management_capability() {
    ///     let info = client.get_execution_info("order-123", 1).await?;
    ///     println!("Execution {}: {} events, status: {}", info.execution_id, info.event_count, info.status);
    /// }
    /// ```
    pub async fn get_execution_info(&self, instance: &str, execution_id: u64) -> Result<ExecutionInfo, ClientError> {
        self.discover_management()?
            .get_execution_info(instance, execution_id)
            .await
            .map_err(ClientError::from)
    }

    /// List all execution IDs for an instance.
    ///
    /// Returns execution IDs in ascending order: \[1\], \[1, 2\], \[1, 2, 3\], etc.
    /// Each execution represents either the initial run or a continuation via ContinueAsNew.
    ///
    /// # Parameters
    ///
    /// * `instance` - The instance ID to query
    ///
    /// # Returns
    ///
    /// Vector of execution IDs in ascending order.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The provider doesn't support management capabilities
    /// - The database query fails
    ///
    /// # Usage
    ///
    /// ```ignore
    /// let client = Client::new(provider);
    /// if client.has_management_capability() {
    ///     let executions = client.list_executions("order-123").await?;
    ///     println!("Instance has {} executions", executions.len()); // [1, 2, 3]
    /// }
    /// ```
    pub async fn list_executions(&self, instance: &str) -> Result<Vec<u64>, ClientError> {
        let mgmt = self.discover_management()?;
        mgmt.list_executions(instance).await.map_err(ClientError::from)
    }

    /// Read the full event history for a specific execution within an instance.
    ///
    /// Returns all events for the specified execution in chronological order.
    /// Each execution has its own independent history starting from OrchestrationStarted.
    ///
    /// # Parameters
    ///
    /// * `instance` - The instance ID
    /// * `execution_id` - The specific execution ID (starts at 1)
    ///
    /// # Returns
    ///
    /// Vector of events in chronological order (oldest first).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The provider doesn't support management capabilities
    /// - The instance or execution doesn't exist
    /// - The database query fails
    ///
    /// # Usage
    ///
    /// ```ignore
    /// let client = Client::new(provider);
    /// if client.has_management_capability() {
    ///     let history = client.read_execution_history("order-123", 1).await?;
    ///     for event in history {
    ///         println!("Event: {:?}", event);
    ///     }
    /// }
    /// ```
    pub async fn read_execution_history(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<crate::Event>, ClientError> {
        let mgmt = self.discover_management()?;
        mgmt.read_history_with_execution_id(instance, execution_id)
            .await
            .map_err(ClientError::from)
    }

    /// Get system-wide metrics for the orchestration engine.
    ///
    /// # Returns
    ///
    /// System metrics including instance counts, execution counts, and status breakdown.
    ///
    /// # Errors
    ///
    /// Returns `Err("Management features not available")` if the provider doesn't implement `ProviderAdmin`.
    ///
    /// # Usage
    ///
    /// ```ignore
    /// let client = Client::new(provider);
    /// if client.has_management_capability() {
    ///     let metrics = client.get_system_metrics().await?;
    ///     println!("System health: {} running, {} completed, {} failed",
    ///         metrics.running_instances, metrics.completed_instances, metrics.failed_instances);
    /// }
    /// ```
    pub async fn get_system_metrics(&self) -> Result<SystemMetrics, ClientError> {
        self.discover_management()?
            .get_system_metrics()
            .await
            .map_err(ClientError::from)
    }

    /// Get the current depths of the internal work queues.
    ///
    /// # Returns
    ///
    /// Queue depths for orchestrator, worker, and timer queues.
    ///
    /// # Errors
    ///
    /// Returns `Err("Management features not available")` if the provider doesn't implement `ProviderAdmin`.
    ///
    /// # Usage
    ///
    /// ```ignore
    /// let client = Client::new(provider);
    /// if client.has_management_capability() {
    ///     let queues = client.get_queue_depths().await?;
    ///     println!("Queue depths - Orchestrator: {}, Worker: {}, Timer: {}",
    ///         queues.orchestrator_queue, queues.worker_queue, queues.timer_queue);
    /// }
    /// ```
    pub async fn get_queue_depths(&self) -> Result<QueueDepths, ClientError> {
        self.discover_management()?
            .get_queue_depths()
            .await
            .map_err(ClientError::from)
    }

    // ===== Hierarchy Operations =====

    /// Get the full instance tree rooted at the given instance.
    ///
    /// Returns all instances in the tree: the root, all children, grandchildren, etc.
    /// Useful for inspecting hierarchy before deletion, or for understanding
    /// sub-orchestration relationships.
    ///
    /// # Parameters
    ///
    /// * `instance_id` - The root instance ID to start from.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::InstanceNotFound`] if the instance doesn't exist.
    /// Returns [`ClientError::ProviderError`] for database/connection issues.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::{Client, ClientError};
    /// # async fn example(client: Client) -> Result<(), ClientError> {
    /// let tree = client.get_instance_tree("order-123").await?;
    /// println!("Will delete {} instances", tree.size());
    /// for id in &tree.all_ids {
    ///     println!("  - {}", id);
    /// }
    /// client.delete_instance("order-123", false).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_instance_tree(&self, instance_id: &str) -> Result<InstanceTree, ClientError> {
        let mgmt = self.discover_management()?;
        mgmt.get_instance_tree(instance_id).await.map_err(ClientError::from)
    }

    // ===== Deletion and Pruning Operations =====

    /// Delete an orchestration instance and all its associated data.
    ///
    /// This removes the instance, all executions, all history events, and any
    /// pending queue messages (orchestrator, worker, timer).
    ///
    /// # Parameters
    ///
    /// * `instance_id` - The ID of the instance to delete.
    /// * `force` - If true, delete even if the instance is in Running state.
    ///   WARNING: Force delete only removes database state; it does NOT cancel
    ///   in-flight tokio tasks. Use `cancel_instance` first for graceful termination.
    ///
    /// # Errors
    ///
    /// - [`ClientError::InstanceStillRunning`] - Instance is running and force=false.
    /// - [`ClientError::CannotDeleteSubOrchestration`] - Instance is a sub-orchestration.
    /// - [`ClientError::InstanceNotFound`] - Instance doesn't exist.
    /// - [`ClientError::ProviderError`] - Database/connection issues.
    ///
    /// # Cascading Delete
    ///
    /// If the instance is a root orchestration with sub-orchestrations, all descendants
    /// are included in the deletion (performed atomically in a single transaction).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::{Client, ClientError};
    /// # async fn example(client: Client) -> Result<(), ClientError> {
    /// // Delete a completed instance
    /// let result = client.delete_instance("order-123", false).await?;
    /// println!("Deleted {} events", result.events_deleted);
    ///
    /// // Graceful pattern: cancel first, then delete
    /// client.cancel_instance("workflow-456", "cleanup").await?;
    /// // Wait for cancellation to complete...
    /// client.delete_instance("workflow-456", false).await?;
    ///
    /// // Force delete an instance stuck in Running state
    /// client.delete_instance("stuck-workflow", true).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_instance(&self, instance_id: &str, force: bool) -> Result<DeleteInstanceResult, ClientError> {
        let mgmt = self.discover_management()?;
        let result = mgmt
            .delete_instance(instance_id, force)
            .await
            .map_err(|e| Self::translate_delete_error(e, instance_id))?;

        info!(
            instance_id = %instance_id,
            force = %force,
            instances_deleted = %result.instances_deleted,
            executions_deleted = %result.executions_deleted,
            events_deleted = %result.events_deleted,
            queue_messages_deleted = %result.queue_messages_deleted,
            "Deleted instance"
        );

        Ok(result)
    }

    /// Delete multiple orchestration instances matching the filter criteria.
    ///
    /// Only instances in terminal states (Completed, Failed) are eligible.
    /// Running instances are silently skipped (not an error).
    ///
    /// # Parameters
    ///
    /// * `filter` - Criteria for selecting instances to delete. All criteria are ANDed.
    ///
    /// # Filter Behavior
    ///
    /// - `instance_ids`: Allowlist of specific IDs to consider
    /// - `completed_before`: Only delete instances completed before this timestamp (ms since epoch)
    /// - `limit`: Maximum number of instances to delete (default: 1000)
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::ProviderError`] for database/connection issues.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::{Client, ClientError, InstanceFilter};
    /// # async fn example(client: Client, now_ms: u64) -> Result<(), ClientError> {
    /// // Delete specific instances
    /// let result = client.delete_instance_bulk(InstanceFilter {
    ///     instance_ids: Some(vec!["order-1".into(), "order-2".into()]),
    ///     ..Default::default()
    /// }).await?;
    ///
    /// // Delete by age (retention policy)
    /// let five_days_ago = now_ms - (5 * 24 * 60 * 60 * 1000);
    /// let result = client.delete_instance_bulk(InstanceFilter {
    ///     completed_before: Some(five_days_ago),
    ///     limit: Some(500),
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_instance_bulk(&self, filter: InstanceFilter) -> Result<DeleteInstanceResult, ClientError> {
        let result = self
            .discover_management()?
            .delete_instance_bulk(filter.clone())
            .await
            .map_err(ClientError::from)?;

        info!(
            filter = ?filter,
            instances_deleted = %result.instances_deleted,
            executions_deleted = %result.executions_deleted,
            events_deleted = %result.events_deleted,
            queue_messages_deleted = %result.queue_messages_deleted,
            "Bulk deleted instances"
        );

        Ok(result)
    }

    /// Prune old executions from a single long-running instance.
    ///
    /// Use this for `ContinueAsNew` workflows that accumulate many executions
    /// over time. The current (active) execution is never pruned.
    ///
    /// # Parameters
    ///
    /// * `instance_id` - The instance to prune executions from.
    /// * `options` - Criteria for selecting executions to delete. All criteria are ANDed.
    ///
    /// # Options Behavior
    ///
    /// - `keep_last`: Keep the last N executions by execution_id
    /// - `completed_before`: Only delete executions completed before this timestamp (ms)
    /// - Running executions are NEVER pruned
    /// - The current execution is NEVER pruned
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::ProviderError`] for database/connection issues.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::{Client, ClientError, PruneOptions};
    /// # async fn example(client: Client, now_ms: u64) -> Result<(), ClientError> {
    /// // Keep only the last 10 executions
    /// let result = client.prune_executions("eternal-workflow", PruneOptions {
    ///     keep_last: Some(10),
    ///     ..Default::default()
    /// }).await?;
    ///
    /// // Delete executions older than 30 days
    /// let thirty_days_ago = now_ms - (30 * 24 * 60 * 60 * 1000);
    /// let result = client.prune_executions("eternal-workflow", PruneOptions {
    ///     completed_before: Some(thirty_days_ago),
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn prune_executions(&self, instance_id: &str, options: PruneOptions) -> Result<PruneResult, ClientError> {
        let result = self
            .discover_management()?
            .prune_executions(instance_id, options.clone())
            .await
            .map_err(ClientError::from)?;

        info!(
            instance_id = %instance_id,
            options = ?options,
            executions_deleted = %result.executions_deleted,
            events_deleted = %result.events_deleted,
            instances_processed = %result.instances_processed,
            "Pruned executions"
        );

        Ok(result)
    }

    /// Prune old executions from multiple instances matching the filter.
    ///
    /// Applies the same prune options to all matching instances.
    ///
    /// # Parameters
    ///
    /// * `filter` - Criteria for selecting instances to process.
    /// * `options` - Criteria for selecting executions to delete within each instance.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::ProviderError`] for database/connection issues.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duroxide::{Client, ClientError, InstanceFilter, PruneOptions};
    /// # async fn example(client: Client) -> Result<(), ClientError> {
    /// // Prune all terminal instances: keep last 10 executions each
    /// let result = client.prune_executions_bulk(
    ///     InstanceFilter { limit: Some(100), ..Default::default() },
    ///     PruneOptions { keep_last: Some(10), ..Default::default() },
    /// ).await?;
    /// println!("Pruned {} executions across {} instances",
    ///     result.executions_deleted, result.instances_processed);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn prune_executions_bulk(
        &self,
        filter: InstanceFilter,
        options: PruneOptions,
    ) -> Result<PruneResult, ClientError> {
        let result = self
            .discover_management()?
            .prune_executions_bulk(filter.clone(), options.clone())
            .await
            .map_err(ClientError::from)?;

        info!(
            filter = ?filter,
            options = ?options,
            executions_deleted = %result.executions_deleted,
            events_deleted = %result.events_deleted,
            instances_processed = %result.instances_processed,
            "Bulk pruned executions"
        );

        Ok(result)
    }

    /// Translate provider errors into more specific client errors for delete operations.
    fn translate_delete_error(e: ProviderError, instance_id: &str) -> ClientError {
        let msg = e.to_string().to_lowercase();
        if msg.contains("not found") {
            ClientError::InstanceNotFound {
                instance_id: instance_id.to_string(),
            }
        } else if msg.contains("still running") {
            ClientError::InstanceStillRunning {
                instance_id: instance_id.to_string(),
            }
        } else if msg.contains("sub-orchestration") || msg.contains("delete the root") {
            ClientError::CannotDeleteSubOrchestration {
                instance_id: instance_id.to_string(),
            }
        } else {
            ClientError::Provider(e)
        }
    }
}
