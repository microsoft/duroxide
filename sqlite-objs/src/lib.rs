// SQLite-objs provider: Azure Storage-backed SQLite via sqlite-objs VFS
// Uses rusqlite (synchronous) with tokio::task::spawn_blocking for async wrapping
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(unexpected_cfgs)]

use rusqlite::{params, Connection, OpenFlags};
use sqlite_objs::SqliteObjsVfs;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::debug;

use duroxide::providers::{
    DeleteInstanceResult, DispatcherCapabilityFilter, ExecutionInfo, InstanceFilter, InstanceInfo, OrchestrationItem,
    Provider, ProviderAdmin, ProviderError, PruneOptions, PruneResult, QueueDepths, ScheduledActivityIdentifier,
    SessionFetchConfig, SystemMetrics, TagFilter, WorkItem,
};
use duroxide::providers::ExecutionMetadata;
use duroxide::{Event, EventKind};

/// Default limit for bulk operations when not specified by caller
const DEFAULT_BULK_OPERATION_LIMIT: u32 = 1000;

/// Configuration options for SqliteObjsProvider
#[derive(Debug, Clone)]
pub struct SqliteObjsOptions {
    /// Azure storage account name
    pub azure_account: String,
    /// Azure storage container name
    pub azure_container: String,
    /// Azure SAS token (URL-encoded)
    pub azure_sas: String,
    /// Database filename within the container
    pub db_name: String,
}

/// SQLite-objs backed provider using Azure Storage VFS.
///
/// This provider uses the same schema as the regular SQLite provider but stores
/// the database in Azure Blob Storage via the sqlite-objs VFS layer. All operations
/// are synchronous (rusqlite) and wrapped with `tokio::task::spawn_blocking`.
pub struct SqliteObjsProvider {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteObjsProvider {
    /// Get a reference to the underlying connection for testing/admin purposes.
    pub fn get_conn(&self) -> &Arc<Mutex<Connection>> {
        &self.conn
    }

    /// Convert rusqlite error to ProviderError with appropriate retry classification
    fn rusqlite_to_provider_error(operation: &str, e: rusqlite::Error) -> ProviderError {
        let error_msg = e.to_string();

        if error_msg.contains("database is locked") || error_msg.contains("SQLITE_BUSY") {
            return ProviderError::retryable(operation, format!("Database locked: {error_msg}"));
        }
        if error_msg.contains("UNIQUE constraint") || error_msg.contains("PRIMARY KEY") {
            return ProviderError::permanent(operation, format!("Constraint violation: {error_msg}"));
        }
        if error_msg.contains("connection") || error_msg.contains("timeout") {
            return ProviderError::retryable(operation, format!("Connection error: {error_msg}"));
        }
        ProviderError::retryable(operation, error_msg)
    }

    /// Create a new SQLite-objs provider backed by Azure Storage.
    ///
    /// # Arguments
    /// * `options` - Azure Storage connection configuration
    ///
    /// # Errors
    /// Returns an error if VFS registration, database connection, or schema initialization fails.
    pub async fn new(options: SqliteObjsOptions) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection, Box<dyn std::error::Error + Send + Sync>> {
            // Register the sqlite-objs VFS (idempotent - ok if already registered)
            SqliteObjsVfs::register_uri(false).ok();

            let uri = format!(
                "file:{}?azure_account={}&azure_container={}&azure_sas={}",
                options.db_name, options.azure_account, options.azure_container, options.azure_sas
            );

            let conn = Connection::open_with_flags_and_vfs(
                &uri,
                OpenFlags::SQLITE_OPEN_READ_WRITE
                    | OpenFlags::SQLITE_OPEN_CREATE
                    | OpenFlags::SQLITE_OPEN_URI,
                "sqlite-objs",
            )?;

            // Configure pragmas — EXCLUSIVE locking required for sqlite-objs VFS
            conn.execute_batch(
                "PRAGMA locking_mode = EXCLUSIVE;
                 PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;
                 PRAGMA busy_timeout = 60000;
                 PRAGMA foreign_keys = ON;
                 PRAGMA cache_size = -64000;",
            )?;

            Ok(conn)
        })
        .await??;

        let provider = Self {
            conn: Arc::new(Mutex::new(conn)),
        };

        // Create schema
        provider.create_schema().await?;

        Ok(provider)
    }

    /// Create a new SQLite-objs provider using a connection string.
    ///
    /// Parses an Azure Storage connection string and container name to create the provider.
    /// Uses Shared Key authentication via `register_with_config`.
    ///
    /// # Arguments
    /// * `connection_string` - Azure Storage connection string
    /// * `container` - Azure Storage container name
    /// * `db_name` - Database filename (e.g., "duroxide.db")
    ///
    /// # Errors
    /// Returns an error if parsing fails or connection/schema initialization fails.
    pub async fn new_from_connection_string(
        connection_string: &str,
        container: &str,
        db_name: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let account = Self::parse_connection_string_field(connection_string, "AccountName")
            .ok_or("Missing AccountName in connection string")?;
        let account_key = Self::parse_connection_string_field(connection_string, "AccountKey")
            .ok_or("Missing AccountKey in connection string")?;

        let config = sqlite_objs::SqliteObjsConfig {
            account: account.clone(),
            container: container.to_string(),
            sas_token: None,
            account_key: Some(account_key),
            endpoint: None,
        };

        let db_name = db_name.to_string();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection, Box<dyn std::error::Error + Send + Sync>> {
            SqliteObjsVfs::register_with_config(&config, false)
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

            let conn = Connection::open_with_flags_and_vfs(
                &db_name,
                OpenFlags::SQLITE_OPEN_READ_WRITE
                    | OpenFlags::SQLITE_OPEN_CREATE,
                "sqlite-objs",
            )?;

            conn.execute_batch(
                "PRAGMA locking_mode = EXCLUSIVE;
                 PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;
                 PRAGMA busy_timeout = 60000;
                 PRAGMA foreign_keys = ON;
                 PRAGMA cache_size = -64000;",
            )?;

            Ok(conn)
        })
        .await??;

        let provider = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        provider.create_schema().await?;
        Ok(provider)
    }

    /// Create a local-file SQLite-objs provider (for testing without Azure).
    ///
    /// Uses rusqlite directly with a local file, no VFS.
    ///
    /// # Errors
    /// Returns an error if database connection or schema initialization fails.
    pub async fn new_local(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let path = path.to_string();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection, rusqlite::Error> {
            let conn = Connection::open(&path)?;
            conn.execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = WAL;
                 PRAGMA busy_timeout = 60000;
                 PRAGMA foreign_keys = ON;
                 PRAGMA cache_size = -64000;
                 PRAGMA wal_autocheckpoint = 10000;",
            )?;
            Ok(conn)
        })
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

        let provider = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        provider.create_schema().await?;
        Ok(provider)
    }

    /// Create an in-memory SQLite-objs provider (for testing).
    ///
    /// # Errors
    /// Returns an error if database connection or schema initialization fails.
    pub async fn new_in_memory() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let conn = tokio::task::spawn_blocking(|| -> Result<Connection, rusqlite::Error> {
            let conn = Connection::open_in_memory()?;
            conn.execute_batch(
                "PRAGMA journal_mode = MEMORY;
                 PRAGMA synchronous = OFF;
                 PRAGMA busy_timeout = 60000;
                 PRAGMA foreign_keys = ON;",
            )?;
            Ok(conn)
        })
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

        let provider = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        provider.create_schema().await?;
        Ok(provider)
    }

    fn parse_connection_string_field(conn_str: &str, field: &str) -> Option<String> {
        for part in conn_str.split(';') {
            let part = part.trim();
            if let Some(value) = part.strip_prefix(&format!("{field}=")) {
                return Some(value.to_string());
            }
        }
        None
    }

    /// Create schema (same tables as the regular SQLite provider)
    async fn create_schema(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS instances (
                    instance_id TEXT PRIMARY KEY,
                    orchestration_name TEXT NOT NULL,
                    orchestration_version TEXT,
                    current_execution_id INTEGER NOT NULL DEFAULT 1,
                    custom_status TEXT,
                    custom_status_version INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    parent_instance_id TEXT REFERENCES instances(instance_id)
                );
                CREATE INDEX IF NOT EXISTS idx_instances_parent ON instances(parent_instance_id);

                CREATE TABLE IF NOT EXISTS executions (
                    instance_id TEXT NOT NULL,
                    execution_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'Running',
                    output TEXT,
                    duroxide_version_major INTEGER,
                    duroxide_version_minor INTEGER,
                    duroxide_version_patch INTEGER,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    PRIMARY KEY (instance_id, execution_id)
                );

                CREATE TABLE IF NOT EXISTS history (
                    instance_id TEXT NOT NULL,
                    execution_id INTEGER NOT NULL,
                    event_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    event_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (instance_id, execution_id, event_id)
                );

                CREATE TABLE IF NOT EXISTS orchestrator_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instance_id TEXT NOT NULL,
                    work_item TEXT NOT NULL,
                    visible_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    lock_token TEXT,
                    locked_until TIMESTAMP,
                    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS worker_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    work_item TEXT NOT NULL,
                    visible_at INTEGER NOT NULL DEFAULT 0,
                    lock_token TEXT,
                    locked_until TIMESTAMP,
                    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    instance_id TEXT,
                    execution_id TEXT,
                    activity_id INTEGER,
                    session_id TEXT,
                    tag TEXT
                );

                CREATE TABLE IF NOT EXISTS instance_locks (
                    instance_id TEXT PRIMARY KEY,
                    lock_token TEXT NOT NULL,
                    locked_until INTEGER NOT NULL,
                    locked_at INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_orch_visible ON orchestrator_queue(visible_at, lock_token);
                CREATE INDEX IF NOT EXISTS idx_orch_instance ON orchestrator_queue(instance_id);
                CREATE INDEX IF NOT EXISTS idx_orch_lock ON orchestrator_queue(lock_token);
                CREATE INDEX IF NOT EXISTS idx_worker_available ON worker_queue(lock_token, id);
                CREATE INDEX IF NOT EXISTS idx_worker_identity ON worker_queue(instance_id, execution_id, activity_id);
                CREATE INDEX IF NOT EXISTS idx_worker_queue_session ON worker_queue(session_id);
                CREATE INDEX IF NOT EXISTS idx_worker_queue_tag ON worker_queue(tag);

                CREATE TABLE IF NOT EXISTS sessions (
                    session_id     TEXT PRIMARY KEY,
                    worker_id      TEXT NOT NULL,
                    locked_until   INTEGER NOT NULL,
                    last_activity_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS kv_store (
                    instance_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    execution_id INTEGER NOT NULL,
                    PRIMARY KEY (instance_id, key)
                );
                CREATE INDEX IF NOT EXISTS idx_kv_store_execution ON kv_store(instance_id, execution_id);
                "#,
            )?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        Ok(())
    }

    /// Generate a unique lock token
    fn generate_lock_token() -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after UNIX epoch")
            .as_nanos();
        format!("lock_{now}_{}", std::process::id())
    }

    /// Get current timestamp in milliseconds
    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after UNIX epoch")
            .as_millis() as i64
    }

    /// Get future timestamp in milliseconds
    fn timestamp_after(duration: Duration) -> i64 {
        Self::now_millis() + duration.as_millis() as i64
    }

    /// Extract event_type string from event kind (for indexing only)
    fn event_type_str(event: &Event) -> &'static str {
        match &event.kind {
            EventKind::OrchestrationStarted { .. } => "OrchestrationStarted",
            EventKind::OrchestrationCompleted { .. } => "OrchestrationCompleted",
            EventKind::OrchestrationFailed { .. } => "OrchestrationFailed",
            EventKind::OrchestrationContinuedAsNew { .. } => "OrchestrationContinuedAsNew",
            EventKind::ActivityScheduled { .. } => "ActivityScheduled",
            EventKind::ActivityCompleted { .. } => "ActivityCompleted",
            EventKind::ActivityFailed { .. } => "ActivityFailed",
            EventKind::ActivityCancelRequested { .. } => "ActivityCancelRequested",
            EventKind::TimerCreated { .. } => "TimerCreated",
            EventKind::TimerFired { .. } => "TimerFired",
            EventKind::ExternalSubscribed { .. } => "ExternalSubscribed",
            EventKind::ExternalEvent { .. } => "ExternalEvent",
            EventKind::SubOrchestrationScheduled { .. } => "SubOrchestrationScheduled",
            EventKind::SubOrchestrationCompleted { .. } => "SubOrchestrationCompleted",
            EventKind::SubOrchestrationFailed { .. } => "SubOrchestrationFailed",
            EventKind::SubOrchestrationCancelRequested { .. } => "SubOrchestrationCancelRequested",
            EventKind::OrchestrationCancelRequested { .. } => "OrchestrationCancelRequested",
            EventKind::ExternalSubscribedCancelled { .. } => "ExternalSubscribedCancelled",
            EventKind::QueueSubscribed { .. } => "ExternalSubscribedPersistent",
            EventKind::QueueEventDelivered { .. } => "ExternalEventPersistent",
            EventKind::QueueSubscriptionCancelled { .. } => "ExternalSubscribedPersistentCancelled",
            EventKind::OrchestrationChained { .. } => "OrchestrationChained",
            EventKind::CustomStatusUpdated { .. } => "CustomStatusUpdated",
            EventKind::KeyValueSet { .. } => "KeyValueSet",
            EventKind::KeyValueCleared { .. } => "KeyValueCleared",
            EventKind::KeyValuesCleared => "KeyValuesCleared",
            #[cfg(feature = "replay-version-test")]
            EventKind::ExternalSubscribed2 { .. } => "ExternalSubscribed2",
            #[cfg(feature = "replay-version-test")]
            EventKind::ExternalEvent2 { .. } => "ExternalEvent2",
        }
    }

    /// Extract instance_id from a work item
    fn extract_instance(item: &WorkItem) -> Option<&str> {
        match item {
            WorkItem::StartOrchestration { instance, .. }
            | WorkItem::ActivityCompleted { instance, .. }
            | WorkItem::ActivityFailed { instance, .. }
            | WorkItem::TimerFired { instance, .. }
            | WorkItem::ExternalRaised { instance, .. }
            | WorkItem::QueueMessage { instance, .. }
            | WorkItem::CancelInstance { instance, .. }
            | WorkItem::ContinueAsNew { instance, .. } => Some(instance),
            #[cfg(feature = "replay-version-test")]
            WorkItem::ExternalRaised2 { instance, .. } => Some(instance),
            WorkItem::SubOrchCompleted { parent_instance, .. }
            | WorkItem::SubOrchFailed { parent_instance, .. } => Some(parent_instance),
            _ => None,
        }
    }

    /// Build tag filter SQL clause for worker queue queries
    fn build_tag_clause(filter: &TagFilter) -> String {
        match filter {
            TagFilter::DefaultOnly => "q.tag IS NULL".to_string(),
            TagFilter::Tags(set) => {
                let placeholders: Vec<String> = (0..set.len()).map(|_| "?".to_string()).collect();
                format!("q.tag IN ({})", placeholders.join(", "))
            }
            TagFilter::DefaultAnd(set) => {
                let placeholders: Vec<String> = (0..set.len()).map(|_| "?".to_string()).collect();
                format!("(q.tag IS NULL OR q.tag IN ({}))", placeholders.join(", "))
            }
            TagFilter::Any => "1".to_string(),
            TagFilter::None => "0".to_string(),
        }
    }

    /// Collect tag values in stable order for binding
    fn collect_tag_values(filter: &TagFilter) -> Vec<String> {
        match filter {
            TagFilter::Tags(set) | TagFilter::DefaultAnd(set) => {
                let mut v: Vec<String> = set.iter().cloned().collect();
                v.sort();
                v
            }
            _ => Vec::new(),
        }
    }
}

#[async_trait::async_trait]
impl Provider for SqliteObjsProvider {
    fn name(&self) -> &str {
        "sqlite-objs"
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn fetch_orchestration_item(
        &self,
        lock_timeout: Duration,
        _poll_timeout: Duration,
        filter: Option<&DispatcherCapabilityFilter>,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError> {
        let conn = self.conn.clone();
        let filter = filter.cloned();
        let lock_timeout = lock_timeout;

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let now_ms = Self::now_millis();

            // Step 1: Find available instance
            let instance_id: Option<String> = if let Some(ref cap_filter) = filter {
                let range = match cap_filter.supported_duroxide_versions.first() {
                    Some(r) => r,
                    None => return Ok(None),
                };
                let min_packed = range.min.major as i64 * 1_000_000
                    + range.min.minor as i64 * 1_000
                    + range.min.patch as i64;
                let max_packed = range.max.major as i64 * 1_000_000
                    + range.max.minor as i64 * 1_000
                    + range.max.patch as i64;

                conn.query_row(
                    r#"
                    SELECT q.instance_id
                    FROM orchestrator_queue q
                    LEFT JOIN instance_locks il ON q.instance_id = il.instance_id
                    LEFT JOIN instances i ON q.instance_id = i.instance_id
                    LEFT JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
                    WHERE q.visible_at <= ?1
                      AND (il.instance_id IS NULL OR il.locked_until <= ?1)
                      AND (
                        e.duroxide_version_major IS NULL
                        OR (e.duroxide_version_major * 1000000 + e.duroxide_version_minor * 1000 + e.duroxide_version_patch) BETWEEN ?2 AND ?3
                      )
                    ORDER BY q.id
                    LIMIT 1
                    "#,
                    params![now_ms, min_packed, max_packed],
                    |row| row.get(0),
                )
                .ok()
            } else {
                conn.query_row(
                    r#"
                    SELECT q.instance_id
                    FROM orchestrator_queue q
                    LEFT JOIN instance_locks il ON q.instance_id = il.instance_id
                    WHERE q.visible_at <= ?1
                      AND (il.instance_id IS NULL OR il.locked_until <= ?1)
                    ORDER BY q.id
                    LIMIT 1
                    "#,
                    params![now_ms],
                    |row| row.get(0),
                )
                .ok()
            };

            let instance_id = match instance_id {
                Some(id) => id,
                None => {
                    eprintln!("[fetch_orch] Step 1: no candidate found");
                    return Ok(None);
                }
            };

            eprintln!("[fetch_orch] Step 1: candidate={instance_id}, now_ms={now_ms}");

            // Step 2: Acquire instance lock
            let lock_token = Self::generate_lock_token();
            let locked_until = Self::timestamp_after(lock_timeout);

            let lock_rows = conn
                .execute(
                    r#"
                    INSERT INTO instance_locks (instance_id, lock_token, locked_until, locked_at)
                    VALUES (?1, ?2, ?3, ?4)
                    ON CONFLICT(instance_id) DO UPDATE
                    SET lock_token = ?2, locked_until = ?3, locked_at = ?4
                    WHERE locked_until <= ?4
                    "#,
                    params![&instance_id, &lock_token, locked_until, now_ms],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("fetch_orchestration_item", e))?;

            eprintln!("[fetch_orch] Step 2: lock_rows={lock_rows}");

            if lock_rows == 0 {
                return Ok(None);
            }

            // Step 3: Mark messages with our lock_token
            let mark_rows = conn.execute(
                "UPDATE orchestrator_queue SET lock_token = ?1, attempt_count = attempt_count + 1 WHERE instance_id = ?2 AND visible_at <= ?3",
                params![&lock_token, &instance_id, now_ms],
            )
            .map_err(|e| Self::rusqlite_to_provider_error("fetch_orchestration_item", e))?;

            eprintln!("[fetch_orch] Step 3: marked {mark_rows} messages");

            // Step 4: Fetch marked messages
            let mut stmt = conn
                .prepare("SELECT id, work_item, attempt_count FROM orchestrator_queue WHERE lock_token = ?1 ORDER BY id")
                .map_err(|e| Self::rusqlite_to_provider_error("fetch_orchestration_item", e))?;

            let mut max_attempt_count: u32 = 0;
            let work_items: Vec<WorkItem> = stmt
                .query_map(params![&lock_token], |row| {
                    let attempt_count: i64 = row.get(2)?;
                    let work_item_str: String = row.get(1)?;
                    Ok((work_item_str, attempt_count))
                })
                .map_err(|e| Self::rusqlite_to_provider_error("fetch_orchestration_item", e))?
                .filter_map(|r| {
                    if let Ok((s, ac)) = r {
                        max_attempt_count = max_attempt_count.max(ac as u32);
                        serde_json::from_str(&s).ok()
                    } else {
                        None
                    }
                })
                .collect();

            if work_items.is_empty() {
                eprintln!("[fetch_orch] Step 4: work_items empty! Releasing lock");
                conn.execute("DELETE FROM instance_locks WHERE instance_id = ?", params![&instance_id]).ok();
                return Ok(None);
            }

            eprintln!("[fetch_orch] Step 4: got {} work items", work_items.len());

            // Step 5: Get instance metadata
            let instance_info: Option<(String, Option<String>, i64)> = conn
                .query_row(
                    "SELECT orchestration_name, orchestration_version, current_execution_id FROM instances WHERE instance_id = ?1",
                    params![&instance_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .ok();

            let (orchestration_name, orchestration_version, current_execution_id, history) =
                if let Some((name, version, exec_id)) = instance_info {
                    // Read history for current execution
                    let mut hist_stmt = conn
                        .prepare("SELECT event_data FROM history WHERE instance_id = ?1 AND execution_id = ?2 ORDER BY event_id")
                        .map_err(|e| Self::rusqlite_to_provider_error("fetch_orchestration_item", e))?;

                    let hist_result: Result<Vec<Event>, _> = hist_stmt
                        .query_map(params![&instance_id, exec_id], |row| {
                            let data: String = row.get(0)?;
                            Ok(data)
                        })
                        .map_err(|e| Self::rusqlite_to_provider_error("fetch_orchestration_item", e))?
                        .enumerate()
                        .map(|(idx, r)| {
                            let data = r.map_err(|e| Self::rusqlite_to_provider_error("fetch_orchestration_item", e))?;
                            serde_json::from_str::<Event>(&data).map_err(|e| {
                                ProviderError::permanent(
                                    "fetch_orchestration_item",
                                    format!("Failed to deserialize history event at position {idx}: {e}"),
                                )
                            })
                        })
                        .collect();

                    match hist_result {
                        Ok(hist) => {
                            let version = version.unwrap_or_else(|| "unknown".to_string());
                            (name, version, exec_id as u64, hist)
                        }
                        Err(_e) => {
                            let error_msg = format!("Failed to deserialize history: {_e}");
                            tracing::warn!(instance = %instance_id, error = %error_msg, "History deserialization failed");
                            let version = version.unwrap_or_else(|| "unknown".to_string());
                            return Ok(Some((
                                OrchestrationItem {
                                    instance: instance_id,
                                    orchestration_name: name,
                                    execution_id: exec_id as u64,
                                    version,
                                    history: vec![],
                                    messages: work_items,
                                    history_error: Some(error_msg),
                                    kv_snapshot: std::collections::HashMap::new(),
                                },
                                lock_token,
                                max_attempt_count,
                            )));
                        }
                    }
                } else if let Some(start_item) = work_items.iter().find(|item| {
                    matches!(item, WorkItem::StartOrchestration { .. } | WorkItem::ContinueAsNew { .. })
                }) {
                    let (orchestration, version) = match start_item {
                        WorkItem::StartOrchestration { orchestration, version, .. }
                        | WorkItem::ContinueAsNew { orchestration, version, .. } => {
                            (orchestration.clone(), version.clone())
                        }
                        _ => unreachable!(),
                    };
                    (
                        orchestration,
                        version.unwrap_or_else(|| "unknown".to_string()),
                        1u64,
                        Vec::new(),
                    )
                } else {
                    // No instance info and no start message
                    let all_queue_messages = work_items.iter().all(|item| matches!(item, WorkItem::QueueMessage { .. }));
                    if all_queue_messages {
                        conn.execute("DELETE FROM orchestrator_queue WHERE lock_token = ?1", params![&lock_token]).ok();
                        conn.execute("DELETE FROM instance_locks WHERE lock_token = ?1", params![&lock_token]).ok();
                        return Ok(None);
                    }
                    // Release locks for retry
                    conn.execute(
                        "UPDATE orchestrator_queue SET lock_token = NULL WHERE lock_token = ?1",
                        params![&lock_token],
                    ).ok();
                    conn.execute("DELETE FROM instance_locks WHERE lock_token = ?1", params![&lock_token]).ok();
                    return Ok(None);
                };

            // Load KV snapshot
            let mut kv_stmt = conn
                .prepare("SELECT key, value FROM kv_store WHERE instance_id = ?")
                .map_err(|e| Self::rusqlite_to_provider_error("fetch_orchestration_item", e))?;
            let kv_snapshot: std::collections::HashMap<String, String> = kv_stmt
                .query_map(params![&instance_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|e| Self::rusqlite_to_provider_error("fetch_orchestration_item", e))?
                .filter_map(|r| r.ok())
                .collect();

            debug!(
                instance = %instance_id,
                messages = work_items.len(),
                history_len = history.len(),
                kv_keys = kv_snapshot.len(),
                "Fetched orchestration item (sqlite-objs)"
            );

            Ok(Some((
                OrchestrationItem {
                    instance: instance_id,
                    orchestration_name,
                    execution_id: current_execution_id,
                    version: orchestration_version,
                    messages: work_items,
                    history,
                    history_error: None,
                    kv_snapshot,
                },
                lock_token,
                max_attempt_count,
            )))
        })
        .await
        .map_err(|e| ProviderError::permanent("fetch_orchestration_item", format!("Task join error: {e}")))?
    }

    async fn ack_orchestration_item(
        &self,
        lock_token: &str,
        execution_id: u64,
        history_delta: Vec<Event>,
        worker_items: Vec<WorkItem>,
        orchestrator_items: Vec<WorkItem>,
        metadata: ExecutionMetadata,
        cancelled_activities: Vec<ScheduledActivityIdentifier>,
    ) -> Result<(), ProviderError> {
        let conn = self.conn.clone();
        let lock_token = lock_token.to_string();
        let execution_id = execution_id;

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            // Get instance from instance_locks
            let instance_id: String = conn
                .query_row(
                    "SELECT instance_id FROM instance_locks WHERE lock_token = ?",
                    params![&lock_token],
                    |row| row.get(0),
                )
                .map_err(|_| ProviderError::permanent("ack_orchestration_item", "Invalid lock token"))?;

            // Delete fetched messages
            conn.execute("DELETE FROM orchestrator_queue WHERE lock_token = ?", params![&lock_token])
                .map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;

            // Create or update instance metadata
            if let (Some(name), Some(version)) = (&metadata.orchestration_name, &metadata.orchestration_version) {
                conn.execute(
                    "INSERT OR IGNORE INTO instances (instance_id, orchestration_name, orchestration_version, current_execution_id, parent_instance_id) VALUES (?, ?, ?, ?, ?)",
                    params![&instance_id, name, version.as_str(), execution_id as i64, &metadata.parent_instance_id],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;

                conn.execute(
                    "UPDATE instances SET orchestration_name = ?, orchestration_version = ? WHERE instance_id = ?",
                    params![name, version, &instance_id],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;
            }

            // Create execution record
            conn.execute(
                "INSERT OR IGNORE INTO executions (instance_id, execution_id, status) VALUES (?, ?, 'Running')",
                params![&instance_id, execution_id as i64],
            )
            .map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;

            // Pin duroxide version
            if let Some(pinned) = &metadata.pinned_duroxide_version {
                conn.execute(
                    "UPDATE executions SET duroxide_version_major = ?, duroxide_version_minor = ?, duroxide_version_patch = ? WHERE instance_id = ? AND execution_id = ?",
                    params![pinned.major as i64, pinned.minor as i64, pinned.patch as i64, &instance_id, execution_id as i64],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;
            }

            // Update current_execution_id
            conn.execute(
                "UPDATE instances SET current_execution_id = MAX(current_execution_id, ?) WHERE instance_id = ?",
                params![execution_id as i64, &instance_id],
            )
            .map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;

            // Append history
            if !history_delta.is_empty() {
                for event in &history_delta {
                    if event.event_id() == 0 {
                        return Err(ProviderError::permanent("ack_orchestration_item", "event_id must be set by runtime"));
                    }
                    let event_data = serde_json::to_string(&event)
                        .expect("Event serialization should never fail");
                    conn.execute(
                        "INSERT INTO history (instance_id, execution_id, event_id, event_type, event_data) VALUES (?, ?, ?, ?, ?)",
                        params![&instance_id, execution_id as i64, event.event_id() as i64, Self::event_type_str(event), &event_data],
                    )
                    .map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;
                }
            }

            // Update execution status/output
            if let Some(status) = &metadata.status {
                let now_ms = Self::now_millis();
                conn.execute(
                    "UPDATE executions SET status = ?, output = ?, completed_at = ? WHERE instance_id = ? AND execution_id = ?",
                    params![status, &metadata.output, now_ms, &instance_id, execution_id as i64],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;
            }

            // Custom status from delta
            let custom_status_from_delta = history_delta.iter().rev().find_map(|e| match &e.kind {
                EventKind::CustomStatusUpdated { status } => Some(status.clone()),
                _ => None,
            });
            match custom_status_from_delta {
                Some(Some(custom_status)) => {
                    conn.execute(
                        "UPDATE instances SET custom_status = ?, custom_status_version = custom_status_version + 1 WHERE instance_id = ?",
                        params![&custom_status, &instance_id],
                    ).map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;
                }
                Some(None) => {
                    conn.execute(
                        "UPDATE instances SET custom_status = NULL, custom_status_version = custom_status_version + 1 WHERE instance_id = ?",
                        params![&instance_id],
                    ).map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;
                }
                None => {}
            }

            // Materialize KV mutations
            for event in &history_delta {
                match &event.kind {
                    EventKind::KeyValueSet { key, value } => {
                        conn.execute(
                            "INSERT INTO kv_store (instance_id, key, value, execution_id) VALUES (?, ?, ?, ?) ON CONFLICT(instance_id, key) DO UPDATE SET value = excluded.value, execution_id = excluded.execution_id",
                            params![&instance_id, key, value, execution_id as i64],
                        ).map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;
                    }
                    EventKind::KeyValueCleared { key } => {
                        conn.execute(
                            "DELETE FROM kv_store WHERE instance_id = ? AND key = ?",
                            params![&instance_id, key],
                        ).map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;
                    }
                    EventKind::KeyValuesCleared => {
                        conn.execute("DELETE FROM kv_store WHERE instance_id = ?", params![&instance_id])
                            .map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;
                    }
                    _ => {}
                }
            }

            // Enqueue worker items
            let now_ms = Self::now_millis();
            for item in worker_items {
                let (activity_instance, activity_execution_id, activity_id, session_id, tag) = match &item {
                    WorkItem::ActivityExecute { instance, execution_id, id, session_id, tag, .. } => {
                        (Some(instance.as_str()), Some(*execution_id), Some(*id), session_id.as_deref(), tag.as_deref())
                    }
                    _ => (None, None, None, None, None),
                };
                let work_item = serde_json::to_string(&item)
                    .map_err(|e| ProviderError::permanent("ack_orchestration_item", format!("Serialization error: {e}")))?;
                conn.execute(
                    "INSERT INTO worker_queue (work_item, visible_at, instance_id, execution_id, activity_id, session_id, tag) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    params![work_item, now_ms, activity_instance, activity_execution_id.map(|e| e as i64), activity_id.map(|a| a as i64), session_id, tag],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;
            }

            // Delete cancelled activities
            for activity in &cancelled_activities {
                conn.execute(
                    "DELETE FROM worker_queue WHERE instance_id = ? AND execution_id = ? AND activity_id = ?",
                    params![&activity.instance, activity.execution_id as i64, activity.activity_id as i64],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;
            }

            // Enqueue orchestrator items
            for item in orchestrator_items {
                let work_item = serde_json::to_string(&item)
                    .map_err(|e| ProviderError::permanent("ack_orchestration_item", format!("Serialization error: {e}")))?;
                let instance = match Self::extract_instance(&item) {
                    Some(i) => i.to_string(),
                    None => continue,
                };
                let visible_at = match &item {
                    WorkItem::TimerFired { fire_at_ms, .. } => *fire_at_ms as i64,
                    _ => Self::now_millis(),
                };
                conn.execute(
                    "INSERT INTO orchestrator_queue (instance_id, work_item, visible_at) VALUES (?, ?, ?)",
                    params![&instance, &work_item, visible_at],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;
            }

            // Validate instance lock still valid
            let now_ms = Self::now_millis();
            let lock_valid: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM instance_locks WHERE instance_id = ? AND lock_token = ? AND locked_until > ?",
                    params![&instance_id, &lock_token, now_ms],
                    |row| row.get(0),
                )
                .unwrap_or(0);

            if lock_valid == 0 {
                return Err(ProviderError::permanent("ack_orchestration_item", "Instance lock expired"));
            }

            // Release instance lock
            conn.execute(
                "DELETE FROM instance_locks WHERE instance_id = ? AND lock_token = ?",
                params![&instance_id, &lock_token],
            )
            .map_err(|e| Self::rusqlite_to_provider_error("ack_orchestration_item", e))?;

            debug!(instance = %instance_id, "Acknowledged orchestration item (sqlite-objs)");
            Ok(())
        })
        .await
        .map_err(|e| ProviderError::permanent("ack_orchestration_item", format!("Task join error: {e}")))?
    }

    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        let conn = self.conn.clone();
        let instance = instance.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            let execution_id: i64 = conn
                .query_row(
                    "SELECT COALESCE(MAX(execution_id), 1) FROM executions WHERE instance_id = ?",
                    params![&instance],
                    |row| row.get(0),
                )
                .unwrap_or(1);

            let mut stmt = conn
                .prepare("SELECT event_data FROM history WHERE instance_id = ? AND execution_id = ? ORDER BY event_id")
                .map_err(|e| Self::rusqlite_to_provider_error("read", e))?;

            let events: Result<Vec<Event>, _> = stmt
                .query_map(params![&instance, execution_id], |row| {
                    let data: String = row.get(0)?;
                    Ok(data)
                })
                .map_err(|e| Self::rusqlite_to_provider_error("read", e))?
                .map(|r| {
                    let data = r.map_err(|e| Self::rusqlite_to_provider_error("read", e))?;
                    serde_json::from_str::<Event>(&data)
                        .map_err(|e| ProviderError::permanent("read", format!("Deserialization error: {e}")))
                })
                .collect();

            events
        })
        .await
        .map_err(|e| ProviderError::permanent("read", format!("Task join error: {e}")))?
    }

    async fn read_with_execution(&self, instance: &str, execution_id: u64) -> Result<Vec<Event>, ProviderError> {
        let conn = self.conn.clone();
        let instance = instance.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            let mut stmt = conn
                .prepare("SELECT event_data FROM history WHERE instance_id = ? AND execution_id = ? ORDER BY event_id")
                .map_err(|e| Self::rusqlite_to_provider_error("read_with_execution", e))?;

            let events: Result<Vec<Event>, _> = stmt
                .query_map(params![&instance, execution_id as i64], |row| {
                    let data: String = row.get(0)?;
                    Ok(data)
                })
                .map_err(|e| Self::rusqlite_to_provider_error("read_with_execution", e))?
                .map(|r| {
                    let data = r.map_err(|e| Self::rusqlite_to_provider_error("read_with_execution", e))?;
                    serde_json::from_str::<Event>(&data)
                        .map_err(|e| ProviderError::permanent("read_with_execution", format!("Deserialization error: {e}")))
                })
                .collect();

            events
        })
        .await
        .map_err(|e| ProviderError::permanent("read_with_execution", format!("Task join error: {e}")))?
    }

    async fn append_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
        new_events: Vec<Event>,
    ) -> Result<(), ProviderError> {
        let conn = self.conn.clone();
        let instance = instance.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            for event in &new_events {
                if event.event_id() == 0 {
                    return Err(ProviderError::permanent("append_with_execution", "event_id must be set by runtime"));
                }
                let event_data = serde_json::to_string(&event)
                    .expect("Event serialization should never fail");
                conn.execute(
                    "INSERT INTO history (instance_id, execution_id, event_id, event_type, event_data) VALUES (?, ?, ?, ?, ?)",
                    params![&instance, execution_id as i64, event.event_id() as i64, Self::event_type_str(event), &event_data],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("append_with_execution", e))?;
            }

            Ok(())
        })
        .await
        .map_err(|e| ProviderError::permanent("append_with_execution", format!("Task join error: {e}")))?
    }

    async fn enqueue_for_orchestrator(&self, item: WorkItem, delay: Option<Duration>) -> Result<(), ProviderError> {
        let conn = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            let work_item = serde_json::to_string(&item)
                .map_err(|e| ProviderError::permanent("enqueue_for_orchestrator", format!("Serialization error: {e}")))?;

            let instance = Self::extract_instance(&item)
                .ok_or_else(|| ProviderError::permanent("enqueue_for_orchestrator", "Invalid work item type"))?
                .to_string();

            let visible_at = if let Some(delay) = delay {
                let delay_ms = delay.as_millis().min(i64::MAX as u128) as i64;
                Self::now_millis().saturating_add(delay_ms)
            } else {
                Self::now_millis()
            };

            conn.execute(
                "INSERT INTO orchestrator_queue (instance_id, work_item, visible_at) VALUES (?, ?, ?)",
                params![&instance, &work_item, visible_at],
            )
            .map_err(|e| Self::rusqlite_to_provider_error("enqueue_for_orchestrator", e))?;

            Ok(())
        })
        .await
        .map_err(|e| ProviderError::permanent("enqueue_for_orchestrator", format!("Task join error: {e}")))?
    }

    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        let conn = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            let (activity_instance, activity_execution_id, activity_id, session_id, tag) = match &item {
                WorkItem::ActivityExecute { instance, execution_id, id, session_id, tag, .. } => {
                    (Some(instance.as_str()), Some(*execution_id), Some(*id), session_id.as_deref(), tag.as_deref())
                }
                _ => (None, None, None, None, None),
            };

            let work_item = serde_json::to_string(&item)
                .map_err(|e| ProviderError::permanent("enqueue_for_worker", format!("Serialization error: {e}")))?;
            let now_ms = Self::now_millis();

            conn.execute(
                "INSERT INTO worker_queue (work_item, visible_at, instance_id, execution_id, activity_id, session_id, tag) VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![work_item, now_ms, activity_instance, activity_execution_id.map(|e| e as i64), activity_id.map(|a| a as i64), session_id, tag],
            )
            .map_err(|e| Self::rusqlite_to_provider_error("enqueue_for_worker", e))?;

            Ok(())
        })
        .await
        .map_err(|e| ProviderError::permanent("enqueue_for_worker", format!("Task join error: {e}")))?
    }

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        _poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        if matches!(tag_filter, TagFilter::None) {
            return Ok(None);
        }

        let conn = self.conn.clone();
        let session = session.cloned();
        let tag_filter = tag_filter.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            let lock_token = Self::generate_lock_token();
            let locked_until = Self::timestamp_after(lock_timeout);
            let now_ms = Self::now_millis();
            let tag_clause = Self::build_tag_clause(&tag_filter);
            let tag_values = Self::collect_tag_values(&tag_filter);

            // Find next available work item
            let next_item: Option<(i64, String, i64, Option<String>)> = if let Some(ref config) = session {
                let sql = format!(
                    r#"
                    SELECT q.id, q.work_item, q.attempt_count, q.session_id
                    FROM worker_queue q
                    LEFT JOIN sessions s ON s.session_id = q.session_id AND s.locked_until > ?1
                    WHERE q.visible_at <= ?1
                      AND (q.lock_token IS NULL OR q.locked_until <= ?1)
                      AND (q.session_id IS NULL OR s.worker_id = ?2 OR s.session_id IS NULL)
                      AND ({tag_clause})
                    ORDER BY q.id
                    LIMIT 1
                    "#,
                );

                let mut stmt = conn.prepare(&sql)
                    .map_err(|e| Self::rusqlite_to_provider_error("fetch_work_item", e))?;

                // Build params dynamically
                let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![
                    Box::new(now_ms),
                    Box::new(config.owner_id.clone()),
                ];
                for val in &tag_values {
                    param_values.push(Box::new(val.clone()));
                }
                let params_ref: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|v| v.as_ref()).collect();

                stmt.query_row(params_ref.as_slice(), |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .ok()
            } else {
                let sql = format!(
                    r#"
                    SELECT q.id, q.work_item, q.attempt_count, q.session_id
                    FROM worker_queue q
                    WHERE q.visible_at <= ?1
                      AND (q.lock_token IS NULL OR q.locked_until <= ?1)
                      AND q.session_id IS NULL
                      AND ({tag_clause})
                    ORDER BY q.id
                    LIMIT 1
                    "#,
                );

                let mut stmt = conn.prepare(&sql)
                    .map_err(|e| Self::rusqlite_to_provider_error("fetch_work_item", e))?;

                let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![
                    Box::new(now_ms),
                ];
                for val in &tag_values {
                    param_values.push(Box::new(val.clone()));
                }
                let params_ref: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|v| v.as_ref()).collect();

                stmt.query_row(params_ref.as_slice(), |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .ok()
            };

            let (id, work_item_str, current_attempt_count, session_id) = match next_item {
                Some(item) => item,
                None => return Ok(None),
            };

            // Lock the item
            conn.execute(
                "UPDATE worker_queue SET lock_token = ?1, locked_until = ?2, attempt_count = attempt_count + 1 WHERE id = ?3",
                params![&lock_token, locked_until, id],
            )
            .map_err(|e| Self::rusqlite_to_provider_error("fetch_work_item", e))?;

            // Session handling
            if let (Some(sid), Some(config)) = (&session_id, &session) {
                let session_locked_until = now_ms + config.lock_timeout.as_millis() as i64;
                let upsert_rows = conn.execute(
                    r#"
                    INSERT INTO sessions (session_id, worker_id, locked_until, last_activity_at)
                    VALUES (?1, ?2, ?3, ?4)
                    ON CONFLICT (session_id) DO UPDATE
                    SET worker_id = ?2, locked_until = ?3, last_activity_at = ?4
                    WHERE sessions.locked_until <= ?4 OR sessions.worker_id = ?2
                    "#,
                    params![sid, &config.owner_id, session_locked_until, now_ms],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("fetch_work_item", e))?;

                if upsert_rows == 0 {
                    // Session claim failed - revert lock
                    conn.execute(
                        "UPDATE worker_queue SET lock_token = NULL, locked_until = NULL, attempt_count = attempt_count - 1 WHERE id = ?",
                        params![id],
                    ).ok();
                    return Ok(None);
                }
            }

            let attempt_count = (current_attempt_count + 1) as u32;
            let work_item: WorkItem = serde_json::from_str(&work_item_str)
                .map_err(|e| ProviderError::permanent("fetch_work_item", format!("Deserialization error: {e}")))?;

            Ok(Some((work_item, lock_token, attempt_count)))
        })
        .await
        .map_err(|e| ProviderError::permanent("fetch_work_item", format!("Task join error: {e}")))?
    }

    async fn ack_work_item(&self, token: &str, completion: Option<WorkItem>) -> Result<(), ProviderError> {
        let conn = self.conn.clone();
        let token = token.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let now_ms = Self::now_millis();

            // Delete and get session_id
            let session_id: Option<String> = conn
                .query_row(
                    "DELETE FROM worker_queue WHERE lock_token = ? AND locked_until > ? RETURNING session_id",
                    params![&token, now_ms],
                    |row| row.get(0),
                )
                .map_err(|_| {
                    ProviderError::permanent(
                        "ack_work_item",
                        "Activity was cancelled or lock expired (worker queue row not found or lock invalid)",
                    )
                })?;

            // Piggyback session update
            if let Some(ref sid) = session_id {
                conn.execute(
                    "UPDATE sessions SET last_activity_at = ?1 WHERE session_id = ?2 AND locked_until > ?1",
                    params![now_ms, sid],
                ).ok();
            }

            if let Some(completion) = completion {
                let instance = match &completion {
                    WorkItem::ActivityCompleted { instance, .. } | WorkItem::ActivityFailed { instance, .. } => instance,
                    _ => {
                        return Err(ProviderError::permanent("ack_work_item", "Invalid completion type"));
                    }
                };

                let work_item = serde_json::to_string(&completion)
                    .map_err(|e| ProviderError::permanent("ack_work_item", format!("Serialization error: {e}")))?;
                let now_ms = Self::now_millis();

                conn.execute(
                    "INSERT INTO orchestrator_queue (instance_id, work_item, visible_at) VALUES (?, ?, ?)",
                    params![instance, work_item, now_ms],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("ack_work_item", e))?;
            }

            Ok(())
        })
        .await
        .map_err(|e| ProviderError::permanent("ack_work_item", format!("Task join error: {e}")))?
    }

    async fn renew_work_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError> {
        let conn = self.conn.clone();
        let token = token.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let now_ms = Self::now_millis();
            let locked_until = Self::timestamp_after(extend_for);

            // Try update and check session
            let session_id: Option<String> = conn
                .query_row(
                    "UPDATE worker_queue SET locked_until = ?1 WHERE lock_token = ?2 AND locked_until > ?3 RETURNING session_id",
                    params![locked_until, &token, now_ms],
                    |row| row.get(0),
                )
                .map_err(|_| {
                    ProviderError::permanent(
                        "renew_work_item_lock",
                        "Lock renewal failed - activity was cancelled or lock expired",
                    )
                })?;

            if let Some(ref sid) = session_id {
                conn.execute(
                    "UPDATE sessions SET last_activity_at = ?1 WHERE session_id = ?2 AND locked_until > ?1",
                    params![now_ms, sid],
                ).ok();
            }

            Ok(())
        })
        .await
        .map_err(|e| ProviderError::permanent("renew_work_item_lock", format!("Task join error: {e}")))?
    }

    async fn renew_session_lock(
        &self,
        owner_ids: &[&str],
        extend_for: Duration,
        idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        if owner_ids.is_empty() {
            return Ok(0);
        }

        let conn = self.conn.clone();
        let owner_ids: Vec<String> = owner_ids.iter().map(|s| s.to_string()).collect();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let now_ms = Self::now_millis();
            let locked_until = now_ms + extend_for.as_millis() as i64;
            let idle_cutoff = now_ms - idle_timeout.as_millis() as i64;

            let placeholders: Vec<String> = owner_ids.iter().map(|_| "?".to_string()).collect();
            let sql = format!(
                "UPDATE sessions SET locked_until = ? WHERE worker_id IN ({}) AND locked_until > ? AND last_activity_at > ?",
                placeholders.join(", ")
            );

            let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(locked_until)];
            for id in &owner_ids {
                param_values.push(Box::new(id.clone()));
            }
            param_values.push(Box::new(now_ms));
            param_values.push(Box::new(idle_cutoff));
            let params_ref: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|v| v.as_ref()).collect();

            let count = conn
                .execute(&sql, params_ref.as_slice())
                .map_err(|e| Self::rusqlite_to_provider_error("renew_session_lock", e))?;

            Ok(count)
        })
        .await
        .map_err(|e| ProviderError::permanent("renew_session_lock", format!("Task join error: {e}")))?
    }

    async fn cleanup_orphaned_sessions(&self, _idle_timeout: Duration) -> Result<usize, ProviderError> {
        let conn = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let now_ms = Self::now_millis();

            let count = conn
                .execute(
                    r#"
                    DELETE FROM sessions
                    WHERE locked_until < ?1
                      AND NOT EXISTS (
                          SELECT 1 FROM worker_queue WHERE worker_queue.session_id = sessions.session_id
                      )
                    "#,
                    params![now_ms],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("cleanup_orphaned_sessions", e))?;

            Ok(count)
        })
        .await
        .map_err(|e| ProviderError::permanent("cleanup_orphaned_sessions", format!("Task join error: {e}")))?
    }

    async fn abandon_work_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        let conn = self.conn.clone();
        let token = token.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let now_ms = Self::now_millis();
            let visible_at = delay.map(|d| Self::timestamp_after(d)).unwrap_or(now_ms);

            let sql = if ignore_attempt {
                "UPDATE worker_queue SET lock_token = NULL, locked_until = NULL, visible_at = ?1, attempt_count = MAX(0, attempt_count - 1) WHERE lock_token = ?2"
            } else {
                "UPDATE worker_queue SET lock_token = NULL, locked_until = NULL, visible_at = ?1 WHERE lock_token = ?2"
            };

            let result = conn
                .execute(sql, params![visible_at, &token])
                .map_err(|e| Self::rusqlite_to_provider_error("abandon_work_item", e))?;

            if result == 0 {
                return Err(ProviderError::permanent("abandon_work_item", "Invalid lock token or already acked"));
            }

            Ok(())
        })
        .await
        .map_err(|e| ProviderError::permanent("abandon_work_item", format!("Task join error: {e}")))?
    }

    async fn renew_orchestration_item_lock(&self, token: &str, extend_for: Duration) -> Result<(), ProviderError> {
        let conn = self.conn.clone();
        let token = token.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let locked_until = Self::timestamp_after(extend_for);
            let now_ms = Self::now_millis();

            let result = conn
                .execute(
                    "UPDATE instance_locks SET locked_until = ?1 WHERE lock_token = ?2 AND locked_until > ?3",
                    params![locked_until, &token, now_ms],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("renew_orchestration_item_lock", e))?;

            if result == 0 {
                return Err(ProviderError::permanent(
                    "renew_orchestration_item_lock",
                    "Lock token invalid, expired, or already acked",
                ));
            }

            Ok(())
        })
        .await
        .map_err(|e| ProviderError::permanent("renew_orchestration_item_lock", format!("Task join error: {e}")))?
    }

    async fn abandon_orchestration_item(
        &self,
        lock_token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        let conn = self.conn.clone();
        let lock_token = lock_token.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            let instance_id: String = conn
                .query_row(
                    "SELECT instance_id FROM instance_locks WHERE lock_token = ?",
                    params![&lock_token],
                    |row| row.get(0),
                )
                .map_err(|_| ProviderError::permanent("abandon_orchestration_item", "Invalid lock token"))?;

            conn.execute("DELETE FROM instance_locks WHERE lock_token = ?", params![&lock_token])
                .map_err(|e| Self::rusqlite_to_provider_error("abandon_orchestration_item", e))?;

            if ignore_attempt {
                conn.execute(
                    "UPDATE orchestrator_queue SET attempt_count = MAX(0, attempt_count - 1) WHERE instance_id = ?",
                    params![&instance_id],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("abandon_orchestration_item", e))?;
            }

            if let Some(delay) = delay {
                let delay_ms = delay.as_millis().min(i64::MAX as u128) as i64;
                let visible_at = Self::now_millis().saturating_add(delay_ms);
                conn.execute(
                    "UPDATE orchestrator_queue SET visible_at = ? WHERE instance_id = ? AND visible_at <= ?",
                    params![visible_at, &instance_id, Self::now_millis()],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("abandon_orchestration_item", e))?;
            }

            Ok(())
        })
        .await
        .map_err(|e| ProviderError::permanent("abandon_orchestration_item", format!("Task join error: {e}")))?
    }

    fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
        Some(self as &dyn ProviderAdmin)
    }

    async fn get_custom_status(
        &self,
        instance: &str,
        last_seen_version: u64,
    ) -> Result<Option<(Option<String>, u64)>, ProviderError> {
        let conn = self.conn.clone();
        let instance = instance.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            let result: Option<(Option<String>, i64)> = conn
                .query_row(
                    "SELECT custom_status, custom_status_version FROM instances WHERE instance_id = ? AND custom_status_version > ?",
                    params![&instance, last_seen_version as i64],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            Ok(result.map(|(s, v)| (s, v as u64)))
        })
        .await
        .map_err(|e| ProviderError::permanent("get_custom_status", format!("Task join error: {e}")))?
    }

    async fn get_kv_value(&self, instance: &str, key: &str) -> Result<Option<String>, ProviderError> {
        let conn = self.conn.clone();
        let instance = instance.to_string();
        let key = key.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            let result: Option<String> = conn
                .query_row(
                    "SELECT value FROM kv_store WHERE instance_id = ? AND key = ?",
                    params![&instance, &key],
                    |row| row.get(0),
                )
                .ok();

            Ok(result)
        })
        .await
        .map_err(|e| ProviderError::permanent("get_kv_value", format!("Task join error: {e}")))?
    }
}

#[async_trait::async_trait]
impl ProviderAdmin for SqliteObjsProvider {
    async fn list_instances(&self) -> Result<Vec<String>, ProviderError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let mut stmt = conn
                .prepare("SELECT instance_id FROM instances ORDER BY created_at DESC")
                .map_err(|e| Self::rusqlite_to_provider_error("list_instances", e))?;
            let instances: Vec<String> = stmt
                .query_map([], |row| row.get(0))
                .map_err(|e| Self::rusqlite_to_provider_error("list_instances", e))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(instances)
        })
        .await
        .map_err(|e| ProviderError::permanent("list_instances", format!("Task join error: {e}")))?
    }

    async fn list_instances_by_status(&self, status: &str) -> Result<Vec<String>, ProviderError> {
        let conn = self.conn.clone();
        let status = status.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let mut stmt = conn
                .prepare(
                    "SELECT i.instance_id FROM instances i JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id WHERE e.status = ? ORDER BY i.created_at DESC",
                )
                .map_err(|e| Self::rusqlite_to_provider_error("list_instances_by_status", e))?;
            let instances: Vec<String> = stmt
                .query_map(params![&status], |row| row.get(0))
                .map_err(|e| Self::rusqlite_to_provider_error("list_instances_by_status", e))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(instances)
        })
        .await
        .map_err(|e| ProviderError::permanent("list_instances_by_status", format!("Task join error: {e}")))?
    }

    async fn list_executions(&self, instance: &str) -> Result<Vec<u64>, ProviderError> {
        let conn = self.conn.clone();
        let instance = instance.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let mut stmt = conn
                .prepare("SELECT execution_id FROM executions WHERE instance_id = ? ORDER BY execution_id")
                .map_err(|e| Self::rusqlite_to_provider_error("list_executions", e))?;
            let executions: Vec<u64> = stmt
                .query_map(params![&instance], |row| {
                    let id: i64 = row.get(0)?;
                    Ok(id as u64)
                })
                .map_err(|e| Self::rusqlite_to_provider_error("list_executions", e))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(executions)
        })
        .await
        .map_err(|e| ProviderError::permanent("list_executions", format!("Task join error: {e}")))?
    }

    async fn read_history_with_execution_id(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError> {
        self.read_with_execution(instance, execution_id).await
    }

    async fn read_history(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        self.read(instance).await
    }

    async fn latest_execution_id(&self, instance: &str) -> Result<u64, ProviderError> {
        let conn = self.conn.clone();
        let instance = instance.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let id: i64 = conn
                .query_row(
                    "SELECT COALESCE(MAX(execution_id), 1) FROM executions WHERE instance_id = ?",
                    params![&instance],
                    |row| row.get(0),
                )
                .unwrap_or(1);
            Ok(id as u64)
        })
        .await
        .map_err(|e| ProviderError::permanent("latest_execution_id", format!("Task join error: {e}")))?
    }

    async fn get_instance_info(&self, instance: &str) -> Result<InstanceInfo, ProviderError> {
        let conn = self.conn.clone();
        let instance = instance.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            conn.query_row(
                r#"
                SELECT i.instance_id, i.orchestration_name, i.orchestration_version,
                       i.current_execution_id, i.created_at, i.updated_at,
                       i.parent_instance_id, e.status, e.output
                FROM instances i
                LEFT JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
                WHERE i.instance_id = ?
                "#,
                params![&instance],
                |row| {
                    Ok(InstanceInfo {
                        instance_id: row.get(0)?,
                        orchestration_name: row.get(1)?,
                        orchestration_version: row.get::<_, Option<String>>(2)?.unwrap_or_else(|| "unknown".to_string()),
                        current_execution_id: row.get::<_, i64>(3)? as u64,
                        created_at: row.get::<_, i64>(4).unwrap_or(0) as u64,
                        updated_at: row.get::<_, i64>(5).unwrap_or(0) as u64,
                        parent_instance_id: row.get(6)?,
                        status: row.get::<_, Option<String>>(7)?.unwrap_or_else(|| "Unknown".to_string()),
                        output: row.get(8)?,
                    })
                },
            )
            .map_err(|_| ProviderError::permanent("get_instance_info", format!("Instance {instance} not found")))
        })
        .await
        .map_err(|e| ProviderError::permanent("get_instance_info", format!("Task join error: {e}")))?
    }

    async fn get_execution_info(&self, instance: &str, execution_id: u64) -> Result<ExecutionInfo, ProviderError> {
        let conn = self.conn.clone();
        let instance = instance.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            conn.query_row(
                r#"
                SELECT e.execution_id, e.status, e.output, e.started_at, e.completed_at,
                       COUNT(h.event_id) as event_count
                FROM executions e
                LEFT JOIN history h ON e.instance_id = h.instance_id AND e.execution_id = h.execution_id
                WHERE e.instance_id = ? AND e.execution_id = ?
                GROUP BY e.execution_id
                "#,
                params![&instance, execution_id as i64],
                |row| {
                    Ok(ExecutionInfo {
                        execution_id: row.get::<_, i64>(0)? as u64,
                        status: row.get(1)?,
                        output: row.get(2)?,
                        started_at: row.get::<_, i64>(3).unwrap_or(0) as u64,
                        completed_at: row.get::<_, Option<i64>>(4)?.map(|t| t as u64),
                        event_count: row.get::<_, i64>(5).unwrap_or(0) as usize,
                    })
                },
            )
            .map_err(|_| {
                ProviderError::permanent(
                    "get_execution_info",
                    format!("Execution {execution_id} not found for instance {instance}"),
                )
            })
        })
        .await
        .map_err(|e| ProviderError::permanent("get_execution_info", format!("Task join error: {e}")))?
    }

    async fn get_system_metrics(&self) -> Result<SystemMetrics, ProviderError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            let (total, running, completed, failed): (i64, i64, i64, i64) = conn
                .query_row(
                    r#"
                    SELECT COUNT(*),
                           SUM(CASE WHEN e.status = 'Running' THEN 1 ELSE 0 END),
                           SUM(CASE WHEN e.status = 'Completed' THEN 1 ELSE 0 END),
                           SUM(CASE WHEN e.status = 'Failed' THEN 1 ELSE 0 END)
                    FROM instances i
                    LEFT JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
                    "#,
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap_or((0, 0, 0, 0));

            let total_executions: i64 = conn
                .query_row("SELECT COUNT(*) FROM executions", [], |row| row.get(0))
                .unwrap_or(0);

            let total_events: i64 = conn
                .query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
                .unwrap_or(0);

            Ok(SystemMetrics {
                total_instances: total as u64,
                total_executions: total_executions as u64,
                running_instances: running as u64,
                completed_instances: completed as u64,
                failed_instances: failed as u64,
                total_events: total_events as u64,
            })
        })
        .await
        .map_err(|e| ProviderError::permanent("get_system_metrics", format!("Task join error: {e}")))?
    }

    async fn get_queue_depths(&self) -> Result<QueueDepths, ProviderError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            let orch: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM orchestrator_queue WHERE lock_token IS NULL",
                    [],
                    |row| row.get(0),
                )
                .unwrap_or(0);

            let worker: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM worker_queue WHERE lock_token IS NULL",
                    [],
                    |row| row.get(0),
                )
                .unwrap_or(0);

            Ok(QueueDepths {
                orchestrator_queue: orch as usize,
                worker_queue: worker as usize,
                timer_queue: 0,
            })
        })
        .await
        .map_err(|e| ProviderError::permanent("get_queue_depths", format!("Task join error: {e}")))?
    }

    async fn list_children(&self, instance_id: &str) -> Result<Vec<String>, ProviderError> {
        let conn = self.conn.clone();
        let instance_id = instance_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let mut stmt = conn
                .prepare("SELECT instance_id FROM instances WHERE parent_instance_id = ?")
                .map_err(|e| Self::rusqlite_to_provider_error("list_children", e))?;
            let children: Vec<String> = stmt
                .query_map(params![&instance_id], |row| row.get(0))
                .map_err(|e| Self::rusqlite_to_provider_error("list_children", e))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(children)
        })
        .await
        .map_err(|e| ProviderError::permanent("list_children", format!("Task join error: {e}")))?
    }

    async fn get_parent_id(&self, instance_id: &str) -> Result<Option<String>, ProviderError> {
        let conn = self.conn.clone();
        let instance_id = instance_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let result: Option<Option<String>> = conn
                .query_row(
                    "SELECT parent_instance_id FROM instances WHERE instance_id = ?",
                    params![&instance_id],
                    |row| row.get(0),
                )
                .ok();
            match result {
                Some(parent) => Ok(parent),
                None => Err(ProviderError::permanent("get_parent_id", format!("Instance {instance_id} not found"))),
            }
        })
        .await
        .map_err(|e| ProviderError::permanent("get_parent_id", format!("Task join error: {e}")))?
    }

    async fn delete_instances_atomic(
        &self,
        ids: &[String],
        force: bool,
    ) -> Result<DeleteInstanceResult, ProviderError> {
        if ids.is_empty() {
            return Ok(DeleteInstanceResult::default());
        }

        let conn = self.conn.clone();
        let ids = ids.to_vec();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");
            let placeholders: String = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");

            // Check terminal state if not force
            if !force {
                let check_sql = format!(
                    "SELECT i.instance_id, e.status FROM instances i LEFT JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id WHERE i.instance_id IN ({placeholders})"
                );
                let mut stmt = conn.prepare(&check_sql)
                    .map_err(|e| Self::rusqlite_to_provider_error("delete_instances_atomic", e))?;
                let params_ref: Vec<&dyn rusqlite::types::ToSql> = ids.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
                let mut rows = stmt.query(params_ref.as_slice())
                    .map_err(|e| Self::rusqlite_to_provider_error("delete_instances_atomic", e))?;
                while let Some(row) = rows.next().map_err(|e| Self::rusqlite_to_provider_error("delete_instances_atomic", e))? {
                    let status: Option<String> = row.get(1).ok();
                    if status.as_deref() == Some("Running") {
                        let instance_id: String = row.get(0).unwrap_or_default();
                        return Err(ProviderError::permanent(
                            "delete_instances_atomic",
                            format!("Instance {instance_id} is still running. Use force=true to delete anyway."),
                        ));
                    }
                }
            }

            let mut result = DeleteInstanceResult::default();

            // Count before delete
            let count_sql = format!("SELECT COUNT(*) FROM history WHERE instance_id IN ({placeholders})");
            let mut stmt = conn.prepare(&count_sql).map_err(|e| Self::rusqlite_to_provider_error("delete_instances_atomic", e))?;
            let params_ref: Vec<&dyn rusqlite::types::ToSql> = ids.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
            result.events_deleted = stmt.query_row(params_ref.as_slice(), |row| row.get::<_, i64>(0)).unwrap_or(0) as u64;

            let count_sql = format!("SELECT COUNT(*) FROM executions WHERE instance_id IN ({placeholders})");
            let mut stmt = conn.prepare(&count_sql).map_err(|e| Self::rusqlite_to_provider_error("delete_instances_atomic", e))?;
            let params_ref: Vec<&dyn rusqlite::types::ToSql> = ids.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
            result.executions_deleted = stmt.query_row(params_ref.as_slice(), |row| row.get::<_, i64>(0)).unwrap_or(0) as u64;

            // Delete from all tables
            for table in &["history", "executions", "orchestrator_queue", "worker_queue", "instance_locks", "kv_store", "instances"] {
                let del_sql = format!("DELETE FROM {} WHERE instance_id IN ({})", table, placeholders);
                let mut stmt = conn.prepare(&del_sql).map_err(|e| Self::rusqlite_to_provider_error("delete_instances_atomic", e))?;
                let params_ref: Vec<&dyn rusqlite::types::ToSql> = ids.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
                stmt.execute(params_ref.as_slice()).map_err(|e| Self::rusqlite_to_provider_error("delete_instances_atomic", e))?;
            }

            result.instances_deleted = ids.len() as u64;
            Ok(result)
        })
        .await
        .map_err(|e| ProviderError::permanent("delete_instances_atomic", format!("Task join error: {e}")))?
    }

    async fn prune_executions(&self, instance_id: &str, options: PruneOptions) -> Result<PruneResult, ProviderError> {
        let conn = self.conn.clone();
        let instance_id = instance_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            let current_execution_id: i64 = conn
                .query_row(
                    "SELECT current_execution_id FROM instances WHERE instance_id = ?",
                    params![&instance_id],
                    |row| row.get(0),
                )
                .map_err(|_| ProviderError::permanent("prune_executions", format!("Instance {instance_id} not found")))?;

            // Find executions to prune
            let mut conditions = vec![
                "instance_id = ?".to_string(),
                "execution_id != ?".to_string(),
                "status != 'Running'".to_string(),
            ];

            if let Some(keep_last) = options.keep_last {
                conditions.push(format!(
                    "execution_id NOT IN (SELECT execution_id FROM executions WHERE instance_id = ? ORDER BY execution_id DESC LIMIT {keep_last})"
                ));
            }

            if options.completed_before.is_some() {
                conditions.push("completed_at < ?".to_string());
            }

            let sql = format!("SELECT execution_id FROM executions WHERE {}", conditions.join(" AND "));
            let mut stmt = conn.prepare(&sql).map_err(|e| Self::rusqlite_to_provider_error("prune_executions", e))?;

            let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![
                Box::new(instance_id.clone()),
                Box::new(current_execution_id),
            ];
            if options.keep_last.is_some() {
                param_values.push(Box::new(instance_id.clone()));
            }
            if let Some(completed_before) = options.completed_before {
                param_values.push(Box::new(completed_before as i64));
            }
            let params_ref: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|v| v.as_ref()).collect();

            let exec_ids: Vec<i64> = stmt
                .query_map(params_ref.as_slice(), |row| row.get(0))
                .map_err(|e| Self::rusqlite_to_provider_error("prune_executions", e))?
                .filter_map(|r| r.ok())
                .collect();

            if exec_ids.is_empty() {
                return Ok(PruneResult {
                    instances_processed: 1,
                    executions_deleted: 0,
                    events_deleted: 0,
                });
            }

            let mut total_events = 0u64;
            for exec_id in &exec_ids {
                let event_count: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM history WHERE instance_id = ? AND execution_id = ?",
                        params![&instance_id, exec_id],
                        |row| row.get(0),
                    )
                    .unwrap_or(0);
                total_events += event_count as u64;

                conn.execute(
                    "DELETE FROM history WHERE instance_id = ? AND execution_id = ?",
                    params![&instance_id, exec_id],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("prune_executions", e))?;

                conn.execute(
                    "DELETE FROM executions WHERE instance_id = ? AND execution_id = ?",
                    params![&instance_id, exec_id],
                )
                .map_err(|e| Self::rusqlite_to_provider_error("prune_executions", e))?;
            }

            Ok(PruneResult {
                executions_deleted: exec_ids.len() as u64,
                events_deleted: total_events,
                instances_processed: 1,
            })
        })
        .await
        .map_err(|e| ProviderError::permanent("prune_executions", format!("Task join error: {e}")))?
    }

    async fn delete_instance_bulk(&self, filter: InstanceFilter) -> Result<DeleteInstanceResult, ProviderError> {
        let conn = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            let mut sql = String::from(
                r#"
                SELECT i.instance_id
                FROM instances i
                LEFT JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
                WHERE i.parent_instance_id IS NULL
                  AND e.status IN ('Completed', 'Failed', 'ContinuedAsNew')
                "#,
            );

            let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![];

            if let Some(ref ids) = filter.instance_ids {
                if ids.is_empty() {
                    return Ok(DeleteInstanceResult::default());
                }
                let placeholders: Vec<&str> = ids.iter().map(|_| "?").collect();
                sql.push_str(&format!(" AND i.instance_id IN ({})", placeholders.join(",")));
                for id in ids {
                    param_values.push(Box::new(id.clone()));
                }
            }

            if let Some(completed_before) = filter.completed_before {
                sql.push_str(" AND e.completed_at < ?");
                param_values.push(Box::new(completed_before as i64));
            }

            let limit = filter.limit.unwrap_or(DEFAULT_BULK_OPERATION_LIMIT);
            sql.push_str(&format!(" LIMIT {limit}"));

            let mut stmt = conn.prepare(&sql)
                .map_err(|e| Self::rusqlite_to_provider_error("delete_instance_bulk", e))?;
            let params_ref: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|v| v.as_ref()).collect();

            let instance_ids: Vec<String> = stmt
                .query_map(params_ref.as_slice(), |row| row.get(0))
                .map_err(|e| Self::rusqlite_to_provider_error("delete_instance_bulk", e))?
                .filter_map(|r| r.ok())
                .collect();

            if instance_ids.is_empty() {
                return Ok(DeleteInstanceResult::default());
            }

            drop(stmt);

            let mut result = DeleteInstanceResult::default();
            let placeholders: String = instance_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");

            let count_sql = format!("SELECT COUNT(*) FROM history WHERE instance_id IN ({placeholders})");
            let mut stmt = conn.prepare(&count_sql).map_err(|e| Self::rusqlite_to_provider_error("delete_instance_bulk", e))?;
            let params_ref: Vec<&dyn rusqlite::types::ToSql> = instance_ids.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
            result.events_deleted = stmt.query_row(params_ref.as_slice(), |row| row.get::<_, i64>(0)).unwrap_or(0) as u64;

            let count_sql = format!("SELECT COUNT(*) FROM executions WHERE instance_id IN ({placeholders})");
            let mut stmt = conn.prepare(&count_sql).map_err(|e| Self::rusqlite_to_provider_error("delete_instance_bulk", e))?;
            let params_ref: Vec<&dyn rusqlite::types::ToSql> = instance_ids.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
            result.executions_deleted = stmt.query_row(params_ref.as_slice(), |row| row.get::<_, i64>(0)).unwrap_or(0) as u64;

            for table in &["history", "executions", "orchestrator_queue", "worker_queue", "instance_locks", "kv_store", "instances"] {
                let del_sql = format!("DELETE FROM {} WHERE instance_id IN ({})", table, placeholders);
                let mut stmt = conn.prepare(&del_sql).map_err(|e| Self::rusqlite_to_provider_error("delete_instance_bulk", e))?;
                let params_ref: Vec<&dyn rusqlite::types::ToSql> = instance_ids.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
                stmt.execute(params_ref.as_slice()).map_err(|e| Self::rusqlite_to_provider_error("delete_instance_bulk", e))?;
            }

            result.instances_deleted = instance_ids.len() as u64;
            Ok(result)
        })
        .await
        .map_err(|e| ProviderError::permanent("delete_instance_bulk", format!("Task join error: {e}")))?
    }

    async fn prune_executions_bulk(
        &self,
        filter: InstanceFilter,
        options: PruneOptions,
    ) -> Result<PruneResult, ProviderError> {
        let conn = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("mutex poisoned");

            let mut sql = String::from(
                "SELECT i.instance_id FROM instances i WHERE 1=1",
            );

            let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![];

            if let Some(ref ids) = filter.instance_ids {
                if ids.is_empty() {
                    return Ok(PruneResult::default());
                }
                let placeholders: Vec<&str> = ids.iter().map(|_| "?").collect();
                sql.push_str(&format!(" AND i.instance_id IN ({})", placeholders.join(",")));
                for id in ids {
                    param_values.push(Box::new(id.clone()));
                }
            }

            let limit = filter.limit.unwrap_or(DEFAULT_BULK_OPERATION_LIMIT);
            sql.push_str(&format!(" LIMIT {limit}"));

            let mut stmt = conn.prepare(&sql)
                .map_err(|e| Self::rusqlite_to_provider_error("prune_executions_bulk", e))?;
            let params_ref: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|v| v.as_ref()).collect();

            let instance_ids: Vec<String> = stmt
                .query_map(params_ref.as_slice(), |row| row.get(0))
                .map_err(|e| Self::rusqlite_to_provider_error("prune_executions_bulk", e))?
                .filter_map(|r| r.ok())
                .collect();

            if instance_ids.is_empty() {
                return Ok(PruneResult::default());
            }

            drop(stmt);

            let mut total_result = PruneResult::default();

            for instance_id in &instance_ids {
                let current_execution_id: i64 = conn
                    .query_row(
                        "SELECT current_execution_id FROM instances WHERE instance_id = ?",
                        params![instance_id],
                        |row| row.get(0),
                    )
                    .unwrap_or(1);

                let mut conditions = vec![
                    "instance_id = ?".to_string(),
                    "execution_id != ?".to_string(),
                    "status != 'Running'".to_string(),
                ];

                if let Some(keep_last) = options.keep_last {
                    conditions.push(format!(
                        "execution_id NOT IN (SELECT execution_id FROM executions WHERE instance_id = ? ORDER BY execution_id DESC LIMIT {keep_last})"
                    ));
                }

                if options.completed_before.is_some() {
                    conditions.push("completed_at < ?".to_string());
                }

                let exec_sql = format!("SELECT execution_id FROM executions WHERE {}", conditions.join(" AND "));
                let mut stmt = conn.prepare(&exec_sql).map_err(|e| Self::rusqlite_to_provider_error("prune_executions_bulk", e))?;

                let mut p_values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![
                    Box::new(instance_id.clone()),
                    Box::new(current_execution_id),
                ];
                if options.keep_last.is_some() {
                    p_values.push(Box::new(instance_id.clone()));
                }
                if let Some(completed_before) = options.completed_before {
                    p_values.push(Box::new(completed_before as i64));
                }
                let p_ref: Vec<&dyn rusqlite::types::ToSql> = p_values.iter().map(|v| v.as_ref()).collect();

                let exec_ids: Vec<i64> = stmt
                    .query_map(p_ref.as_slice(), |row| row.get(0))
                    .map_err(|e| Self::rusqlite_to_provider_error("prune_executions_bulk", e))?
                    .filter_map(|r| r.ok())
                    .collect();

                drop(stmt);

                for exec_id in &exec_ids {
                    let event_count: i64 = conn
                        .query_row(
                            "SELECT COUNT(*) FROM history WHERE instance_id = ? AND execution_id = ?",
                            params![instance_id, exec_id],
                            |row| row.get(0),
                        )
                        .unwrap_or(0);
                    total_result.events_deleted += event_count as u64;

                    conn.execute("DELETE FROM history WHERE instance_id = ? AND execution_id = ?", params![instance_id, exec_id])
                        .map_err(|e| Self::rusqlite_to_provider_error("prune_executions_bulk", e))?;
                    conn.execute("DELETE FROM executions WHERE instance_id = ? AND execution_id = ?", params![instance_id, exec_id])
                        .map_err(|e| Self::rusqlite_to_provider_error("prune_executions_bulk", e))?;
                }

                total_result.executions_deleted += exec_ids.len() as u64;
                if !exec_ids.is_empty() {
                    total_result.instances_processed += 1;
                }
            }

            Ok(total_result)
        })
        .await
        .map_err(|e| ProviderError::permanent("prune_executions_bulk", format!("Task join error: {e}")))?
    }
}
