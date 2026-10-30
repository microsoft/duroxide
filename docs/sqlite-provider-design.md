# SQLite Provider Design for Duroxide

## Status: Implemented ✅

The SQLite provider has been successfully implemented with full transactional support.

## Executive Summary

SQLite is an excellent choice for a transactional Provider because:
- **ACID compliance** with full transactional support
- **Single-file deployment** with zero configuration
- **Excellent performance** for local/embedded scenarios
- **Battle-tested reliability** used in millions of applications
- **Built-in concurrency** with proper locking mechanisms

## Schema Design

### Core Tables

```sql
-- Instance metadata and current state
CREATE TABLE instances (
    instance_id TEXT PRIMARY KEY,
    orchestration_name TEXT NOT NULL,
    orchestration_version TEXT NOT NULL,
    current_execution_id INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'Running', -- Running, Completed, Failed, ContinuedAsNew
    parent_instance_id TEXT,  -- For sub-orchestrations: tracks parent for cascade deletion
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parent_instance_id) REFERENCES instances(instance_id) ON DELETE CASCADE
);

CREATE INDEX idx_instances_parent ON instances(parent_instance_id);

-- Multi-execution support for ContinueAsNew
CREATE TABLE executions (
    instance_id TEXT NOT NULL,
    execution_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'Running',
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    PRIMARY KEY (instance_id, execution_id),
    FOREIGN KEY (instance_id) REFERENCES instances(instance_id)
);

-- Event history (append-only)
CREATE TABLE history (
    instance_id TEXT NOT NULL,
    execution_id INTEGER NOT NULL,
    sequence_num INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_data TEXT NOT NULL, -- JSON serialized Event
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (instance_id, execution_id, sequence_num),
    FOREIGN KEY (instance_id, execution_id) REFERENCES executions(instance_id, execution_id)
);

-- Orchestrator queue with visibility support (also handles timers via visible_at)
CREATE TABLE orchestrator_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    work_item TEXT NOT NULL, -- JSON serialized WorkItem
    visible_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    lock_token TEXT,
    locked_until TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_visible (visible_at, lock_token),
    INDEX idx_instance (instance_id)
);

-- Worker queue for activity execution
CREATE TABLE worker_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    work_item TEXT NOT NULL, -- JSON serialized WorkItem
    visible_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    lock_token TEXT,
    locked_until TIMESTAMP,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    session_id TEXT,                  -- Session affinity routing key
    INDEX idx_available (visible_at, lock_token, id),
    INDEX idx_session (session_id)
);

-- Session affinity tracking (flow table)
CREATE TABLE sessions (
    session_id     TEXT PRIMARY KEY,  -- Flow key
    worker_id      TEXT NOT NULL,     -- Owning backend
    locked_until   INTEGER NOT NULL,  -- Heartbeat lease expiry
    last_activity_at INTEGER NOT NULL -- Last activity flow timestamp
);
```

### Key Design Decisions

1. **Separate queues** for orchestrator and worker (timers reuse the orchestrator queue via delayed visibility) to avoid contention
2. **Lock tokens with expiry** for crash recovery
3. **Visibility timestamps** for delayed message processing
4. **Normalized schema** for efficient queries and updates
5. **Session table as a flow table** — routes session-bound activities to owning workers (analogous to network flow affinity)
5. **JSON storage** for flexible event/work item serialization

Visibility timestamps use signed 64-bit Unix milliseconds. Relative delays
passed to queue enqueue/abandon operations saturate at `i64::MAX` if their
future timestamp exceeds that range. This does not cover absolute
`TimerFired.fire_at_ms` timestamps.

Eligible worker items are ordered by `visible_at, id`, supported by the
`idx_worker_ready_order` index. A deferred item goes behind already-ready work,
even if its delay expires before the backlog drains. Session and tag filtering
use the same ordering.

## Implementation Plan

### Phase 1: Core Provider Structure

```rust
use sqlx::{SqlitePool, Transaction, Sqlite};
use serde_json;
use std::time::{SystemTime, Duration};
use crate::providers::{Provider, WorkItem, OrchestrationItem};
use crate::Event;

pub struct SqliteProvider {
    pool: SqlitePool,
    lock_timeout: Duration,
    history_cap: usize,
}

impl SqliteProvider {
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = SqlitePool::connect(database_url).await?;
        
        // Run migrations
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await?;
        
        Ok(Self {
            pool,
            lock_timeout: Duration::from_secs(30),
            history_cap: 1024,
        })
    }
    
    async fn with_transaction<F, R>(&self, f: F) -> Result<R, String>
    where
        F: FnOnce(&mut Transaction<'_, Sqlite>) -> Result<R, sqlx::Error>,
    {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        match f(&mut tx) {
            Ok(result) => {
                tx.commit().await.map_err(|e| e.to_string())?;
                Ok(result)
            }
            Err(e) => {
                tx.rollback().await.ok();
                Err(e.to_string())
            }
        }
    }
}
```

### Phase 2: Core Atomic Methods

```rust
#[async_trait::async_trait]
impl Provider for SqliteProvider {
    async fn fetch_orchestration_item(&self) -> Option<OrchestrationItem> {
        let mut tx = self.pool.begin().await.ok()?;
        
        // Get next available instance with work
        let row = sqlx::query!(
            r#"
            SELECT DISTINCT instance_id
            FROM orchestrator_queue
            WHERE visible_at <= datetime('now')
              AND (lock_token IS NULL OR locked_until < datetime('now'))
            ORDER BY visible_at
            LIMIT 1
            "#
        )
        .fetch_optional(&mut tx)
        .await
        .ok()??;
        
        let instance_id = row.instance_id;
        let lock_token = generate_lock_token();
        let locked_until = SystemTime::now() + self.lock_timeout;
        
        // Lock all messages for this instance
        sqlx::query!(
            r#"
            UPDATE orchestrator_queue
            SET lock_token = ?1, locked_until = ?2
            WHERE instance_id = ?3
              AND visible_at <= datetime('now')
              AND (lock_token IS NULL OR locked_until < datetime('now'))
            "#,
            lock_token,
            locked_until,
            instance_id
        )
        .execute(&mut tx)
        .await
        .ok()?;
        
        // Fetch locked messages
        let messages = sqlx::query!(
            r#"
            SELECT id, work_item
            FROM orchestrator_queue
            WHERE lock_token = ?1
            ORDER BY id
            "#,
            lock_token
        )
        .fetch_all(&mut tx)
        .await
        .ok()?;
        
        // Deserialize work items
        let work_items: Vec<WorkItem> = messages
            .iter()
            .filter_map(|r| serde_json::from_str(&r.work_item).ok())
            .collect();
        
        // Load history
        let history = self.read_history_in_tx(&mut tx, &instance_id).await.ok()?;
        
        tx.commit().await.ok()?;
        
        Some(OrchestrationItem {
            instance: instance_id,
            messages: work_items,
            history,
            lock_token,
        })
    }
    
    async fn ack_orchestration_item(
        &self,
        lock_token: &str,
        history_delta: Vec<Event>,
        worker_items: Vec<WorkItem>,
        orchestrator_items: Vec<WorkItem>,
    ) -> Result<(), String> {
        self.with_transaction(|tx| async move {
            // 1. Delete acknowledged messages
            sqlx::query!(
                "DELETE FROM orchestrator_queue WHERE lock_token = ?",
                lock_token
            )
            .execute(tx)
            .await?;
            
            // 2. Get instance from the lock
            let instance = self.get_instance_from_lock(tx, lock_token).await?;
            
            // 3. Append history
            self.append_history_in_tx(tx, &instance, history_delta).await?;
            
            // 4. Enqueue worker items
            for item in worker_items {
                self.enqueue_worker_in_tx(tx, item).await?;
            }
            
            // 5. Enqueue orchestrator items (may include TimerFired with delayed visibility)
            for item in orchestrator_items {
                self.enqueue_orchestrator_in_tx(tx, item).await?;
            }
            
            Ok(())
        }).await
    }
}
```

## Advantages Over Filesystem Provider

### 1. **True ACID Transactions**
```sql
BEGIN TRANSACTION;
-- All operations succeed or fail together
DELETE FROM orchestrator_queue WHERE lock_token = ?;
INSERT INTO history ...;
INSERT INTO worker_queue ...;
-- TimerFired items reuse orchestrator_queue via delayed visibility
INSERT INTO orchestrator_queue ...;
COMMIT;
```