#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]

use rusqlite::{params, Connection, OpenFlags};
use sqlite_objs::{SqliteObjsConfig, SqliteObjsVfs};
use std::error::Error;
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type DynError = Box<dyn Error + Send + Sync>;

fn load_dotenv() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let env_path = Path::new(manifest_dir).join(".env");
        dotenvy::from_path(&env_path).ok();
    });
}

fn azure_config_from_env() -> Result<SqliteObjsConfig, DynError> {
    load_dotenv();

    let conn_str = std::env::var("AZURE_STORAGE_CONNECTION_STRING")
        .map_err(|_| "missing AZURE_STORAGE_CONNECTION_STRING")?;
    let container = std::env::var("AZURE_STORAGE_CONTAINER")
        .map_err(|_| "missing AZURE_STORAGE_CONTAINER")?;

    let account = parse_connection_string_field(&conn_str, "AccountName")
        .ok_or("missing AccountName in AZURE_STORAGE_CONNECTION_STRING")?;
    let account_key = parse_connection_string_field(&conn_str, "AccountKey")
        .ok_or("missing AccountKey in AZURE_STORAGE_CONNECTION_STRING")?;

    Ok(SqliteObjsConfig {
        account,
        container,
        sas_token: None,
        account_key: Some(account_key),
        endpoint: None,
    })
}

fn parse_connection_string_field(conn_str: &str, field: &str) -> Option<String> {
    conn_str.split(';').find_map(|part| {
        let trimmed = part.trim();
        trimmed
            .strip_prefix(&format!("{field}="))
            .map(ToOwned::to_owned)
    })
}

fn register_vfs(config: &SqliteObjsConfig) -> Result<(), DynError> {
    static REGISTER_RESULT: OnceLock<Result<(), String>> = OnceLock::new();
    let result = REGISTER_RESULT.get_or_init(|| {
        SqliteObjsVfs::register_with_config(config, false).map_err(|e| e.to_string())
    });

    match result {
        Ok(()) => Ok(()),
        Err(e) => Err(e.clone().into()),
    }
}

fn open_connection(db_name: &str) -> Result<Connection, DynError> {
    let config = azure_config_from_env()?;
    register_vfs(&config)?;

    let conn = Connection::open_with_flags_and_vfs(
        db_name,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        "sqlite-objs",
    )?;

    conn.execute_batch(
        "PRAGMA locking_mode = EXCLUSIVE;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 60000;
         PRAGMA cache_size = -64000;",
    )?;

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS repro_log (
             writer TEXT NOT NULL,
             iteration INTEGER NOT NULL,
             created_at_ms INTEGER NOT NULL
         );",
    )?;

    Ok(conn)
}

fn run_worker(worker: &str, db_name: &str, iterations: u32, hold_ms: u64) -> Result<(), DynError> {
    eprintln!("[{worker}] opening db {db_name}");
    let conn = open_connection(db_name)?;

    for iteration in 0..iterations {
        conn.execute_batch("BEGIN IMMEDIATE;")?;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_millis() as i64;

        conn.execute(
            "INSERT INTO repro_log (writer, iteration, created_at_ms) VALUES (?1, ?2, ?3)",
            params![worker, iteration as i64, now_ms],
        )?;

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM repro_log", [], |row| row.get(0))?;
        eprintln!("[{worker}] iteration={iteration} rows={count}");

        if hold_ms > 0 {
            thread::sleep(Duration::from_millis(hold_ms));
        }

        conn.execute_batch("COMMIT;")?;
    }

    eprintln!("[{worker}] done");
    Ok(())
}

fn default_db_name() -> String {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis();
    format!("azure-vfs-repro-{now_ms}.db")
}

fn run_spawn_two(db_name: &str, iterations: u32, hold_ms: u64) -> Result<(), DynError> {
    let exe = std::env::current_exe()?;

    let mut child1 = Command::new(&exe)
        .arg("child")
        .arg("proc-a")
        .arg(db_name)
        .arg(iterations.to_string())
        .arg(hold_ms.to_string())
        .spawn()?;

    let mut child2 = Command::new(&exe)
        .arg("child")
        .arg("proc-b")
        .arg(db_name)
        .arg(iterations.to_string())
        .arg(hold_ms.to_string())
        .spawn()?;

    let status1 = child1.wait()?;
    let status2 = child2.wait()?;
    eprintln!("[parent] child statuses: {status1}, {status2}");

    if !status1.success() || !status2.success() {
        return Err("one or both child processes failed".into());
    }

    Ok(())
}

fn run_threads(db_name: &str, threads: usize, iterations: u32, hold_ms: u64) -> Result<(), DynError> {
    let mut handles = Vec::with_capacity(threads);

    for index in 0..threads {
        let worker = format!("thread-{index}");
        let db = db_name.to_string();
        handles.push(thread::spawn(move || run_worker(&worker, &db, iterations, hold_ms)));
    }

    for handle in handles {
        match handle.join() {
            Ok(result) => result?,
            Err(_) => return Err("thread panicked".into()),
        }
    }

    Ok(())
}

fn run_threads_distinct(db_prefix: &str, threads: usize, iterations: u32, hold_ms: u64) -> Result<(), DynError> {
    let mut handles = Vec::with_capacity(threads);

    for index in 0..threads {
        let worker = format!("thread-{index}");
        let db = format!("{db_prefix}-{index}.db");
        handles.push(thread::spawn(move || run_worker(&worker, &db, iterations, hold_ms)));
    }

    for handle in handles {
        match handle.join() {
            Ok(result) => result?,
            Err(_) => return Err("thread panicked".into()),
        }
    }

    Ok(())
}

fn print_usage() {
    eprintln!(
        "usage:\n  cargo run --bin azure_vfs_repro -- spawn-two [db_name] [iterations] [hold_ms]\n  cargo run --bin azure_vfs_repro -- threads [db_name] [threads] [iterations] [hold_ms]\n  cargo run --bin azure_vfs_repro -- threads-distinct [db_prefix] [threads] [iterations] [hold_ms]\n  cargo run --bin azure_vfs_repro -- child <worker> <db_name> <iterations> <hold_ms>"
    );
}

fn main() -> Result<(), DynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .without_time()
        .init();

    let mut args = std::env::args().skip(1);
    let Some(mode) = args.next() else {
        print_usage();
        return Err("missing mode".into());
    };

    match mode.as_str() {
        "spawn-two" => {
            let db_name = args.next().unwrap_or_else(default_db_name);
            let iterations = args.next().and_then(|v| v.parse().ok()).unwrap_or(20);
            let hold_ms = args.next().and_then(|v| v.parse().ok()).unwrap_or(10);
            eprintln!("[parent] mode=spawn-two db={db_name} iterations={iterations} hold_ms={hold_ms}");
            run_spawn_two(&db_name, iterations, hold_ms)
        }
        "threads" => {
            let db_name = args.next().unwrap_or_else(default_db_name);
            let threads = args.next().and_then(|v| v.parse().ok()).unwrap_or(2);
            let iterations = args.next().and_then(|v| v.parse().ok()).unwrap_or(20);
            let hold_ms = args.next().and_then(|v| v.parse().ok()).unwrap_or(10);
            eprintln!("[parent] mode=threads db={db_name} threads={threads} iterations={iterations} hold_ms={hold_ms}");
            run_threads(&db_name, threads, iterations, hold_ms)
        }
        "threads-distinct" => {
            let db_prefix = args.next().unwrap_or_else(|| default_db_name().trim_end_matches(".db").to_string());
            let threads = args.next().and_then(|v| v.parse().ok()).unwrap_or(2);
            let iterations = args.next().and_then(|v| v.parse().ok()).unwrap_or(20);
            let hold_ms = args.next().and_then(|v| v.parse().ok()).unwrap_or(10);
            eprintln!(
                "[parent] mode=threads-distinct db_prefix={db_prefix} threads={threads} iterations={iterations} hold_ms={hold_ms}"
            );
            run_threads_distinct(&db_prefix, threads, iterations, hold_ms)
        }
        "child" => {
            let worker = args.next().ok_or("missing worker name")?;
            let db_name = args.next().ok_or("missing db_name")?;
            let iterations = args
                .next()
                .ok_or("missing iterations")?
                .parse::<u32>()
                .map_err(|_| "invalid iterations")?;
            let hold_ms = args
                .next()
                .ok_or("missing hold_ms")?
                .parse::<u64>()
                .map_err(|_| "invalid hold_ms")?;
            run_worker(&worker, &db_name, iterations, hold_ms)
        }
        _ => {
            print_usage();
            Err(format!("unknown mode: {mode}").into())
        }
    }
}