//! Durable delivery queue backed by SQLite (WAL mode).
//!
//! State machine: `pending -> inflight -> delivered`
//!                `pending -> inflight -> pending (retry, backoff) ... -> dead (DLQ)`
//!                `dead|delivered -> pending (manual replay)`
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Pending,
    Inflight,
    Delivered,
    Dead,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Pending => "pending",
            Status::Inflight => "inflight",
            Status::Delivered => "delivered",
            Status::Dead => "dead",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => Status::Pending,
            "inflight" => Status::Inflight,
            "delivered" => Status::Delivered,
            "dead" => Status::Dead,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Delivery {
    pub id: i64,
    pub idempotency_key: String,
    pub source: String,
    pub status: Status,
    pub attempts: u32,
    pub next_attempt_at: i64,
    pub last_error: Option<String>,
    pub last_status_code: Option<u16>,
    pub created_at: i64,
    pub updated_at: i64,
    pub delivered_at: Option<i64>,
    /// Payload as UTF-8 (lossy) for display; the raw bytes are relayed untouched.
    pub payload: String,
}

#[derive(Debug, Default, Clone, Serialize, PartialEq, Eq)]
pub struct Stats {
    pub pending: i64,
    pub inflight: i64,
    pub delivered: i64,
    pub dead: i64,
}

pub enum Enqueued {
    New(i64),
    Duplicate(i64),
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS deliveries (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key  TEXT NOT NULL UNIQUE,
    source           TEXT NOT NULL,
    payload          BLOB NOT NULL,
    content_type     TEXT NOT NULL DEFAULT 'application/json',
    status           TEXT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  INTEGER NOT NULL,
    last_error       TEXT,
    last_status_code INTEGER,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    delivered_at     INTEGER
);
CREATE INDEX IF NOT EXISTS idx_due ON deliveries(status, next_attempt_at);
"#;

const COLS: &str = "id, idempotency_key, source, status, attempts, next_attempt_at, last_error, \
                    last_status_code, created_at, updated_at, delivered_at, payload";

fn row_to_delivery(r: &Row) -> rusqlite::Result<Delivery> {
    let status: String = r.get(3)?;
    let payload: Vec<u8> = r.get(11)?;
    Ok(Delivery {
        id: r.get(0)?,
        idempotency_key: r.get(1)?,
        source: r.get(2)?,
        status: Status::parse(&status).unwrap_or(Status::Pending),
        attempts: r.get(4)?,
        next_attempt_at: r.get(5)?,
        last_error: r.get(6)?,
        last_status_code: r.get(7)?,
        created_at: r.get(8)?,
        updated_at: r.get(9)?,
        delivered_at: r.get(10)?,
        payload: String::from_utf8_lossy(&payload).into_owned(),
    })
}

/// A claimed unit of work handed to the worker.
#[derive(Debug, Clone)]
pub struct Job {
    pub id: i64,
    pub idempotency_key: String,
    pub source: String,
    pub payload: Vec<u8>,
    pub content_type: String,
    pub attempts: u32,
}

impl Store {
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch(SCHEMA)?;
        let s = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        s.recover_inflight()?;
        Ok(s)
    }

    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Jobs left `inflight` by a crash are returned to `pending` (at-least-once delivery).
    pub fn recover_inflight(&self) -> Result<usize> {
        let c = self.conn.lock().unwrap();
        Ok(c.execute(
            "UPDATE deliveries SET status='pending', updated_at=?1 WHERE status='inflight'",
            params![now_ms()],
        )?)
    }

    /// Inserts an event unless the idempotency key was already seen.
    pub fn enqueue(
        &self,
        key: &str,
        source: &str,
        payload: &[u8],
        content_type: &str,
    ) -> Result<Enqueued> {
        let c = self.conn.lock().unwrap();
        let now = now_ms();
        let inserted = c.execute(
            "INSERT INTO deliveries (idempotency_key, source, payload, content_type, status, next_attempt_at, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?5, ?5)
             ON CONFLICT(idempotency_key) DO NOTHING",
            params![key, source, payload, content_type, now],
        )?;
        let id: i64 = c.query_row(
            "SELECT id FROM deliveries WHERE idempotency_key=?1",
            params![key],
            |r| r.get(0),
        )?;
        Ok(if inserted == 1 {
            Enqueued::New(id)
        } else {
            Enqueued::Duplicate(id)
        })
    }

    /// Atomically claims up to `limit` due jobs, marking them `inflight`.
    pub fn claim_due(&self, now: i64, limit: u32) -> Result<Vec<Job>> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction()?;
        let jobs = {
            let mut st = tx.prepare(
                "SELECT id, idempotency_key, source, payload, content_type, attempts FROM deliveries
                 WHERE status='pending' AND next_attempt_at <= ?1 ORDER BY next_attempt_at LIMIT ?2",
            )?;
            let rows = st.query_map(params![now, limit], |r| {
                Ok(Job {
                    id: r.get(0)?,
                    idempotency_key: r.get(1)?,
                    source: r.get(2)?,
                    payload: r.get(3)?,
                    content_type: r.get(4)?,
                    attempts: r.get(5)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for j in &jobs {
            tx.execute(
                "UPDATE deliveries SET status='inflight', updated_at=?2 WHERE id=?1",
                params![j.id, now],
            )?;
        }
        tx.commit()?;
        Ok(jobs)
    }

    pub fn mark_delivered(&self, id: i64, code: u16) -> Result<()> {
        let c = self.conn.lock().unwrap();
        let now = now_ms();
        c.execute(
            "UPDATE deliveries SET status='delivered', attempts=attempts+1, last_status_code=?2,
             last_error=NULL, delivered_at=?3, updated_at=?3 WHERE id=?1",
            params![id, code, now],
        )?;
        Ok(())
    }

    /// Records a failed attempt; moves to DLQ (`dead`) when `next_attempt_at` is `None`.
    pub fn mark_failed(
        &self,
        id: i64,
        code: Option<u16>,
        err: &str,
        next_attempt_at: Option<i64>,
    ) -> Result<()> {
        let c = self.conn.lock().unwrap();
        let now = now_ms();
        let (status, next) = match next_attempt_at {
            Some(t) => ("pending", t),
            None => ("dead", now),
        };
        c.execute(
            "UPDATE deliveries SET status=?2, attempts=attempts+1, last_status_code=?3, last_error=?4,
             next_attempt_at=?5, updated_at=?6 WHERE id=?1",
            params![id, status, code, err, next, now],
        )?;
        Ok(())
    }

    /// Re-queues a dead (or delivered) delivery for immediate redelivery and resets its attempt budget.
    pub fn replay(&self, id: i64) -> Result<bool> {
        let c = self.conn.lock().unwrap();
        let now = now_ms();
        let n = c.execute(
            "UPDATE deliveries SET status='pending', attempts=0, next_attempt_at=?2, updated_at=?2
             WHERE id=?1 AND status IN ('dead','delivered')",
            params![id, now],
        )?;
        Ok(n == 1)
    }

    pub fn get(&self, id: i64) -> Result<Option<Delivery>> {
        let c = self.conn.lock().unwrap();
        Ok(c.query_row(
            &format!("SELECT {COLS} FROM deliveries WHERE id=?1"),
            params![id],
            row_to_delivery,
        )
        .optional()?)
    }

    pub fn list(
        &self,
        status: Option<Status>,
        limit: u32,
        before_id: Option<i64>,
    ) -> Result<Vec<Delivery>> {
        let c = self.conn.lock().unwrap();
        let mut st = c.prepare(&format!(
            "SELECT {COLS} FROM deliveries
             WHERE (?1 IS NULL OR status=?1) AND (?2 IS NULL OR id < ?2)
             ORDER BY id DESC LIMIT ?3"
        ))?;
        let rows = st.query_map(
            params![status.map(|s| s.as_str()), before_id, limit],
            row_to_delivery,
        )?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn stats(&self) -> Result<Stats> {
        let c = self.conn.lock().unwrap();
        let mut st = c.prepare("SELECT status, COUNT(*) FROM deliveries GROUP BY status")?;
        let mut s = Stats::default();
        let rows = st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        for row in rows {
            let (k, v) = row?;
            match k.as_str() {
                "pending" => s.pending = v,
                "inflight" => s.inflight = v,
                "delivered" => s.delivered = v,
                "dead" => s.dead = v,
                _ => {}
            }
        }
        Ok(s)
    }

    pub fn ping(&self) -> bool {
        self.conn
            .lock()
            .unwrap()
            .query_row("SELECT 1", [], |r| r.get::<_, i64>(0))
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotent_enqueue() {
        let s = Store::in_memory().unwrap();
        let a = s.enqueue("k1", "wa", b"{}", "application/json").unwrap();
        let b = s.enqueue("k1", "wa", b"{}", "application/json").unwrap();
        match (a, b) {
            (Enqueued::New(x), Enqueued::Duplicate(y)) => assert_eq!(x, y),
            _ => panic!("expected new then duplicate"),
        }
        assert_eq!(s.stats().unwrap().pending, 1);
    }

    #[test]
    fn lifecycle_retry_dead_replay() {
        let s = Store::in_memory().unwrap();
        s.enqueue("k", "wa", b"x", "text/plain").unwrap();
        let jobs = s.claim_due(now_ms(), 10).unwrap();
        assert_eq!(jobs.len(), 1);
        assert!(
            s.claim_due(now_ms(), 10).unwrap().is_empty(),
            "inflight must not be re-claimed"
        );
        let id = jobs[0].id;
        s.mark_failed(id, Some(500), "boom", Some(now_ms() + 60_000))
            .unwrap();
        assert!(s.claim_due(now_ms(), 10).unwrap().is_empty(), "not yet due");
        assert_eq!(s.claim_due(now_ms() + 61_000, 10).unwrap().len(), 1);
        s.mark_failed(id, Some(500), "boom", None).unwrap();
        let d = s.get(id).unwrap().unwrap();
        assert_eq!(d.status, Status::Dead);
        assert_eq!(d.attempts, 2);
        assert!(s.replay(id).unwrap());
        let d = s.get(id).unwrap().unwrap();
        assert_eq!((d.status, d.attempts), (Status::Pending, 0));
        assert!(!s.replay(id).unwrap(), "pending cannot be replayed");
    }

    #[test]
    fn recovers_inflight_after_crash() {
        let s = Store::in_memory().unwrap();
        s.enqueue("k", "wa", b"x", "text/plain").unwrap();
        s.claim_due(now_ms(), 10).unwrap();
        assert_eq!(s.recover_inflight().unwrap(), 1);
        assert_eq!(s.stats().unwrap().pending, 1);
    }
}
