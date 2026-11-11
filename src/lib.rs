//! Ledger — per-request cost accounting for inference calls.
//!
//! Rust port of `ledger.py` (Python stays as the reference implementation).
//! Every inference call gets a cost line: tokens in/out, model, node, timestamp.
//! Rollups per tenant/day/model. Append-only JSONL storage, thread-safe.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub ts: String,
    pub day: String,
    pub tenant: String,
    pub model: String,
    pub node: String,
    pub tokens_in: u64,
    pub tokens_out: u64,
}

#[derive(Debug)]
pub enum LedgerError {
    InvalidInput(String),
    Io(std::io::Error),
}

impl std::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LedgerError::InvalidInput(m) => write!(f, "{m}"),
            LedgerError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for LedgerError {}

impl From<std::io::Error> for LedgerError {
    fn from(e: std::io::Error) -> Self {
        LedgerError::Io(e)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum By {
    Tenant,
    Day,
    Model,
}

impl By {
    fn parse(s: &str) -> Result<By, LedgerError> {
        match s {
            "tenant" => Ok(By::Tenant),
            "day" => Ok(By::Day),
            "model" => Ok(By::Model),
            other => Err(LedgerError::InvalidInput(format!(
                "by must be 'tenant', 'day' or 'model', got '{other}'"
            ))),
        }
    }
}

fn now_iso_utc() -> String {
    // ISO-8601 UTC timestamp without external deps.
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch");
    let secs = dur.as_secs() as i64;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Civil-from-days algorithm (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    format!("{year:04}-{month:02}-{d:02}T{h:02}:{m:02}:{s:02}+00:00")
}

fn encode(e: &Entry) -> String {
    format!(
        "ts={},day={},tenant={},model={},node={},tokens_in={},tokens_out={}",
        e.ts, e.day, e.tenant, e.model, e.node, e.tokens_in, e.tokens_out
    )
}

fn decode(line: &str) -> Result<Entry, LedgerError> {
    let mut parts = BTreeMap::new();
    for p in line.split(',') {
        let (k, v) = p
            .split_once('=')
            .ok_or_else(|| LedgerError::InvalidInput(format!("bad ledger line: {line}")))?;
        parts.insert(k.to_string(), v.to_string());
    }
    let get = |k: &str| {
        parts
            .get(k)
            .cloned()
            .ok_or_else(|| LedgerError::InvalidInput(format!("missing field {k}")))
    };
    Ok(Entry {
        ts: get("ts")?,
        day: get("day")?,
        tenant: get("tenant")?,
        model: get("model")?,
        node: get("node")?,
        tokens_in: get("tokens_in")?.parse().map_err(|_| {
            LedgerError::InvalidInput("tokens_in is not a valid integer".to_string())
        })?,
        tokens_out: get("tokens_out")?.parse().map_err(|_| {
            LedgerError::InvalidInput("tokens_out is not a valid integer".to_string())
        })?,
    })
}

/// Append-only cost ledger with rollups and CSV export. Thread-safe.
pub struct Ledger {
    path: Option<PathBuf>,
    lines: Mutex<Vec<Entry>>,
}

impl Ledger {
    /// Open a ledger. If `path` is given and exists, reload lines from disk.
    pub fn new(path: Option<&Path>) -> Result<Ledger, LedgerError> {
        let mut lines = Vec::new();
        if let Some(p) = path {
            if p.exists() {
                let f = BufReader::new(File::open(p)?);
                for line in f.lines() {
                    let line = line?;
                    let line = line.trim();
                    if !line.is_empty() {
                        lines.push(decode(line)?);
                    }
                }
            }
        }
        Ok(Ledger {
            path: path.map(|p| p.to_path_buf()),
            lines: Mutex::new(lines),
        })
    }

    /// Append one cost line. `ts` defaults to now (UTC, ISO-8601).
    pub fn record(
        &self,
        tenant: &str,
        model: &str,
        node: &str,
        tokens_in: u64,
        tokens_out: u64,
        ts: Option<&str>,
    ) -> Result<Entry, LedgerError> {
        if tenant.is_empty() || model.is_empty() || node.is_empty() {
            return Err(LedgerError::InvalidInput(
                "tenant, model and node are required".to_string(),
            ));
        }
        let ts = match ts {
            Some(t) => t.to_string(),
            None => now_iso_utc(),
        };
        let entry = Entry {
            day: ts.chars().take(10).collect(),
            ts,
            tenant: tenant.to_string(),
            model: model.to_string(),
            node: node.to_string(),
            tokens_in,
            tokens_out,
        };
        let mut lines = self.lock();
        lines.push(entry.clone());
        if let Some(p) = &self.path {
            let mut f = OpenOptions::new().create(true).append(true).open(p)?;
            writeln!(f, "{}", encode(&entry))?;
        }
        Ok(entry)
    }

    /// Sum tokens_in+tokens_out grouped by tenant, day or model.
    pub fn rollup(&self, by: By) -> Result<BTreeMap<String, u64>, LedgerError> {
        let lines = self.lock();
        let mut sums: BTreeMap<String, u64> = BTreeMap::new();
        for e in lines.iter() {
            let key = match by {
                By::Day => e.day.clone(),
                By::Tenant => e.tenant.clone(),
                By::Model => e.model.clone(),
            };
            *sums.entry(key).or_insert(0) += e.tokens_in + e.tokens_out;
        }
        Ok(sums)
    }

    /// Write all lines to a CSV file. Returns the path.
    pub fn export(&self, path: &Path) -> Result<String, LedgerError> {
        let rows = self.lock().clone();
        let mut f = File::create(path)?;
        writeln!(f, "ts,tenant,model,node,tokens_in,tokens_out")?;
        for e in &rows {
            writeln!(
                f,
                "{},{},{},{},{},{}",
                e.ts, e.tenant, e.model, e.node, e.tokens_in, e.tokens_out
            )?;
        }
        Ok(path.to_string_lossy().into_owned())
    }

    /// Number of recorded lines.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> MutexGuard<'_, Vec<Entry>> {
        self.lines.lock().expect("ledger mutex poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("ledger-rs-test-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn record_appends_line() {
        let d = tmpdir("append");
        let p = d.join("ledger.jsonl");
        let led = Ledger::new(Some(&p)).unwrap();
        led.record("tenant-a", "glm-5.3-flash", "node-1", 100, 50, None)
            .unwrap();
        assert_eq!(led.len(), 1);
        let first = fs::read_to_string(&p).unwrap();
        assert!(first.lines().next().unwrap().contains("tenant=tenant-a"));
    }

    #[test]
    fn record_defaults_ts_now() {
        let led = Ledger::new(None).unwrap();
        let e = led.record("tenant-a", "m", "n", 1, 1, None).unwrap();
        assert!(e.ts.contains('T'));
    }

    #[test]
    fn record_rejects_bad_input() {
        let led = Ledger::new(None).unwrap();
        assert!(led.record("", "m", "n", 1, 1, None).is_err());
        assert!(led.record("t", "", "n", 1, 1, None).is_err());
        assert!(led.record("t", "m", "", 1, 1, None).is_err());
        // u64 type makes negative token counts unrepresentable; empty fields covered above.
    }

    #[test]
    fn rollup_by_tenant() {
        let led = Ledger::new(None).unwrap();
        led.record("tenant-a", "m1", "n", 10, 5, None).unwrap();
        led.record("tenant-a", "m2", "n", 20, 5, None).unwrap();
        led.record("tenant-b", "m1", "n", 1, 1, None).unwrap();
        let r = led.rollup(By::Tenant).unwrap();
        assert_eq!(r.get("tenant-a"), Some(&40));
        assert_eq!(r.get("tenant-b"), Some(&2));
    }

    #[test]
    fn rollup_by_model() {
        let led = Ledger::new(None).unwrap();
        led.record("t", "m1", "n", 10, 0, None).unwrap();
        led.record("t", "m2", "n", 3, 2, None).unwrap();
        let r = led.rollup(By::Model).unwrap();
        assert_eq!(r.get("m1"), Some(&10));
        assert_eq!(r.get("m2"), Some(&5));
    }

    #[test]
    fn rollup_by_day() {
        let led = Ledger::new(None).unwrap();
        led.record("t", "m", "n", 4, 1, Some("2026-10-13T10:00:00+00:00"))
            .unwrap();
        led.record("t", "m", "n", 2, 0, Some("2026-10-13T11:00:00+00:00"))
            .unwrap();
        led.record("t", "m", "n", 9, 0, Some("2026-10-14T11:00:00+00:00"))
            .unwrap();
        let r = led.rollup(By::Day).unwrap();
        assert_eq!(r.get("2026-10-13"), Some(&7));
        assert_eq!(r.get("2026-10-14"), Some(&9));
    }

    #[test]
    fn rollup_invalid_key() {
        assert!(By::parse("banana").is_err());
    }

    #[test]
    fn export_csv() {
        let d = tmpdir("csv");
        let led = Ledger::new(None).unwrap();
        led.record(
            "tenant-a",
            "m",
            "n",
            7,
            3,
            Some("2026-10-14T00:00:00+00:00"),
        )
        .unwrap();
        let out = d.join("out.csv");
        led.export(&out).unwrap();
        let content = fs::read_to_string(&out).unwrap();
        let mut lines = content.lines();
        assert_eq!(lines.next().unwrap(), "ts,tenant,model,node,tokens_in,tokens_out");
        let row = lines.next().unwrap();
        assert!(row.starts_with("2026-10-14T00:00:00+00:00,tenant-a,m,n,7,3"));
    }

    #[test]
    fn reload_from_disk() {
        let d = tmpdir("reload");
        let p = d.join("ledger.jsonl");
        let led = Ledger::new(Some(&p)).unwrap();
        led.record("tenant-a", "m", "n", 5, 5, None).unwrap();
        let led2 = Ledger::new(Some(&p)).unwrap();
        assert_eq!(led2.len(), 1);
        assert_eq!(led2.rollup(By::Tenant).unwrap().get("tenant-a"), Some(&10));
    }

    #[test]
    fn threaded_writes() {
        let d = tmpdir("threads");
        let p = d.join("ledger.jsonl");
        let led = std::sync::Arc::new(Ledger::new(Some(&p)).unwrap());
        let mut handles = Vec::new();
        for _ in 0..4 {
            let led = led.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..20 {
                    led.record("tenant-a", "m", "n", 1, 1, None).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(led.len(), 80);
        let content = fs::read_to_string(&p).unwrap();
        assert_eq!(content.lines().count(), 80);
    }
}
