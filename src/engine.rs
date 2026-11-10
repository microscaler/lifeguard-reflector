//! Batching, coalescing, watermark and catch-up logic, independent of Postgres so it can be
//! tested against [`crate::sink::MemorySink`].
//!
//! # Semantics
//!
//! - **Invalidate, never rewrite.** A change deletes `{prefix}{table}:{id}`; the next reader
//!   refills from Postgres. Deleting is idempotent, so duplicates (at-least-once delivery,
//!   overlapping leaders during failover, catch-up overlap) are harmless.
//! - **Batches** collect keys until `batch_max` or until the notification queue is drained (plus
//!   optional `batch_delay`), coalescing repeated keys; one `UNLINK` per batch.
//! - **Watermark.** After a successful flush the newest trigger timestamp is kept and written to
//!   Redis at most once a second. It tells a successor how far this instance got.
//! - **Gaps.** A reflector that was not listening (start-up, reconnect, cache outage overflow)
//!   runs a catch-up: ranged tables invalidate rows changed since `watermark - slack`, every other
//!   configured table is flushed by pattern. A hot standby LISTENs before it holds the lock and
//!   keeps the keys it saw; when it takes over and those cover everything after the old leader's
//!   watermark, it invalidates them instead of running a catch-up.

use crate::config::{CatchUp, Config, TableSpec};
use crate::metrics::metrics;
use microscaler_timing::{Outcome, Site};

/// One invalidation batch's round trip to the cache (`op_duration_seconds`).
static FLUSH: Site = Site::new("lifereflector", "flush");
/// From the first NOTIFY of a batch being received to its invalidation applied in the cache.
static NOTIFY_TO_APPLIED: Site = Site::new("lifereflector", "notify_to_applied");
use crate::payload::{self, Change, Op};
use crate::sink::{CacheSink, SinkError};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch.
#[must_use]
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Whether this instance is applying changes or only remembering them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Listening, not holding the lock: remember keys, invalidate nothing.
    Standby,
    /// Holding the lock: invalidate.
    Leader,
}

/// What a take-over decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakeOver {
    /// The standby's retained keys covered the gap; they were queued for invalidation.
    Gapless,
    /// A catch-up is needed.
    CatchUp,
}

/// Runs the ranged catch-up query for one table: primary keys (text) of rows whose change column
/// is at or after `since_ms`, at most `limit` of them.
pub type RangedQuery<'a> =
    dyn FnMut(&TableSpec, &str, i64, usize) -> Result<Vec<String>, String> + 'a;

/// The reflector's state machine.
pub struct Engine {
    cfg: Arc<Config>,
    sink: Box<dyn CacheSink>,
    role: Role,
    /// Leader: keys to invalidate -> newest trigger ts seen (0 when unknown).
    pending: HashMap<String, i64>,
    /// Leader: tables to flush entirely (TRUNCATE).
    pending_tables: HashSet<String>,
    oldest_ts: Option<i64>,
    newest_ts: Option<i64>,
    first_at: Option<Instant>,
    /// Standby: keys seen -> newest ts.
    retained: HashMap<String, i64>,
    /// Standby: every change with ts >= this is in `retained`.
    retained_since_ms: i64,
    retained_truncates: HashSet<String>,
    watermark_ms: Option<i64>,
    watermark_written: Option<Instant>,
    watermark_dirty: bool,
    needs_catch_up: bool,
    /// The catch-up in progress, worked through a step at a time between live flushes.
    catch_up_plan: Option<CatchUpPlan>,
    consecutive_failures: u32,
    /// Table name -> index into `tables` / `batch_oldest`; at most `MAX_TABLES` + 1 entries.
    table_ix: HashMap<String, usize>,
    /// Per-table metric handles, resolved on a table's first notification.
    tables: Vec<crate::metrics::TableMetrics>,
    /// Leader: oldest trigger ts of each table in the pending batch.
    batch_oldest: Vec<Option<i64>>,
    /// When the sink last completed a round trip (flush, watermark write, keep-alive ping).
    sink_used: Instant,
}

/// One unit of catch-up work.
#[derive(Debug, Clone)]
enum CatchUpTask {
    /// `SCAN` + `UNLINK` of a pattern, resumable at `cursor`; `mode` is the metric label.
    Scan {
        pattern: String,
        cursor: i64,
        mode: &'static str,
    },
    /// The change-column query of one table, with a flush of the table as the fallback.
    Ranged {
        spec: TableSpec,
        column: String,
        since_ms: i64,
    },
}

#[derive(Debug)]
struct CatchUpPlan {
    tasks: std::collections::VecDeque<CatchUpTask>,
    started_ms: i64,
}

impl Engine {
    /// New engine in [`Role::Standby`].
    #[must_use]
    pub fn new(cfg: Arc<Config>, sink: Box<dyn CacheSink>) -> Self {
        Self {
            cfg,
            sink,
            role: Role::Standby,
            pending: HashMap::new(),
            pending_tables: HashSet::new(),
            oldest_ts: None,
            newest_ts: None,
            first_at: None,
            retained: HashMap::new(),
            retained_since_ms: now_ms(),
            retained_truncates: HashSet::new(),
            watermark_ms: None,
            watermark_written: None,
            watermark_dirty: false,
            needs_catch_up: false,
            catch_up_plan: None,
            consecutive_failures: 0,
            table_ix: HashMap::new(),
            tables: Vec::new(),
            batch_oldest: Vec::new(),
            sink_used: Instant::now(),
        }
    }

    /// The sink in use.
    #[must_use]
    pub fn sink(&self) -> &dyn CacheSink {
        &*self.sink
    }

    /// Replace the sink (after a cache reconnect).
    pub fn set_sink(&mut self, sink: Box<dyn CacheSink>) {
        self.sink = sink;
        self.consecutive_failures = 0;
        self.sink_used = Instant::now();
    }

    /// Keep the cache connection warm: `PING` it when it has gone `every` or longer without a
    /// round trip (`Duration::ZERO`: always). A connection left idle can make the next round trip
    /// pay for itself going cold; in the first pp pilot one flush after a quiet spell took
    /// 250-500 ms against a few ms for the rest. `Ok(true)` when a ping was sent.
    ///
    /// # Errors
    ///
    /// The ping failed: the caller reconnects now, before a flush needs the connection.
    pub fn keep_warm(&mut self, every: Duration) -> Result<bool, SinkError> {
        if self.sink_used.elapsed() < every {
            return Ok(false);
        }
        self.sink.ping()?;
        self.sink_used = Instant::now();
        metrics().cache_pings.inc();
        Ok(true)
    }

    /// Current role.
    #[must_use]
    pub fn role(&self) -> Role {
        self.role
    }

    /// Flushes that failed in a row (reset on success).
    #[must_use]
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// `true` when a catch-up must run before the cache can be trusted again.
    #[must_use]
    pub fn needs_catch_up(&self) -> bool {
        self.needs_catch_up
    }

    /// Cache key for one row.
    #[must_use]
    pub fn key(&self, table: &str, id: &str) -> String {
        let mut k = String::with_capacity(self.cfg.key_prefix.len() + table.len() + id.len() + 1);
        k.push_str(&self.cfg.key_prefix);
        k.push_str(table);
        k.push(':');
        k.push_str(id);
        k
    }

    fn table_pattern(&self, table: &str) -> String {
        format!("{}{table}:*", self.cfg.key_prefix)
    }

    /// Keys waiting for the next flush (leader) or retained (standby).
    #[must_use]
    pub fn len(&self) -> usize {
        match self.role {
            Role::Leader => self.pending.len() + self.pending_tables.len(),
            Role::Standby => self.retained.len(),
        }
    }

    /// `true` when [`Engine::len`] is zero.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Decode and record one notification payload. Invalid payloads are counted and dropped.
    pub fn push_payload(&mut self, raw: &str) {
        #[allow(clippy::cast_precision_loss)]
        metrics().payload_bytes.observe(raw.len() as f64);
        match payload::parse(raw) {
            Ok(c) => self.push(&c),
            Err(e) => {
                metrics().invalid_payloads.inc();
                log::warn!("lifereflector: event=invalid_payload {e}");
            }
        }
    }

    /// Index of a table's metric handles, resolving them on its first change. Beyond
    /// [`crate::metrics::MAX_TABLES`] distinct tables every further one is
    /// [`crate::metrics::OTHER_TABLE`].
    fn table_index(&mut self, table: &str) -> usize {
        if let Some(&i) = self.table_ix.get(table) {
            return i;
        }
        let label = if self.table_ix.len() >= crate::metrics::MAX_TABLES {
            crate::metrics::OTHER_TABLE
        } else {
            table
        };
        if let Some(&i) = self.table_ix.get(label) {
            return i;
        }
        let i = self.tables.len();
        self.tables.push(metrics().table(label));
        self.batch_oldest.push(None);
        self.table_ix.insert(label.to_owned(), i);
        i
    }

    /// Record one change.
    pub fn push(&mut self, c: &Change) {
        let ti = self.table_index(&c.table);
        self.tables[ti].notifications.inc();
        if let Some(t) = c.ts_ms {
            #[allow(clippy::cast_precision_loss)]
            self.tables[ti]
                .notify_seconds
                .observe((now_ms() - t).max(0) as f64 / 1000.0);
        }
        let ts = c.ts_ms.unwrap_or(0);
        match self.role {
            Role::Standby => {
                if c.op == Op::Truncate {
                    self.retained_truncates.insert(c.table.clone());
                } else {
                    let k = self.key(&c.table, &c.id);
                    let e = self.retained.entry(k).or_insert(ts);
                    *e = (*e).max(ts);
                }
                if self.retained.len() > self.cfg.max_pending {
                    // Bounded memory: forget, and remember that only later changes are covered.
                    self.retained.clear();
                    self.retained_truncates.clear();
                    self.retained_since_ms = now_ms();
                }
            }
            Role::Leader => {
                if c.op == Op::Truncate {
                    self.pending_tables.insert(c.table.clone());
                } else {
                    let k = self.key(&c.table, &c.id);
                    match self.pending.get_mut(&k) {
                        Some(e) => {
                            *e = (*e).max(ts);
                            metrics().coalesced.inc();
                        }
                        None => {
                            self.pending.insert(k, ts);
                        }
                    }
                }
                if let Some(t) = c.ts_ms {
                    self.oldest_ts = Some(self.oldest_ts.map_or(t, |o| o.min(t)));
                    self.newest_ts = Some(self.newest_ts.map_or(t, |n| n.max(t)));
                    let o = &mut self.batch_oldest[ti];
                    *o = Some(o.map_or(t, |x| x.min(t)));
                }
                if self.first_at.is_none() {
                    self.first_at = Some(Instant::now());
                }
                if self.pending.len() > self.cfg.max_pending {
                    // The cache has been failing long enough to hold this many keys: drop them
                    // and invalidate by catch-up once it is back.
                    metrics().overflows.inc();
                    log::warn!(
                        "lifereflector: event=overflow pending={} exceeds LIFEREFLECTOR_MAX_PENDING; scheduling catch-up",
                        self.pending.len()
                    );
                    self.clear_pending();
                    self.needs_catch_up = true;
                    self.catch_up_plan = None;
                }
                metrics()
                    .pending
                    .set(i64::try_from(self.pending.len()).unwrap_or(i64::MAX));
            }
        }
    }

    fn clear_pending(&mut self) {
        self.pending.clear();
        self.pending_tables.clear();
        self.oldest_ts = None;
        self.newest_ts = None;
        self.first_at = None;
        self.batch_oldest.fill(None);
        metrics().pending.set(0);
    }

    /// When a non-full batch becomes due by waiting out `batch_delay`; `None` with nothing pending.
    #[must_use]
    pub fn batch_due_at(&self) -> Option<Instant> {
        self.first_at.map(|t| t + self.cfg.batch_delay)
    }

    /// `true` when the leader should flush now: the batch is full, or it has waited
    /// `batch_delay`. With `queue_drained`, a zero delay flushes immediately.
    #[must_use]
    pub fn due(&self, queue_drained: bool) -> bool {
        if self.role != Role::Leader || self.is_empty() {
            return false;
        }
        if self.pending.len() >= self.cfg.batch_max {
            return true;
        }
        let waited = self.first_at.map_or(Duration::ZERO, |t| t.elapsed());
        queue_drained && waited >= self.cfg.batch_delay
    }

    /// Invalidate everything pending. On failure the batch is kept for the next attempt.
    ///
    /// # Errors
    ///
    /// The cache operation failed.
    pub fn flush(&mut self) -> Result<usize, SinkError> {
        if self.role != Role::Leader || self.is_empty() {
            return Ok(0);
        }
        let m = metrics();
        let t = Instant::now();
        let flush_timer = FLUSH.start();
        let result = (|| {
            for table in &self.pending_tables {
                let n = self.sink.invalidate_matching(&self.table_pattern(table))?;
                m.keys_present.inc_by(n);
            }
            let keys: Vec<String> = self.pending.keys().cloned().collect();
            let n = self.sink.invalidate(&keys)?;
            m.keys_present.inc_by(n);
            Ok::<usize, SinkError>(keys.len())
        })();
        flush_timer.finish(if result.is_ok() {
            Outcome::Ok
        } else {
            Outcome::Err
        });
        match result {
            Ok(n) => {
                m.flushes.with_label_values(&["ok"]).inc();
                if let Some(received) = self.first_at {
                    // NOTIFY received (the batch's first) to the invalidation applied in Redis.
                    NOTIFY_TO_APPLIED.record(Outcome::Ok, received.elapsed());
                    m.apply_seconds.observe(received.elapsed().as_secs_f64());
                }
                let now = now_ms();
                for (i, oldest) in self.batch_oldest.iter().enumerate() {
                    if let Some(o) = oldest {
                        #[allow(clippy::cast_precision_loss)]
                        self.tables[i]
                            .lag_seconds
                            .observe((now - o).max(0) as f64 / 1000.0);
                    }
                }
                #[allow(clippy::cast_precision_loss)]
                m.batch_size.observe(n as f64);
                m.keys_invalidated.inc_by(n as u64);
                m.flush_seconds.observe(t.elapsed().as_secs_f64());
                if let Some(oldest) = self.oldest_ts {
                    #[allow(clippy::cast_precision_loss)]
                    m.lag_seconds
                        .observe((now_ms() - oldest).max(0) as f64 / 1000.0);
                }
                // While a catch-up is pending the watermark stays put: a successor must still
                // see the gap it covers.
                if let Some(newest) = self.newest_ts.filter(|_| !self.needs_catch_up) {
                    if self.watermark_ms.is_none_or(|w| newest > w) {
                        self.watermark_ms = Some(newest);
                        self.watermark_dirty = true;
                        m.watermark_ms.set(newest);
                    }
                }
                self.clear_pending();
                self.consecutive_failures = 0;
                self.sink_used = Instant::now();
                Ok(n)
            }
            Err(e) => {
                m.flushes.with_label_values(&["error"]).inc();
                self.consecutive_failures += 1;
                Err(e)
            }
        }
    }

    /// Write the watermark to the cache if it moved and the last write is older than `every`.
    ///
    /// # Errors
    ///
    /// The cache write failed (the watermark stays dirty).
    pub fn persist_watermark(&mut self, every: Duration) -> Result<(), SinkError> {
        if !self.watermark_dirty || self.role != Role::Leader || self.needs_catch_up {
            return Ok(());
        }
        if self.watermark_written.is_some_and(|t| t.elapsed() < every) {
            return Ok(());
        }
        if let Some(w) = self.watermark_ms {
            self.sink.set_i64(&self.cfg.watermark_key, w)?;
            self.sink_used = Instant::now();
        }
        self.watermark_written = Some(Instant::now());
        self.watermark_dirty = false;
        Ok(())
    }

    /// Become leader. Decides whether the keys retained as a standby cover the gap since the
    /// previous leader's watermark.
    ///
    /// # Errors
    ///
    /// The watermark could not be read.
    pub fn take_over(&mut self) -> Result<TakeOver, SinkError> {
        let watermark = self.sink.get_i64(&self.cfg.watermark_key)?;
        self.watermark_ms = watermark;
        if let Some(w) = watermark {
            metrics().watermark_ms.set(w);
        }
        let slack = i64::try_from(self.cfg.catch_up_slack.as_millis()).unwrap_or(i64::MAX);
        let covered = watermark.is_some_and(|w| self.retained_since_ms <= w.saturating_sub(slack));
        let from = watermark.map_or(i64::MIN, |w| w.saturating_sub(slack));
        let retained = std::mem::take(&mut self.retained);
        let truncates = std::mem::take(&mut self.retained_truncates);
        self.role = Role::Leader;
        self.clear_pending();
        if covered {
            for (k, ts) in retained {
                // ts == 0: payload without a timestamp; keep it to be safe.
                if ts == 0 || ts >= from {
                    self.pending.insert(k, ts);
                }
            }
            self.pending_tables.extend(truncates);
            if !self.is_empty() {
                self.first_at = Some(Instant::now());
            }
            self.needs_catch_up = false;
            self.catch_up_plan = None;
            Ok(TakeOver::Gapless)
        } else {
            self.needs_catch_up = true;
            self.catch_up_plan = None;
            Ok(TakeOver::CatchUp)
        }
    }

    /// Return to standby (lost the session); retained keys start empty from now.
    pub fn step_down(&mut self) {
        self.role = Role::Standby;
        self.clear_pending();
        self.retained.clear();
        self.retained_truncates.clear();
        self.retained_since_ms = now_ms();
        self.catch_up_plan = None;
        metrics().leader.set(0);
        log::info!("lifereflector: event=leader state=released");
    }

    /// Bring the cache back in line after missed notifications (see the module docs), all at
    /// once. The service loop uses [`Engine::catch_up_step`] instead, so live invalidations keep
    /// flowing while a large cache is scanned.
    ///
    /// # Errors
    ///
    /// A cache operation failed; the catch-up stays scheduled.
    pub fn catch_up(&mut self, ranged: &mut RangedQuery<'_>) -> Result<(), SinkError> {
        while !self.catch_up_step(ranged)? {}
        Ok(())
    }

    /// Do one unit of catch-up work: plan it on the first call (from the watermark), then one
    /// `SCAN` page or one ranged query per call. Returns `true` when the catch-up is complete.
    ///
    /// `ranged` runs the change-column query for tables configured with one; it receives the
    /// table, its change column, the lower bound in ms and a row limit. A table whose query fails
    /// or returns more than `max_pending` rows is flushed instead.
    ///
    /// # Errors
    ///
    /// A cache operation failed; the step is retried on the next call.
    pub fn catch_up_step(&mut self, ranged: &mut RangedQuery<'_>) -> Result<bool, SinkError> {
        if !self.needs_catch_up {
            return Ok(true);
        }
        if self.catch_up_plan.is_none() {
            self.catch_up_plan = Some(self.plan_catch_up()?);
        }
        let m = metrics();
        let Some(plan) = self.catch_up_plan.as_mut() else {
            return Ok(true);
        };
        let Some(task) = plan.tasks.front_mut() else {
            // Everything up to the catch-up start is now covered.
            let mark = plan.started_ms.max(self.watermark_ms.unwrap_or(i64::MIN));
            self.watermark_ms = Some(mark);
            self.watermark_dirty = true;
            m.watermark_ms.set(mark);
            self.catch_up_plan = None;
            self.needs_catch_up = false;
            return Ok(true);
        };
        match task {
            CatchUpTask::Scan {
                pattern,
                cursor,
                mode,
            } => {
                let (next, n) = self.sink.invalidate_matching_page(pattern, *cursor)?;
                m.catch_up_keys.inc_by(n);
                if next == 0 {
                    m.catch_ups.with_label_values(&[*mode]).inc();
                    log::info!("lifereflector: event=catch_up mode={mode} done");
                    plan.tasks.pop_front();
                } else {
                    *cursor = next;
                }
            }
            CatchUpTask::Ranged {
                spec,
                column,
                since_ms,
            } => {
                let (spec, column, since_ms) = (spec.clone(), column.clone(), *since_ms);
                let ids = match ranged(&spec, &column, since_ms, self.cfg.max_pending) {
                    Ok(ids) if ids.len() <= self.cfg.max_pending => Some(ids),
                    Ok(_) => None,
                    Err(e) => {
                        log::warn!(
                            "lifereflector: event=catch_up mode=ranged table={} failed ({e}); flushing it",
                            spec.table
                        );
                        None
                    }
                };
                let keys: Option<Vec<String>> =
                    ids.map(|ids| ids.iter().map(|id| self.key(&spec.table, id)).collect());
                let pattern = self.table_pattern(&spec.table);
                if let Some(keys) = keys {
                    let n = self.sink.invalidate(&keys)?;
                    m.catch_ups.with_label_values(&["ranged"]).inc();
                    m.catch_up_keys.inc_by(n);
                    log::info!(
                        "lifereflector: event=catch_up mode=ranged table={} keys={n} done",
                        spec.table
                    );
                    if let Some(plan) = self.catch_up_plan.as_mut() {
                        plan.tasks.pop_front();
                    }
                } else if let Some(plan) = self.catch_up_plan.as_mut() {
                    if let Some(front) = plan.tasks.front_mut() {
                        *front = CatchUpTask::Scan {
                            pattern,
                            cursor: 0,
                            mode: "flush",
                        };
                    }
                }
            }
        }
        Ok(false)
    }

    fn plan_catch_up(&mut self) -> Result<CatchUpPlan, SinkError> {
        let watermark = match self.watermark_ms {
            Some(w) => Some(w),
            None => self.sink.get_i64(&self.cfg.watermark_key)?,
        };
        let slack = i64::try_from(self.cfg.catch_up_slack.as_millis()).unwrap_or(i64::MAX);
        let mut tasks = std::collections::VecDeque::new();
        if self.cfg.tables.is_empty() {
            // Nothing says which tables are cached: everything under the prefix goes.
            tasks.push_back(CatchUpTask::Scan {
                pattern: format!("{}*", self.cfg.key_prefix),
                cursor: 0,
                mode: "full",
            });
        }
        for spec in &self.cfg.tables {
            match (&spec.catch_up, watermark) {
                (CatchUp::Ranged { column }, Some(w)) => tasks.push_back(CatchUpTask::Ranged {
                    spec: spec.clone(),
                    column: column.clone(),
                    since_ms: w.saturating_sub(slack),
                }),
                _ => tasks.push_back(CatchUpTask::Scan {
                    pattern: self.table_pattern(&spec.table),
                    cursor: 0,
                    mode: "flush",
                }),
            }
        }
        Ok(CatchUpPlan {
            tasks,
            started_ms: now_ms(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::MemorySink;

    fn cfg(f: impl FnOnce(&mut Config)) -> Arc<Config> {
        let mut c = Config::new("pg", "redis");
        c.key_prefix = "lg:".into();
        c.batch_max = 4;
        c.max_pending = 100;
        f(&mut c);
        Arc::new(c)
    }

    fn change(table: &str, id: &str, ts: i64) -> Change {
        Change {
            table: table.into(),
            op: Op::Update,
            id: id.into(),
            version: Some(ts),
            ts_ms: Some(ts),
        }
    }

    /// Engine whose sink is shared with the test through an `Arc`.
    struct Shared(Arc<MemorySink>);
    impl CacheSink for Shared {
        fn invalidate(&self, k: &[String]) -> Result<u64, SinkError> {
            self.0.invalidate(k)
        }
        fn invalidate_matching_page(&self, p: &str, c: i64) -> Result<(i64, u64), SinkError> {
            self.0.invalidate_matching_page(p, c)
        }
        fn get_i64(&self, k: &str) -> Result<Option<i64>, SinkError> {
            self.0.get_i64(k)
        }
        fn set_i64(&self, k: &str, v: i64) -> Result<(), SinkError> {
            self.0.set_i64(k, v)
        }
        fn ping(&self) -> Result<(), SinkError> {
            self.0.ping()
        }
        fn eviction_policy(&self) -> Option<String> {
            self.0.eviction_policy()
        }
    }

    /// A cache connection that goes cold: the first operation after `cold_after` without a round
    /// trip pays `penalty` first (as the pilot's one slow flush after a quiet spell did).
    struct ColdSink {
        inner: Arc<MemorySink>,
        last: std::sync::Mutex<Instant>,
        cold_after: Duration,
        penalty: Duration,
    }
    impl ColdSink {
        fn touch(&self) {
            let mut last = self.last.lock().unwrap();
            if last.elapsed() >= self.cold_after {
                std::thread::sleep(self.penalty);
            }
            *last = Instant::now();
        }
    }
    impl CacheSink for ColdSink {
        fn invalidate(&self, k: &[String]) -> Result<u64, SinkError> {
            self.touch();
            self.inner.invalidate(k)
        }
        fn invalidate_matching_page(&self, p: &str, c: i64) -> Result<(i64, u64), SinkError> {
            self.touch();
            self.inner.invalidate_matching_page(p, c)
        }
        fn get_i64(&self, k: &str) -> Result<Option<i64>, SinkError> {
            self.touch();
            self.inner.get_i64(k)
        }
        fn set_i64(&self, k: &str, v: i64) -> Result<(), SinkError> {
            self.touch();
            self.inner.set_i64(k, v)
        }
        fn ping(&self) -> Result<(), SinkError> {
            self.touch();
            self.inner.ping()
        }
        fn eviction_policy(&self) -> Option<String> {
            self.inner.eviction_policy()
        }
    }

    /// The first flush after an idle spell of four times the cold threshold, with the leader
    /// loop waking every 10 ms and calling `keep_warm` (or not, the control).
    fn first_flush_after_idle(keep_warm: bool) -> Duration {
        let cold_after = Duration::from_millis(60);
        let s = Arc::new(MemorySink::new());
        let mut e = Engine::new(
            cfg(|_| {}),
            Box::new(ColdSink {
                inner: s.clone(),
                last: std::sync::Mutex::new(Instant::now()),
                cold_after,
                penalty: Duration::from_millis(300),
            }),
        );
        e.take_over().unwrap();
        e.catch_up(&mut |_, _, _, _| Ok(vec![])).unwrap();
        e.keep_warm(Duration::ZERO).unwrap();
        let idle_until = Instant::now() + cold_after * 4;
        while Instant::now() < idle_until {
            if keep_warm {
                e.keep_warm(Duration::from_millis(20)).unwrap();
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        s.put("lg:t:1", "x");
        e.push(&change("t", "1", 1));
        let t = Instant::now();
        assert_eq!(e.flush().unwrap(), 1);
        assert!(!s.contains("lg:t:1"));
        t.elapsed()
    }

    #[test]
    fn keep_warm_keeps_the_first_flush_after_idle_fast() {
        let cold = first_flush_after_idle(false);
        assert!(
            cold >= Duration::from_millis(300),
            "control: without keep-alive the first flush after idle pays the cold penalty ({cold:?})"
        );
        let warm = first_flush_after_idle(true);
        assert!(
            warm < Duration::from_millis(50),
            "first flush after an idle spell took {warm:?} with keep-alive"
        );
    }

    #[test]
    fn keep_warm_pings_only_when_idle_long_enough() {
        let (mut e, s) = engine(cfg(|_| {}));
        assert!(e.keep_warm(Duration::ZERO).unwrap(), "ZERO always pings");
        assert!(!e.keep_warm(Duration::from_mins(1)).unwrap(), "just used");
        s.set_failing(Some("connection reset"));
        assert!(
            e.keep_warm(Duration::ZERO).is_err(),
            "a failed ping is reported"
        );
    }

    fn engine(c: Arc<Config>) -> (Engine, Arc<MemorySink>) {
        let s = Arc::new(MemorySink::new());
        (Engine::new(c, Box::new(Shared(s.clone()))), s)
    }

    #[test]
    fn leader_coalesces_and_flushes_in_one_call() {
        let (mut e, s) = engine(cfg(|_| {}));
        assert_eq!(e.take_over().unwrap(), TakeOver::CatchUp);
        e.catch_up(&mut |_, _, _, _| Ok(vec![])).unwrap();
        s.put("lg:items:1", "x");
        s.put("lg:items:2", "y");
        s.put("lg:other:1", "z");
        e.push(&change("items", "1", 10));
        e.push(&change("items", "1", 11));
        e.push(&change("items", "2", 12));
        assert_eq!(e.len(), 2);
        assert!(e.due(true));
        let before = s
            .invalidate_calls
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(e.flush().unwrap(), 2);
        assert_eq!(
            s.invalidate_calls
                .load(std::sync::atomic::Ordering::Relaxed)
                - before,
            1
        );
        assert!(!s.contains("lg:items:1") && !s.contains("lg:items:2"));
        assert!(s.contains("lg:other:1"));
        e.persist_watermark(Duration::ZERO).unwrap();
        assert_eq!(
            s.get_i64("lifereflector:watermark").unwrap(),
            Some(e.watermark_ms.unwrap())
        );
    }

    #[test]
    fn batch_full_is_due_before_queue_drains() {
        let (mut e, _s) = engine(cfg(|c| c.batch_delay = Duration::from_mins(1)));
        e.take_over().unwrap();
        for i in 0..3 {
            e.push(&change("t", &i.to_string(), 1));
        }
        assert!(!e.due(true), "delay not reached and batch not full");
        e.push(&change("t", "3", 1));
        assert!(e.due(false), "full batch");
    }

    #[test]
    fn failed_flush_keeps_the_batch() {
        let (mut e, s) = engine(cfg(|_| {}));
        e.take_over().unwrap();
        s.put("lg:t:1", "x");
        e.push(&change("t", "1", 5));
        s.set_failing(Some("down"));
        assert!(e.flush().is_err());
        assert_eq!(e.consecutive_failures(), 1);
        assert_eq!(e.len(), 1);
        s.set_failing(None);
        assert_eq!(e.flush().unwrap(), 1);
        assert!(!s.contains("lg:t:1"));
        assert_eq!(e.consecutive_failures(), 0);
    }

    #[test]
    fn flush_is_timed_and_notify_to_applied_counts_successes_only() {
        use microscaler_timing::Outcome::{Err as Failed, Ok as Done};
        let count = |op: &str, outcome: microscaler_timing::Outcome| {
            microscaler_timing::snapshot()
                .into_iter()
                .filter(|r| r.component == "lifereflector" && r.op == op && r.outcome == outcome)
                .map(|r| r.count())
                .sum::<u64>()
        };
        // Other tests flush concurrently in this process: counts only ever grow by at least ours.
        let (applied, flush_ok, flush_err) = (
            count("notify_to_applied", Done),
            count("flush", Done),
            count("flush", Failed),
        );
        let (mut e, s) = engine(cfg(|_| {}));
        e.take_over().unwrap();
        e.push(&change("t", "9", 5));
        s.set_failing(Some("down"));
        assert!(e.flush().is_err());
        s.set_failing(None);
        assert_eq!(e.flush().unwrap(), 1);
        assert!(count("flush", Failed) > flush_err);
        assert!(count("flush", Done) > flush_ok);
        assert!(count("notify_to_applied", Done) > applied);
        let text = crate::metrics::metrics().scrape_text();
        assert!(text.contains(
            r#"op_duration_seconds_count{component="lifereflector",op="flush",outcome="ok"}"#
        ));
    }

    #[test]
    fn overflow_schedules_catch_up() {
        let (mut e, _s) = engine(cfg(|c| c.max_pending = 3));
        e.take_over().unwrap();
        e.catch_up(&mut |_, _, _, _| Ok(vec![])).unwrap();
        assert!(!e.needs_catch_up());
        for i in 0..5 {
            e.push(&change("t", &i.to_string(), 1));
        }
        assert!(e.needs_catch_up());
        assert!(e.len() < 3);
    }

    #[test]
    fn truncate_flushes_the_table() {
        let (mut e, s) = engine(cfg(|_| {}));
        e.take_over().unwrap();
        s.put("lg:t:1", "x");
        s.put("lg:t:2", "x");
        s.put("lg:u:1", "x");
        e.push(&Change {
            table: "t".into(),
            op: Op::Truncate,
            id: String::new(),
            version: None,
            ts_ms: None,
        });
        e.flush().unwrap();
        assert!(!s.contains("lg:t:1") && !s.contains("lg:t:2"));
        assert!(s.contains("lg:u:1"));
    }

    #[test]
    fn catch_up_ranged_and_flush() {
        let (mut e, s) = engine(cfg(|c| {
            c.tables = vec![
                TableSpec {
                    table: "r".into(),
                    catch_up: CatchUp::Ranged {
                        column: "changed_at".into(),
                    },
                },
                TableSpec {
                    table: "f".into(),
                    catch_up: CatchUp::Flush,
                },
            ];
        }));
        s.set_i64("lifereflector:watermark", 10_000).unwrap();
        for k in ["lg:r:1", "lg:r:2", "lg:f:1", "lg:f:2", "lg:x:1"] {
            s.put(k, "v");
        }
        assert_eq!(e.take_over().unwrap(), TakeOver::CatchUp);
        let mut seen = None;
        e.catch_up(&mut |spec, col, since, _| {
            seen = Some((spec.table.clone(), col.to_owned(), since));
            Ok(vec!["2".into()])
        })
        .unwrap();
        assert_eq!(seen, Some(("r".into(), "changed_at".into(), 5_000)));
        assert!(
            s.contains("lg:r:1"),
            "ranged catch-up only touches changed rows"
        );
        assert!(!s.contains("lg:r:2"));
        assert!(
            !s.contains("lg:f:1") && !s.contains("lg:f:2"),
            "flush tables are emptied"
        );
        assert!(s.contains("lg:x:1"), "unconfigured tables are left alone");
        assert!(!e.needs_catch_up());
    }

    #[test]
    fn catch_up_steps_interleave_with_live_flushes() {
        let (mut e, s) = engine(cfg(|c| {
            c.tables = vec![TableSpec {
                table: "f".into(),
                catch_up: CatchUp::Flush,
            }];
        }));
        s.put("lg:f:1", "v");
        s.put("lg:live:1", "v");
        e.take_over().unwrap();
        assert!(e.needs_catch_up());
        // A live change arrives before the catch-up has run: it is flushed on its own.
        e.push(&change("live", "1", now_ms()));
        e.flush().unwrap();
        assert!(!s.contains("lg:live:1"));
        assert!(
            e.needs_catch_up(),
            "live flush does not finish the catch-up"
        );
        assert!(
            e.watermark_ms.is_none(),
            "watermark does not move while a catch-up is pending"
        );
        while !e.catch_up_step(&mut |_, _, _, _| Ok(vec![])).unwrap() {}
        assert!(!s.contains("lg:f:1"));
        assert!(!e.needs_catch_up());
        assert!(e.watermark_ms.is_some());
    }

    #[test]
    fn ranged_without_watermark_flushes() {
        let (mut e, s) = engine(cfg(|c| {
            c.tables = vec![TableSpec {
                table: "r".into(),
                catch_up: CatchUp::Ranged { column: "c".into() },
            }];
        }));
        s.put("lg:r:1", "v");
        e.take_over().unwrap();
        e.catch_up(&mut |_, _, _, _| panic!("no watermark: must not run a ranged query"))
            .unwrap();
        assert!(!s.contains("lg:r:1"));
    }

    #[test]
    fn standby_retained_keys_make_take_over_gapless() {
        let (mut e, s) = engine(cfg(|c| c.catch_up_slack = Duration::from_millis(0)));
        // Standby started listening now; the old leader's watermark is later.
        let start = now_ms();
        e.push(&change("t", "old", start - 1_000_000)); // before the watermark: already handled
        e.push(&change("t", "new", start + 10));
        s.set_i64("lifereflector:watermark", start + 5).unwrap();
        s.put("lg:t:old", "v");
        s.put("lg:t:new", "v");
        assert_eq!(e.take_over().unwrap(), TakeOver::Gapless);
        assert!(!e.needs_catch_up());
        e.flush().unwrap();
        assert!(s.contains("lg:t:old"), "handled by the previous leader");
        assert!(!s.contains("lg:t:new"));
    }

    #[test]
    fn standby_without_coverage_needs_catch_up() {
        let (mut e, s) = engine(cfg(|_| {}));
        // Watermark older than when the standby began listening (minus slack): a gap.
        s.set_i64("lifereflector:watermark", now_ms() - 60_000)
            .unwrap();
        assert_eq!(e.take_over().unwrap(), TakeOver::CatchUp);
        assert!(e.needs_catch_up());
    }

    #[test]
    fn standby_memory_is_bounded() {
        let (mut e, _s) = engine(cfg(|c| c.max_pending = 3));
        for i in 0..10 {
            e.push(&change("t", &i.to_string(), 1));
        }
        assert!(e.len() <= 3);
    }

    #[test]
    fn invalid_payload_is_counted_not_fatal() {
        let (mut e, _s) = engine(cfg(|_| {}));
        let before = metrics().invalid_payloads.get();
        e.push_payload("{oops");
        assert_eq!(metrics().invalid_payloads.get(), before + 1);
        assert!(e.is_empty());
    }

    #[test]
    fn per_table_notify_lag_and_apply_are_recorded() {
        let (mut e, _s) = engine(cfg(|_| {}));
        e.take_over().unwrap();
        e.catch_up(&mut |_, _, _, _| Ok(vec![])).unwrap();
        let m = metrics();
        let lag = m.table_lag_seconds.with_label_values(&["lagtab"]);
        let notify = m.notify_seconds.with_label_values(&["lagtab"]);
        let (l0, s0, n0) = (
            lag.get_sample_count(),
            lag.get_sample_sum(),
            notify.get_sample_count(),
        );
        let a0 = m.apply_seconds.get_sample_count();
        let t = now_ms() - 40;
        e.push(&change("lagtab", "1", t));
        e.push(&change("lagtab", "2", t + 10));
        e.flush().unwrap();
        assert_eq!(notify.get_sample_count() - n0, 2, "one per notification");
        assert_eq!(lag.get_sample_count() - l0, 1, "one per table per batch");
        assert!(
            lag.get_sample_sum() - s0 >= 0.039,
            "the table's oldest change"
        );
        assert!(m.apply_seconds.get_sample_count() > a0);
        // A batch without the table records nothing for it.
        e.push(&change("lagtab_other", "1", now_ms()));
        e.flush().unwrap();
        assert_eq!(lag.get_sample_count() - l0, 1);
    }

    #[test]
    fn table_labels_are_bounded() {
        let (mut e, _s) = engine(cfg(|c| c.max_pending = 10_000));
        for i in 0..crate::metrics::MAX_TABLES + 10 {
            e.push(&change(&format!("bound_{i}"), "1", now_ms()));
        }
        assert_eq!(e.table_ix.len(), crate::metrics::MAX_TABLES + 1);
        assert!(e.table_ix.contains_key(crate::metrics::OTHER_TABLE));
        assert_eq!(e.tables.len(), e.table_ix.len());
    }

    #[test]
    fn payload_size_is_observed() {
        let (mut e, _s) = engine(cfg(|_| {}));
        let before = metrics().payload_bytes.get_sample_count();
        e.push_payload(r#"{"table":"items","op":"UPDATE","id":1,"v":1,"ts":1}"#);
        assert!(metrics().payload_bytes.get_sample_count() > before);
    }
}
