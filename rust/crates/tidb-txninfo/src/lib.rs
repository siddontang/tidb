// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Transaction-history summary metadata from `pkg/session/txninfo/summary.go`.
//!
//! TiDB identifies a completed transaction by the FNV-1a digest of its SQL
//! digest sequence and keeps the most recent distinct sequences in an LRU
//! cache. The process recorder owns synchronization, duration admission and
//! rendering; session/storage completion and server configuration share it.

use std::collections::{hash_map::Entry, HashMap, VecDeque};

/// One retained SQL-digest sequence and its transaction digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionSummary {
    digest: u64,
    sql_digests: Vec<String>,
}

impl TransactionSummary {
    /// Returns the FNV-1a digest used as the source cache key.
    #[must_use]
    pub const fn digest(&self) -> u64 {
        self.digest
    }

    /// Returns the SQL digest sequence in source order.
    #[must_use]
    pub fn sql_digests(&self) -> &[String] {
        &self.sql_digests
    }

    /// Returns the lower-case hexadecimal digest shown by TiDB.
    #[must_use]
    pub fn digest_hex(&self) -> String {
        format!("{:x}", self.digest)
    }
}

/// LRU cache for completed transaction SQL-digest sequences.
#[derive(Clone, Debug, Default)]
pub struct TransactionSummaryCache {
    capacity: usize,
    entries: HashMap<u64, TransactionSummary>,
    /// Keys from most recently used to least recently used.
    lru: VecDeque<u64>,
}

impl TransactionSummaryCache {
    /// Creates an empty cache with the source capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    /// Returns the configured maximum number of distinct summaries.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Records a completed transaction and updates its LRU position.
    pub fn on_transaction_end<I, S>(&mut self, sql_digests: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let sql_digests: Vec<String> = sql_digests
            .into_iter()
            .map(|digest| digest.as_ref().to_owned())
            .collect();
        let key = fnv1a_digest(&sql_digests);

        let is_new = match self.entries.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(TransactionSummary {
                    digest: key,
                    sql_digests,
                });
                true
            }
            Entry::Occupied(_) => false,
        };
        if !is_new {
            self.remove_lru_key(key);
        }
        self.lru.push_front(key);

        if self.lru.len() > self.capacity {
            if let Some(evicted) = self.lru.pop_back() {
                self.entries.remove(&evicted);
            }
        }
    }

    /// Resizes the cache and evicts least-recently-used entries as needed.
    pub fn resize(&mut self, capacity: usize) {
        self.capacity = capacity;
        while self.lru.len() > self.capacity {
            if let Some(evicted) = self.lru.pop_back() {
                self.entries.remove(&evicted);
            }
        }
    }

    /// Returns summaries from most recently used to least recently used.
    #[must_use]
    pub fn summaries(&self) -> Vec<TransactionSummary> {
        self.lru
            .iter()
            .filter_map(|key| self.entries.get(key).cloned())
            .collect()
    }

    fn remove_lru_key(&mut self, key: u64) {
        if let Some(index) = self.lru.iter().position(|candidate| *candidate == key) {
            self.lru.remove(index);
        }
    }
}

/// Computes the source FNV-1a 64-bit digest over concatenated SQL digests.
fn fnv1a_digest(sql_digests: &[String]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for digest in sql_digests {
        for byte in digest.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3_u64);
        }
    }
    hash
}

/// Go TrxHistoryRecorder: one synchronized policy and cache per process.
#[derive(Debug)]
pub struct TransactionHistoryRecorder {
    state: std::sync::Mutex<HistoryState>,
    /// The recorders [`Self::fork`] made, which follow this one's settings.
    forks: std::sync::Mutex<Vec<std::sync::Weak<TransactionHistoryRecorder>>>,
}

#[derive(Debug)]
struct HistoryState {
    min_duration: std::time::Duration,
    summaries: TransactionSummaryCache,
}

impl TransactionHistoryRecorder {
    /// Creates a recorder; Go's unconfigured recorder has capacity zero.
    pub fn new(capacity: usize, min_duration: std::time::Duration) -> Self {
        Self {
            state: std::sync::Mutex::new(HistoryState {
                min_duration,
                summaries: TransactionSummaryCache::new(capacity),
            }),
            forks: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// A recorder of its own with this one's settings, which it keeps
    /// following: a node serving several keyspaces gives each keyspace's
    /// process list one, so `TRX_SUMMARY` holds only that keyspace's
    /// transactions (multi-keyspace design §6.8).
    pub fn fork(&self) -> std::sync::Arc<Self> {
        let (capacity, min_duration) = {
            let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            (state.summaries.capacity(), state.min_duration)
        };
        let fork = std::sync::Arc::new(Self::new(capacity, min_duration));
        let mut forks = self.forks.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        forks.retain(|fork| fork.strong_count() > 0);
        forks.push(std::sync::Arc::downgrade(&fork));
        fork
    }

    fn each_fork(&self, apply: impl Fn(&Self)) {
        let forks: Vec<_> = self
            .forks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .collect();
        for fork in forks {
            apply(&fork);
        }
    }

    /// Records an ended transaction using the physical milliseconds of its TSO.
    pub fn on_transaction_end(&self, start_ts: u64, digests: Vec<String>) {
        self.on_transaction_end_at(start_ts, digests, std::time::SystemTime::now());
    }

    /// Explicit observation time for deterministic source-contract checks.
    pub fn on_transaction_end_at(
        &self,
        start_ts: u64,
        digests: Vec<String>,
        now: std::time::SystemTime,
    ) {
        let start = tikv_client::oracle::get_time_from_ts(start_ts);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Go compares a signed duration, so a future TSO fails even at zero.
        if now
            .duration_since(start)
            .is_ok_and(|duration| duration >= state.min_duration)
        {
            state.summaries.on_transaction_end(digests);
        }
    }

    /// Changes duration admission without discarding retained history.
    pub fn set_min_duration(&self, duration: std::time::Duration) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .min_duration = duration;
        self.each_fork(|fork| fork.set_min_duration(duration));
    }

    /// Resizes immediately, evicting oldest entries; zero clears and disables retention.
    pub fn resize(&self, capacity: usize) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .summaries
            .resize(capacity);
        self.each_fork(|fork| fork.resize(capacity));
    }

    /// Returns source FNV digests and ordered JSON SQL-digest arrays, newest first.
    pub fn rows(&self) -> Vec<Vec<tidb_datatype::Datum>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .summaries
            .summaries()
            .into_iter()
            .map(|summary| {
                vec![
                    tidb_datatype::Datum::new_string_datum(summary.digest_hex()),
                    tidb_datatype::Datum::new_string_datum(
                        serde_json::to_string(summary.sql_digests())
                            .expect("digest strings serialize"),
                    ),
                ]
            })
            .collect()
    }
}

/// Go txninfo.Recorder, initialized from canonical configuration at startup.
pub static RECORDER: std::sync::LazyLock<std::sync::Arc<TransactionHistoryRecorder>> =
    std::sync::LazyLock::new(|| {
        std::sync::Arc::new(TransactionHistoryRecorder::new(
            0,
            std::time::Duration::from_secs(1),
        ))
    });

#[cfg(test)]
mod fork_tests {
    use super::TransactionHistoryRecorder;
    use std::time::{Duration, SystemTime};

    /// Design §6.8: each keyspace of a multi-keyspace node records into a
    /// fork of the process's recorder, so one keyspace's transactions never
    /// show in another's `TRX_SUMMARY`; the forks keep following the
    /// process's capacity and minimum duration.
    #[test]
    fn forks_record_apart_and_follow_the_process_settings() {
        let process = TransactionHistoryRecorder::new(0, Duration::from_secs(1));
        let (a, b) = (process.fork(), process.fork());
        let millis = (SystemTime::now() - Duration::from_secs(10))
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let ts = u64::try_from(millis).unwrap() << 18;
        a.on_transaction_end(ts, vec!["d1".into()]);
        assert!(a.rows().is_empty(), "capacity zero records nothing");
        process.resize(8);
        process.set_min_duration(Duration::ZERO);
        a.on_transaction_end(ts, vec!["d1".into()]);
        b.on_transaction_end(ts, vec!["d2".into()]);
        b.on_transaction_end(ts, vec!["d3".into()]);
        assert_eq!(a.rows().len(), 1);
        assert_eq!(b.rows().len(), 2);
        assert!(process.rows().is_empty());
        drop(a);
        process.resize(1);
        assert_eq!(b.rows().len(), 1);
    }
}

