use serde::{Deserialize, Serialize};
use std::{collections::HashMap, hash::Hash, time::Duration};

use crate::{default_cacher::CacheEntry, DefaultCacher, DiskCache, LfruCache};

pub struct Syncer<K: Clone + Eq + Hash, V: Clone, D, const N: usize> {
    inner: tokio::sync::Mutex<Inner<K, V, D, N>>,
}

impl<K, V, D, const N: usize> Syncer<K, V, D, N>
where
    K: Hash + Clone + Eq + TryFrom<String>,
    V: Hash + Clone,
    D: DiskCache<Key = K, Value = V>,
{
    pub async fn new(
        disk_cache: D,
        items_count: usize,
        fp_p: f64,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner: tokio::sync::Mutex::new(
                Inner::new(disk_cache, items_count, fp_p, timeout).await?,
            ),
        })
    }

    pub async fn touch(&self, key: K) {
        self.inner.lock().await.touch(key)
    }

    pub async fn touch_many(&self, keys: Vec<K>) {
        self.inner.lock().await.touch_many(keys)
    }

    pub async fn load(&self, key: K) -> anyhow::Result<SyncStatus<K, V>> {
        self.inner.lock().await.load(key).await
    }

    pub async fn store(&self, index: K, cache: V) -> anyhow::Result<()> {
        self.inner.lock().await.store(index, cache).await
    }
}

struct Inner<K: Clone + Eq, V: Clone, D, const N: usize> {
    cacher: DefaultCacher<K, LfruCache<CacheEntry<K, V>, N, N>, D>,
    in_process: HashMap<K, ProcessEntry>,
    timeout: Duration,
}

impl<K, V, D, const N: usize> Inner<K, V, D, N>
where
    K: Hash + Clone + Eq + TryFrom<String>,
    V: Hash + Clone,
    D: DiskCache<Key = K, Value = V>,
{
    async fn new(
        disk_cache: D,
        items_count: usize,
        fp_p: f64,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        let cacher = DefaultCacher::new_and_init_bloom(disk_cache, items_count, fp_p).await?;

        Ok(Self {
            cacher,
            in_process: HashMap::new(),
            timeout,
        })
    }

    fn touch(&mut self, key: K) {
        self.in_process
            .entry(key)
            .and_modify(ProcessEntry::touch)
            .or_default();
    }

    fn touch_many(&mut self, keys: Vec<K>) {
        for key in keys {
            self.touch(key);
        }
    }

    async fn store(&mut self, key: K, value: V) -> anyhow::Result<()> {
        let frequency = self
            .in_process
            .remove_entry(&key)
            .map(|(_, entry)| entry.frequency as usize)
            .unwrap_or_default();

        self.cacher.store(key, value, frequency).await
    }

    async fn load(&mut self, key: K) -> anyhow::Result<SyncStatus<K, V>> {
        // check bloom filter
        if !self.cacher.bloom_filter.check_and_set(&key) {
            tracing::debug!("syncer inner load: not exist in bloom filter & disk, Need Sync");
            return Ok(self.add_sync_process(key));
        }

        // check hot cache
        if let Some(cache) = self.load_from_hot_cache(&key) {
            tracing::debug!("syncer inner load: load_from_hot_cache");
            return Ok(SyncStatus::Synced(cache));
        }

        // check in-process sync task
        if let Some(status) = self.check_process(&key) {
            tracing::debug!("syncer inner load: in-process");
            return Ok(status);
        }

        tracing::debug!("syncer inner load: wait for load_from_disk");
        // check disk cache
        self.load_from_disk(key).await
    }

    async fn load_from_disk(&mut self, key: K) -> anyhow::Result<SyncStatus<K, V>> {
        let status = match self.cacher.disk_cache.load(&key).await? {
            Some(value) => {
                // insert into lru hot cache
                self.cacher
                    .hot_cache
                    .insert_into_lru(CacheEntry::new(key, value.clone()));
                SyncStatus::Synced(value)
            }
            None => self.add_sync_process(key),
        };

        Ok(status)
    }

    fn load_from_hot_cache(&mut self, key: &K) -> Option<V> {
        self.cacher.load_from_hot_cache(key)
    }

    fn check_process(&mut self, key: &K) -> Option<SyncStatus<K, V>> {
        if let Some(in_process) = self.in_process.get_mut(key) {
            in_process.frequency += 1;

            if in_process.is_timeout(self.timeout) {
                // timeout, reset process instant & return NeedSync
                in_process.instant = std::time::Instant::now();
                return Some(SyncStatus::NeedSync(key.clone()));
            }

            return Some(SyncStatus::AlreadyInProcess(key.clone()));
        }

        None
    }

    fn add_sync_process(&mut self, key: K) -> SyncStatus<K, V> {
        self.in_process.insert(key.clone(), Default::default());
        SyncStatus::NeedSync(key)
    }
}

#[derive(Debug)]
struct ProcessEntry {
    instant: std::time::Instant,
    frequency: i16,
    syncer_num: u16,
}

impl Default for ProcessEntry {
    fn default() -> Self {
        Self {
            instant: std::time::Instant::now(),
            frequency: 1,
            syncer_num: 1,
        }
    }
}

impl ProcessEntry {
    fn touch(&mut self) {
        self.instant = std::time::Instant::now();
        self.frequency += 1;
    }

    fn is_timeout(&self, timeout: Duration) -> bool {
        self.instant.elapsed() >= timeout * self.back_off()
    }

    fn back_off(&self) -> u32 {
        match self.syncer_num {
            1 => 1,
            2 => 10,
            3 => 30,
            _ => 30,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SyncStatus<K, V> {
    AlreadyInProcess(K),
    NeedSync(K),
    Synced(V),
}
