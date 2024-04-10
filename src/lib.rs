#![feature(new_uninit, allocator_api)]

#[cfg(test)]
mod tests;

mod bloom_filter;
pub use bloom_filter::BloomFilter;

mod cache;
pub use cache::Cache;

mod disk_cache;
use disk_cache::DiskCache;

use std::{collections::HashMap, hash::Hash, time::Duration};

#[derive(Debug)]
pub struct Syncer<K: Clone + Eq + Hash, V: Clone, D, const N: usize> {
    inner: tokio::sync::Mutex<Inner<K, V, D, N>>,
}

impl<K, V, D, const N: usize> Syncer<K, V, D, N>
where
    K: Hash + Clone + Eq + TryFrom<String>,
    V: Hash + Clone,
    D: DiskCache<Key = K, Value = V>,
{
    pub fn new(disk_cache: D, items_count: usize, fp_p: f64, timeout: Duration) -> Self {
        Self {
            inner: tokio::sync::Mutex::new(Inner::new(disk_cache, items_count, fp_p, timeout)),
        }
    }

    pub async fn load(&self, key: K) -> anyhow::Result<SyncStatus<K, V>> {
        println!("Syncer load");
        self.inner.lock().await.load(key).await
    }

    pub async fn store(&self, index: K, cache: V) -> anyhow::Result<()> {
        self.inner.lock().await.store(index, cache).await
    }

    pub async fn init_bloom_filter(&self) -> anyhow::Result<()> {
        self.inner.lock().await.init_bloom_filter().await
    }
}

#[derive(Debug)]
struct Inner<K: Clone + Eq, V: Clone, D, const N: usize> {
    bloom_filter: BloomFilter<K>,
    hot_cache: Cache<CacheEnrty<K, V>, N>,
    disk_cache: D,

    in_process: HashMap<K, ProcessEntry>,
    timeout: Duration,
}

impl<K, V, D, const N: usize> Inner<K, V, D, N>
where
    K: Hash + Clone + Eq + TryFrom<String>,
    V: Hash + Clone,
    D: DiskCache<Key = K, Value = V>,
{
    fn new(disk_cache: D, items_count: usize, fp_p: f64, timeout: Duration) -> Self {
        let hot_cache: Cache<CacheEnrty<K, V>, N> = Cache::default();
        let bloom_filter: BloomFilter<K> = BloomFilter::new_for_fp_rate(items_count, fp_p);
        Self {
            bloom_filter,
            hot_cache,
            disk_cache,
            in_process: HashMap::new(),
            timeout,
        }
    }

    async fn store(&mut self, key: K, cache: V) -> anyhow::Result<()> {
        // set bloom filter
        self.bloom_filter.set(&key);

        // remove from in-process map
        if let Some((key, ProcessEntry { frequency, .. })) = self.in_process.remove_entry(&key) {
            // insert into hot cache
            self.hot_cache
                .insert(CacheEnrty::new(key, cache.clone()), frequency);
        }

        Ok(self.disk_cache.store(key, cache).await?)
    }

    async fn load(&mut self, key: K) -> anyhow::Result<SyncStatus<K, V>> {
        // check bloom filter
        println!("syncer inner load: wait for check_and_set bloom filter");
        if !self.bloom_filter.check_and_set(&key) {
            // TODO:
            // check disk cache & update state.
            // if we already init bloom filter, we can promise it not exist in disk.
            // so we can skip this.
            if self.disk_cache.exist(&key).await {
                println!("syncer inner load: not exist in bloom filter, load_from_disk");
                return self.load_from_disk(key).await;
            }
            println!("syncer inner load: not exist in bloom filter & disk, Need Sync");
            return Ok(self.add_sync_process(key));
        }
        println!("syncer inner load: check_and_set bloom filter");

        // check hot cache
        if let Some(cache) = self.load_from_hot_cache(&key) {
            return Ok(SyncStatus::Synced(cache));
        }
        println!("syncer inner load: load_from_hot_cache");

        // check in-process sync task
        if let Some(status) = self.check_process(&key) {
            return Ok(status);
        }
        println!("syncer inner load: check_process");

        println!("syncer inner load: wait for load_from_disk");
        // check disk cache
        self.load_from_disk(key).await
    }

    async fn load_from_disk(&mut self, key: K) -> anyhow::Result<SyncStatus<K, V>> {
        let status = match self.disk_cache.load(&key).await? {
            Some(value) => {
                // insert into lru hot cache
                self.hot_cache
                    .insert_into_lru(CacheEnrty::new(key, value.clone()));
                SyncStatus::Synced(value)
            }
            None => self.add_sync_process(key),
        };

        Ok(status)
    }

    fn load_from_hot_cache(&mut self, key: &K) -> Option<V> {
        self.hot_cache
            .find(|item| &item.key == key)
            .map(|item| item.value.clone())
    }

    fn check_process(&mut self, key: &K) -> Option<SyncStatus<K, V>> {
        if let Some(in_process) = self.in_process.get_mut(key) {
            in_process.frequency += 1;

            if in_process.instant.elapsed() >= self.timeout {
                // timeout, reset process instant & return NeedSync
                in_process.instant = std::time::Instant::now();
                return Some(SyncStatus::NeedSync(key.clone()));
            }

            return Some(SyncStatus::AlreadyInProcess);
        }

        None
    }

    fn add_sync_process(&mut self, key: K) -> SyncStatus<K, V> {
        self.in_process.insert(
            key.clone(),
            ProcessEntry {
                instant: std::time::Instant::now(),
                frequency: 1,
            },
        );
        SyncStatus::NeedSync(key)
    }

    async fn init_bloom_filter(&mut self) -> anyhow::Result<()> {
        let mut dirs = vec![];

        let disk_dir = self.disk_cache.directory();
        let mut disk_dir = tokio::fs::read_dir(disk_dir).await.unwrap();
        while let Ok(Some(dir_entry)) = disk_dir.next_entry().await {
            if let Ok(file_type) = dir_entry.file_type().await {
                if file_type.is_dir() {
                    dirs.push(dir_entry.path())
                }
            }
        }

        let keys: Vec<_> = tokio::task::spawn_blocking(move || {
            dirs.iter()
                .filter_map(|dir| std::fs::read_dir(dir).ok())
                .flatten()
                .map(Result::unwrap)
                .filter(|dir| dir.file_type().unwrap().is_file())
                .filter_map(|entry| entry.file_name().into_string().ok())
                .collect()
        })
        .await?;

        keys.into_iter()
            .filter_map(|k| k.try_into().ok())
            .for_each(|key| self.bloom_filter.set(&key));

        Ok(())
    }
}

#[derive(Debug)]
struct ProcessEntry {
    instant: std::time::Instant,
    frequency: i16,
}

#[derive(Clone, Debug)]
struct CacheEnrty<K: Clone, V: Clone> {
    key: K,
    value: V,
}

impl<K: Clone, V: Clone> CacheEnrty<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

#[derive(Debug)]
pub enum SyncStatus<K, V> {
    AlreadyInProcess,
    NeedSync(K),
    Synced(V),
}
