use std::hash::Hash;

use crate::{default_cacher::CacheEntry, DefaultCacher, DiskCache, LfruCache};

pub struct LfuTwoQueues<K: Clone + Eq + Hash, V: Clone, D, const FN: usize, const RN: usize> {
    inner: tokio::sync::Mutex<Inner<K, V, D, FN, RN>>,
}

impl<K, V, D, const FN: usize, const RN: usize> LfuTwoQueues<K, V, D, FN, RN>
where
    K: Hash + Clone + Eq + TryFrom<String>,
    V: Hash + Clone,
    D: DiskCache<Key = K, Value = V>,
{
    pub async fn new(disk_cache: D, items_count: usize, fp_p: f64) -> anyhow::Result<Self> {
        Ok(Self {
            inner: tokio::sync::Mutex::new(Inner::new(disk_cache, items_count, fp_p).await?),
        })
    }

    pub async fn load(&self, key: &K, weight: usize) -> anyhow::Result<Option<V>> {
        self.inner.lock().await.load(key, weight).await
    }

    pub async fn lookup(&self, key: &K, weight: usize) -> anyhow::Result<Option<V>> {
        self.inner.lock().await.lookup(key, weight).await
    }

    pub async fn store(&self, index: K, value: V, weight: usize) -> anyhow::Result<()> {
        self.inner.lock().await.store(index, value, weight).await
    }
}

struct Inner<K: Clone + Eq, V: Clone, D, const FN: usize, const RN: usize> {
    cacher: DefaultCacher<K, LfruCache<CacheEntry<K, V>, FN, RN>, D>,
}

impl<K, V, D, const FN: usize, const RN: usize> Inner<K, V, D, FN, RN>
where
    K: Hash + Clone + Eq + TryFrom<String>,
    V: Hash + Clone,
    D: DiskCache<Key = K, Value = V>,
{
    async fn new(disk_cache: D, items_count: usize, fp_p: f64) -> anyhow::Result<Self> {
        let cacher = DefaultCacher::new_and_init_bloom(disk_cache, items_count, fp_p).await?;

        Ok(Self { cacher })
    }

    async fn store(&mut self, key: K, value: V, weight: usize) -> anyhow::Result<()> {
        self.cacher.store(key, value, weight).await
    }

    async fn load(&mut self, key: &K, weight: usize) -> anyhow::Result<Option<V>> {
        Ok(self
            .cacher
            .load(key, weight, DefaultCacher::load_from_hot_cache)
            .await)
    }

    async fn lookup(&mut self, key: &K, weight: usize) -> anyhow::Result<Option<V>> {
        Ok(self
            .cacher
            .load(key, weight, DefaultCacher::lookup_hot_cache)
            .await)
    }
}
