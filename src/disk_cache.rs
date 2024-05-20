pub trait DiskCache<K, V> {
    type Error: std::error::Error + Send + Sync + 'static;

    fn load(
        &self,
        key: &K,
    ) -> impl std::future::Future<Output = Result<Option<V>, Self::Error>> + Send;
    fn store(
        &mut self,
        key: &K,
        value: V,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
    fn exist(&self, key: &K) -> impl std::future::Future<Output = bool> + Send;
    fn directory(&self) -> &std::path::Path;
}
