pub trait DiskCache {
    type Key: std::hash::Hash;
    type Value;
    type Error: std::error::Error + Send + Sync + 'static;

    fn load(
        &self,
        key: &Self::Key,
    ) -> impl std::future::Future<Output = Result<Option<Self::Value>, Self::Error>> + Send;
    fn store(
        &mut self,
        key: Self::Key,
        value: Self::Value,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
    fn exist(&self, key: &Self::Key) -> impl std::future::Future<Output = bool> + Send;
    fn directory(&self) -> &std::path::Path;
}
