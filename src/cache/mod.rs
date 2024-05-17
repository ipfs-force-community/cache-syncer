mod lfru;
pub use lfru::LfruCache;

pub trait Cache<T>: Default {
    fn insert(&mut self, val: T);
    fn insert_with_weight(&mut self, val: T, weight: usize);
    fn find<F: FnMut(&T) -> bool>(&mut self, pred: F) -> Option<&T>;
    fn lookup<F: FnMut(&T) -> bool>(&mut self, pred: F) -> Option<&T>;
}
