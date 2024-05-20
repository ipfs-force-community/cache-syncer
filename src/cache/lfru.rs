use super::Cache;

#[derive(Debug)]
pub struct LfruCache<T, const FN: usize, const RN: usize> {
    pub(self) entries: arrayvec::ArrayVec<Entry<T>, FN>,
    pub(self) lru_cache: uluru::LRUCache<T, RN>,
    /// Index of the min entry. Because our frequency will only decrease, so we don't need to store the index `head`.
    tail: u16,
    min: i16,
}

#[derive(Debug)]
struct Entry<T> {
    val: T,
    /// The frequency will only decrease.
    pre_freq: i16,
}

impl<T, const FN: usize, const RN: usize> Cache<T> for LfruCache<T, FN, RN> {
    fn insert(&mut self, val: T) {
        self._insert(val, 0)
    }

    fn insert_with_weight(&mut self, val: T, weight: usize) {
        self._insert(val, weight)
    }

    fn find<F: FnMut(&T) -> bool>(&mut self, pred: F) -> Option<&T> {
        self._find(pred)
    }

    fn lookup<F: FnMut(&T) -> bool>(&mut self, pred: F) -> Option<&T> {
        self._lookup(pred)
    }
}

impl<T, const FN: usize, const RN: usize> LfruCache<T, FN, RN> {
    /// Insert a given key in the cache.
    ///
    /// If the cache is full, the min item will be removed and returned.
    fn _insert(&mut self, val: T, weight: usize) {
        let pre_freq = weight.try_into().unwrap_or(i16::MAX);
        let entry = Entry { val, pre_freq };
        // pre_freq < min
        // if len == cap return pre_freq
        // if len != cap min = pre_freq & push back
        //
        // pre_freq >= min
        // if len == cap replace
        // if len != cap push back
        if self.entries.capacity() == self.entries.len() {
            if self.min >= pre_freq {
                self.insert_into_lru(entry.val);
                return;
            }
            self.min = pre_freq;
            let old = core::mem::replace(&mut self.entries[self.tail as usize], entry);
            self.insert_into_lru(old.val);
        } else {
            self.entries.push(entry);

            if self.min > pre_freq {
                self.min = pre_freq;
                self.tail = self.entries.len() as u16 - 1;

                return;
            }
        };
        // set min, has been pushed, at least one entry
        let (tail, pre_freq) = self
            .entries
            .iter()
            .map(|item| item.pre_freq)
            .enumerate()
            .min_by(|x, y| (x.1).cmp(&y.1))
            .unwrap();
        self.tail = tail as u16;
        self.min = pre_freq;
    }

    pub fn insert_into_lru(&mut self, val: T) -> Option<T> {
        self.lru_cache.insert(val)
    }

    /// Returns the first item in the cache that matches the given predicate and reduce entry.pre_freq.
    fn _lookup<F>(&mut self, mut pred: F) -> Option<&T>
    where
        F: FnMut(&T) -> bool,
    {
        for entry in self.entries.iter_mut() {
            if pred(&entry.val) {
                return Some(&entry.val);
            }
        }

        if self.lru_cache.touch(pred) {
            return self.lru_cache.front();
        }

        None
    }

    /// Returns the first item in the cache that matches the given predicate.
    fn _find<F>(&mut self, mut pred: F) -> Option<&T>
    where
        F: FnMut(&T) -> bool,
    {
        // let iter = self.entries.iter_mut().enumerate();
        for (i, entry) in self.entries.iter_mut().enumerate() {
            if pred(&entry.val) {
                entry.pre_freq -= 1;
                if entry.pre_freq < self.min {
                    self.min = entry.pre_freq;
                    self.tail = i as u16;
                }
                return Some(&entry.val);
            }
        }

        if self.lru_cache.touch(pred) {
            return self.lru_cache.front();
        }

        None
    }
}

impl<T, const FN: usize, const RN: usize> Default for LfruCache<T, FN, RN> {
    fn default() -> Self {
        let cache = LfruCache {
            entries: arrayvec::ArrayVec::new(),
            lru_cache: Default::default(),
            min: i16::MAX,
            tail: 0,
        };
        assert!(
            cache.entries.capacity() < u16::MAX as usize,
            "Capacity overflow"
        );
        cache
    }
}

#[cfg(test)]
mod tests {
    type PieceIndex = u64;
    const PIECE_SIZE: usize = 1048672;
    use super::*;

    #[derive(Clone)]
    struct PieceEnrty {
        #[allow(unused)]
        piece: Box<[u8; PIECE_SIZE]>,
        piece_index: PieceIndex,
    }

    impl PieceEnrty {
        fn new(i: u8) -> PieceEnrty {
            PieceEnrty {
                piece: Box::new([i; PIECE_SIZE]),
                piece_index: PieceIndex::from(i as u64),
            }
        }
    }

    impl std::fmt::Debug for PieceEnrty {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PieceEnrty")
                .field("piece_index", &self.piece_index)
                .finish()
        }
    }

    #[test]
    fn insert_and_find() {
        let mut cacher: LfruCache<PieceEnrty, 10, 10> = LfruCache::default();
        cacher.insert_with_weight(PieceEnrty::new(0), 0);
        let ret = cacher.find(|item| item.piece_index == 0u64.into());
        assert!(ret.is_some());

        for i in 1..=11 {
            cacher.insert_with_weight(PieceEnrty::new(i), i.into());
        }

        let ret_1 = cacher
            .find(|item| item.piece_index == 0u64.into())
            .map(|item| item.piece_index);
        let ret_2 = cacher
            .find(|item| item.piece_index == 1u64.into())
            .map(|item| item.piece_index);
        assert!(ret_1.is_some());
        assert!(ret_2.is_some());
        assert!(
            cacher.lru_cache.len() == 2,
            "lru cache len: {}",
            cacher.lru_cache.len()
        );

        for _ in 0..11 {
            let ret = cacher
                .find(|item| item.piece_index == 8u64.into())
                .map(|item| item.piece_index);
            assert!(ret.is_some());
        }

        cacher.insert_with_weight(PieceEnrty::new(0), 0);
        for i in 12..=21 {
            cacher.insert_with_weight(PieceEnrty::new(i), i.into());
        }

        let ret = cacher
            .find(|item| item.piece_index == 8u64.into())
            .map(|item| item.piece_index);
        assert!(ret.is_none(), "find: {ret:#?}");
        let ret = cacher.find(|item| item.piece_index == 0u64.into());
        assert!(ret.is_some());
    }
}

// mod compression {
//     pub(crate) fn zstd_compress(
//         source: &mut impl std::io::Read,
//         compressed: impl std::io::Write,
//         level: i32,
//     ) {
//         let mut encoder = zstd::Encoder::new(compressed, level).unwrap();

//         std::io::copy(source, &mut encoder).unwrap();
//         encoder.finish().unwrap();
//     }

//     #[cfg(test)]
//     mod test {
//         use std::io::Read;

//         #[test]
//         fn zstd_compression() {
//             #[allow(unused)]
//             #[derive(Debug)]
//             struct CompresseRate {
//                 origin_len: usize,
//                 base64_len: usize,
//                 compressed_len: usize,
//                 elapsed: std::time::Duration,
//                 rate: f64,
//             }

//             let dirs = [
//                 "./pieces-cache/67",
//                 // "./pieces-cache/515",
//                 // "./pieces-cache/1019",
//             ];

//             let mut buf: Vec<u8> = Vec::with_capacity(1048737);
//             let mut buf_str: Vec<u8> = Vec::with_capacity(1880337);
//             let mut compressed: Vec<u8> = Vec::with_capacity(1048737);

//             let rates: Vec<_> = dirs
//                 .iter()
//                 .flat_map(|dir| std::fs::read_dir(dir).unwrap())
//                 .map(Result::unwrap)
//                 .map(|entry| {
//                     use base64::Engine;
//                     let mut f = std::fs::File::open(entry.path()).unwrap();
//                     f.read_to_end(&mut buf).unwrap();
//                     let origin_len = buf.len();
//                     buf_str.resize(origin_len * 4 / 3 + 4, 0);
//                     let base64_len = base64::engine::general_purpose::STANDARD_NO_PAD.encode_slice(buf.as_slice(), buf_str.as_mut_slice()).unwrap();
//                     let instant = std::time::Instant::now();
//                     super::zstd_compress(&mut buf_str.as_slice(), &mut compressed, 13);
//                     let compressed_len = compressed.len();
//                     let elapsed = instant.elapsed();
//                     buf.clear();
//                     // buf_str.clear();
//                     compressed.clear();
//                     CompresseRate {
//                         origin_len,
//                         base64_len,
//                         compressed_len,
//                         elapsed,
//                         rate: compressed_len as f64 / origin_len as f64,
//                     }
//                 })
//                 .collect();

//             for rate in rates {
//                 println!("compress rate: {rate:?}");
//             }
//         }
//     }
// }
