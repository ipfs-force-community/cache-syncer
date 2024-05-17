#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BloomFilter<T> {
    capacity: usize,
    k_num: usize,
    sips: [siphasher::sip::SipHasher13; 2],
    bit_vec: bitvec::vec::BitVec,

    _phantom: std::marker::PhantomData<T>,
}

impl<T> BloomFilter<T> {
    pub const BLOOM_METADATA: &'static str = "bloom_metadata.bin";

    pub fn new_for_fp_rate(items_count: usize, fp_p: f64) -> Self {
        let capacity = Self::compute_capacity(items_count, fp_p);
        let mut seed = [0u8; 32];
        getrandom::getrandom(&mut seed).unwrap();
        Self::new_with_seed(capacity, items_count, &seed)
    }

    pub fn new_with_seed(capacity: usize, items_count: usize, seed: &[u8; 32]) -> Self {
        assert!(capacity > 0 && items_count > 0);
        use siphasher::sip::SipHasher13;

        let k_num = Self::optimal_k_num(capacity as u64, items_count);
        let bitmap = bitvec::vec::BitVec::repeat(false, capacity);
        let mut k1 = [0u8; 16];
        let mut k2 = [0u8; 16];
        k1.copy_from_slice(&seed[0..16]);
        k2.copy_from_slice(&seed[16..32]);
        let sips = [
            SipHasher13::new_with_key(&k1),
            SipHasher13::new_with_key(&k2),
        ];
        Self {
            capacity,
            k_num,
            sips,
            bit_vec: bitmap,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn set(&mut self, item: &T)
    where
        T: std::hash::Hash,
    {
        let mut hashes = [0usize, 0usize];
        for k_i in 0..self.k_num {
            let bit_offset = self.bloom_hash(&mut hashes, item, k_i) % self.capacity;
            self.bit_vec.set(bit_offset, true);
        }
    }

    /// Check if an item is present in the set.
    /// There can be false positives, but no false negatives.
    pub fn check(&self, item: &T) -> bool
    where
        T: std::hash::Hash,
    {
        let mut hashes = [0usize, 0usize];
        for k_i in 0..self.k_num {
            let bit_offset = self.bloom_hash(&mut hashes, item, k_i) % self.capacity;
            if self.bit_vec.get(bit_offset).unwrap() == false {
                return false;
            }
        }
        true
    }

    /// Record the presence of an item in the set,
    /// and return the previous state of this item.
    pub fn check_and_set(&mut self, item: &T) -> bool
    where
        T: std::hash::Hash,
    {
        let mut hashes = [0usize, 0usize];
        let mut found = true;
        for k_i in 0..self.k_num {
            let bit_offset = self.bloom_hash(&mut hashes, item, k_i) % self.capacity;
            if self.bit_vec.get(bit_offset).unwrap() == false {
                found = false;
                self.bit_vec.set(bit_offset, true);
            }
        }
        found
    }

    fn bloom_hash(&self, hashes: &mut [usize; 2], item: &T, k_i: usize) -> usize
    where
        T: std::hash::Hash,
    {
        use std::hash::Hasher as _;

        if k_i < 2 {
            let sip = &mut self.sips[k_i].clone();
            item.hash(sip);
            let hash = sip.finish() as usize;
            hashes[k_i] = hash;
            hash
        } else {
            //largest u64 prime
            (hashes[0]).wrapping_add((k_i).wrapping_mul(hashes[1])) % 0xFFFF_FFFF_FFFF_FFC5usize
        }
    }

    pub fn compute_capacity(items_count: usize, fp_p: f64) -> usize {
        assert!(items_count > 0);
        assert!(fp_p > 0.0 && fp_p < 1.0);
        let log2 = std::f64::consts::LN_2;
        let log2_2 = log2 * log2;
        ((items_count as f64) * f64::ln(fp_p) / -log2_2).ceil() as usize
    }

    fn optimal_k_num(bitmap_bits: u64, items_count: usize) -> usize {
        let m = bitmap_bits as f64;
        let n = items_count as f64;
        let k_num = (m / n * f64::ln(2.0f64)).ceil() as usize;
        std::cmp::max(k_num, 1)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use getrandom::getrandom;

    type PieceIndex = i64;

    #[test]
    fn bloom_test_set() {
        let mut bloom = BloomFilter::new_for_fp_rate(100, 0.01);
        let mut k = vec![0u8, 4];
        getrandom(&mut k).unwrap();
        assert!(!bloom.check(&k));
        bloom.set(&k);
        assert!(bloom.check(&k));
    }

    #[test]
    fn bloom_test_check_and_set() {
        let mut bloom = BloomFilter::new_for_fp_rate(100, 0.01);
        let mut k = vec![0u8, 16];
        getrandom(&mut k).unwrap();
        assert!(!bloom.check_and_set(&k));
        assert!(bloom.check_and_set(&k));
    }

    #[test]
    fn bloom_test_load() {
        let mut bloom = BloomFilter::<PieceIndex>::new_for_fp_rate(1_000_000, 0.01);
        for i in 0..100_000 {
            bloom.set(&i.into());
        }

        for i in 0..100_000 {
            assert!((bloom.check(&i.into())));
        }

        let bytes = postcard::to_allocvec(&bloom).unwrap();
        drop(bloom);
        let bloom_from_bytes: BloomFilter<PieceIndex> =
            postcard::from_bytes(bytes.as_slice()).unwrap();
        for i in 0..100_000 {
            assert!((bloom_from_bytes.check(&i.into())));
        }
    }
}
