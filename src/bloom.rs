use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A simple Bloom filter for fast negative lookups.
///
/// Returns `true` for "maybe present" and `false` for "definitely not present".
/// False positive rate depends on the number of bits and hash functions relative
/// to the number of inserted items.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BloomFilter {
    /// Bit vector stored as bytes.
    bits: Vec<u8>,
    /// Number of bits in the filter.
    num_bits: usize,
    /// Number of hash functions to use.
    num_hashes: u32,
    /// Number of items inserted.
    count: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter optimized for the expected number of items
    /// and desired false positive rate (0.0 to 1.0).
    ///
    /// # Example
    /// ```
    /// use iceberg::bloom::BloomFilter;
    /// let bf = BloomFilter::new(1000, 0.01); // 1000 items, 1% FP rate
    /// ```
    pub fn new(expected_items: usize, fp_rate: f64) -> Self {
        let fp_rate = fp_rate.clamp(0.0001, 0.5);
        let expected_items = expected_items.max(1);

        // Optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let num_bits =
            (-(expected_items as f64) * fp_rate.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        let num_bits = num_bits.max(64); // minimum 64 bits

        // Optimal number of hashes: k = (m/n) * ln(2)
        let num_hashes = ((num_bits as f64 / expected_items as f64) * 2.0_f64.ln()).ceil() as u32;
        let num_hashes = num_hashes.clamp(1, 16);

        let num_bytes = num_bits.div_ceil(8);

        Self {
            bits: vec![0u8; num_bytes],
            num_bits,
            num_hashes,
            count: 0,
        }
    }

    /// Insert a key into the filter.
    pub fn insert(&mut self, key: &[u8]) {
        for i in 0..self.num_hashes {
            let bit = self.hash(key, i);
            let byte_idx = bit / 8;
            let bit_idx = bit % 8;
            self.bits[byte_idx] |= 1 << bit_idx;
        }
        self.count += 1;
    }

    /// Check if a key might be in the filter.
    /// Returns `false` if definitely not present, `true` if maybe present.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let bit = self.hash(key, i);
            let byte_idx = bit / 8;
            let bit_idx = bit % 8;
            if self.bits[byte_idx] & (1 << bit_idx) == 0 {
                return false;
            }
        }
        true
    }

    /// Number of items inserted.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Number of bits in the filter.
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Estimated false positive rate given current fill.
    pub fn estimated_fp_rate(&self) -> f64 {
        let ones = self
            .bits
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum::<usize>();
        let fill_ratio = ones as f64 / self.num_bits as f64;
        fill_ratio.powi(self.num_hashes as i32)
    }

    /// Memory usage in bytes.
    pub fn size_bytes(&self) -> usize {
        self.bits.len()
    }

    /// Merge another bloom filter into this one (union).
    /// Both filters must have the same parameters.
    pub fn merge(&mut self, other: &BloomFilter) -> bool {
        if self.num_bits != other.num_bits || self.num_hashes != other.num_hashes {
            return false;
        }
        for (a, b) in self.bits.iter_mut().zip(other.bits.iter()) {
            *a |= *b;
        }
        self.count += other.count;
        true
    }

    /// Hash a key with a given seed to produce a bit index.
    fn hash(&self, key: &[u8], seed: u32) -> usize {
        let mut hasher = Sha256::new();
        hasher.update(seed.to_le_bytes());
        hasher.update(key);
        let result = hasher.finalize();
        let val = u64::from_le_bytes(result[..8].try_into().unwrap());
        (val as usize) % self.num_bits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_insert_and_lookup() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert(b"hello");
        bf.insert(b"world");

        assert!(bf.may_contain(b"hello"));
        assert!(bf.may_contain(b"world"));
        assert!(!bf.may_contain(b"missing"));
    }

    #[test]
    fn no_false_negatives() {
        let mut bf = BloomFilter::new(1000, 0.01);
        let keys: Vec<String> = (0..500).map(|i| format!("key_{}", i)).collect();

        for k in &keys {
            bf.insert(k.as_bytes());
        }

        // Every inserted key MUST be found (no false negatives)
        for k in &keys {
            assert!(bf.may_contain(k.as_bytes()), "false negative for {}", k);
        }
    }

    #[test]
    fn false_positive_rate_reasonable() {
        let mut bf = BloomFilter::new(1000, 0.01);
        for i in 0..1000 {
            bf.insert(format!("key_{}", i).as_bytes());
        }

        let mut false_positives = 0;
        let test_count = 10000;
        for i in 0..test_count {
            if bf.may_contain(format!("other_{}", i).as_bytes()) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / test_count as f64;
        // Should be below 5% (we asked for 1%, allow some slack)
        assert!(fp_rate < 0.05, "FP rate too high: {:.2}%", fp_rate * 100.0);
    }

    #[test]
    fn empty_filter_has_no_matches() {
        let bf = BloomFilter::new(100, 0.01);
        assert!(!bf.may_contain(b"anything"));
        assert_eq!(bf.count(), 0);
    }

    #[test]
    fn merge_filters() {
        let mut bf1 = BloomFilter::new(100, 0.01);
        let mut bf2 = BloomFilter::new(100, 0.01);

        bf1.insert(b"alpha");
        bf2.insert(b"beta");

        assert!(!bf1.may_contain(b"beta"));
        assert!(bf1.merge(&bf2));
        assert!(bf1.may_contain(b"alpha"));
        assert!(bf1.may_contain(b"beta"));
    }

    #[test]
    fn merge_incompatible_fails() {
        let mut bf1 = BloomFilter::new(100, 0.01);
        let bf2 = BloomFilter::new(1000, 0.1);
        assert!(!bf1.merge(&bf2));
    }

    #[test]
    fn count_tracks_inserts() {
        let mut bf = BloomFilter::new(100, 0.01);
        assert_eq!(bf.count(), 0);
        bf.insert(b"a");
        bf.insert(b"b");
        assert_eq!(bf.count(), 2);
    }

    #[test]
    fn size_bytes_reasonable() {
        let bf = BloomFilter::new(1000, 0.01);
        // For 1000 items at 1% FP, should be ~1.2KB
        assert!(bf.size_bytes() > 100);
        assert!(bf.size_bytes() < 10000);
    }
}
