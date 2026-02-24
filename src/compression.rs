use lz4_flex::{compress_prepend_size, decompress_size_prepended};

/// Compress data using LZ4.
pub fn compress(data: &[u8]) -> Vec<u8> {
    compress_prepend_size(data)
}

/// Decompress LZ4-compressed data.
pub fn decompress(data: &[u8]) -> Result<Vec<u8>, lz4_flex::block::DecompressError> {
    decompress_size_prepended(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let original = b"hello world, this is a test of lz4 compression!";
        let compressed = compress(original);
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn compression_reduces_repetitive_data() {
        let data: Vec<u8> = "abcdefgh".repeat(1000).into_bytes();
        let compressed = compress(&data);
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn empty_data() {
        let compressed = compress(b"");
        let decompressed = decompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }
}
