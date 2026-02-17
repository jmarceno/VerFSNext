use xxhash_rust::xxh3::xxh3_128;

pub fn hash128(bytes: &[u8]) -> [u8; 16] {
    xxh3_128(bytes).to_le_bytes()
}
