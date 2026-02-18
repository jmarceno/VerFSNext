use xxhash_rust::xxh3::xxh3_128;

pub fn hash128(bytes: &[u8]) -> [u8; 16] {
    xxh3_128(bytes).to_le_bytes()
}

pub fn hash128_with_domain(domain: u8, bytes: &[u8]) -> [u8; 16] {
    let mut tagged = Vec::with_capacity(1 + bytes.len());
    tagged.push(domain);
    tagged.extend_from_slice(bytes);
    xxh3_128(&tagged).to_le_bytes()
}
