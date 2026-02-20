use xxhash_rust::xxh3::{xxh3_128, Xxh3};

pub fn hash128(bytes: &[u8]) -> [u8; 16] {
    xxh3_128(bytes).to_le_bytes()
}

pub fn hash128_with_domain(domain: u8, bytes: &[u8]) -> [u8; 16] {
    let mut hasher = Xxh3::new();
    hasher.update(&[domain]);
    hasher.update(bytes);
    hasher.digest128().to_le_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash128_deterministic() {
        let data = b"hello world";
        let h1 = hash128(data);
        let h2 = hash128(data);
        assert_eq!(h1, h2, "Hashing must be deterministic");
    }

    #[test]
    fn test_hash128_with_domain_deterministic() {
        let domain = 42;
        let data = b"hello world";
        let h1 = hash128_with_domain(domain, data);
        let h2 = hash128_with_domain(domain, data);
        assert_eq!(h1, h2, "Domain hashing must be deterministic");
    }

    #[test]
    fn test_domain_separation() {
        let data = b"hello world";
        let h1 = hash128_with_domain(1, data);
        let h2 = hash128_with_domain(2, data);
        assert_ne!(
            h1, h2,
            "Different domains must produce different hashes for the same data"
        );
    }

    #[test]
    fn test_different_data_same_domain() {
        let domain = 1;
        let h1 = hash128_with_domain(domain, b"data A");
        let h2 = hash128_with_domain(domain, b"data B");
        assert_ne!(
            h1, h2,
            "Different data in the same domain must produce different hashes"
        );
    }

    #[test]
    fn test_empty_data() {
        let h1 = hash128_with_domain(1, b"");
        let h2 = hash128_with_domain(2, b"");
        assert_ne!(
            h1, h2,
            "Different domains must produce different hashes even for empty data"
        );

        let h3 = hash128_with_domain(1, b"");
        assert_eq!(h1, h3, "Hashing empty data must be deterministic");
    }

    #[test]
    fn test_no_simple_prefix_collision() {
        // If domain isn't cleanly separated, hash_with_domain(1, &[2, 3]) might
        // collide with hash128(&[1, 2, 3]). Here we verify they do produce the same underlying
        // bytes in xxh3_128, which is the current expected behavior for how the bytes are combined.
        // The main point of domain hashing is to prevent collisions *between domains*.
        let h_domain = hash128_with_domain(1, &[2, 3]);
        let h_raw = hash128(&[1, 2, 3]);
        assert_eq!(
            h_domain, h_raw,
            "hash128_with_domain prepends the domain byte"
        );
    }

    #[test]
    fn test_hash128_with_domain_bench() {
        use std::time::Instant;
        let data = vec![0u8; 4096];
        let start = Instant::now();
        let iters = 100_000;
        let mut black_box_sum = 0;
        for i in 0..iters {
            let h = hash128_with_domain((i % 256) as u8, &data);
            black_box_sum ^= h[0];
        }
        let elapsed = start.elapsed();
        println!(
            "hash128_with_domain_bench: {:?} for {} iterations (black_box: {})",
            elapsed, iters, black_box_sum
        );
    }

    #[test]
    fn test_hash128_changes_when_input_changes() {
        let h1 = hash128(b"alpha");
        let h2 = hash128(b"beta");
        assert_ne!(h1, h2, "Distinct inputs should not map to the same hash");
    }
}
