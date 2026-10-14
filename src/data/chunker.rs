//! UltraCDC content-defined chunking (Zhou et al., IPCCC 2022).
//!
//! A cut point depends only on the bytes since the previous cut, never on
//! where the data came from or how it was split into requests, so identical
//! byte runs produce identical chunks wherever they appear in a file.

/// Bit pattern the sliding 8-byte window is compared against.
const PATTERN: u64 = 0xAAAA_AAAA_AAAA_AAAA;
/// Cut mask used before the normal size is reached (fewer cut points).
const MASK_S: u32 = 0x2F;
/// Cut mask used after the normal size is reached (more cut points).
const MASK_L: u32 = 0x2C;
/// Consecutive identical 8-byte windows after which a low-entropy run is cut.
const LOW_ENTROPY_STRING_THRESHOLD: u32 = 64;
const WINDOW: usize = 8;

/// Smallest configurable minimum chunk size: the window must fit before it.
pub const MIN_CHUNK_SIZE_LIMIT: usize = 64;
/// Largest configurable maximum chunk size.
pub const MAX_CHUNK_SIZE_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct UltraCdc {
    min_size: usize,
    normal_size: usize,
    max_size: usize,
}

/// Hamming distance of one byte to the pattern byte `0xAA`.
const fn pattern_distance_table() -> [u8; 256] {
    let mut table = [0_u8; 256];
    let mut byte = 0;
    while byte < 256 {
        table[byte] = ((byte as u8) ^ 0xAA).count_ones() as u8;
        byte += 1;
    }
    table
}

const PATTERN_DISTANCE: [u8; 256] = pattern_distance_table();

fn read_window(data: &[u8], at: usize) -> u64 {
    let mut bytes = [0_u8; WINDOW];
    bytes.copy_from_slice(&data[at..at + WINDOW]);
    u64::from_le_bytes(bytes)
}

impl UltraCdc {
    /// Sizes must satisfy `MIN_CHUNK_SIZE_LIMIT <= min <= normal <= max <=
    /// MAX_CHUNK_SIZE_LIMIT`, which `Config::validate` enforces.
    pub fn new(min_size: usize, normal_size: usize, max_size: usize) -> Self {
        assert!(
            (MIN_CHUNK_SIZE_LIMIT..=normal_size).contains(&min_size)
                && normal_size <= max_size
                && max_size <= MAX_CHUNK_SIZE_LIMIT,
            "invalid UltraCDC sizes min={min_size} normal={normal_size} max={max_size}"
        );
        Self {
            min_size,
            normal_size,
            max_size,
        }
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Length of the chunk that starts at `data[0]`.
    ///
    /// `data` must hold at least `max_size` bytes, or every byte left before
    /// the end of the data run: the cut is then the same as with any longer
    /// input, and a run shorter than a full chunk ends at its last byte.
    pub fn cut(&self, data: &[u8]) -> usize {
        let available = data.len();
        let limit = available.min(self.max_size);
        if limit > self.min_size {
            let mut window = read_window(data, self.min_size - WINDOW);
            let mut distance = (window ^ PATTERN).count_ones();
            let mut mask = MASK_S;
            let mut low_entropy_count = 0_u32;
            let mut i = self.min_size;
            while i + WINDOW <= limit {
                if i >= self.normal_size {
                    mask = MASK_L;
                }
                let incoming = read_window(data, i);
                if incoming == window {
                    // Sliding over a repeated window leaves the distance as is.
                    low_entropy_count += 1;
                    if low_entropy_count == LOW_ENTROPY_STRING_THRESHOLD {
                        return i + WINDOW;
                    }
                    i += WINDOW;
                    continue;
                }
                low_entropy_count = 0;
                for j in 0..WINDOW {
                    if distance & mask == 0 {
                        return i + j;
                    }
                    distance = distance + u32::from(PATTERN_DISTANCE[data[i + j] as usize])
                        - u32::from(PATTERN_DISTANCE[data[i + j - WINDOW] as usize]);
                }
                window = incoming;
                i += WINDOW;
            }
        }
        limit
    }
}

