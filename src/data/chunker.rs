use cdc_chunkers::{ultra, SizeParams};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkSpan {
    pub offset: usize,
    pub len: usize,
}

pub struct UltraStreamChunker {
    sizes: SizeParams,
    carry: Vec<u8>,
    consumed_prefix: usize,
    scanned_len: usize,
}

impl UltraStreamChunker {
    pub fn new(min: usize, avg: usize, max: usize) -> Self {
        Self {
            sizes: SizeParams::new(min, avg, max),
            carry: Vec::new(),
            consumed_prefix: 0,
            scanned_len: 0,
        }
    }

    pub fn feed(&mut self, bytes: &[u8]) -> Vec<ChunkSpan> {
        if bytes.is_empty() {
            return Vec::new();
        }
        self.carry.extend_from_slice(bytes);
        self.emit_ready(false)
    }

    pub fn finish(&mut self) -> Vec<ChunkSpan> {
        self.emit_ready(true)
    }

    fn emit_ready(&mut self, finalize: bool) -> Vec<ChunkSpan> {
        let mut out = Vec::new();

        while !self.carry.is_empty() {
            let skip_len = self.scanned_len.max(self.sizes.min);

            let mut sizes = self.sizes;
            sizes.min = skip_len;

            let mut chunker = ultra::Chunker::new(&self.carry, sizes);
            let Some(chunk) = chunker.next() else {
                break;
            };

            if chunk.pos + chunk.len == self.carry.len() && !finalize {
                self.scanned_len = self.carry.len().saturating_sub(8);
                break;
            }

            out.push(ChunkSpan {
                offset: self.consumed_prefix + chunk.pos,
                len: chunk.len,
            });

            self.carry.drain(..chunk.len);
            self.consumed_prefix += chunk.len;
            self.scanned_len = 0;
        }

        if finalize {
            self.carry.clear();
            self.scanned_len = 0;
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::UltraStreamChunker;

    #[test]
    fn streaming_chunker_matches_single_pass_coverage() {
        let mut streaming = UltraStreamChunker::new(2048, 4096, 16384);
        let data = vec![42_u8; 100_000];
        let mut spans = Vec::new();
        for part in data.chunks(1024) {
            spans.extend(streaming.feed(part));
        }
        spans.extend(streaming.finish());

        let covered = spans.iter().map(|span| span.len).sum::<usize>();
        assert_eq!(covered, data.len());
    }

    #[test]
    fn benchmark_streaming_chunker_baseline() {
        use std::time::Instant;
        let mut streaming = UltraStreamChunker::new(1024 * 1024, 4 * 1024 * 1024, 16 * 1024 * 1024);

        let chunk_size = 4096; // Feed 4KB at a time (like a slow disk or stream)
        let total_size = 64 * 1024 * 1024; // 64 MB total

        // Generate pseudo-random data to prevent extreme early match patterns
        // but keep it fast to generate.
        let mut data = vec![0_u8; total_size];
        for i in 0..total_size {
            data[i] = (i % 251) as u8;
        }

        let start = Instant::now();
        let mut spans = Vec::new();
        for part in data.chunks(chunk_size) {
            spans.extend(streaming.feed(part));
        }
        spans.extend(streaming.finish());

        let duration = start.elapsed();
        println!(
            "baseline_benchmark: Chunking 64MB (4KB feeds, 4MB avg chunks) took: {:?}",
            duration
        );
        assert!(!spans.is_empty());
    }
}
