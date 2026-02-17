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
}

impl UltraStreamChunker {
    pub fn new(min: usize, avg: usize, max: usize) -> Self {
        Self {
            sizes: SizeParams::new(min, avg, max),
            carry: Vec::new(),
            consumed_prefix: 0,
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
        if self.carry.is_empty() {
            return Vec::new();
        }

        let mut chunker = ultra::Chunker::new(&self.carry, self.sizes);
        let chunks = chunker.generate_chunks();
        if chunks.is_empty() {
            return Vec::new();
        }

        let emit_count = if finalize {
            chunks.len()
        } else {
            chunks.len().saturating_sub(1)
        };

        let mut out = Vec::with_capacity(emit_count);
        for chunk in chunks.iter().take(emit_count) {
            out.push(ChunkSpan {
                offset: self.consumed_prefix + chunk.pos,
                len: chunk.len,
            });
        }

        if emit_count == 0 {
            return out;
        }

        let consumed = chunks[emit_count - 1].pos + chunks[emit_count - 1].len;
        self.carry.drain(..consumed);
        self.consumed_prefix += consumed;

        if finalize {
            self.carry.clear();
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
}
