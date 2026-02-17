use anyhow::{bail, Context, Result};
use rayon::prelude::*;

pub const CODEC_RAW: u8 = 0;
pub const CODEC_ZSTD: u8 = 1;

#[derive(Debug, Clone)]
pub struct CompressedChunk {
    pub codec: u8,
    pub compressed: Vec<u8>,
    pub uncompressed_len: u32,
    pub compressed_len: u32,
}

#[derive(Debug, Clone)]
pub struct PendingChunk {
    pub hash: [u8; 16],
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ReadyChunk {
    pub hash: [u8; 16],
    pub chunk: CompressedChunk,
}

pub fn compress_chunk(data: &[u8], zstd_level: i32) -> Result<CompressedChunk> {
    if data.len() > u32::MAX as usize {
        bail!("chunk too large for u32 length: {}", data.len());
    }

    let compressed =
        zstd::bulk::compress(data, zstd_level).context("failed to compress chunk with zstd")?;
    let (codec, payload) = if compressed.len() < data.len() {
        (CODEC_ZSTD, compressed)
    } else {
        (CODEC_RAW, data.to_vec())
    };

    Ok(CompressedChunk {
        codec,
        uncompressed_len: data.len() as u32,
        compressed_len: payload.len() as u32,
        compressed: payload,
    })
}

pub fn compress_parallel(chunks: Vec<PendingChunk>, zstd_level: i32) -> Result<Vec<ReadyChunk>> {
    chunks
        .into_par_iter()
        .map(|pending| {
            let chunk = compress_chunk(&pending.data, zstd_level)?;
            Ok(ReadyChunk {
                hash: pending.hash,
                chunk,
            })
        })
        .collect()
}

pub fn decompress_chunk(
    codec: u8,
    payload: &[u8],
    expected_uncompressed_len: u32,
) -> Result<Vec<u8>> {
    let out = match codec {
        CODEC_RAW => payload.to_vec(),
        CODEC_ZSTD => zstd::bulk::decompress(payload, expected_uncompressed_len as usize)
            .context("failed to decompress zstd chunk")?,
        _ => bail!("unsupported chunk codec {}", codec),
    };

    if out.len() != expected_uncompressed_len as usize {
        bail!(
            "chunk length mismatch after decode: expected {}, got {}",
            expected_uncompressed_len,
            out.len()
        );
    }
    Ok(out)
}
