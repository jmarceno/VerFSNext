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

#[cfg(test)]
mod tests {
    use super::{
        compress_chunk, compress_parallel, decompress_chunk, PendingChunk, CODEC_RAW, CODEC_ZSTD,
    };

    #[test]
    fn compress_chunk_uses_raw_for_small_uncompressible_input() {
        let data = [0xAB_u8];
        let chunk = compress_chunk(&data, 3).expect("compress");
        assert_eq!(chunk.codec, CODEC_RAW);
        assert_eq!(chunk.uncompressed_len, data.len() as u32);
        assert_eq!(chunk.compressed_len, data.len() as u32);
        assert_eq!(chunk.compressed, data);
    }

    #[test]
    fn compress_chunk_uses_zstd_when_smaller() {
        let data = vec![0_u8; 64 * 1024];
        let chunk = compress_chunk(&data, 3).expect("compress");
        assert_eq!(chunk.codec, CODEC_ZSTD);
        assert_eq!(chunk.uncompressed_len, data.len() as u32);
        assert_eq!(chunk.compressed_len, chunk.compressed.len() as u32);
        assert!(chunk.compressed_len < chunk.uncompressed_len);
    }

    #[test]
    fn decompress_roundtrip_for_raw_and_zstd() {
        let raw_data = b"verfsnext-raw".to_vec();
        let raw_chunk = compress_chunk(&raw_data, 3).expect("compress raw");
        let raw_out = decompress_chunk(
            raw_chunk.codec,
            &raw_chunk.compressed,
            raw_chunk.uncompressed_len,
        )
        .expect("decompress raw");
        assert_eq!(raw_out, raw_data);

        let zstd_data = vec![7_u8; 128 * 1024];
        let zstd_chunk = compress_chunk(&zstd_data, 3).expect("compress zstd");
        let zstd_out = decompress_chunk(
            zstd_chunk.codec,
            &zstd_chunk.compressed,
            zstd_chunk.uncompressed_len,
        )
        .expect("decompress zstd");
        assert_eq!(zstd_out, zstd_data);
    }

    #[test]
    fn decompress_rejects_invalid_inputs() {
        let err = decompress_chunk(255, b"payload", 7).expect_err("codec must be rejected");
        assert!(
            err.to_string().contains("unsupported chunk codec"),
            "unexpected error: {err}"
        );

        let err = decompress_chunk(CODEC_RAW, b"abc", 5).expect_err("len mismatch must fail");
        assert!(
            err.to_string()
                .contains("chunk length mismatch after decode"),
            "unexpected error: {err}"
        );

        let err =
            decompress_chunk(CODEC_ZSTD, b"not-zstd", 1024).expect_err("invalid zstd must fail");
        assert!(
            err.to_string().contains("failed to decompress zstd chunk"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn compress_parallel_keeps_hash_to_payload_mapping() {
        let inputs = vec![
            PendingChunk {
                hash: [1_u8; 16],
                data: vec![0_u8; 32 * 1024],
            },
            PendingChunk {
                hash: [2_u8; 16],
                data: b"second chunk payload".to_vec(),
            },
            PendingChunk {
                hash: [3_u8; 16],
                data: vec![5_u8; 8 * 1024],
            },
        ];

        let ready = compress_parallel(inputs.clone(), 3).expect("parallel compress");
        assert_eq!(ready.len(), inputs.len());

        for (i, out) in ready.iter().enumerate() {
            assert_eq!(out.hash, inputs[i].hash);
            let plain = decompress_chunk(
                out.chunk.codec,
                &out.chunk.compressed,
                out.chunk.uncompressed_len,
            )
            .expect("roundtrip");
            assert_eq!(plain, inputs[i].data);
        }
    }
}
