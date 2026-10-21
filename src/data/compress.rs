use anyhow::{bail, Context, Result};

pub const CODEC_RAW: u8 = 0;
pub const CODEC_ZSTD: u8 = 1;

/// Compresses `data` with zstd and returns the compressed bytes, or `None`
/// when they would not be smaller than `data` (the chunk is then stored raw
/// as `CODEC_RAW`).
pub fn compress_payload(data: &[u8], zstd_level: i32) -> Result<Option<Vec<u8>>> {
    if data.len() > u32::MAX as usize {
        bail!("chunk too large for u32 length: {}", data.len());
    }
    let compressed =
        zstd::bulk::compress(data, zstd_level).context("failed to compress chunk with zstd")?;
    Ok((compressed.len() < data.len()).then_some(compressed))
}

/// Decodes a stored payload; a raw payload is returned as it is.
pub fn decompress_chunk(
    codec: u8,
    payload: Vec<u8>,
    expected_uncompressed_len: u32,
) -> Result<Vec<u8>> {
    let out = match codec {
        CODEC_RAW => payload,
        CODEC_ZSTD => zstd::bulk::decompress(&payload, expected_uncompressed_len as usize)
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
    use super::{compress_payload, decompress_chunk, CODEC_RAW, CODEC_ZSTD};

    #[test]
    fn compress_payload_declines_small_uncompressible_input() {
        let data = [0xAB_u8];
        assert!(compress_payload(&data, 3).expect("compress").is_none());
    }

    #[test]
    fn compress_payload_compresses_when_smaller() {
        let data = vec![0_u8; 64 * 1024];
        let compressed = compress_payload(&data, 3)
            .expect("compress")
            .expect("zeros compress");
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn decompress_roundtrip_for_raw_and_zstd() {
        let raw_data = b"verfsnext-raw".to_vec();
        assert!(compress_payload(&raw_data, 3).expect("compress raw").is_none());
        let raw_out =
            decompress_chunk(CODEC_RAW, raw_data.clone(), raw_data.len() as u32).expect("decompress raw");
        assert_eq!(raw_out, raw_data);

        let zstd_data = vec![7_u8; 128 * 1024];
        let compressed = compress_payload(&zstd_data, 3)
            .expect("compress zstd")
            .expect("repeated bytes compress");
        let zstd_out = decompress_chunk(CODEC_ZSTD, compressed, zstd_data.len() as u32)
            .expect("decompress zstd");
        assert_eq!(zstd_out, zstd_data);
    }

    #[test]
    fn decompress_rejects_invalid_inputs() {
        let err = decompress_chunk(255, b"payload".to_vec(), 7).expect_err("codec must be rejected");
        assert!(
            err.to_string().contains("unsupported chunk codec"),
            "unexpected error: {err}"
        );

        let err = decompress_chunk(CODEC_RAW, b"abc".to_vec(), 5).expect_err("len mismatch must fail");
        assert!(
            err.to_string()
                .contains("chunk length mismatch after decode"),
            "unexpected error: {err}"
        );

        let err =
            decompress_chunk(CODEC_ZSTD, b"not-zstd".to_vec(), 1024).expect_err("invalid zstd must fail");
        assert!(
            err.to_string().contains("failed to decompress zstd chunk"),
            "unexpected error: {err}"
        );
    }
}
