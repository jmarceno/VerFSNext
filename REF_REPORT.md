# SurrealKV Zero-Copy Refactoring Feasibility Report

This document details the findings of a deep review into `surrealkv` for the purpose of introducing `rkyv` and zero-copy semantics on critical paths.

## Executive Summary

The refactoring of `surrealkv` to utilize `rkyv` for zero-copy serialization is highly feasible for several critical components and promises significant performance gains, particularly in write throughput, recovery time, and read latency for index lookups.

**Key Recommendations:**
1.  **WAL & Batch:** Replace manual `Batch` serialization with `rkyv`. This is the highest impact change, enabling zero-copy access during WAL replay and reducing allocation overhead during commits.
2.  **SSTable Index & Metadata:** Replace custom block formats with `rkyv` archives. This will allow index blocks to be mapped and searched directly without parsing, significantly reducing read amplification and memory usage.
3.  **MemTable Layout:** Refactor the internal `Node` structure to store `InternalKey` components contiguously. This enables zero-copy `InternalKeyRef` creation during iteration and flushing.
4.  **SSTable Data Blocks:** **Do not use `rkyv` for data blocks.** The current prefix compression scheme is critical for storage efficiency. `rkyv` does not natively support this, and the trade-off (larger SSTables vs. zero-copy) is likely negative for general-purpose use.

## Detailed Component Analysis

### 1. Write-Ahead Log (WAL) & Batch

**Current Implementation:**
-   **Structure:** `Batch` contains a `Vec<BatchEntry>`. `BatchEntry` holds `key` (`Vec<u8>`), `value` (`Option<Vec<u8>>`), etc.
-   **Serialization:** `Batch::encode` manually serializes entries into a `Vec<u8>`. This involves multiple allocations and memory copies.
-   **Deserialization:** `Batch::decode` parses the byte slice and reconstructs a `Batch` struct on the heap, copying all keys and values.

**Proposed Change:**
-   **Data Structure:** Define an `rkyv`-compatible `Batch` structure.
    ```rust
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    pub struct Batch {
        entries: Vec<BatchEntry>, // rkyv::Vec
        // ...
    }
    ```
-   **Write Path:** Use `rkyv::to_bytes` to serialize the `Batch` into an `AlignedVec`. Pass this directly to the WAL writer. This avoids intermediate allocations for the encoded buffer.
-   **Read Path (Recovery):** Instead of `Batch::decode`, use `rkyv::access::<Batch>` on the raw bytes read from the WAL. This returns a reference to the archived batch *without allocation or copying*.

**Expected Gains:**
-   **Write Path:** Reduced CPU usage and memory allocation during `commit()`.
-   **Recovery:** significantly faster WAL replay. Recovery becomes dominated by I/O and MemTable insertion, rather than deserialization overhead.

### 2. SSTable Index & Metadata

**Current Implementation:**
-   **Index:** Uses a custom block-based format. Index blocks are read, decompressed, and parsed into heap-allocated structures (`Index` struct).
-   **Metadata:** `TableMetadata` is serialized/deserialized using `bincode` or custom encoding.
-   **Lookups:** Index lookups involve binary searching the parsed index structure.

**Proposed Change:**
-   **Structure:** Define `Index` and `TableMetadata` as `rkyv` archives.
-   **Storage:** Store the archived bytes directly in the SSTable blocks (compressed or uncompressed).
-   **Access:**
    -   **Index:** Map the index block (or read into memory) and access it via `rkyv`. The archive layout allows binary search directly on the serialized data.
    -   **Metadata:** Access directly via `rkyv`.

**Expected Gains:**
-   **Read Path:** Zero-copy index lookups. The index block can be kept in the OS page cache and accessed without parsing.
-   **Memory:** Reduced memory footprint for open tables (no need to keep parsed index structures in memory).

### 3. MemTable (Skiplist)

**Current Implementation:**
-   **Node Layout:** `Node` struct in the Arena stores `key_offset` (pointer to user key), `key_trailer` (u64), and `key_timestamp` (u64) as separate fields.
-   **Iteration:** `SkiplistIterator` constructs an `InternalKeyRef` by copying these disparate components into a contiguous `encoded_key_buf`. This copy happens for *every* entry visited during iteration (scan, flush, compaction).

**Proposed Change:**
-   **Node Layout:** Refactor `Node` to store the full `InternalKey` (`[user_key][trailer][timestamp]`) contiguously in the Arena's variable-length data section.
-   **Iteration:** `InternalKeyRef` can point directly to this contiguous memory in the Arena.
-   **Structure:**
    ```rust
    // Conceptually in Arena:
    // [NodeHeader] [InternalKey (UserKey + Trailer + Timestamp)] [Value]
    ```

**Expected Gains:**
-   **Read/Scan:** Zero-copy iteration.
-   **Flush:** Faster flushing to SSTables as keys are already in the format expected by the block writer (mostly).

### 4. SSTable Data Blocks

**Current Implementation:**
-   **Format:** Uses prefix compression (shared prefix length, unshared length, suffix).
-   **Access:** Iteration requires incremental reconstruction of keys. Keys are not stored contiguously.

**Analysis:**
-   `rkyv` requires data to be in a specific, relative-pointer based format. It does not support prefix compression natively.
-   Switching to `rkyv` for data blocks would mean abandoning prefix compression.
-   **Trade-off:** Zero-copy vs. Disk Space & I/O.
-   Prefix compression typically saves 30-50% of key storage space. Losing this would increase read amplification (more I/O).

**Verdict:**
-   **Keep the current custom block format.** The storage savings of prefix compression outweigh the CPU cost of decompression/decoding for data blocks.
-   **Optimization:** Ensure `Value`s are returned as references to the block buffer where possible, rather than copying.

### 5. Value Log (VLog)

**Current Implementation:**
-   **Format:** Simple append-only log: `[header][key][value][crc]`.
-   **Values:** Often compressed individually.

**Analysis:**
-   VLog entries are unstructured blobs (`Vec<u8>`).
-   `rkyv` adds metadata overhead which isn't necessary for opaque byte blobs.
-   **Optimization:** The `ValuePointer` (used in SSTables to point to VLog) is frequently serialized. This struct is small and could be `rkyv`-ized, but it's currently 25 bytes fixed-size. Custom encoding is likely optimal here.

**Verdict:** No changes recommended for VLog format.

## Implementation Plan

### Phase 1: WAL & Batch Refactoring (Highest Impact)
1.  Add `rkyv` dependency.
2.  Define `Batch` and `BatchEntry` with `#[derive(Archive, Serialize, Deserialize)]`.
3.  Update `CommitPipeline` to use `rkyv::to_bytes::<_, 4096>(batch)` (scratch buffer size TBD).
4.  Update `Wal::writer` to accept `AlignedVec`.
5.  Update `Wal::reader` and recovery logic to use `rkyv::access::<Batch>` and iterate over the archived entries.
6.  *Breaking Change:* This invalidates existing WAL files.

### Phase 2: MemTable Layout Optimization
1.  Modify `Node` struct in `skiplist.rs`.
2.  Update `new_node` allocator to reserve space for `trailer` and `timestamp` contiguous with `key`.
3.  Update `SkiplistIterator` to return `InternalKeyRef` pointing directly to Arena memory.
4.  *Internal Change:* No disk format change, but requires careful unsafe code modification.

### Phase 3: SSTable Index & Meta
1.  Define `Index` and `TableMetadata` using `rkyv`.
2.  Update `TableWriter` to serialize these structures using `rkyv`.
3.  Update `Table` reader to map/load these blocks and access them via `rkyv`.
4.  *Breaking Change:* Invalidates existing SSTables.

## Risks & Mitigation
-   **Disk Format Compatibility:** All proposed changes are breaking. As per instructions, backward compatibility is not a concern, but a clean break (e.g., version check) should be enforced to prevent corruption when opening old DBs.
-   **Unsafe Code:** `rkyv` guarantees are strong, but manual memory layout changes in `Skiplist` (Phase 2) involve `unsafe` Rust. Comprehensive testing (Miri) is required.
