use integer_encoding::VarInt;
use rkyv::{Archive, Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::vlog::{ValuePointer, VALUE_POINTER_SIZE};
use crate::{InternalKeyKind, Key, Value};

pub(crate) const MAX_BATCH_SIZE: u64 = 1 << 32;
pub(crate) const BATCH_VERSION: u8 = 1;

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(bytecheck())]
struct BatchEntryArchive {
    kind: u8,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    timestamp: u64,
    valueptr: Option<Vec<u8>>,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(bytecheck())]
struct BatchArchive {
    version: u8,
    starting_seq_num: u64,
    entries: Vec<BatchEntryArchive>,
}

/// Represents a single entry in a batch
#[derive(Debug, Clone)]
pub(crate) struct BatchEntry {
    pub kind: InternalKeyKind,
    pub key: Key,
    pub value: Option<Value>,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct Batch {
    pub(crate) version: u8,
    pub(crate) entries: Vec<BatchEntry>,
    pub(crate) valueptrs: Vec<Option<ValuePointer>>, /* Parallel array to entries, None for
                                                      * inline values */
    pub(crate) starting_seq_num: u64, // Starting sequence number for this batch
    pub(crate) size: u64,             // Total size of all records (not serialized)
}

impl Default for Batch {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Batch {
    pub(crate) fn new(starting_seq_num: u64) -> Self {
        Self {
            entries: Vec::new(),
            valueptrs: Vec::new(),
            version: BATCH_VERSION,
            starting_seq_num,
            size: 0,
        }
    }

    // TODO: add a test for grow
    pub(crate) fn grow(&mut self, record_size: u64) -> Result<()> {
        if self.size + record_size > MAX_BATCH_SIZE {
            return Err(Error::BatchTooLarge);
        }
        self.size += record_size;
        self.entries.reserve(1);
        self.valueptrs.reserve(1);
        Ok(())
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let entries = self
            .entries
            .iter()
            .zip(self.valueptrs.iter())
            .map(|(entry, valueptr)| BatchEntryArchive {
                kind: entry.kind as u8,
                key: entry.key.clone(),
                value: entry.value.clone().filter(|value| !value.is_empty()),
                timestamp: entry.timestamp,
                valueptr: valueptr.as_ref().map(|ptr| ptr.encode().to_vec()),
            })
            .collect::<Vec<_>>();

        let archived = BatchArchive {
            version: self.version,
            starting_seq_num: self.starting_seq_num,
            entries,
        };

        rkyv::to_bytes::<rkyv::rancor::Error>(&archived)
            .map(|bytes| bytes.to_vec())
            .map_err(|e| Error::Corruption(format!("failed to encode WAL batch archive: {e}")))
    }

    #[cfg(test)]
    pub(crate) fn set(&mut self, key: Key, value: Value, timestamp: u64) -> Result<()> {
        self.add_record(InternalKeyKind::Set, key, Some(value), timestamp)
    }

    #[cfg(test)]
    pub(crate) fn delete(&mut self, key: Key, timestamp: u64) -> Result<()> {
        self.add_record(InternalKeyKind::Delete, key, None, timestamp)
    }

    /// Internal method to add a record with optional value pointer
    fn add_record_internal(
        &mut self,
        kind: InternalKeyKind,
        key: Key,
        value: Option<Value>,
        valueptr: Option<ValuePointer>,
        timestamp: u64,
    ) -> Result<()> {
        let key_len = key.len();
        let value_len = value.as_ref().map_or(0, |v| v.len());

        // Calculate the total size needed for this record
        let record_size = 1u64 + // kind
			(key_len as u64).required_space() as u64 +
			key_len as u64 +
			(value_len as u64).required_space() as u64 +
			value_len as u64 +
			8u64; // timestamp (8 bytes)

        self.grow(record_size)?;

        let entry = BatchEntry {
            kind,
            key,
            value,
            timestamp,
        };

        self.entries.push(entry);
        self.valueptrs.push(valueptr);

        Ok(())
    }

    pub(crate) fn add_record(
        &mut self,
        kind: InternalKeyKind,
        key: Key,
        value: Option<Value>,
        timestamp: u64,
    ) -> Result<()> {
        self.add_record_internal(kind, key, value, None, timestamp)
    }

    pub(crate) fn add_record_with_valueptr(
        &mut self,
        kind: InternalKeyKind,
        key: Key,
        value: Option<Value>,
        valueptr: Option<ValuePointer>,
        timestamp: u64,
    ) -> Result<()> {
        self.add_record_internal(kind, key, value, valueptr, timestamp)
    }

    pub(crate) fn count(&self) -> u32 {
        self.entries.len() as u32
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get entries for VLog processing
    #[cfg(test)]
    pub(crate) fn entries(&self) -> &[BatchEntry] {
        &self.entries
    }

    /// Set the starting sequence number for this batch
    pub(crate) fn set_starting_seq_num(&mut self, seq_num: u64) {
        self.starting_seq_num = seq_num;
    }

    /// Get the highest sequence number used in this batch
    pub(crate) fn get_highest_seq_num(&self) -> u64 {
        if self.entries.is_empty() {
            self.starting_seq_num
        } else {
            self.starting_seq_num + (self.entries.len() - 1) as u64
        }
    }

    /// Get an iterator over entries with their sequence numbers
    pub(crate) fn entries_with_seq_nums(
        &self,
    ) -> Result<impl Iterator<Item = (usize, &BatchEntry, u64, u64)>> {
        Ok(self
            .entries
            .iter()
            .enumerate()
            .map(move |(i, entry)| (i, entry, self.starting_seq_num + i as u64, entry.timestamp)))
    }

    /// Decode a batch from encoded data
    pub(crate) fn decode(data: &[u8]) -> Result<Self> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(data.len());
        aligned.extend_from_slice(data);
        let archived = rkyv::from_bytes::<BatchArchive, rkyv::rancor::Error>(&aligned)
            .map_err(|e| Error::Corruption(format!("invalid WAL batch archive: {e}")))?;

        if archived.version != BATCH_VERSION {
            return Err(Error::InvalidBatchRecord);
        }

        let mut entries = Vec::with_capacity(archived.entries.len());
        let mut valueptrs = Vec::with_capacity(archived.entries.len());
        for archived_entry in archived.entries {
            let kind = InternalKeyKind::from(archived_entry.kind);
            if kind == InternalKeyKind::Invalid {
                return Err(Error::InvalidBatchRecord);
            }

            let valueptr = match archived_entry.valueptr {
                Some(ptr) => {
                    if ptr.len() != VALUE_POINTER_SIZE {
                        return Err(Error::Corruption(format!(
                            "invalid WAL batch value pointer size: got {}, expected {}",
                            ptr.len(),
                            VALUE_POINTER_SIZE
                        )));
                    }
                    Some(ValuePointer::decode(&ptr)?)
                }
                None => None,
            };

            entries.push(BatchEntry {
                kind,
                key: archived_entry.key,
                value: archived_entry.value,
                timestamp: archived_entry.timestamp,
            });
            valueptrs.push(valueptr);
        }

        Ok(Self {
            version: archived.version,
            entries,
            valueptrs,
            starting_seq_num: archived.starting_seq_num,
            size: 0, // Decoded batches don't track size
        })
    }
}
