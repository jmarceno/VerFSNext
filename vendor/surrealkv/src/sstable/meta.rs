use rkyv::{Archive, Deserialize, Serialize};

use crate::error::Error;
use crate::sstable::table::TableFormat;
use crate::{CompressionType, InternalKey, Result};

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(bytecheck())]
struct PropertiesArchive {
    id: u64,
    table_format: u8,
    num_entries: u64,
    num_deletions: u64,
    data_size: u64,
    oldest_vlog_file_id: u64,
    num_data_blocks: u64,
    index_size: u64,
    index_partitions: u64,
    top_level_index_size: u64,
    filter_size: u64,
    raw_key_size: u64,
    raw_value_size: u64,
    created_at: u128,
    item_count: u64,
    key_count: u64,
    tombstone_count: u64,
    num_soft_deletes: u64,
    num_range_deletions: u64,
    block_size: u32,
    block_count: u32,
    compression: u8,
    seqnos: (u64, u64),
    oldest_key_time: Option<u64>,
    newest_key_time: Option<u64>,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(bytecheck())]
struct TableMetadataArchive {
    has_point_keys: Option<bool>,
    smallest_seq_num: Option<u64>,
    largest_seq_num: Option<u64>,
    properties: PropertiesArchive,
    smallest_point: Option<Vec<u8>>,
    largest_point: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(crate) struct Properties {
    pub(crate) id: u64,
    pub(crate) table_format: TableFormat,
    pub(crate) num_entries: u64,
    pub(crate) num_deletions: u64,
    pub(crate) data_size: u64,
    pub(crate) oldest_vlog_file_id: u64,
    pub(crate) num_data_blocks: u64,

    // Index metrics
    pub(crate) index_size: u64,
    pub(crate) index_partitions: u64,
    pub(crate) top_level_index_size: u64,

    // Filter metrics
    pub(crate) filter_size: u64,

    // Raw size metrics (uncompressed)
    pub(crate) raw_key_size: u64,
    pub(crate) raw_value_size: u64,

    pub(crate) created_at: u128,
    pub(crate) item_count: u64,
    pub(crate) key_count: u64,
    pub(crate) tombstone_count: u64,
    pub(crate) num_soft_deletes: u64,

    // Range deletion metrics
    pub(crate) num_range_deletions: u64,

    pub(crate) block_size: u32,
    pub(crate) block_count: u32,
    pub(crate) compression: CompressionType,
    pub(crate) seqnos: (u64, u64),

    // Time metrics
    pub(crate) oldest_key_time: Option<u64>,
    pub(crate) newest_key_time: Option<u64>,
}

impl Properties {
    pub(crate) fn new() -> Self {
        Properties {
            id: 0,
            table_format: TableFormat::LSMV1,
            num_entries: 0,
            num_deletions: 0,
            data_size: 0,
            oldest_vlog_file_id: 0,
            num_data_blocks: 0,
            index_size: 0,
            index_partitions: 0,
            top_level_index_size: 0,
            filter_size: 0,
            raw_key_size: 0,
            raw_value_size: 0,
            created_at: 0,
            item_count: 0,
            key_count: 0,
            tombstone_count: 0,
            num_soft_deletes: 0,
            num_range_deletions: 0,
            block_size: 0,
            block_count: 0,
            compression: CompressionType::None,
            seqnos: (0, 0),
            oldest_key_time: None,
            newest_key_time: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn encode(&self) -> Vec<u8> {
        let archived = PropertiesArchive {
            id: self.id,
            table_format: self.table_format as u8,
            num_entries: self.num_entries,
            num_deletions: self.num_deletions,
            data_size: self.data_size,
            oldest_vlog_file_id: self.oldest_vlog_file_id,
            num_data_blocks: self.num_data_blocks,
            index_size: self.index_size,
            index_partitions: self.index_partitions,
            top_level_index_size: self.top_level_index_size,
            filter_size: self.filter_size,
            raw_key_size: self.raw_key_size,
            raw_value_size: self.raw_value_size,
            created_at: self.created_at,
            item_count: self.item_count,
            key_count: self.key_count,
            tombstone_count: self.tombstone_count,
            num_soft_deletes: self.num_soft_deletes,
            num_range_deletions: self.num_range_deletions,
            block_size: self.block_size,
            block_count: self.block_count,
            compression: self.compression as u8,
            seqnos: self.seqnos,
            oldest_key_time: self.oldest_key_time,
            newest_key_time: self.newest_key_time,
        };
        rkyv::to_bytes::<rkyv::rancor::Error>(&archived)
            .map(|bytes| bytes.to_vec())
            .expect("rkyv failed to encode SSTable properties")
    }

    #[allow(dead_code)]
    pub(crate) fn decode(buf: Vec<u8>) -> Result<Self> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(buf.len());
        aligned.extend_from_slice(&buf);
        let archived = rkyv::from_bytes::<PropertiesArchive, rkyv::rancor::Error>(&aligned)
            .map_err(|e| Error::Corruption(format!("invalid SSTable properties archive: {e}")))?;

        Ok(Self {
            id: archived.id,
            table_format: TableFormat::from_u8(archived.table_format)?,
            num_entries: archived.num_entries,
            num_deletions: archived.num_deletions,
            data_size: archived.data_size,
            oldest_vlog_file_id: archived.oldest_vlog_file_id,
            num_data_blocks: archived.num_data_blocks,
            index_size: archived.index_size,
            index_partitions: archived.index_partitions,
            top_level_index_size: archived.top_level_index_size,
            filter_size: archived.filter_size,
            raw_key_size: archived.raw_key_size,
            raw_value_size: archived.raw_value_size,
            created_at: archived.created_at,
            item_count: archived.item_count,
            key_count: archived.key_count,
            tombstone_count: archived.tombstone_count,
            num_soft_deletes: archived.num_soft_deletes,
            num_range_deletions: archived.num_range_deletions,
            block_size: archived.block_size,
            block_count: archived.block_count,
            compression: CompressionType::try_from(archived.compression)?,
            seqnos: archived.seqnos,
            oldest_key_time: archived.oldest_key_time,
            newest_key_time: archived.newest_key_time,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub(crate) has_point_keys: Option<bool>,
    pub(crate) smallest_seq_num: Option<u64>,
    pub(crate) largest_seq_num: Option<u64>,
    pub(crate) properties: Properties,
    pub(crate) smallest_point: Option<InternalKey>,
    pub(crate) largest_point: Option<InternalKey>,
}

impl TableMetadata {
    pub(crate) fn new() -> Self {
        TableMetadata {
            smallest_point: None,
            largest_point: None,
            has_point_keys: None,
            smallest_seq_num: None,
            largest_seq_num: None,
            properties: Properties::new(),
        }
    }

    pub(crate) fn set_smallest_point_key(&mut self, k: InternalKey) {
        self.smallest_point = Some(k);
        self.has_point_keys = Some(true);
    }

    pub(crate) fn set_largest_point_key(&mut self, k: InternalKey) {
        self.largest_point = Some(k);
        self.has_point_keys = Some(true);
    }

    pub(crate) fn update_seq_num(&mut self, seq_num: u64) {
        self.smallest_seq_num = Some(self.smallest_seq_num.map_or(seq_num, |s| s.min(seq_num)));
        self.largest_seq_num = Some(self.largest_seq_num.map_or(seq_num, |l| l.max(seq_num)));
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let archived = TableMetadataArchive {
            has_point_keys: self.has_point_keys,
            smallest_seq_num: self.smallest_seq_num,
            largest_seq_num: self.largest_seq_num,
            properties: PropertiesArchive {
                id: self.properties.id,
                table_format: self.properties.table_format as u8,
                num_entries: self.properties.num_entries,
                num_deletions: self.properties.num_deletions,
                data_size: self.properties.data_size,
                oldest_vlog_file_id: self.properties.oldest_vlog_file_id,
                num_data_blocks: self.properties.num_data_blocks,
                index_size: self.properties.index_size,
                index_partitions: self.properties.index_partitions,
                top_level_index_size: self.properties.top_level_index_size,
                filter_size: self.properties.filter_size,
                raw_key_size: self.properties.raw_key_size,
                raw_value_size: self.properties.raw_value_size,
                created_at: self.properties.created_at,
                item_count: self.properties.item_count,
                key_count: self.properties.key_count,
                tombstone_count: self.properties.tombstone_count,
                num_soft_deletes: self.properties.num_soft_deletes,
                num_range_deletions: self.properties.num_range_deletions,
                block_size: self.properties.block_size,
                block_count: self.properties.block_count,
                compression: self.properties.compression as u8,
                seqnos: self.properties.seqnos,
                oldest_key_time: self.properties.oldest_key_time,
                newest_key_time: self.properties.newest_key_time,
            },
            smallest_point: self.smallest_point.as_ref().map(InternalKey::encode),
            largest_point: self.largest_point.as_ref().map(InternalKey::encode),
        };
        rkyv::to_bytes::<rkyv::rancor::Error>(&archived)
            .map(|bytes| bytes.to_vec())
            .expect("rkyv failed to encode table metadata")
    }

    pub(crate) fn decode(src: &[u8]) -> Result<TableMetadata> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(src.len());
        aligned.extend_from_slice(src);
        let archived = rkyv::from_bytes::<TableMetadataArchive, rkyv::rancor::Error>(&aligned)
            .map_err(|e| Error::Corruption(format!("invalid SSTable metadata archive: {e}")))?;

        let properties = Properties {
            id: archived.properties.id,
            table_format: TableFormat::from_u8(archived.properties.table_format)?,
            num_entries: archived.properties.num_entries,
            num_deletions: archived.properties.num_deletions,
            data_size: archived.properties.data_size,
            oldest_vlog_file_id: archived.properties.oldest_vlog_file_id,
            num_data_blocks: archived.properties.num_data_blocks,
            index_size: archived.properties.index_size,
            index_partitions: archived.properties.index_partitions,
            top_level_index_size: archived.properties.top_level_index_size,
            filter_size: archived.properties.filter_size,
            raw_key_size: archived.properties.raw_key_size,
            raw_value_size: archived.properties.raw_value_size,
            created_at: archived.properties.created_at,
            item_count: archived.properties.item_count,
            key_count: archived.properties.key_count,
            tombstone_count: archived.properties.tombstone_count,
            num_soft_deletes: archived.properties.num_soft_deletes,
            num_range_deletions: archived.properties.num_range_deletions,
            block_size: archived.properties.block_size,
            block_count: archived.properties.block_count,
            compression: CompressionType::try_from(archived.properties.compression)?,
            seqnos: archived.properties.seqnos,
            oldest_key_time: archived.properties.oldest_key_time,
            newest_key_time: archived.properties.newest_key_time,
        };

        Ok(TableMetadata {
            has_point_keys: archived.has_point_keys,
            smallest_seq_num: archived.smallest_seq_num,
            largest_seq_num: archived.largest_seq_num,
            properties,
            smallest_point: archived
                .smallest_point
                .as_ref()
                .map(|k| InternalKey::decode(k)),
            largest_point: archived
                .largest_point
                .as_ref()
                .map(|k| InternalKey::decode(k)),
        })
    }
}
