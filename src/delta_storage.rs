//! Delta-compressed storage engine for ADS-B data.
//!
//! Uses keyframe + delta encoding to achieve ~85% compression vs raw records.
//!
//! # Storage Format
//!
//! ```text
//! Data File (.delta):
//! ┌─────────────────────────────────────┐
//! │ Header (64 bytes)                    │
//! │  - Magic: "ADSB-DLT"                │
//! │  - Version: u32                      │
//! │  - Chunk base timestamp: u64         │
//! │  - Record count: u64                 │
//! ├─────────────────────────────────────┤
//! │ Records (variable length)            │
//! │  - Keyframes (36 bytes)              │
//! │  - Deltas (8-16 bytes)               │
//! └─────────────────────────────────────┘
//!
//! Index File (.dix):
//! ┌─────────────────────────────────────┐
//! │ Header (64 bytes)                    │
//! │  - Magic: "ADSB-DIX"                │
//! │  - Version: u32                      │
//! │  - Aircraft count: u64               │
//! ├─────────────────────────────────────┤
//! │ Hash Table (16M entries x 24 bytes) │
//! │  - ICAO: u32                         │
//! │  - Last keyframe offset: u64         │
//! │  - Record count: u32                 │
//! │  - Last timestamp: u64               │
//! └─────────────────────────────────────┘
//! ```

use crate::delta::{
    AircraftState, DeltaEncoder, DeltaFieldMask, DeltaFlags, DeltaHeader, DeltaStats, Keyframe,
    PositionDelta, VelocityDelta,
};
use crate::types::AircraftRecord;
use bytemuck::{Pod, Zeroable};
use memmap2::{MmapMut, MmapOptions};
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeltaStorageError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid file format")]
    InvalidFormat,
    #[error("Version mismatch: expected {expected}, got {got}")]
    VersionMismatch { expected: u32, got: u32 },
    #[error("Storage full")]
    StorageFull,
    #[error("Corrupted data")]
    Corrupted,
}

const INDEX_MAGIC: &[u8; 8] = b"ADSB-DIX";
const DATA_MAGIC: &[u8; 8] = b"ADSB-DLT";
const CURRENT_VERSION: u32 = 1;

const HASH_TABLE_SIZE: usize = 16 * 1024 * 1024;
const INDEX_HEADER_SIZE: usize = 64;
const DATA_HEADER_SIZE: usize = 64;

/// Keyframe interval (emit keyframe every N records per aircraft)
const KEYFRAME_INTERVAL: u32 = 30;

/// Index entry for delta storage (24 bytes)
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct DeltaIndexEntry {
    icao: u32,
    record_count: u32,
    last_keyframe_offset: u64,
    last_timestamp_ms: u64,
}

impl DeltaIndexEntry {
    const SIZE: usize = std::mem::size_of::<Self>();

    fn is_empty(&self) -> bool {
        self.icao == 0
    }
}

/// Index file header
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct DeltaIndexHeader {
    magic: [u8; 8],
    version: u32,
    _pad1: u32,
    aircraft_count: u64,
    total_records: u64,
    keyframe_count: u64,
    delta_count: u64,
    _reserved: [u8; 16],
}

/// Data file header
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct DeltaDataHeader {
    magic: [u8; 8],
    version: u32,
    _pad1: u32,
    base_timestamp_ms: u64,
    record_count: u64,
    write_offset: u64,
    total_bytes_saved: u64,
    _reserved: [u8; 16],
}

/// Delta-compressed storage engine
pub struct DeltaStorage {
    index_file: File,
    data_file: File,
    index_mmap: RwLock<MmapMut>,
    data_mmap: RwLock<MmapMut>,
    write_offset: AtomicU64,
    data_capacity: AtomicU64,
    encoder: RwLock<DeltaEncoder>,
    stats: RwLock<DeltaStats>,
}

impl DeltaStorage {
    /// Open or create delta storage at the given path.
    pub fn open<P: AsRef<Path>>(base_path: P) -> Result<Self, DeltaStorageError> {
        let base = base_path.as_ref();
        let index_path = base.with_extension("dix");
        let data_path = base.with_extension("delta");

        let (index_file, index_mmap, is_new) = Self::open_index(&index_path)?;
        let (data_file, data_mmap, write_offset, capacity) =
            Self::open_data(&data_path, is_new)?;

        Ok(Self {
            index_file,
            data_file,
            index_mmap: RwLock::new(index_mmap),
            data_mmap: RwLock::new(data_mmap),
            write_offset: AtomicU64::new(write_offset),
            data_capacity: AtomicU64::new(capacity),
            encoder: RwLock::new(DeltaEncoder::new(KEYFRAME_INTERVAL)),
            stats: RwLock::new(DeltaStats::default()),
        })
    }

    fn open_index(path: &Path) -> Result<(File, MmapMut, bool), DeltaStorageError> {
        let index_size = INDEX_HEADER_SIZE + HASH_TABLE_SIZE * DeltaIndexEntry::SIZE;
        let is_new = !path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        if is_new || file.metadata()?.len() == 0 {
            file.set_len(index_size as u64)?;
        }

        let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        if is_new || mmap[..8] != *INDEX_MAGIC {
            let header = DeltaIndexHeader {
                magic: *INDEX_MAGIC,
                version: CURRENT_VERSION,
                _pad1: 0,
                aircraft_count: 0,
                total_records: 0,
                keyframe_count: 0,
                delta_count: 0,
                _reserved: [0; 16],
            };
            mmap[..INDEX_HEADER_SIZE].copy_from_slice(bytemuck::bytes_of(&header));
            mmap.flush()?;
        } else {
            let header: &DeltaIndexHeader =
                bytemuck::from_bytes(&mmap[..std::mem::size_of::<DeltaIndexHeader>()]);
            if header.version != CURRENT_VERSION {
                return Err(DeltaStorageError::VersionMismatch {
                    expected: CURRENT_VERSION,
                    got: header.version,
                });
            }
        }

        Ok((file, mmap, is_new))
    }

    fn open_data(path: &Path, is_new: bool) -> Result<(File, MmapMut, u64, u64), DeltaStorageError> {
        let initial_capacity: u64 = 512 * 1024 * 1024; // 512MB (smaller due to compression)

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let file_len = file.metadata()?.len();
        let capacity = if file_len < initial_capacity {
            file.set_len(initial_capacity)?;
            initial_capacity
        } else {
            file_len
        };

        let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        let write_offset = if is_new || mmap[..8] != *DATA_MAGIC {
            let header = DeltaDataHeader {
                magic: *DATA_MAGIC,
                version: CURRENT_VERSION,
                _pad1: 0,
                base_timestamp_ms: 0,
                record_count: 0,
                write_offset: DATA_HEADER_SIZE as u64,
                total_bytes_saved: 0,
                _reserved: [0; 16],
            };
            mmap[..DATA_HEADER_SIZE].copy_from_slice(bytemuck::bytes_of(&header));
            mmap.flush()?;
            DATA_HEADER_SIZE as u64
        } else {
            let header: &DeltaDataHeader =
                bytemuck::from_bytes(&mmap[..std::mem::size_of::<DeltaDataHeader>()]);
            header.write_offset
        };

        Ok((file, mmap, write_offset, capacity))
    }

    /// Write a batch of aircraft records using delta compression.
    pub fn write_batch(&self, records: &[AircraftRecord]) -> Result<usize, DeltaStorageError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut written = 0;
        let mut bytes_written = 0u64;
        let mut keyframes_written = 0u64;
        let mut deltas_written = 0u64;

        let mut index_guard = self.index_mmap.write();
        let mut data_guard = self.data_mmap.write();
        let mut encoder = self.encoder.write();

        for record in records {
            let icao = record.icao;
            let timestamp_ms = record.timestamp_ms;

            // Extract current state from record
            let current_state = Self::record_to_state(record);

            // Check if we need a keyframe
            let needs_keyframe = encoder.needs_keyframe(icao, timestamp_ms)
                || !encoder.can_encode_position_delta(
                    icao,
                    record.lat_microdeg,
                    record.lon_microdeg,
                );

            let write_pos = self.write_offset.load(Ordering::Acquire);

            if needs_keyframe {
                // Write keyframe
                let keyframe = Self::record_to_keyframe(record);
                let needed = write_pos + Keyframe::SIZE as u64;

                if needed > self.data_capacity.load(Ordering::Acquire) {
                    self.grow_data_file(&mut data_guard)?;
                }

                let bytes = bytemuck::bytes_of(&keyframe);
                data_guard[write_pos as usize..write_pos as usize + Keyframe::SIZE]
                    .copy_from_slice(bytes);

                self.write_offset
                    .fetch_add(Keyframe::SIZE as u64, Ordering::Release);
                bytes_written += Keyframe::SIZE as u64;
                keyframes_written += 1;

                // Update index
                let slot = self.find_or_create_slot(&mut index_guard, icao)?;
                let entry = self.get_index_entry_mut(&mut index_guard, slot);

                if entry.icao == 0 {
                    entry.icao = icao;
                    entry.last_keyframe_offset = write_pos;
                    entry.record_count = 1;
                    entry.last_timestamp_ms = timestamp_ms;

                    let header: &mut DeltaIndexHeader =
                        bytemuck::from_bytes_mut(&mut index_guard[..INDEX_HEADER_SIZE]);
                    header.aircraft_count += 1;
                } else {
                    entry.last_keyframe_offset = write_pos;
                    entry.record_count += 1;
                    entry.last_timestamp_ms = timestamp_ms;
                }

                encoder.update_state(icao, current_state, true);
            } else {
                // Write delta
                let prev_state = encoder.get_state(icao).unwrap();
                let (delta_bytes, delta_size) =
                    Self::encode_delta(record, prev_state, timestamp_ms)?;

                let needed = write_pos + delta_size as u64;
                if needed > self.data_capacity.load(Ordering::Acquire) {
                    self.grow_data_file(&mut data_guard)?;
                }

                data_guard[write_pos as usize..write_pos as usize + delta_size]
                    .copy_from_slice(&delta_bytes[..delta_size]);

                self.write_offset
                    .fetch_add(delta_size as u64, Ordering::Release);
                bytes_written += delta_size as u64;
                deltas_written += 1;

                // Update index
                let slot = self.find_or_create_slot(&mut index_guard, icao)?;
                let entry = self.get_index_entry_mut(&mut index_guard, slot);
                entry.record_count += 1;
                entry.last_timestamp_ms = timestamp_ms;

                encoder.update_state(icao, current_state, false);
            }

            written += 1;
        }

        // Update headers
        let data_header: &mut DeltaDataHeader =
            bytemuck::from_bytes_mut(&mut data_guard[..DATA_HEADER_SIZE]);
        data_header.record_count += written as u64;
        data_header.write_offset = self.write_offset.load(Ordering::Acquire);

        // Calculate bytes saved vs raw storage
        let raw_bytes = (written as u64) * (std::mem::size_of::<AircraftRecord>() as u64);
        if raw_bytes > bytes_written {
            data_header.total_bytes_saved += raw_bytes - bytes_written;
        }

        let index_header: &mut DeltaIndexHeader =
            bytemuck::from_bytes_mut(&mut index_guard[..INDEX_HEADER_SIZE]);
        index_header.total_records += written as u64;
        index_header.keyframe_count += keyframes_written;
        index_header.delta_count += deltas_written;

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.keyframes += keyframes_written;
            stats.deltas += deltas_written;
            stats.total_bytes += bytes_written;
        }

        Ok(written)
    }

    fn record_to_state(record: &AircraftRecord) -> AircraftState {
        AircraftState {
            timestamp_ms: record.timestamp_ms,
            lat: record.lat_microdeg,
            lon: record.lon_microdeg,
            alt_baro: record.alt_baro_raw,
            alt_geom: record.alt_geom_raw,
            gs: record.gs_raw as u16,
            track: record.track_raw as u16,
            vrate: record.baro_rate_raw,
            squawk: record.squawk,
            on_ground: record.air_ground == 1, // Ground = 1
        }
    }

    fn record_to_keyframe(record: &AircraftRecord) -> Keyframe {
        let mut flags = 0u8;
        if record.air_ground == 1 {
            flags |= 1 << DeltaFlags::ON_GROUND;
        }
        flags |= 1 << DeltaFlags::IS_KEYFRAME;

        Keyframe {
            icao: record.icao,
            timestamp_lo: (record.timestamp_ms & 0xFFFFFFFF) as u32,
            lat: record.lat_microdeg,
            lon: record.lon_microdeg,
            alt_baro: record.alt_baro_raw,
            alt_geom: record.alt_geom_raw,
            gs: record.gs_raw as u16,
            track: record.track_raw as u16,
            vrate: record.baro_rate_raw,
            squawk: record.squawk,
            callsign_idx: 0, // TODO: string interning
            reg_idx: 0,
            type_idx: 0,
            flags,
            category: record.category,
        }
    }

    fn encode_delta(
        record: &AircraftRecord,
        prev: &AircraftState,
        timestamp_ms: u64,
    ) -> Result<([u8; 24], usize), DeltaStorageError> {
        let mut buf = [0u8; 24];
        let mut offset = 0;

        // Time delta (saturate at u16::MAX = ~65 seconds)
        let time_delta = timestamp_ms
            .saturating_sub(prev.timestamp_ms)
            .min(u16::MAX as u64) as u16;

        // Build field mask
        let mut field_mask = DeltaFieldMask::default();

        let lat_delta = record.lat_microdeg - prev.lat;
        let lon_delta = record.lon_microdeg - prev.lon;
        let has_position = lat_delta != 0 || lon_delta != 0;
        if has_position {
            field_mask.set(DeltaFieldMask::POSITION);
        }

        let alt_baro_delta = record.alt_baro_raw - prev.alt_baro;
        if alt_baro_delta != 0 {
            field_mask.set(DeltaFieldMask::ALT_BARO);
        }

        let alt_geom_delta = record.alt_geom_raw - prev.alt_geom;
        if alt_geom_delta != 0 {
            field_mask.set(DeltaFieldMask::ALT_GEOM);
        }

        let gs_delta = record.gs_raw - prev.gs as i16;
        let track_delta = record.track_raw - prev.track as i16;
        let vrate_delta = record.baro_rate_raw - prev.vrate;
        let has_velocity = gs_delta != 0 || track_delta != 0 || vrate_delta != 0;
        if has_velocity {
            field_mask.set(DeltaFieldMask::VELOCITY);
        }

        if record.squawk != prev.squawk {
            field_mask.set(DeltaFieldMask::SQUAWK);
        }

        // Build flags
        let mut flags = DeltaFlags::default();
        if record.air_ground == 1 {
            flags.set(DeltaFlags::ON_GROUND);
        }

        // Write header (8 bytes)
        let header = DeltaHeader {
            icao: record.icao,
            time_delta,
            flags,
            field_mask,
        };
        buf[offset..offset + DeltaHeader::SIZE].copy_from_slice(bytemuck::bytes_of(&header));
        offset += DeltaHeader::SIZE;

        // Write optional fields
        if field_mask.has(DeltaFieldMask::POSITION) {
            if let Some(pos_delta) = PositionDelta::new(lat_delta, lon_delta) {
                buf[offset..offset + 4].copy_from_slice(bytemuck::bytes_of(&pos_delta));
                offset += 4;
            } else {
                // Should have triggered keyframe - this is a bug
                return Err(DeltaStorageError::Corrupted);
            }
        }

        if field_mask.has(DeltaFieldMask::ALT_BARO) {
            // Delta in 32ft units
            let delta = (alt_baro_delta / 32).clamp(i8::MIN as i16, i8::MAX as i16) as i8;
            buf[offset] = delta as u8;
            offset += 1;
        }

        if field_mask.has(DeltaFieldMask::ALT_GEOM) {
            let delta = (alt_geom_delta / 32).clamp(i8::MIN as i16, i8::MAX as i16) as i8;
            buf[offset] = delta as u8;
            offset += 1;
        }

        if field_mask.has(DeltaFieldMask::VELOCITY) {
            // Normalize track delta to -180..180 (use i32 to avoid overflow)
            let mut track_d = track_delta as i32;
            if track_d > 18000 {
                track_d -= 36000;
            } else if track_d < -18000 {
                track_d += 36000;
            }
            // Convert from 0.01° units to 1° units
            let track_deg = (track_d / 100).clamp(-127, 127) as i16;

            // GS is in 0.1kt units, convert to kt
            let gs_kt = (gs_delta / 10).clamp(-31, 31) as i16;

            // VRate is in 8fpm units, convert delta
            let vrate_fpm = (vrate_delta * 8).clamp(-500, 500) as i16;

            if let Some(vel_delta) = VelocityDelta::new(gs_kt, track_deg, vrate_fpm) {
                buf[offset..offset + 3].copy_from_slice(&vel_delta.bytes);
                offset += 3;
            } else {
                // Velocity change too large - write zeros (will lose precision)
                buf[offset..offset + 3].copy_from_slice(&[0x20, 0x80, 0x08]); // zero deltas
                offset += 3;
            }
        }

        if field_mask.has(DeltaFieldMask::SQUAWK) {
            buf[offset..offset + 2].copy_from_slice(&record.squawk.to_le_bytes());
            offset += 2;
        }

        Ok((buf, offset))
    }

    fn find_or_create_slot(
        &self,
        mmap: &mut MmapMut,
        icao: u32,
    ) -> Result<usize, DeltaStorageError> {
        let hash = Self::hash_icao(icao);
        let mut slot = hash % HASH_TABLE_SIZE;
        let start_slot = slot;

        loop {
            let entry = self.get_index_entry(mmap, slot);
            if entry.is_empty() || entry.icao == icao {
                return Ok(slot);
            }

            slot = (slot + 1) % HASH_TABLE_SIZE;
            if slot == start_slot {
                return Err(DeltaStorageError::StorageFull);
            }
        }
    }

    fn get_index_entry(&self, mmap: &MmapMut, slot: usize) -> DeltaIndexEntry {
        let offset = INDEX_HEADER_SIZE + slot * DeltaIndexEntry::SIZE;
        *bytemuck::from_bytes(&mmap[offset..offset + DeltaIndexEntry::SIZE])
    }

    fn get_index_entry_mut<'a>(
        &self,
        mmap: &'a mut MmapMut,
        slot: usize,
    ) -> &'a mut DeltaIndexEntry {
        let offset = INDEX_HEADER_SIZE + slot * DeltaIndexEntry::SIZE;
        bytemuck::from_bytes_mut(&mut mmap[offset..offset + DeltaIndexEntry::SIZE])
    }

    fn hash_icao(icao: u32) -> usize {
        let mut hash = 2166136261u32;
        hash ^= icao & 0xFF;
        hash = hash.wrapping_mul(16777619);
        hash ^= (icao >> 8) & 0xFF;
        hash = hash.wrapping_mul(16777619);
        hash ^= (icao >> 16) & 0xFF;
        hash = hash.wrapping_mul(16777619);
        hash as usize
    }

    fn grow_data_file(&self, mmap: &mut MmapMut) -> Result<(), DeltaStorageError> {
        let current_cap = self.data_capacity.load(Ordering::Acquire);
        let new_cap = current_cap * 2;

        self.data_file.set_len(new_cap)?;
        *mmap = unsafe { MmapOptions::new().map_mut(&self.data_file)? };
        self.data_capacity.store(new_cap, Ordering::Release);

        tracing::info!(
            "Grew delta data file to {} MB",
            new_cap / (1024 * 1024)
        );
        Ok(())
    }

    /// Get storage statistics.
    pub fn stats(&self) -> DeltaStorageStats {
        let index_guard = self.index_mmap.read();
        let data_guard = self.data_mmap.read();

        let index_header: &DeltaIndexHeader =
            bytemuck::from_bytes(&index_guard[..std::mem::size_of::<DeltaIndexHeader>()]);
        let data_header: &DeltaDataHeader =
            bytemuck::from_bytes(&data_guard[..std::mem::size_of::<DeltaDataHeader>()]);

        let internal_stats = self.stats.read();

        DeltaStorageStats {
            aircraft_count: index_header.aircraft_count,
            total_records: index_header.total_records,
            keyframe_count: index_header.keyframe_count,
            delta_count: index_header.delta_count,
            data_size_bytes: self.write_offset.load(Ordering::Acquire),
            data_capacity_bytes: self.data_capacity.load(Ordering::Acquire),
            bytes_saved: data_header.total_bytes_saved,
            avg_bytes_per_record: internal_stats.avg_bytes_per_record(),
            compression_ratio: internal_stats.compression_ratio(),
        }
    }

    /// Flush all pending writes to disk.
    pub fn sync(&self) -> Result<(), DeltaStorageError> {
        {
            let index_guard = self.index_mmap.write();
            index_guard.flush()?;
        }
        {
            let data_guard = self.data_mmap.write();
            data_guard.flush()?;
        }
        self.index_file.sync_all()?;
        self.data_file.sync_all()?;
        Ok(())
    }
}

/// Delta storage statistics.
#[derive(Debug, Clone)]
pub struct DeltaStorageStats {
    pub aircraft_count: u64,
    pub total_records: u64,
    pub keyframe_count: u64,
    pub delta_count: u64,
    pub data_size_bytes: u64,
    pub data_capacity_bytes: u64,
    pub bytes_saved: u64,
    pub avg_bytes_per_record: f64,
    pub compression_ratio: f64,
}

impl DeltaStorageStats {
    pub fn data_size_mb(&self) -> f64 {
        self.data_size_bytes as f64 / (1024.0 * 1024.0)
    }

    pub fn bytes_saved_mb(&self) -> f64 {
        self.bytes_saved as f64 / (1024.0 * 1024.0)
    }

    pub fn keyframe_ratio(&self) -> f64 {
        if self.total_records == 0 {
            0.0
        } else {
            self.keyframe_count as f64 / self.total_records as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_record(icao: u32, ts: u64, lat: i32, lon: i32, alt: i16) -> AircraftRecord {
        let mut record = AircraftRecord::zeroed();
        record.icao = icao;
        record.timestamp_ms = ts;
        record.lat_microdeg = lat;
        record.lon_microdeg = lon;
        record.alt_baro_raw = alt;
        record.gs_raw = 2500; // 250 knots in 0.1kt units
        record.track_raw = 9000; // 90 degrees in 0.01° units
        record
    }

    #[test]
    fn test_delta_storage_create() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test");

        let storage = DeltaStorage::open(&path).unwrap();
        let stats = storage.stats();
        assert_eq!(stats.aircraft_count, 0);
        assert_eq!(stats.total_records, 0);
    }

    #[test]
    fn test_delta_storage_write_keyframe() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test");

        let storage = DeltaStorage::open(&path).unwrap();

        // First record should be a keyframe
        let record = make_record(0x740828, 1000, 51_500_000, -100_000, 350);
        let written = storage.write_batch(&[record]).unwrap();
        assert_eq!(written, 1);

        let stats = storage.stats();
        assert_eq!(stats.aircraft_count, 1);
        assert_eq!(stats.keyframe_count, 1);
        assert_eq!(stats.delta_count, 0);
    }

    #[test]
    fn test_delta_storage_write_delta() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test");

        let storage = DeltaStorage::open(&path).unwrap();

        // First record - keyframe
        let record1 = make_record(0x740828, 1000, 51_500_000, -100_000, 350);
        storage.write_batch(&[record1]).unwrap();

        // Second record - should be delta (small position change)
        let record2 = make_record(0x740828, 2000, 51_500_100, -99_900, 352);
        storage.write_batch(&[record2]).unwrap();

        let stats = storage.stats();
        assert_eq!(stats.keyframe_count, 1);
        assert_eq!(stats.delta_count, 1);
        assert_eq!(stats.total_records, 2);

        println!("Avg bytes/record: {:.1}", stats.avg_bytes_per_record);
        println!("Compression ratio: {:.1}x", stats.compression_ratio);
    }

    #[test]
    fn test_delta_storage_compression() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test");

        let storage = DeltaStorage::open(&path).unwrap();

        // Simulate continuous tracking - 100 records
        let icao = 0x740828;
        let mut lat = 51_500_000i32;
        let mut lon = -100_000i32;
        let mut alt = 350i16;

        for i in 0..100 {
            let record = make_record(icao, i * 1000, lat, lon, alt);
            storage.write_batch(&[record]).unwrap();

            // Small movements
            lat += 50; // ~5m
            lon -= 30;
            if i % 10 == 0 {
                alt += 4; // 100ft
            }
        }

        let stats = storage.stats();
        println!("\nCompression test results:");
        println!("  Total records: {}", stats.total_records);
        println!("  Keyframes: {}", stats.keyframe_count);
        println!("  Deltas: {}", stats.delta_count);
        println!("  Data size: {:.2} KB", stats.data_size_bytes as f64 / 1024.0);
        println!("  Avg bytes/record: {:.1}", stats.avg_bytes_per_record);
        println!("  Compression ratio: {:.1}x", stats.compression_ratio);
        println!("  Bytes saved: {:.2} KB", stats.bytes_saved_mb() * 1024.0);

        // Should achieve at least 4x compression
        assert!(stats.compression_ratio > 4.0);
    }
}
