//! Compact binary format for ADS-B records.
//!
//! Design goals:
//! - Minimize storage size while preserving all meaningful precision
//! - O(1) decode without decompression
//! - Cache-friendly access patterns
//!
//! # Compression strategies:
//!
//! 1. **String interning**: Callsigns, registrations, aircraft types stored once
//!    in a string table, referenced by 16-bit index (saves ~24 bytes/record)
//!
//! 2. **Coordinate packing**:
//!    - Lat: ±90° needs 1 sign + enough precision. At 0.0001° (~11m), need 21 bits
//!    - Lon: ±180° needs 22 bits at same precision
//!    - Pack both into 6 bytes (48 bits) with ~5.5m precision
//!
//! 3. **Timestamp deltas**: Store base timestamp per chunk, then u16 ms deltas
//!    (good for 65 seconds per chunk)
//!
//! 4. **Altitude packing**:
//!    - Real range: -1000 to 60000 ft, store as i16 with 4ft resolution (not 25)
//!    - Or use 18 bits for full range at 1ft resolution
//!
//! 5. **Flag consolidation**: Pack all boolean flags into 3 bytes (24 flags)
//!
//! 6. **Enum packing**: data_source(4) + air_ground(2) + emergency(4) + category(4) = 14 bits
//!
//! 7. **Angular values**: Track/heading 0-360° at 0.1° = 3600 values = 12 bits
//!
//! 8. **Speed packing**: 0-800 knots at 0.5kt = 1600 values = 11 bits

use bytemuck::{Pod, Zeroable};
use std::collections::HashMap;
use parking_lot::RwLock;

/// String table for interned callsigns, registrations, and aircraft types.
/// Reduces per-record overhead from 24 bytes to 6 bytes (3x u16 indices).
#[derive(Debug, Default)]
pub struct StringTable {
    strings: RwLock<Vec<CompactString>>,
    index: RwLock<HashMap<[u8; 12], u16>>,
}

/// Compact string storage (up to 12 bytes, null-padded)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Pod, Zeroable)]
#[repr(C)]
pub struct CompactString {
    pub data: [u8; 12],
}

impl CompactString {
    pub fn new(s: &[u8]) -> Self {
        let mut data = [0u8; 12];
        let len = s.len().min(12);
        data[..len].copy_from_slice(&s[..len]);
        Self { data }
    }

    pub fn as_str(&self) -> &str {
        let end = self.data.iter().position(|&b| b == 0).unwrap_or(12);
        std::str::from_utf8(&self.data[..end]).unwrap_or("")
    }
}

impl StringTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Intern a string, returning its index (0 = empty/none)
    pub fn intern(&self, s: &[u8]) -> u16 {
        if s.is_empty() || s.iter().all(|&b| b == 0) {
            return 0;
        }

        let mut key = [0u8; 12];
        let len = s.len().min(12);
        key[..len].copy_from_slice(&s[..len]);

        // Check if already interned
        {
            let index = self.index.read();
            if let Some(&idx) = index.get(&key) {
                return idx;
            }
        }

        // Add new string
        let mut strings = self.strings.write();
        let mut index = self.index.write();

        // Double-check after acquiring write lock
        if let Some(&idx) = index.get(&key) {
            return idx;
        }

        let idx = strings.len() as u16 + 1; // 0 is reserved for "none"
        strings.push(CompactString { data: key });
        index.insert(key, idx);
        idx
    }

    /// Look up a string by index, returns owned String
    pub fn lookup(&self, idx: u16) -> Option<String> {
        if idx == 0 {
            return None;
        }
        let strings = self.strings.read();
        strings.get((idx - 1) as usize).map(|s| s.as_str().to_string())
    }

    /// Get raw string data for serialization
    pub fn get_raw(&self, idx: u16) -> Option<CompactString> {
        if idx == 0 {
            return None;
        }
        let strings = self.strings.read();
        strings.get((idx - 1) as usize).copied()
    }
}

/// Packed coordinate pair (lat + lon in 8 bytes with ~1.1m precision)
/// - Latitude: stored as i32, microdegrees (±90° = ±90,000,000)
/// - Longitude: stored as i32, microdegrees (±180° = ±180,000,000)
/// This keeps ~11cm precision which is better than ADS-B provides anyway.
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct PackedPosition {
    /// Latitude in microdegrees (divide by 1,000,000 for degrees)
    pub lat: i32,
    /// Longitude in microdegrees
    pub lon: i32,
}

impl PackedPosition {
    pub fn new(lat_deg: f64, lon_deg: f64) -> Self {
        Self {
            lat: (lat_deg * 1_000_000.0) as i32,
            lon: (lon_deg * 1_000_000.0) as i32,
        }
    }

    pub fn latitude(&self) -> f64 {
        self.lat as f64 / 1_000_000.0
    }

    pub fn longitude(&self) -> f64 {
        self.lon as f64 / 1_000_000.0
    }

    pub fn is_valid(&self) -> bool {
        self.lat != i32::MIN && self.lon != i32::MIN
    }
}

/// Packed flags (3 bytes = 24 bits for all validity and state flags)
#[derive(Debug, Clone, Copy, Pod, Zeroable, Default)]
#[repr(C)]
pub struct PackedFlags {
    pub bytes: [u8; 3],
}

impl PackedFlags {
    // Byte 0: validity flags part 1
    pub const HAS_POSITION: u8 = 0;
    pub const HAS_ALT_BARO: u8 = 1;
    pub const HAS_ALT_GEOM: u8 = 2;
    pub const HAS_GS: u8 = 3;
    pub const HAS_TRACK: u8 = 4;
    pub const HAS_MAG_HEADING: u8 = 5;
    pub const HAS_TRUE_HEADING: u8 = 6;
    pub const HAS_BARO_RATE: u8 = 7;

    // Byte 1: validity flags part 2
    pub const HAS_GEOM_RATE: u8 = 8;
    pub const HAS_SQUAWK: u8 = 9;
    pub const HAS_CALLSIGN: u8 = 10;
    pub const HAS_ROLL: u8 = 11;
    pub const HAS_NAV_QNH: u8 = 12;
    pub const HAS_NAV_ALT: u8 = 13;
    pub const HAS_NAV_HEADING: u8 = 14;
    pub const HAS_WIND: u8 = 15;

    // Byte 2: validity flags part 3 + state flags
    pub const HAS_TEMP: u8 = 16;
    pub const HAS_IAS_TAS: u8 = 17;
    pub const HAS_MACH: u8 = 18;
    pub const ON_GROUND: u8 = 19;
    pub const IS_TEMP_ICAO: u8 = 20;
    pub const HAS_EMERGENCY: u8 = 21;
    pub const SPI: u8 = 22;
    pub const ALERT: u8 = 23;

    #[inline]
    pub fn set(&mut self, flag: u8) {
        let byte = (flag / 8) as usize;
        let bit = flag % 8;
        self.bytes[byte] |= 1 << bit;
    }

    #[inline]
    pub fn get(&self, flag: u8) -> bool {
        let byte = (flag / 8) as usize;
        let bit = flag % 8;
        (self.bytes[byte] & (1 << bit)) != 0
    }
}

/// Packed enums (2 bytes for data_source, air_ground, emergency, category)
/// Layout: [data_source:4][air_ground:2][emergency:4][category:6]
#[derive(Debug, Clone, Copy, Pod, Zeroable, Default)]
#[repr(C)]
pub struct PackedEnums {
    pub bytes: [u8; 2],
}

impl PackedEnums {
    pub fn new(data_source: u8, air_ground: u8, emergency: u8, category: u8) -> Self {
        let b0 = (data_source & 0x0F) | ((air_ground & 0x03) << 4) | ((emergency & 0x03) << 6);
        let b1 = ((emergency >> 2) & 0x03) | ((category & 0x3F) << 2);
        Self { bytes: [b0, b1] }
    }

    pub fn data_source(&self) -> u8 {
        self.bytes[0] & 0x0F
    }

    pub fn air_ground(&self) -> u8 {
        (self.bytes[0] >> 4) & 0x03
    }

    pub fn emergency(&self) -> u8 {
        ((self.bytes[0] >> 6) & 0x03) | ((self.bytes[1] & 0x03) << 2)
    }

    pub fn category(&self) -> u8 {
        (self.bytes[1] >> 2) & 0x3F
    }
}

/// Packed angular values (track, headings) - 12 bits each = 0.088° precision
/// Layout: [track:12][mag_heading:12] = 3 bytes
#[derive(Debug, Clone, Copy, Pod, Zeroable, Default)]
#[repr(C)]
pub struct PackedAngles {
    pub bytes: [u8; 3],
}

impl PackedAngles {
    /// Create from degrees (0-360 range)
    pub fn new(track_deg: f32, mag_heading_deg: f32) -> Self {
        // Convert to 12-bit values (0-4095 represents 0-360°)
        let track = ((track_deg * 4096.0 / 360.0) as u16).min(4095);
        let heading = ((mag_heading_deg * 4096.0 / 360.0) as u16).min(4095);

        Self {
            bytes: [
                (track & 0xFF) as u8,
                ((track >> 8) & 0x0F) as u8 | ((heading & 0x0F) as u8) << 4,
                (heading >> 4) as u8,
            ],
        }
    }

    pub fn track(&self) -> f32 {
        let raw = (self.bytes[0] as u16) | ((self.bytes[1] as u16 & 0x0F) << 8);
        raw as f32 * 360.0 / 4096.0
    }

    pub fn mag_heading(&self) -> f32 {
        let raw = ((self.bytes[1] as u16) >> 4) | ((self.bytes[2] as u16) << 4);
        raw as f32 * 360.0 / 4096.0
    }
}

/// Packed speeds: ground speed (11 bits, 0.5kt) + vertical rate (12 bits, 8fpm)
/// Plus true heading (12 bits) = 35 bits, use 5 bytes
#[derive(Debug, Clone, Copy, Pod, Zeroable, Default)]
#[repr(C)]
pub struct PackedVelocity {
    pub bytes: [u8; 5],
}

impl PackedVelocity {
    /// Create from raw values
    /// - gs: ground speed in knots
    /// - vrate: vertical rate in fpm
    /// - true_heading: in degrees
    pub fn new(gs: f32, vrate: i16, true_heading: f32) -> Self {
        // GS: 0-1023.5 knots at 0.5kt resolution (11 bits)
        let gs_raw = ((gs * 2.0) as u16).min(2047);
        // VRate: ±16376 fpm at 8fpm resolution (12 bits signed)
        let vrate_raw = ((vrate / 8).clamp(-2048, 2047) + 2048) as u16;
        // True heading: 12 bits (same as track)
        let hdg_raw = ((true_heading * 4096.0 / 360.0) as u16).min(4095);

        Self {
            bytes: [
                (gs_raw & 0xFF) as u8,
                ((gs_raw >> 8) & 0x07) as u8 | ((vrate_raw & 0x1F) << 3) as u8,
                ((vrate_raw >> 5) & 0x7F) as u8 | ((hdg_raw & 0x01) << 7) as u8,
                ((hdg_raw >> 1) & 0xFF) as u8,
                ((hdg_raw >> 9) & 0x07) as u8,
            ],
        }
    }

    pub fn ground_speed(&self) -> f32 {
        let raw = (self.bytes[0] as u16) | ((self.bytes[1] as u16 & 0x07) << 8);
        raw as f32 * 0.5
    }

    pub fn vertical_rate(&self) -> i16 {
        let raw = ((self.bytes[1] as u16) >> 3) | ((self.bytes[2] as u16 & 0x7F) << 5);
        (raw as i16 - 2048) * 8
    }

    pub fn true_heading(&self) -> f32 {
        let raw = ((self.bytes[2] as u16) >> 7)
            | ((self.bytes[3] as u16) << 1)
            | ((self.bytes[4] as u16 & 0x07) << 9);
        raw as f32 * 360.0 / 4096.0
    }
}

/// Ultra-compact aircraft record (48 bytes vs original 112 bytes = 57% reduction)
///
/// Fields ordered to avoid padding:
/// - 8-byte aligned: position (8)
/// - 4-byte aligned: icao (4)
/// - 2-byte aligned: all u16/i16 fields (22 bytes)
/// - 1-byte aligned: packed structs and u8s (14 bytes)
/// Total: 48 bytes
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct CompactRecord {
    // === 8-byte aligned (offset 0) ===
    /// Packed lat/lon (8 bytes)
    pub position: PackedPosition,

    // === 4-byte aligned (offset 8) ===
    /// ICAO address (24 bits) + temp flag (bit 24)
    pub icao: u32,

    // === 2-byte aligned (offset 12) ===
    /// Milliseconds offset from chunk base timestamp
    pub timestamp_delta: u16,
    /// Barometric altitude (25ft units)
    pub alt_baro: i16,
    /// Geometric altitude (25ft units)
    pub alt_geom: i16,
    /// Squawk code
    pub squawk: u16,
    /// String table index for callsign
    pub callsign_idx: u16,
    /// String table index for registration
    pub registration_idx: u16,
    /// String table index for aircraft type
    pub type_idx: u16,
    /// Indicated airspeed in knots
    pub ias: u16,
    /// Nav/MCP altitude in 4ft units (0-262140 ft range)
    pub nav_alt: u16,

    // === 1-byte aligned (offset 32) ===
    /// Track and magnetic heading (3 bytes)
    pub angles: PackedAngles,
    /// Ground speed, vertical rate, true heading (5 bytes)
    pub velocity: PackedVelocity,
    /// All validity and state flags (3 bytes)
    pub flags: PackedFlags,
    /// Data source, air/ground, emergency, category (2 bytes)
    pub enums: PackedEnums,
    /// RSSI in dBm + 50 (0-255 = -50 to +205 dBm)
    pub rssi: u8,
    /// Number of receivers
    pub receiver_count: u8,
    /// Roll angle in degrees (±127°)
    pub roll: i8,
    /// Padding for alignment to 4 bytes (total 48)
    pub _padding: [u8; 2],
}

impl CompactRecord {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

/// Chunk header for a group of compact records
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct ChunkHeader {
    /// Base timestamp for this chunk (ms since epoch)
    pub base_timestamp_ms: u64,
    /// Number of records in this chunk
    pub record_count: u32,
    /// Size of string table section in bytes
    pub string_table_size: u32,
}

impl ChunkHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_record_size() {
        assert_eq!(CompactRecord::SIZE, 48, "CompactRecord should be 48 bytes");
        println!("CompactRecord size: {} bytes (vs 112 original = {:.0}% reduction)",
            CompactRecord::SIZE,
            (1.0 - CompactRecord::SIZE as f64 / 112.0) * 100.0
        );
    }

    #[test]
    fn test_packed_position() {
        let pos = PackedPosition::new(51.5074, -0.1278);
        assert!((pos.latitude() - 51.5074).abs() < 0.000001);
        assert!((pos.longitude() - (-0.1278)).abs() < 0.000001);
    }

    #[test]
    fn test_packed_angles() {
        let angles = PackedAngles::new(270.5, 180.0);
        assert!((angles.track() - 270.5).abs() < 0.1);
        assert!((angles.mag_heading() - 180.0).abs() < 0.1);
    }

    #[test]
    fn test_packed_velocity() {
        let vel = PackedVelocity::new(450.0, 2000, 90.0);
        assert!((vel.ground_speed() - 450.0).abs() < 1.0);
        assert!((vel.vertical_rate() - 2000).abs() < 16);
        assert!((vel.true_heading() - 90.0).abs() < 0.1);
    }

    #[test]
    fn test_packed_enums() {
        let enums = PackedEnums::new(5, 2, 7, 33);
        assert_eq!(enums.data_source(), 5);
        assert_eq!(enums.air_ground(), 2);
        assert_eq!(enums.emergency(), 7);
        assert_eq!(enums.category(), 33);
    }

    #[test]
    fn test_packed_flags() {
        let mut flags = PackedFlags::default();
        flags.set(PackedFlags::HAS_POSITION);
        flags.set(PackedFlags::HAS_ALT_BARO);
        flags.set(PackedFlags::ON_GROUND);

        assert!(flags.get(PackedFlags::HAS_POSITION));
        assert!(flags.get(PackedFlags::HAS_ALT_BARO));
        assert!(flags.get(PackedFlags::ON_GROUND));
        assert!(!flags.get(PackedFlags::HAS_SQUAWK));
    }

    #[test]
    fn test_string_table() {
        let table = StringTable::new();

        let idx1 = table.intern(b"UAL123");
        let idx2 = table.intern(b"UAL123");
        let idx3 = table.intern(b"DAL456");

        assert_eq!(idx1, idx2, "Same string should return same index");
        assert_ne!(idx1, idx3, "Different strings should have different indices");
        assert_eq!(table.intern(b""), 0, "Empty string should return 0");
    }
}
