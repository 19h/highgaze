//! Delta-compressed aircraft records.
//!
//! Stores position updates as deltas from previous state, achieving
//! ~80% compression vs raw records for continuous tracking.
//!
//! # Format
//!
//! Two record types:
//! 1. **Keyframe** (40 bytes): Full state, emitted on first sight or after gap
//! 2. **Delta** (8-16 bytes): Changes from previous state
//!
//! # Compression ratios
//!
//! | Scenario | Avg bytes/record |
//! |----------|------------------|
//! | Original AircraftRecord | 112 |
//! | CompactRecord | 48 |
//! | Delta (continuous) | ~12 |
//! | Delta (with keyframes 1:30) | ~14 |

use bytemuck::{Pod, Zeroable};
use std::collections::HashMap;

/// Flags indicating which fields are present in a delta record
#[derive(Debug, Clone, Copy, Pod, Zeroable, Default)]
#[repr(C)]
pub struct DeltaFlags(pub u8);

impl DeltaFlags {
    pub const HAS_POSITION: u8 = 0;
    pub const HAS_ALT_BARO: u8 = 1;
    pub const HAS_ALT_GEOM: u8 = 2;
    pub const HAS_VELOCITY: u8 = 3;  // gs + track + vrate packed
    pub const HAS_SQUAWK: u8 = 4;
    pub const ON_GROUND: u8 = 5;
    pub const IS_KEYFRAME: u8 = 7;  // If set, this is actually a keyframe

    #[inline]
    pub fn set(&mut self, flag: u8) {
        self.0 |= 1 << flag;
    }

    #[inline]
    pub fn get(&self, flag: u8) -> bool {
        (self.0 & (1 << flag)) != 0
    }

    pub fn is_keyframe(&self) -> bool {
        self.get(Self::IS_KEYFRAME)
    }
}

/// Keyframe record - full aircraft state (40 bytes)
/// Used for first sighting or after significant gap/change
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct Keyframe {
    /// ICAO address
    pub icao: u32,
    /// Timestamp (ms since epoch, lower 32 bits - combine with chunk base)
    pub timestamp_lo: u32,
    /// Latitude in microdegrees
    pub lat: i32,
    /// Longitude in microdegrees
    pub lon: i32,
    /// Barometric altitude (25ft units)
    pub alt_baro: i16,
    /// Geometric altitude (25ft units)
    pub alt_geom: i16,
    /// Ground speed (0.1kt units)
    pub gs: u16,
    /// Track (0.01° units, 0-35999)
    pub track: u16,
    /// Vertical rate (8fpm units)
    pub vrate: i16,
    /// Squawk
    pub squawk: u16,
    /// Callsign index
    pub callsign_idx: u16,
    /// Registration index
    pub reg_idx: u16,
    /// Type index
    pub type_idx: u16,
    /// Flags (data source, on_ground, etc)
    pub flags: u8,
    /// Category
    pub category: u8,
}

impl Keyframe {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

/// Delta record - changes from previous state (8 bytes base + optional fields)
///
/// Base (always present): 8 bytes
/// - icao: u32
/// - time_delta: u16 (ms since last record for this aircraft)
/// - flags: u8
/// - field_mask: u8 (which optional fields follow)
///
/// Optional fields (1-8 bytes based on field_mask):
/// - position: i16, i16 (delta lat/lon in ~1.1m units) - 4 bytes
/// - alt_baro: i8 (delta in 32ft units, ±4096ft range) - 1 byte
/// - alt_geom: i8 (delta in 32ft units) - 1 byte
/// - velocity: packed gs_delta(i6) + track_delta(i8) + vrate_delta(i6) - 3 bytes
/// - squawk: u16 - 2 bytes
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct DeltaHeader {
    /// ICAO address (could use 3 bytes but 4 for alignment)
    pub icao: u32,
    /// Milliseconds since last record for this aircraft
    pub time_delta: u16,
    /// State flags (on_ground, etc)
    pub flags: DeltaFlags,
    /// Which optional fields are present
    pub field_mask: DeltaFieldMask,
}

impl DeltaHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

/// Bitmask for optional fields in delta record
#[derive(Debug, Clone, Copy, Pod, Zeroable, Default)]
#[repr(C)]
pub struct DeltaFieldMask(pub u8);

impl DeltaFieldMask {
    pub const POSITION: u8 = 0;    // 4 bytes: i16 lat_delta, i16 lon_delta
    pub const ALT_BARO: u8 = 1;    // 1 byte: i8 * 32ft
    pub const ALT_GEOM: u8 = 2;    // 1 byte: i8 * 32ft
    pub const VELOCITY: u8 = 3;    // 3 bytes: packed gs/track/vrate deltas
    pub const SQUAWK: u8 = 4;      // 2 bytes: new squawk

    #[inline]
    pub fn set(&mut self, field: u8) {
        self.0 |= 1 << field;
    }

    #[inline]
    pub fn has(&self, field: u8) -> bool {
        (self.0 & (1 << field)) != 0
    }

    /// Calculate total size of optional fields
    pub fn optional_size(&self) -> usize {
        let mut size = 0;
        if self.has(Self::POSITION) { size += 4; }
        if self.has(Self::ALT_BARO) { size += 1; }
        if self.has(Self::ALT_GEOM) { size += 1; }
        if self.has(Self::VELOCITY) { size += 3; }
        if self.has(Self::SQUAWK) { size += 2; }
        size
    }
}

/// Position delta (4 bytes)
/// Each unit is ~1.1m (microdegree / 10)
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct PositionDelta {
    /// Latitude delta in 0.1 microdegree units (~1.1m)
    pub lat: i16,
    /// Longitude delta in 0.1 microdegree units
    pub lon: i16,
}

impl PositionDelta {
    /// Range: ±3.2km at ~1.1m resolution
    pub const SCALE: f64 = 10.0; // microdegrees per unit

    pub fn new(lat_microdeg_delta: i32, lon_microdeg_delta: i32) -> Option<Self> {
        let lat = lat_microdeg_delta / Self::SCALE as i32;
        let lon = lon_microdeg_delta / Self::SCALE as i32;

        if lat >= i16::MIN as i32 && lat <= i16::MAX as i32 &&
           lon >= i16::MIN as i32 && lon <= i16::MAX as i32 {
            Some(Self { lat: lat as i16, lon: lon as i16 })
        } else {
            None // Delta too large, need keyframe
        }
    }

    pub fn apply(&self, base_lat: i32, base_lon: i32) -> (i32, i32) {
        (
            base_lat + (self.lat as i32 * Self::SCALE as i32),
            base_lon + (self.lon as i32 * Self::SCALE as i32),
        )
    }
}

/// Packed velocity delta (3 bytes)
/// Layout: [gs_delta:6][track_delta_lo:2] [track_delta_hi:6][vrate_delta_lo:2] [vrate_delta_hi:4][unused:4]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct VelocityDelta {
    pub bytes: [u8; 3],
}

impl VelocityDelta {
    /// GS delta: ±31 knots at 1kt resolution (6 bits signed)
    /// Track delta: ±127° at 1° resolution (8 bits signed)
    /// VRate delta: ±500 fpm at ~16fpm resolution (6 bits signed, *16)
    pub fn new(gs_delta_kt: i16, track_delta_deg: i16, vrate_delta_fpm: i16) -> Option<Self> {
        // Clamp and check ranges
        let gs = gs_delta_kt.clamp(-31, 31);
        let track = track_delta_deg.clamp(-127, 127);
        let vrate = (vrate_delta_fpm / 16).clamp(-31, 31);

        // Check if we lost precision
        if gs != gs_delta_kt || track != track_delta_deg ||
           (vrate * 16 - vrate_delta_fpm).abs() > 32 {
            return None; // Need keyframe for large changes
        }

        let gs_u = (gs + 32) as u8; // 0-63
        let track_u = (track + 128) as u8; // 0-255
        let vrate_u = (vrate + 32) as u8; // 0-63

        Some(Self {
            bytes: [
                (gs_u & 0x3F) | ((track_u & 0x03) << 6),
                ((track_u >> 2) & 0x3F) | ((vrate_u & 0x03) << 6),
                (vrate_u >> 2) & 0x0F,
            ],
        })
    }

    pub fn gs_delta(&self) -> i16 {
        let raw = (self.bytes[0] & 0x3F) as i16;
        raw - 32
    }

    pub fn track_delta(&self) -> i16 {
        let raw = ((self.bytes[0] >> 6) as u16) | (((self.bytes[1] & 0x3F) as u16) << 2);
        raw as i16 - 128
    }

    pub fn vrate_delta(&self) -> i16 {
        let raw = ((self.bytes[1] >> 6) as u8) | ((self.bytes[2] & 0x0F) << 2);
        (raw as i16 - 32) * 16
    }
}

/// State tracker for computing deltas per aircraft
#[derive(Debug, Clone)]
pub struct AircraftState {
    pub timestamp_ms: u64,
    pub lat: i32,
    pub lon: i32,
    pub alt_baro: i16,
    pub alt_geom: i16,
    pub gs: u16,
    pub track: u16,
    pub vrate: i16,
    pub squawk: u16,
    pub on_ground: bool,
}

/// Delta encoder - maintains state per aircraft
pub struct DeltaEncoder {
    states: HashMap<u32, AircraftState>,
    keyframe_interval: u32, // Force keyframe every N records
    record_counts: HashMap<u32, u32>,
}

impl DeltaEncoder {
    pub fn new(keyframe_interval: u32) -> Self {
        Self {
            states: HashMap::new(),
            keyframe_interval,
            record_counts: HashMap::new(),
        }
    }

    /// Determine if we need a keyframe for this aircraft
    pub fn needs_keyframe(&self, icao: u32, timestamp_ms: u64) -> bool {
        match self.states.get(&icao) {
            None => true, // First sighting
            Some(state) => {
                // Keyframe if gap > 60 seconds
                if timestamp_ms.saturating_sub(state.timestamp_ms) > 60_000 {
                    return true;
                }
                // Keyframe every N records
                let count = self.record_counts.get(&icao).copied().unwrap_or(0);
                count >= self.keyframe_interval
            }
        }
    }

    /// Update state after emitting a record
    pub fn update_state(&mut self, icao: u32, state: AircraftState, is_keyframe: bool) {
        self.states.insert(icao, state);
        if is_keyframe {
            self.record_counts.insert(icao, 0);
        } else {
            *self.record_counts.entry(icao).or_insert(0) += 1;
        }
    }

    /// Get previous state for computing delta
    pub fn get_state(&self, icao: u32) -> Option<&AircraftState> {
        self.states.get(&icao)
    }

    /// Check if position delta fits in i16 range
    pub fn can_encode_position_delta(&self, icao: u32, new_lat: i32, new_lon: i32) -> bool {
        if let Some(state) = self.states.get(&icao) {
            let lat_delta = new_lat - state.lat;
            let lon_delta = new_lon - state.lon;
            PositionDelta::new(lat_delta, lon_delta).is_some()
        } else {
            false
        }
    }
}

/// Statistics for delta compression
#[derive(Debug, Default)]
pub struct DeltaStats {
    pub keyframes: u64,
    pub deltas: u64,
    pub total_bytes: u64,
}

impl DeltaStats {
    pub fn avg_bytes_per_record(&self) -> f64 {
        let total = self.keyframes + self.deltas;
        if total == 0 { 0.0 } else { self.total_bytes as f64 / total as f64 }
    }

    pub fn compression_ratio(&self) -> f64 {
        let total = self.keyframes + self.deltas;
        if total == 0 { 0.0 } else { 112.0 / self.avg_bytes_per_record() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyframe_size() {
        assert_eq!(Keyframe::SIZE, 36);
        println!("Keyframe size: {} bytes", Keyframe::SIZE);
    }

    #[test]
    fn test_delta_header_size() {
        assert_eq!(DeltaHeader::SIZE, 8);
    }

    #[test]
    fn test_position_delta() {
        // ~100m movement
        let delta = PositionDelta::new(1000, -500).unwrap();
        let (new_lat, new_lon) = delta.apply(51_500_000, -100_000);

        // Should be within ~10m of expected
        assert!((new_lat - 51_501_000).abs() < 100);
        assert!((new_lon - (-100_500)).abs() < 100);
    }

    #[test]
    fn test_position_delta_overflow() {
        // ~50km movement - too large for i16
        let delta = PositionDelta::new(500_000, 500_000);
        assert!(delta.is_none());
    }

    #[test]
    fn test_velocity_delta() {
        let vd = VelocityDelta::new(5, -10, 200).unwrap();
        assert_eq!(vd.gs_delta(), 5);
        assert_eq!(vd.track_delta(), -10);
        assert!((vd.vrate_delta() - 192).abs() <= 16); // Within resolution
    }

    #[test]
    fn test_velocity_delta_overflow() {
        // GS change too large
        let vd = VelocityDelta::new(50, 0, 0);
        assert!(vd.is_none());
    }

    #[test]
    fn test_field_mask_size() {
        let mut mask = DeltaFieldMask::default();
        assert_eq!(mask.optional_size(), 0);

        mask.set(DeltaFieldMask::POSITION);
        assert_eq!(mask.optional_size(), 4);

        mask.set(DeltaFieldMask::ALT_BARO);
        mask.set(DeltaFieldMask::VELOCITY);
        assert_eq!(mask.optional_size(), 4 + 1 + 3);
    }

    #[test]
    fn test_typical_delta_size() {
        // Typical case: position + altitude + velocity changes
        let mut mask = DeltaFieldMask::default();
        mask.set(DeltaFieldMask::POSITION);
        mask.set(DeltaFieldMask::ALT_BARO);
        mask.set(DeltaFieldMask::VELOCITY);

        let total = DeltaHeader::SIZE + mask.optional_size();
        assert_eq!(total, 16); // 8 + 4 + 1 + 3 = 16 bytes

        println!("Typical delta record: {} bytes", total);
        println!("vs Keyframe: {} bytes", Keyframe::SIZE);
        println!("vs CompactRecord: 48 bytes");
        println!("vs Original: 112 bytes");
    }
}
