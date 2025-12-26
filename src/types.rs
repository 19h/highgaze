//! Core data types for ADS-B aircraft tracking.

use bytemuck::{Pod, Zeroable};
use std::fmt;

/// ICAO 24-bit aircraft address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Pod, Zeroable)]
#[repr(C)]
pub struct IcaoAddress(pub u32);

impl IcaoAddress {
    pub const fn new(addr: u32) -> Self {
        Self(addr & 0x00FF_FFFF)
    }

    pub const fn is_temp(&self) -> bool {
        self.0 & (1 << 24) != 0
    }

    pub const fn raw(&self) -> u32 {
        self.0 & 0x00FF_FFFF
    }
}

impl fmt::Display for IcaoAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_temp() {
            write!(f, "~{:06x}", self.raw())
        } else {
            write!(f, "{:06x}", self.raw())
        }
    }
}

/// Aircraft data source type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DataSource {
    AdsbIcao = 0,
    AdsbIcaoNt = 1,
    AdsrIcao = 2,
    TisbIcao = 3,
    Adsc = 4,
    Mlat = 5,
    Other = 6,
    ModeS = 7,
    AdsbOther = 8,
    AdsrOther = 9,
    TisbTrackfile = 10,
    TisbOther = 11,
    ModeAc = 12,
    Unknown = 15,
}

impl From<u8> for DataSource {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::AdsbIcao,
            1 => Self::AdsbIcaoNt,
            2 => Self::AdsrIcao,
            3 => Self::TisbIcao,
            4 => Self::Adsc,
            5 => Self::Mlat,
            6 => Self::Other,
            7 => Self::ModeS,
            8 => Self::AdsbOther,
            9 => Self::AdsrOther,
            10 => Self::TisbTrackfile,
            11 => Self::TisbOther,
            12 => Self::ModeAc,
            _ => Self::Unknown,
        }
    }
}

/// Air/Ground state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AirGround {
    Unknown = 0,
    Ground = 1,
    Airborne = 2,
    Invalid = 3,
}

impl From<u8> for AirGround {
    fn from(v: u8) -> Self {
        match v & 0x0F {
            0 => Self::Unknown,
            1 => Self::Ground,
            2 => Self::Airborne,
            _ => Self::Invalid,
        }
    }
}

/// Navigation modes bitmask.
#[derive(Debug, Clone, Copy, Default)]
pub struct NavModes(pub u8);

impl NavModes {
    pub const fn autopilot(&self) -> bool {
        self.0 & 0x01 != 0
    }
    pub const fn vnav(&self) -> bool {
        self.0 & 0x02 != 0
    }
    pub const fn alt_hold(&self) -> bool {
        self.0 & 0x04 != 0
    }
    pub const fn approach(&self) -> bool {
        self.0 & 0x08 != 0
    }
    pub const fn lnav(&self) -> bool {
        self.0 & 0x10 != 0
    }
    pub const fn tcas(&self) -> bool {
        self.0 & 0x20 != 0
    }
}

/// Validity flags for aircraft fields (packed into 5 bytes).
#[derive(Debug, Clone, Copy, Default, Pod, Zeroable)]
#[repr(C)]
pub struct ValidityFlags {
    pub flags: [u8; 5],
}

impl ValidityFlags {
    // Byte 0 (u8[73] equivalent)
    pub const fn has_nic_baro(&self) -> bool {
        self.flags[0] & 0x01 != 0
    }
    pub const fn has_alert(&self) -> bool {
        self.flags[0] & 0x02 != 0
    }
    pub const fn has_spi(&self) -> bool {
        self.flags[0] & 0x04 != 0
    }
    pub const fn has_flight(&self) -> bool {
        self.flags[0] & 0x08 != 0
    }
    pub const fn has_alt_baro(&self) -> bool {
        self.flags[0] & 0x10 != 0
    }
    pub const fn has_alt_geom(&self) -> bool {
        self.flags[0] & 0x20 != 0
    }
    pub const fn has_position(&self) -> bool {
        self.flags[0] & 0x40 != 0
    }
    pub const fn has_gs(&self) -> bool {
        self.flags[0] & 0x80 != 0
    }

    // Byte 1 (u8[74] equivalent)
    pub const fn has_ias(&self) -> bool {
        self.flags[1] & 0x01 != 0
    }
    pub const fn has_tas(&self) -> bool {
        self.flags[1] & 0x02 != 0
    }
    pub const fn has_mach(&self) -> bool {
        self.flags[1] & 0x04 != 0
    }
    pub const fn has_track(&self) -> bool {
        self.flags[1] & 0x08 != 0
    }
    pub const fn has_track_rate(&self) -> bool {
        self.flags[1] & 0x10 != 0
    }
    pub const fn has_roll(&self) -> bool {
        self.flags[1] & 0x20 != 0
    }
    pub const fn has_mag_heading(&self) -> bool {
        self.flags[1] & 0x40 != 0
    }
    pub const fn has_true_heading(&self) -> bool {
        self.flags[1] & 0x80 != 0
    }

    // Byte 2 (u8[75] equivalent)
    pub const fn has_baro_rate(&self) -> bool {
        self.flags[2] & 0x01 != 0
    }
    pub const fn has_geom_rate(&self) -> bool {
        self.flags[2] & 0x02 != 0
    }
    pub const fn has_nic_a(&self) -> bool {
        self.flags[2] & 0x04 != 0
    }
    pub const fn has_nic_c(&self) -> bool {
        self.flags[2] & 0x08 != 0
    }
    pub const fn has_nac_p(&self) -> bool {
        self.flags[2] & 0x20 != 0
    }
    pub const fn has_nac_v(&self) -> bool {
        self.flags[2] & 0x40 != 0
    }
    pub const fn has_sil(&self) -> bool {
        self.flags[2] & 0x80 != 0
    }

    // Byte 3 (u8[76] equivalent)
    pub const fn has_gva(&self) -> bool {
        self.flags[3] & 0x01 != 0
    }
    pub const fn has_sda(&self) -> bool {
        self.flags[3] & 0x02 != 0
    }
    pub const fn has_squawk(&self) -> bool {
        self.flags[3] & 0x04 != 0
    }
    pub const fn has_emergency(&self) -> bool {
        self.flags[3] & 0x08 != 0
    }
    pub const fn has_nav_qnh(&self) -> bool {
        self.flags[3] & 0x20 != 0
    }
    pub const fn has_nav_altitude_mcp(&self) -> bool {
        self.flags[3] & 0x40 != 0
    }
    pub const fn has_nav_altitude_fms(&self) -> bool {
        self.flags[3] & 0x80 != 0
    }

    // Byte 4 (u8[77] equivalent)
    pub const fn has_nav_altitude_src(&self) -> bool {
        self.flags[4] & 0x01 != 0
    }
    pub const fn has_nav_heading(&self) -> bool {
        self.flags[4] & 0x02 != 0
    }
    pub const fn has_nav_modes(&self) -> bool {
        self.flags[4] & 0x04 != 0
    }
    pub const fn has_wind(&self) -> bool {
        self.flags[4] & 0x10 != 0
    }
    pub const fn has_temp(&self) -> bool {
        self.flags[4] & 0x20 != 0
    }
}

/// Compact aircraft state record for storage.
/// Designed for optimal cache locality and minimal memory footprint.
/// Total size: 104 bytes (carefully aligned to avoid padding)
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct AircraftRecord {
    // === 8-byte aligned section (offset 0) ===
    /// Timestamp in milliseconds since epoch
    pub timestamp_ms: u64,              // 8 bytes, offset 0

    // === 4-byte aligned section (offset 8) ===
    /// ICAO address (24-bit + temp flag)
    pub icao: u32,                      // 4 bytes, offset 8
    /// Latitude in microdegrees (i32 gives ~11cm precision)
    pub lat_microdeg: i32,              // 4 bytes, offset 12
    /// Longitude in microdegrees
    pub lon_microdeg: i32,              // 4 bytes, offset 16

    // === 2-byte aligned section (offset 20) ===
    /// Database flags
    pub db_flags: u16,                  // 2 bytes, offset 20
    /// Barometric altitude in feet (i16 * 25)
    pub alt_baro_raw: i16,              // 2 bytes, offset 22
    /// Geometric altitude in feet (i16 * 25)
    pub alt_geom_raw: i16,              // 2 bytes, offset 24
    /// Ground speed in knots * 10
    pub gs_raw: i16,                    // 2 bytes, offset 26
    /// True airspeed in knots
    pub tas: u16,                       // 2 bytes, offset 28
    /// Indicated airspeed in knots
    pub ias: u16,                       // 2 bytes, offset 30
    /// Mach * 1000
    pub mach_raw: i16,                  // 2 bytes, offset 32
    /// Track angle * 90 (gives 0.01 degree precision)
    pub track_raw: i16,                 // 2 bytes, offset 34
    /// Magnetic heading * 90
    pub mag_heading_raw: i16,           // 2 bytes, offset 36
    /// True heading * 90
    pub true_heading_raw: i16,          // 2 bytes, offset 38
    /// Barometric vertical rate in ft/min (raw * 8)
    pub baro_rate_raw: i16,             // 2 bytes, offset 40
    /// Geometric vertical rate in ft/min (raw * 8)
    pub geom_rate_raw: i16,             // 2 bytes, offset 42
    /// Roll angle * 100
    pub roll_raw: i16,                  // 2 bytes, offset 44
    /// Track rate * 100
    pub track_rate_raw: i16,            // 2 bytes, offset 46
    /// Squawk code (packed hex)
    pub squawk: u16,                    // 2 bytes, offset 48
    /// Nav altitude MCP * 4
    pub nav_alt_mcp_raw: u16,           // 2 bytes, offset 50
    /// Nav altitude FMS * 4
    pub nav_alt_fms_raw: u16,           // 2 bytes, offset 52
    /// Nav QNH * 10
    pub nav_qnh_raw: i16,               // 2 bytes, offset 54
    /// Nav heading * 90
    pub nav_heading_raw: i16,           // 2 bytes, offset 56
    /// Wind direction
    pub wind_dir: i16,                  // 2 bytes, offset 58
    /// Wind speed
    pub wind_speed: i16,                // 2 bytes, offset 60
    /// Outside air temperature
    pub oat: i16,                       // 2 bytes, offset 62
    /// Total air temperature
    pub tat: i16,                       // 2 bytes, offset 64

    // === 1-byte aligned section (offset 66) ===
    /// Category
    pub category: u8,                   // 1 byte, offset 66
    /// NIC
    pub nic: u8,                        // 1 byte, offset 67
    /// Nav modes
    pub nav_modes: u8,                  // 1 byte, offset 68
    /// Data source type
    pub data_source: u8,                // 1 byte, offset 69
    /// Air/ground state
    pub air_ground: u8,                 // 1 byte, offset 70
    /// Emergency code
    pub emergency: u8,                  // 1 byte, offset 71
    /// RSSI (scaled)
    pub rssi: u8,                       // 1 byte, offset 72
    /// Receiver count
    pub receiver_count: u8,             // 1 byte, offset 73
    /// Validity flags (5 bytes)
    pub validity: ValidityFlags,        // 5 bytes, offset 74
    /// Version info packed
    pub versions: u8,                   // 1 byte, offset 79
    /// Quality indicators packed: sil(2) | gva(2) | sda(2) | nic_a(1) | nic_c(1)
    pub quality: u8,                    // 1 byte, offset 80
    /// NIC/NAC packed: nac_p(4) | nac_v(4)
    pub nac: u8,                        // 1 byte, offset 81

    // === Byte arrays (offset 82) ===
    /// Callsign (8 bytes, null-padded)
    pub callsign: [u8; 8],              // 8 bytes, offset 82
    /// Aircraft type (4 bytes, null-padded)
    pub aircraft_type: [u8; 4],         // 4 bytes, offset 90
    /// Registration (12 bytes, null-padded)
    pub registration: [u8; 12],         // 12 bytes, offset 94
    /// Padding to align struct to 8 bytes (total 112 bytes)
    pub _padding: [u8; 6],              // 6 bytes, offset 106 -> total 112 (divisible by 8)
}

impl AircraftRecord {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn icao_address(&self) -> IcaoAddress {
        IcaoAddress(self.icao)
    }

    pub fn latitude(&self) -> Option<f64> {
        if self.validity.has_position() && self.lat_microdeg != i32::MIN {
            Some(self.lat_microdeg as f64 / 1_000_000.0)
        } else {
            None
        }
    }

    pub fn longitude(&self) -> Option<f64> {
        if self.validity.has_position() && self.lon_microdeg != i32::MIN {
            Some(self.lon_microdeg as f64 / 1_000_000.0)
        } else {
            None
        }
    }

    pub fn alt_baro(&self) -> Option<i32> {
        if self.validity.has_alt_baro() {
            if self.air_ground == AirGround::Ground as u8 {
                Some(0) // On ground
            } else {
                Some(self.alt_baro_raw as i32 * 25)
            }
        } else {
            None
        }
    }

    pub fn alt_geom(&self) -> Option<i32> {
        if self.validity.has_alt_geom() {
            Some(self.alt_geom_raw as i32 * 25)
        } else {
            None
        }
    }

    pub fn ground_speed(&self) -> Option<f32> {
        if self.validity.has_gs() {
            Some(self.gs_raw as f32 / 10.0)
        } else {
            None
        }
    }

    pub fn track(&self) -> Option<f32> {
        if self.validity.has_track() {
            Some(self.track_raw as f32 / 90.0)
        } else {
            None
        }
    }

    pub fn callsign_str(&self) -> Option<&str> {
        if self.validity.has_flight() {
            let end = self.callsign.iter().position(|&b| b == 0).unwrap_or(8);
            std::str::from_utf8(&self.callsign[..end]).ok()
        } else {
            None
        }
    }

    pub fn registration_str(&self) -> Option<&str> {
        let end = self
            .registration
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(12);
        if end > 0 {
            std::str::from_utf8(&self.registration[..end]).ok()
        } else {
            None
        }
    }

    pub fn aircraft_type_str(&self) -> Option<&str> {
        let end = self
            .aircraft_type
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(4);
        if end > 0 {
            std::str::from_utf8(&self.aircraft_type[..end]).ok()
        } else {
            None
        }
    }
}

/// Response header from binCraft endpoint.
#[derive(Debug, Clone, Copy)]
pub struct ResponseHeader {
    pub timestamp_ms: u64,
    pub stride: u32,
    pub aircraft_count_with_pos: u32,
    pub globe_index: u32,
    pub south: i16,
    pub west: i16,
    pub north: i16,
    pub east: i16,
    pub message_count: u32,
    pub receiver_lat: f64,
    pub receiver_lon: f64,
    pub version: u32,
    pub message_rate: f32,
    pub use_message_rate: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_size() {
        // Ensure record is a reasonable size for cache efficiency
        assert!(AircraftRecord::SIZE <= 128, "Record too large: {}", AircraftRecord::SIZE);
        println!("AircraftRecord size: {} bytes", AircraftRecord::SIZE);
    }

    #[test]
    fn test_icao_address() {
        let addr = IcaoAddress::new(0x740828);
        assert_eq!(addr.raw(), 0x740828);
        assert!(!addr.is_temp());
        assert_eq!(format!("{}", addr), "740828");

        let temp = IcaoAddress(0x01740828);
        assert!(temp.is_temp());
        assert_eq!(format!("{}", temp), "~740828");
    }
}
