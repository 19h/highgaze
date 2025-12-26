//! Binary protocol parser for binCraft format.

use crate::types::{AircraftRecord, ResponseHeader, ValidityFlags};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Buffer too small: need {need} bytes, got {got}")]
    BufferTooSmall { need: usize, got: usize },
    #[error("Invalid stride: {0}")]
    InvalidStride(u32),
    #[error("Zstd decompression failed: {0}")]
    DecompressError(#[from] std::io::Error),
    #[error("Base64 decode failed: {0}")]
    Base64Error(#[from] base64::DecodeError),
    #[error("Server returned error: {0}")]
    ServerResponse(String),
}

/// Minimum supported stride (older format)
const MIN_STRIDE: u32 = 112;
/// Maximum supported stride (newer format with rId)
const MAX_STRIDE: u32 = 120;
/// Version threshold for new seen_pos location
const VERSION_NEW_SEEN: u32 = 20240218;
/// Version threshold for new RSSI calculation
const VERSION_NEW_RSSI: u32 = 20250403;

/// Zstd magic bytes
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Parse a binCraft response (may be base64 encoded, zstd compressed, or raw).
pub fn parse_response(data: &[u8]) -> Result<(ResponseHeader, Vec<AircraftRecord>), ParseError> {
    // Check if response looks like an error (HTML/JSON text)
    if data.starts_with(b"<!") || data.starts_with(b"<html") || data.starts_with(b"{") {
        let text = String::from_utf8_lossy(&data[..data.len().min(500)]);
        return Err(ParseError::ServerResponse(text.to_string()));
    }

    // Check if data starts with zstd magic (raw compressed)
    if data.len() >= 4 && data[0..4] == ZSTD_MAGIC {
        let decompressed = zstd::decode_all(data)?;
        return parse_binary(&decompressed);
    }

    // Try base64 decode (for saved files)
    let trimmed: Vec<u8> = data
        .iter()
        .copied()
        .filter(|&b| !b.is_ascii_whitespace())
        .collect();

    use base64::Engine;
    if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(&trimmed) {
        // Check if decoded is zstd compressed
        if decoded.len() >= 4 && decoded[0..4] == ZSTD_MAGIC {
            let decompressed = zstd::decode_all(decoded.as_slice())?;
            return parse_binary(&decompressed);
        }
        // Maybe it's raw binary after base64
        return parse_binary(&decoded);
    }

    // Last resort: try as raw binary
    parse_binary(data)
}

/// Parse raw binary binCraft data.
pub fn parse_binary(data: &[u8]) -> Result<(ResponseHeader, Vec<AircraftRecord>), ParseError> {
    if data.len() < 52 {
        return Err(ParseError::BufferTooSmall {
            need: 52,
            got: data.len(),
        });
    }

    // Parse header
    let header = parse_header(data)?;

    if header.stride < MIN_STRIDE || header.stride > MAX_STRIDE {
        return Err(ParseError::InvalidStride(header.stride));
    }

    let stride = header.stride as usize;

    // Parse aircraft records
    let mut records = Vec::with_capacity((data.len() - stride) / stride);
    let mut offset = stride; // First record starts after header

    while offset + stride <= data.len() {
        let record = parse_aircraft_record(&data[offset..offset + stride], &header);
        records.push(record);
        offset += stride;
    }

    Ok((header, records))
}

fn parse_header(data: &[u8]) -> Result<ResponseHeader, ParseError> {
    let u32_view = |off: usize| -> u32 {
        u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]])
    };
    let i32_view = |off: usize| -> i32 {
        i32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]])
    };
    let i16_view = |off: usize| -> i16 { i16::from_le_bytes([data[off], data[off + 1]]) };

    // Timestamp: u32[0] + u32[1] * 2^32, in milliseconds
    let ts_low = u32_view(0) as u64;
    let ts_high = u32_view(4) as u64;
    let timestamp_ms = ts_low + ts_high * 4_294_967_296;

    let stride = u32_view(8);
    let aircraft_count_with_pos = u32_view(12);
    let globe_index = u32_view(16);

    // Bounds at offset 20 (4 x i16)
    let south = i16_view(20);
    let west = i16_view(22);
    let north = i16_view(24);
    let east = i16_view(26);

    let message_count = u32_view(28);

    // Receiver position at offset 32
    let receiver_lat = i32_view(32) as f64 / 1_000_000.0;
    let receiver_lon = i32_view(36) as f64 / 1_000_000.0;

    let version = u32_view(40);
    let message_rate = u32_view(44) as f32 / 10.0;
    let use_message_rate = (u32_view(48) & 1) != 0;

    Ok(ResponseHeader {
        timestamp_ms,
        stride,
        aircraft_count_with_pos,
        globe_index,
        south,
        west,
        north,
        east,
        message_count,
        receiver_lat,
        receiver_lon,
        version,
        message_rate,
        use_message_rate,
    })
}

fn parse_aircraft_record(data: &[u8], header: &ResponseHeader) -> AircraftRecord {
    // Helper closures for reading different types
    let u8_at = |off: usize| -> u8 { data[off] };
    let u16_at = |off: usize| -> u16 { u16::from_le_bytes([data[off], data[off + 1]]) };
    let i16_at = |off: usize| -> i16 { i16::from_le_bytes([data[off], data[off + 1]]) };
    let _u32_at = |off: usize| -> u32 {
        u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]])
    };
    let i32_at = |off: usize| -> i32 {
        i32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]])
    };

    // ICAO address (lower 24 bits) + temp flag (bit 24)
    let icao_raw = i32_at(0);
    let icao = if icao_raw & (1 << 24) != 0 {
        (icao_raw & 0x00FF_FFFF) as u32 | (1 << 24)
    } else {
        (icao_raw & 0x00FF_FFFF) as u32
    };

    // Seen times depend on version
    let _seen_pos = if header.version >= VERSION_NEW_SEEN {
        i32_at(108) as f32 / 10.0
    } else {
        u16_at(4) as f32 / 10.0
    };

    // Position (microdegrees from raw / 1e6)
    let lon_raw = i32_at(8);
    let lat_raw = i32_at(12);

    // Rates
    let baro_rate_raw = i16_at(16);
    let geom_rate_raw = i16_at(18);

    // Altitudes
    let alt_baro_raw = i16_at(20);
    let alt_geom_raw = i16_at(22);

    // Nav altitudes
    let nav_alt_mcp_raw = u16_at(24);
    let nav_alt_fms_raw = u16_at(26);

    // Nav QNH and heading
    let nav_qnh_raw = i16_at(28);
    let nav_heading_raw = i16_at(30);

    // Squawk
    let squawk = u16_at(32);

    // Speed and motion
    let gs_raw = i16_at(34);
    let mach_raw = i16_at(36);
    let roll_raw = i16_at(38);
    let track_raw = i16_at(40);
    let track_rate_raw = i16_at(42);
    let mag_heading_raw = i16_at(44);
    let true_heading_raw = i16_at(46);

    // Wind
    let wind_dir = i16_at(48);
    let wind_speed = i16_at(50);

    // Temperature
    let oat = i16_at(52);
    let tat = i16_at(54);

    // Airspeed
    let tas = u16_at(56);
    let ias = u16_at(58);

    // RC and messages
    let _rc = u16_at(60);
    let _messages = u16_at(62);

    // Category and NIC
    let category = u8_at(64);
    let nic = u8_at(65);

    // Nav modes
    let nav_modes = u8_at(66);

    // Emergency and type
    let emergency = u8_at(67) & 0x0F;
    let data_source = (u8_at(67) & 0xF0) >> 4;

    // Air/ground and nav altitude source
    let air_ground = u8_at(68) & 0x0F;
    let _nav_altitude_src = (u8_at(68) & 0xF0) >> 4;

    // Versions
    let _sil_type = u8_at(69) & 0x0F;
    let adsb_version = (u8_at(69) & 0xF0) >> 4;
    let adsr_version = u8_at(70) & 0x0F;
    let tisb_version = (u8_at(70) & 0xF0) >> 4;

    // NAC
    let nac_p = u8_at(71) & 0x0F;
    let nac_v = (u8_at(71) & 0xF0) >> 4;

    // Quality indicators
    let sil = u8_at(72) & 0x03;
    let gva = (u8_at(72) & 0x0C) >> 2;
    let sda = (u8_at(72) & 0x30) >> 4;
    let nic_a = (u8_at(72) & 0x40) >> 6;
    let nic_c = (u8_at(72) & 0x80) >> 7;

    // Validity flags (bytes 73-77)
    let validity = ValidityFlags {
        flags: [u8_at(73), u8_at(74), u8_at(75), u8_at(76), u8_at(77)],
    };

    // Callsign (bytes 78-85)
    let mut callsign = [0u8; 8];
    for (i, b) in callsign.iter_mut().enumerate() {
        let c = u8_at(78 + i);
        if c == 0 {
            break;
        }
        *b = c;
    }

    // DB flags
    let db_flags = u16_at(86);

    // Aircraft type (bytes 88-91)
    let mut aircraft_type = [0u8; 4];
    for (i, b) in aircraft_type.iter_mut().enumerate() {
        let c = u8_at(88 + i);
        if c == 0 {
            break;
        }
        *b = c;
    }

    // Registration (bytes 92-103)
    let mut registration = [0u8; 12];
    for (i, b) in registration.iter_mut().enumerate() {
        let c = u8_at(92 + i);
        if c == 0 {
            break;
        }
        *b = c;
    }

    // Receiver count and RSSI
    let receiver_count = u8_at(104);
    let rssi_raw = u8_at(105);
    let rssi = if header.version >= VERSION_NEW_RSSI {
        // Direct scaling: 0-255 -> -50 to 0 dBm
        ((rssi_raw as f32 * 50.0 / 255.0) - 50.0).round() as u8
    } else {
        // Legacy log scaling
        let level = (rssi_raw as f32).powi(2) / 65025.0 + 0.00001125;
        (10.0 * level.log10()).round() as u8
    };

    // Pack versions
    let versions = (adsb_version << 4) | (adsr_version & 0x03) | ((tisb_version & 0x03) << 2);

    // Pack quality
    let quality =
        (sil & 0x03) | ((gva & 0x03) << 2) | ((sda & 0x03) << 4) | (nic_a << 6) | (nic_c << 7);

    // Pack NAC
    let nac = (nac_p & 0x0F) | ((nac_v & 0x0F) << 4);

    AircraftRecord {
        timestamp_ms: header.timestamp_ms,
        icao,
        lat_microdeg: lat_raw,
        lon_microdeg: lon_raw,
        alt_baro_raw,
        alt_geom_raw,
        gs_raw,
        tas,
        ias,
        mach_raw,
        track_raw,
        mag_heading_raw,
        true_heading_raw,
        baro_rate_raw,
        geom_rate_raw,
        roll_raw,
        track_rate_raw,
        squawk,
        nav_alt_mcp_raw,
        nav_alt_fms_raw,
        nav_qnh_raw,
        nav_heading_raw,
        wind_dir,
        wind_speed,
        oat,
        tat,
        category,
        nic,
        nav_modes,
        data_source,
        air_ground,
        emergency,
        rssi,
        receiver_count,
        validity,
        versions,
        quality,
        nac,
        db_flags,
        callsign,
        aircraft_type,
        registration,
        _padding: [0; 6],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_header() {
        // Minimal test with synthetic data
        let mut data = vec![0u8; 116];

        // Set timestamp
        data[0..4].copy_from_slice(&1000u32.to_le_bytes());
        data[4..8].copy_from_slice(&0u32.to_le_bytes());

        // Set stride
        data[8..12].copy_from_slice(&112u32.to_le_bytes());

        // Set version
        data[40..44].copy_from_slice(&VERSION_NEW_SEEN.to_le_bytes());

        let header = parse_header(&data).unwrap();
        assert_eq!(header.stride, 112);
        assert_eq!(header.version, VERSION_NEW_SEEN);
    }
}
