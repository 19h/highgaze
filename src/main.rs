//! ADS-B Data Collector CLI
//!
//! Continuously fetches aircraft data from ADS-B Exchange and stores it locally.

use highgaze::{
    client::{AdsbClient, BoundingBox, ClientConfig},
    collector::{Collector, CollectorConfig},
    delta_collector::{DeltaCollector, DeltaCollectorConfig},
    delta_storage::DeltaStorage,
    protocol,
    storage::Storage,
    types::IcaoAddress,
};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Parser)]
#[command(name = "highgaze")]
#[command(about = "High-performance ADS-B data collector", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Path to data storage
    #[arg(short, long, default_value = "adsb_data")]
    data_path: PathBuf,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Start collecting data continuously
    Collect {
        /// Session ID cookie (adsbx_sid)
        #[arg(long, env = "ADSB_SESSION_ID")]
        session_id: String,

        /// API key cookie (adsbx_api)
        #[arg(long, env = "ADSB_API_KEY")]
        api_key: String,

        /// Fetch interval in seconds
        #[arg(short, long, default_value = "10")]
        interval: u64,

        /// Optional ICAO hex to specifically track
        #[arg(long)]
        track_hex: Option<String>,

        /// South bound of bounding box
        #[arg(long, default_value = "-90")]
        south: f64,

        /// North bound of bounding box
        #[arg(long, default_value = "90")]
        north: f64,

        /// West bound of bounding box
        #[arg(long, default_value = "-180")]
        west: f64,

        /// East bound of bounding box
        #[arg(long, default_value = "180")]
        east: f64,

        /// Use delta compression (saves ~85% storage)
        #[arg(long)]
        delta: bool,
    },

    /// Query stored data
    Query {
        /// ICAO address to look up (hex format, e.g., "740828")
        #[arg(short, long)]
        icao: String,

        /// Show only the last N records
        #[arg(short, long)]
        limit: Option<usize>,
    },

    /// Show storage statistics
    Stats {
        /// Show delta storage stats instead of raw storage
        #[arg(long)]
        delta: bool,
    },

    /// Parse a single binary file (for testing)
    Parse {
        /// Path to binary file (base64 + zstd encoded)
        file: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Initialize logging
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&cli.log_level));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    match cli.command {
        Commands::Collect {
            session_id,
            api_key,
            interval,
            track_hex,
            south,
            north,
            west,
            east,
            delta,
        } => {
            if delta {
                run_delta_collector(
                    &cli.data_path,
                    session_id,
                    api_key,
                    interval,
                    track_hex,
                    BoundingBox::new(south, north, west, east),
                )
                .await?;
            } else {
                run_collector(
                    &cli.data_path,
                    session_id,
                    api_key,
                    interval,
                    track_hex,
                    BoundingBox::new(south, north, west, east),
                )
                .await?;
            }
        }

        Commands::Query { icao, limit } => {
            query_icao(&cli.data_path, &icao, limit)?;
        }

        Commands::Stats { delta } => {
            if delta {
                show_delta_stats(&cli.data_path)?;
            } else {
                show_stats(&cli.data_path)?;
            }
        }

        Commands::Parse { file } => {
            parse_file(&file)?;
        }
    }

    Ok(())
}

async fn run_collector(
    data_path: &PathBuf,
    session_id: String,
    api_key: String,
    interval: u64,
    track_hex: Option<String>,
    bbox: BoundingBox,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Starting ADS-B collector");
    tracing::info!("Data path: {}", data_path.display());
    tracing::info!("Fetch interval: {}s", interval);
    tracing::info!(
        "Bounding box: S={}, N={}, W={}, E={}",
        bbox.south,
        bbox.north,
        bbox.west,
        bbox.east
    );

    let mut client_config = ClientConfig::new(session_id, api_key)
        .with_timeout(Duration::from_secs(30));

    if let Some(hex) = track_hex {
        tracing::info!("Tracking specific ICAO: {}", hex);
        client_config = client_config.with_find_hex(hex);
    }

    let client = AdsbClient::new(client_config)?;
    let storage = Storage::open(data_path)?;

    let collector_config = CollectorConfig {
        fetch_interval: Duration::from_secs(interval),
        bbox,
        sync_interval: Duration::from_secs(30),
        ..Default::default()
    };

    let collector = Collector::new(client, storage, collector_config);
    let stats = collector.stats();

    // Spawn stats reporting task
    let stats_handle = {
        let stats = stats.clone();
        let storage_path = data_path.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                let s = stats.snapshot();
                let unique = Storage::open(&storage_path)
                    .map(|s| s.stats().aircraft_count)
                    .unwrap_or(0);
                let bytes_written = s.records_written * 112; // AircraftRecord::SIZE
                tracing::info!(
                    "Stats: fetches={}, unique={}, received={}, written={}, errors={}, net={}MB, disk={}MB",
                    s.fetches,
                    unique,
                    s.records_received,
                    s.records_written,
                    s.errors,
                    s.bytes_received / (1024 * 1024),
                    bytes_written / (1024 * 1024)
                );
            }
        })
    };

    // Handle Ctrl+C
    let collector_handle = tokio::spawn(async move {
        collector.run().await
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down...");
        }
        result = collector_handle => {
            if let Err(e) = result {
                tracing::error!("Collector error: {}", e);
            }
        }
    }

    stats_handle.abort();

    let final_stats = stats.snapshot();
    let unique = Storage::open(data_path)
        .map(|s| s.stats().aircraft_count)
        .unwrap_or(0);
    let bytes_written = final_stats.records_written * 112;
    tracing::info!("Final statistics:");
    tracing::info!("  Total fetches: {}", final_stats.fetches);
    tracing::info!("  Unique aircraft: {}", unique);
    tracing::info!("  Total records received: {}", final_stats.records_received);
    tracing::info!("  Total records written: {}", final_stats.records_written);
    tracing::info!("  Total errors: {}", final_stats.errors);
    tracing::info!(
        "  Network received: {} MB",
        final_stats.bytes_received / (1024 * 1024)
    );
    tracing::info!(
        "  Disk written: {} MB",
        bytes_written / (1024 * 1024)
    );

    Ok(())
}

async fn run_delta_collector(
    data_path: &PathBuf,
    session_id: String,
    api_key: String,
    interval: u64,
    track_hex: Option<String>,
    bbox: BoundingBox,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Starting ADS-B collector with DELTA COMPRESSION");
    tracing::info!("Data path: {}", data_path.display());
    tracing::info!("Fetch interval: {}s", interval);
    tracing::info!(
        "Bounding box: S={}, N={}, W={}, E={}",
        bbox.south,
        bbox.north,
        bbox.west,
        bbox.east
    );

    let mut client_config = ClientConfig::new(session_id, api_key)
        .with_timeout(Duration::from_secs(30));

    if let Some(hex) = track_hex {
        tracing::info!("Tracking specific ICAO: {}", hex);
        client_config = client_config.with_find_hex(hex);
    }

    let client = AdsbClient::new(client_config)?;
    let storage = DeltaStorage::open(data_path)?;

    let collector_config = DeltaCollectorConfig {
        fetch_interval: Duration::from_secs(interval),
        bbox,
        sync_interval: Duration::from_secs(30),
        ..Default::default()
    };

    let collector = DeltaCollector::new(client, storage, collector_config);
    let stats = collector.stats();

    // Spawn stats reporting task
    let stats_handle = {
        let stats = stats.clone();
        let storage_path = data_path.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                let s = stats.snapshot();
                let storage_stats = DeltaStorage::open(&storage_path)
                    .map(|s| s.stats())
                    .ok();

                if let Some(ss) = storage_stats {
                    tracing::info!(
                        "Stats: fetches={}, aircraft={}, written={}, errors={}, \
                         net={}MB, disk={:.1}MB, ratio={:.1}x",
                        s.fetches,
                        ss.aircraft_count,
                        s.records_written,
                        s.errors,
                        s.bytes_received / (1024 * 1024),
                        ss.data_size_mb(),
                        ss.compression_ratio
                    );
                } else {
                    tracing::info!(
                        "Stats: fetches={}, received={}, written={}, errors={}",
                        s.fetches,
                        s.records_received,
                        s.records_written,
                        s.errors
                    );
                }
            }
        })
    };

    // Handle Ctrl+C
    let collector_handle = tokio::spawn(async move {
        collector.run().await
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down...");
        }
        result = collector_handle => {
            if let Err(e) = result {
                tracing::error!("Delta collector error: {}", e);
            }
        }
    }

    stats_handle.abort();

    let final_stats = stats.snapshot();
    let storage_stats = DeltaStorage::open(data_path)
        .map(|s| s.stats())
        .ok();

    tracing::info!("Final statistics:");
    tracing::info!("  Total fetches: {}", final_stats.fetches);
    tracing::info!("  Total records received: {}", final_stats.records_received);
    tracing::info!("  Total records written: {}", final_stats.records_written);
    tracing::info!("  Total errors: {}", final_stats.errors);

    if let Some(ss) = storage_stats {
        tracing::info!("  Unique aircraft: {}", ss.aircraft_count);
        tracing::info!("  Keyframes: {}", ss.keyframe_count);
        tracing::info!("  Deltas: {}", ss.delta_count);
        tracing::info!("  Keyframe ratio: {:.1}%", ss.keyframe_ratio() * 100.0);
        tracing::info!("  Avg bytes/record: {:.1}", ss.avg_bytes_per_record);
        tracing::info!("  Compression ratio: {:.1}x", ss.compression_ratio);
        tracing::info!("  Data size: {:.2} MB", ss.data_size_mb());
        tracing::info!("  Bytes saved: {:.2} MB", ss.bytes_saved_mb());
    }

    Ok(())
}

fn query_icao(
    data_path: &PathBuf,
    icao_str: &str,
    limit: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let icao_val = u32::from_str_radix(icao_str, 16)?;
    let icao = IcaoAddress::new(icao_val);

    tracing::info!("Looking up ICAO: {}", icao);

    let storage = Storage::open(data_path)?;

    match storage.lookup(icao) {
        Some(records) => {
            let display_records: Vec<_> = if let Some(n) = limit {
                records.iter().rev().take(n).collect()
            } else {
                records.iter().collect()
            };

            println!("Found {} records for ICAO {}", records.len(), icao);
            println!();

            for record in display_records {
                println!("Timestamp: {}", record.timestamp_ms);

                if let (Some(lat), Some(lon)) = (record.latitude(), record.longitude()) {
                    println!("  Position: {:.6}, {:.6}", lat, lon);
                }

                if let Some(alt) = record.alt_baro() {
                    println!("  Altitude (baro): {} ft", alt);
                }

                if let Some(gs) = record.ground_speed() {
                    println!("  Ground speed: {:.1} kt", gs);
                }

                if let Some(track) = record.track() {
                    println!("  Track: {:.1}°", track);
                }

                if let Some(callsign) = record.callsign_str() {
                    println!("  Callsign: {}", callsign);
                }

                if let Some(reg) = record.registration_str() {
                    println!("  Registration: {}", reg);
                }

                if let Some(atype) = record.aircraft_type_str() {
                    println!("  Type: {}", atype);
                }

                println!();
            }
        }
        None => {
            println!("No records found for ICAO {}", icao);
        }
    }

    Ok(())
}

fn show_stats(data_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let storage = Storage::open(data_path)?;
    let stats = storage.stats();

    println!("Storage Statistics");
    println!("==================");
    println!("Aircraft tracked: {}", stats.aircraft_count);
    println!("Total records: {}", stats.total_records);
    println!("Data size: {:.2} MB", stats.data_size_mb());
    println!("Capacity: {:.2} MB", stats.data_capacity_bytes as f64 / (1024.0 * 1024.0));
    println!("Utilization: {:.1}%", stats.utilization() * 100.0);

    Ok(())
}

fn show_delta_stats(data_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let storage = DeltaStorage::open(data_path)?;
    let stats = storage.stats();

    println!("Delta Storage Statistics");
    println!("========================");
    println!("Aircraft tracked: {}", stats.aircraft_count);
    println!("Total records: {}", stats.total_records);
    println!("  Keyframes: {}", stats.keyframe_count);
    println!("  Deltas: {}", stats.delta_count);
    println!("  Keyframe ratio: {:.1}%", stats.keyframe_ratio() * 100.0);
    println!();
    println!("Compression:");
    println!("  Avg bytes/record: {:.1}", stats.avg_bytes_per_record);
    println!("  Compression ratio: {:.1}x", stats.compression_ratio);
    println!("  Bytes saved: {:.2} MB", stats.bytes_saved_mb());
    println!();
    println!("Storage:");
    println!("  Data size: {:.2} MB", stats.data_size_mb());
    println!("  Capacity: {:.2} MB", stats.data_capacity_bytes as f64 / (1024.0 * 1024.0));

    // Compare to raw storage
    let raw_size = stats.total_records as f64 * 112.0 / (1024.0 * 1024.0);
    println!();
    println!("Comparison:");
    println!("  Raw storage would be: {:.2} MB", raw_size);
    println!("  Actual size: {:.2} MB", stats.data_size_mb());
    println!("  Space savings: {:.1}%", (1.0 - stats.data_size_mb() / raw_size) * 100.0);

    Ok(())
}

fn parse_file(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(path)?;

    tracing::info!("Parsing file: {} ({} bytes)", path.display(), data.len());

    let (header, records) = protocol::parse_response(&data)?;

    println!("Header:");
    println!("  Timestamp: {} ms", header.timestamp_ms);
    println!("  Stride: {} bytes", header.stride);
    println!("  Aircraft with position: {}", header.aircraft_count_with_pos);
    println!("  Version: {}", header.version);
    println!("  Receiver: {:.6}, {:.6}", header.receiver_lat, header.receiver_lon);
    println!();
    println!("Parsed {} aircraft records", records.len());
    println!();

    // Show first 10 records
    for (i, record) in records.iter().take(10).enumerate() {
        let icao = record.icao_address();
        println!("Record {}:", i + 1);
        println!("  ICAO: {}", icao);

        if let (Some(lat), Some(lon)) = (record.latitude(), record.longitude()) {
            println!("  Position: {:.6}, {:.6}", lat, lon);
        }

        if let Some(alt) = record.alt_baro() {
            println!("  Altitude: {} ft", alt);
        }

        if let Some(callsign) = record.callsign_str() {
            println!("  Callsign: {}", callsign);
        }

        if let Some(reg) = record.registration_str() {
            println!("  Registration: {}", reg);
        }

        println!();
    }

    if records.len() > 10 {
        println!("... and {} more records", records.len() - 10);
    }

    Ok(())
}
