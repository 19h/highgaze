//! High-performance ADS-B data collector and storage library.
//!
//! This library provides functionality to:
//! - Fetch ADS-B aircraft data from exchange APIs
//! - Parse binary binCraft protocol
//! - Store data in a custom high-performance format
//! - Query historical aircraft data
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
//! │   Client    │───▶│  Protocol   │───▶│   Storage   │
//! │  (HTTP/WS)  │    │  (Parser)   │    │  (Indexed)  │
//! └─────────────┘    └─────────────┘    └─────────────┘
//!        │                                     │
//!        └─────────────┬───────────────────────┘
//!                      ▼
//!              ┌─────────────┐
//!              │  Collector  │
//!              │ (Orchestrator)│
//!              └─────────────┘
//! ```
//!
//! # Example
//!
//! ```no_run
//! use adsb_collector::{
//!     client::{AdsbClient, ClientConfig},
//!     collector::{Collector, CollectorConfig},
//!     storage::Storage,
//! };
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configure client with authentication
//!     let client_config = ClientConfig::new(
//!         "session_id".to_string(),
//!         "api_key".to_string(),
//!     );
//!
//!     let client = AdsbClient::new(client_config)?;
//!     let storage = Storage::open("adsb_data")?;
//!
//!     let collector = Collector::new(
//!         client,
//!         storage,
//!         CollectorConfig::default(),
//!     );
//!
//!     // Run the collector
//!     collector.run().await?;
//!
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod collector;
pub mod compact;
pub mod delta;
pub mod delta_collector;
pub mod delta_storage;
pub mod protocol;
pub mod storage;
pub mod types;

pub use client::{AdsbClient, ClientConfig, BoundingBox};
pub use collector::{Collector, CollectorBuilder, CollectorConfig};
pub use delta_collector::{DeltaCollector, DeltaCollectorConfig};
pub use delta_storage::{DeltaStorage, DeltaStorageStats};
pub use protocol::parse_response;
pub use storage::Storage;
pub use types::{AircraftRecord, IcaoAddress};
