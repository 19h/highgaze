//! Continuous data collection orchestrator.

use crate::client::{AdsbClient, BoundingBox, ClientConfig, ClientError};
use crate::protocol::{self, ParseError};
use crate::storage::{Storage, StorageError};
use crate::types::AircraftRecord;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::sleep;

#[derive(Debug, Error)]
pub enum CollectorError {
    #[error("Client error: {0}")]
    Client(#[from] ClientError),
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Collector stopped")]
    Stopped,
}

/// Statistics for the collector.
#[derive(Debug, Default)]
pub struct CollectorStats {
    pub fetches: AtomicU64,
    pub records_written: AtomicU64,
    pub records_received: AtomicU64,
    pub errors: AtomicU64,
    pub bytes_received: AtomicU64,
    pub last_fetch_ms: AtomicU64,
}

impl CollectorStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            fetches: self.fetches.load(Ordering::Relaxed),
            records_written: self.records_written.load(Ordering::Relaxed),
            records_received: self.records_received.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            last_fetch_ms: self.last_fetch_ms.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub fetches: u64,
    pub records_written: u64,
    pub records_received: u64,
    pub errors: u64,
    pub bytes_received: u64,
    pub last_fetch_ms: u64,
}

/// Configuration for the collector.
#[derive(Debug, Clone)]
pub struct CollectorConfig {
    /// Interval between fetches
    pub fetch_interval: Duration,
    /// Bounding box to query
    pub bbox: BoundingBox,
    /// Number of worker tasks for parallel storage writes
    pub write_workers: usize,
    /// Buffer size for write queue
    pub write_buffer_size: usize,
    /// Sync interval (flush to disk)
    pub sync_interval: Duration,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            fetch_interval: Duration::from_secs(1),
            bbox: BoundingBox::GLOBAL,
            write_workers: 4,
            write_buffer_size: 10000,
            sync_interval: Duration::from_secs(30),
        }
    }
}

/// The main collector that orchestrates fetching and storage.
pub struct Collector {
    client: AdsbClient,
    storage: Arc<Storage>,
    config: CollectorConfig,
    stats: Arc<CollectorStats>,
    running: Arc<AtomicBool>,
}

impl Collector {
    /// Create a new collector.
    pub fn new(
        client: AdsbClient,
        storage: Storage,
        config: CollectorConfig,
    ) -> Self {
        Self {
            client,
            storage: Arc::new(storage),
            config,
            stats: Arc::new(CollectorStats::new()),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get a reference to the collector stats.
    pub fn stats(&self) -> Arc<CollectorStats> {
        Arc::clone(&self.stats)
    }

    /// Check if the collector is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Stop the collector.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Run the collector continuously.
    pub async fn run(&self) -> Result<(), CollectorError> {
        self.running.store(true, Ordering::SeqCst);

        // Channel for passing records to storage workers
        let (tx, rx) = mpsc::channel::<Vec<AircraftRecord>>(self.config.write_buffer_size);

        // Spawn storage workers
        let storage_handle = self.spawn_storage_workers(rx);

        // Spawn sync task
        let sync_handle = self.spawn_sync_task();

        // Main fetch loop
        let fetch_result = self.fetch_loop(tx).await;

        // Signal workers to stop
        self.running.store(false, Ordering::SeqCst);

        // Wait for workers
        let _ = storage_handle.await;
        let _ = sync_handle.await;

        fetch_result
    }

    async fn fetch_loop(
        &self,
        tx: mpsc::Sender<Vec<AircraftRecord>>,
    ) -> Result<(), CollectorError> {
        let mut interval = tokio::time::interval(self.config.fetch_interval);
        let mut backoff = Duration::from_secs(1);
        const MAX_BACKOFF: Duration = Duration::from_secs(60);

        while self.running.load(Ordering::Relaxed) {
            interval.tick().await;

            let start = Instant::now();

            match self.fetch_and_parse().await {
                Ok((records, bytes)) => {
                    let elapsed = start.elapsed();

                    self.stats.fetches.fetch_add(1, Ordering::Relaxed);
                    self.stats.bytes_received.fetch_add(bytes as u64, Ordering::Relaxed);
                    self.stats
                        .records_received
                        .fetch_add(records.len() as u64, Ordering::Relaxed);
                    self.stats
                        .last_fetch_ms
                        .store(elapsed.as_millis() as u64, Ordering::Relaxed);

                    tracing::debug!(
                        "Fetched {} aircraft in {:?} ({} bytes)",
                        records.len(),
                        elapsed,
                        bytes
                    );

                    // Send records to storage workers
                    if !records.is_empty() {
                        if tx.send(records).await.is_err() {
                            tracing::warn!("Storage channel closed");
                            break;
                        }
                    }

                    // Reset backoff on success
                    backoff = Duration::from_secs(1);
                }
                Err(CollectorError::Client(ClientError::RateLimited { retry_after })) => {
                    let wait = retry_after.unwrap_or(backoff);
                    tracing::warn!("Rate limited, waiting {:?}", wait);
                    self.stats.errors.fetch_add(1, Ordering::Relaxed);
                    sleep(wait).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
                Err(e) => {
                    tracing::error!("Fetch error: {}", e);
                    self.stats.errors.fetch_add(1, Ordering::Relaxed);
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }

        Ok(())
    }

    async fn fetch_and_parse(&self) -> Result<(Vec<AircraftRecord>, usize), CollectorError> {
        let data = self.client.fetch(self.config.bbox).await?;
        let bytes = data.len();

        let (_, records) = protocol::parse_response(&data)?;
        Ok((records, bytes))
    }

    fn spawn_storage_workers(
        &self,
        mut rx: mpsc::Receiver<Vec<AircraftRecord>>,
    ) -> tokio::task::JoinHandle<()> {
        let storage = Arc::clone(&self.storage);
        let stats = Arc::clone(&self.stats);
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                match rx.recv().await {
                    Some(records) => {
                        match storage.write_batch(&records) {
                            Ok(written) => {
                                stats.records_written.fetch_add(written as u64, Ordering::Relaxed);
                            }
                            Err(e) => {
                                tracing::error!("Storage write error: {}", e);
                                stats.errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    None => break,
                }
            }
        })
    }

    fn spawn_sync_task(&self) -> tokio::task::JoinHandle<()> {
        let storage = Arc::clone(&self.storage);
        let running = Arc::clone(&self.running);
        let sync_interval = self.config.sync_interval;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(sync_interval);

            while running.load(Ordering::Relaxed) {
                interval.tick().await;

                if let Err(e) = storage.sync() {
                    tracing::error!("Sync error: {}", e);
                } else {
                    tracing::debug!("Synced storage to disk");
                }
            }

            // Final sync
            if let Err(e) = storage.sync() {
                tracing::error!("Final sync error: {}", e);
            }
        })
    }
}

/// Builder for creating a Collector with custom configuration.
pub struct CollectorBuilder {
    client_config: Option<ClientConfig>,
    storage_path: Option<String>,
    collector_config: CollectorConfig,
}

impl CollectorBuilder {
    pub fn new() -> Self {
        Self {
            client_config: None,
            storage_path: None,
            collector_config: CollectorConfig::default(),
        }
    }

    pub fn client_config(mut self, config: ClientConfig) -> Self {
        self.client_config = Some(config);
        self
    }

    pub fn storage_path(mut self, path: String) -> Self {
        self.storage_path = Some(path);
        self
    }

    pub fn fetch_interval(mut self, interval: Duration) -> Self {
        self.collector_config.fetch_interval = interval;
        self
    }

    pub fn bbox(mut self, bbox: BoundingBox) -> Self {
        self.collector_config.bbox = bbox;
        self
    }

    pub fn sync_interval(mut self, interval: Duration) -> Self {
        self.collector_config.sync_interval = interval;
        self
    }

    pub fn build(self) -> Result<Collector, CollectorError> {
        let client_config = self
            .client_config
            .ok_or_else(|| CollectorError::Client(ClientError::AuthError))?;

        let storage_path = self.storage_path.unwrap_or_else(|| "adsb_data".to_string());

        let client = AdsbClient::new(client_config)?;
        let storage = Storage::open(&storage_path)?;

        Ok(Collector::new(client, storage, self.collector_config))
    }
}

impl Default for CollectorBuilder {
    fn default() -> Self {
        Self::new()
    }
}
