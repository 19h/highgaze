//! Delta-compressed data collection orchestrator.

use crate::client::{AdsbClient, BoundingBox, ClientError};
use crate::delta_storage::{DeltaStorage, DeltaStorageError, DeltaStorageStats};
use crate::protocol::{self, ParseError};
use crate::types::AircraftRecord;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::sleep;

#[derive(Debug, Error)]
pub enum DeltaCollectorError {
    #[error("Client error: {0}")]
    Client(#[from] ClientError),
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),
    #[error("Storage error: {0}")]
    Storage(#[from] DeltaStorageError),
    #[error("Collector stopped")]
    Stopped,
}

/// Statistics for the delta collector.
#[derive(Debug, Default)]
pub struct DeltaCollectorStats {
    pub fetches: AtomicU64,
    pub records_written: AtomicU64,
    pub records_received: AtomicU64,
    pub errors: AtomicU64,
    pub bytes_received: AtomicU64,
    pub last_fetch_ms: AtomicU64,
}

impl DeltaCollectorStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> DeltaStatsSnapshot {
        DeltaStatsSnapshot {
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
pub struct DeltaStatsSnapshot {
    pub fetches: u64,
    pub records_written: u64,
    pub records_received: u64,
    pub errors: u64,
    pub bytes_received: u64,
    pub last_fetch_ms: u64,
}

/// Configuration for the delta collector.
#[derive(Debug, Clone)]
pub struct DeltaCollectorConfig {
    pub fetch_interval: Duration,
    pub bbox: BoundingBox,
    pub write_buffer_size: usize,
    pub sync_interval: Duration,
}

impl Default for DeltaCollectorConfig {
    fn default() -> Self {
        Self {
            fetch_interval: Duration::from_secs(1),
            bbox: BoundingBox::GLOBAL,
            write_buffer_size: 10000,
            sync_interval: Duration::from_secs(30),
        }
    }
}

/// Delta-compressed collector that uses DeltaStorage.
pub struct DeltaCollector {
    client: AdsbClient,
    storage: Arc<DeltaStorage>,
    config: DeltaCollectorConfig,
    stats: Arc<DeltaCollectorStats>,
    running: Arc<AtomicBool>,
}

impl DeltaCollector {
    pub fn new(
        client: AdsbClient,
        storage: DeltaStorage,
        config: DeltaCollectorConfig,
    ) -> Self {
        Self {
            client,
            storage: Arc::new(storage),
            config,
            stats: Arc::new(DeltaCollectorStats::new()),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn stats(&self) -> Arc<DeltaCollectorStats> {
        Arc::clone(&self.stats)
    }

    pub fn storage_stats(&self) -> DeltaStorageStats {
        self.storage.stats()
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub async fn run(&self) -> Result<(), DeltaCollectorError> {
        self.running.store(true, Ordering::SeqCst);

        let (tx, rx) = mpsc::channel::<Vec<AircraftRecord>>(self.config.write_buffer_size);

        let storage_handle = self.spawn_storage_worker(rx);
        let sync_handle = self.spawn_sync_task();

        let fetch_result = self.fetch_loop(tx).await;

        self.running.store(false, Ordering::SeqCst);

        let _ = storage_handle.await;
        let _ = sync_handle.await;

        fetch_result
    }

    async fn fetch_loop(
        &self,
        tx: mpsc::Sender<Vec<AircraftRecord>>,
    ) -> Result<(), DeltaCollectorError> {
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
                    self.stats
                        .bytes_received
                        .fetch_add(bytes as u64, Ordering::Relaxed);
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

                    if !records.is_empty() {
                        if tx.send(records).await.is_err() {
                            tracing::warn!("Storage channel closed");
                            break;
                        }
                    }

                    backoff = Duration::from_secs(1);
                }
                Err(DeltaCollectorError::Client(ClientError::RateLimited { retry_after })) => {
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

    async fn fetch_and_parse(&self) -> Result<(Vec<AircraftRecord>, usize), DeltaCollectorError> {
        let data = self.client.fetch(self.config.bbox).await?;
        let bytes = data.len();

        let (_, records) = protocol::parse_response(&data)?;
        Ok((records, bytes))
    }

    fn spawn_storage_worker(
        &self,
        mut rx: mpsc::Receiver<Vec<AircraftRecord>>,
    ) -> tokio::task::JoinHandle<()> {
        let storage = Arc::clone(&self.storage);
        let stats = Arc::clone(&self.stats);
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                match rx.recv().await {
                    Some(records) => match storage.write_batch(&records) {
                        Ok(written) => {
                            stats
                                .records_written
                                .fetch_add(written as u64, Ordering::Relaxed);
                        }
                        Err(e) => {
                            tracing::error!("Delta storage write error: {}", e);
                            stats.errors.fetch_add(1, Ordering::Relaxed);
                        }
                    },
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
                    tracing::debug!("Synced delta storage to disk");
                }
            }

            if let Err(e) = storage.sync() {
                tracing::error!("Final sync error: {}", e);
            }
        })
    }
}
