//! HTTP client for fetching ADS-B data from the exchange.

use reqwest::{
    header::{HeaderMap, HeaderValue, ACCEPT, CACHE_CONTROL, COOKIE, REFERER},
    Client, StatusCode,
};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("Server returned error status: {status}")]
    ServerError { status: StatusCode },
    #[error("Rate limited, retry after {retry_after:?}")]
    RateLimited { retry_after: Option<Duration> },
    #[error("Authentication failed")]
    AuthError,
    #[error("Invalid response")]
    InvalidResponse,
}

/// Bounding box for geographic queries.
#[derive(Debug, Clone, Copy)]
pub struct BoundingBox {
    pub south: f64,
    pub north: f64,
    pub west: f64,
    pub east: f64,
}

impl BoundingBox {
    /// Global bounding box covering the entire world.
    pub const GLOBAL: Self = Self {
        south: -90.0,
        north: 90.0,
        west: -180.0,
        east: 180.0,
    };

    /// Create a bounding box from coordinates.
    pub fn new(south: f64, north: f64, west: f64, east: f64) -> Self {
        Self {
            south,
            north,
            west,
            east,
        }
    }

    fn to_query_string(&self) -> String {
        format!("{},{},{},{}", self.south, self.north, self.west, self.east)
    }
}

/// Configuration for the ADS-B client.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Session ID cookie (adsbx_sid)
    pub session_id: String,
    /// API cookie (adsbx_api)
    pub api_key: String,
    /// Request timeout
    pub timeout: Duration,
    /// Optional specific ICAO hex to track
    pub find_hex: Option<String>,
}

impl ClientConfig {
    pub fn new(session_id: String, api_key: String) -> Self {
        Self {
            session_id,
            api_key,
            timeout: Duration::from_secs(30),
            find_hex: None,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_find_hex(mut self, hex: String) -> Self {
        self.find_hex = Some(hex);
        self
    }
}

/// Client for fetching ADS-B data.
pub struct AdsbClient {
    client: Client,
    config: ClientConfig,
    base_url: String,
}

impl AdsbClient {
    /// Create a new ADS-B client.
    pub fn new(config: ClientConfig) -> Result<Self, ClientError> {
        let mut headers = HeaderMap::new();

        headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
        headers.insert(
            "accept-language",
            HeaderValue::from_static("en-GB,en-US;q=0.9,en;q=0.8"),
        );
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        headers.insert("pragma", HeaderValue::from_static("no-cache"));
        headers.insert("sec-fetch-dest", HeaderValue::from_static("empty"));
        headers.insert("sec-fetch-mode", HeaderValue::from_static("cors"));
        headers.insert("sec-fetch-site", HeaderValue::from_static("same-origin"));
        headers.insert(
            "x-requested-with",
            HeaderValue::from_static("XMLHttpRequest"),
        );

        // Set cookies
        let cookie = format!(
            "adsbx_sid={}; adsbx_api={}",
            config.session_id, config.api_key
        );
        headers.insert(COOKIE, HeaderValue::from_str(&cookie).unwrap());

        headers.insert(
            REFERER,
            HeaderValue::from_static("https://globe.adsbexchange.com/"),
        );

        let client = Client::builder()
            .default_headers(headers)
            .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .timeout(config.timeout)
            .gzip(true)
            .brotli(true)
            .deflate(true)
            .build()?;

        Ok(Self {
            client,
            config,
            base_url: "https://globe.adsbexchange.com".to_string(),
        })
    }

    /// Fetch aircraft data for a bounding box.
    pub async fn fetch(&self, bbox: BoundingBox) -> Result<Vec<u8>, ClientError> {
        let mut url = format!(
            "{}/re-api/?binCraft&zstd&box={}",
            self.base_url,
            bbox.to_query_string()
        );

        if let Some(ref hex) = self.config.find_hex {
            url.push_str(&format!("&find_hex={}", hex));
        }

        tracing::debug!("Fetching: {}", url);

        let response = self.client.get(&url).send().await?;

        match response.status() {
            StatusCode::OK => {
                let bytes = response.bytes().await?;
                Ok(bytes.to_vec())
            }
            StatusCode::TOO_MANY_REQUESTS => {
                let retry_after = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(Duration::from_secs);

                Err(ClientError::RateLimited { retry_after })
            }
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(ClientError::AuthError),
            status => Err(ClientError::ServerError { status }),
        }
    }

    /// Fetch global aircraft data.
    pub async fn fetch_global(&self) -> Result<Vec<u8>, ClientError> {
        self.fetch(BoundingBox::GLOBAL).await
    }

    /// Update the session cookies.
    pub fn update_cookies(&mut self, session_id: String, api_key: String) {
        self.config.session_id = session_id;
        self.config.api_key = api_key;
        // Note: In production, we'd rebuild the client with new headers
        // For now, caller should create a new client
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bounding_box() {
        // Format: south,north,west,east
        let bbox = BoundingBox::new(40.0, 45.0, -75.0, -70.0);
        assert_eq!(bbox.to_query_string(), "40,45,-75,-70");
    }

    #[test]
    fn test_global_bbox() {
        // Format: south,north,west,east
        let bbox = BoundingBox::GLOBAL;
        assert_eq!(bbox.to_query_string(), "-90,90,-180,180");
    }
}
