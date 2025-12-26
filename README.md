# Highgaze

High-performance ADS-B data collector and storage system written in Rust.

## Features

- **Continuous data collection** from ADS-B Exchange API
- **Binary protocol parsing** (binCraft format)
- **Delta compression** - saves ~85% storage compared to raw format
- **Memory-mapped storage** for fast indexed access
- **Query interface** for historical aircraft data
- **Bounding box filtering** for regional data collection

## Installation

```bash
cargo build --release
```

## Usage

### Collecting Data

Start continuous data collection:

```bash
# Basic collection (global)
highgaze collect \
  --session-id $ADSB_SESSION_ID \
  --api-key $ADSB_API_KEY

# With delta compression (recommended)
highgaze collect \
  --session-id $ADSB_SESSION_ID \
  --api-key $ADSB_API_KEY \
  --delta

# Regional collection with bounding box
highgaze collect \
  --session-id $ADSB_SESSION_ID \
  --api-key $ADSB_API_KEY \
  --south 24.0 --north 50.0 --west -125.0 --east -66.0 \
  --delta

# Track specific aircraft by ICAO hex
highgaze collect \
  --session-id $ADSB_SESSION_ID \
  --api-key $ADSB_API_KEY \
  --track-hex 740828
```

Environment variables `ADSB_SESSION_ID` and `ADSB_API_KEY` can be used instead of CLI arguments.

### Querying Data

Look up records by ICAO address:

```bash
# Query all records for an aircraft
highgaze query --icao 740828

# Show only the last 10 records
highgaze query --icao 740828 --limit 10
```

### Storage Statistics

```bash
# Raw storage stats
highgaze stats

# Delta storage stats
highgaze stats --delta
```

### Parse Binary File

For testing/debugging, parse a single binCraft response file:

```bash
highgaze parse path/to/file.bin
```

## Options

| Option | Description | Default |
|--------|-------------|---------|
| `-d, --data-path` | Path to data storage | `adsb_data` |
| `-l, --log-level` | Log level (trace, debug, info, warn, error) | `info` |
| `-i, --interval` | Fetch interval in seconds | `10` |
| `--delta` | Enable delta compression | disabled |

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Client    │───>│  Protocol   │───>│   Storage   │
│  (HTTP)     │    │  (Parser)   │    │  (Indexed)  │
└─────────────┘    └─────────────┘    └─────────────┘
       │                                     │
       └─────────────┬───────────────────────┘
                     v
             ┌──────────────┐
             │  Collector   │
             │(Orchestrator)│
             └──────────────┘
```

## Library Usage

```rust
use adsb_collector::{
    client::{AdsbClient, ClientConfig},
    collector::{Collector, CollectorConfig},
    storage::Storage,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client_config = ClientConfig::new(
        "session_id".to_string(),
        "api_key".to_string(),
    );

    let client = AdsbClient::new(client_config)?;
    let storage = Storage::open("adsb_data")?;

    let collector = Collector::new(
        client,
        storage,
        CollectorConfig::default(),
    );

    collector.run().await?;
    Ok(())
}
```

## License

MIT
