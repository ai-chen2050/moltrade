# Relayer

> A high-performance and scalable Nostr protocol relay service

## Project Overview

Rust service that ingests Nostr events, deduplicates, forwards to downstreams, and optionally manages bot/follower subscriptions with settlement-driven credit issuance.

## What It Does

- Nostr relay client pool with health checks and filtering by allowed kinds.
- Deduplication hotset (RocksDB-backed) to avoid re-forwarding the same event.
- Downstream streaming via WebSocket.
- REST API for health/metrics, relay admin (token-protected), bot/subscription registry (Postgres), trade record/settlement, and credit queries.
- Settlement worker polls an explorer for tx hashes, updates trade status, and awards credits using configurable leader/follower rates and profit multipliers.

## Architecture (concise)

- `relay_pool`: connects to configured relays, streams events.
- `dedupe_engine`: Bloom + LRU + RocksDB hotset to drop duplicates.
- `event_router`: batches, filters, and routes to downstream + optional fanout.
- `downstream`: WebSocket server for streaming events to clients.
- `api`: Axum REST for ops, subscriptions, trades, credits; metrics endpoint.
- `subscription_service` (Postgres): bots, follower shared secrets, trade_executions, credits.
- `settlement_worker`: polls tx hashes, marks confirmed/failed, issues credits.

## Config Highlights (see config.template.toml)

- `[relay]`, `[deduplication]`, `[output]`, `[monitoring]`
- `[postgres]` to enable subscriptions/fanout/trade tracking
- `[settlement]` base URL, poll interval, batch_limit, token; `[settlement.credit]` leader/follower rates, min_credit, profit_multiplier, enable
- `[subscriptions]` daily_limit (per bot eth_address for POST)

## Quick Start

### Prerequisites

- Rust 1.89.0+ (latest stable version recommended)
- Cargo (Rust package manager)
- macOS or Linux (Windows requires WSL)

### Installation

1. **Clone the project**

   ```bash
   git clone https://github.com/hetu-project/moltrade.git
   cd relayer
   ```

2. **Create configuration file**

   ```bash
   cp config.template.toml config.toml
   # Edit config.toml to customize as needed
   # OR use makefile compile tool
   make setup-env
   ```

3. **Compile and run**
   ```bash
   make build                  # Build the project
   make run                    # Run the project with config.toml
   # Or
   make dev                    # Dev mode (debug build) with config.template.toml
   ```

### Using Makefile

The project includes a Makefile with convenient build commands:

```bash
make help           # Show all available commands
make build          # Build release version (optimized)
make dev            # Build development version (debug)
make run            # Run release version
make debug          # Run development version
make test           # Run unit tests
make bench          # Run benchmark tests
make clean          # Clean build artifacts
make fmt            # Format code
make lint           # Code check
make release        # Build release and show binary path
make docker-build   # Build Docker image
make docker-run     # Run Docker container
make docker-push    # Push Docker image
```

### Direct Execution

```bash
# Use default relays (example relays)
cargo run --release

# Use configuration file
cargo run --release -- --config config.toml

# Set log level
RUST_LOG=moltrade_relayer=debug cargo run --release
```

## API Endpoints

Please refer to [docs/API.md](docs/API.md) for detailed API endpoint documentation.

### Subscription fanout (optional)

Enable Postgres to register bots and followers, filter Nostr kinds, and stream encrypted fanout payloads.

Config snippet:

```toml
[filters]
allowed_kinds = [30931, 30932, 30933, 30934]

[postgres]
dsn = "postgres://postgres:postgres@localhost:5432/moltrade"
max_connections = 5
```

REST endpoints:

- POST `/api/bots/register` `{ bot_pubkey, name }`
- POST `/api/subscriptions` `{ bot_pubkey, follower_pubkey, shared_secret }`
- GET `/api/subscriptions/:bot_pubkey`

WebSockets:

- `/ws` streams filtered Nostr events
- `/fanout` streams encrypted follower payloads (enabled when Postgres is configured)

## Configuration File

### Configuration Template (config.template.toml)

```toml
[relay]
# Relay connection configuration
health_check_interval = 30      # Health check interval (seconds)
max_connections = 10000         # Maximum connections
bootstrap_relays = [            # Bootstrap relay list
  "wss://relay.damus.io",
  "wss://nos.lol",
]

[deduplication]
# Deduplication engine configuration
rocksdb_path = "./data/rocksdb" # RocksDB data path
hotset_size = 10000             # Hotset size
bloom_capacity = 1000000        # Bloom filter capacity
lru_size = 50000                # LRU cache size

[output]
# Output configuration
websocket_enabled = true        # Enable WebSocket
websocket_port = 8080           # WebSocket port
bind_address = "127.0.0.1"      # Bind address for REST/WebSocket
batch_size = 100                # Batch processing size
max_latency_ms = 100            # Maximum latency (milliseconds)

[monitoring]
# Monitoring configuration
log_level = "info"              # Log level (trace/debug/info/warn/error)
prometheus_port = 9090          # Prometheus port
```

## Operations and Deployment

### Docker Deployment

```bash
# Build Docker image
make docker-build

# Run Docker container
make docker-run

# View container logs
docker logs moltrade-relayer

# Stop container
docker stop moltrade-relayer
```

## API

See [docs/API.md](../docs/API.md) for request/response examples. Notable headers: `X-Settlement-Token` for relay admin and settlement-protected routes.

### Debug Logging

```bash
# Enable debug logging
RUST_LOG=moltrade_relayer=debug cargo run --release

# View specific module logs
RUST_LOG=moltrade_relayer::core::relay_pool=trace cargo run --release

# Save logs to file
RUST_LOG=moltrade_relayer=info cargo run --release > relay.log 2>&1
```

## Development Guide

### Code Standards

- Follow official Rust coding conventions
- Use `cargo fmt` for code formatting
- Use `cargo clippy` for code quality checks

```bash
make fmt    # Format code
make lint   # Check code
```

### Adding New Features

1. Create new file in corresponding module directory
2. Declare module in `mod.rs`
3. Implement feature and write unit tests
4. Run `cargo test` to verify

### Performance Optimization

- Use `cargo bench` for benchmarking
- Use `perf` or Flamegraph for performance analysis
- Focus on zero-copy and async operation optimization

## Contributing

Contributions are welcome! Please ensure:

1. Code passes `cargo clippy` checks
2. Code conforms to `cargo fmt` formatting
3. Add appropriate unit tests
4. Update relevant documentation

## License

[MIT](../LICENSE)
