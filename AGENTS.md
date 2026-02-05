# Project Guidelines

## Code Style

- Rust 2024 workspace; run `make fmt` and `make lint` (Clippy) before commits; use the feature-based module layout (`api`, `core`, `storage`) and wire new modules in `mod.rs`.

## Architecture

- Startup flow initializes tracing, metrics, RocksDB, dedupe warmup, relay pool, optional Postgres subscription service, settlement worker, event router, and Axum HTTP server with optional WebSocket fanout streaming [relayer/src/main.rs](relayer/src/main.rs#L36-L221).
- Dedup engine warms from RocksDB to avoid resend on restart [relayer/src/main.rs](relayer/src/main.rs#L56-L66); event router batches with max-latency cap and optional fanout [relayer/src/main.rs](relayer/src/main.rs#L152-L171).
- Settlement worker polls Hyperliquid explorer and applies credit config when Postgres is enabled [relayer/src/main.rs](relayer/src/main.rs#L105-L133).

## Build and Test

- Setup: `cp relayer/config.template.toml relayer/config.toml && make setup-env` in relayer root [relayer/README.md](relayer/README.md#L42-L58).
- Common commands: `make build`, `make run`, `make dev`, `make test`, `make bench`, `make fmt`, `make lint`, Docker targets (`make docker-build`, `make docker-run`) [relayer/README.md](relayer/README.md#L60-L187).
- Direct run: `cargo run --release -- --config config.toml`; set logs via `RUST_LOG=moltrade_relayer=debug` [relayer/README.md](relayer/README.md#L89-L100).

## Project Conventions

- REST routes are centralized in the router builder [relayer/src/api/rest_api.rs](relayer/src/api/rest_api.rs#L64-L104); prefer adding endpoints there with shared `AppState`.
- Trade insert/update handlers treat duplicate `tx_hash` as idempotent OK and map FK violations to 400 [relayer/src/api/rest_api.rs](relayer/src/api/rest_api.rs#L374-L459).
- Daily subscription throttling enforced per bot ETH address; set `subscriptions.daily_limit` to zero to disable [relayer/src/api/rest_api.rs](relayer/src/api/rest_api.rs#L512-L520).
- ETH addresses validated on bot registration [relayer/src/api/rest_api.rs](relayer/src/api/rest_api.rs#L503-L510).

## Integration Points

- Postgres DSN/config gates subscriptions, trades, credits, and fanout [relayer/config.template.toml](relayer/config.template.toml#L25-L32).
- Settlement/admin endpoints require `X-Settlement-Token` when configured; missing token means no auth [relayer/src/api/rest_api.rs](relayer/src/api/rest_api.rs#L492-L500) and set via [relayer/config.template.toml](relayer/config.template.toml#L39-L43).
- Nostr secret key read from config; invalid/empty keys trigger auto-generation and persistence when config path is known [relayer/src/main.rs](relayer/src/main.rs#L67-L133).

## Security

- Replace the sample `nostr.secret_key` before running production [relayer/config.template.toml](relayer/config.template.toml#L17-L21); keep config files out of VCS.
- Always set `settlement.token` to protect relay admin and settlement update routes [relayer/src/api/rest_api.rs](relayer/src/api/rest_api.rs#L492-L500).
- Subscription/bot endpoints validate ETH format and enforce daily limits; keep `subscriptions.daily_limit` consistent with abuse expectations [relayer/src/api/rest_api.rs](relayer/src/api/rest_api.rs#L503-L520).
