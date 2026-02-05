## API Endpoints

Base URL defaults to `http://localhost:8080` (configurable via `output.websocket_port`). Set `TOKEN` in examples below to your `settlement.token` if configured; omit the header when unset.

### Health

```bash
curl http://localhost:8080/health
```

### Connection Status

```bash
curl http://localhost:8080/status
```

### Metrics (Prometheus)

```bash
curl http://localhost:8080/metrics
```

### Metrics Summary (JSON)

```bash
curl http://localhost:8080/api/metrics/summary
```

### Memory (JSON)

```bash
curl http://localhost:8080/api/metrics/memory
```

### Relays

List relays:

```bash
curl http://localhost:8080/api/relays
```

Add relay:

```bash
curl -X POST http://localhost:8080/api/relays/add \
  -H "Content-Type: application/json" \
  -H "X-Settlement-Token: ${TOKEN}" \
  -d '{"url": "wss://relay.example.com"}'
```

Remove relay:

```bash
curl -X DELETE http://localhost:8080/api/relays/remove \
  -H "Content-Type: application/json" \
  -H "X-Settlement-Token: ${TOKEN}" \
  -d '{"url": "wss://relay.example.com"}'
```

### Bots

Register or upsert a bot:

```bash
curl -X POST http://localhost:8080/api/bots/register \
  -H "Content-Type: application/json" \
  -d '{"bot_pubkey":"<bot_pubkey>","nostr_pubkey":"<nostr_pubkey>","eth_address":"0xabc...","name":"my-bot"}'
```

### Subscriptions

Add or update a subscription (follower shared secret):

```bash
curl -X POST http://localhost:8080/api/subscriptions \
  -H "Content-Type: application/json" \
  -d '{"bot_pubkey":"<bot_pubkey>","follower_pubkey":"<follower_pubkey>","shared_secret":"<shared_secret>"}'
```

List subscriptions for a bot:

```bash
curl http://localhost:8080/api/subscriptions/<bot_pubkey>
```

Notes: subscription POSTs are rate-limited per bot `eth_address` via `[subscriptions].daily_limit` (default 1000; set to 0 to disable). GET is unrestricted. Exceeding the limit returns HTTP 429.

### Trades

Record a trade for later settlement/PnL lookup (usually called by trader after execution):

```bash
curl -X POST http://localhost:8080/api/trades/record \
  -H "Content-Type: application/json" \
  -H "X-Settlement-Token: ${TOKEN}" \
  -d '{"bot_pubkey":"<bot_pubkey>","follower_pubkey":"<follower_pubkey|null>","role":"leader","symbol":"ETH-USDC","side":"buy","size":1.0,"price":2500.0,"tx_hash":"0xdeadbeef"}'
```

Update trade settlement/PnL (requires token if configured):

```bash
curl -X POST http://localhost:8080/api/trades/settlement \
  -H "Content-Type: application/json" \
  -H "X-Settlement-Token: ${TOKEN}" \
  -d '{"tx_hash":"0xdeadbeef","status":"confirmed","pnl":12.3,"pnl_usd":45.6}'
```

### Credits

Query follower credits (filters optional):

```bash
curl "http://localhost:8080/api/credits?bot_pubkey=<bot_pubkey>&follower_pubkey=<follower_pubkey>"
```

Returns an array of `{ bot_pubkey, follower_pubkey, credits }` sorted by credits. Credits are issued by the settlement worker using the `[settlement.credit]` config (leader/follower rates, min_credit, profit_multiplier, enable flag).
