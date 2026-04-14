# volt-node

`volt-node` is a standalone verifier node for Volt.

It is designed for Volt's current trust model:

- Volt keeps a canonical execution server
- nodes run in parallel as independent witnesses
- nodes rebuild state from full ledger history
- nodes compare heads and checkpoints with peers
- `VoltScan` shows whether verifier nodes agree with canonical execution

This package is meant to live in its own GitHub repo so you can clone it on another machine and test multi-node behavior without bringing the whole Volt app along.

## What It Does

- mirrors the Volt transaction ledger
- mirrors the Volt system event ledger
- verifies both append-only hash chains locally
- rebuilds ledger-derived balance state locally
- stores checkpoints of observed heads
- exposes a node status endpoint
- exposes local ledger and event exports so peers can sync from each other
- compares peer heads and derives a lightweight peer-majority canonical head
- reports verifier agreement back to Volt when authenticated with a valid node key

## What It Does Not Do Yet

- full p2p gossip transport
- threshold signatures for admin actions
- slashing or staking
- automatic failover of the canonical writer
- client-side enforcement against a disputed server

## Directory Layout

When a node runs, it creates a local `.volt-node/` directory by default:

- `points.db`
- `node-config.json`
- `node-status.json`
- `conflicts.log`
- `node-history.json`
- `checkpoints.json`
- `ledger-export.json`
- `system-events-export.json`
- `backups/points-latest.db`

## Fresh Machine Setup

On the new computer:

```bash
git clone <your-volt-node-repo-url>
cd volt-node
npm install
cp .env.example .env
```

Edit `.env` and set at least:

```bash
VOLT_NODE_BASE_URL=https://your-volt-host.example
VOLT_NODE_STATUS_PORT=8788
```

Then start the node:

```bash
npm run start
```

If `VOLT_NODE_AUTH_KEY` is missing, first boot will prompt you to paste the node key generated from the `VoltScan -> Node Key` tab. `volt-node` will save that key into its local `.env` automatically.

## Multi-Node Test Flow

1. On your main Volt instance, log in and open `VoltScan -> Node Key`.
2. Generate a node key for your operator account.
3. Clone this repo on another machine.
4. Set `VOLT_NODE_BASE_URL` to your Volt server.
5. Run `npm install` and `npm run start`.
6. Paste the node key on first boot.
7. Open `http://127.0.0.1:8788/` on that machine to inspect local node health.
8. Check `VoltScan` on the main server to confirm the node registers, reports, and contributes to weighted confidence.

## Commands

Initialize only:

```bash
npm run init
```

Sync once:

```bash
npm run sync
```

Verify local chain state:

```bash
npm run verify
```

Show current status:

```bash
npm run status
```

Run syntax checks before pushing:

```bash
npm run check
```

## Environment Variables

- `VOLT_NODE_AUTH_KEY`
- `VOLT_NODE_BASE_URL`
- `VOLT_NODE_PRIMARY_STATUS_URL`
- `VOLT_NODE_LEDGER_SOURCE`
- `VOLT_NODE_EVENTS_SOURCE`
- `VOLT_NODE_PEERS`
- `VOLT_NODE_PUBLIC_URL`
- `VOLT_NODE_STATUS_HOST`
- `VOLT_NODE_STATUS_PORT`
- `VOLT_NODE_SYNC_INTERVAL_MS`
- `VOLT_NODE_REPORT_URL`
- `VOLT_NODE_REPORT_KEY`
- `VOLT_NODE_NAME`
- `VOLT_NODE_ID`
- `VOLT_NODE_ROLE`

## Endpoints

When running, a node exposes:

- `GET /`
- `GET /node/status`
- `GET /exports/ledger.json`
- `GET /exports/system-events.json`

That means another node can sync from this node instead of only from the canonical server, and operators also get a local dashboard UI.

## Trust Model

The canonical Volt server is still allowed to propose history.

The node layer exists to:

- detect rewritten history
- detect hash-chain violations
- detect head regressions
- detect disagreement between peers and the primary server
- keep an independently replayed local state copy

Important caveat:

If a brand-new node only ever sees already-rewritten history and has no outside witness or peer comparison, it cannot prove the earlier rewrite happened. Existing peers and checkpoints are what turn this into meaningful protection.

## Recommended Next Steps

- add signed checkpoints from known operator keys
- add explicit admin-action signatures inside system events
- add range sync instead of whole-export sync
- add client-side `verified / disputed` UX
- add peer gossip and fork evidence exchange
