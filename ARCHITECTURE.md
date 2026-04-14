# Architecture

## Goal

`volt-node` exists to let Volt keep a centralized application server while moving verification and history witnessing out to independent operators.

The central server remains the proposer.
Nodes become the verifiers and witnesses.

## Core Model

1. The Volt server publishes the append-only transaction ledger.
2. The Volt server publishes the append-only system event ledger.
3. A node imports both ledgers into its own local SQLite database.
4. The node verifies both hash chains.
5. The node replays balances from the transaction ledger.
6. The node stores checkpoints for the heads it has seen.
7. The node compares its local head to peer node heads.
8. The node exposes its own exports so future nodes can sync from peers instead of only from the server.

## Security Boundary

This package is designed to detect:

- transaction hash-chain tampering
- event hash-chain tampering
- count regressions
- head changes without count growth
- disagreement between peer nodes
- divergence between a server-reported head and a peer-majority head

This package does not yet fully solve:

- malicious history rewrites before any honest node witnessed them
- server abuse that is technically allowed by the current event rules
- prevention or slashing

## Trust Assumptions

The strongest protection appears only when at least one of these is true:

- multiple nodes already witnessed old history
- nodes compare with one another regularly
- nodes keep old checkpoints
- clients prefer node-verified state over raw server state

## Current Consensus Shape

This is not full blockchain consensus.

Today the package implements a lightweight witness-consensus model:

- collect online peer statuses
- group them by the combined transaction/event head
- pick the most common head as the canonical network head
- prefer syncing from sources that match that majority head
- flag disagreement when the local head differs

That is enough to turn a centralized server into a monitored proposer, which is the right intermediate step for Volt.

## Recommended Hardening

The next meaningful upgrades are:

1. Signed checkpoints from known operator keys
2. Signed privileged admin actions inside the event stream
3. Range sync and fork proofs instead of whole-export replacement
4. Peer gossip for discovered heads and checkpoints
5. Client UX that marks state as `verified`, `disputed`, or `isolated`
