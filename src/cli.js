#!/usr/bin/env node
const fs = require('fs');
const http = require('http');
const path = require('path');
const readline = require('readline/promises');
const SQLite = require('sqlite3').verbose();
const { createLedgerService } = require('./ledgerService');
const { createEventLedgerService } = require('./eventLedgerService');
const { rebuildStateFromLedgers } = require('./stateRecovery');

function stripEnvQuotes(value) {
  const trimmed = String(value || '').trim();
  if (
    (trimmed.startsWith("'") && trimmed.endsWith("'")) ||
    (trimmed.startsWith('"') && trimmed.endsWith('"'))
  ) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) return;
  const content = fs.readFileSync(filePath, 'utf8');
  for (const line of content.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIndex = trimmed.indexOf('=');
    if (eqIndex <= 0) continue;
    const key = trimmed.slice(0, eqIndex).trim();
    const value = stripEnvQuotes(trimmed.slice(eqIndex + 1));
    if (key && typeof process.env[key] === 'undefined') {
      process.env[key] = value;
    }
  }
}

loadEnvFile(path.join(process.cwd(), '.env'));

const HISTORY_WINDOW_MS = 48 * 60 * 60 * 1000;
const DEFAULT_STATUS_PORT = Number(process.env.VOLT_NODE_STATUS_PORT || 8788);
const DEFAULT_STATUS_HOST = String(process.env.VOLT_NODE_STATUS_HOST || '127.0.0.1').trim();
const DEFAULT_NODE_DIR = path.join(process.cwd(), '.volt-node');
const LEGACY_SYNC_INTERVAL_MS = 30000;
const DEFAULT_SYNC_INTERVAL_MS = 2000;
const MIN_SYNC_INTERVAL_MS = 1000;
const DEFAULT_REQUEST_TIMEOUT_MS = 1500;
const DEFAULT_EXPORT_TIMEOUT_MS = 4000;

function resolveNodeDir(inputDir) {
  return path.resolve(inputDir || DEFAULT_NODE_DIR);
}

function getNodePaths(nodeDir) {
  return {
    dir: nodeDir,
    dbPath: path.join(nodeDir, 'points.db'),
    tempDbPath: path.join(nodeDir, 'points.next.db'),
    configPath: path.join(nodeDir, 'node-config.json'),
    statusPath: path.join(nodeDir, 'node-status.json'),
    conflictLogPath: path.join(nodeDir, 'conflicts.log'),
    historyPath: path.join(nodeDir, 'node-history.json'),
    checkpointsPath: path.join(nodeDir, 'checkpoints.json'),
    ledgerExportPath: path.join(nodeDir, 'ledger-export.json'),
    eventsExportPath: path.join(nodeDir, 'system-events-export.json'),
    backupsDir: path.join(nodeDir, 'backups'),
    latestBackupPath: path.join(nodeDir, 'backups', 'points-latest.db'),
  };
}

function rotateLatestFileBackup(sourcePath, backupPath) {
  if (!sourcePath || !backupPath) return;
  ensureDir(path.dirname(backupPath));
  if (!fs.existsSync(sourcePath)) return;

  const tempBackupPath = `${backupPath}\.tmp`;
  fs.copyFileSync(sourcePath, tempBackupPath);
  fs.renameSync(tempBackupPath, backupPath);
}

function rotateLatestBackup(paths) {
  ensureDir(paths.backupsDir);

  if (!fs.existsSync(paths.dbPath)) {
    return;
  }

  const tempBackupPath = `${paths.latestBackupPath}.tmp`;
  fs.copyFileSync(paths.dbPath, tempBackupPath);
  fs.renameSync(tempBackupPath, paths.latestBackupPath);

  for (const name of fs.readdirSync(paths.backupsDir)) {
    if (name === path.basename(paths.latestBackupPath)) continue;
    if (!/^node-.*\.db$/i.test(name)) continue;
    fs.rmSync(path.join(paths.backupsDir, name), { force: true });
  }
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function readJson(filePath, fallback = null) {
  if (!fs.existsSync(filePath)) return fallback;
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJson(filePath, data) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
}

function upsertEnvFileValue(filePath, key, value) {
  const nextLine = `${key}=${value}`;
  let lines = [];

  if (fs.existsSync(filePath)) {
    lines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/);
  }

  let replaced = false;
  lines = lines.map((line) => {
    if (line.startsWith(`${key}=`)) {
      replaced = true;
      return nextLine;
    }
    return line;
  });

  if (!replaced) {
    lines.push(nextLine);
  }

  fs.writeFileSync(
    filePath,
    `${lines.filter((line, index, arr) => !(index === arr.length - 1 && line === '')).join('\n')}\n`,
    'utf8'
  );
}

function appendLine(filePath, line) {
  ensureDir(path.dirname(filePath));
  fs.appendFileSync(filePath, `${line}\n`, 'utf8');
}

function touchFile(filePath) {
  ensureDir(path.dirname(filePath));
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, '', 'utf8');
  }
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function createNodeId(input) {
  return String(input || 'volt-node')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'volt-node';
}

function normalizeBaseUrl(input) {
  const value = String(input || '').trim().replace(/\/+$/, '');
  return value || null;
}

function normalizePositiveInteger(value, fallback) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) {
    return fallback;
  }
  return Math.floor(number);
}

function normalizeSyncIntervalMs(value, fallback = DEFAULT_SYNC_INTERVAL_MS) {
  return Math.max(MIN_SYNC_INTERVAL_MS, normalizePositiveInteger(value, fallback));
}

function normalizePeerUrl(peer) {
  const value = normalizeBaseUrl(peer);
  if (!value) return null;
  if (/\/node\/status$/i.test(value)) return value;
  return `${value}/node/status`;
}

function normalizeExportsBase(peerStatusUrl) {
  const value = String(peerStatusUrl || '').trim().replace(/\/+$/, '');
  return value.replace(/\/node\/status$/i, '');
}

function parsePeerList(value) {
  if (Array.isArray(value)) {
    return value.map((entry) => normalizePeerUrl(entry)).filter(Boolean);
  }

  return String(value || '')
    .split(',')
    .map((entry) => normalizePeerUrl(entry))
    .filter(Boolean);
}

function pruneHistory(entries) {
  const cutoff = Date.now() - HISTORY_WINDOW_MS;
  return (Array.isArray(entries) ? entries : []).filter((entry) => {
    const observedAt = Date.parse(entry?.observedAt || entry?.updatedAt || 0);
    return Number.isFinite(observedAt) && observedAt >= cutoff;
  });
}

function loadHistory(paths) {
  const history = readJson(paths.historyPath, { self: [], peers: {} }) || { self: [], peers: {} };
  history.self = pruneHistory(history.self);
  history.peers = Object.fromEntries(
    Object.entries(history.peers || {}).map(([peerUrl, entries]) => [peerUrl, pruneHistory(entries)])
  );
  return history;
}

function saveHistory(paths, history) {
  writeJson(paths.historyPath, {
    self: pruneHistory(history?.self || []),
    peers: Object.fromEntries(
      Object.entries(history?.peers || {}).map(([peerUrl, entries]) => [peerUrl, pruneHistory(entries)])
    ),
  });
}

function recordSelfHistory(paths, status) {
  const history = loadHistory(paths);
  history.self.push({
    observedAt: new Date().toISOString(),
    status: status?.status || 'unknown',
    running: Boolean(status?.running),
    consensusHealthy: Boolean(status?.consensus?.healthy),
    conflictDetected: Boolean(status?.conflictDetected),
    transactionCount: Number(status?.ledger?.transactionCount || 0),
    eventCount: Number(status?.systemEvents?.eventCount || 0),
    ledgerHash: status?.ledger?.latestHash || null,
    eventHash: status?.systemEvents?.latestHash || null,
  });
  saveHistory(paths, history);
}

function recordPeerHistory(paths, peerUrl, sample) {
  const history = loadHistory(paths);
  history.peers[peerUrl] = pruneHistory([...(history.peers[peerUrl] || []), sample]);
  saveHistory(paths, history);
}

function summarizeHistory(paths) {
  const history = loadHistory(paths);
  const selfEntries = history.self || [];
  const peerEntries = history.peers || {};
  const healthySelfEntries = selfEntries.filter((entry) => entry.status === 'healthy');
  const peerSummaries = Object.entries(peerEntries).map(([peerUrl, entries]) => {
    const latest = entries[entries.length - 1] || null;
    return {
      peerUrl,
      samples48h: entries.length,
      onlineSamples48h: entries.filter((entry) => entry.online).length,
      lastObservedAt: latest?.observedAt || null,
      lastSeenAt: latest?.lastSeenAt || null,
      lastOnline: Boolean(latest?.online),
      latestNodeId: latest?.nodeId || null,
      latestNodeName: latest?.nodeName || null,
    };
  });

  return {
    windowHours: 48,
    self: {
      samples48h: selfEntries.length,
      healthySamples48h: healthySelfEntries.length,
      lastObservedAt: selfEntries[selfEntries.length - 1]?.observedAt || null,
    },
    peers: {
      trackedPeers: peerSummaries.length,
      seenPeers48h: peerSummaries.filter((entry) => entry.onlineSamples48h > 0).length,
      peerSummaries,
    },
  };
}

function buildSelfDescriptor(config, paths) {
  const publicUrl = normalizeBaseUrl(config.publicUrl);
  return {
    nodeId: config.nodeId,
    nodeName: config.nodeName,
    publicUrl,
    statusUrl: publicUrl ? normalizePeerUrl(publicUrl) : null,
    statusPort: Number(config.statusPort || DEFAULT_STATUS_PORT),
    statusHost: config.statusHost || DEFAULT_STATUS_HOST,
    nodeDir: paths.dir,
    role: config.role || 'verifier',
    mode: config.mode || 'parallel-consensus',
  };
}

function buildRuntimeConfigSummary(config) {
  return {
    baseUrl: config.baseUrl || null,
    primaryStatusUrl: config.primaryStatusUrl || null,
    discoveryUrl: config.discoveryUrl || null,
    registerUrl: config.registerUrl || null,
    heartbeatUrl: config.heartbeatUrl || null,
    reportUrl: config.reportUrl || null,
    peers: Array.isArray(config.peers) ? config.peers : [],
    ledgerSource: config.ledgerSource || null,
    eventsSource: config.eventsSource || null,
    syncIntervalMs: normalizeSyncIntervalMs(config.syncIntervalMs, DEFAULT_SYNC_INTERVAL_MS),
    requestTimeoutMs: normalizePositiveInteger(config.requestTimeoutMs, DEFAULT_REQUEST_TIMEOUT_MS),
    exportTimeoutMs: normalizePositiveInteger(config.exportTimeoutMs, DEFAULT_EXPORT_TIMEOUT_MS),
    hasAnyUpstream: Boolean(
      config.baseUrl ||
      (Array.isArray(config.peers) && config.peers.length) ||
      (config.ledgerSource && config.eventsSource)
    ),
  };
}

function setNodeStatus(paths, nextStatus) {
  writeJson(paths.statusPath, {
    updatedAt: new Date().toISOString(),
    ...nextStatus,
  });
  recordSelfHistory(paths, {
    updatedAt: new Date().toISOString(),
    ...nextStatus,
  });
}

function loadCheckpoints(paths) {
  return readJson(paths.checkpointsPath, { entries: [] }) || { entries: [] };
}

function saveCheckpoint(paths, checkpoint) {
  const existing = loadCheckpoints(paths);
  const entries = [...(existing.entries || []), checkpoint].slice(-500);
  writeJson(paths.checkpointsPath, { entries });
}

async function fetchWithTimeout(url, options = {}) {
  const { timeoutMs: rawTimeoutMs, ...fetchOptions } = options;
  const timeoutMs = normalizePositiveInteger(rawTimeoutMs, DEFAULT_REQUEST_TIMEOUT_MS);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, {
      ...fetchOptions,
      signal: controller.signal,
    });
  } catch (error) {
    if (error?.name === 'AbortError') {
      throw new Error(`Request timed out after ${timeoutMs}ms`);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchJson(url, options = {}) {
  const response = await fetchWithTimeout(url, {
    ...options,
    headers: {
      accept: 'application/json',
      ...(options.headers || {}),
    },
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status} ${response.statusText}`);
  }

  return response.json();
}

function deriveHeadKey(status) {
  const ledger = status?.ledger || {};
  const systemEvents = status?.systemEvents || {};
  if (!ledger.latestHash || !systemEvents.latestHash) return null;

  return [
    Number(ledger.transactionCount || 0),
    ledger.latestHash,
    Number(systemEvents.eventCount || 0),
    systemEvents.latestHash,
  ].join(':');
}

function buildConflictSummary(previousStatus, ledgerIntegrity, eventIntegrity) {
  const issues = [];
  const previousLedger = previousStatus?.ledger || null;
  const previousEvents = previousStatus?.systemEvents || null;

  if (!ledgerIntegrity.valid) {
    issues.push(`Ledger integrity failed: ${ledgerIntegrity.reason || 'unknown reason'}`);
  }
  if (!eventIntegrity.valid) {
    issues.push(`System event integrity failed: ${eventIntegrity.reason || 'unknown reason'}`);
  }

  if (previousLedger?.transactionCount && ledgerIntegrity.transactionCount < previousLedger.transactionCount) {
    issues.push(
      `Ledger transaction count regressed from ${previousLedger.transactionCount} to ${ledgerIntegrity.transactionCount}.`
    );
  }
  if (previousEvents?.eventCount && eventIntegrity.eventCount < previousEvents.eventCount) {
    issues.push(
      `System event count regressed from ${previousEvents.eventCount} to ${eventIntegrity.eventCount}.`
    );
  }

  if (
    previousLedger?.transactionCount === ledgerIntegrity.transactionCount &&
    previousLedger?.latestHash &&
    previousLedger.latestHash !== ledgerIntegrity.latestHash
  ) {
    issues.push('Ledger head hash changed without a transaction count increase.');
  }

  if (
    previousEvents?.eventCount === eventIntegrity.eventCount &&
    previousEvents?.latestHash &&
    previousEvents.latestHash !== eventIntegrity.latestHash
  ) {
    issues.push('System event head hash changed without an event count increase.');
  }

  return issues;
}

function buildProjectionCountDiff(localCounts, peerCounts) {
  const keys = Array.from(new Set([
    ...Object.keys(localCounts || {}),
    ...Object.keys(peerCounts || {}),
  ])).sort();
  return keys
    .map((key) => {
      const localValue = Number(localCounts?.[key] || 0);
      const peerValue = Number(peerCounts?.[key] || 0);
      if (localValue === peerValue) return null;
      return `${key}: local=${localValue}, peer=${peerValue}`;
    })
    .filter(Boolean);
}

function buildPeerDisagreement(localStatus, peerStatus, peerUrl) {
  const issues = [];
  const localLedger = localStatus?.ledger || {};
  const peerLedger = peerStatus?.ledger || {};
  const localEvents = localStatus?.systemEvents || {};
  const peerEvents = peerStatus?.systemEvents || {};

  if (peerStatus?.status === 'error') {
    issues.push(`${peerUrl} reports status=error`);
  }
  if (peerLedger.valid === false) {
    issues.push(`${peerUrl} reports invalid ledger integrity`);
  }
  if (peerEvents.valid === false) {
    issues.push(`${peerUrl} reports invalid system event integrity`);
  }
  if (localLedger.latestHash && peerLedger.latestHash && localLedger.latestHash !== peerLedger.latestHash) {
    issues.push(`${peerUrl} ledger head differs from local head`);
  }
  if (localEvents.latestHash && peerEvents.latestHash && localEvents.latestHash !== peerEvents.latestHash) {
    issues.push(`${peerUrl} system event head differs from local head`);
  }
  if (
    localStatus?.projection?.fingerprint &&
    peerStatus?.projection?.fingerprint &&
    localStatus.projection.fingerprint !== peerStatus.projection.fingerprint
  ) {
    const localFingerprint = String(localStatus?.projection?.fingerprint || '').slice(0, 16);
    const peerFingerprint = String(peerStatus?.projection?.fingerprint || '').slice(0, 16);
    const countDiff = buildProjectionCountDiff(localStatus?.projection?.counts, peerStatus?.projection?.counts);
    const detail = countDiff.length ? ` • count deltas: ${countDiff.join('; ')}` : '';
    issues.push(
      `${peerUrl} projected state differs from local state • local=${localFingerprint} • peer=${peerFingerprint}${detail}`
    );
  }

  return issues;
}

async function fetchPeerStatus(peerUrl, config = {}) {
  const observedAt = new Date().toISOString();
  try {
    const payload = await fetchJson(peerUrl, {
      timeoutMs: normalizePositiveInteger(config.requestTimeoutMs, DEFAULT_REQUEST_TIMEOUT_MS),
    });
    return {
      peerUrl,
      observedAt,
      online: true,
      payload,
      lastSeenAt: payload?.updatedAt || observedAt,
      baseUrl: normalizeExportsBase(peerUrl),
    };
  } catch (error) {
    return {
      peerUrl,
      observedAt,
      online: false,
      error: error.message || String(error),
      lastSeenAt: null,
      baseUrl: normalizeExportsBase(peerUrl),
    };
  }
}

async function fetchDiscoveredPeers(config) {
  const discoveryUrl = String(config.discoveryUrl || '').trim();
  if (!discoveryUrl) {
    return {
      checkedAt: new Date().toISOString(),
      source: null,
      peers: [],
    };
  }

  try {
    const payload = await fetchJson(discoveryUrl, {
      timeoutMs: normalizePositiveInteger(config.requestTimeoutMs, DEFAULT_REQUEST_TIMEOUT_MS),
    });
    const nodes = Array.isArray(payload?.nodes) ? payload.nodes : [];
    const seedStatusUrls = Array.isArray(payload?.seedStatusUrls) ? payload.seedStatusUrls : [];
    const selfStatusUrl = config.publicUrl ? normalizePeerUrl(config.publicUrl) : null;
    const peers = Array.from(new Set([
      ...seedStatusUrls.map((entry) => normalizePeerUrl(entry)).filter(Boolean),
      ...nodes
        .map((node) => normalizePeerUrl(node?.statusUrl || node?.nodeUrl || ''))
        .filter(Boolean),
    ])).filter((peerUrl) => !selfStatusUrl || peerUrl !== selfStatusUrl);

    return {
      checkedAt: new Date().toISOString(),
      source: discoveryUrl,
      peers,
    };
  } catch (error) {
    return {
      checkedAt: new Date().toISOString(),
      source: discoveryUrl,
      error: error.message || String(error),
      peers: [],
    };
  }
}

function resolveExportUrlsFromBase(baseUrl) {
  const cleanBase = normalizeBaseUrl(baseUrl);
  if (!cleanBase) return null;
  return {
    ledgerUrl: `${cleanBase}/exports/ledger.json`,
    eventsUrl: `${cleanBase}/exports/system-events.json`,
  };
}

function loadNodeConfig(paths) {
  const saved = readJson(paths.configPath, {}) || {};
  const baseUrl = normalizeBaseUrl(process.env.VOLT_NODE_BASE_URL || saved.baseUrl || '');
  const explicitLedgerSource = String(process.env.VOLT_NODE_LEDGER_SOURCE || saved.ledgerSource || '').trim() || null;
  const explicitEventsSource = String(process.env.VOLT_NODE_EVENTS_SOURCE || saved.eventsSource || '').trim() || null;
  const primaryStatusUrl = normalizePeerUrl(process.env.VOLT_NODE_PRIMARY_STATUS_URL || saved.primaryStatusUrl || baseUrl || '');
  const publicUrl = normalizeBaseUrl(process.env.VOLT_NODE_PUBLIC_URL || saved.publicUrl || '');
  const discoveryUrl = String(
    process.env.VOLT_NODE_DISCOVERY_URL || saved.discoveryUrl || (baseUrl ? `${baseUrl}/api/nodes` : '')
  ).trim() || null;
  const registerUrl = String(
    process.env.VOLT_NODE_REGISTER_URL || saved.registerUrl || (baseUrl ? `${baseUrl}/api/nodes/register` : '')
  ).trim() || null;
  const heartbeatUrl = String(
    process.env.VOLT_NODE_HEARTBEAT_URL || saved.heartbeatUrl || (baseUrl ? `${baseUrl}/api/nodes/heartbeat` : '')
  ).trim() || null;
  const nodeName = String(process.env.VOLT_NODE_NAME || saved.nodeName || path.basename(paths.dir) || 'Volt Node').trim();
  const nodeId = createNodeId(process.env.VOLT_NODE_ID || saved.nodeId || nodeName || paths.dir);
  const reportUrl = String(process.env.VOLT_NODE_REPORT_URL || saved.reportUrl || (baseUrl ? `${baseUrl}/api/consensus/report` : '')).trim() || null;
  const reportKey = String(process.env.VOLT_NODE_REPORT_KEY || saved.reportKey || '').trim() || null;
  const nodeAuthKey = String(process.env.VOLT_NODE_AUTH_KEY || saved.nodeAuthKey || '').trim() || null;
  const operatorDiscordId = String(process.env.VOLT_NODE_OPERATOR_DISCORD_ID || saved.operatorDiscordId || '').trim() || null;
  const peers = process.env.VOLT_NODE_PEERS
    ? parsePeerList(process.env.VOLT_NODE_PEERS)
    : parsePeerList(saved.peers || []);

  const savedSyncIntervalMs = normalizePositiveInteger(saved.syncIntervalMs, null);
  const hasExplicitSyncInterval = typeof process.env.VOLT_NODE_SYNC_INTERVAL_MS !== 'undefined';
  const syncIntervalSeed = hasExplicitSyncInterval
    ? process.env.VOLT_NODE_SYNC_INTERVAL_MS
    : (
        savedSyncIntervalMs === null || savedSyncIntervalMs === LEGACY_SYNC_INTERVAL_MS
          ? DEFAULT_SYNC_INTERVAL_MS
          : savedSyncIntervalMs
      );

  return {
    ...saved,
    createdAt: saved.createdAt || new Date().toISOString(),
    nodeId,
    nodeName,
    role: process.env.VOLT_NODE_ROLE || saved.role || 'verifier',
    mode: 'parallel-consensus',
    dbPath: paths.dbPath,
    ledgerExportPath: paths.ledgerExportPath,
    eventsExportPath: paths.eventsExportPath,
    baseUrl,
    publicUrl,
    primaryStatusUrl,
    discoveryUrl,
    registerUrl,
    heartbeatUrl,
    reportUrl,
    reportKey,
    nodeAuthKey,
    operatorDiscordId,
    statusHost: String(process.env.VOLT_NODE_STATUS_HOST || saved.statusHost || DEFAULT_STATUS_HOST),
    statusPort: Number(process.env.VOLT_NODE_STATUS_PORT || saved.statusPort || DEFAULT_STATUS_PORT),
    peers,
    ledgerSource: explicitLedgerSource,
    eventsSource: explicitEventsSource,
    syncIntervalMs: normalizeSyncIntervalMs(syncIntervalSeed, DEFAULT_SYNC_INTERVAL_MS),
    requestTimeoutMs: normalizePositiveInteger(
      process.env.VOLT_NODE_REQUEST_TIMEOUT_MS || saved.requestTimeoutMs,
      DEFAULT_REQUEST_TIMEOUT_MS
    ),
    exportTimeoutMs: normalizePositiveInteger(
      process.env.VOLT_NODE_EXPORT_TIMEOUT_MS || saved.exportTimeoutMs,
      DEFAULT_EXPORT_TIMEOUT_MS
    ),
    notes: 'Standalone verifier node for Volt. Prefers peer-served history and falls back to the execution server when needed.',
  };
}

function saveNodeConfig(paths, nextConfig) {
  const sanitizedConfig = { ...(nextConfig || {}) };
  delete sanitizedConfig.nodeAuthKey;
  writeJson(paths.configPath, sanitizedConfig);
}

function printHelp() {
  console.log([
    'volt-node',
    '',
    'Usage:',
    '  node src/cli.js init [dir]',
    '  node src/cli.js sync [dir]',
    '  node src/cli.js start [dir]',
    '  node src/cli.js verify [dir]',
    '  node src/cli.js status [dir]',
    '',
    'Environment:',
    '  VOLT_NODE_BASE_URL',
    '  VOLT_NODE_PRIMARY_STATUS_URL',
    '  VOLT_NODE_REPORT_URL',
    '  VOLT_NODE_REPORT_KEY',
    '  VOLT_NODE_PEERS',
    '  VOLT_NODE_PUBLIC_URL',
    '  VOLT_NODE_SYNC_INTERVAL_MS',
    '  VOLT_NODE_REQUEST_TIMEOUT_MS',
    '  VOLT_NODE_EXPORT_TIMEOUT_MS',
  ].join('\n'));
}

function createDatabase(dbPath) {
  return new Promise((resolve, reject) => {
    const db = new SQLite.Database(dbPath, (error) => {
      if (error) return reject(error);
      db.configure('busyTimeout', 5000);
      db.run('PRAGMA journal_mode = WAL');
      resolve(db);
    });
  });
}

async function openNodeServices(dbPath) {
  const db = await createDatabase(dbPath);
  const ledgerService = createLedgerService({
    db,
    ensureUserEconomyRow: async () => {},
  });
  const eventLedgerService = createEventLedgerService({ db });

  await ledgerService.initialize();
  await eventLedgerService.initialize();

  return { db, ledgerService, eventLedgerService };
}

async function closeDb(dbOrServices) {
  const db = dbOrServices?.db || dbOrServices;
  if (!db) return;
  await new Promise((resolve) => {
    db.close(() => resolve());
  });
}

async function commandInit(dirArg) {
  const nodeDir = resolveNodeDir(dirArg);
  const paths = getNodePaths(nodeDir);
  ensureDir(nodeDir);
  ensureDir(paths.backupsDir);
  touchFile(paths.conflictLogPath);
  saveNodeConfig(paths, loadNodeConfig(paths));

  const services = await openNodeServices(paths.dbPath);
  try {
    console.log(`Initialized volt-node at ${nodeDir}`);
    console.log(`Database: ${paths.dbPath}`);
    console.log(`Config: ${paths.configPath}`);
  } finally {
    await closeDb(services);
  }
}

async function resolveSourceToFile(source, fallbackFilePath, options = {}) {
  const value = String(source || '').trim();
  if (!value) {
    throw new Error(`Missing source for ${fallbackFilePath}`);
  }

  if (/^https?:\/\//i.test(value)) {
    const response = await fetchWithTimeout(value, {
      timeoutMs: normalizePositiveInteger(options.timeoutMs, DEFAULT_EXPORT_TIMEOUT_MS),
    });
    if (!response.ok) {
      throw new Error(`Failed fetching ${value}: ${response.status} ${response.statusText}`);
    }
    const text = await response.text();
    fs.writeFileSync(fallbackFilePath, text, 'utf8');
    return fallbackFilePath;
  }

  const resolvedPath = path.resolve(value);
  if (!fs.existsSync(resolvedPath)) {
    throw new Error(`Source file not found: ${resolvedPath}`);
  }
  fs.copyFileSync(resolvedPath, fallbackFilePath);
  return fallbackFilePath;
}

async function fetchConsensusCandidates(config) {
  const discoveredPeers = await fetchDiscoveredPeers(config);
  const peers = Array.from(new Set([
    ...(config.primaryStatusUrl ? [normalizePeerUrl(config.primaryStatusUrl)] : []),
    ...(discoveredPeers.peers || []),
    ...parsePeerList(config.peers),
  ].filter(Boolean)));

  const results = await Promise.all(peers.map((peerUrl) => fetchPeerStatus(peerUrl, config)));
  const online = results.filter((entry) => entry.online && deriveHeadKey(entry.payload));
  const byHead = new Map();

  for (const result of online) {
    const headKey = deriveHeadKey(result.payload);
    if (!byHead.has(headKey)) {
      byHead.set(headKey, []);
    }
    byHead.get(headKey).push(result);
  }

  const rankedHeads = [...byHead.entries()]
    .map(([headKey, entries]) => ({ headKey, count: entries.length, entries }))
    .sort((left, right) => right.count - left.count);

  const canonical = rankedHeads[0] || null;
  return {
    checkedAt: new Date().toISOString(),
    discovery: discoveredPeers,
    peers: results,
    online,
    rankedHeads,
    canonical,
  };
}

function buildSyncTargets(config, consensusCandidates) {
  const targets = [];
  const seen = new Set();

  function addTarget(type, label, ledgerSource, eventsSource, matchHeadKey = null) {
    if (!ledgerSource || !eventsSource) return;
    const key = `${type}:${ledgerSource}:${eventsSource}:${matchHeadKey || ''}`;
    if (seen.has(key)) return;
    seen.add(key);
    targets.push({ type, label, ledgerSource, eventsSource, matchHeadKey });
  }

  const canonical = consensusCandidates?.canonical;
  if (canonical) {
    for (const entry of canonical.entries) {
      const exportsBase = entry.baseUrl;
      const urls = resolveExportUrlsFromBase(exportsBase);
      if (!urls) continue;
      addTarget('peer-majority', exportsBase, urls.ledgerUrl, urls.eventsUrl, canonical.headKey);
    }
  }

  for (const peer of consensusCandidates?.online || []) {
    const urls = resolveExportUrlsFromBase(peer.baseUrl);
    if (!urls) continue;
    addTarget('peer', peer.baseUrl, urls.ledgerUrl, urls.eventsUrl, deriveHeadKey(peer.payload));
  }

  if (config.ledgerSource && config.eventsSource) {
    addTarget('configured', 'configured-source', config.ledgerSource, config.eventsSource);
  }

  if (config.baseUrl) {
    addTarget(
      'execution-fallback',
      'execution-server-fallback',
      `${config.baseUrl}/exports/ledger.json`,
      `${config.baseUrl}/exports/system-events.json`
    );
  }

  return targets;
}

async function importIntoTempDb(paths, ledgerSource, eventsSource, options = {}) {
  await Promise.all([
    resolveSourceToFile(ledgerSource, paths.ledgerExportPath, options),
    resolveSourceToFile(eventsSource, paths.eventsExportPath, options),
  ]);

  if (fs.existsSync(paths.tempDbPath)) {
    fs.rmSync(paths.tempDbPath, { force: true });
  }

  const ledgerPayload = JSON.parse(fs.readFileSync(paths.ledgerExportPath, 'utf8'));
  const eventPayload = JSON.parse(fs.readFileSync(paths.eventsExportPath, 'utf8'));
  const rebuilt = await rebuildStateFromLedgers({
    targetPath: paths.tempDbPath,
    ledgerPayload,
    eventPayload,
    overwrite: true,
  });

  const services = await openNodeServices(paths.tempDbPath);
  try {
    const [ledgerIntegrity, eventIntegrity, replay] = await Promise.all([
      services.ledgerService.verifyLedgerIntegrity(),
      services.eventLedgerService.verifyIntegrity(),
      services.ledgerService.replayLedger(),
    ]);

    return {
      services,
      ledgerResult: { imported: rebuilt.transactionCount },
      eventResult: { imported: rebuilt.systemEventCount },
      ledgerIntegrity,
      eventIntegrity,
      replay,
      projectedMirror: rebuilt,
    };
  } catch (error) {
    await closeDb(services);
    throw error;
  }
}

async function syncNode(dirArg, overrides = {}) {
  const nodeDir = resolveNodeDir(dirArg);
  const paths = getNodePaths(nodeDir);
  ensureDir(nodeDir);
  ensureDir(paths.backupsDir);

  const config = {
    ...loadNodeConfig(paths),
    ...(overrides.config || {}),
  };
  saveNodeConfig(paths, config);

  const previousStatus = readJson(paths.statusPath, null);
  const consensusCandidates = await fetchConsensusCandidates(config).catch(() => ({
    checkedAt: new Date().toISOString(),
    peers: [],
    online: [],
    rankedHeads: [],
    canonical: null,
  }));
  const syncTargets = buildSyncTargets(config, consensusCandidates);

  let successfulTarget = null;
  let imported = null;
  let lastError = null;

  for (const target of syncTargets) {
    try {
      imported = await importIntoTempDb(paths, target.ledgerSource, target.eventsSource, {
        timeoutMs: normalizePositiveInteger(config.exportTimeoutMs, DEFAULT_EXPORT_TIMEOUT_MS),
      });
      successfulTarget = target;
      break;
    } catch (error) {
      lastError = error;
    }
  }

  if (!successfulTarget || !imported) {
    throw lastError || new Error(
      'No valid sync source was available. Configure peer nodes with VOLT_NODE_PEERS or set VOLT_NODE_BASE_URL for execution-layer fallback.'
    );
  }

  const { services, ledgerResult, eventResult, ledgerIntegrity, eventIntegrity, replay, projectedMirror } = imported;
  try {
    const conflicts = buildConflictSummary(previousStatus, ledgerIntegrity, eventIntegrity);
    const localHeadKey = deriveHeadKey({
      ledger: ledgerIntegrity,
      systemEvents: eventIntegrity,
    });
    const canonicalHeadKey = consensusCandidates?.canonical?.headKey || null;

    if (canonicalHeadKey && localHeadKey && canonicalHeadKey !== localHeadKey) {
      conflicts.push('Local imported head does not match the peer-majority head.');
    }

    const trustedBy = (consensusCandidates?.canonical?.entries || []).map((entry) => entry.peerUrl);
    const checkpoint = {
      observedAt: new Date().toISOString(),
      localHeadKey,
      canonicalHeadKey,
      sourceLabel: successfulTarget.label,
      transactionCount: ledgerIntegrity.transactionCount,
      eventCount: eventIntegrity.eventCount,
      ledgerHash: ledgerIntegrity.latestHash,
      eventHash: eventIntegrity.latestHash,
      trustedBy,
    };

    await closeDb(services);

    if (fs.existsSync(paths.dbPath)) {
      rotateLatestBackup(paths);
      fs.rmSync(paths.dbPath, { force: true });
    }

    fs.renameSync(paths.tempDbPath, paths.dbPath);
    saveCheckpoint(paths, checkpoint);
    rotateLatestFileBackup(paths.dbPath, paths.latestBackupPath);

    const syncedAt = new Date().toISOString();
    saveNodeConfig(paths, {
      ...config,
      lastSyncedAt: syncedAt,
      latestTransactionHash: ledgerIntegrity.latestHash,
      latestEventHash: eventIntegrity.latestHash,
      transactionCount: ledgerIntegrity.transactionCount,
      eventCount: eventIntegrity.eventCount,
      lastSyncSource: successfulTarget.label,
    });

    const consensus = {
      checkedAt: consensusCandidates.checkedAt,
      canonicalHeadKey,
      localHeadKey,
      healthy: (!canonicalHeadKey || canonicalHeadKey === localHeadKey),
      witnessCount: Number(consensusCandidates?.canonical?.count || 0),
      trustedBy,
      syncSource: successfulTarget.label,
    };

    const status = {
      ...buildSelfDescriptor(config, paths),
      nodeDir,
      running: false,
      status: conflicts.length ? 'attention' : 'healthy',
      dbPath: paths.dbPath,
      lastSyncedAt: syncedAt,
      conflictDetected: conflicts.length > 0,
      conflicts,
      history: summarizeHistory(paths),
      consensus,
      reporting: previousStatus?.reporting || {
        reportUrl: config.reportUrl || null,
        lastReportAt: null,
        lastReportStatus: 'idle',
        lastReportError: null,
      },
      projection: {
        dbPath: paths.dbPath,
        backupPath: paths.latestBackupPath,
        fingerprint: projectedMirror.projectionFingerprint,
        counts: projectedMirror.projectionCounts,
        accountCount: Object.keys(replay.users || {}).length,
        richestAccounts: Object.entries(replay.users || {})
          .map(([userId, balances]) => ({ userId, total: Number(balances.total || 0) }))
          .sort((left, right) => right.total - left.total || left.userId.localeCompare(right.userId))
          .slice(0, 10),
      },
      ledger: {
        transactionCount: ledgerIntegrity.transactionCount,
        latestHash: ledgerIntegrity.latestHash,
        valid: ledgerIntegrity.valid,
      },
      systemEvents: {
        eventCount: eventIntegrity.eventCount,
        latestHash: eventIntegrity.latestHash,
        valid: eventIntegrity.valid,
      },
    };
    setNodeStatus(paths, status);

    if (conflicts.length) {
      appendLine(paths.conflictLogPath, `[${syncedAt}] ${conflicts.join(' | ')}`);
    }

    return {
      nodeDir,
      syncSource: successfulTarget,
      ledgerResult,
      eventResult,
      ledgerIntegrity,
      eventIntegrity,
      consensus,
      projection: {
        dbPath: paths.dbPath,
        backupPath: paths.latestBackupPath,
        fingerprint: projectedMirror.projectionFingerprint,
        counts: projectedMirror.projectionCounts,
      },
      conflicts,
    };
  } finally {
    if (fs.existsSync(paths.tempDbPath)) {
      try {
        await closeDb(services);
      } catch (error) {
        // ignore close errors
      }
      fs.rmSync(paths.tempDbPath, { force: true });
    }
  }
}

async function commandSync(dirArg) {
  const result = await syncNode(dirArg);
  console.log(`Synced volt-node at ${result.nodeDir}`);
  console.log(`Source: ${result.syncSource.label}`);
  console.log(`Transactions imported: ${result.ledgerResult.imported}`);
  console.log(`System events imported: ${result.eventResult.imported}`);
  console.log(`Ledger verified: ${result.ledgerIntegrity.valid}`);
  console.log(`Events verified: ${result.eventIntegrity.valid}`);
  console.log(`Consensus healthy: ${result.consensus.healthy}`);
  if (result.conflicts.length) {
    console.log(`Conflicts detected: ${result.conflicts.join(' | ')}`);
  }
}

async function commandVerify(dirArg) {
  const nodeDir = resolveNodeDir(dirArg);
  const paths = getNodePaths(nodeDir);
  const services = await openNodeServices(paths.dbPath);

  try {
    const [ledgerIntegrity, eventIntegrity, replay] = await Promise.all([
      services.ledgerService.verifyLedgerIntegrity(),
      services.eventLedgerService.verifyIntegrity(),
      services.ledgerService.replayLedger(),
    ]);

    console.log(JSON.stringify({
      nodeDir,
      dbPath: paths.dbPath,
      ledger: ledgerIntegrity,
      systemEvents: eventIntegrity,
      projection: {
        accountCount: Object.keys(replay.users || {}).length,
      },
    }, null, 2));
  } finally {
    await closeDb(services);
  }
}

async function commandStatus(dirArg) {
  const nodeDir = resolveNodeDir(dirArg);
  const paths = getNodePaths(nodeDir);
  const config = readJson(paths.configPath, {});

  if (!fs.existsSync(paths.dbPath)) {
    console.log(JSON.stringify({
      ...buildSelfDescriptor(config, paths),
      nodeDir,
      initialized: fs.existsSync(paths.configPath),
      synced: false,
      dbPath: paths.dbPath,
      statusPath: paths.statusPath,
      message: 'Node database not found. Run `npm run sync` after init.',
    }, null, 2));
    return;
  }

  const savedStatus = readJson(paths.statusPath, {});
  const services = await openNodeServices(paths.dbPath);
  try {
    const [ledgerIntegrity, eventIntegrity] = await Promise.all([
      services.ledgerService.verifyLedgerIntegrity(),
      services.eventLedgerService.verifyIntegrity(),
    ]);

    console.log(JSON.stringify({
      ...buildSelfDescriptor(config, paths),
      nodeDir,
      running: Boolean(savedStatus?.running),
      status: savedStatus?.status || 'unknown',
      dbPath: paths.dbPath,
      lastSyncedAt: config.lastSyncedAt || null,
      lastErrorAt: savedStatus?.lastErrorAt || null,
      lastError: savedStatus?.lastError || null,
      statusPath: paths.statusPath,
      conflictLogPath: paths.conflictLogPath,
      checkpointsPath: paths.checkpointsPath,
      conflictDetected: Boolean(savedStatus?.conflictDetected),
      conflicts: savedStatus?.conflicts || [],
      consensus: savedStatus?.consensus || null,
      reporting: savedStatus?.reporting || null,
      history: summarizeHistory(paths),
      peerNetwork: savedStatus?.peerNetwork || {
        checkedAt: null,
        totalPeers: 0,
        onlinePeers: 0,
        inSyncPeers: 0,
        disagreeingPeers: 0,
        peers: [],
      },
      ledger: {
        transactionCount: ledgerIntegrity.transactionCount,
        latestHash: ledgerIntegrity.latestHash,
        valid: ledgerIntegrity.valid,
      },
      systemEvents: {
        eventCount: eventIntegrity.eventCount,
        latestHash: eventIntegrity.latestHash,
        valid: eventIntegrity.valid,
      },
    }, null, 2));
  } finally {
    await closeDb(services);
  }
}

async function refreshPeerNetwork(paths, config) {
  const currentStatus = readJson(paths.statusPath, {}) || {};
  const discoveredPeers = await fetchDiscoveredPeers(config);
  const peerUrls = Array.from(new Set([
    ...(config.primaryStatusUrl ? [normalizePeerUrl(config.primaryStatusUrl)] : []),
    ...(discoveredPeers.peers || []),
    ...parsePeerList(config.peers),
  ].filter(Boolean)));

  if (!peerUrls.length) {
    const peerNetwork = {
      checkedAt: new Date().toISOString(),
      discovery: discoveredPeers,
      totalPeers: 0,
      onlinePeers: 0,
      offlinePeers: 0,
      inSyncPeers: 0,
      disagreeingPeers: 0,
      peers: [],
    };
    setNodeStatus(paths, {
      ...currentStatus,
      ...buildSelfDescriptor(config, paths),
      peerNetwork,
      history: summarizeHistory(paths),
    });
    return peerNetwork;
  }

  const peerResults = await Promise.all(peerUrls.map((peerUrl) => fetchPeerStatus(peerUrl, config)));
  const checkedAt = new Date().toISOString();
  const peers = peerResults.map((result) => {
    if (!result.online) {
      recordPeerHistory(paths, result.peerUrl, result);
      return {
        peerUrl: result.peerUrl,
        online: false,
        inSync: false,
        status: 'offline',
        lastSeenAt: null,
        error: result.error,
      };
    }

    const peerStatus = result.payload || {};
    const peerIssues = buildPeerDisagreement(currentStatus, peerStatus, result.peerUrl);
    const peerEntry = {
      peerUrl: result.peerUrl,
      online: true,
      inSync: peerIssues.length === 0,
      status: peerStatus?.status || 'unknown',
      nodeId: peerStatus?.nodeId || null,
      nodeName: peerStatus?.nodeName || null,
      nodeUrl: result.baseUrl || null,
      lastSeenAt: result.lastSeenAt,
      updatedAt: peerStatus?.updatedAt || result.observedAt,
      ledger: peerStatus?.ledger || null,
      systemEvents: peerStatus?.systemEvents || null,
      conflicts: peerIssues,
    };
    recordPeerHistory(paths, result.peerUrl, {
      observedAt: result.observedAt,
      online: true,
      lastSeenAt: result.lastSeenAt,
      nodeId: peerEntry.nodeId,
      nodeName: peerEntry.nodeName,
      status: peerEntry.status,
      inSync: peerEntry.inSync,
      ledgerHash: peerEntry?.ledger?.latestHash || null,
      eventHash: peerEntry?.systemEvents?.latestHash || null,
    });
    return peerEntry;
  });

  const peerConflicts = peers.flatMap((peer) => peer.conflicts || []);
  if (peerConflicts.length) {
    appendLine(paths.conflictLogPath, `[${checkedAt}] ${peerConflicts.join(' | ')}`);
  }

  const peerNetwork = {
    checkedAt,
    discovery: discoveredPeers,
    totalPeers: peers.length,
    onlinePeers: peers.filter((peer) => peer.online).length,
    offlinePeers: peers.filter((peer) => !peer.online).length,
    inSyncPeers: peers.filter((peer) => peer.online && peer.inSync).length,
    disagreeingPeers: peers.filter((peer) => peer.online && !peer.inSync).length,
    peers,
  };

  setNodeStatus(paths, {
    ...currentStatus,
    ...buildSelfDescriptor(config, paths),
    conflictDetected: Boolean(currentStatus?.conflictDetected || peerConflicts.length),
    conflicts: [...new Set([...(currentStatus?.conflicts || []), ...peerConflicts])].slice(-20),
    peerNetwork,
    history: summarizeHistory(paths),
  });

  return peerNetwork;
}

async function pushConsensusReport(paths, config) {
  const reportUrl = String(config.reportUrl || '').trim();
  if (!reportUrl) {
    return { status: 'disabled', error: null };
  }

  const currentStatus = readJson(paths.statusPath, {}) || {};
  const payload = {
    nodeId: currentStatus.nodeId || config.nodeId,
    nodeName: currentStatus.nodeName || config.nodeName,
    nodeUrl: config.publicUrl || null,
    statusUrl: currentStatus.statusUrl || (config.publicUrl ? normalizePeerUrl(config.publicUrl) : null),
    updatedAt: currentStatus.updatedAt || new Date().toISOString(),
    status: currentStatus.status || 'unknown',
    lastError: currentStatus.lastError || null,
    conflicts: currentStatus.conflicts || [],
    consensus: currentStatus.consensus || null,
    ledger: currentStatus.ledger || null,
    systemEvents: currentStatus.systemEvents || null,
    projection: currentStatus.projection || null,
  };

  try {
    const response = await fetchWithTimeout(reportUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(config.nodeAuthKey ? { 'x-volt-node-key': config.nodeAuthKey } : {}),
        ...(config.reportKey ? { 'x-volt-consensus-key': config.reportKey } : {}),
      },
      body: JSON.stringify(payload),
      timeoutMs: normalizePositiveInteger(config.requestTimeoutMs, DEFAULT_REQUEST_TIMEOUT_MS),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const nextStatus = readJson(paths.statusPath, {}) || {};
    setNodeStatus(paths, {
      ...nextStatus,
      reporting: {
        reportUrl,
        lastReportAt: new Date().toISOString(),
        lastReportStatus: 'ok',
        lastReportError: null,
      },
    });

    return { status: 'ok', error: null };
  } catch (error) {
    const message = error.message || String(error);
    const nextStatus = readJson(paths.statusPath, {}) || {};
    setNodeStatus(paths, {
      ...nextStatus,
      reporting: {
        reportUrl,
        lastReportAt: new Date().toISOString(),
        lastReportStatus: 'error',
        lastReportError: message,
      },
    });
    return { status: 'error', error: message };
  }
}

async function pushNodePresence(paths, config, mode = 'heartbeat') {
  const targetUrl = String(
    mode === 'register'
      ? (config.registerUrl || '')
      : (config.heartbeatUrl || config.registerUrl || '')
  ).trim();

  if (!targetUrl) {
    return { status: 'disabled', error: null };
  }

  const currentStatus = readJson(paths.statusPath, {}) || {};
  const payload = {
    nodeId: currentStatus.nodeId || config.nodeId,
    nodeName: currentStatus.nodeName || config.nodeName,
    nodeUrl: config.publicUrl || null,
    statusUrl: currentStatus.statusUrl || (config.publicUrl ? normalizePeerUrl(config.publicUrl) : null),
    updatedAt: currentStatus.updatedAt || new Date().toISOString(),
    status: currentStatus.status || 'unknown',
    role: currentStatus.role || config.role || 'verifier',
    mode: currentStatus.mode || config.mode || 'parallel-consensus',
    softwareVersion: '0.1.0',
    lastError: currentStatus.lastError || null,
    conflicts: currentStatus.conflicts || [],
    consensus: currentStatus.consensus || null,
    ledger: currentStatus.ledger || null,
    systemEvents: currentStatus.systemEvents || null,
    projection: currentStatus.projection || null,
  };

  try {
    const response = await fetchWithTimeout(targetUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(config.nodeAuthKey ? { 'x-volt-node-key': config.nodeAuthKey } : {}),
      },
      body: JSON.stringify(payload),
      timeoutMs: normalizePositiveInteger(config.requestTimeoutMs, DEFAULT_REQUEST_TIMEOUT_MS),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const nextStatus = readJson(paths.statusPath, {}) || {};
    setNodeStatus(paths, {
      ...nextStatus,
      discovery: {
        registerUrl: config.registerUrl || null,
        heartbeatUrl: config.heartbeatUrl || null,
        lastPresenceAt: new Date().toISOString(),
        lastPresenceMode: mode,
        lastPresenceStatus: 'ok',
        lastPresenceError: null,
      },
    });

    return { status: 'ok', error: null };
  } catch (error) {
    const message = error.message || String(error);
    const nextStatus = readJson(paths.statusPath, {}) || {};
    setNodeStatus(paths, {
      ...nextStatus,
      discovery: {
        registerUrl: config.registerUrl || null,
        heartbeatUrl: config.heartbeatUrl || null,
        lastPresenceAt: new Date().toISOString(),
        lastPresenceMode: mode,
        lastPresenceStatus: 'error',
        lastPresenceError: message,
      },
    });
    return { status: 'error', error: message };
  }
}

async function ensureNodeAuthKey(paths, config) {
  if (config.nodeAuthKey) {
    return config;
  }

  if (!process.stdin.isTTY || !process.stdout.isTTY) {
    throw new Error('Missing VOLT_NODE_AUTH_KEY. Generate a node key in Volt and add it to your .env file.');
  }

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  try {
    const enteredKey = String(await rl.question('Paste your Volt node key: ')).trim();
    if (!enteredKey) {
      throw new Error('A Volt node key is required to start this node.');
    }

    const envPath = path.join(process.cwd(), '.env');
    upsertEnvFileValue(envPath, 'VOLT_NODE_AUTH_KEY', enteredKey);
    process.env.VOLT_NODE_AUTH_KEY = enteredKey;
    return loadNodeConfig(paths);
  } finally {
    rl.close();
  }
}

function buildDashboardHtml(config) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>${escapeHtml(config.nodeName || 'volt-node')}</title>
  <style>
    :root {
      --bg: #0f172a;
      --panel: #111827;
      --panel-2: #1f2937;
      --ok: #22c55e;
      --warn: #ef4444;
      --text: #e5e7eb;
      --muted: #94a3b8;
      --accent: #38bdf8;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: ui-sans-serif, system-ui, sans-serif;
      background: radial-gradient(circle at top, #172554 0%, var(--bg) 55%);
      color: var(--text);
      min-height: 100vh;
    }
    .wrap {
      max-width: 1100px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }
    .hero, .panel, .card {
      border: 1px solid rgba(148, 163, 184, 0.18);
      background: rgba(17, 24, 39, 0.92);
      border-radius: 20px;
      box-shadow: 0 30px 80px rgba(0, 0, 0, 0.28);
    }
    .hero {
      padding: 24px;
      display: grid;
      grid-template-columns: 1.4fr 1fr;
      gap: 20px;
      margin-bottom: 20px;
    }
    .eyebrow {
      text-transform: uppercase;
      letter-spacing: 0.18em;
      font-size: 12px;
      color: var(--accent);
      margin-bottom: 12px;
    }
    h1, h2, h3 { margin: 0 0 10px; }
    p, .muted { color: var(--muted); }
    .badge {
      display: inline-flex;
      align-items: center;
      padding: 8px 12px;
      border-radius: 999px;
      font-weight: 700;
      font-size: 12px;
      letter-spacing: 0.08em;
    }
    .ok { background: rgba(34, 197, 94, 0.18); color: #bbf7d0; }
    .warn { background: rgba(239, 68, 68, 0.18); color: #fecaca; }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 16px;
      margin: 20px 0;
    }
    .card {
      padding: 18px;
      background: rgba(31, 41, 55, 0.82);
    }
    .value {
      font-size: 28px;
      font-weight: 800;
      margin-bottom: 6px;
    }
    .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .panel {
      padding: 20px;
      margin-top: 20px;
    }
    .list {
      display: grid;
      gap: 12px;
    }
    .row {
      padding: 14px 16px;
      border-radius: 14px;
      background: rgba(31, 41, 55, 0.82);
      border: 1px solid rgba(148, 163, 184, 0.12);
    }
    code {
      word-break: break-all;
      color: #bfdbfe;
    }
    @media (max-width: 820px) {
      .hero { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div>
        <div class="eyebrow">Volt Node</div>
        <h1 id="nodeName">Loading node...</h1>
        <p id="nodeSummary" class="muted">Polling local verifier status.</p>
      </div>
      <div>
        <div class="eyebrow">Execution Agreement</div>
        <div id="agreementBadge" class="badge warn">Checking</div>
        <p id="agreementText" class="muted">Waiting for the latest local consensus comparison.</p>
      </div>
    </section>

    <section class="grid" id="summaryGrid"></section>

    <section class="panel">
      <div class="eyebrow">Heads</div>
      <div class="list">
        <div class="row"><strong>Ledger Head</strong><br /><code id="ledgerHead">-</code></div>
        <div class="row"><strong>Event Head</strong><br /><code id="eventHead">-</code></div>
        <div class="row"><strong>Peer-Majority Head</strong><br /><code id="consensusHead">-</code></div>
      </div>
    </section>

    <section class="panel">
      <div class="eyebrow">Setup</div>
      <div id="setupPanel" class="list">
        <div class="row">Loading setup...</div>
      </div>
    </section>

    <section class="panel">
      <div class="eyebrow">Network Presence</div>
      <div class="list">
        <div class="row"><strong>Volt Presence</strong><br /><span id="presenceStatus">-</span></div>
        <div class="row"><strong>Consensus Report</strong><br /><span id="reportStatus">-</span></div>
        <div class="row"><strong>Witnessing Nodes</strong><br /><span id="witnesses">-</span></div>
      </div>
    </section>

    <section class="panel">
      <div class="eyebrow">Conflicts</div>
      <div id="conflictList" class="list">
        <div class="row">No conflicts reported.</div>
      </div>
    </section>
  </div>

  <script>
    function shortHash(value) {
      return value ? value.slice(0, 18) + '...' : 'n/a';
    }

    function escapeHtml(value) {
      return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    async function loadStatus() {
      const response = await fetch('/node/status', { cache: 'no-store' });
      const status = await response.json();

      document.getElementById('nodeName').textContent = status.nodeName || status.nodeId || 'volt-node';
      document.getElementById('nodeSummary').textContent = [
        status.status || 'unknown',
        status.lastSyncedAt ? 'last sync ' + new Date(status.lastSyncedAt).toLocaleString() : 'never synced',
        status.running ? 'running' : 'idle',
      ].join(' • ');

      const healthy = Boolean(status?.consensus?.healthy);
      const badge = document.getElementById('agreementBadge');
      badge.textContent = healthy ? 'AGREE' : 'DISAGREE';
      badge.className = 'badge ' + (healthy ? 'ok' : 'warn');
      document.getElementById('agreementText').textContent = healthy
        ? 'Local replay matches the peer-majority head.'
        : 'Local replay differs from the peer-majority head or no trusted witness majority exists.';

      const cards = [
        { label: 'Transactions', value: Number(status?.ledger?.transactionCount || 0).toLocaleString() },
        { label: 'Events', value: Number(status?.systemEvents?.eventCount || 0).toLocaleString() },
        { label: 'Accounts', value: Number(status?.projection?.accountCount || 0).toLocaleString() },
        { label: 'Witnesses', value: Number(status?.consensus?.witnessCount || 0).toLocaleString() },
      ];
      document.getElementById('summaryGrid').innerHTML = cards.map((card) =>
        '<article class="card">' +
          '<div class="value">' + escapeHtml(card.value) + '</div>' +
          '<div class="label">' + escapeHtml(card.label) + '</div>' +
        '</article>'
      ).join('');

      document.getElementById('ledgerHead').textContent = shortHash(status?.ledger?.latestHash);
      document.getElementById('eventHead').textContent = shortHash(status?.systemEvents?.latestHash);
      document.getElementById('consensusHead').textContent = status?.consensus?.canonicalHeadKey || 'n/a';
      document.getElementById('presenceStatus').textContent = [
        status?.discovery?.lastPresenceStatus || 'idle',
        status?.discovery?.lastPresenceMode ? 'via ' + status.discovery.lastPresenceMode : '',
        status?.discovery?.lastPresenceAt ? 'at ' + new Date(status.discovery.lastPresenceAt).toLocaleString() : '',
        status?.discovery?.lastPresenceError ? '(' + status.discovery.lastPresenceError + ')' : '',
      ].filter(Boolean).join(' ');

      document.getElementById('reportStatus').textContent = [
        status?.reporting?.lastReportStatus || 'idle',
        status?.reporting?.lastReportAt ? 'at ' + new Date(status.reporting.lastReportAt).toLocaleString() : '',
        status?.reporting?.lastReportError ? '(' + status.reporting.lastReportError + ')' : '',
      ].filter(Boolean).join(' ');

      const runtimeConfig = status?.config || {};
      const setupPanel = document.getElementById('setupPanel');
      if (setupPanel) {
        setupPanel.innerHTML = '<div class="row">' +
          '<form id="setupForm">' +
            '<div style="display:grid;gap:12px;">' +
              '<label><strong>Execution Server URL</strong><br /><input name="baseUrl" value="' + escapeHtml(runtimeConfig.baseUrl || '') + '" placeholder="https://volt.example.com" style="width:100%;margin-top:6px;padding:10px;border-radius:10px;border:1px solid #334155;background:#0f172a;color:#e5e7eb;" /></label>' +
              '<label><strong>Other Volt Nodes (optional)</strong><br /><input name="peers" value="' + escapeHtml((runtimeConfig.peers || []).join(', ')) + '" placeholder="Only needed for manual overrides" style="width:100%;margin-top:6px;padding:10px;border-radius:10px;border:1px solid #334155;background:#0f172a;color:#e5e7eb;" /></label>' +
              '<label><strong>Sync Interval (ms)</strong><br /><input name="syncIntervalMs" type="number" min="1000" step="500" value="' + escapeHtml(runtimeConfig.syncIntervalMs || 2000) + '" placeholder="2000" style="width:100%;margin-top:6px;padding:10px;border-radius:10px;border:1px solid #334155;background:#0f172a;color:#e5e7eb;" /></label>' +
              '<label><strong>Consensus Report URL</strong><br /><input name="reportUrl" value="' + escapeHtml(runtimeConfig.reportUrl || '') + '" placeholder="https://volt.example.com/api/consensus/report" style="width:100%;margin-top:6px;padding:10px;border-radius:10px;border:1px solid #334155;background:#0f172a;color:#e5e7eb;" /></label>' +
              '<label><strong>Report Key</strong><br /><input name="reportKey" value="" placeholder="Optional shared secret" style="width:100%;margin-top:6px;padding:10px;border-radius:10px;border:1px solid #334155;background:#0f172a;color:#e5e7eb;" /></label>' +
              '<button type="submit" style="padding:12px 14px;border:0;border-radius:999px;background:#38bdf8;color:#082f49;font-weight:800;cursor:pointer;">Save Node Config</button>' +
              '<div class="muted">' + (runtimeConfig.hasAnyUpstream ? 'This node will auto-discover other Volt nodes from your main server.' : 'Set your main Volt server URL so this node can auto-discover the network.') + '</div>' +
            '</div>' +
          '</form>' +
        '</div>';

        const setupForm = document.getElementById('setupForm');
        setupForm?.addEventListener('submit', async (event) => {
          event.preventDefault();
          const formData = new FormData(setupForm);
          const payload = {
            baseUrl: String(formData.get('baseUrl') || '').trim(),
            peers: String(formData.get('peers') || '').trim(),
            syncIntervalMs: String(formData.get('syncIntervalMs') || '').trim(),
            reportUrl: String(formData.get('reportUrl') || '').trim(),
            reportKey: String(formData.get('reportKey') || '').trim(),
          };
          const response = await fetch('/api/setup', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          if (!response.ok) {
            const errorText = await response.text();
            alert('Failed to save setup: ' + errorText);
            return;
          }
          await loadStatus();
          alert('Node config saved. The next sync cycle will use the new upstream settings.');
        });
      }
      document.getElementById('witnesses').textContent = Array.isArray(status?.consensus?.trustedBy) && status.consensus.trustedBy.length
        ? status.consensus.trustedBy.join(', ')
        : 'No peer witnesses yet';

      const conflicts = Array.isArray(status?.conflicts) ? status.conflicts : [];
      document.getElementById('conflictList').innerHTML = conflicts.length
        ? conflicts.map((entry) => '<div class="row">' + escapeHtml(entry) + '</div>').join('')
        : '<div class="row">No conflicts reported.</div>';
    }

    loadStatus().catch(console.error);
    setInterval(() => {
      loadStatus().catch(console.error);
    }, 2000);
  </script>
</body>
</html>`;
}

function collectJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', (chunk) => {
      raw += chunk;
      if (raw.length > 1024 * 1024) {
        reject(new Error('Request body too large.'));
      }
    });
    req.on('end', () => {
      if (!raw.trim()) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch (error) {
        reject(new Error('Invalid JSON body.'));
      }
    });
    req.on('error', reject);
  });
}

function startNodeStatusServer(paths, config) {
  const port = Number(config.statusPort || DEFAULT_STATUS_PORT);
  const host = String(config.statusHost || DEFAULT_STATUS_HOST);
  const dashboardHtml = buildDashboardHtml(config);
  const server = http.createServer(async (req, res) => {
    if (req.url === '/' || req.url === '/index.html') {
      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store',
      });
      res.end(dashboardHtml);
      return;
    }

    if (req.url === '/node/status' || req.url === '/api/status') {
      const status = readJson(paths.statusPath, {}) || {};
      const payload = {
        ...status,
        ...buildSelfDescriptor(config, paths),
        config: buildRuntimeConfigSummary(config),
        history: summarizeHistory(paths),
      };
      res.writeHead(200, {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
        'Access-Control-Allow-Origin': '*',
      });
      res.end(JSON.stringify(payload, null, 2));
      return;
    }

    if (req.url === '/api/setup' && req.method === 'POST') {
      try {
        const body = await collectJsonBody(req);
        const nextBaseUrl = normalizeBaseUrl(body.baseUrl || '');
        const nextPeers = parsePeerList(body.peers || '');
        const nextReportUrl = String(body.reportUrl || '').trim() || (nextBaseUrl ? `${nextBaseUrl}/api/consensus/report` : null);
        const nextPrimaryStatusUrl = normalizePeerUrl(nextBaseUrl || '');
        const nextDiscoveryUrl = nextBaseUrl ? `${nextBaseUrl}/api/nodes` : null;
        const nextRegisterUrl = nextBaseUrl ? `${nextBaseUrl}/api/nodes/register` : null;
        const nextHeartbeatUrl = nextBaseUrl ? `${nextBaseUrl}/api/nodes/heartbeat` : null;
        const nextSyncIntervalMs = normalizeSyncIntervalMs(
          body.syncIntervalMs || config.syncIntervalMs || DEFAULT_SYNC_INTERVAL_MS,
          DEFAULT_SYNC_INTERVAL_MS
        );
        const nextConfig = {
          ...loadNodeConfig(paths),
          baseUrl: nextBaseUrl,
          primaryStatusUrl: nextPrimaryStatusUrl,
          discoveryUrl: nextDiscoveryUrl,
          registerUrl: nextRegisterUrl,
          heartbeatUrl: nextHeartbeatUrl,
          syncIntervalMs: nextSyncIntervalMs,
          reportUrl: nextReportUrl,
          reportKey: String(body.reportKey || '').trim() || config.reportKey || null,
          peers: nextPeers,
          ledgerSource: null,
          eventsSource: null,
        };
        saveNodeConfig(paths, nextConfig);
        Object.assign(config, nextConfig);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: true, config: buildRuntimeConfigSummary(config) }));
      } catch (error) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ error: error.message || String(error) }));
      }
      return;
    }

    if (req.url === '/exports/ledger.json') {
      try {
        const services = await openNodeServices(paths.dbPath);
        try {
          const payload = await services.ledgerService.exportLedger();
          res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8',
            'Cache-Control': 'no-store',
            'Access-Control-Allow-Origin': '*',
          });
          res.end(JSON.stringify(payload, null, 2));
        } finally {
          await closeDb(services);
        }
      } catch (error) {
        res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ error: error.message || String(error) }));
      }
      return;
    }

    if (req.url === '/exports/system-events.json') {
      try {
        const services = await openNodeServices(paths.dbPath);
        try {
          const payload = await services.eventLedgerService.exportEvents();
          res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8',
            'Cache-Control': 'no-store',
            'Access-Control-Allow-Origin': '*',
          });
          res.end(JSON.stringify(payload, null, 2));
        } finally {
          await closeDb(services);
        }
      } catch (error) {
        res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ error: error.message || String(error) }));
      }
      return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ error: 'Not found' }));
  });

  server.on('error', (error) => {
    console.warn(`[volt-node] status endpoint unavailable • ${error.message || error}`);
  });

  server.listen(port, host, () => {
    console.log(`[volt-node] serving dashboard on http://${host}:${port}/`);
    console.log(`[volt-node] serving status on http://${host}:${port}/node/status`);
  });

  return server;
}

async function commandStart(dirArg) {
  const nodeDir = resolveNodeDir(dirArg);
  const paths = getNodePaths(nodeDir);
  await commandInit(nodeDir);

  const config = await ensureNodeAuthKey(paths, loadNodeConfig(paths));
  config.syncIntervalMs = normalizeSyncIntervalMs(config.syncIntervalMs, DEFAULT_SYNC_INTERVAL_MS);
  saveNodeConfig(paths, config);
  const statusServer = startNodeStatusServer(paths, config);
  let nextCycleTimer = null;
  let shuttingDown = false;

  async function runSyncCycle() {
    const cycleStartedAt = Date.now();
    try {
      setNodeStatus(paths, {
        ...buildSelfDescriptor(config, paths),
        ...(readJson(paths.statusPath, {}) || {}),
        nodeDir,
        running: true,
        status: 'syncing',
        dbPath: paths.dbPath,
      });

      const result = await syncNode(nodeDir, { config });
      const peerNetwork = await refreshPeerNetwork(paths, config);
      const peerConflicts = (peerNetwork.peers || []).flatMap((peer) => peer.conflicts || []);
      const combinedConflicts = [...new Set([...(result.conflicts || []), ...peerConflicts])];
      setNodeStatus(paths, {
        ...buildSelfDescriptor(config, paths),
        nodeDir,
        running: true,
        status: combinedConflicts.length ? 'attention' : 'healthy',
        dbPath: paths.dbPath,
        lastSyncedAt: new Date().toISOString(),
        conflictDetected: combinedConflicts.length > 0,
        conflicts: combinedConflicts,
        lastErrorAt: null,
        lastError: null,
        history: summarizeHistory(paths),
        peerNetwork,
        consensus: result.consensus,
        projection: (readJson(paths.statusPath, {}) || {}).projection || result.projection || null,
        ledger: {
          transactionCount: result.ledgerIntegrity.transactionCount,
          latestHash: result.ledgerIntegrity.latestHash,
          valid: result.ledgerIntegrity.valid,
        },
        systemEvents: {
          eventCount: result.eventIntegrity.eventCount,
          latestHash: result.eventIntegrity.latestHash,
          valid: result.eventIntegrity.valid,
        },
      });

      const reportResult = await pushConsensusReport(paths, config);
      const presenceResult = await pushNodePresence(paths, config, 'heartbeat');

      console.log(
        `[volt-node] synced ok • tx=${result.ledgerIntegrity.transactionCount} • events=${result.eventIntegrity.eventCount}`
      );
      console.log(`[volt-node] discovery heartbeat • ${presenceResult.status}`);
      console.log(`[volt-node] execution bridge • ${reportResult.status}`);
      if (combinedConflicts.length) {
        console.log(`[volt-node] conflict detected • ${combinedConflicts.join(' | ')}`);
      }
    } catch (error) {
      const now = new Date().toISOString();
      const message = error.message || String(error);
      appendLine(paths.conflictLogPath, `[${now}] sync failure: ${message}`);
      setNodeStatus(paths, {
        ...buildSelfDescriptor(config, paths),
        ...(readJson(paths.statusPath, {}) || {}),
        nodeDir,
        running: true,
        status: 'error',
        dbPath: paths.dbPath,
        lastErrorAt: now,
        lastError: message,
        history: summarizeHistory(paths),
        conflictDetected: true,
        conflicts: [...(readJson(paths.statusPath, {})?.conflicts || []), `sync failure: ${message}`].slice(-10),
      });
      await pushConsensusReport(paths, config).catch(() => {});
      console.error(`[volt-node] sync failed • ${message}`);
    } finally {
      if (!shuttingDown) {
        const elapsedMs = Date.now() - cycleStartedAt;
        const intervalMs = normalizeSyncIntervalMs(config.syncIntervalMs, DEFAULT_SYNC_INTERVAL_MS);
        const nextDelayMs = Math.max(0, intervalMs - elapsedMs);
        nextCycleTimer = setTimeout(runSyncCycle, nextDelayMs);
      }
    }
  }

  console.log(`[volt-node] starting in ${nodeDir}`);
  console.log(`[volt-node] configured ledger source: ${config.ledgerSource}`);
  console.log(`[volt-node] configured event source: ${config.eventsSource}`);
  console.log(`[volt-node] primary status source: ${config.primaryStatusUrl || 'none'}`);
  console.log(`[volt-node] execution report url: ${config.reportUrl || 'disabled'}`);
  console.log(`[volt-node] sync interval: ${config.syncIntervalMs}ms`);

  setNodeStatus(paths, {
    ...buildSelfDescriptor(config, paths),
    nodeDir,
    running: true,
    status: 'starting',
    dbPath: paths.dbPath,
    conflictDetected: false,
    conflicts: [],
  });

  const registerResult = await pushNodePresence(paths, config, 'register').catch((error) => ({
    status: 'error',
    error: error.message || String(error),
  }));
  console.log(`[volt-node] discovery register • ${registerResult.status}`);

  await runSyncCycle();

  async function shutdown(signal) {
    shuttingDown = true;
    if (nextCycleTimer) {
      clearTimeout(nextCycleTimer);
      nextCycleTimer = null;
    }
    statusServer.close();
    const currentStatus = readJson(paths.statusPath, {}) || {};
    setNodeStatus(paths, {
      ...currentStatus,
      ...buildSelfDescriptor(config, paths),
      nodeDir,
      running: false,
      status: 'stopped',
      dbPath: paths.dbPath,
      stoppedAt: new Date().toISOString(),
      stopSignal: signal,
    });
    await pushConsensusReport(paths, config).catch(() => {});
    await pushNodePresence(paths, config, 'heartbeat').catch(() => {});
    console.log(`[volt-node] stopped • ${signal}`);
    process.exit(0);
  }

  process.on('SIGINT', () => {
    shutdown('SIGINT');
  });
  process.on('SIGTERM', () => {
    shutdown('SIGTERM');
  });
}

async function main() {
  const [, , command, ...args] = process.argv;

  switch (command) {
    case 'init':
      await commandInit(args[0]);
      break;
    case 'sync':
      await commandSync(args[0]);
      break;
    case 'verify':
      await commandVerify(args[0]);
      break;
    case 'status':
      await commandStatus(args[0]);
      break;
    case 'start':
      await commandStart(args[0]);
      break;
    case 'help':
    case '--help':
    case '-h':
    case undefined:
      printHelp();
      break;
    default:
      throw new Error(`Unknown node command: ${command}`);
  }
}

main().catch((error) => {
  console.error(error.message || error);
  process.exitCode = 1;
});
