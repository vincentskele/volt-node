const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const GENESIS_HASH = 'VOLT_LEDGER_GENESIS_V1';
const LEDGER_EXPORT_VERSION = 1;

function stableStringify(value) {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableStringify(entry)).join(',')}]`;
  }

  const keys = Object.keys(value).sort();
  return `{${keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(',')}}`;
}

function normalizeUserId(userId) {
  if (userId === null || typeof userId === 'undefined' || userId === '') {
    return null;
  }

  return String(userId);
}

function parseMetadata(rawMetadata) {
  if (!rawMetadata) return {};
  if (typeof rawMetadata === 'object') return rawMetadata;

  try {
    return JSON.parse(rawMetadata);
  } catch (error) {
    return {};
  }
}

function normalizeMetadataForStorage(metadata, fromUserId, toUserId) {
  const baseMetadata = metadata && typeof metadata === 'object' && !Array.isArray(metadata)
    ? { ...metadata }
    : {};

  if (!baseMetadata.fromAccount) {
    baseMetadata.fromAccount = fromUserId ? 'wallet' : 'system';
  }

  if (!baseMetadata.toAccount) {
    baseMetadata.toAccount = toUserId ? 'wallet' : 'system';
  }

  if (!baseMetadata.ledgerVersion) {
    baseMetadata.ledgerVersion = LEDGER_EXPORT_VERSION;
  }

  return stableStringify(baseMetadata);
}

function getLedgerAccounts(metadata, fromUserId, toUserId) {
  return {
    fromAccount: metadata.fromAccount || (fromUserId ? 'wallet' : 'system'),
    toAccount: metadata.toAccount || (toUserId ? 'wallet' : 'system'),
  };
}

function computeTransactionHash(transaction) {
  const payload = [
    transaction.id,
    transaction.timestamp,
    transaction.type,
    transaction.from_user_id ?? '',
    transaction.to_user_id ?? '',
    transaction.amount,
    transaction.metadata,
    transaction.previous_hash,
  ].join('|');

  return crypto.createHash('sha256').update(payload).digest('hex');
}

function formatTransactionRow(row, { parse = true } = {}) {
  const formatted = {
    id: Number(row.id),
    timestamp: Number(row.timestamp),
    type: row.type,
    from_user_id: normalizeUserId(row.from_user_id),
    to_user_id: normalizeUserId(row.to_user_id),
    amount: Number(row.amount),
    metadata_raw: row.metadata || '{}',
    previous_hash: row.previous_hash,
    hash: row.hash,
  };

  formatted.metadata = parse ? parseMetadata(formatted.metadata_raw) : formatted.metadata_raw;
  return formatted;
}

function createLedgerService({ db, ensureUserEconomyRow = async () => {} }) {
  const cache = {
    loaded: false,
    latestId: 0,
    latestHash: GENESIS_HASH,
    users: new Map(),
  };

  let initializationPromise = null;

  function createExecutor(database = db) {
    return {
      run(sql, params = []) {
        return new Promise((resolve, reject) => {
          database.run(sql, params, function onRun(error) {
            if (error) return reject(error);
            resolve({ changes: this.changes ?? 0, lastID: this.lastID ?? null });
          });
        });
      },
      get(sql, params = []) {
        return new Promise((resolve, reject) => {
          database.get(sql, params, (error, row) => {
            if (error) return reject(error);
            resolve(row || null);
          });
        });
      },
      all(sql, params = []) {
        return new Promise((resolve, reject) => {
          database.all(sql, params, (error, rows) => {
            if (error) return reject(error);
            resolve(rows || []);
          });
        });
      },
    };
  }

  const defaultExecutor = createExecutor(db);

  async function initialize() {
    if (!initializationPromise) {
      initializationPromise = (async () => {
        await defaultExecutor.run(`
          CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            type TEXT NOT NULL,
            from_user_id TEXT,
            to_user_id TEXT,
            amount INTEGER NOT NULL CHECK(amount > 0),
            metadata TEXT NOT NULL DEFAULT '{}',
            previous_hash TEXT NOT NULL,
            hash TEXT NOT NULL UNIQUE
          )
        `);

        await defaultExecutor.run(
          `CREATE INDEX IF NOT EXISTS idx_transactions_from_user_id ON transactions(from_user_id)`
        );
        await defaultExecutor.run(
          `CREATE INDEX IF NOT EXISTS idx_transactions_to_user_id ON transactions(to_user_id)`
        );
        await defaultExecutor.run(
          `CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp)`
        );

        await defaultExecutor.run(`
          CREATE TRIGGER IF NOT EXISTS transactions_prevent_update
          BEFORE UPDATE ON transactions
          BEGIN
            SELECT RAISE(ABORT, 'transactions are append-only');
          END
        `);

        await defaultExecutor.run(`
          CREATE TRIGGER IF NOT EXISTS transactions_prevent_delete
          BEFORE DELETE ON transactions
          BEGIN
            SELECT RAISE(ABORT, 'transactions are append-only');
          END
        `);
      })().catch((error) => {
        initializationPromise = null;
        throw error;
      });
    }

    return initializationPromise;
  }

  function ensureUserState(state, userId) {
    if (!state.users.has(userId)) {
      state.users.set(userId, {
        wallet: 0,
        bank: 0,
        total: 0,
        accounts: {},
      });
    }

    return state.users.get(userId);
  }

  function recomputeUserTotal(userState) {
    userState.total = Object.values(userState.accounts).reduce((sum, value) => sum + value, 0);
    userState.wallet = userState.accounts.wallet || 0;
    userState.bank = userState.accounts.bank || 0;
  }

  function applyTransactionToState(state, row) {
    const metadata = parseMetadata(row.metadata);
    const { fromAccount, toAccount } = getLedgerAccounts(metadata, row.from_user_id, row.to_user_id);

    if (row.from_user_id) {
      const fromUserState = ensureUserState(state, row.from_user_id);
      fromUserState.accounts[fromAccount] = (fromUserState.accounts[fromAccount] || 0) - Number(row.amount);
      recomputeUserTotal(fromUserState);
    }

    if (row.to_user_id) {
      const toUserState = ensureUserState(state, row.to_user_id);
      toUserState.accounts[toAccount] = (toUserState.accounts[toAccount] || 0) + Number(row.amount);
      recomputeUserTotal(toUserState);
    }

    state.latestId = Number(row.id);
    state.latestHash = row.hash;
  }

  async function rebuildCache() {
    await initialize();

    const rows = await defaultExecutor.all(`SELECT * FROM transactions ORDER BY id ASC`);
    const nextState = {
      loaded: true,
      latestId: 0,
      latestHash: GENESIS_HASH,
      users: new Map(),
    };

    for (const row of rows) {
      applyTransactionToState(nextState, row);
    }

    cache.loaded = true;
    cache.latestId = nextState.latestId;
    cache.latestHash = nextState.latestHash;
    cache.users = nextState.users;
    return cache;
  }

  async function refreshCache() {
    if (!cache.loaded) {
      return rebuildCache();
    }

    return cache;
  }

  async function getUserAccountBalance(userId, account = 'wallet', { executor = defaultExecutor } = {}) {
    await initialize();
    const row = await executor.get(
      `SELECT COALESCE(SUM(CASE
         WHEN to_user_id = ? AND json_extract(metadata, '$.toAccount') = ? THEN amount
         WHEN from_user_id = ? AND json_extract(metadata, '$.fromAccount') = ? THEN -amount
         ELSE 0
       END), 0) AS balance
       FROM transactions`,
      [userId, account, userId, account]
    );
    return Number(row?.balance || 0);
  }

  async function appendTransaction(entry, options = {}) {
    await initialize();

    const executor = options.executor || defaultExecutor;
    const inTransaction = Boolean(options.inTransaction);
    const fromUserId = normalizeUserId(entry.fromUserId);
    const toUserId = normalizeUserId(entry.toUserId);
    const amount = Number(entry.amount);

    if (!entry.type || typeof entry.type !== 'string') {
      throw new Error('Transaction type is required.');
    }

    if (!Number.isInteger(amount) || amount <= 0) {
      throw new Error('Transaction amount must be a positive integer.');
    }

    const metadata = normalizeMetadataForStorage(entry.metadata, fromUserId, toUserId);
    const parsedMetadata = parseMetadata(metadata);
    const timestamp = Number(entry.timestamp || Math.floor(Date.now() / 1000));

    if (!inTransaction) {
      await executor.run('BEGIN IMMEDIATE');
    }

    try {
      if (fromUserId && entry.enforceSufficientFunds) {
        const fromAccount = parsedMetadata.fromAccount || 'wallet';
        const balance = await getUserAccountBalance(fromUserId, fromAccount, { executor });
        if (balance < amount) {
          throw new Error('Insufficient funds.');
        }
      }

      if (fromUserId) await ensureUserEconomyRow(fromUserId);
      if (toUserId) await ensureUserEconomyRow(toUserId);

      const latestRow = await executor.get(`SELECT id, hash FROM transactions ORDER BY id DESC LIMIT 1`);
      const nextId = Number(latestRow?.id || 0) + 1;
      const previousHash = latestRow?.hash || GENESIS_HASH;
      const rowToInsert = {
        id: nextId,
        timestamp,
        type: entry.type,
        from_user_id: fromUserId,
        to_user_id: toUserId,
        amount,
        metadata,
        previous_hash: previousHash,
      };
      const hash = computeTransactionHash(rowToInsert);

      await executor.run(
        `INSERT INTO transactions (
           id,
           timestamp,
           type,
           from_user_id,
           to_user_id,
           amount,
           metadata,
           previous_hash,
           hash
         )
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          rowToInsert.id,
          rowToInsert.timestamp,
          rowToInsert.type,
          rowToInsert.from_user_id,
          rowToInsert.to_user_id,
          rowToInsert.amount,
          rowToInsert.metadata,
          rowToInsert.previous_hash,
          hash,
        ]
      );

      if (!inTransaction) {
        await executor.run('COMMIT');
      }

      cache.loaded = false;
      return formatTransactionRow({ ...rowToInsert, hash });
    } catch (error) {
      if (!inTransaction) {
        try {
          await executor.run('ROLLBACK');
        } catch (rollbackError) {
          // Preserve original error.
        }
      }
      throw error;
    }
  }

  async function getBalance(userId) {
    const state = await refreshCache();
    const user = state.users.get(String(userId));
    return {
      wallet: Number(user?.wallet || 0),
      bank: Number(user?.bank || 0),
      total: Number(user?.total || 0),
    };
  }

  async function getLeaderboard(limit = 10) {
    const state = await refreshCache();
    return [...state.users.entries()]
      .map(([userId, info]) => ({ userId, total: info.total, wallet: info.wallet, bank: info.bank }))
      .sort((left, right) => right.total - left.total || left.userId.localeCompare(right.userId))
      .slice(0, Math.max(1, Number(limit) || 10));
  }

  async function getFullLedger(options = {}) {
    await initialize();
    const sinceId = Number(options.sinceId || 0);
    const rows = await defaultExecutor.all(
      `SELECT * FROM transactions WHERE id > ? ORDER BY id ASC`,
      [sinceId]
    );

    return rows.map((row) => formatTransactionRow(row, { parse: options.parse !== false }));
  }

  async function verifyLedgerIntegrity(providedRows = null) {
    await initialize();

    const rows = providedRows
      ? providedRows.map((row) => ({
        ...row,
        metadata: typeof row.metadata_raw !== 'undefined'
          ? row.metadata_raw
          : (typeof row.metadata === 'string' ? row.metadata : stableStringify(row.metadata || {})),
      }))
      : await defaultExecutor.all(`SELECT * FROM transactions ORDER BY id ASC`);

    let previousHash = GENESIS_HASH;
    let previousId = 0;

    for (let index = 0; index < rows.length; index += 1) {
      const row = rows[index];
      const normalizedRow = {
        id: Number(row.id),
        timestamp: Number(row.timestamp),
        type: row.type,
        from_user_id: normalizeUserId(row.from_user_id),
        to_user_id: normalizeUserId(row.to_user_id),
        amount: Number(row.amount),
        metadata: typeof row.metadata === 'string'
          ? row.metadata
          : (typeof row.metadata_raw === 'string' ? row.metadata_raw : stableStringify(row.metadata || {})),
        previous_hash: row.previous_hash,
        hash: row.hash,
      };

      if (!Number.isInteger(normalizedRow.id) || normalizedRow.id <= previousId) {
        return {
          valid: false,
          firstBrokenIndex: index,
          transactionId: normalizedRow.id,
          reason: 'Transaction IDs are not strictly increasing.',
        };
      }

      if (normalizedRow.previous_hash !== previousHash) {
        return {
          valid: false,
          firstBrokenIndex: index,
          transactionId: normalizedRow.id,
          reason: 'Previous hash mismatch.',
        };
      }

      const expectedHash = computeTransactionHash(normalizedRow);
      if (normalizedRow.hash !== expectedHash) {
        return {
          valid: false,
          firstBrokenIndex: index,
          transactionId: normalizedRow.id,
          reason: 'Hash mismatch.',
        };
      }

      previousHash = normalizedRow.hash;
      previousId = normalizedRow.id;
    }

    return {
      valid: true,
      firstBrokenIndex: null,
      transactionId: null,
      reason: null,
      transactionCount: rows.length,
      latestHash: previousHash,
      genesisHash: GENESIS_HASH,
    };
  }

  async function replayLedger() {
    const state = await rebuildCache();
    const users = {};

    for (const [userID, balance] of [...state.users.entries()].sort(([left], [right]) => left.localeCompare(right))) {
      users[userID] = {
        wallet: balance.wallet,
        bank: balance.bank,
        total: balance.total,
        accounts: { ...balance.accounts },
      };
    }

    return {
      users,
      latestTransactionId: state.latestId,
      latestHash: state.latestHash,
      transactionCount: await getTransactionCount(),
    };
  }

  async function getTransactionCount({ executor = defaultExecutor } = {}) {
    await initialize();
    const row = await executor.get(`SELECT COUNT(*) AS count FROM transactions`);
    return Number(row?.count || 0);
  }

  async function exportLedger(options = {}) {
    await initialize();
    const sinceId = Number(options.sinceId || 0);
    const rows = await defaultExecutor.all(
      `SELECT * FROM transactions WHERE id > ? ORDER BY id ASC`,
      [sinceId]
    );
    const integrity = await verifyLedgerIntegrity(rows);
    const payload = {
      exportVersion: LEDGER_EXPORT_VERSION,
      exportedAt: new Date().toISOString(),
      genesisHash: GENESIS_HASH,
      sinceId,
      integrity,
      transactions: rows.map((row) => ({
        id: Number(row.id),
        timestamp: Number(row.timestamp),
        type: row.type,
        from_user_id: normalizeUserId(row.from_user_id),
        to_user_id: normalizeUserId(row.to_user_id),
        amount: Number(row.amount),
        metadata: row.metadata,
        previous_hash: row.previous_hash,
        hash: row.hash,
      })),
    };

    if (options.outputPath) {
      const resolvedPath = path.resolve(options.outputPath);
      await fs.promises.mkdir(path.dirname(resolvedPath), { recursive: true });
      await fs.promises.writeFile(resolvedPath, JSON.stringify(payload, null, 2), 'utf8');
    }

    return payload;
  }

  async function importLedger(source) {
    await initialize();

    let payload = source;
    if (typeof source === 'string') {
      const resolvedPath = path.resolve(source);
      payload = JSON.parse(await fs.promises.readFile(resolvedPath, 'utf8'));
    }

    const transactions = Array.isArray(payload) ? payload : payload?.transactions;
    if (!Array.isArray(transactions)) {
      throw new Error('Imported ledger must be an array or an object with a transactions array.');
    }

    const existingCount = await getTransactionCount();
    if (existingCount > 0) {
      throw new Error('Import requires an empty transactions table.');
    }

    const integrity = await verifyLedgerIntegrity(transactions);
    if (!integrity.valid) {
      throw new Error(`Imported ledger failed verification at index ${integrity.firstBrokenIndex}: ${integrity.reason}`);
    }

    const preparedRows = transactions.map((row) => ({
      id: Number(row.id),
      timestamp: Number(row.timestamp),
      type: row.type,
      from_user_id: normalizeUserId(row.from_user_id),
      to_user_id: normalizeUserId(row.to_user_id),
      amount: Number(row.amount),
      metadata: typeof row.metadata === 'string' ? row.metadata : stableStringify(row.metadata || {}),
      previous_hash: row.previous_hash,
      hash: row.hash,
    }));

    const distinctUsers = new Set();
    for (const row of preparedRows) {
      if (row.from_user_id) distinctUsers.add(row.from_user_id);
      if (row.to_user_id) distinctUsers.add(row.to_user_id);
    }
    await Promise.all([...distinctUsers].map((userId) => ensureUserEconomyRow(userId)));

    await defaultExecutor.run('BEGIN IMMEDIATE');
    try {
      for (const row of preparedRows) {
        await defaultExecutor.run(
          `INSERT INTO transactions (
             id,
             timestamp,
             type,
             from_user_id,
             to_user_id,
             amount,
             metadata,
             previous_hash,
             hash
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            row.id,
            row.timestamp,
            row.type,
            row.from_user_id,
            row.to_user_id,
            row.amount,
            row.metadata,
            row.previous_hash,
            row.hash,
          ]
        );
      }

      await defaultExecutor.run('COMMIT');
    } catch (error) {
      try {
        await defaultExecutor.run('ROLLBACK');
      } catch (rollbackError) {
        // Ignore rollback errors so the original failure is preserved.
      }
      throw error;
    }

    cache.loaded = false;
    cache.latestId = 0;
    cache.latestHash = GENESIS_HASH;
    cache.users = new Map();

    return {
      imported: preparedRows.length,
      latestHash: preparedRows[preparedRows.length - 1]?.hash || GENESIS_HASH,
    };
  }

  return {
    GENESIS_HASH,
    LEDGER_EXPORT_VERSION,
    stableStringify,
    parseMetadata,
    formatTransactionRow,
    createExecutor,
    initialize,
    appendTransaction,
    getBalance,
    getLeaderboard,
    getFullLedger,
    verifyLedgerIntegrity,
    replayLedger,
    getTransactionCount,
    exportLedger,
    importLedger,
  };
}

module.exports = {
  GENESIS_HASH,
  LEDGER_EXPORT_VERSION,
  stableStringify,
  createLedgerService,
};
