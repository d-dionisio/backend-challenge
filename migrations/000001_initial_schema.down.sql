DROP TRIGGER IF EXISTS prevent_ledger_update
ON wallet_ledger_entries;

DROP TRIGGER IF EXISTS prevent_ledger_delete
ON wallet_ledger_entries;

DROP FUNCTION IF EXISTS prevent_ledger_mutation();

DROP TABLE IF EXISTS wallet_ledger_entries;

DROP TABLE IF EXISTS wager_transactions;

DROP TABLE IF EXISTS wallets;