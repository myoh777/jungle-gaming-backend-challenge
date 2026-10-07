DROP TABLE IF EXISTS outbox_events;
DROP FUNCTION IF EXISTS outbox_events_payload_immutable();
DROP TABLE IF EXISTS inbox_messages;
DROP TABLE IF EXISTS wallet_ledger_entries;
DROP FUNCTION IF EXISTS wallet_ledger_entries_immutable();
DROP TABLE IF EXISTS wager_transactions;
DROP TABLE IF EXISTS wallets;
