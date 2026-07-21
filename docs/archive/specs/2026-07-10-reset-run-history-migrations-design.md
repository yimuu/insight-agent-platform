# Run History Migration Reset Design

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

## Goal

Treat run history as disposable development data and remove all legacy event migration support.

## Design

The current typed `run_events` schema becomes the baseline schema in both SQLite and PostgreSQL `001_create_run_history.sql` migrations. The event rebuild migration is deleted, and store startup runs sqlx migrations directly without pre-migration schema creation or legacy-column repair.

Existing development databases are not upgraded. Developers must delete the SQLite history database or recreate the PostgreSQL development volume after this change. Current runtime behavior, SSE envelopes, run-history API envelopes, and query indexes remain unchanged.

## Verification

A repository-level migration layout test verifies that each `001` migration defines typed event columns and that no event rebuild migration remains. Existing SQLite store tests, API tests, runner tests, and PostgreSQL integration tests verify fresh-database behavior.
