# Changelog

## 0.4.0

- This version changes the ids of the tables used by Teleport to BIGSERIAL and BIGINT, to avoid problems with overflow.
- An sql-only UPSERT strategy was implemented. It's chosen automatically, based on the version of the database.
- DDL triggers were implemented. To enable it, put `use_event_triggers: true` in the configuration.

