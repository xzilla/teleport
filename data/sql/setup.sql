-- Create schema for teleport tables teleport
CREATE SCHEMA IF NOT EXISTS teleport;

-- Define event_kind type.
-- ddl = schema changes
-- dml = data manipulation changes
-- outgoing events are created by triggers on the source
-- incoming events are created and consumed by teleport on the target
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'event_kind') THEN
		CREATE TYPE teleport.event_kind AS ENUM ('ddl', 'dml');
	END IF;
END
$$;

-- Define event_status type.
-- building = DDL/DML started and the previous state is saved
-- waiting_replication = events that need to be replayed to target
-- replicated = replicated events to target
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'event_status') THEN
		CREATE TYPE teleport.event_status AS ENUM ('building', 'waiting_batch', 'batched', 'ignored');
	END IF;
END
$$;

-- Define batch_status type.
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'batch_status') THEN
		CREATE TYPE teleport.batch_status AS ENUM ('waiting_data', 'waiting_transmission', 'transmitted', 'waiting_apply', 'applied');
	END IF;
END
$$;

-- Define batch_storage_type type.
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'batch_storage_type') THEN
		CREATE TYPE teleport.batch_storage_type AS ENUM ('db', 'fs');
	END IF;
END
$$;

-- Create table to store teleport events
CREATE TABLE IF NOT EXISTS teleport.event (
	id serial primary key,
	kind teleport.event_kind,
	status teleport.event_status,
	trigger_tag text,
	trigger_event text,
	transaction_id int,
	data text
);

-- Create table to store batches of data
CREATE TABLE IF NOT EXISTS teleport.batch (
	id serial primary key,
	status teleport.batch_status,
	storage_type teleport.batch_storage_type,
	data text,
	source text,
	target text,
	waiting_reexecution boolean not null default false
);

-- Create table to store events of a given batch
CREATE TABLE IF NOT EXISTS teleport.batch_events (
	batch_id int,
	event_id int
);

-- Returns current schema of all tables in all schemas as a JSON
-- JSON array containing each column's definition.
CREATE OR REPLACE FUNCTION get_schema() RETURNS text AS $$
BEGIN
	RETURN (
		SELECT json_agg(row_to_json(data)) FROM (
			-- The catalog pg_namespace stores namespaces. A namespace is the structure
			-- underlying SQL schemas: each namespace can have a separate collection of
			-- relations, types, etc. without name conflicts.
			SELECT
				oid AS oid,
				nspname AS schema_name,
				nspowner AS owner_id,
				(
					-- The catalog pg_class catalogs tables and most everything else that has columns
					-- or is otherwise similar to a table. This includes indexes (but see also
					-- pg_index), sequences, views, composite types, and some kinds of special
					-- relation; see relkind. Below, when we mean all of these kinds of objects we
					-- speak of "relations". Not all columns are meaningful for all relation types.
					SELECT array_to_json(array_agg(row_to_json(class)))
					FROM (
						SELECT
							oid AS oid,
							relnamespace AS namespace_oid,
							relkind AS relation_kind,
							relname AS relation_name,
							(
								-- The catalog pg_attribute stores information about table columns. There will be
								-- exactly one pg_attribute row for every column in every table in the database.
								-- (There will also be attribute entries for indexes, and indeed all objects that
								-- have pg_class entries.)
								SELECT array_to_json(array_agg(row_to_json(attr)))
								FROM (
									SELECT
										a.attrelid AS class_oid,
										a.attname AS attr_name,
										a.attnum AS attr_num,
										t.typname AS type_name,
										t.oid AS type_oid,
										COALESCE((
											SELECT (a.attnum = ANY(indkey))
											FROM pg_index i
											WHERE indrelid = a.attrelid AND indisprimary
										), false) AS is_primary_key
									FROM pg_attribute a
									INNER JOIN pg_type t
										ON a.atttypid = t.oid
								) attr
								WHERE
									attr.class_oid = class.oid AND
									-- Ordinary columns are numbered from 1 up.
									-- System columns have negative numbers.
									attr.attr_num > 0
							) AS attributes
						FROM pg_class class
						-- r = ordinary table,
						-- i = index,
						-- S = sequence,
						-- v = view,
						-- c = composite type,
						-- s = special,
						-- t = TOAST table
						WHERE class.relkind IN ('r', 'i')
					) class
					WHERE class.namespace_oid = namespace.oid
				) AS classes,
				(
					-- The catalog pg_type stores information about data types. Base types and enum
					-- types (scalar types) are created with CREATE TYPE, and domains with CREATE
					-- DOMAIN. A composite type is automatically created for each table in the
					-- database, to represent the row structure of the table. It is also possible to
					-- create composite types with CREATE TYPE AS.
					SELECT array_to_json(array_agg(row_to_json(pgtype)))
					FROM (
						SELECT
							pgtype.oid AS oid,
							pgtype.typnamespace AS namespace_oid,
							pgtype.typname AS type_name,
							(
								-- The catalog pg_attribute stores information about table columns. There will be
								-- exactly one pg_attribute row for every column in every table in the database.
								-- (There will also be attribute entries for indexes, and indeed all objects that
								-- have pg_class entries.)
								SELECT array_to_json(array_agg(row_to_json(enum)))
								FROM (
									SELECT
										oid AS oid,
										enumtypid AS type_oid,
										enumlabel AS name
									FROM pg_enum
								) enum
								WHERE
									enum.type_oid = pgtype.oid
							) AS enums
						FROM pg_type pgtype
						-- typtype is:
						-- b for a base type
						-- c for a composite type (e.g., a table's row type)
						-- d for a domain
						-- e for an enum type
						-- p for a pseudo-type
						-- r for a range type
						WHERE typtype = 'e'
					) pgtype
					WHERE pgtype.namespace_oid = namespace.oid
				) AS types
			FROM pg_namespace namespace
			WHERE
				namespace.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'teleport')
		) data
	);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_cuid() RETURNS text AS $$
BEGIN
END;
$$ LANGUAGE plpgsql;
