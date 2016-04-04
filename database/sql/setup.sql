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
		CREATE TYPE teleport.event_kind AS ENUM ('outgoing_ddl', 'outgoing_dml', 'incoming_ddl', 'incoming_dml');
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
		CREATE TYPE teleport.event_status AS ENUM ('building', 'waiting_batch', 'batched');
	END IF;
END
$$;

-- Define batch_status type.
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'batch_status') THEN
		CREATE TYPE teleport.batch_status AS ENUM ('waiting_transmission', 'transmitted', 'waiting_apply', 'applied');
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
	data text,
	source text,
	target text
);

-- Returns current schema of all tables in all schemas as a JSON
-- JSON array containing each column's definition.
CREATE OR REPLACE FUNCTION get_current_schema() RETURNS text AS $$
BEGIN
	RETURN (
		SELECT json_agg(row_to_json(schema))
			FROM (SELECT c.table_schema, c.table_name, c.column_name, c.data_type, c.udt_schema, c.udt_name, c.character_maximum_length, tc.constraint_type
				FROM INFORMATION_SCHEMA.COLUMNS c
				LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
				ON c.table_schema = tc.constraint_schema AND c.table_name = tc.table_name
			) AS schema
		-- Ignore postgres' and teleport internal schemas
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'teleport')
	);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_cuid() RETURNS text AS $$
BEGIN
END;
$$ LANGUAGE plpgsql;
