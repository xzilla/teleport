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
		CREATE TYPE teleport.event_status AS ENUM ('building', 'waiting_replication', 'replicated');
	END IF;
END
$$;

-- Create table to store teleport events
CREATE TABLE IF NOT EXISTS teleport.event (
	id serial primary key,
	batch_id int,
	kind teleport.event_kind,
	status teleport.event_status,
	trigger_tag text,
	trigger_event text,
	transaction_id int
);

-- Create table to store batches of data
CREATE TABLE IF NOT EXISTS teleport.batch (
	id serial primary key,
	data text
);
