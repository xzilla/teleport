-- Create schema for teleport tables teleport
CREATE SCHEMA IF NOT EXISTS teleport;

-- Define Event_kind type.
-- ddl = schema changes
-- dml = data manipulation changes
-- outgoing events are created by triggers on the source
-- incoming events are created and consumed by teleport on the target
CREATE TYPE teleport.event_kind AS ENUM ('outgoing_ddl', 'outgoing_dml', 'incoming_ddl', 'incoming_dml');

-- Create table to store teleport events
CREATE TABLE IF NOT EXISTS teleport.event (
	id serial primary key,
	batch_id int,
	kind teleport.event_kind,
	trigger_tag text,
	trigger_event text
);

-- Create table to store batches of data
CREATE TABLE IF NOT EXISTS teleport.batch (
	id serial primary key,
	data text
);

-- Returns current schema of all tables in all schemas as a JSON
-- JSON array containing each column's definition.
CREATE OR REPLACE FUNCTION get_current_schema() RETURNS text AS $$
BEGIN
	RETURN (
		SELECT json_agg(row_to_json(schema))
			FROM (SELECT table_schema, table_name, column_name, data_type, udt_schema, udt_name, character_maximum_length
				FROM INFORMATION_SCHEMA.COLUMNS
			) AS schema
		-- Ignore postgres' and teleport internal schemas
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'teleport')
	);
END;
$$ LANGUAGE plpgsql;

-- Creates a batch with the new schema definition and attaches it
-- to a event describing a outgoing DDL change.
CREATE OR REPLACE FUNCTION ddl_event_end() RETURNS event_trigger AS $$
BEGIN
	WITH batch_rows AS (
		INSERT INTO teleport.batch (data) VALUES (get_current_schema()) RETURNING id
	)
	INSERT INTO teleport.event (batch_id, kind, trigger_tag, trigger_event) VALUES
	(
		(SELECT id FROM batch_rows)::integer,
		'outgoing_ddl',
		tg_tag,
		tg_event
	);
END;
$$
LANGUAGE plpgsql;

-- Install ddl event when it ends
CREATE EVENT TRIGGER teleport_end_ddl_trigger ON ddl_command_end EXECUTE PROCEDURE ddl_event_end();
