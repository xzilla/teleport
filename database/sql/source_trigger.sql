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

-- Creates a batch with the previous schema definition and attaches it
-- to a event describing a outgoing DDL change.
CREATE OR REPLACE FUNCTION ddl_event_start() RETURNS event_trigger AS $$
BEGIN
	INSERT INTO teleport.event (data, kind, trigger_tag, trigger_event, transaction_id, status) VALUES
	(
		get_current_schema()::text,
		'outgoing_ddl',
		tg_tag,
		tg_event,
		txid_current(),
		'building'
	);
END;
$$
LANGUAGE plpgsql;

-- Updates a batch and event with the schema after the DDL execution
-- and update event's status to waiting_replication
CREATE OR REPLACE FUNCTION ddl_event_end() RETURNS event_trigger AS $$
DECLARE
	event_row teleport.event%ROWTYPE;
BEGIN
	SELECT * INTO event_row FROM teleport.event WHERE status = 'building' AND transaction_id = txid_current();

	WITH all_json_key_value AS (
		SELECT 'pre' AS key, data::text AS value FROM teleport.event WHERE id = event_row.id
		UNION
		SELECT 'post' AS key, get_current_schema()::text AS value
	)
	UPDATE teleport.event
		SET status = 'waiting_replication',
			data = (SELECT json_object_agg(s.key, s.value)
				FROM all_json_key_value s
			)
	WHERE id = event_row.id;
END;
$$
LANGUAGE plpgsql;

-- Install ddl event when it starts and ends
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'teleport_start_ddl_trigger') THEN
		CREATE EVENT TRIGGER teleport_start_ddl_trigger ON ddl_command_start EXECUTE PROCEDURE ddl_event_start();
		CREATE EVENT TRIGGER teleport_end_ddl_trigger ON ddl_command_end EXECUTE PROCEDURE ddl_event_end();
	END IF;
END
$$;
