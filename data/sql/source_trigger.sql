-- Creates a batch with the previous schema definition and attaches it
-- to a event describing a outgoing DDL change.
CREATE OR REPLACE FUNCTION teleport_ddl_event_start() RETURNS event_trigger AS $$
BEGIN
	INSERT INTO teleport.event (data, kind, trigger_tag, trigger_event, transaction_id, status) VALUES
	(
		get_schema()::text,
		'ddl',
		tg_event,
		tg_tag,
		txid_current(),
		'building'
	);
EXCEPTION WHEN OTHERS THEN
END;
$$
LANGUAGE plpgsql;

-- Updates a batch and event with the schema after the DDL execution
-- and update event's status to waiting_batch
CREATE OR REPLACE FUNCTION teleport_ddl_event_end() RETURNS event_trigger AS $$
DECLARE
	event_row teleport.event%ROWTYPE;
BEGIN
	SELECT * INTO event_row FROM teleport.event WHERE status = 'building' AND transaction_id = txid_current();

	WITH all_json_key_value AS (
		SELECT 'pre' AS key, data::json AS value FROM teleport.event WHERE id = event_row.id
		UNION ALL
		SELECT 'post' AS key, get_schema()::json AS value
	)
	UPDATE teleport.event
		SET status = 'waiting_batch',
			data = (SELECT json_object_agg(s.key, s.value)
				FROM all_json_key_value s
			)
	WHERE id = event_row.id;
EXCEPTION WHEN OTHERS THEN
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION teleport_dml_event() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO teleport.event (data, kind, trigger_tag, trigger_event, transaction_id, status) VALUES
	(
		(
			SELECT row_to_json(data) FROM (
				SELECT
					(CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE OLD END) as pre,
					(CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE NEW END) as post
			) data
		)::text,
		-- row_to_json(CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END)::text,
		'dml',
		CONCAT(TG_TABLE_SCHEMA, '.', TG_RELNAME),
		TG_OP,
		txid_current(),
		'waiting_batch'
	);
	RETURN NULL;
EXCEPTION WHEN OTHERS THEN
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Install ddl event when it starts and ends
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'teleport_start_ddl_trigger') THEN
		CREATE EVENT TRIGGER teleport_start_ddl_trigger ON ddl_command_start EXECUTE PROCEDURE teleport_ddl_event_start();
		CREATE EVENT TRIGGER teleport_end_ddl_trigger ON ddl_command_end EXECUTE PROCEDURE teleport_ddl_event_end();
	END IF;
END
$$;
