-- Creates a batch with the previous schema definition and attaches it
-- to a event describing a outgoing DDL change.
CREATE OR REPLACE FUNCTION ddl_event_start() RETURNS event_trigger AS $$
BEGIN
	INSERT INTO teleport.event (data, kind, trigger_tag, trigger_event, transaction_id, status) VALUES
	(
		get_current_schema()::text,
		'ddl',
		tg_tag,
		tg_event,
		txid_current(),
		'building'
	);
END;
$$
LANGUAGE plpgsql;

-- Updates a batch and event with the schema after the DDL execution
-- and update event's status to waiting_batch
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
		SET status = 'waiting_batch',
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
