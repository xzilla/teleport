-- -- Creates a batch with the previous schema definition and attaches it
-- -- to a event describing a outgoing DDL change.
-- CREATE OR REPLACE FUNCTION teleport_ddl_event_start() RETURNS void AS $$
-- BEGIN
-- 	INSERT INTO teleport.event (data, kind, trigger_tag, trigger_event, transaction_id, status) VALUES
-- 	(
-- 		get_schema()::text,
-- 		'ddl',
-- 		'ddl_command_start',
-- 		'',
-- 		txid_current(),
-- 		'building'
-- 	);
-- EXCEPTION WHEN OTHERS THEN
-- END;
-- $$
-- LANGUAGE plpgsql;

-- Updates a batch and event with the schema after the DDL execution
-- and update event's status to waiting_batch
CREATE OR REPLACE FUNCTION teleport_ddl_watcher() RETURNS void AS $$
DECLARE
	event_row teleport.event%ROWTYPE;
BEGIN
	SELECT * INTO event_row FROM teleport.event WHERE status = 'building' LIMIT 1;

	IF (SELECT event_row.id IS NOT NULL) THEN
		WITH all_json_key_value AS (
			SELECT data::json AS pre, teleport_get_schema()::json AS post FROM teleport.event WHERE id = event_row.id
		)
		UPDATE teleport.event
			SET status = 'waiting_batch',
				data = (SELECT row_to_json(all_json_key_value) FROM all_json_key_value)
		WHERE id = event_row.id;
	END IF;

	INSERT INTO teleport.event (data, kind, trigger_tag, trigger_event, transaction_id, status) VALUES
	(
		teleport_get_schema()::text,
		'ddl',
		'ddl_command_start',
		'',
		txid_current(),
		'building'
	);
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

-- -- Install ddl event when it starts and ends
-- DO $$
-- BEGIN
-- 	IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'teleport_start_ddl_trigger') THEN
-- 		CREATE EVENT TRIGGER teleport_start_ddl_trigger ON ddl_command_start EXECUTE PROCEDURE teleport_ddl_event_start();
-- 		CREATE EVENT TRIGGER teleport_end_ddl_trigger ON ddl_command_end EXECUTE PROCEDURE teleport_ddl_event_end();
-- 	END IF;
-- END
-- $$;
