-- Updates a batch and event with the schema after the DDL execution
-- and update event's status to waiting_batch
CREATE OR REPLACE FUNCTION teleport.ddl_watcher() RETURNS void AS $$
DECLARE
	event_row teleport.event%ROWTYPE;
	data json;
BEGIN
	SELECT * INTO event_row FROM teleport.event WHERE status = 'building' and kind = 'ddl' and trigger_tag = 'ddl_command_start' LIMIT 1;

	data := teleport.get_schema()::json;

	IF (SELECT event_row.id IS NOT NULL) THEN
		WITH all_json_key_value AS (
			SELECT event_row.data::json AS pre, data AS post
		)
		UPDATE teleport.event
			SET status = 'waiting_batch',
				data = (SELECT row_to_json(all_json_key_value) FROM all_json_key_value)
		WHERE id = event_row.id;
	END IF;

	INSERT INTO teleport.event (data, kind, trigger_tag, trigger_event, transaction_id, status) VALUES
	(
		data::text,
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

