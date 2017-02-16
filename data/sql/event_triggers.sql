-- Before a ddl event is called, it saves the current schema in the events
-- table
CREATE OR REPLACE FUNCTION teleport.before_ddl_func() RETURNS event_trigger AS $$
BEGIN
    INSERT INTO teleport.event (data, kind, trigger_tag, trigger_event, transaction_id, status) VALUES
    (
	teleport.get_schema()::text,
	'ddl',
	'ddl_command_start',
	'',
	txid_current(),
	'building'
    );
END;
$$
LANGUAGE plpgsql;

DROP EVENT TRIGGER IF EXISTS teleport_before_ddl;
CREATE EVENT TRIGGER teleport_before_ddl ON ddl_command_start
EXECUTE PROCEDURE teleport.before_ddl_func();

-- After the ddl event is finished, we save the current schema, so the ddl diff
-- can correnctly create the patches for a batch.
CREATE OR REPLACE FUNCTION teleport.after_ddl_func() RETURNS event_trigger AS $$
DECLARE
	event_row teleport.event%ROWTYPE;
	post json;
BEGIN
    SELECT * INTO event_row FROM teleport.event WHERE status = 'building' and kind = 'ddl' and trigger_tag = 'ddl_command_start' ORDER BY id DESC LIMIT 1;

    post := teleport.get_schema()::json;

    IF (SELECT event_row.id IS NOT NULL) THEN
	WITH pre_post_schemas AS (SELECT event_row.data::json AS pre, post)
	UPDATE teleport.event
	SET status = 'waiting_batch',
	data = (SELECT row_to_json(pre_post_schemas) FROM pre_post_schemas)
	WHERE id = event_row.id;
    END IF;
END;
$$
LANGUAGE plpgsql;

DROP EVENT TRIGGER IF EXISTS teleport_after_ddl;
CREATE EVENT TRIGGER teleport_after_ddl ON ddl_command_end
EXECUTE PROCEDURE teleport.after_ddl_func();


