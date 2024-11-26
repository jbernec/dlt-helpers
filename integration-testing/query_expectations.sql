
CREATE VIEW event_log_raw AS SELECT * FROM event_log(TABLE(dlt_catalog.dlt_schema.raw_farmers_market));


CREATE OR REPLACE TEMP VIEW latest_update AS SELECT origin.update_id AS id FROM event_log_raw WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1;