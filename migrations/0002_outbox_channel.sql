-- The outbox router derives the destination topic from a column. Routing by
-- aggregate_type puts every event of a domain on one topic; routing by a
-- dedicated channel column lets the domain split its events across as many
-- topics as it needs, without the connector knowing any event type.
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS channel TEXT NOT NULL DEFAULT '';

-- Existing rows keep their behaviour: one topic per aggregate type.
UPDATE outbox SET channel = aggregate_type WHERE channel = '';

-- A row with an empty channel would route to `business..events`, so make the
-- fallback explicit rather than leaving it to the writer.
ALTER TABLE outbox ALTER COLUMN channel DROP DEFAULT;
ALTER TABLE outbox ADD CONSTRAINT outbox_channel_not_blank CHECK (channel <> '');
