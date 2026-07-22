CREATE UNIQUE INDEX IF NOT EXISTS uq_stream_event_ai_digest_date
ON stream_event (code, (payload -> 'digest' ->> 'date'))
WHERE type = 'AI';
