-- move.raw.replays_cache.replays.sql
WITH to_move AS (
  SELECT replay_id, start_time, raw_jsonb
  FROM raw.replays_cache
  WHERE start_time < NOW() - INTERVAL '24 hours'
  ORDER BY start_time ASC
  LIMIT 10000
),
deleted AS (
  DELETE FROM raw.replays_cache
  USING to_move
  WHERE raw.replays_cache.replay_id = to_move.replay_id
  RETURNING raw.replays_cache.replay_id, raw.replays_cache.start_time, raw.replays_cache.raw_jsonb
)
INSERT INTO raw.replays (replay_id, start_time, raw_jsonb)
SELECT replay_id, start_time, raw_jsonb
FROM deleted
ORDER BY start_time ASC
ON CONFLICT (replay_id) DO NOTHING;