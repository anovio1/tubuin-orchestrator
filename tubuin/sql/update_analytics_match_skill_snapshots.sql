-- #0: HOW TO RUN THIS SCRIPT FOR THE VERY FIRST TIME (OR AFTER A DISASTER)
-- If the 'analytics.snapshot_metadata' table is empty or you need to re-process
-- all history, you MUST manually set the 'last_loaded_at' timestamp for this
-- job to a very old date eg '1970-01-01'.

-- #1: How This Script Handles Normal Downtime
-- This script is safe even if it's down for a long time (e.g., 1 week)

-- Step 1: Define our constants in one place.
-- lookback_interval is how far back we're willing to re-scan for late data
-- process_delay ensures we don't process data from the very latest minute

WITH config AS (
    SELECT
        '36 hours'::INTERVAL AS lookback_interval,
        '5 minutes'::INTERVAL AS process_delay
),
-- Step 2: Get the last successful watermark
last_watermark AS (
    SELECT last_loaded_at
    FROM analytics.snapshot_metadata
    WHERE key = 'match_skill_snapshots'
),
-- Step 3: Define the exact window of source data to scan
scan_window AS (
    SELECT
        (SELECT last_loaded_at FROM last_watermark) - (SELECT lookback_interval FROM config) AS start_ts,
        NOW() - (SELECT process_delay FROM config) AS end_ts
),
-- Step 4: Find all candidate replays
candidate_replays AS (
    SELECT replay_id, category, start_time
    FROM derived.replays r
    WHERE r.start_time >= (SELECT start_ts FROM scan_window)
      AND r.start_time < (SELECT end_ts FROM scan_window)
      AND r.category IS NOT NULL
),
-- Step 5: Prepare the candidates for insertion
candidate_set AS (
    SELECT
        mp.user_id,
        r.category,
        mp.skill,
        r.start_time
    FROM derived.match_players_unlogged mp
    JOIN candidate_replays r USING (replay_id)
    WHERE mp.user_id IS NOT NULL
),
-- Step 6: Insert the data.
inserted AS (
    INSERT INTO analytics.match_skill_snapshots (user_id, category, skill, start_time)
    SELECT user_id, category, skill, start_time
    FROM candidate_set
    ON CONFLICT (user_id, category, start_time) DO NOTHING
)
-- Step 7: Update the watermark.
-- The new watermark is the END of our scan window
UPDATE analytics.snapshot_metadata
SET last_loaded_at = (SELECT end_ts FROM scan_window)
WHERE key = 'match_skill_snapshots';