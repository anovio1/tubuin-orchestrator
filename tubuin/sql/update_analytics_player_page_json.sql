-- ### Project Summary: Player Page Data Generation
-- #### What: What is this query?
-- This is a powerful SQL script that builds the complete data "snapshot" for every active player in one go. It calculates everything needed for a player's profile page: their win/loss stats, their skill ratings, their recent match history, and more, packaging it all into a single JSON object for each person.

-- #### Why: Why did we build this?
-- This new SQL script replaces an older, slower Python script. The goal was to make generating the player page data much faster and more efficient by doing all the heavy lifting directly inside the PostgreSQL database, which is what it's built for.

-- #### Tables Required:
-- *   `derived.replays`
-- *   `derived.match_players_unlogged`
-- *   `derived.match_skill_deltas`
-- *   `analytics.match_skill_snapshots`

-- #### Steps Forward:
-- Our goal is to set up an automated "ETL" job using Prefect. The best way to do this is with a "Decoupled" architecture.
-- **ELI5 of the Decoupled Architecture:**
-- Imagine you're making a big report about a student.
-- *   Instead of writing one giant report from scratch every day, you have different helpers.
-- *   **Helper 1** just looks at **today's attendance** to see which students were even here.
-- *   **Helper 2** then takes that short list of students and calculates their **grades for the whole semester**.
-- *   **Helper 3** takes the same short list and looks up their **sports stats for the entire year**.

-- Our SQL query will work the same way:
-- 1.  It will quickly find players active in just the **last 24 hours**.
-- 2.  For those few players, it will then look back **6 months** into our pre-calculated `analytics` tables to get their full skill history and other long-term stats.

-- This makes our daily job very fast and light, while still providing the rich, historical data we need for the player pages. The next step is to break our big SQL script into these smaller, independent "helper" scripts and have Prefect run them.
-- #############################################################################
-- # 4.1 - SET-BASED SQL with DENSE SKILL HISTORY
-- # Aims for closer parity with Python script's skillHistory output.
-- # WARNING: The skillHistory part will be VERY resource-intensive.
-- #############################################################################
-- Step 1: Define your processing window parameters
WITH processing_params AS (
    SELECT
        -- DATE '2025-01-01' AS from_date, -- Front end view is coupled to this range, 6 months here = 6 months front end view
        -- DATE '2025-05-31' AS to_date
        %(timestamptz_from)s::timestamptz AS from_date,
        %(timestamptz_to)s::timestamptz AS to_date
),
date_boundaries AS (
    SELECT
        pp.from_date, pp.to_date,
        (pp.from_date)::TIMESTAMPTZ AS from_timestamptz,
        (pp.to_date + INTERVAL '1 day')::TIMESTAMPTZ AS to_timestamptz
    FROM processing_params pp
),
constants AS (
    SELECT
        28.0::REAL AS hard_game_os_threshold,
        10.0::REAL AS k_factor,
        0.3::REAL AS min_hard_game_pct, ARRAY(SELECT generate_series(0, 58, 2)) AS lobby_os_bins
),
active_users AS (
    SELECT DISTINCT mpu.user_id
    FROM derived.match_players_unlogged mpu
    JOIN derived.replays r ON mpu.replay_id = r.replay_id
    CROSS JOIN date_boundaries db
    WHERE mpu.user_id IS NOT NULL
      AND r.start_time >= db.from_timestamptz AND r.start_time < db.to_timestamptz
),
all_relevant_matches AS (
    SELECT
        mpu.user_id, mpu.replay_id, mpu.name AS username_in_match,
        mpu.skill AS player_skill_in_match, mpu.won, mpu.faction,
        r.start_time, r.category, r.map_name, r.ranked, r.avg_lobby_skill
    FROM derived.match_players_unlogged mpu
    JOIN derived.replays r ON mpu.replay_id = r.replay_id
    WHERE mpu.user_id IN (SELECT user_id FROM active_users)
      AND r.start_time >= (SELECT from_timestamptz FROM date_boundaries)
      AND r.start_time < (SELECT to_timestamptz FROM date_boundaries)
      AND r.category IS NOT NULL
),
--  Component for: OS, gets final os per mode
--      now uses (and relies on derived.match_skill_deltas, a materialized view)
-- latest_skill_from_deltas AS (
--     SELECT DISTINCT ON (user_id, category)
--         user_id, category, latest_skill AS skill_from_deltas_table
--     FROM derived.match_skill_deltas
--     WHERE user_id IN (SELECT user_id FROM active_users)
--     ORDER BY user_id, category, latest_played_time DESC
-- ),
-- latest_skill_from_matches_in_period AS (
--     SELECT DISTINCT ON (user_id, category)
--            user_id, category, player_skill_in_match AS skill_from_matches
--     FROM all_relevant_matches
--     ORDER BY user_id, category, start_time DESC
-- ),
-- final_skill_per_mode AS (
--     SELECT
--         -- Base list of all user/category pairs that we need a skill for.
--         pairs.user_id,
--         pairs.category,
--         -- Use COALESCE to implement the fallback logic.
--         COALESCE(deltas.skill_from_deltas_table, matches.skill_from_matches) AS current_os
--     FROM
--         (SELECT DISTINCT user_id, category FROM all_relevant_matches) AS pairs
--     LEFT JOIN latest_skill_from_deltas AS deltas
--         ON pairs.user_id = deltas.user_id AND pairs.category = deltas.category
--     LEFT JOIN latest_skill_from_matches_in_period AS matches
--         ON pairs.user_id = matches.user_id AND pairs.category = matches.category
-- ),
final_skill_per_mode AS (
    SELECT user_id, category, latest_skill AS current_os
    FROM derived.match_skill_deltas
    WHERE user_id IN (SELECT user_id FROM active_users)
),
-- == COMPONENT CTEs (Calculate stats for ALL active users at once) ==
-- Component for: skillPerMode
users_skill_per_mode_components AS (
    SELECT
        arm.user_id,
        arm.category,
        -- Aggregation remains the same
        MAX(arm.start_time) AS last_played_timestamp_in_period,
        COUNT(*) FILTER (WHERE arm.avg_lobby_skill >= c.hard_game_os_threshold) AS ga,
        COUNT(*) FILTER (WHERE arm.avg_lobby_skill >= c.hard_game_os_threshold AND arm.won) AS wa_count,
        COUNT(*) FILTER (WHERE arm.avg_lobby_skill < c.hard_game_os_threshold) AS gb,
        COUNT(*) FILTER (WHERE arm.avg_lobby_skill < c.hard_game_os_threshold AND arm.won) AS wb_count,
        COUNT(*) AS total_games_in_mode
    FROM all_relevant_matches arm
    CROSS JOIN constants c
    GROUP BY arm.user_id, arm.category
),
users_skill_per_mode_with_as AS (
    SELECT
        s.user_id,
        s.category,
        -- Get the final skill from our new CTE, with a final fallback to 0.0
        COALESCE(fspm.current_os, 0.0) AS current_os,
        s.last_played_timestamp_in_period, s.ga, s.gb, s.total_games_in_mode,
        s.wa_count, s.wb_count,
        CASE WHEN s.ga > 0 THEN ROUND((s.wa_count::DECIMAL / s.ga), 2) ELSE 0.0 END AS wa_rate,
        CASE WHEN s.gb > 0 THEN ROUND((s.wb_count::DECIMAL / s.gb), 2) ELSE 0.0 END AS wb_rate,
        CASE -- Adjusted OS uses the new, correct current_os
            WHEN s.total_games_in_mode = 0 THEN COALESCE(fspm.current_os, 0.0)
            WHEN s.ga::REAL / NULLIF(s.total_games_in_mode, 0) < c.min_hard_game_pct THEN COALESCE(fspm.current_os, 0.0) - c.k_factor * 0.5
            ELSE COALESCE(fspm.current_os, 0.0) + ((((CASE WHEN s.ga > 0 THEN s.wa_count::REAL / s.ga ELSE 0 END * s.ga) + (CASE WHEN s.gb > 0 THEN s.wb_count::REAL / s.gb ELSE 0 END * s.gb)) / NULLIF(s.total_games_in_mode, 0)) - 0.5) * c.k_factor + (CASE WHEN (CASE WHEN s.ga > 0 THEN s.wa_count::REAL / s.ga ELSE 0 END) > (CASE WHEN s.gb > 0 THEN s.wb_count::REAL / s.gb ELSE 0 END) THEN ((CASE WHEN s.ga > 0 THEN s.wa_count::REAL / s.ga ELSE 0 END) - (CASE WHEN s.gb > 0 THEN s.wb_count::REAL / s.gb ELSE 0 END)) * (c.k_factor * 0.25) ELSE 0 END)
        END AS adjusted_os
    FROM users_skill_per_mode_components s
    -- Join the final skill data to the aggregated stats
    LEFT JOIN final_skill_per_mode fspm ON s.user_id = fspm.user_id AND s.category = fspm.category
    CROSS JOIN constants c
),
users_skill_per_mode_json AS (
    SELECT user_id, jsonb_object_agg(category, jsonb_build_object('mode', category, 'os', ROUND(current_os::numeric, 1), 'as', ROUND(adjusted_os::numeric, 2), 'ga', ga, 'wa', wa_rate, 'gb', gb, 'wb', wb_rate, 'lastPlayed', to_char(last_played_timestamp_in_period, 'YYYY-MM-DD'))) AS data
    FROM users_skill_per_mode_with_as GROUP BY user_id
),
-- Component for: winLossSummary (perMode and total)
users_win_loss_summary_components AS (
    SELECT arm.user_id, arm.category, COUNT(*) FILTER (WHERE arm.won) AS wins, COUNT(*) FILTER (WHERE NOT arm.won) AS losses
    FROM all_relevant_matches arm GROUP BY arm.user_id, arm.category
),
users_win_loss_summary_json AS (
    SELECT user_id,
           jsonb_build_object(
               'total', jsonb_build_object('wins', SUM(wins), 'losses', SUM(losses), 'games', SUM(wins+losses), 'winRate', CASE WHEN SUM(wins+losses) > 0 THEN ROUND((SUM(wins)::DECIMAL*100 / SUM(wins+losses)),1) ELSE 0 END),
               'perMode', jsonb_object_agg(category, jsonb_build_object('mode', category, 'wins', wins, 'losses', losses, 'rate', CASE WHEN wins+losses > 0 THEN ROUND((wins::DECIMAL*100 / (wins+losses)),1) ELSE 0 END) ORDER BY category)
           ) AS data
    FROM users_win_loss_summary_components GROUP BY user_id
),
-- Component for: matchHistory
users_match_history_ranked AS (
    SELECT arm.user_id,
           to_char(arm.start_time, 'DD Mon YYYY') AS date_str, arm.category AS mode, arm.map_name AS map,
           CASE WHEN arm.won THEN 'Win' ELSE 'Loss' END AS result, arm.faction, arm.ranked, arm.username_in_match AS username,
           ROW_NUMBER() OVER (PARTITION BY arm.user_id ORDER BY arm.start_time DESC) as rn
    FROM all_relevant_matches arm
),
users_match_history_json AS (
    SELECT user_id, jsonb_agg(jsonb_build_object('date', date_str, 'mode', mode, 'map', map, 'result', result, 'faction', faction, 'ranked', ranked, 'username', username) ORDER BY rn ASC) AS data
    FROM users_match_history_ranked WHERE rn <= 10 GROUP BY user_id
),
-- Component for: usernames
users_usernames_agg AS (
    SELECT arm.user_id, arm.username_in_match AS name, MAX(arm.start_time) AS last_seen_timestamp
    FROM all_relevant_matches arm WHERE arm.username_in_match IS NOT NULL GROUP BY arm.user_id, arm.username_in_match
),
users_usernames_json AS (
    SELECT user_id, jsonb_agg(jsonb_build_object('name', name, 'lastSeen', to_char(last_seen_timestamp, 'YYYY-MM-DD')) ORDER BY last_seen_timestamp DESC) AS data
    FROM users_usernames_agg GROUP BY user_id
),
-- Component for: activityData
users_activity_data_daily_agg AS (
    SELECT arm.user_id, date_trunc('day', arm.start_time) AS activity_date, COUNT(*) AS games, AVG(arm.player_skill_in_match) AS avg_skill
    FROM all_relevant_matches arm GROUP BY arm.user_id, date_trunc('day', arm.start_time)
),
users_activity_data_json_intermediate AS (
    SELECT user_id, activity_date, games, avg_skill,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date DESC) as rn
    FROM users_activity_data_daily_agg
),
users_activity_data_json AS (
    SELECT user_id, jsonb_object_agg(to_char(activity_date, 'YYYY-MM-DD'), jsonb_build_object('games', games, 'avgSkill', ROUND(avg_skill::numeric,1)) ORDER BY activity_date DESC) AS data
    FROM users_activity_data_json_intermediate
    WHERE rn <= 84 GROUP BY user_id
),
-- Component for: lobbyData
users_lobby_data_binned AS (
    SELECT arm.user_id, arm.category,
           width_bucket(arm.avg_lobby_skill, c.lobby_os_bins) -1  as bin_index,
           COUNT(arm.replay_id) as game_count
    FROM all_relevant_matches arm CROSS JOIN constants c
    WHERE arm.avg_lobby_skill IS NOT NULL GROUP BY arm.user_id, arm.category, bin_index
    
    UNION ALL -- Add this block

    SELECT arm.user_id, 'All' AS category, -- Hardcode the 'All' category
           width_bucket(arm.avg_lobby_skill, c.lobby_os_bins) -1  as bin_index,
           COUNT(arm.replay_id) as game_count
    FROM all_relevant_matches arm CROSS JOIN constants c
    WHERE arm.avg_lobby_skill IS NOT NULL GROUP BY arm.user_id, bin_index -- Group without category
),
users_lobby_data_inner_json AS ( -- Step 1 for lobbyData: aggregate counts per user/category
    SELECT
        all_user_cats.user_id, all_user_cats.category,
        COALESCE(jsonb_agg(COALESCE(game_count, 0) ORDER BY bin_idx), '[]'::jsonb) as counts_json_array
    FROM (SELECT DISTINCT user_id, category FROM users_lobby_data_binned) all_user_cats
    CROSS JOIN generate_series(0, array_length((SELECT lobby_os_bins FROM constants), 1) -1 ) all_bins(bin_idx)
    CROSS JOIN constants c
    LEFT JOIN users_lobby_data_binned uldb
      ON all_user_cats.user_id = uldb.user_id AND all_user_cats.category = uldb.category AND all_bins.bin_idx = uldb.bin_index
    GROUP BY all_user_cats.user_id, all_user_cats.category
),
users_lobby_data_json AS ( -- Step 2 for lobbyData: aggregate category JSONs per user
    SELECT user_id, jsonb_object_agg(category, counts_json_array ORDER BY CASE WHEN category = 'All' THEN 0 ELSE 1 END, category) AS data
    FROM users_lobby_data_inner_json GROUP BY user_id
),
-- Component for: factionData (Applying Refinement 2)
users_faction_data_agg AS (
    SELECT arm.user_id, arm.category, arm.faction, COUNT(*) FILTER (WHERE arm.won) AS wins, COUNT(*) FILTER (WHERE NOT arm.won) AS losses
    FROM all_relevant_matches arm WHERE arm.faction IS NOT NULL GROUP BY arm.user_id, arm.category, arm.faction

    UNION ALL -- Add this block

    SELECT arm.user_id, 'All' AS category, arm.faction, COUNT(*) FILTER (WHERE arm.won) AS wins, COUNT(*) FILTER (WHERE NOT arm.won) AS losses
    FROM all_relevant_matches arm WHERE arm.faction IS NOT NULL GROUP BY arm.user_id, arm.faction -- Group without category
),
users_faction_data_inner_json AS ( -- Step 1 for factionData: build JSON for each category within each user
    SELECT
        user_id, category,
        jsonb_build_object(
            'labels',   COALESCE(jsonb_agg(DISTINCT faction ORDER BY faction), '[]'::jsonb),
            'datasets', jsonb_build_array(
                            jsonb_build_object('label', 'Wins', 'data', COALESCE(jsonb_agg(COALESCE(wins,0) ORDER BY faction), '[]'::jsonb)),
                            jsonb_build_object('label', 'Losses', 'data', COALESCE(jsonb_agg(COALESCE(losses,0) ORDER BY faction), '[]'::jsonb))
                        )
        ) as category_faction_data_json
    FROM users_faction_data_agg
    GROUP BY user_id, category
),
users_faction_data_json AS ( -- Step 2 for factionData: aggregate category JSONs per user
    SELECT user_id, jsonb_object_agg(category, category_faction_data_json ORDER BY CASE WHEN category = 'All' THEN 0 ELSE 1 END, category) AS data
    FROM users_faction_data_inner_json
    GROUP BY user_id
),
-- Component for: skillHistory (Dense Grid with Carry-Forward)
-- Step 1: Get daily average skills for each user/category within the period
users_daily_avg_skill_snapshots AS (
    SELECT
        mss.user_id,
        mss.category,
        date_trunc('day', mss.start_time)::DATE AS history_date,
        AVG(mss.skill) AS avg_daily_skill
    FROM analytics.match_skill_snapshots mss
    CROSS JOIN date_boundaries db
    WHERE mss.user_id IN (SELECT user_id FROM active_users) -- Only for active users
      AND mss.start_time >= db.from_timestamptz
      AND mss.start_time < db.to_timestamptz
    GROUP BY mss.user_id, mss.category, date_trunc('day', mss.start_time)::DATE
),
-- Step 2: Generate all dates in the processing period (for global labels)
all_period_dates AS (
    SELECT generate_series(
               (SELECT from_date FROM processing_params),
               (SELECT to_date FROM processing_params),
               '1 day'::interval
           )::DATE AS history_date
),
-- Step 3: Create a grid of (user_id, category, all_period_dates)
-- Only for user/category combinations that actually have some data in users_daily_avg_skill_snapshots
user_category_date_grid AS (
    SELECT
        ucp.user_id,
        ucp.category,
        apd.history_date
    FROM (SELECT DISTINCT user_id, category FROM users_daily_avg_skill_snapshots) ucp
    CROSS JOIN all_period_dates apd
),
-- Step 4: Left join actual skills to the grid and apply LAG to carry forward
user_skill_history_filled_grid AS (
    SELECT
        ucdg.user_id,
        ucdg.category,
        ucdg.history_date,
        COALESCE(
            udass.avg_daily_skill,
            LAG(udass.avg_daily_skill, 1, 0.0) OVER (PARTITION BY ucdg.user_id, ucdg.category ORDER BY ucdg.history_date)
        ) AS filled_avg_skill
    FROM user_category_date_grid ucdg
    LEFT JOIN users_daily_avg_skill_snapshots udass
        ON ucdg.user_id = udass.user_id
       AND ucdg.category = udass.category
       AND ucdg.history_date = udass.history_date
),
-- Step 5: Aggregate into the final JSON structure per user
users_skill_history_json AS (
    SELECT
        g.user_id,
        jsonb_build_object(
            'labels', (SELECT COALESCE(jsonb_agg(to_char(apd.history_date, 'YYYY-MM-DD') ORDER BY apd.history_date), '[]'::jsonb)
                       FROM all_period_dates apd),
            'datasets', COALESCE(jsonb_agg(
                            jsonb_build_object(
                                'label', g.category,
                                'data', g.skill_data_array
                            ) ORDER BY g.category
                        ), '[]'::jsonb)
        ) AS data
    FROM (
        SELECT
            user_id,
            category,
            jsonb_agg(ROUND(filled_avg_skill::numeric, 1) ORDER BY history_date) AS skill_data_array
        FROM user_skill_history_filled_grid
        GROUP BY user_id, category
    ) g
    GROUP BY g.user_id
)
-- == FINAL ASSEMBLY AND INSERT ==
INSERT INTO analytics.player_page_json_v4 (user_id, json_data, data_start_date, data_end_date, last_generated_at)
SELECT
    au.user_id,
    jsonb_build_object(
        '_version', 1, '_generated', now(), 'userId', au.user_id,
        'skillPerMode', COALESCE(uspmj.data, '{}'::jsonb),
        'winLossSummary', COALESCE(uwlsj.data, jsonb_build_object('total',jsonb_build_object('wins',0,'losses',0,'games',0,'winRate',0.0),'perMode','{}'::jsonb)),
        'matchHistory', COALESCE(umhj.data, '[]'::jsonb),
        'usernames', COALESCE(uuj.data, '[]'::jsonb),
        'activityData', COALESCE(uadj.data, '{}'::jsonb),
        'lobbyData', COALESCE(uldj.data, '{}'::jsonb),
        'factionData', COALESCE(ufdj.data, '{}'::jsonb),
        'skillHistory', COALESCE(ushj.data, jsonb_build_object('labels', '[]'::jsonb, 'datasets', '[]'::jsonb)) -- Using the new dense skill history
--        'toxicity', '{"composite": {"value": 0.42, "delta": "+0.05", "percentile": "84th"}, "total": {"value": 1375, "delta": "+82", "percentile": "76th"}, "uhOhs": {"value": 47, "delta": "+3", "percentile": "68th"}}'::jsonb
    ) AS final_generated_json,
    (SELECT from_date FROM processing_params),
    (SELECT to_date FROM processing_params),
    now()
FROM active_users au
LEFT JOIN users_skill_per_mode_json uspmj ON au.user_id = uspmj.user_id
LEFT JOIN users_win_loss_summary_json uwlsj ON au.user_id = uwlsj.user_id
LEFT JOIN users_match_history_json umhj ON au.user_id = umhj.user_id
LEFT JOIN users_usernames_json uuj ON au.user_id = uuj.user_id
LEFT JOIN users_activity_data_json uadj ON au.user_id = uadj.user_id
LEFT JOIN users_lobby_data_json uldj ON au.user_id = uldj.user_id
LEFT JOIN users_faction_data_json ufdj ON au.user_id = ufdj.user_id
LEFT JOIN users_skill_history_json ushj ON au.user_id = ushj.user_id -- This now joins the dense skill history
ON CONFLICT (user_id) DO UPDATE SET
    json_data = EXCLUDED.json_data,
    data_start_date = EXCLUDED.data_start_date,
    data_end_date = EXCLUDED.data_end_date,
    last_generated_at = now();