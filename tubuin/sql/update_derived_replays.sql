with combined_replays as (
	WITH uniq_small AS (
	    SELECT DISTINCT ON (replay_id)
	    rrc.id,
	    rrc.replay_id,
	    rrc.start_time,
	    rrc.raw_jsonb
	    FROM raw.replays_cache rrc
	    ORDER BY
	    replay_id,
	    start_time ASC    -- keep the latest per replay_id
	)
	-- 2) Full-join to get all large rows + any small-only rows:
	select
	    COALESCE(l.id, u.id) as id,
	    COALESCE(l.replay_id, u.replay_id) AS replay_id,
	    COALESCE(l.start_time, u.start_time) as start_time,
	    coalesce(l.raw_jsonb, u.raw_jsonb) as raw_jsonb
	FROM (
		select * from raw.replays
		-- filter here
		where 
		start_time >= %(timestamptz_from)s::timestamptz
		and not exists (
		        select 1
		        from derived.replays b
		        where b.replay_id = raw.replays.replay_id
		    )
		-- end filter
	) as l
	FULL JOIN uniq_small AS u
	    ON l.replay_id = u.replay_id
	order by start_time desc
),
filtered_mp as (
	select * from derived.match_players_unlogged
	where exists (
		select 1 from combined_replays
		where derived.match_players_unlogged.replay_id = combined_replays.replay_id
	) 
),
player_groups as (
  SELECT
    mp.replay_id,
    COUNT(distinct mp.player_id) AS player_count,
    COUNT(DISTINCT mp.ally_team_id) AS team_count,
    AVG(mp.skill)::real    AS avg_skill
  FROM filtered_mp mp
  where mp.user_id IS NOT null and mp.skill is not null
  GROUP BY mp.replay_id
),
replay_meta AS (
	select * from (
	  SELECT
	    r.replay_id,
	    r.start_time,
	    -- JSON extracts
	    (r.raw_jsonb ->> 'mapId')::int                             AS map_id,
	    (r.raw_jsonb -> 'Map' ->> 'scriptName')                    AS map_name,
	    (r.raw_jsonb -> 'gameSettings' ->> 'ranked_game')::boolean AS ranked,
	    -- classify by player/team counts
	    CASE
	      WHEN pg.player_count = 2 THEN '1v1'
	      WHEN pg.team_count > 2 THEN 'FFA'
	      WHEN pg.player_count BETWEEN 4 AND 10 THEN 'SmallTeam'
	      WHEN pg.player_count BETWEEN 11 AND 16 THEN 'LargeTeam'
	      ELSE NULL
	    END                                                         AS category,
	    -- lobby aggregates
	    avg_skill 												AS avg_lobby_skill,
	    pg.player_count                              			AS player_count,
	    pg.team_count                           				AS team_count,
	    -- more JSON fields
	    (r.raw_jsonb ->> 'hasBots')::boolean                      AS has_bots,
	    (r.raw_jsonb ->> 'durationMs')::int                       AS duration_ms,
	    r.raw_jsonb ->> 'engineVersion'                           AS engine_version,
	    r.raw_jsonb ->> 'gameVersion'                             AS game_version,
	    jsonb_array_length(r.raw_jsonb -> 'Spectators')           AS spectator_count
	  FROM combined_replays AS r
	  JOIN player_groups AS pg
	    ON pg.replay_id = r.replay_id
  )
)
INSERT INTO derived.replays (
  replay_id,
  start_time,
  map_id,
  map_name,
  ranked,
  category,
  avg_lobby_skill,
  player_count,
  team_count,
  has_bots,
  duration_ms,
  engine_version,
  game_version,
  spectator_count
)
SELECT
  replay_id,
  start_time,
  map_id,
  map_name,
  ranked,
  category,
  avg_lobby_skill,
  player_count,
  team_count,
  has_bots,
  duration_ms,
  engine_version,
  game_version,
  spectator_count
FROM replay_meta
ORDER BY start_time ASC
ON CONFLICT (replay_id) DO UPDATE
SET
  category         = EXCLUDED.category,
  avg_lobby_skill  = EXCLUDED.avg_lobby_skill,
  team_count       = EXCLUDED.team_count;