-- 1 query to deduped game details
-- This query combines game details with game metadata to remove duplicate player records
-- for each game, ensuring only the first occurrence (based on game date) is kept.
WITH deduped AS (
    SELECT
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*, 
        ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) as row_num
    FROM game_details gd 
    JOIN games g ON gd.game_id = g.game_id
)
SELECT 
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,    
    (team_id = home_team_id) AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP' IN comment), 0) > 0 AS dim_did_not_play,
    COALESCE(POSITION('DND' IN comment), 0) > 0 AS dim_did_not_dress,
    COALESCE(POSITION('NWT' IN comment), 0) > 0 AS dim_did_not_team,
    CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    "TO" AS m_turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1;

-- DDL for users devices cumulated
CREATE TABLE user_devices_cumulated (
	user_id NUMERIC,
	device_id NUMERIC,
	browser_type TEXT,
	date DATE,
	device_activity_datelist DATE[],
	PRIMARY KEY (user_id, device_id, browser_type, date)
)

-- A DDL for an user_devices_cumulated table that has
-- This query processes events data to deduplicate today's device activity and merge it with historical data.
INSERT INTO user_devices_cumulated
WITH today AS (
	SELECT 
		e.user_id,
		DATE(e.event_time) AS date,
		e.device_id,
		d.browser_type,
		ROW_NUMBER() OVER (PARTITION BY e.user_id, e.device_id, d.browser_type) AS row_num
	FROM events AS e
	LEFT JOIN devices AS d
	ON e.device_id = d.device_id
	WHERE DATE(e.event_time) = ('2023-01-31')
	AND e.user_id IS NOT NULL
	AND e.device_id IS NOT NULL
	
), deduped_today AS (

	SELECT
	*
	FROM today
	WHERE row_num = 1
), yesterday AS (

	SELECT
	*
	FROM user_devices_cumulated
	WHERE date = DATE('2023-01-30')
)
SELECT 
	COALESCE(t.user_id, y.user_id) AS user_id,
	COALESCE(t.device_id, y.device_id) AS device_id,
	COALESCE(t.browser_type, y.browser_type) AS browser_type,
	COALESCE(t.date, y.date + 1) AS date,
	CASE 
		WHEN y.device_activity_datelist IS NULL
		THEN ARRAY[t.date]
		WHEN t.date IS NULL
		THEN y.device_activity_datelist
		ELSE y.device_activity_datelist || ARRAY[t.date]
	END AS device_activity_datelist
FROM deduped_today AS t
FULL OUTER JOIN yesterday AS y
ON t.user_id = y.user_id
AND t.device_id = y.device_id
AND t.browser_type = y.browser_type;

-- A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column
WITH user_devices AS (
	SELECT
	*
	FROM user_devices_cumulated
	WHERE date = DATE('2023-01-31')
), series AS (

	SELECT * FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
), place_holder_ints AS (
	SELECT 
		CASE 
			WHEN device_activity_datelist @> ARRAY[DATE(s.series_date)]
			THEN CAST(POW(2, 32 -(date - DATE(s.series_date))-1) AS BIGINT)
			ELSE 0
		END AS placeholder_int_value, *
	FROM user_devices AS ud
	CROSS JOIN series AS s
)
SELECT 
	user_id,
	device_id,
	browser_type,
	device_activity_datelist,
	CAST(CAST(SUM(p.placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int
FROM place_holder_ints AS p
GROUP BY user_id, device_id, browser_type, device_activity_datelist;

-- A DDL for hosts_cumulated table
CREATE TABLE hosts_cumulated (
	host TEXT,
	month_start DATE,
	host_activity_datelist DATE[],
	PRIMARY KEY (host, month_start)
);

-- The incremental query to generate host_activity_datelist
-- Incremental update query for hosts_cumulated
INSERT INTO hosts_cumulated
WITH today AS (
	SELECT 
		host,
		DATE(event_time) AS date
	FROM events
	WHERE DATE(event_time) = DATE ('2023-01-10')
	GROUP BY host,DATE(event_time)
	), yesterday AS(
	SELECT 
		*
	FROM hosts_cumulated
	WHERE month_start = DATE('2023-01-01')
)
SELECT
	COALESCE(t.host,y.host) AS host,
	COALESCE(DATE_TRUNC('month', t.date), y.month_start) AS month_start,
	CASE 
		WHEN y.host_activity_datelist IS NULL
		THEN ARRAY[t.date]
		WHEN t.date IS NULL
		THEN y.host_activity_datelist
		ELSE y.host_activity_datelist || ARRAY[t.date]
	END AS host_activity_datelist
FROM today AS t
FULL OUTER JOIN yesterday AS y
ON t.host = y.host
ON CONFLICT(host, month_start)
DO
	UPDATE SET host_activity_datelist = EXCLUDED.host_activity_datelist


-- A monthly, reduced fact table DDL host_activity_reduced
-- Stores reduced fact data for hosts including unique visitors and hit counts by month.
CREATE TABLE host_activity_reduced (
	host TEXT,
	month_start DATE,
	hit_array BIGINT[],
	unique_visitors BIGINT[],
	PRIMARY KEY (host, month_start)
)

--An incremental query that loads host_activity_reduced
INSERT INTO host_activity_reduced
WITH today AS (
	SELECT 
		host,
		DATE(event_time) AS date,
		COUNT(DISTINCT(user_id)) AS unique_visitors,
		COUNT(1) AS hits
	FROM events
	WHERE DATE(event_time) = DATE ('2023-01-10')
	GROUP BY host,DATE(event_time)
	), yesterday AS(
	SELECT 
		*
	FROM host_activity_reduced
	WHERE month_start = DATE('2023-01-01')
)
SELECT
	COALESCE(t.host,y.host) AS host,
	COALESCE(DATE_TRUNC('month', t.date), y.month_start) AS month_start,
	CASE 
		WHEN y.unique_visitors IS NOT NULL
		THEN y.unique_visitors || ARRAY[COALESCE(t.unique_visitors,0)]
		WHEN y.unique_visitors IS NULL
		THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month',date)), 0)]) || ARRAY[COALESCE(t.hits,0)]
	END AS unique_visitors,
	CASE 
		WHEN y.hit_array IS NOT NULL
		THEN y.hit_array || ARRAY[COALESCE(t.hits,0)]
		WHEN y.hit_array IS NULL
		THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month',date)), 0)]) || ARRAY[COALESCE(t.unique_visitors,0)]
	END AS hit_array
FROM today AS t
FULL OUTER JOIN yesterday AS y
ON t.host = y.host
ON CONFLICT(host, month_start)
DO
	UPDATE SET unique_visitors = EXCLUDED.unique_visitors,
		hit_array = EXCLUDED.hit_array












