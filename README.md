# Query Documentation

This repository contains various SQL queries and table definitions for managing and analyzing data. Below is a detailed explanation of the purpose and functionality of each script.

## Table of Contents
- [Game Details Deduplication Query](#game-details-deduplication-query)
- [User Devices Cumulated Table](#user-devices-cumulated-table)
- [Host Activity Cumulated Table](#host-activity-cumulated-table)
- [Host Activity Reduced Table](#host-activity-reduced-table)

---

## Game Details Deduplication Query

**Purpose:**
- This query deduplicates player game details by ensuring only one record per player-game combination is kept.
- It incorporates game metadata, calculates metrics, and derives dimensional attributes such as whether the player played at home or not.

**Key Transformations:**
- Deduplication is achieved using `ROW_NUMBER()` over `game_id`, `team_id`, and `player_id`.
- Metrics such as minutes played are calculated by converting time strings into decimal values.

**Output Columns:**
- Dimensional attributes (e.g., `dim_game_date`, `dim_team_id`, `dim_is_playing_at_home`).
- Metrics (e.g., `m_pts`, `m_fga`, `m_reb`).

**Usage:**
- Use this query to populate a clean and deduplicated game details fact table.

---

## User Devices Cumulated Table

**DDL:**
```sql
CREATE TABLE user_devices_cumulated (
    user_id NUMERIC,
    device_id NUMERIC,
    browser_type TEXT,
    date DATE,
    device_activity_datelist DATE[],
    PRIMARY KEY (user_id, device_id, browser_type, date)
);
```

**Incremental Query:**
- This query updates the `user_devices_cumulated` table daily by merging new event data with historical data.
- Ensures deduplication for daily activity and appends the date to an array of device activity dates (`device_activity_datelist`).

**Key Logic:**
- Deduplication for today's data using `ROW_NUMBER()`.
- Merging historical and new data via `FULL OUTER JOIN`.
- Conditional logic for updating `device_activity_datelist`.

**Output Columns:**
- `device_activity_datelist` provides a historical log of activity dates for each user-device-browser combination.

---

## Host Activity Cumulated Table

**DDL:**
```sql
CREATE TABLE hosts_cumulated (
    host TEXT,
    month_start DATE,
    host_activity_datelist DATE[],
    PRIMARY KEY (host, month_start)
);
```

**Incremental Query:**
- This query updates host activity by appending daily activity to `host_activity_datelist`.

**Key Logic:**
- Aggregates daily activity per host.
- Merges with historical data to update `host_activity_datelist`.

**Conflict Handling:**
- On conflict, the existing record is updated with the new list of activity dates.

---

## Host Activity Reduced Table

**DDL:**
```sql
CREATE TABLE host_activity_reduced (
    host TEXT,
    month_start DATE,
    hit_array BIGINT[],
    unique_visitors BIGINT[],
    PRIMARY KEY (host, month_start)
);
```

**Incremental Query:**
- Loads reduced host activity metrics including daily hits and unique visitors.
- Updates arrays (`hit_array`, `unique_visitors`) with daily values.

**Key Logic:**
- Creates zero-filled arrays for missing days.
- Appends daily metrics to historical arrays for the given host and month.

**Usage:**
- Ideal for tracking and analyzing host activity trends over time.
