# tubuin\sql\commands.py
from sql.queries import *
from sql.sql_command import SQLCommand


class MoveRawReplays(SQLCommand):
    label = "Move Raw Replays"
    query = move_raw_replays_cache_replays


class UpdateDerivedMatchPlayers(SQLCommand):
    label = "Update Derived Match Players"
    query = update_derived_match_player_unlogged


class UpdateDerivedReplays(SQLCommand):
    label = "Update Derived Replays"
    query = update_derived_replays


class UpdateAnalyticsSkillSnapshots(SQLCommand):
    label = "Update Analytics Match Skill Snapshots"
    query = update_analytics_match_skill_snapshots


class RefreshMaterialSkillDeltas(SQLCommand):
    label = "Refresh Materialized View: Match Skill Deltas"
    commit = False
    query = refresh_mat_derived_match_skill_deltas
