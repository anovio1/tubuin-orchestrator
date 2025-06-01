# sql/queries.py
from . import __getattr__
from abc import ABC, abstractmethod

move_raw_replays_cache_replays = __getattr__("move_raw_replays_cache_replays")
update_derived_match_player_unlogged = __getattr__("update_derived_match_player_unlogged")
update_derived_replays = __getattr__("update_derived_replays")

# Stats Generation
update_analytics_match_skill_snapshots = __getattr__("update_analytics_match_skill_snapshots")
refresh_mat_derived_match_skill_deltas = __getattr__("refresh_mat_derived_match_skill_deltas")