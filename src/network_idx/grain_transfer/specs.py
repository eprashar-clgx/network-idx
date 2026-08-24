"""
Declarative specifications for grain-transfer promotions.

A promotion moves a feature table from its native grain to another — aggregating upward
(parcel or block to tract) or, in the downward direction, broadcasting a coarse value to
finer rows. Each promotion is described here as data, not code: which column identifies
the source grain, how the target grain key is derived from it, and which aggregate to
apply to every carried column. ``promote`` turns one of these specs plus an adapter into
SQL, so the analytical decisions (what to sum, what to take the max of, how to reach the
tract id) live in one readable place and the SQL generation stays mechanical.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from network_idx.grain_transfer.adapters.base import (
    MAX,
    SUM,
    SUPPORTED_OPS,
)

# The two directions a promotion can move a feature. Aggregating upward groups finer rows
# into coarser ones with an aggregate per column; broadcasting downward joins a coarse
# table onto a finer grain map so every fine row inherits its parent's value.
AGGREGATE_UP = "aggregate_up"
BROADCAST_DOWN = "broadcast_down"


@dataclass(frozen=True)
class Aggregation:
    """
    One output column of a promotion and how it is computed from the source.

    ``out_col`` is the column name in the promoted table, ``op`` is a logical aggregate
    operation the adapter knows how to render, and ``expr`` is the source column (or, for
    a conditional count, a boolean condition) the aggregate is applied to. When ``expr``
    is omitted it defaults to ``out_col``, which is the common case of carrying a column
    through under the same name.
    """

    out_col: str
    op: str
    expr: str = ""

    def __post_init__(self) -> None:
        """Validate the operation and default ``expr`` to the output column name."""
        if self.op not in SUPPORTED_OPS:
            raise ValueError(
                f"Aggregation {self.out_col!r} uses unsupported op {self.op!r}; "
                f"expected one of {sorted(SUPPORTED_OPS)}."
            )
        if not self.expr:
            object.__setattr__(self, "expr", self.out_col)


@dataclass(frozen=True)
class PromotionSpec:
    """
    A complete, dialect-neutral description of one grain-transfer promotion.

    ``direction`` selects aggregate-up or broadcast-down. For an aggregate-up promotion,
    ``target_key`` is the grain column emitted by the promoted table and ``key_expr`` is
    the SQL expression that derives it from the source row (for example the first eleven
    characters of a block geoid to reach a tract). When the source grain cannot be
    reduced to the target with an expression alone — parcels reaching a tract, say — an
    optional crosswalk is joined: ``xwalk_source_key`` on the source matches
    ``xwalk_join_key`` on the crosswalk, whose ``xwalk_target_expr`` then supplies the
    target key. ``aggregations`` lists every carried column, and ``filter_sql`` is an
    optional predicate applied before grouping.
    """

    name: str
    direction: str
    target_key: str
    aggregations: tuple[Aggregation, ...]
    key_expr: str = ""
    xwalk_source_key: str = ""
    xwalk_join_key: str = ""
    xwalk_target_expr: str = ""
    filter_sql: str = ""

    def __post_init__(self) -> None:
        """Validate the direction and the key-derivation configuration."""
        if self.direction not in (AGGREGATE_UP, BROADCAST_DOWN):
            raise ValueError(
                f"PromotionSpec {self.name!r} has unknown direction {self.direction!r}."
            )
        if not self.aggregations:
            raise ValueError(f"PromotionSpec {self.name!r} carries no columns.")
        has_expr = bool(self.key_expr)
        has_xwalk = bool(self.xwalk_source_key and self.xwalk_join_key and self.xwalk_target_expr)
        if has_expr == has_xwalk:
            raise ValueError(
                f"PromotionSpec {self.name!r} must derive its target key either from "
                "key_expr or from a full crosswalk (xwalk_source_key, xwalk_join_key, "
                "xwalk_target_expr), but not both or neither."
            )

    @property
    def uses_crosswalk(self) -> bool:
        """Return whether this promotion reaches its target grain through a crosswalk join."""
        return not self.key_expr


def _sum(col: str) -> Aggregation:
    """Return a sum aggregation carrying ``col`` through under the same name."""
    return Aggregation(col, SUM)


def _max(col: str) -> Aggregation:
    """Return a max aggregation carrying ``col`` through under the same name."""
    return Aggregation(col, MAX)


# Per-technology FCC speed columns summed or maxed when rolling block rows up to a tract.
# Location counts add across the blocks in a tract; the advertised speeds take the best
# (maximum) value seen, matching the original block-to-tract aggregation. Provider counts
# are intentionally left out here: a block-level provider count cannot be re-aggregated to
# a distinct tract-level count without double counting providers that span blocks, so that
# column is produced from provider-grain data by its own promotion.
_FCC_SPEED_TECHS = ("cable", "copper", "fiber")
_FCC_SPEEDS_AGGREGATIONS = tuple(
    agg
    for tech in _FCC_SPEED_TECHS
    for agg in (
        _sum(f"{tech}_location_count"),
        _max(f"{tech}_max_download_speed"),
        _max(f"{tech}_max_upload_speed"),
    )
)

# Roll the block-grain FCC fixed-speeds table up to census tract. The tract geoid is the
# first eleven characters of the block geoid (state+county+tract), and every carried
# column is summed or maxed per the block-to-tract rules above.
FCC_SPEEDS_CT_SPEC = PromotionSpec(
    name="fcc_fixed_speeds_ct",
    direction=AGGREGATE_UP,
    target_key="tract_geoid",
    key_expr="SUBSTR(block_geoid, 1, 11)",
    aggregations=_FCC_SPEEDS_AGGREGATIONS,
)
