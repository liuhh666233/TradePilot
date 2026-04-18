"""Calendar helpers for ETF all-weather stage one."""

from __future__ import annotations

import pandas as pd


def build_rebalance_calendar(
    trading_calendar: pd.DataFrame,
    *,
    anchor_day: int = 20,
    rule_name: str = "first_open_on_or_after_anchor_day",
    calendar_source: str = "canonical_trading_calendar",
) -> pd.DataFrame:
    """Build the monthly ETF all-weather rebalance calendar.

    The frozen v1 rule is the first open trading day on or after the 20th
    calendar day of each month. The input frame is expected to contain at least
    `trade_date` and `is_open` columns for one exchange.
    """

    if trading_calendar.empty:
        return pd.DataFrame(
            columns=[
                "rebalance_date",
                "calendar_month",
                "rule_name",
                "anchor_day",
                "previous_rebalance_date",
                "calendar_source",
            ]
        )

    calendar = trading_calendar.copy()
    calendar["trade_date"] = pd.to_datetime(calendar["trade_date"], errors="coerce")
    calendar["is_open"] = calendar["is_open"].astype(bool)
    calendar = calendar.dropna(subset=["trade_date"]).sort_values("trade_date").reset_index(drop=True)
    calendar["calendar_month"] = calendar["trade_date"].dt.strftime("%Y-%m")
    eligible = calendar.loc[calendar["is_open"] & (calendar["trade_date"].dt.day >= anchor_day)].copy()
    if eligible.empty:
        return pd.DataFrame(
            columns=[
                "rebalance_date",
                "calendar_month",
                "rule_name",
                "anchor_day",
                "previous_rebalance_date",
                "calendar_source",
            ]
        )

    rebalance = (
        eligible.groupby("calendar_month", as_index=False)
        .agg(rebalance_date=("trade_date", "min"))
        .sort_values("rebalance_date")
        .reset_index(drop=True)
    )
    rebalance["rule_name"] = rule_name
    rebalance["anchor_day"] = anchor_day
    rebalance["previous_rebalance_date"] = rebalance["rebalance_date"].shift(1)
    rebalance["calendar_source"] = calendar_source
    return rebalance.loc[
        :,
        [
            "rebalance_date",
            "calendar_month",
            "rule_name",
            "anchor_day",
            "previous_rebalance_date",
            "calendar_source",
        ],
    ]
