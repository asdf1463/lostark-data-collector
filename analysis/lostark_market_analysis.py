#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lost Ark market analysis
========================

재현 대상
---------
1) 데이터/시간축 검증
   - DB timestamp는 KST로 해석
   - life_materials / crafted_items의 YDayAvgPrice는 수집일 - 1일을 price_date로 사용
   - 같은 price_date-item의 중복 실행은 일별 1개로 통합
   - 0원(거래 없음)은 분석에서 제외
   - 누락일은 보간하지 않음

2) 아비도스: 생산비 중심 동적 시계열 회귀
   Δlog(P_t)
     = β0 Δlog(Cost_t)
     + Σ φ_k Δlog(P_{t-k})
     + Σ β_k Δlog(Cost_{t-k})
     + weekday FE
     + ε_t
   - 6/24, 8/5 전후 ±7일 제외
   - t-1~t-3가 실제 달력상 연속된 날짜에만 사용
   - HAC(Newey-West, lag=7)

3) 6/24 구조적 충격: Interrupted Time Series
   y_t = log(P_t / Cost_t)
   y_t = α + ρ y_{t-1} + β Time + δ Post + θ TimeAfter + weekday + ε_t
   - 이벤트 전후 ±35일
   - HAC(Newey-West, lag=7)

4) 배틀아이템
   A. 암흑 수류탄 통합 동적 모형
      Δlog(P_t)
        = β Δlog(Cost_t)
        + φ Δlog(P_{t-1})
        + weekday
        + event-day dummies
        + ε_t

   B. 5/20 성스러운 계열 상대 프리미엄 ITS
      y_t = mean[log(P/Cost)]_Holy - mean[log(P/Cost)]_Control
      y_t = α + ρ y_{t-1} + β Time + δ Post + θ TimeAfter + weekday + ε_t
      - 전후 ±35일

5) 시각화
   - 시장 배경: 생활재료 지수 vs 10레벨 겁화
   - 아비도스 동적 회귀 핵심 계수
   - 6/24 원가조정 가격지수
   - 암흑 수류탄 통합모형 요일 계수
   - 5/20 성스러운 계열 상대 프리미엄

실행
----
python lostark_market_analysis.py --db lostark_ts_data.db --out outputs

필수 패키지
-----------
pandas, numpy, matplotlib, statsmodels
"""

from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
import platform
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import statsmodels
import statsmodels.api as sm


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

KST = "Asia/Seoul"
HAC_LAGS = 7

ABYDOS_EVENTS_TO_EXCLUDE = [
    pd.Timestamp("2026-06-24"),
    pd.Timestamp("2026-08-05"),
]

DARK_GRENADE_EVENT_DATES = [
    pd.Timestamp("2026-04-22"),
    pd.Timestamp("2026-05-20"),
    pd.Timestamp("2026-08-05"),
    pd.Timestamp("2026-09-23"),
]

ROUTES = {
    "채집": ("들꽃", "수줍은 들꽃", "아비도스 들꽃"),
    "벌목": ("목재", "부드러운 목재", "아비도스 목재"),
    "채광": ("철광석", "묵직한 철광석", "아비도스 철광석"),
    "수렵": ("두툼한 생고기", "다듬은 생고기", "아비도스 두툼한 생고기"),
    "낚시": ("생선", "붉은 살 생선", "아비도스 태양 잉어"),
    "고고학": ("고대 유물", "희귀한 유물", "아비도스 유물"),
}

CORE_ITEMS = [
    "아비도스 융화 재료",
    "상급 아비도스 융화 재료",
    "암흑 수류탄",
    "성스러운 폭탄",
    "성스러운 부적",
    "정령의 회복약",
]


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------

def configure_matplotlib() -> None:
    """Korean-capable font fallbacks without requiring a specific OS."""
    plt.rcParams["font.sans-serif"] = [
        "NanumSquare",
        "NanumGothic",
        "Malgun Gothic",
        "AppleGothic",
        "DejaVu Sans",
    ]
    plt.rcParams["axes.unicode_minus"] = False


def pct_from_logcoef(x: float) -> float:
    """Convert log coefficient to exact percent change."""
    return 100.0 * (np.exp(x) - 1.0)


def stars(p: float) -> str:
    if pd.isna(p):
        return ""
    if p < 0.001:
        return "***"
    if p < 0.01:
        return "**"
    if p < 0.05:
        return "*"
    if p < 0.10:
        return "†"
    return ""


def joint_wald_pvalue(result, terms: list[str]) -> float:
    """Joint H0: all named coefficients == 0."""
    names = list(result.params.index)
    R = np.zeros((len(terms), len(names)))
    for i, term in enumerate(terms):
        R[i, names.index(term)] = 1.0
    test = result.wald_test(R, scalar=True)
    return float(test.pvalue)


def regression_table(result, terms: list[str] | None = None) -> pd.DataFrame:
    if terms is None:
        terms = list(result.params.index)
    ci = result.conf_int()
    rows = []
    for t in terms:
        rows.append(
            {
                "term": t,
                "coef": float(result.params[t]),
                "std_err": float(result.bse[t]),
                "stat": float(result.tvalues[t]),
                "p_value": float(result.pvalues[t]),
                "ci_low": float(ci.loc[t, 0]),
                "ci_high": float(ci.loc[t, 1]),
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------
# Load / preprocess
# ---------------------------------------------------------------------

def load_data(db_path: Path):
    # Never create a missing DB or write to the collector's database.
    db_path = db_path.resolve(strict=True)
    with closing(sqlite3.connect(db_path.as_uri() + "?mode=ro", uri=True)) as conn:
        conn.execute("BEGIN")
        life = pd.read_sql("SELECT * FROM life_materials", conn)
        craft = pd.read_sql("SELECT * FROM crafted_items", conn)
        gem = pd.read_sql("SELECT * FROM gem_prices", conn)

    for df in (life, craft, gem):
        # DB string is a naive local timestamp; project collection clock is KST.
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["timestamp_kst"] = df["timestamp"].dt.tz_localize(KST)

    # YDayAvgPrice = previous KST calendar day's average.
    life["price_date"] = (
        life["timestamp_kst"].dt.normalize() - pd.Timedelta(days=1)
    ).dt.tz_localize(None)

    craft["price_date"] = (
        craft["timestamp_kst"].dt.normalize() - pd.Timedelta(days=1)
    ).dt.tz_localize(None)

    # Gem price is a point-in-time observation: do NOT shift one day.
    gem["price_date"] = (
        gem["timestamp_kst"].dt.normalize()
    ).dt.tz_localize(None)

    return life, craft, gem


def audit_duplicates(df: pd.DataFrame, value_col: str) -> dict:
    g = df.groupby(["price_date", "item_name"])[value_col].agg(["size", "nunique"])
    dup = g[g["size"] > 1]
    return {
        "duplicate_groups": int(len(dup)),
        "duplicate_groups_with_different_values": int((dup["nunique"] > 1).sum()),
    }


def make_daily_panels(life, craft, gem):
    life, craft, gem = life.copy(), craft.copy(), gem.copy()
    for frame in (life, craft):
        frame["yday_avg_price"] = frame["yday_avg_price"].mask(
            frame["yday_avg_price"] <= 0
        )
        if audit_duplicates(frame, "yday_avg_price")["duplicate_groups_with_different_values"]:
            raise ValueError("Conflicting positive YDayAvgPrice duplicates; inspect the DB before analysis.")
    # Same price_date-item duplicates are duplicates of YDayAvgPrice.
    # Mean is safe here because audit confirms duplicate values agree.
    life_d = (
        life.groupby(["price_date", "item_name"], as_index=False)["yday_avg_price"]
        .mean()
    )
    craft_d = (
        craft.groupby(["price_date", "item_name"], as_index=False)["yday_avg_price"]
        .mean()
    )

    # Gem is point-in-time: same-day multiple collections can legitimately differ.
    # Use the last KST observation of each day for daily benchmark visualization.
    gem_d = (
        gem.sort_values(["timestamp_kst", "id"])
        .drop_duplicates(["price_date", "gem_name"], keep="last")
    )
    gem_d["top5_avg_price"] = gem_d["top5_avg_price"].mask(gem_d["top5_avg_price"] <= 0)

    life_p = (
        life_d.pivot(index="price_date", columns="item_name", values="yday_avg_price")
        .sort_index()
        .mask(lambda x: x <= 0)
    )
    craft_p = (
        craft_d.pivot(index="price_date", columns="item_name", values="yday_avg_price")
        .sort_index()
        .mask(lambda x: x <= 0)
    )

    return life_d, craft_d, gem_d, life_p, craft_p


# ---------------------------------------------------------------------
# Production costs
# ---------------------------------------------------------------------

def build_cost_series(life_p: pd.DataFrame):
    abydos_costs = {}
    upper_abydos_costs = {}

    # Market life-material prices are quoted per 100 units.
    # Recipes below produce 10 fusion materials.
    for route, (basic, advanced, abydos) in ROUTES.items():
        abydos_costs[route] = (
            life_p[basic] * 86 / 100
            + life_p[advanced] * 45 / 100
            + life_p[abydos] * 33 / 100
            + 400
        ) / 10

        upper_abydos_costs[route] = (
            life_p[basic] * 112 / 100
            + life_p[advanced] * 59 / 100
            + life_p[abydos] * 43 / 100
            + 520
        ) / 10

    abydos_cost_table = pd.DataFrame(abydos_costs)
    upper_cost_table = pd.DataFrame(upper_abydos_costs)

    costs = {
        "abydos_route_costs": abydos_cost_table,
        "upper_abydos_route_costs": upper_cost_table,
        "abydos_min": abydos_cost_table.min(axis=1),
        "upper_abydos_min": upper_cost_table.min(axis=1),

        # Battle-item recipes: output 3 items per craft.
        "holy_bomb": (
            life_p["철광석"] * 30 / 100
            + life_p["묵직한 철광석"] * 15 / 100
            + life_p["단단한 철광석"] * 4 / 100
            + 15
        ) / 3,

        "holy_charm": (
            life_p["철광석"] * 28 / 100
            + life_p["묵직한 철광석"] * 14 / 100
            + life_p["단단한 철광석"] * 4 / 100
            + 15
        ) / 3,

        "dark_grenade": (
            life_p["목재"] * 17 / 100
            + life_p["부드러운 목재"] * 12 / 100
            + life_p["튼튼한 목재"] * 4 / 100
            + 15
        ) / 3,

        "spirit_potion": (
            life_p["들꽃"] * 33 / 100
            + life_p["수줍은 들꽃"] * 25 / 100
            + life_p["화사한 들꽃"] * 8 / 100
            + 30
        ) / 3,
    }
    return costs


# ---------------------------------------------------------------------
# Model 1: Abydos dynamic regression
# ---------------------------------------------------------------------

def fit_abydos_dynamic(
    price: pd.Series,
    cost: pd.Series,
    event_dates=ABYDOS_EVENTS_TO_EXCLUDE,
    event_window: int = 7,
):
    """
    Final report specification.

    Important:
    1) Reindex to a complete daily calendar.
    2) Compute Δlog and lags on the calendar.
    3) Require current and t-1..t-3 variables to be nonmissing.
    4) Exclude rows where current day OR one of the prior 3 days falls inside
       a ±7-day event window. This prevents lag contamination.
    """
    d = pd.concat([price.rename("price"), cost.rename("cost")], axis=1).sort_index()
    idx = pd.date_range(d.index.min(), d.index.max(), freq="D")
    f = d.reindex(idx)

    f["dlogp"] = np.log(f["price"]).diff()
    f["dlogc"] = np.log(f["cost"]).diff()

    for k in range(1, 4):
        f[f"p_lag{k}"] = f["dlogp"].shift(k)
        f[f"c_lag{k}"] = f["dlogc"].shift(k)

    needed = (
        ["dlogp", "dlogc"]
        + [f"p_lag{k}" for k in range(1, 4)]
        + [f"c_lag{k}" for k in range(1, 4)]
    )
    valid = f[needed].notna().all(axis=1)

    excluded_dates = set()
    for event_date in event_dates:
        excluded_dates.update(
            pd.date_range(
                event_date - pd.Timedelta(days=event_window),
                event_date + pd.Timedelta(days=event_window),
                freq="D",
            )
        )

    # Current observation and its three lag dates must all be outside the event windows.
    no_event_contamination = pd.Series(
        [
            all((dt - pd.Timedelta(days=k)) not in excluded_dates for k in range(4))
            for dt in f.index
        ],
        index=f.index,
    )

    m = f[valid & no_event_contamination].copy()

    wd = pd.get_dummies(
        m.index.dayofweek, prefix="wd", drop_first=True, dtype=float
    )
    wd.index = m.index

    x_cols = (
        ["dlogc"]
        + [f"p_lag{k}" for k in range(1, 4)]
        + [f"c_lag{k}" for k in range(1, 4)]
    )
    X = sm.add_constant(pd.concat([m[x_cols], wd], axis=1))

    result = sm.OLS(m["dlogp"], X).fit(
        cov_type="HAC", cov_kwds={"maxlags": HAC_LAGS}
    )

    diagnostics = {
        "N": int(len(m)),
        "R2": float(result.rsquared),
        "Adj_R2": float(result.rsquared_adj),
        "lagged_cost_joint_p": joint_wald_pvalue(
            result, ["c_lag1", "c_lag2", "c_lag3"]
        ),
        "weekday_joint_p": joint_wald_pvalue(
            result, ["wd_1", "wd_2", "wd_3", "wd_4", "wd_5", "wd_6"]
        ),
    }
    return m, result, diagnostics


# ---------------------------------------------------------------------
# Model 2: 6/24 structural shock ITS
# ---------------------------------------------------------------------

def fit_its_level(
    y: pd.Series,
    event_date: str | pd.Timestamp,
    window_days: int = 35,
):
    """
    AR(1) + linear trend + level shift + post-event slope shift + weekday FE.
    """
    event_date = pd.Timestamp(event_date)
    d = y.rename("y").to_frame().dropna()
    d = d.loc[
        event_date - pd.Timedelta(days=window_days):
        event_date + pd.Timedelta(days=window_days)
    ].copy()

    idx = pd.date_range(d.index.min(), d.index.max(), freq="D")
    f = d.reindex(idx)

    f["lag1"] = f["y"].shift(1)
    f["time"] = np.arange(len(f), dtype=float)
    f["post"] = (f.index >= event_date).astype(float)
    f["time_after"] = np.where(
        f.index >= event_date,
        (f.index - event_date).days,
        0,
    ).astype(float)

    wd = pd.get_dummies(
        f.index.dayofweek, prefix="wd", drop_first=True, dtype=float
    )
    wd.index = f.index

    m = pd.concat([f, wd], axis=1).dropna()

    X = sm.add_constant(
        m[["lag1", "time", "post", "time_after"] + list(wd.columns)]
    )
    result = sm.OLS(m["y"], X).fit(
        cov_type="HAC", cov_kwds={"maxlags": HAC_LAGS}
    )
    return m, result


# ---------------------------------------------------------------------
# Model 3: Dark grenade dynamic model
# ---------------------------------------------------------------------

def fit_dark_grenade_model(price: pd.Series, cost: pd.Series):
    d = pd.concat([price.rename("price"), cost.rename("cost")], axis=1).sort_index()
    idx = pd.date_range(d.index.min(), d.index.max(), freq="D")
    f = d.reindex(idx)

    f["dlogp"] = np.log(f["price"]).diff()
    f["dlogc"] = np.log(f["cost"]).diff()
    f["lag1"] = f["dlogp"].shift(1)

    for event_date in DARK_GRENADE_EVENT_DATES:
        key = "event_" + event_date.strftime("%m%d")
        f[key] = (f.index == event_date).astype(float)

    wd = pd.get_dummies(
        f.index.dayofweek, prefix="wd", drop_first=True, dtype=float
    )
    wd.index = f.index

    m = pd.concat([f, wd], axis=1).dropna(
        subset=["dlogp", "dlogc", "lag1"]
    )

    event_cols = ["event_" + d.strftime("%m%d") for d in DARK_GRENADE_EVENT_DATES]
    X = sm.add_constant(
        m[["dlogc", "lag1"] + list(wd.columns) + event_cols]
    )
    result = sm.OLS(m["dlogp"], X).fit(
        cov_type="HAC", cov_kwds={"maxlags": HAC_LAGS}
    )
    return m, result


# ---------------------------------------------------------------------
# Model 4: 5/20 Holy premium ITS
# ---------------------------------------------------------------------

def build_holy_premium(craft_p: pd.DataFrame, costs: dict) -> pd.DataFrame:
    p = pd.DataFrame(
        {
            "holy_bomb": np.log(
                craft_p["성스러운 폭탄"] / costs["holy_bomb"]
            ),
            "holy_charm": np.log(
                craft_p["성스러운 부적"] / costs["holy_charm"]
            ),
            "dark": np.log(
                craft_p["암흑 수류탄"] / costs["dark_grenade"]
            ),
            "potion": np.log(
                craft_p["정령의 회복약"] / costs["spirit_potion"]
            ),
        }
    )
    # Keep treatment/control composition fixed when an individual item is missing.
    p["holy"] = p[["holy_bomb", "holy_charm"]].mean(axis=1, skipna=False)
    p["control"] = p[["dark", "potion"]].mean(axis=1, skipna=False)
    p["holy_premium"] = p["holy"] - p["control"]
    return p


# ---------------------------------------------------------------------
# Data audit
# ---------------------------------------------------------------------

def build_audit(life, craft, craft_p):
    life_audit = audit_duplicates(life, "yday_avg_price")
    craft_audit = audit_duplicates(craft, "yday_avg_price")

    start = min(life["price_date"].min(), craft["price_date"].min())
    end = max(life["price_date"].max(), craft["price_date"].max())
    full_days = pd.date_range(start, end, freq="D")

    core_counts = {}
    for item in CORE_ITEMS:
        s = craft_p[item].reindex(full_days)
        core_counts[item] = int(s.notna().sum())

    core_present = (
        craft_p[CORE_ITEMS]
        .reindex(full_days)
        .notna()
    )
    missing_core_days = [
        d.strftime("%Y-%m-%d")
        for d in core_present.index[~core_present.any(axis=1)]
    ]

    return {
        "life_duplicates": life_audit,
        "craft_duplicates": craft_audit,
        "core_item_observed_days": core_counts,
        "analysis_calendar_days": len(full_days),
        "analysis_start": start.strftime("%Y-%m-%d"),
        "analysis_end": end.strftime("%Y-%m-%d"),
        "common_missing_core_days": missing_core_days,
        "days_missing_any_core_item": [
            d.strftime("%Y-%m-%d")
            for d in core_present.index[~core_present.all(axis=1)]
        ],
        "nonpositive_rows_excluded": {
            "life_materials": int((life["yday_avg_price"] <= 0).sum()),
            "crafted_items": int((craft["yday_avg_price"] <= 0).sum()),
        },
    }


# ---------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------

def plot_market_background(life_p, gem_d, out_dir: Path):
    excluded_life = {"낚시의 결정", "수렵의 결정", "고고학의 결정", "오레하 유물"}
    core_life = [c for c in life_p.columns if c not in excluded_life]

    base = life_p.loc["2026-03-29":"2026-04-04", core_life].apply(
        lambda s: np.exp(np.log(s.dropna()).mean())
    )
    life_index = life_p[core_life].div(base, axis=1).mul(100).median(axis=1)

    gem_s = (
        gem_d[gem_d["gem_name"] == "10레벨 겁화"]
        .set_index("price_date")["top5_avg_price"]
        .sort_index()
    )
    gem_base = np.exp(
        np.log(gem_s.loc["2026-03-30":"2026-04-05"]).mean()
    )
    gem_index = gem_s / gem_base * 100

    daily = pd.concat(
        [life_index.rename("핵심 생활재료"), gem_index.rename("10레벨 겁화")],
        axis=1, sort=True,
    )
    daily = daily.reindex(pd.date_range(daily.index.min(), daily.index.max(), freq="D"))
    plot_df = daily.rolling(7, min_periods=3, center=True).median().where(daily.notna())

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(plot_df.index, plot_df["핵심 생활재료"], label="핵심 생활재료 지수")
    ax.plot(plot_df.index, plot_df["10레벨 겁화"], label="10레벨 겁화")
    ax.set_title("시장 배경: 핵심 생활재료와 10레벨 겁화의 장기 경로")
    ax.set_ylabel("기준기간 = 100 (7일 중앙값)")
    ax.legend()
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(out_dir / "01_market_background.png", dpi=200, bbox_inches="tight")
    plt.close(fig)

    plot_df.to_csv(out_dir / "01_market_background_data.csv", encoding="utf-8-sig")


def plot_abydos_dynamic(models: dict, out_dir: Path):
    labels = ["당일 원가 변화", "가격 t-1", "가격 t-2", "가격 t-3"]
    keys = ["dlogc", "p_lag1", "p_lag2", "p_lag3"]

    x = np.arange(len(labels))
    width = 0.36
    fig, ax = plt.subplots(figsize=(11, 5.7))

    for j, label in enumerate(["일반", "상급"]):
        result = models[label]["result"]
        vals = [result.params[k] for k in keys]
        bars = ax.bar(
            x + (j - 0.5) * width,
            vals,
            width,
            label=f"{label} 아비도스 융화 재료",
        )
        for bar, key, val in zip(bars, keys, vals):
            p = result.pvalues[key]
            y = val + (0.025 if val >= 0 else -0.045)
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                y,
                f"{val:.2f}{stars(p)}",
                ha="center",
                va="center",
                fontsize=10,
            )

    ax.axhline(0, linewidth=0.8)
    ax.set_xticks(x, labels)
    ax.set_ylabel("동적 회귀계수")
    ax.set_title("아비도스 동적 시계열 회귀: 생산비와 전일 가격변화")
    ax.margins(y=0.18)
    ax.legend()
    ax.grid(axis="y", alpha=0.2)
    fig.tight_layout()
    fig.savefig(
        out_dir / "02_abydos_dynamic_coefficients.png",
        dpi=200,
        bbox_inches="tight",
    )
    plt.close(fig)


def plot_0624_break(craft_p, costs, out_dir: Path):
    normal_ratio = craft_p["아비도스 융화 재료"] / costs["abydos_min"]
    upper_ratio = craft_p["상급 아비도스 융화 재료"] / costs["upper_abydos_min"]

    normal_base = np.exp(
        np.log(normal_ratio.loc["2026-06-17":"2026-06-23"]).mean()
    )
    upper_base = np.exp(
        np.log(upper_ratio.loc["2026-06-17":"2026-06-23"]).mean()
    )

    plot_df = pd.DataFrame(
        {
            "일반 아비도스": normal_ratio / normal_base * 100,
            "상급 아비도스": upper_ratio / upper_base * 100,
        }
    ).loc["2026-06-17":"2026-06-30"]

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(plot_df.index, plot_df["일반 아비도스"], marker="o", label="일반")
    ax.plot(plot_df.index, plot_df["상급 아비도스"], marker="o", label="상급")
    ax.axvline(pd.Timestamp("2026-06-24"), linewidth=1)
    ax.axhline(100, linewidth=0.8, alpha=0.6)
    ax.set_title("6/24 시스템 변경: 시장가격 / 최저 제작원가")
    ax.set_ylabel("6/17~6/23 평균 = 100")
    ax.legend()
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(out_dir / "03_0624_structural_break.png", dpi=200, bbox_inches="tight")
    plt.close(fig)

    plot_df.to_csv(out_dir / "03_0624_structural_break_data.csv", encoding="utf-8-sig")


def plot_dark_weekday(result, out_dir: Path):
    names = ["월", "화", "수", "목", "금", "토", "일"]
    effects = [0.0]
    lows = [0.0]
    highs = [0.0]

    ci = result.conf_int()
    for i in range(1, 7):
        key = f"wd_{i}"
        effects.append(100 * result.params[key])
        lows.append(100 * ci.loc[key, 0])
        highs.append(100 * ci.loc[key, 1])

    effects = np.array(effects)
    yerr = np.vstack(
        [effects - np.array(lows), np.array(highs) - effects]
    )

    fig, ax = plt.subplots(figsize=(10.5, 5.6))
    ax.bar(names, effects)
    ax.errorbar(names, effects, yerr=yerr, fmt="none", capsize=4)
    ax.axhline(0, linewidth=0.8)
    ax.set_title("암흑 수류탄 통합 동적 모형의 요일효과")
    ax.set_ylabel("월요일 대비 일간 로그수익률 차이 (%p)")
    ax.grid(axis="y", alpha=0.2)
    fig.tight_layout()
    fig.savefig(out_dir / "04_dark_grenade_weekday.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_holy_premium(premium, out_dir: Path):
    baseline = premium.loc[
        "2026-05-13":"2026-05-19", "holy_premium"
    ].mean()

    event_index = np.exp(premium["holy_premium"] - baseline) * 100
    event_index = event_index.loc["2026-05-13":"2026-06-03"]

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(event_index.index, event_index.values, marker="o")
    ax.axvline(pd.Timestamp("2026-05-20"), linewidth=1)
    ax.axhline(100, linewidth=0.8, alpha=0.6)
    ax.set_title("5/20 콘텐츠 업데이트 전후 성스러운 계열 상대 프리미엄")
    ax.set_ylabel("5/13~5/19 평균 = 100")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(out_dir / "05_holy_premium_0520.png", dpi=200, bbox_inches="tight")
    plt.close(fig)

    event_index.rename("holy_premium_index").to_csv(
        out_dir / "05_holy_premium_0520_data.csv",
        encoding="utf-8-sig",
    )


# ---------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------

def export_model_outputs(
    out_dir: Path,
    abydos_models: dict,
    its_models: dict,
    dark_result,
    holy_result,
):
    # Abydos
    for label in ["일반", "상급"]:
        r = abydos_models[label]["result"]
        regression_table(r).to_csv(
            out_dir / f"model_abydos_dynamic_{label}.csv",
            index=False,
            encoding="utf-8-sig",
        )

    # 6/24 ITS
    for label in ["일반", "상급"]:
        regression_table(its_models[label]).to_csv(
            out_dir / f"model_0624_its_{label}.csv",
            index=False,
            encoding="utf-8-sig",
        )

    # Battle items
    regression_table(dark_result).to_csv(
        out_dir / "model_dark_grenade_dynamic.csv",
        index=False,
        encoding="utf-8-sig",
    )
    regression_table(holy_result).to_csv(
        out_dir / "model_holy_premium_0520_its.csv",
        index=False,
        encoding="utf-8-sig",
    )


def print_key_results(abydos_models, its_models, dark_result, holy_result):
    print("\n=== Abydos dynamic regression ===")
    for label in ["일반", "상급"]:
        r = abydos_models[label]["result"]
        diag = abydos_models[label]["diagnostics"]
        print(
            f"{label}: Cost_t={r.params['dlogc']:.3f} "
            f"(p={r.pvalues['dlogc']:.4g}), "
            f"Price_t-1={r.params['p_lag1']:.3f} "
            f"(p={r.pvalues['p_lag1']:.4g}), "
            f"R2={diag['R2']:.3f}, N={diag['N']}"
        )
        print(
            f"    lagged cost joint p={diag['lagged_cost_joint_p']:.4g}, "
            f"weekday joint p={diag['weekday_joint_p']:.4g}"
        )

    print("\n=== 6/24 ITS ===")
    for label in ["일반", "상급"]:
        r = its_models[label]
        post = r.params["post"]
        print(
            f"{label}: post={post:.4f} "
            f"({pct_from_logcoef(post):+.1f}%), "
            f"p={r.pvalues['post']:.4g}"
        )

    print("\n=== Dark grenade dynamic model ===")
    for key in ["dlogc", "lag1", "wd_3", "wd_5",
                "event_0422", "event_0520", "event_0805", "event_0923"]:
        print(
            f"{key}: coef={dark_result.params[key]:.4f}, "
            f"p={dark_result.pvalues[key]:.4g}"
        )

    print("\n=== Holy premium 5/20 ITS ===")
    for key in ["lag1", "time", "post", "time_after"]:
        val = holy_result.params[key]
        print(
            f"{key}: coef={val:.4f}, p={holy_result.pvalues[key]:.4g}"
        )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "lostark_ts_data.db",
        help="SQLite DB path",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("analysis_outputs"),
        help="Output directory",
    )
    parser.add_argument(
        "--end-date", type=pd.Timestamp,
        help="Inclusive KST price date (YYYY-MM-DD), e.g. 2026-09-24 for handoff reproduction",
    )
    args = parser.parse_args()
    if args.end_date is not None:
        if args.end_date.tzinfo is not None or args.end_date != args.end_date.normalize():
            parser.error("--end-date must be a calendar date without time or timezone")

    args.out.mkdir(parents=True, exist_ok=True)
    configure_matplotlib()

    life, craft, gem = load_data(args.db)
    if args.end_date is not None:
        life = life.loc[life["price_date"] <= args.end_date].copy()
        craft = craft.loc[craft["price_date"] <= args.end_date].copy()
        gem = gem.loc[gem["price_date"] <= args.end_date].copy()
    if life.empty or craft.empty or gem.empty:
        parser.error("No observations available for the requested analysis period")

    # Audits
    audit = {
        "life_duplicates": audit_duplicates(life, "yday_avg_price"),
        "craft_duplicates": audit_duplicates(craft, "yday_avg_price"),
    }

    life_d, craft_d, gem_d, life_p, craft_p = make_daily_panels(
        life, craft, gem
    )
    audit.update(build_audit(life, craft, craft_p))
    audit["db_sha256"] = hashlib.sha256(args.db.read_bytes()).hexdigest()
    audit["requested_end_date"] = (
        args.end_date.strftime("%Y-%m-%d") if args.end_date is not None else None
    )
    audit["versions"] = {
        "python": platform.python_version(), "pandas": pd.__version__,
        "numpy": np.__version__, "matplotlib": matplotlib.__version__,
        "statsmodels": statsmodels.__version__,
    }

    with open(args.out / "data_audit.json", "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)

    costs = build_cost_series(life_p)

    # Abydos dynamic models
    abydos_models = {}
    for label, price, cost in [
        ("일반", craft_p["아비도스 융화 재료"], costs["abydos_min"]),
        ("상급", craft_p["상급 아비도스 융화 재료"], costs["upper_abydos_min"]),
    ]:
        m, r, diag = fit_abydos_dynamic(price, cost)
        abydos_models[label] = {
            "data": m,
            "result": r,
            "diagnostics": diag,
        }

    # 6/24 ITS
    normal_y = np.log(
        craft_p["아비도스 융화 재료"] / costs["abydos_min"]
    )
    upper_y = np.log(
        craft_p["상급 아비도스 융화 재료"] / costs["upper_abydos_min"]
    )
    _, its_normal = fit_its_level(normal_y, "2026-06-24", window_days=35)
    _, its_upper = fit_its_level(upper_y, "2026-06-24", window_days=35)
    its_models = {"일반": its_normal, "상급": its_upper}

    # Dark grenade
    _, dark_result = fit_dark_grenade_model(
        craft_p["암흑 수류탄"], costs["dark_grenade"]
    )

    # Holy premium 5/20
    holy_premium = build_holy_premium(craft_p, costs)
    _, holy_result = fit_its_level(
        holy_premium["holy_premium"], "2026-05-20", window_days=35
    )

    # Charts
    plot_market_background(life_p, gem_d, args.out)
    plot_abydos_dynamic(abydos_models, args.out)
    plot_0624_break(craft_p, costs, args.out)
    plot_dark_weekday(dark_result, args.out)
    plot_holy_premium(holy_premium, args.out)

    # Tables
    export_model_outputs(
        args.out,
        abydos_models,
        its_models,
        dark_result,
        holy_result,
    )
    with open(args.out / "model_diagnostics.json", "w", encoding="utf-8") as f:
        json.dump({
            "abydos_dynamic": {
                label: model["diagnostics"] for label, model in abydos_models.items()
            },
            "other_models": {
                name: {"N": int(r.nobs), "R2": float(r.rsquared)}
                for name, r in {
                    "its_normal": its_normal, "its_upper": its_upper,
                    "dark_grenade": dark_result, "holy_premium": holy_result,
                }.items()
            },
        }, f, ensure_ascii=False, indent=2)

    print_key_results(
        abydos_models,
        its_models,
        dark_result,
        holy_result,
    )
    print(f"\nOutputs saved to: {args.out.resolve()}")


if __name__ == "__main__":
    main()
