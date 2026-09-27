"""Inference checks for the handoff analysis; does not modify the collector or DB.

Calendar HAC uses Bartlett weights on actual day distances and n/(n-k).
Its zero score at an omitted date is not an imputed price or observation.
Normal/chi-square inference remains asymptotic, not exact small-sample inference.
Single-date events are assessed with blocked leave-out errors, not dummy p-values.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import scipy
from scipy.stats import chi2, norm
import statsmodels.api as sm
from statsmodels.stats.multitest import multipletests

import lostark_market_analysis as base


def calendar_hac(result, dates, bandwidth=7, correction=True):
    """Sandwich covariance, including same-day covariance for stacked equations."""
    if not isinstance(bandwidth, int) or bandwidth < 0:
        raise ValueError("bandwidth must be a nonnegative integer")
    dates = pd.DatetimeIndex(dates)
    x = np.asarray(result.model.exog, dtype=float)
    residual = np.asarray(result.resid, dtype=float)
    n, k = x.shape
    if len(dates) != n or dates.hasnans or not (dates == dates.normalize()).all():
        raise ValueError("one normalized calendar date is required per observation")
    if np.linalg.matrix_rank(x) != k or n <= k:
        raise ValueError("full-rank design with residual degrees of freedom required")
    scores = pd.DataFrame(x * residual[:, None], index=dates).groupby(level=0).sum()
    # Missing days contribute no score; no price/residual is interpolated.
    scores = scores.reindex(pd.date_range(dates.min(), dates.max()), fill_value=0).to_numpy()
    meat = scores.T @ scores
    for lag in range(1, min(bandwidth, len(scores) - 1) + 1):
        cross = scores[lag:].T @ scores[:-lag]
        meat += (1 - lag / (bandwidth + 1)) * (cross + cross.T)
    bread = np.asarray(result.normalized_cov_params)
    covariance = bread @ meat @ bread
    if correction:
        covariance *= n / (n - k)
    return (covariance + covariance.T) / 2


def contrast(result, covariance, weights):
    names = list(result.model.exog_names)
    vector = np.array([weights.get(name, 0.) for name in names])
    estimate = float(vector @ np.asarray(result.params))
    variance = float(vector @ covariance @ vector)
    if variance <= 0:
        raise ValueError("contrast variance must be positive")
    se = float(np.sqrt(variance))
    return dict(coef=estimate, std_err=se, p_value=float(2 * norm.sf(abs(estimate / se))),
                ci_low=estimate - norm.ppf(.975) * se, ci_high=estimate + norm.ppf(.975) * se)


def joint_pvalue(result, covariance, names):
    indices = [result.model.exog_names.index(name) for name in names]
    coefficients = np.asarray(result.params)[indices]
    subcov = covariance[np.ix_(indices, indices)]
    if np.linalg.matrix_rank(subcov) != len(indices):
        raise ValueError("joint covariance is singular")
    statistic = coefficients @ np.linalg.solve(subcov, coefficients)
    return float(chi2.sf(statistic, len(indices)))


def table(result, dates, bandwidth=7):
    covariance = calendar_hac(result, dates, bandwidth)
    rows = [{"term": term, **contrast(result, covariance, {term: 1.})}
            for term in result.model.exog_names]
    return pd.DataFrame(rows), covariance


def dynamic_model(price, cost, lags=3, event_window=7):
    frame = pd.concat([price.rename("price"), cost.rename("cost")], axis=1).sort_index()
    frame = frame.reindex(pd.date_range(frame.index.min(), frame.index.max()))
    frame["dlogp"] = np.log(frame.price).diff()
    frame["dlogc"] = np.log(frame.cost).diff()
    columns = ["dlogc"]
    for prefix, series in [("p", "dlogp"), ("c", "dlogc")]:
        for lag in range(1, lags + 1):
            frame[f"{prefix}_lag{lag}"] = frame[series].shift(lag)
            columns.append(f"{prefix}_lag{lag}")
    valid = frame[["dlogp"] + columns].notna().all(axis=1)
    excluded = set()
    for event in base.ABYDOS_EVENTS_TO_EXCLUDE:
        excluded.update(pd.date_range(event - pd.Timedelta(days=event_window),
                                      event + pd.Timedelta(days=event_window)))
    valid &= np.array([all(date - pd.Timedelta(days=k) not in excluded for k in range(lags + 1))
                       for date in frame.index])
    sample = frame.loc[valid].copy()
    weekdays = pd.get_dummies(pd.Series(sample.index.dayofweek, index=sample.index),
                             prefix="wd", drop_first=True, dtype=float)
    x = sm.add_constant(pd.concat([sample[columns], weekdays], axis=1))
    return sample, sm.OLS(sample.dlogp, x).fit()


def its_with_lags(y, event_date, window=35, lags=1, warmup=None):
    """Alternative AR orders on the same daily grid; warmup fixes the sample."""
    event = pd.Timestamp(event_date)
    dates = pd.date_range(event - pd.Timedelta(days=window), event + pd.Timedelta(days=window))
    frame = y.reindex(dates).rename("y").to_frame()
    frame["time"] = np.arange(len(frame), dtype=float)
    frame["post"] = (dates >= event).astype(float)
    frame["time_after"] = np.maximum((dates - event).days, 0).astype(float)
    for lag in range(1, lags + 1):
        frame[f"lag{lag}"] = frame.y.shift(lag)
    weekdays = pd.get_dummies(pd.Series(dates.dayofweek, index=dates), prefix="wd", drop_first=True, dtype=float)
    frame = pd.concat([frame, weekdays], axis=1)
    # Even lower-order fits require the same prior observed dates for comparison.
    common_valid = pd.Series(True, index=dates)
    for lag in range(1, max(lags, warmup or 0) + 1):
        common_valid &= frame.y.shift(lag).notna()
    sample = frame.loc[common_valid].dropna()
    result = sm.OLS(sample.y, sm.add_constant(sample.drop(columns="y"))).fit()
    return sample, result


def paired_its(normal_y, upper_y, window=35, lags=1, warmup=None):
    """Fully interacted equations: allow group-specific AR/trend/weekday terms."""
    m0, r0 = its_with_lags(normal_y, "2026-06-24", window, lags, warmup)
    m1, r1 = its_with_lags(upper_y, "2026-06-24", window, lags, warmup)
    common = m0.index.intersection(m1.index)
    blocks, targets, dates = [], [], []
    for label, sample, result in [("normal", m0, r0), ("upper", m1, r1)]:
        x = pd.DataFrame(result.model.exog, index=sample.index,
                         columns=result.model.exog_names).loc[common]
        blocks.append(x.add_prefix(label + ":").reset_index(drop=True))
        targets.extend(sample.loc[common, "y"])
        dates.extend(common)
    x = pd.concat(blocks, ignore_index=True).fillna(0.)
    return pd.DatetimeIndex(dates), sm.OLS(np.asarray(targets), x).fit()


def dark_design(price, cost):
    sample, result = base.fit_dark_grenade_model(price, cost)
    columns = [name for name in result.model.exog_names if not name.startswith("event_")]
    x = pd.DataFrame(result.model.exog, index=sample.index,
                     columns=result.model.exog_names)[columns]
    return sample, x


def blocked_error(sample, x, date, buffer_days=7, event_buffer=3, min_train=40):
    """Two-sided leave-out diagnostic using observed cost/lag, not a live forecast.

    The held-out date and nearby observations cannot enter its coefficient fit.
    Known event neighborhoods are excluded for every candidate by the same rule.
    """
    training = np.abs((sample.index - date).days) > buffer_days
    for event in base.DARK_GRENADE_EVENT_DATES:
        training &= np.abs((sample.index - event).days) > event_buffer
    if training.sum() < min_train:
        raise ValueError("too few training observations")
    model = sm.OLS(sample.loc[training, "dlogp"], x.loc[training]).fit()
    if np.linalg.matrix_rank(model.model.exog) != x.shape[1]:
        raise ValueError("rank-deficient event reference fit")
    residual = np.asarray(model.resid)
    scale = 1.4826 * np.median(np.abs(residual - np.median(residual)))
    if scale <= 0:
        raise ValueError("zero reference residual scale")
    row = x.loc[date].to_numpy()
    prediction = float(row @ np.asarray(model.params))
    error = float(sample.loc[date, "dlogp"] - prediction)
    denominator = scale * np.sqrt(1 + row @ model.normalized_cov_params @ row)
    return {"date": str(date.date()), "error_log": error,
            "relative_deviation_percent": float(100 * np.expm1(error)),
            "score": float(error / denominator), "training_n": int(training.sum())}


def event_placebos(sample, x, buffer_days=7):
    placebo_dates = [date for date in sample.index
                     if all(abs((date - event).days) > 7 for event in base.DARK_GRENADE_EVENT_DATES)]
    placebos = pd.DataFrame([blocked_error(sample, x, date, buffer_days) for date in placebo_dates])
    rows = []
    for event in base.DARK_GRENADE_EVENT_DATES:
        if event not in sample.index:
            rows.append({"date": str(event.date()), "status": "missing_observation"})
            continue
        row = blocked_error(sample, x, event, buffer_days)
        for label, pool in [("all_days", placebos), ("same_weekday", placebos.loc[
                pd.to_datetime(placebos.date).dt.dayofweek == event.dayofweek])]:
            # Descriptive rank only: no randomized dates or exchangeability claim.
            row[label + "_placebo_n"] = len(pool)
            row[label + "_tail_fraction"] = float(
                (1 + (pool.score.abs() >= abs(row["score"])).sum()) / (1 + len(pool)))
        rows.append(row)
    return pd.DataFrame(rows), placebos


def residual_calendar_correlations(result, dates):
    residual = pd.Series(np.asarray(result.resid), index=pd.DatetimeIndex(dates))
    residual = residual.reindex(pd.date_range(residual.index.min(), residual.index.max()))
    rows = []
    for lag in range(1, 8):
        pair = pd.concat([residual, residual.shift(lag)], axis=1).dropna()
        rows.append({"calendar_lag": lag, "pairs": len(pair),
                     "correlation": float(pair.iloc[:, 0].corr(pair.iloc[:, 1]))})
    return rows


def run(db, out, end_date):
    out.mkdir(parents=True, exist_ok=True)
    life, craft, gem = base.load_data(db)
    if end_date is not None:
        life, craft, gem = [frame.loc[frame.price_date <= end_date].copy()
                            for frame in (life, craft, gem)]
    _, _, _, life_p, craft_p = base.make_daily_panels(life, craft, gem)
    costs = base.build_cost_series(life_p)
    audit = base.build_audit(life, craft, craft_p)
    audit.update(db_sha256=hashlib.sha256(db.read_bytes()).hexdigest(),
                 covariance="Bartlett actual calendar day distance; n/(n-k); normal/chi-square inference",
                 scipy_version=scipy.__version__, pandas_version=pd.__version__,
                 numpy_version=np.__version__, statsmodels_version=base.statsmodels.__version__)
    all_rows, sensitivity, joint, residuals = [], [], [], {}

    def collect(name, result, dates):
        coefficients, covariance = table(result, dates)
        coefficients.insert(0, "model", name)
        coefficients["n"] = int(result.nobs)
        coefficients["r2"] = float(result.rsquared)
        all_rows.extend(coefficients.to_dict("records"))
        residuals[name] = residual_calendar_correlations(result, dates)
        return covariance

    for label, item, cost_key in [("normal", "아비도스 융화 재료", "abydos_min"),
                                  ("upper", "상급 아비도스 융화 재료", "upper_abydos_min")]:
        sample, result = dynamic_model(craft_p[item], costs[cost_key])
        covariance = collect("abydos_" + label, result, sample.index)
        joint.append({"model": "abydos_" + label, "test": "weekday_joint",
                      "p_value": joint_pvalue(result, covariance, [f"wd_{i}" for i in range(1, 7)])})
        for lags in [1, 2, 3]:
            for event_window in [5, 7, 10]:
                m, r = dynamic_model(craft_p[item], costs[cost_key], lags, event_window)
                for bandwidth in [3, 7, 14]:
                    v = calendar_hac(r, m.index, bandwidth)
                    for term in ["dlogc", "p_lag1"]:
                        sensitivity.append({"model": "abydos_" + label, "term": term,
                                            "lags": lags, "event_window": event_window,
                                            "bandwidth": bandwidth, "n": int(r.nobs),
                                            **contrast(r, v, {term: 1})})

    ys = {"normal": np.log(craft_p["아비도스 융화 재료"] / costs["abydos_min"]),
          "upper": np.log(craft_p["상급 아비도스 융화 재료"] / costs["upper_abydos_min"])}
    premium = base.build_holy_premium(craft_p, costs)
    holy_variants = {"both_controls": premium.holy_premium,
                     "dark_only": premium.holy - premium.dark,
                     "potion_only": premium.holy - premium.potion}
    main_tests = []
    for name, y, date in [("its_normal", ys["normal"], "2026-06-24"),
                           ("its_upper", ys["upper"], "2026-06-24"),
                           ("holy_both_controls", holy_variants["both_controls"], "2026-05-20")]:
        sample, result = base.fit_its_level(y, date)
        covariance = collect(name, result, sample.index)
        main_tests.append({"model": name, "test": "post_slope_conditional",
                           **contrast(result, covariance, {"time": 1, "time_after": 1})})

    for window in [21, 28, 35]:
        for label, y in {**{"its_"+k: v for k,v in ys.items()},
                          **{"holy_"+k: v for k,v in holy_variants.items()}}.items():
            date = "2026-06-24" if label.startswith("its_") else "2026-05-20"
            sample, result = base.fit_its_level(y, date, window)
            for bandwidth in [3, 7, 14]:
                covariance = calendar_hac(result, sample.index, bandwidth)
                for term, weights in [("post", {"post": 1}), ("time_after", {"time_after": 1}),
                                       ("post_slope", {"time": 1, "time_after": 1})]:
                    sensitivity.append({"model": label, "term": term, "window": window,
                                        "bandwidth": bandwidth, "n": int(result.nobs),
                                        **contrast(result, covariance, weights)})
        dates, result = paired_its(ys["normal"], ys["upper"], window)
        for bandwidth in [3, 7, 14]:
            covariance = calendar_hac(result, dates, bandwidth)
            test = contrast(result, covariance, {"normal:post": 1, "upper:post": -1})
            sensitivity.append({"model": "paired_its", "term": "normal_minus_upper_post",
                                "window": window, "bandwidth": bandwidth, "n": int(result.nobs), **test})
            if window == 35 and bandwidth == 7:
                main_tests.append({"model": "paired_its", "test": "normal_minus_upper_post", **test})

    ar_diagnostics = []
    # Residual autocorrelation motivates AR-order checks; compare a fixed sample.
    for lags in [1, 2, 3, 7]:
        for label, y in {**{"its_"+k: v for k,v in ys.items()},
                          **{"holy_"+k: v for k,v in holy_variants.items()}}.items():
            date = "2026-06-24" if label.startswith("its_") else "2026-05-20"
            m, r = its_with_lags(y, date, lags=lags, warmup=7)
            autocorrelations = residual_calendar_correlations(r, m.index)
            ar_diagnostics.append({"model": label, "lags": lags, "n": int(r.nobs),
                                   "residual_ac1": autocorrelations[0]["correlation"],
                                   "max_abs_residual_ac_1_to_7": max(abs(row["correlation"]) for row in autocorrelations)})
            for bandwidth in [3, 7, 14]:
                v = calendar_hac(r, m.index, bandwidth)
                for term, weights in [("post", {"post": 1}), ("time_after", {"time_after": 1}),
                                       ("post_slope", {"time": 1, "time_after": 1})]:
                    sensitivity.append({"model": label, "term": term, "window": 35,
                                        "lags": lags, "common_warmup": 7, "bandwidth": bandwidth,
                                        "n": int(r.nobs), **contrast(r, v, weights)})
        dates, r = paired_its(ys["normal"], ys["upper"], lags=lags, warmup=7)
        for bandwidth in [3, 7, 14]:
            sensitivity.append({"model": "paired_its", "term": "normal_minus_upper_post",
                                "window": 35, "lags": lags, "common_warmup": 7,
                                "bandwidth": bandwidth, "n": int(r.nobs),
                                **contrast(r, calendar_hac(r, dates, bandwidth), {"normal:post": 1, "upper:post": -1})})

    sample, x = dark_design(craft_p["암흑 수류탄"], costs["dark_grenade"])
    keep = ~sample.index.isin(base.DARK_GRENADE_EVENT_DATES)
    result = sm.OLS(sample.loc[keep, "dlogp"], x.loc[keep]).fit()
    covariance = collect("dark_non_event", result, sample.index[keep])
    weekdays = [f"wd_{i}" for i in range(1, 7)]
    joint.append({"model": "dark_non_event", "test": "weekday_joint",
                  "p_value": joint_pvalue(result, covariance, weekdays)})
    weekday_rows = [{"term": term, **contrast(result, covariance, {term: 1})} for term in weekdays]
    adjusted = multipletests([row["p_value"] for row in weekday_rows], method="holm")[1]
    for row, p in zip(weekday_rows, adjusted):
        row["holm_p_value"] = float(p)
    for bandwidth in [3, 7, 14]:
        covariance = calendar_hac(result, sample.index[keep], bandwidth)
        for term in ["dlogc", "lag1", "wd_3", "wd_5"]:
            sensitivity.append({"model": "dark_non_event", "term": term, "bandwidth": bandwidth,
                                "n": int(result.nobs), **contrast(result, covariance, {term: 1})})
    for buffer_days in [3, 7, 14]:
        events, placebos = event_placebos(sample, x, buffer_days)
        events.to_csv(out/f"event_diagnostics_buffer{buffer_days}.csv", index=False, encoding="utf-8-sig")
        placebos.to_csv(out/f"placebo_errors_buffer{buffer_days}.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(all_rows).to_csv(out/"calendar_hac_models.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(sensitivity).to_csv(out/"sensitivity.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(main_tests).to_csv(out/"contrasts.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(weekday_rows).to_csv(out/"dark_weekdays_holm.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(ar_diagnostics).to_csv(out/"its_ar_diagnostics.csv", index=False, encoding="utf-8-sig")
    (out/"audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    (out/"joint_tests.json").write_text(json.dumps(joint, indent=2), encoding="utf-8")
    (out/"residual_correlations.json").write_text(json.dumps(residuals, indent=2), encoding="utf-8")
    print(pd.DataFrame(main_tests).to_string(index=False))
    print(json.dumps(joint, indent=2))
    print(f"Saved {len(sensitivity)} sensitivity contrasts to {out.resolve()}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path(__file__).resolve().parents[1]/"lostark_ts_data.db")
    parser.add_argument("--out", type=Path, default=Path("analysis_outputs/robustness"))
    parser.add_argument("--end-date", type=pd.Timestamp)
    args = parser.parse_args()
    if args.end_date is not None and (args.end_date.tzinfo is not None or args.end_date != args.end_date.normalize()):
        parser.error("end-date must be a calendar date without a timezone")
    run(args.db, args.out, args.end_date)


if __name__ == "__main__":
    main()
