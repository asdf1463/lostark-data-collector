"""Regression checks for calendar and price handling; no API calls required."""
import sqlite3
from contextlib import closing
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import lostark_market_analysis as analysis


class PreprocessingTests(unittest.TestCase):
    def test_kst_previous_day_and_read_only_missing_path(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.db"
            with self.assertRaises(FileNotFoundError):
                analysis.load_data(path)
            self.assertFalse(path.exists())
            with closing(sqlite3.connect(path)) as conn:
                for table in ("life_materials", "crafted_items", "gem_prices"):
                    conn.execute(f"CREATE TABLE {table} (timestamp TEXT)")
                    conn.execute(f"INSERT INTO {table} VALUES ('2026-04-01 00:15:00')")
                conn.commit()
            before = path.read_bytes()
            life, craft, gem = analysis.load_data(path)
            self.assertEqual(life.loc[0, "price_date"], pd.Timestamp("2026-03-31"))
            self.assertEqual(craft.loc[0, "price_date"], pd.Timestamp("2026-03-31"))
            self.assertEqual(gem.loc[0, "price_date"], pd.Timestamp("2026-04-01"))
            self.assertEqual(str(life.timestamp_kst.dt.tz), "Asia/Seoul")
            self.assertEqual(path.read_bytes(), before)

    def panels(self, prices=(100, 100)):
        life = pd.DataFrame({
            "price_date": pd.to_datetime(["2026-04-01"] * 2),
            "item_name": ["item"] * 2, "yday_avg_price": prices,
        })
        gem = pd.DataFrame({
            "id": [2, 1], "gem_name": ["gem"] * 2,
            "price_date": pd.to_datetime(["2026-04-01"] * 2),
            "timestamp_kst": pd.to_datetime(["2026-04-01 18:00", "2026-04-01 12:00"]).tz_localize(analysis.KST),
            "top5_avg_price": [0, 200],
        })
        return analysis.make_daily_panels(life, life.copy(), gem)

    def test_identical_duplicates_collapse_and_last_gem_zero_stays_missing(self):
        life, _, gem, prices, _ = self.panels()
        self.assertEqual(len(life), 1)
        self.assertEqual(prices.iloc[0, 0], 100)
        self.assertEqual(gem.iloc[0]["id"], 2)
        self.assertTrue(pd.isna(gem.iloc[0]["top5_avg_price"]))

    def test_zero_is_excluded_before_daily_aggregation(self):
        self.assertEqual(self.panels((0, 100))[3].iloc[0, 0], 100)
        self.assertTrue(pd.isna(self.panels((0, 0))[3].iloc[0, 0]))

    def test_conflicting_positive_duplicates_fail(self):
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            self.panels((100, 110))

    def test_holy_premium_requires_all_four_items(self):
        prices = pd.DataFrame({
            "성스러운 폭탄": [100, 100], "성스러운 부적": [np.nan, 100],
            "암흑 수류탄": [100, 100], "정령의 회복약": [100, 100],
        })
        costs = {key: pd.Series([10, 10]) for key in
                 ["holy_bomb", "holy_charm", "dark_grenade", "spirit_potion"]}
        result = analysis.build_holy_premium(prices, costs)
        self.assertTrue(pd.isna(result.loc[0, "holy_premium"]))
        self.assertEqual(result.loc[1, "holy_premium"], 0)

    def test_audit_tracks_actual_range_and_partial_missingness(self):
        dates = pd.date_range("2026-09-24", periods=3)
        raw = pd.DataFrame({"price_date": dates, "item_name": "item", "yday_avg_price": 1})
        prices = pd.DataFrame(1., index=dates, columns=analysis.CORE_ITEMS)
        prices.iloc[1, 0] = np.nan
        prices.iloc[2, :] = np.nan
        result = analysis.build_audit(raw, raw, prices)
        self.assertEqual(result["analysis_calendar_days"], 3)
        self.assertEqual(result["analysis_end"], "2026-09-26")
        self.assertEqual(result["common_missing_core_days"], ["2026-09-26"])
        self.assertEqual(result["days_missing_any_core_item"], ["2026-09-25", "2026-09-26"])


class CalendarTests(unittest.TestCase):
    def setUp(self):
        dates = pd.date_range("2026-04-01", periods=200)
        rng = np.random.default_rng(42)
        self.price = pd.Series(np.exp(4 + rng.normal(0, .04, len(dates)).cumsum()), index=dates)
        self.cost = pd.Series(np.exp(3 + rng.normal(0, .02, len(dates)).cumsum()), index=dates)
        self.gap = pd.Timestamp("2026-05-01")
        self.price = self.price.drop(self.gap)
        self.cost = self.cost.drop(self.gap)

    def test_abydos_requires_five_consecutive_price_dates(self):
        sample, result, _ = analysis.fit_abydos_dynamic(self.price, self.cost, event_dates=[])
        for k in range(5):
            self.assertNotIn(self.gap + pd.Timedelta(days=k), sample.index)
        self.assertIn(self.gap + pd.Timedelta(days=5), sample.index)
        self.assertTrue(all(f"wd_{k}" in result.params for k in range(1, 7)))

    def test_abydos_event_exclusion_includes_lag_dates(self):
        event = pd.Timestamp("2026-06-24")
        sample, _, _ = analysis.fit_abydos_dynamic(self.price, self.cost, event_dates=[event])
        for date in pd.date_range(event - pd.Timedelta(days=7), event + pd.Timedelta(days=10)):
            self.assertNotIn(date, sample.index)
        self.assertIn(event + pd.Timedelta(days=11), sample.index)

    def test_dark_lag_does_not_bridge_gap(self):
        sample, _ = analysis.fit_dark_grenade_model(self.price, self.cost)
        for k in range(3):
            self.assertNotIn(self.gap + pd.Timedelta(days=k), sample.index)
        self.assertIn(self.gap + pd.Timedelta(days=3), sample.index)

    def test_its_lag_does_not_bridge_gap(self):
        sample, _ = analysis.fit_its_level(np.log(self.price / self.cost), "2026-05-20")
        self.assertNotIn(self.gap, sample.index)
        self.assertNotIn(self.gap + pd.Timedelta(days=1), sample.index)
        self.assertIn(self.gap + pd.Timedelta(days=2), sample.index)


if __name__ == "__main__":
    unittest.main()
