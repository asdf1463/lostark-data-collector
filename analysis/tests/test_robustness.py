"""Independent checks of calendar covariance and event holdout handling."""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.sandwich_covariance import cov_hac, S_hac_simple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import lostark_market_analysis as base
import lostark_market_robustness as robust


class CovarianceTests(unittest.TestCase):
    def setUp(self):
        rng = np.random.default_rng(19)
        self.dates = pd.date_range('2026-01-01', periods=90)
        self.x = pd.DataFrame({'const': 1., 'cost': rng.normal(size=90)}, index=self.dates)
        self.y = pd.Series(.3 + .7*self.x.cost + rng.normal(size=90), index=self.dates)

    def test_matches_statsmodels_on_complete_daily_calendar(self):
        r = sm.OLS(self.y, self.x).fit()
        for correction in [True, False]:
            for bandwidth in [0, 3, 7, 14]:
                np.testing.assert_allclose(robust.calendar_hac(r, self.dates, bandwidth, correction),
                                           cov_hac(r, nlags=bandwidth, use_correction=correction), atol=1e-12)

    def test_missing_days_match_independent_zero_score_embedding(self):
        keep = ~np.isin(np.arange(90), [20, 21, 22, 48])
        r = sm.OLS(self.y[keep], self.x[keep]).fit()
        scores = np.zeros((90, 2))
        scores[keep] = r.model.exog*np.asarray(r.resid)[:, None]
        bread = np.asarray(r.normalized_cov_params)
        expected = bread @ S_hac_simple(scores, nlags=7) @ bread
        expected *= r.nobs/(r.nobs-2)
        actual = robust.calendar_hac(r, self.dates[keep])
        np.testing.assert_allclose(actual, expected, atol=1e-12)
        self.assertFalse(np.allclose(actual, cov_hac(r, nlags=7)))
        self.assertGreater(np.linalg.eigvalsh(actual).min(), -1e-12)

    def test_same_day_dependence_preserved_in_stacked_equations(self):
        n = len(self.x)
        x = np.zeros((2*n, 4))
        x[:n, :2], x[n:, 2:] = self.x, self.x
        r = sm.OLS(np.r_[self.y, 2*self.y], x).fit()
        covariance = robust.calendar_hac(r, self.dates.append(self.dates), correction=False)
        first = sm.OLS(self.y, self.x).fit()
        expected = robust.calendar_hac(first, self.dates, correction=False)
        np.testing.assert_allclose(covariance[:2, 2:], 2*expected, atol=1e-12)

    def test_contrast_matches_statsmodels_linear_test(self):
        r = sm.OLS(self.y, self.x).fit()
        v = robust.calendar_hac(r, self.dates)
        actual = robust.contrast(r, v, {'const': 1, 'cost': -1})
        expected = r.t_test([1, -1], cov_p=v, use_t=False)
        self.assertAlmostEqual(actual['p_value'], float(np.asarray(expected.pvalue).item()))

    def test_invalid_dates_and_singular_design_fail(self):
        r = sm.OLS(self.y, self.x).fit()
        with self.assertRaises(ValueError):
            robust.calendar_hac(r, self.dates + pd.Timedelta(hours=1))
        r = sm.OLS(self.y, pd.concat([self.x, self.x], axis=1)).fit()
        with self.assertRaises(ValueError):
            robust.calendar_hac(r, self.dates)


class ModelTests(unittest.TestCase):
    def setUp(self):
        rng = np.random.default_rng(28)
        dates = pd.date_range('2026-03-29', periods=180)
        self.price = pd.Series(np.exp(4 + rng.normal(0, .02, len(dates)).cumsum()), index=dates)
        self.cost = pd.Series(np.exp(3 + rng.normal(0, .01, len(dates)).cumsum()), index=dates)

    def test_three_lag_model_matches_handoff_coefficients_and_dates(self):
        sample, r = robust.dynamic_model(self.price, self.cost)
        original_sample, original, _ = base.fit_abydos_dynamic(self.price, self.cost)
        self.assertTrue(sample.index.equals(original_sample.index))
        np.testing.assert_allclose(r.params, original.params, atol=1e-12)

    def test_event_response_is_not_used_to_fit_its_prediction(self):
        sample, x = robust.dark_design(self.price, self.cost)
        event = pd.Timestamp('2026-05-20')
        first = robust.blocked_error(sample, x, event)
        altered = sample.copy()
        altered.loc[event-pd.Timedelta(days=7):event+pd.Timedelta(days=7), 'dlogp'] += 1
        second = robust.blocked_error(altered, x, event)
        self.assertAlmostEqual(second['error_log'] - first['error_log'], 1.)
        self.assertEqual(second['training_n'], first['training_n'])

    def test_paired_fit_preserves_separate_equation_estimates(self):
        y0, y1 = np.log(self.price/self.cost), np.log(self.price/self.cost)*.5 + .01
        dates, paired = robust.paired_its(y0, y1)
        for prefix, y in [('normal', y0), ('upper', y1)]:
            _, r = base.fit_its_level(y, '2026-06-24')
            np.testing.assert_allclose(paired.params[[prefix+':'+k for k in r.params.index]], r.params, atol=1e-10)

    def test_ar_order_comparisons_use_identical_dates(self):
        y = np.log(self.price/self.cost).drop(pd.Timestamp('2026-06-13'))
        indices = [robust.its_with_lags(y, '2026-06-24', lags=k, warmup=7)[0].index
                   for k in [1, 2, 3, 7]]
        self.assertTrue(all(index.equals(indices[0]) for index in indices))
        self.assertNotIn(pd.Timestamp('2026-06-20'), indices[0])


if __name__ == '__main__':
    unittest.main()
