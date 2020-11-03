from pylim import limstrategies
import unittest


class TestLimStrategies(unittest.TestCase):

    def test_quarterly(self):
        res = limstrategies.quarterly('FP')
        self.assertAlmostEqual(res['2020']['2020-01-02'], 614.5, 1)

        res = limstrategies.quarterly('Show 1: FP/7.45-FB')
        self.assertAlmostEqual(res['2020']['2020-01-02'], 15.998, 3)

    def test_calendar(self):
        res = limstrategies.calendar('FP')
        self.assertAlmostEqual(res['2020']['2020-01-02'], 600.3, 1)

        res = limstrategies.calendar('Show 1: FP/7.45-FB')
        self.assertAlmostEqual(res['2020']['2020-01-02'], 16.5, 1)

    def test_spread1(self):
        res = limstrategies.spread('FB', x=1, y=2, start_year=2019, end_year=2020)
        self.assertAlmostEqual(res[2020]['2019-01-02'], -0.09, 2)
        # res = limstrategies.spread('FB', x='F', y='G', start_year=2019, end_year=2020)
        # self.assertAlmostEqual(res['2020']['2019-01-02'], -0.09, 2)

    def test_spread2(self):
        # check year increment
        res = limstrategies.spread('FB', x=12, y=12, start_year=2019, end_year=2021)
        self.assertAlmostEqual(res[2020]['2020-01-02'], 3.1, 1)

    def test_spread3(self):
        # quarterly
        res = limstrategies.spread('FB', x='Q1', y='Q2', start_year=2019, end_year=2021)
        self.assertAlmostEqual(res[2020]['2019-01-02'], -0.29, 2)

    def test_spread4(self):
        # calendar
        res = limstrategies.spread('FB', x='CAL19', y='CAL20')
        self.assertAlmostEqual(res['CAL 2019-2020']['2019-01-02'], -1.094, 2)

    def test_spread_formula1(self):
        res = limstrategies.spread('Show 1: FP/7.45-FB', x=1, y=2, start_year=2019, end_year=2020)
        self.assertAlmostEqual(res[2020]['2019-01-02'], -0.21, 2)
        self.assertAlmostEqual(res[2019]['2018-01-02'], -0.13, 2)

    def test_spread_formula2(self):
        # quarterly
        res = limstrategies.spread('Show 1: FP/7.45-FB', x='Q1', y='Q2', start_year=2019, end_year=2021)
        self.assertAlmostEqual(res[2020]['2019-01-02'], -0.38, 2)
        self.assertAlmostEqual(res[2019]['2018-01-02'], -0.14, 2)

    def test_spread_formula3(self):
        # calendar
        res = limstrategies.spread('Show 1: FP/7.45-FB', x='CAL19', y='CAL20')
        self.assertAlmostEqual(res['CAL 2019-2020']['2019-01-02'], -1.48, 2)


if __name__ == '__main__':
    unittest.main()
