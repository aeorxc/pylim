from pylim import limqueryutils
import unittest


class TestLim(unittest.TestCase):

    def test_build_futures_contracts_formula_query(self):
        f = 'Show 1: FP/7.45-FB'
        m = ['FP', 'FB']
        c = ['2020F', '2020G']
        res = limqueryutils.build_futures_contracts_formula_query(f, m, c)
        self.assertIn('FP_2020F/7.45-FB_2020F', res)
        self.assertIn('FP_2020G/7.45-FB_2020G', res)


if __name__ == '__main__':
    unittest.main()