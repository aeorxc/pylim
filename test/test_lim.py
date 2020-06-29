import pandas as pd, os
from pylim import lim
import unittest


class TestLim(unittest.TestCase):

    def test_lim_query(self):
        q = 'Show \r\nFB: FB FP: FP when date is after 2019'
        res = lim.query(q)
        self.assertIsNotNone(res)
        self.assertIn('FB', res.columns)
        self.assertIn('FP', res.columns)

    def test_extneded_query(self):
        q = '''
        LET
        FP = FP(ROLLOVER_DATE = "5 days before expiration day",ROLLOVER_POLICY = "actual prices")
        FP_M2 = FP(ROLLOVER_DATE = "5 days before expiration day",ROLLOVER_POLICY = "2 nearby actual prices")
        
        SHOW
        FP: FP
        FP_02: FP_M2
        '''
        res = lim.query(q)
        self.assertIsNotNone(res)
        self.assertIn('FP', res.columns)
        self.assertIn('FP_02', res.columns)

    def test_series(self):
        res = lim.series('FP_2020J')
        self.assertEqual(res['FP_2020J']['2020-01-02'], 608.5)

        res = lim.series({'FP_2020J' : 'GO', 'FB_2020J' : 'Brent'})
        self.assertEqual(res['GO']['2020-01-02'], 608.5)
        self.assertEqual(res['Brent']['2020-01-02'], 65.56)

    def test_series2(self):
        res = lim.series('PA0002779.6.2')
        self.assertEqual(res['PA0002779.6.2']['2020-01-02'], 479.75)

    def test_series3(self):
        res = lim.series('PUMFE03')
        self.assertEqual(res['PUMFE03']['2020-01-01'], 463.716)

    def test_series4(self):
        res = lim.series('PJABA00')
        self.assertEqual(res['PJABA00']['1990-01-02'], 246.5)

    def test_curve(self):
        res = lim.curve({'FP': 'GO', 'FB': 'Brent'})
        self.assertIn('GO', res.columns)
        self.assertIn('Brent', res.columns)

        res = lim.curve('FB', curve_dates=pd.to_datetime('2020-03-17'))
        self.assertIn('FB', res.columns)

    def test_curve2(self):
        res = lim.curve({'FP': 'GO', 'FB': 'Brent'}, curve_formula='Show 1: FP/7.45-FB')
        self.assertIn('GO', res.columns)
        self.assertIn('Brent', res.columns)
        self.assertIn('1', res.columns)

    def test_curve_history(self):
        res = lim.curve('FP', curve_dates=[pd.to_datetime('2020-03-17'), pd.to_datetime('2020-03-18')])
        self.assertIn('2020/03/17', res.columns)
        self.assertIn('2020/03/18', res.columns)

    def test_curve_formula(self):
        res = lim.curve_formula(curve_formula='Show 1: FP/7.45-FB', valid_symbols=['FP', 'FB'])
        self.assertIn('FP', res.columns)
        self.assertIn('FB', res.columns)
        self.assertIn('1', res.columns)

    def test_curve_formula2(self):
        cd = [pd.to_datetime('2020-02-02'), pd.to_datetime('2020-04-04')]
        res = lim.curve_formula(curve_formula='Show 1: FP/7.45-FB', curve_dates=cd, valid_symbols=['FP', 'FB'])
        self.assertIn('2020/02/02', res.columns)
        self.assertIn('2020/04/04', res.columns)

    def test_symbol_contracts(self):
        res = lim.get_symbol_contract_list('FB', monthly_contracts_only=True)
        self.assertIn('FB_1998J', res)
        self.assertIn('FB_2020Z', res)

    def test_futures_contracts(self):
        res = lim.futures_contracts('FB')
        self.assertIn('FB_2020Z', res.columns)

    def test_cont_futures_rollover(self):
        res = lim.continuous_futures_rollover('FB', months=['M1', 'M12'], after_date=2019)
        print(res.head())
        self.assertEqual(res['M1'][pd.to_datetime('2020-01-02')], 66.25)
        self.assertEqual(res['M12'][pd.to_datetime('2020-01-02')], 60.94)

    def test_pra_symbol(self):
        self.assertFalse(lim.check_pra_symbol('FB'))
        self.assertTrue(lim.check_pra_symbol('AAGXJ00'))
        self.assertTrue(lim.check_pra_symbol('PGACR00'))
        self.assertTrue(lim.check_pra_symbol('PA0005643.6.0'))

    def test_metadata(self):
        symbols = 'PCAAS00'
        m = lim.metadata(symbols)
        self.assertIn('PCAAS00', m.columns)

        symbols = ('FB', 'PCAAS00', 'PUMFE03')
        m = lim.metadata(symbols)

        self.assertIn('FB', m.columns)
        self.assertIn('PCAAS00', m.columns)
        self.assertIn('PUMFE03', m.columns)

        symbols = 'PJABA00'
        m = lim.metadata(symbols)
        self.assertIn('PJABA00', m.columns)
        self.assertEqual(m.loc['column_starts'][0][0], pd.to_datetime('1979-09-03'))
        self.assertEqual(m.loc['column_starts'][0][1], pd.to_datetime('2011-01-31'))
        self.assertEqual(m.loc['column_starts'][0][2], pd.to_datetime('1979-09-03'))


if __name__ == '__main__':
    unittest.main()