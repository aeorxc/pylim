import pandas as pd
from datetime import datetime
from pylim import lim
import unittest

curyear = datetime.today().year


class TestLim(unittest.TestCase):

    def test_lim_query(self):
        q = 'Show \r\nFB: FB FP: FP when date is after 2019'
        res = lim.query(q)
        self.assertIsNotNone(res)
        self.assertIn('FB', res.columns)
        self.assertIn('FP', res.columns)

    def test_extended_query(self):
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
        res = lim.series('FP_2020J', start_date=datetime(2020, 1, 1))
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

        res = lim.curve('FB', curve_dates=(pd.to_datetime('2020-03-17'),))
        self.assertEqual(res['2020/03/17']['2020-05-01'], 28.73)
        self.assertEqual(res['2020/03/17']['2020-08-01'], 33.25)

    def test_curve2(self):
        res = lim.curve({'FP': 'GO', 'FB': 'Brent'}, curve_formula='Show 1: FP/7.45-FB')
        self.assertIn('GO', res.columns)
        self.assertIn('Brent', res.columns)
        self.assertIn('1', res.columns)

    def test_curve_history(self):
        res = lim.curve('FP', curve_dates=(pd.to_datetime('2020-03-17'), pd.to_datetime('2020-03-18')))
        self.assertIn('2020/03/17', res.columns)
        self.assertIn('2020/03/18', res.columns)

    def test_curve_formula(self):
        res = lim.curve_formula(formula='Show 1: FP/7.45-FB')
        self.assertIn('FP', res.columns)
        self.assertIn('FB', res.columns)
        self.assertIn('1', res.columns)

    def test_curve_formula2(self):
        cd = (pd.to_datetime('2020-02-02'), pd.to_datetime('2020-04-04'))
        res = lim.curve_formula(formula='Show 1: FP/7.45-FB', curve_dates=cd)
        self.assertIn('2020/02/02', res.columns)
        self.assertIn('2020/04/04', res.columns)
        self.assertEqual(res['2020/02/02']['2020-05-01'], 10.929)
        self.assertEqual(res['2020/04/04']['2020-08-01'], 8.50930)

    def test_symbol_contracts(self):
        res = lim.get_symbol_contract_list('FB', monthly_contracts_only=True)
        self.assertIn('FB_1998J', res)
        self.assertIn('FB_2020Z', res)

        res = lim.get_symbol_contract_list(('CL','FB'), monthly_contracts_only=True)
        self.assertIn('CL_1998J', res)
        self.assertIn('FB_2020Z', res)

    def test_futures_contracts(self):
        res = lim.contracts('FB', start_year=2020, start_date='2020-01-01')
        self.assertIn('2020Z', res.columns)
        self.assertIn(f'{curyear}Z', res.columns)

        res = lim.contracts('FB', months=['Z'], start_date='date is within 5 days')
        self.assertIn(f'{curyear}Z', res.columns)

    def test_futures_contracts_formula(self):
        res = lim.contracts(formula='Show 1: FP/7.45-FB', months=['F'], start_year=2020, start_date='2020-01-01')
        self.assertIn(f'{curyear}F', res.columns)
        self.assertAlmostEqual(res['2021F']['2020-01-02'], 16.95, 2)

    def test_cont_futures_rollover(self):
        res = lim.continuous_futures_rollover('FB', months=['M1', 'M12'], after_date=2019)
        self.assertEqual(res['M1'][pd.to_datetime('2020-01-02')], 66.25)
        self.assertEqual(res['M12'][pd.to_datetime('2020-01-02')], 60.94)

    def test_structre1(self):
        res = lim.structure('FB', 1, 2, start_date='2020-01-01')
        self.assertAlmostEqual(res['M1-M2'][pd.to_datetime('2020-01-02')], 0.689, 2)

        res = lim.structure('FB', 1, 12, start_date='2020-01-01')
        self.assertAlmostEqual(res['M1-M12'][pd.to_datetime('2020-01-02')], 5.31, 2)

    def test_metadata(self):
        symbols = ('FB', 'PCAAS00', 'PUMFE03', 'PJABA00')
        m = lim.relations(symbols, show_columns=True, date_range=True)
        self.assertTrue(isinstance(m['FB']['daterange'], pd.DataFrame))

        self.assertIn('FB', m.columns)
        self.assertIn('PCAAS00', m.columns)
        self.assertIn('PUMFE03', m.columns)
        self.assertIn('PJABA00', m.columns)

        self.assertEqual(m['PJABA00']['daterange']['start']['Low'], pd.to_datetime('1979-09-03'))
        self.assertEqual(m['PJABA00']['daterange']['start']['Close'], pd.to_datetime('2011-01-31'))
        self.assertEqual(m['PJABA00']['daterange']['start']['High'], pd.to_datetime('1979-09-03'))

    def test_relations1(self):
        symbol = 'TopRelation:Futures:Cboe'
        res = lim.relations(symbol, desc=True)
        self.assertEqual(res['Cboe']['description'], "Chicago Board Options Exchange")

    def test_relations2(self):
        symbol = 'FB,CL'
        res = lim.relations(symbol, show_children=True)
        self.assertIn('FB', res.loc['children']['FB']['name'].iloc[0])
        self.assertIn('CL', res.loc['children']['CL']['name'].iloc[0])

    def test_find_symbols_in_path1(self):
        path = 'TopRelation:Futures:Ipe'
        res = lim.find_symbols_in_path(path)
        self.assertIn('WI_Q21', res)
        self.assertIn('FB', res)

    def test_find_symbols_in_query(self):
        q = 'Show 1: FP/7.45-FB'
        r = lim.find_symbols_in_query(q)
        self.assertIn('FP', r)
        self.assertIn('FB', r)


if __name__ == '__main__':
    unittest.main()