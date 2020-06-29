import pandas as pd, os
from pylim import lim
import unittest


class TestLimQueryCache(unittest.TestCase):

    def test_query_cache(self):
        dirname, filename = os.path.split(os.path.abspath(__file__))
        q = 'Show \r\nFB: FB FP: FP'

        rf = lim.query_hash(q)
        test_out_loc = os.path.join(dirname, rf)
        if os.path.exists(test_out_loc):
            os.remove(test_out_loc)

        res = lim.query(q, cache_inc=True)

        self.assertTrue(os.path.exists(test_out_loc))
        res = lim.query(q, cache_inc=True)
        self.assertTrue(os.path.exists(test_out_loc))

        df = pd.read_hdf(rf, mode='r')
        self.assertIn('FB', df.columns)
        self.assertEqual(df.iloc[-1].name, res.iloc[-1].name)

        if os.path.exists(test_out_loc):
            os.remove(test_out_loc)

    def test_query_cache2(self):
        dirname, filename = os.path.split(os.path.abspath(__file__))

        q = '''
        LET
        FP = FP(ROLLOVER_DATE = "5 days before expiration day",ROLLOVER_POLICY = "actual prices")
        FP_M2 = FP(ROLLOVER_DATE = "5 days before expiration day",ROLLOVER_POLICY = "2 nearby actual prices")

        SHOW
        FP: FP
        FP_02: FP_M2
        '''
        rf = lim.query_hash(q)
        test_out_loc = os.path.join(dirname, rf)
        if os.path.exists(test_out_loc):
            os.remove(test_out_loc)

        res = lim.query(q, cache_inc=True)

        self.assertTrue(os.path.exists(test_out_loc))
        res = lim.query(q, cache_inc=True)
        self.assertTrue(os.path.exists(test_out_loc))

        df = pd.read_hdf(rf, mode='r')
        self.assertIn('FP', df.columns)
        self.assertEqual(df.iloc[-1].name, res.iloc[-1].name)

        if os.path.exists(test_out_loc):
            os.remove(test_out_loc)

if __name__ == '__main__':
    unittest.main()