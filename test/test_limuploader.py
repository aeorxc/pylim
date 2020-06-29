import pandas as pd
from pylim import limuploader
from pylim import lim
import unittest
from random import random
from datetime import datetime


class TestLimUploader(unittest.TestCase):

    def test_upload_series(self):
        r1, r2 = random(), random()
        dn = datetime.now().date()
        columns = ['TopRelation:Test:SPOTPRICE;TopColumn:Price:Close', 'TopRelation:Test:SPOTPRICE2']
        data = {columns[0]: r1, columns[1]: r2}
        df = pd.DataFrame(data, index=[dn])

        dfmeta = {
            'description': 'desc'
        }

        limuploader.upload_series(df, dfmeta)
        df = lim.series(['SPOTPRICE', 'SPOTPRICE2'])
        self.assertAlmostEquals(df.loc[dn]['SPOTPRICE'], r1, 2)
        self.assertAlmostEquals(df.loc[dn]['SPOTPRICE2'], r2, 2)


if __name__ == '__main__':
    unittest.main()