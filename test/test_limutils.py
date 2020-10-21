from pylim import limutils
import unittest


class TestLim(unittest.TestCase):

    def test_pra_symbol(self):
        self.assertFalse(limutils.check_pra_symbol('FB'))
        self.assertTrue(limutils.check_pra_symbol('AAGXJ00'))
        self.assertTrue(limutils.check_pra_symbol('PGACR00'))
        self.assertTrue(limutils.check_pra_symbol('PA0005643.6.0'))


if __name__ == '__main__':
    unittest.main()