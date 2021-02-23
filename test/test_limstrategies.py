import pandas as pd
import pytest

from pylim import limstrategies


def test_quarterly1():
    res = limstrategies.quarterly('FP', start_date='2020-01-01', start_year=2020)
    assert res[2020]['2020-01-02'] == pytest.approx(614.5, abs=0.1)


def test_quarterly2():
    res = limstrategies.quarterly('Show 1: FP/7.45-FB', start_date='2020-01-01', start_year=2020)
    assert res[2020]['2020-01-02'] == pytest.approx(15.998, abs=0.001)


def test_quarterly_all():
    res = limstrategies.quarterly('Show 1: FP/7.45-FB', quarter=0, start_date=pd.to_datetime('2020-01-01'), start_year=2020)
    assert res['Q1_2020']['2020-01-02'] == pytest.approx(15.998, abs=0.001)
    assert res['Q2_2020']['2020-01-02'] == pytest.approx(16.064, abs=0.001)
    assert res['Q3_2020']['2020-01-02'] == pytest.approx(16.504, abs=0.001)
    assert res['Q4_2020']['2020-01-02'] == pytest.approx(16.961, abs=0.001)


def test_calendar1():
    res = limstrategies.calendar('FP', start_date='2020-01-01', start_year=2020)
    assert res[2020]['2020-01-02'] == pytest.approx(600.3, abs=0.1)


def test_calendar2():
    res = limstrategies.calendar('Show 1: FP/7.45-FB', start_year=2020)
    assert res[2020]['2020-01-02'] == pytest.approx(16.5, abs=0.1)


def test_spread_standard():
    res = limstrategies.spread('FB', x=1, y=2, start_year=2019, end_year=2020, start_date='2019-01-01')
    assert res[2020]['2019-01-02'] == pytest.approx(-0.09, abs=0.01)


def test_spread_year_increment():
    res = limstrategies.spread('FB', x=12, y=12, start_year=2019, end_year=2021)
    assert res[2020]['2020-01-02'] == pytest.approx(3.1, abs=0.1)


def test_spread_quartely():
    res = limstrategies.spread('FB', x='Q1', y='Q2', start_year=2019, end_year=2021)
    assert res[2020]['2019-01-02'] == pytest.approx(-0.29, abs=0.01)


def test_calendar_spread():
    res = limstrategies.spread('FB', x='CAL19', y='CAL20')
    assert res['CAL 2019-2020']['2019-01-02'] == pytest.approx(-1.094, abs=0.01)


def test_spread_formula_standard():
    res = limstrategies.spread('Show 1: FP/7.45-FB', x=1, y=2, start_year=2019, end_year=2020)
    assert res[2020]['2019-01-02'] == pytest.approx(-0.21, abs=0.01)
    assert res[2019]['2018-01-02'] == pytest.approx(-0.13, abs=0.01)


def test_spread_formula_quarterly():
    res = limstrategies.spread('Show 1: FP/7.45-FB', x='Q1', y='Q2', start_year=2019, end_year=2021)
    assert res[2020]['2019-01-02'] == pytest.approx(-0.38, abs=0.01)
    assert res[2019]['2018-01-02'] == pytest.approx(-0.14, abs=0.01)


def test_spread_formula_calendar():
    res = limstrategies.spread('Show 1: FP/7.45-FB', x='CAL19', y='CAL20')
    assert res['CAL 2019-2020']['2019-01-02'] == pytest.approx(-1.48, abs=0.01)


def test_multi_spread():
    res = limstrategies.multi_spread('Show 1: FP/7.45-FB', spreads=[[6,6], [12, 12]], start_year=2020, start_date='2020-01-01')
    assert res['JunJun_2020']['2020-01-02'] == pytest.approx(-0.45, abs=0.01)
    assert res['DecDec_2020']['2020-01-02'] == pytest.approx(0.46, abs=0.01)


def test_fly1():
    res = limstrategies.fly('FB', x=1, y=2, z=3, start_year=2019, end_year=2020, start_date='2019-01-01')
    assert res[2020]['2019-01-02'] == pytest.approx(0.01, abs=0.01)


def test_fly2():
    res = limstrategies.fly('Show 1: FP/7.45-FB', x=1, y=2, z=3, start_year=2019, end_year=2020, start_date='2019-01-01')
    assert res[2020]['2019-01-02'] == pytest.approx(0.023, abs=0.01)

