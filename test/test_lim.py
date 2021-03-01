from datetime import date
import requests
import pandas as pd
import pytest

from pylim import lim


@pytest.fixture
def current_year() -> int:
    yield date.today().year


def test_lim_query():
    q = 'Show \r\nFB: FB FP: FP when date is after 2019'
    res = lim.query(q)
    assert res is not None
    assert 'FB' in res.columns
    assert 'FP' in res.columns


def test_extended_query():
    q = '''
    LET
    FP = FP(ROLLOVER_DATE = "5 days before expiration day",ROLLOVER_POLICY = "actual prices")
    FP_M2 = FP(ROLLOVER_DATE = "5 days before expiration day",ROLLOVER_POLICY = "2 nearby actual prices")

    SHOW
    FP: FP
    FP_02: FP_M2
    '''
    res = lim.query(q)
    assert res is not None
    assert 'FP' in res.columns
    assert 'FP_02' in res.columns


def test_invalid_query():
    with pytest.raises(requests.HTTPError):
        _ = lim.query('Show 1: Something + Else')


def test_series():
    res = lim.series('FP_2020J', start_date=date(2020, 1, 1))
    assert res['FP_2020J']['2020-01-02'] == 608.5


def test_series2():
    res = lim.series('PA0002779.6.2')
    assert res['PA0002779.6.2']['2020-01-02'] == 479.75


def test_series3():
    res = lim.series('PUMFE03')
    assert res['PUMFE03']['2020-01-01'] == 463.716


def test_series4():
    res = lim.series('PJABA00')
    assert res['PJABA00']['1990-01-02'] == 246.5


def test_series5():
    res = lim.series({'FP_2020J' : 'GO', 'FB_2020J' : 'Brent'})
    assert res['GO']['2020-01-02'] == 608.5
    assert res['Brent']['2020-01-02'] == 65.56


def test_curve():
    res = lim.curve({'FP': 'GO', 'FB': 'Brent'})
    assert 'GO' in res.columns
    assert 'Brent' in res.columns


def test_curve2():
    res = lim.curve({'FP': 'GO', 'FB': 'Brent'})
    assert 'GO' in res.columns
    assert 'Brent' in res.columns


def test_curve3():
    res = lim.curve('FB', curve_dates=(pd.to_datetime('2020-03-17'),))
    assert res['2020/03/17']['2020-05-01'] == 28.73
    assert res['2020/03/17']['2020-08-01'] == 33.25


def test_curve4():
    res = lim.curve('FB', curve_dates=pd.to_datetime('2020-03-17'))
    assert res['2020/03/17']['2020-05-01'] == 28.73
    assert res['2020/03/17']['2020-08-01'] == 33.25


def test_curve_history():
    res = lim.curve('FP', curve_dates=(pd.to_datetime('2020-03-17'), pd.to_datetime('2020-03-18')))
    assert '2020/03/17' in res.columns
    assert '2020/03/18' in res.columns


def test_curve_formula():
    res = lim.curve_formula(formula='Show 1: FP/7.45-FB')
    assert 'FP' in res.columns
    assert 'FB' in res.columns
    assert '1' in res.columns


def test_curve_formula2():
    cd = (pd.to_datetime('2020-02-02'), pd.to_datetime('2020-04-04'))
    res = lim.curve_formula(formula='Show 1: FP/7.45-FB', curve_dates=cd)
    assert '2020/02/02' in res.columns
    assert '2020/04/04' in res.columns
    assert res['2020/02/02']['2020-05-01'] == 10.929
    assert res['2020/04/04']['2020-08-01'] == 8.50930


def test_symbol_contracts1():
    res = lim.get_symbol_contract_list('FB', monthly_contracts_only=True)
    assert 'FB_1998J' in res
    assert 'FB_2020Z' in res


def test_symbol_contracts2():
    res = lim.get_symbol_contract_list(('CL','FB'), monthly_contracts_only=True)
    assert 'CL_1998J' in res
    assert 'FB_2020Z' in res


def test_futures_contracts1(current_year: int):
    res = lim.contracts('FB', start_year=2020, start_date='2020-01-01')
    assert '2020Z' in res.columns
    assert f'{current_year}Z' in res.columns


def test_futures_contracts2(current_year: int):
    res = lim.contracts('FB', months=['Z'], start_date='date is within 5 days')
    assert f'{current_year}Z' in res.columns


def test_futures_contracts_formula(current_year: int):
    res = lim.contracts(formula='Show 1: FP/7.45-FB', months=['F'], start_year=2020, start_date='2020-01-01')
    assert f'{current_year}F' in res.columns
    assert res['2021F']['2020-01-02'] == pytest.approx(16.95, abs=0.01)


def test_cont_futures_rollover():
    res = lim.continuous_futures_rollover('FB', months=['M1', 'M12'], after_date=2019)
    assert res['M1'][pd.to_datetime('2020-01-02')] == 66.25
    assert res['M12'][pd.to_datetime('2020-01-02')] == 60.94


def test_structure1():
    res = lim.structure('FB', 1, 2, start_date=pd.to_datetime('2020-01-01'))
    assert res['M1-M2'][pd.to_datetime('2020-01-02')] == pytest.approx(0.689, abs=0.01)


def test_structure2():
    res = lim.structure('FB', 1, 12, start_date=pd.to_datetime('2020-01-01'))
    assert res['M1-M12'][pd.to_datetime('2020-01-02')] == pytest.approx(5.31)


def test_structure3():
    res = lim.structure('Show 1: FP/7.45-FB', 1, 2, start_date=pd.to_datetime('2020-01-01'))
    assert res['M1-M2'][pd.to_datetime('2020-01-02')] == pytest.approx(-0.656, abs=0.01)


def test_structure4():
    res = lim.structure('Show 1: FP/7.45-FB', 1, 12, start_date=pd.to_datetime('2020-01-01'))
    assert res['M1-M12'][pd.to_datetime('2020-01-02')] == pytest.approx(-1.18, abs=0.01)

def test_metadata():
    symbols = ('FB', 'PCAAS00', 'PUMFE03', 'PJABA00')
    m = lim.relations(symbols, show_columns=True, date_range=True)
    assert isinstance(m['FB']['daterange'], pd.DataFrame)
    assert 'FB' in m.columns
    assert 'PCAAS00' in m.columns
    assert 'PUMFE03' in m.columns
    assert 'PJABA00' in m.columns
    assert m['PJABA00']['daterange']['start']['Low'] == pd.to_datetime('1979-09-03')
    assert m['PJABA00']['daterange']['start']['Close'] == pd.to_datetime('2011-01-31')
    assert m['PJABA00']['daterange']['start']['High'] == pd.to_datetime('1979-09-03')


def test_relations1():
    symbol = 'TopRelation:Futures:Cboe'
    res = lim.relations(symbol, desc=True)
    assert res['Cboe']['description'] == "Chicago Board Options Exchange"


def test_relations2():
    symbol = 'FB,CL'
    res = lim.relations(symbol, show_children=True)
    assert 'FB' in res.loc['children']['FB']['name'].iloc[0]
    assert 'CL' in res.loc['children']['CL']['name'].iloc[0]


def test_find_symbols_in_path1():
    path = 'TopRelation:Futures:Ipe'
    res = lim.find_symbols_in_path(path)
    assert 'WI_Q21' in res
    assert 'FB' in res


def test_find_symbols_in_query():
    q = 'Show 1: FP/7.45-FB'
    r = lim.find_symbols_in_query(q)
    assert 'FP' in r
    assert 'FB' in r
