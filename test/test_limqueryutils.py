import pandas as pd

from pylim import limqueryutils


def test_build_futures_contracts_formula_query():
    f = 'Show 1: FP/7.45-FB'
    m = ['FP', 'FB']
    c = ['2020F', '2020G']
    res = limqueryutils.build_futures_contracts_formula_query(f, m, c)
    assert 'FP_2020F/7.45-FB_2020F' in res
    assert 'FP_2020G/7.45-FB_2020G' in res


def test_build_futures_contracts_formula_query_2():
    f = 'Show 1: FP/7.45-FP_LONGER'
    m = ['FP', 'FP_LONGER']
    c = ['2020F', '2020G']
    res = limqueryutils.build_futures_contracts_formula_query(f, m, c)
    assert 'FP_2020F/7.45-FP_LONGER_2020F' in res
    assert 'FP_2020G/7.45-FP_LONGER_2020G' in res


def test_build_build_curve_query():
    matches = {'FP': 'FUTURES', 'FB': 'FUTURES'}
    formula = 'Show 1: FP/7.45-FB'
    column = 'Close'
    curve_dates = pd.to_datetime('2020-05-01')
    res = limqueryutils.build_curve_query(symbols=matches, curve_date=curve_dates, column=column,
                                          curve_formula_str=formula)

    assert 'ATTR @FP = forward_curve(FP,"Close","05/01/2020","","","days","",0 day ago)' in res
    assert 'ATTR @FB = forward_curve(FB,"Close","05/01/2020","","","days","",0 day ago)' in res
    assert '1: @FP/7.45-@FB' in res


def test_build_build_curve_query_mix_types():
    matches = {'FP': 'FUTURES', 'FB': 'FUTURES', 'GBPUSD': 'NORMAL'}
    formula = 'Show 1: FP/7.45-FB + GBPUSD'
    column = 'Close'
    curve_dates = pd.to_datetime('2020-05-01')
    res = limqueryutils.build_curve_query(symbols=matches, curve_date=curve_dates, column=column,
                                          curve_formula_str=formula)

    assert 'ATTR @FP = forward_curve(FP,"Close","05/01/2020","","","days","",0 day ago)' in res
    assert 'ATTR @FB = forward_curve(FB,"Close","05/01/2020","","","days","",0 day ago)' in res
    assert 'ATTR @GBPUSD = if GBPUSD is defined then GBPUSD else GBPUSD on previous {GBPUSD is defined} ENDIF' in res
    assert '1: @FP/7.45-@FB + 0' in res

