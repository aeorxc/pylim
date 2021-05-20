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
