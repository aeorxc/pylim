import typing as t
import datetime
import pandas as pd
import calendar as cal
from commodutil import forwards
from pylim import lim
from pylim import limutils
from pylim import limqueryutils as lqu


def quarterly(symbol: str, quarter: int = 1, start_year=datetime.date.today().year,
              end_year=datetime.date.today().year + 2, start_date=t.Optional[datetime.date]):
    """
    Given a symbol or formula, calculate the quarterly average and return as a series of yearly timeseries
    :param symbol:
    :param quarter:
    :param start_year:
    :param end_year:
    :return:
    """
    cmap = {1: ['F', 'G', 'H'], 2: ['J', 'K', 'M'], 3: ['N', 'Q', 'U'], 4: ['V', 'X', 'Z']}

    if quarter == 0:  # calc Q1,Q2,Q3,Q4
        df = lim.contracts(symbol, start_year=start_year, end_year=end_year, start_date=start_date)
        dfs = []
        for qtr in cmap:  # filter columns for each quarter
            d = df[[x for x in df.columns if x[-1] in cmap[qtr]]]
            d = limutils.convert_lim_contracts_to_datetime(d)
            d = limutils.pivots_contract_by_year(d)
            d = d.rename(columns={x: 'Q%s_%s' % (qtr, x) for x in d.columns})  # eg Q12020
            dfs.append(d)
        return pd.concat(dfs, 1)
    else:
        return calendar(symbol, start_year=start_year, end_year=end_year, months=cmap[quarter], start_date=start_date)


def calendar(symbol, start_year=datetime.date.today().year, end_year=datetime.date.today().year + 2,
             months: t.Optional[t.Tuple[str, ...]] = None,
             start_date: t.Optional[datetime.date] = None):
    """
    Given a symbol or formula, calculate the calendar (yearly) average and return as a series of yearly timeseries
    :param months:
    :param symbol:
    :param quarter:
    :param start_year:
    :param end_year:
    :return:
    """
    df = lim.contracts(symbol, start_year=start_year, end_year=end_year, months=months, start_date=start_date)
    df = limutils.convert_lim_contracts_to_datetime(df)
    return limutils.pivots_contract_by_year(df)


def spread(symbol: str, x: t.Tuple[int, str], y: t.Tuple[int, str], z: t.Optional[t.Tuple[int, str]] = None,
           start_year: datetime.date.today().year = None, end_year: datetime.date.today().year = None,
           start_date: t.Optional[datetime.date] = None) -> pd.DataFrame:
    contracts = lim.contracts(symbol, start_year=start_year, end_year=end_year, months=[x, y, z],
                                 start_date=start_date)
    contracts = limutils.convert_lim_contracts_to_datetime(contracts)

    if z is not None:
        if isinstance(x, int) and isinstance(y, int) and isinstance(z, int):
            return forwards.fly(contracts, x, y, z)

    if isinstance(x, int) and isinstance(y, int):
        return forwards.time_spreads_monthly(contracts, x, y)

    if isinstance(x, str) and isinstance(y, str):
        x, y = x.upper(), y.upper()
        if x.startswith('Q') and y.startswith('Q'):
            return forwards.time_spreads_quarterly(contracts, x, y)

        if x.startswith('CAL') and y.startswith('CAL'):
            return forwards.cal_spreads(forwards.cal_contracts(contracts))


def fly(symbol: str, x: t.Tuple[int, str], y: t.Tuple[int, str], z: t.Optional[t.Tuple[int, str]],
        start_year: t.Optional[datetime.date] = None,
        end_year=None, start_date: t.Optional[datetime.date] = None):
    return spread(symbol, x, y, z, start_year=start_year, end_year=end_year, start_date=start_date)


def multi_spread(symbol, spreads, start_year: datetime.date.today().year = None,
                 end_year: datetime.date.today().year = None,
                 start_date: t.Optional[datetime.date] = None) -> pd.DataFrame:
    contracts = lim.contracts(symbol, start_year=start_year, end_year=end_year, start_date=start_date)
    contracts = limutils.convert_lim_contracts_to_datetime(contracts)

    dfs = []
    for spread in spreads:
        r = forwards.time_spreads_monthly(contracts, spread[0], spread[1])
        r = r.rename(
            columns={x: '%s%s_%s' % (cal.month_abbr[spread[0]], cal.month_abbr[spread[1]], x) for x in r.columns})
        dfs.append(r)

    res = pd.concat(dfs, 1)
    return res


def structure(symbol: str, mx: int, my: int, start_date: t.Optional[datetime.date] = None) -> pd.DataFrame:
    if not symbol.lower().startswith('show'):
        return lim.structure(symbol, mx, my, start_date=start_date)

    matches = lim.find_symbols_in_query(symbol)
    clause = lqu.extract_clause(symbol)

    q = lqu.build_structure_query(clause, matches, mx, my, start_date)
    res = lim.query(q)
    return res
