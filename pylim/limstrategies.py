import pandas as pd
from commodutil import forwards
from pylim import lim
from pylim import limutils


curyear = lim.curyear


def _contracts(symbol, start_year=None, end_year=None, months=None):
    if symbol.lower().startswith('show'):
        df = lim.futures_contracts_formula(symbol, start_year=start_year, end_year=end_year, months=months)
    else:
        df = lim.futures_contracts(symbol, start_year=start_year, end_year=end_year, months=months)
    return df


def quarterly(symbol, quarter=1, start_year=curyear, end_year=curyear+2):
    """
    Given a symbol or formula, calculate the quarterly average and return as a series of yearly timeseries
    :param symbol:
    :param quarter:
    :param start_year:
    :param end_year:
    :return:
    """
    cmap = {1: ['F', 'G', 'H'], 2: ['J', 'K', 'M'], 3: ['N', 'Q', 'U'], 4: ['V', 'X', 'Z']}
    return calendar(symbol, start_year=start_year, end_year=end_year, months=cmap[quarter])


def calendar(symbol, start_year=curyear, end_year=curyear+2, months=None):
    """
    Given a symbol or formula, calculate the calendar (yearly) average and return as a series of yearly timeseries
    :param symbol:
    :param quarter:
    :param start_year:
    :param end_year:
    :return:
    """
    df = _contracts(symbol, start_year=start_year, end_year=end_year, months=months)
    return limutils.pivots_contract_by_year(df)


def spread(symbol, x, y, z=None, start_year=None, end_year=None):
    contracts = _contracts(symbol, start_year=start_year, end_year=end_year, months=[x,y,z])
    contracts = contracts.rename(columns={x: pd.to_datetime(forwards.convert_contract_to_date(x)) for x in contracts.columns})
    contracts = contracts.reindex(sorted(contracts.columns), axis=1) # sort values otherwise column selection in code below doesn't work

    if z is not None:
        if isinstance(x,int) and isinstance(y,int) and isinstance(z,int):
            return forwards.fly(contracts, x, y, z)

    if isinstance(x, int) and isinstance(y, int):
        return forwards.time_spreads_monthly(contracts, x, y)

    if isinstance(x, str) and isinstance(y, str):
        x, y = x.upper(), y.upper()
        if x.startswith('Q') and y.startswith('Q'):
            return forwards.time_spreads_quarterly(contracts, x, y)

        if x.startswith('CAL') and y.startswith('CAL'):
            return forwards.cal_spreads(forwards.cal_contracts(contracts))
