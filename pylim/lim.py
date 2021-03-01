import re
import typing as t
from datetime import date

import pandas as pd
from lxml import etree

from pylim import limutils
from pylim import limqueryutils
from pylim.core import get_lim_session, query
from pylim.limutils import is_sequence


def series(symbols: t.Union[str, dict, tuple], start_date: t.Optional[t.Union[str, date]] = None) -> pd.DataFrame:
    scall = symbols
    if isinstance(scall, str):
        scall = tuple([scall])
    elif isinstance(scall, dict):
        scall = tuple(scall)

    # Get metadata if we have PRA symbol.
    meta = None
    if any([limutils.check_pra_symbol(x) for x in scall]):
        meta = relations(scall, show_columns=True, date_range=True)

    q = limqueryutils.build_series_query(scall, meta, start_date=start_date)
    res = query(q)

    if isinstance(symbols, dict):
        res = res.rename(columns=symbols)

    return res


def curve(
    symbols: t.Union[str, dict, tuple],
    column: str = 'Close',
    curve_dates: t.Optional[t.Union[date, t.Tuple[date, ...]]] = None,
) -> pd.DataFrame:
    scall = symbols
    if isinstance(scall, str):
        if limqueryutils.is_formula(symbols):
            return curve_formula(symbols, column=column, curve_dates=curve_dates)
        scall = tuple([scall])
    elif isinstance(scall, dict):
        scall = tuple(scall)

    if curve_dates is not None:
        if not is_sequence(curve_dates):
            curve_dates = (curve_dates,)
        q = limqueryutils.build_curve_history_query(scall, curve_dates, column)
    else:
        if is_sequence(curve_dates) and len(curve_dates):
            curve_date = curve_dates[0]
        else:
            curve_date = curve_dates
        q = limqueryutils.build_curve_query(scall, curve_date, column)
    res = query(q)

    if isinstance(symbols, dict):
        res = res.rename(columns=symbols)

    # Reindex dates to start of month.
    res = res.resample('MS').mean()
    return res


def curve_formula(
    formula: str,
    column: str = 'Close',
    curve_dates: t.Optional[t.Tuple[date, ...]] = None,
    matches: t.Optional[t.Tuple[str, ...]] = None
) -> pd.DataFrame:
    """
    Calculate a forward curve using existing symbols.
    """
    if matches is None:
        matches = find_symbols_in_query(formula)
    if curve_dates is None or len(curve_dates) == 1:
        if is_sequence(curve_dates):
            curve_dates = curve_dates[0]
        q = limqueryutils.build_curve_query(symbols=matches, curve_date=curve_dates, column=column, curve_formula_str=formula)
        res = query(q)
        res = res.resample('MS').mean()
    else:
        dfs, res = [], None
        if not is_sequence(curve_dates):
            curve_dates = [curve_dates]
        for d in curve_dates:
            rx = curve_formula(formula, column=column, curve_dates=(d,), matches=matches)
            if rx is not None:
                rx = rx[['1']].rename(columns={'1': d.strftime("%Y/%m/%d")})
                dfs.append(rx)
        if len(dfs) > 0:
            res = pd.concat(dfs, 1)
            res = res.dropna(how='all', axis=0)

    return res


def continuous_futures_rollover(
    symbol: str,
    months: t.Tuple[str, ...] = ('M1',),
    rollover_date: str = '5 days before expiration day',
    after_date: t.Optional[date] = None
) -> pd.DataFrame:
    if after_date is None:
        after_date = date.today().year - 1
    q = limqueryutils.build_continuous_futures_rollover_query(
        symbol, months=months, rollover_date=rollover_date, after_date=after_date
    )
    res = query(q)
    return res


def _contracts(
    formula: str,
    matches: t.Tuple[str, ...],
    contracts_list: t.Tuple[str, ...],
    start_date: t.Optional[date] = None,
) -> pd.DataFrame:
    s = []
    for match in matches:
        r = [x.split('_')[-1] for x in contracts_list if match in x]
        s.append(set(r))

    common_contacts = list(set(s[0].intersection(*s)))

    q = limqueryutils.build_futures_contracts_formula_query(
        formula, matches=matches, contracts=common_contacts, start_date=start_date
    )
    df = query(q)
    return df


def contracts(
    formula: str,
    start_year: t.Optional[int] = None,
    end_year: t.Optional[int] = None,
    months: t.Optional[t.Tuple[str, ...]] = None,
    start_date: t.Optional[date] = None,
) -> pd.DataFrame:
    matches = find_symbols_in_query(formula)
    contracts_list = get_symbol_contract_list(matches, monthly_contracts_only=True)
    contracts_list = limutils.filter_contracts(contracts_list, start_year=start_year, end_year=end_year, months=months)

    return _contracts(formula, matches=matches, contracts_list=contracts_list, start_date=start_date)


def structure(symbol: str, mx: int, my: int, start_date: t.Optional[date] = None) -> pd.DataFrame:
    matches = find_symbols_in_query(symbol)
    clause = limqueryutils.extract_clause(symbol)

    q = limqueryutils.build_structure_query(clause, matches, mx, my, start_date)
    res = query(q)
    return res


def relations(
    symbol: t.Union[str, t.Tuple[str, ...]],
    show_children: bool = False,
    show_columns: bool = False,
    desc: bool = False,
    date_range: bool = False,
) -> pd.DataFrame:
    """
    Given a list of symbols call API to get Tree Relations, return as response.
    """
    if is_sequence(symbol):
        symbol = ','.join(set(symbol))
    url = f'/rs/api/schema/relations/{symbol}'
    params = {
        'showChildren': str(show_children).lower(),
        'showColumns': str(show_columns).lower(),
        'desc': str(desc).lower(),
        'dateRange': str(date_range).lower(),
    }
    with get_lim_session() as session:
        response = session.get(url, params=params)
    root = etree.fromstring(response.text.encode('utf-8'))
    df = pd.concat([pd.Series(x.values(), index=x.attrib) for x in root], 1, sort=False)
    if show_children:
        df = limutils.relinfo_children(df, root)
    if date_range:
        df = limutils.relinfo_daterange(df, root)
    # Make symbol names the header.
    df.columns = df.loc['name']
    return df


def find_symbols_in_path(path: str) -> list:
    """
    Given a path in the LIM tree hierarchy, find all symbols in that path.
    """
    symbols = []
    df = relations(path, show_children=True)

    for col in df.columns:
        children = df[col]['children']
        for i, row in children.iterrows():
            if row.type == 'FUTURES' or row.type == 'NORMAL':
                symbols.append(row['name'])
            elif row.type == 'CATEGORY':
                rec_symbols = find_symbols_in_path(f'{path}:{row["name"]}')
                symbols = symbols + rec_symbols
    return symbols


def get_symbol_contract_list(
    symbol: t.Union[str, t.Tuple[str, ...]],
    monthly_contracts_only: bool = False,
) -> list:
    """
    Given a symbol pull all futures contracts related to it.
    """
    response = relations(symbol, show_children=True).T
    response = response[(response.hasChildren == '1') & (pd.notnull(response.children))].T
    children = response.loc['children']
    children = pd.concat(children.values)
    contracts_list = list(children.name.values)
    if monthly_contracts_only:
        contracts_list = [c for c in contracts_list if re.findall(r'\d\d\d\d\w', c)]
    return contracts_list


def find_symbols_in_query(q: str) -> tuple:
    m = re.findall(r'\w[a-zA-Z0-9_]+', q)
    if 'Show' in m:
        m.remove('Show')
    rel = relations(tuple(m)).T
    rel = rel[rel['type'].isin(['FUTURES', 'NORMAL'])]
    if len(rel) > 0:
        return tuple(rel['name'])
    return ()
