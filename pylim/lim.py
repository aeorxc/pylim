import logging
import os
import re
import time
import typing as t
from collections.abc import Sequence
from datetime import date
from itertools import chain
from urllib.parse import urljoin
from urllib.request import getproxies

import pandas as pd
import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from pylim import limutils
from pylim import limqueryutils

limServer = os.environ['LIMSERVER'].replace('"', '')
limUserName = os.environ['LIMUSERNAME'].replace('"', '')
limPassword = os.environ['LIMPASSWORD'].replace('"', '')

calltries = 50
sleep = 0.5

headers = {
    'Content-Type': 'application/xml',
}


def get_session():
    """
    HTTP Session object configured for requesting data from LIM API.
    """
    retry_adapter = HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=2,
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            status_forcelist=[429, 500, 502, 503, 504],
        ),
    )
    session = requests.Session()
    session.auth = (limUserName, limPassword)
    session.headers = headers
    session.proxies = getproxies()
    session.mount("http://", retry_adapter)
    session.mount("https://", retry_adapter)
    return session


def is_sequence(obj: t.Any) -> bool:
    if isinstance(obj, str):
        return False
    return isinstance(obj, Sequence)


def query(q: str, id: t.Optional[int] = None, tries: int = calltries) -> pd.DataFrame:
    if tries == 0:
        raise Exception('Run out of tries')

    r = f'<DataRequest><Query><Text>{q}</Text></Query></DataRequest>'
    base_url = urljoin(limServer, f'/rs/api/datarequests')
    with get_session() as session:
        if id is None:
            response = session.post(base_url, data=r)
        else:
            response = session.get(f'{base_url}/{id}')
    try:
        response.raise_for_status()
    except requests.RequestException:
        logging.error(f'Received response: Code: {response.status_code} Msg: {response.text}')
        raise

    root = etree.fromstring(response.text.encode('utf-8'))
    req_status = int(root.attrib['status'])
    if req_status == 100:
        res = limutils.build_dataframe(root[0])
        return res
    elif req_status == 110:
        logging.info('Invalid query')
        raise Exception(root.attrib['statusMsg'])
    elif req_status == 130:
        logging.info('No data')
    elif req_status == 200:
        logging.debug('Not complete')
        reqId = int(root.attrib['id'])
        time.sleep(sleep)
        return query(q, reqId, tries - 1)
    else:
        raise Exception(root.attrib['statusMsg'])


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
    curve_formula: str = None,
) -> pd.DataFrame:
    scall = symbols
    if isinstance(scall, str):
        scall = tuple([scall])
    elif isinstance(scall, dict):
        scall = tuple(scall)

    if curve_formula is None and curve_dates is not None:
        q = limqueryutils.build_curve_history_query(scall, curve_dates, column)
    else:
        if is_sequence(curve_dates) and len(curve_dates):
            curve_date = curve_dates[0]
        else:
            curve_date = curve_dates
        q = limqueryutils.build_curve_query(scall, curve_date, column, curve_formula=curve_formula)
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
) -> pd.DataFrame:
    """
    Calculate a forward curve using existing symbols.
    """
    matches = find_symbols_in_query(formula)
    if curve_dates is None:
        res = curve(matches, column=column, curve_formula=formula)
    else:
        dfs, res = [], None
        if not is_sequence(curve_dates):
            curve_dates = [curve_dates]
        for d in curve_dates:
            rx = curve(matches, column=column, curve_dates=d, curve_formula=formula)
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
    contracts_list = get_symbol_contract_list(tuple(matches), monthly_contracts_only=True)
    contracts_list = limutils.filter_contracts(contracts_list, start_year=start_year, end_year=end_year, months=months)

    return _contracts(formula, matches=matches, contracts_list=contracts_list, start_date=start_date)


def structure(symbol: str, mx: int, my: int, start_date: t.Optional[date] = None) -> pd.DataFrame:
    sx = limqueryutils.continuous_convention(symbol, symbol, mx=mx)
    sy = limqueryutils.continuous_convention(symbol, symbol, mx=my)

    df = series([sx, sy], start_date=start_date)
    df[f'M{mx}-M{my}'] = df[sx] - df[sy]

    return df


def relations(
    symbol: t.Union[str, t.Tuple],
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
    url = urljoin(limServer, f'/rs/api/schema/relations/{symbol}')
    params = {
        'showChildren': str(show_children).lower(),
        'showColumns': str(show_columns).lower(),
        'desc': str(desc).lower(),
        'dateRange': str(date_range).lower(),
    }
    with get_session() as session:
        response = session.get(url, params=params)
    try:
        response.raise_for_status()
    except requests.RequestException:
        logging.error(f'Received response: Code: {response.status_code} Msg: {response.text}')
        raise
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
            if row.type == 'CATEGORY':
                rec_symbols = find_symbols_in_path(f'{path}:{row["name"]}')
                symbols = symbols + rec_symbols
    return symbols


def get_symbol_contract_list(symbol: str, monthly_contracts_only: bool = False) -> list:
    """
    Given a symbol pull all futures contracts related to it.
    """
    response = relations(symbol, show_children=True)
    children = response.loc['children']
    contracts = chain.from_iterable(
        children[symbol]['name'].to_list() for symbol in response.columns
    )
    if monthly_contracts_only:
        return [c for c in contracts if re.findall(r'\d\d\d\d\w', c)]
    return list(contracts_new)


def find_symbols_in_query(q: str) -> list:
    m = re.findall(r'\w[a-zA-Z0-9_]+', q)
    if 'Show' in m:
        m.remove('Show')
    rel = relations(tuple(m)).T
    rel = rel[rel['type'].isin(['FUTURES', 'NORMAL'])]
    if len(rel) > 0:
        return list(rel['name'])
    return []
