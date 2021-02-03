import typing as t
import pandas as pd
import os
import re
import time
import datetime
from urllib.request import getproxies
from lxml import etree
import requests
from functools import lru_cache
import logging
from pylim import limutils
from pylim import limqueryutils

limServer = os.environ['LIMSERVER'].replace('"', '')
limUserName = os.environ['LIMUSERNAME'].replace('"', '')
limPassword = os.environ['LIMPASSWORD'].replace('"', '')

lim_datarequests_url = f'{limServer}/rs/api/datarequests'
lim_schema_url = f'{limServer}/rs/api/schema/relations'

calltries = 50
sleep = 2.5

headers = {
    'Content-Type': 'application/xml',
}

# HTTP Session object configured for requesting data from LIM API.
session = requests.Session()
session.proxies = getproxies()
session.headers = headers
session.auth = (limUserName, limPassword)


def query(q: str, id: t.Optional[int] = None, tries: int = calltries) -> pd.DataFrame:
    r = '<DataRequest><Query><Text>{}</Text></Query></DataRequest>'.format(q)

    if tries == 0:
        raise Exception('Run out of tries')

    if id is None:
        response = session.post(lim_datarequests_url, data=r)
    else:
        uri = f'{lim_datarequests_url}/{id}'
        response = session.get(uri)
    try:
        response.raise_for_status()
    except requests.RequestException:
        logging.error('Received response: Code: {} Msg: {}', response.status_code, response.text)
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


def series(symbols: t.Union[str, dict], start_date: t.Optional[t.Union[str, datetime.date]] = None) -> pd.DataFrame:
    scall = symbols
    if isinstance(scall, str):
        scall = [scall]
    if isinstance(scall, dict):
        scall = list(scall.keys())

    # get metadata if we have PRA symbol
    meta = None
    if any([limutils.check_pra_symbol(x) for x in scall]):
        meta = relations(tuple(scall), show_columns=True, date_range=True)

    q = limqueryutils.build_series_query(scall, meta, start_date=start_date)
    res = query(q)

    if isinstance(symbols, dict):
        res = res.rename(columns=symbols)

    return res


def curve(
    symbols: t.Union[str, dict],
    column: str = 'Close',
    curve_dates: t.Optional[t.Tuple[datetime.date, ...]] = None,
    curve_formula: str = None,
) -> pd.DataFrame:
    scall = symbols
    if isinstance(scall, str):
        scall = [scall]
    if isinstance(scall, dict):
        scall = list(scall.keys())

    if curve_formula is None and curve_dates is not None:
        q = limqueryutils.build_curve_history_query(scall, curve_dates, column)
    else:
        q = limqueryutils.build_curve_query(scall, curve_dates, column, curve_formula=curve_formula)
    res = query(q)

    if isinstance(symbols, dict):
        res = res.rename(columns=symbols)

    # Reindex dates to start of month.
    res = res.resample('MS').mean()
    return res


def curve_formula(
    formula: str,
    column: str = 'Close',
    curve_dates: t.Optional[t.Tuple[datetime.date, ...]] = None,
) -> pd.DataFrame:
    """
    Calculate a forward curve using existing symbols.
    """
    matches = find_symbols_in_query(formula)
    if curve_dates is None:
        res = curve(matches, column=column, curve_formula=formula)
    else:
        dfs, res = [], None
        if not isinstance(curve_dates, list) and not isinstance(curve_dates, tuple):
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
    after_date: t.Optional[datetime.date] = None
) -> pd.DataFrame:
    if after_date is None:
        after_date = datetime.date.today().year - 1
    q = limqueryutils.build_continuous_futures_rollover_query(
        symbol, months=months, rollover_date=rollover_date, after_date=after_date
    )
    res = query(q)
    return res


def contracts(
    formula: str,
    start_year: t.Optional[int] = None,
    end_year: t.Optional[int] = None,
    months: t.Optional[t.Tuple[str, ...]] = None,
    start_date: t.Optional[datetime.date] = None,
) -> pd.DataFrame:
    matches = find_symbols_in_query(formula)
    contracts = get_symbol_contract_list(tuple(matches), monthly_contracts_only=True)
    contracts = limutils.filter_contracts(contracts, start_year=start_year, end_year=end_year, months=months)

    s = []
    for match in matches:
        r = [x.split('_')[-1] for x in contracts if match in x]
        s.append(set(r))

    common_contacts = list(set(s[0].intersection(*s)))

    q = limqueryutils.build_futures_contracts_formula_query(
        formula, matches=matches, contracts=common_contacts, start_date=start_date
    )
    df = query(q)
    return df


def structure(symbol: str, mx: int, my: int, start_date: t.Optional[datetime.date] = None) -> pd.DataFrame:
    sx = limqueryutils.continuous_convention(symbol, symbol, mx=mx)
    sy = limqueryutils.continuous_convention(symbol, symbol, mx=my)

    df = series([sx, sy], start_date=start_date)
    df[f'M{mx}-M{my}'] = df[sx] - df[sy]

    return df


@lru_cache(maxsize=None)
def relations(symbol: str, show_children: bool = False, show_columns: bool = False, desc: bool = False,
              date_range: bool = False) -> pd.DataFrame:
    """
    Given a list of symbols call API to get Tree Relations, return as response.
    """
    if isinstance(symbol, list) or isinstance(symbol, tuple):
        symbol = set(symbol)
        symbol = ','.join(symbol)
    uri = f'{lim_schema_url}/{symbol}'
    params = {
        'showChildren': 'true' if show_children else 'false',
        'showColumns': 'true' if show_columns else 'false',
        'desc': 'true' if desc else 'false',
        'dateRange': 'true' if date_range else 'false',
    }
    resp = session.get(uri, params=params)

    if resp.status_code == 200:
        root = etree.fromstring(resp.text.encode('utf-8'))
        df = pd.concat([pd.Series(x.values(), index=x.attrib) for x in root], 1, sort=False)
        if show_children:
            df = limutils.relinfo_children(df, root)
        if date_range:
            df = limutils.relinfo_daterange(df, root)
        df.columns = df.loc['name']  # make symbol names header
        return df
    else:
        logging.error('Received response: Code: {} Msg: {}', resp.status_code, resp.text)
        raise Exception(resp.text)


@lru_cache(maxsize=None)
def find_symbols_in_path(path: str) -> list:
    """
    Given a path in the LIM tree hierarchy, find all symbols in that path
    :param path:
    :return:
    """
    symbols = []
    df = relations(path, show_children=True)

    for col in df.columns:
        children = df[col]['children']
        for i, row in children.iterrows():
            if row.type == 'FUTURES' or row.type == 'NORMAL':
                symbols.append(row['name'])
            if row.type == 'CATEGORY':
                rec_symbols = find_symbols_in_path('%s:%s' % (path, row['name']))
                symbols = symbols + rec_symbols

    return symbols


@lru_cache(maxsize=None)
def get_symbol_contract_list(symbol: str, monthly_contracts_only: bool = False) -> list:
    """
    Given a symbol pull all futures contracts related to it.
    """
    resp = relations(symbol, show_children=True)
    if resp is not None:
        children = resp.loc['children']
        contracts = []
        for symbol in resp.columns:
            contracts = contracts + list(children[symbol]['name'])
        if monthly_contracts_only:
            contracts = [x for x in contracts if re.findall(r'\d\d\d\d\w', x)]
        return contracts


@lru_cache(maxsize=None)
def find_symbols_in_query(q: str) -> list:
    m = re.findall(r'\w[a-zA-Z0-9_]+', q)
    if 'Show' in m:
        m.remove('Show')
    rel = relations(tuple(m)).T
    rel = rel[rel['type'].isin(['FUTURES', 'NORMAL'])]
    if len(rel) > 0:
        return list(rel['name'])
