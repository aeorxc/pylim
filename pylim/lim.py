import pandas as pd
import os
import re
import time
from lxml import etree
import requests
from functools import lru_cache
import logging
import hashlib


limServer = os.environ['LIMSERVER'].replace('"', '')
limUserName = os.environ['LIMUSERNAME'].replace('"', '')
limPassword = os.environ['LIMPASSWORD'].replace('"', '')

lim_datarequests_url = '{}/rs/api/datarequests'.format(limServer)
lim_schema_url = '{}/rs/api/schema/relations/<SYMBOL>?showChildren=false&desc=true&showColumns=true&dateRange=true'.format(limServer)
lim_schema_futurues_url = '{}/rs/api/schema/relations/<SYMBOL>?showChildren=true&desc=true&showColumns=false&dateRange=true'.format(limServer)

calltries = 50
sleep = 2.5

curyear = pd.datetime.now().year
prevyear = curyear - 1

headers = {
    'Content-Type': 'application/xml',
}

proxies = {
    'http': os.getenv('http_proxy'),
    'https': os.getenv('https_proxy')
}


def alternate_col_val(values, noCols):
    for x in range(0, len(values), noCols):
        yield values[x:x + noCols]


def query_hash(query):
    r = hashlib.md5(query.encode()).hexdigest()
    rf = '{}.h5'.format(r)
    return rf


def build_dataframe(reports):
    columns = [x.text for x in reports.iter(tag='ColumnHeadings')]
    dates = [x.text for x in reports.iter(tag='RowDates')]
    if len(columns) == 0 or len(dates) == 0:
        return # no data, return`1

    values = [float(x.text) for x in reports.iter(tag='Values')]
    values = list(alternate_col_val(values, len(columns)))

    df = pd.DataFrame(values, columns=columns, index=pd.to_datetime(dates))
    return df


def query_cached(q):
    qmod = q
    res_cache = None
    rf = query_hash(q)
    if os.path.exists(rf):
        res_cache = pd.read_hdf(rf, mode='r')
        if res_cache is not None and 'date is after' not in q:
            cutdate = (res_cache.iloc[-1].name + pd.DateOffset(-5)).strftime('%m/%d/%Y')
            qmod += ' when date is after {}'.format(cutdate)

    res = query(qmod)
    hdf = pd.HDFStore(rf)
    if res_cache is None:
        hdf.put('d', res, format='table', data_columns=True)
        hdf.close()
    else:
        res = pd.concat([res_cache, res], sort=True).drop_duplicates()
        hdf.put('d', res, format='table', data_columns=True)
        hdf.close()

    return res


def query(q, id=None, tries=calltries, cache_inc=False):
    if cache_inc:
        return query_cached(q)

    r = '<DataRequest><Query><Text>{}</Text></Query></DataRequest>'.format(q)

    if tries == 0:
        raise Exception('Run out of tries')

    if id is None:
        resp = requests.request("POST", lim_datarequests_url, headers=headers, data=r, auth=(limUserName, limPassword), proxies=proxies)
    else:
        uri = '{}/{}'.format(lim_datarequests_url, id)
        resp = requests.get(uri, headers=headers, auth=(limUserName, limPassword), proxies=proxies)
    status = resp.status_code
    if status == 200:
        root = etree.fromstring(resp.text.encode('utf-8'))
        reqStatus = int(root.attrib['status'])
        if reqStatus == 100:
            res = build_dataframe(root[0])
            return res
        elif reqStatus == 130:
            logging.info('No data')
        elif reqStatus == 200:
            logging.debug('Not complete')
            reqId = int(root.attrib['id'])
            time.sleep(sleep)
            return query(q, reqId, tries - 1)
        else:
            raise Exception(root.attrib['statusMsg'])
    else:
        logging.error('Received response: Code: {} Msg: {}'.format(resp.status_code, resp.text))
        raise Exception(resp.text)


def check_pra_symbol(symbol):
    """
    Check if this is a Platts or Argus Symbol
    :param symbol:
    :return:
    """
    # Platts
    if len(symbol) == 7 and symbol[:2] in [
        'PC', 'PA', 'AA', 'PU', 'F1', 'PH', 'PJ', 'PG', 'PO', 'PP', ]:
        return True

    # Argus
    if '.' in symbol:
        sm = symbol.split('.')[0]
        if len(sm) == 9 and sm.startswith('PA'):
            return True

    return False


def build_series_query(symbols):
    q = 'Show \n'
    for symbol in symbols:
        qx = '{}: {}\n'.format(symbol, symbol)
        if check_pra_symbol(symbol):
            meta = metadata(tuple(symbols))
            meta = meta[symbol]
            r = dict(zip(meta['columns'], meta['column_starts']))
            if 'Low' in r and 'High' in r:
                use_high_low = False
                if 'Close' in r and r['Low'] < r['Close']:
                    use_high_low = True
                if 'MidPoint' in r and r['Low'] < r['MidPoint']:
                    use_high_low = True
                if use_high_low:
                    qx = '%s: (High of %s + Low of %s)/2 \n' % (symbol, symbol, symbol)

        q += qx
    return q


def series(symbols):
    scall = symbols
    if isinstance(scall, str):
        scall = [scall]
    if isinstance(scall, dict):
        scall = list(scall.keys())

    q = build_series_query(scall)
    res = query(q)

    if isinstance(symbols, dict):
        res = res.rename(columns=symbols)

    return res


def build_let_show_when_helper(lets, shows, whens):
    query = '''
            LET
            {0}
            SHOW
            {1}
            WHEN
            {2}
        '''.format(lets, shows, whens)
    return query


def build_curve_history_query(symbols, column='Close', curve_dates=None):
    """
    Build query for single symbol and multiple curve dates
    :param symbols:
    :param column:
    :param curve_dates:
    :return:
    """

    lets, shows, whens = '', '', ''
    counter = 0
    for curve_date in curve_dates:
        counter += 1
        curve_date_str, curve_date_str_nor = curve_date.strftime("%m/%d/%Y"), curve_date.strftime("%Y/%m/%d")

        inc_or = ''
        if len(curve_dates) > 1 and counter != len(curve_dates):
            inc_or = 'OR'
        lets += 'ATTR x{0} = forward_curve({1},"{2}","{3}","","","days","",0 day ago)\n'.format(counter, symbols[0], column, curve_date_str)
        shows += '{0}: x{1}\n'.format(curve_date_str_nor, counter)
        whens += 'x{0} is DEFINED {1}\n'.format(counter, inc_or)
    return build_let_show_when_helper(lets, shows, whens)


def build_curve_query(symbols, column='Close', curve_date=None, curve_formula=None):
    """
    Build query for multiple symbols and single curve dates
    :param symbols:
    :param column:
    :param curve_date:
    :param curve_formula:
    :return:
    """
    lets, shows, whens = '', '', ''
    counter = 0

    for symbol in symbols:
        counter += 1
        curve_date_str = "LAST" if curve_date is None else curve_date.strftime("%m/%d/%Y")

        inc_or = ''
        if len(symbols) > 1 and counter != len(symbols):
            inc_or = 'OR'

        lets += 'ATTR x{1} = forward_curve({1},"{2}","{3}","","","days","",0 day ago)\n'.format(counter, symbol, column, curve_date_str)
        shows += '{0}: x{0}\n'.format(symbol)
        whens += 'x{0} is DEFINED {1}\n'.format(symbol, inc_or)

    if curve_formula is not None:
        if 'Show' in curve_formula or 'show' in curve_formula:
            curve_formula = curve_formula.replace('Show', '').replace('show', '')
        for symbol in symbols:
            curve_formula = curve_formula.replace(symbol, 'x%s' % (symbol))
        shows += curve_formula
    return build_let_show_when_helper(lets, shows, whens)


def curve(symbols, column='Close', curve_dates=None, curve_formula=None):
    scall = symbols
    if isinstance(scall, str):
        scall = [scall]
    if isinstance(scall, dict):
        scall = list(scall.keys())

    if curve_dates is not None and isinstance(curve_dates, list) and len(curve_dates) > 1:
        q = build_curve_history_query(scall, column, curve_dates)
    else:
        q = build_curve_query(scall, column, curve_dates, curve_formula=curve_formula)
    res = query(q)

    if isinstance(symbols, dict):
        res = res.rename(columns=symbols)

    # only keep the current forward curve, discard history
    if res is not None and len(res) > 0:
        res = res['{}-{}'.format(pd.datetime.now().year, pd.datetime.now().month):]
        # reindex dates to start of month
        res = res.resample('MS').mean()
        res = res.dropna(how='any') # only keep months where we have values for all
        return res


def curve_formula(curve_formula, column='Close', curve_dates=None, valid_symbols=None):
    """
    Calculate a forward curve using existing symbols
    :param curve_formula:
    :param column:
    :param curve_dates:
    :param valid_symbols:
    :return:
    """
    if valid_symbols is None:
        valid_symbols = ['FP', 'FB'] # todo get valid list of symbols from API

    matches = re.findall(r"(?=(" + '|'.join(valid_symbols) + r"))", curve_formula)

    if curve_dates is None:
        res = curve(matches, column=column, curve_formula=curve_formula)
    else:
        dfs, res = [], None
        for d in curve_dates:
            rx = curve(matches, column=column, curve_dates=d, curve_formula=curve_formula)
            if rx is not None:
                rx = rx[['1']].rename(columns={'1':d.strftime("%Y/%m/%d")})
                dfs.append(rx)
        if len(dfs) > 0:
            res = pd.concat(dfs, 1)

    return res


def build_continuous_futures_rollover_query(symbol, months=['M1'], rollover_date='5 days before expiration day', after_date=prevyear):
    lets, shows, whens = '', '', 'Date is after {}\n'.format(after_date)
    for month in months:
        m = int(month[1:])
        if m == 1:
            rollover_policy = 'actual prices'
        else:
            rollover_policy = '{} nearby actual prices'.format(m)
        lets += 'M{1} = {0}(ROLLOVER_DATE = "{2}",ROLLOVER_POLICY = "{3}")\n '.format(symbol, m, rollover_date, rollover_policy)
        shows += 'M{0}: M{0} \n '.format(m)

    return build_let_show_when_helper(lets, shows, whens)


def continuous_futures_rollover(symbol, months=['M1'], rollover_date='5 days before expiration day', after_date=prevyear):
    q = build_continuous_futures_rollover_query(symbol, months=months, rollover_date=rollover_date, after_date=after_date)
    res = query(q)
    return res


@lru_cache(maxsize=None)
def futures_contracts(symbol, start_year=curyear, end_year=curyear+2):
    contracts = get_symbol_contract_list(symbol, monthly_contracts_only=True)
    contracts = [x for x in contracts if start_year <= int(x.split('_')[-1][:4]) <= end_year]
    df = series(contracts)
    return df


@lru_cache(maxsize=None)
def get_symbol_contract_list(symbol, monthly_contracts_only=False):
    """
    Given a symbol pull all futurues contracts related to it
    :param symbol:
    :return:
    """
    uri = lim_schema_futurues_url.replace('<SYMBOL>', symbol)
    resp = requests.get(uri, headers=headers, auth=(limUserName, limPassword), proxies=proxies)

    if resp.status_code == 200:
        root = etree.fromstring(resp.text.encode('utf-8'))
        contracts = [x.attrib['name'] for x in root[0][0]]
        if monthly_contracts_only:
            contracts = [x for x in contracts if re.findall('\d\d\d\d\w', x) ]
        return contracts
    else:
        logging.error('Received response: Code: {} Msg: {}'.format(resp.status_code, resp.text))
        raise Exception(resp.text)


@lru_cache(maxsize=None)
def metadata(symbols):
    if isinstance(symbols, str):
        symbols = [symbols]
    uri = lim_schema_url.replace('<SYMBOL>', ','.join(symbols))
    resp = requests.get(uri, headers=headers, auth=(limUserName, limPassword), proxies=proxies)
    if resp.status_code == 200:
        root = etree.fromstring(resp.text.encode('utf-8'))
        metadata = pd.concat([pd.Series(x.values(), index=x.attrib) for x in root], 1, sort=False)
        columntracker = {}
        columnstart = {}
        columnend = {}
        for symbolindex in metadata.columns:
            cols = root[symbolindex].find('Columns')
            if cols is not None and len(cols) > 0:
                colsnames = [x.attrib['cName'] for x in cols.getchildren()]
                columntracker[symbolindex] = colsnames
                colstarti, colendi = [], []
                for col in cols:
                    colranges = col.getchildren()
                    colstarti.append(pd.to_datetime(colranges[0].text[:10]))
                    colendi.append(pd.to_datetime(colranges[1].text[:10]))

                columnstart[symbolindex] = colstarti
                columnend[symbolindex] = colendi
        metadata = metadata.append(pd.Series(columntracker, name='columns'))
        metadata = metadata.append(pd.Series(columnstart, name='column_starts'))
        metadata = metadata.append(pd.Series(columnend, name='column_ends'))

        metadata.columns = metadata.loc['name']
        return metadata
