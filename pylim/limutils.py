import pandas as pd
from commodutil import forwards
import re


def alternate_col_val(values, noCols):
    for x in range(0, len(values), noCols):
        yield values[x:x + noCols]


def build_dataframe(reports):
    columns = [x.text for x in reports.iter(tag='ColumnHeadings')]
    dates = [x.text for x in reports.iter(tag='RowDates')]
    if len(columns) == 0 or len(dates) == 0:
        return # no data, return`1

    values = [float(x.text) for x in reports.iter(tag='Values')]
    values = list(alternate_col_val(values, len(columns)))

    df = pd.DataFrame(values, columns=columns, index=pd.to_datetime(dates))
    return df


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


def relinfo_children(df, root):
    """
    Convert the children from relinfo into a dataframe and attached to main result
    :param df:
    :param root:
    :return:
    """
    dfs = []

    for col in df.columns:
        namec = pd.Series([x.attrib['name'] for x in root[col][0]], dtype='object')
        namec.name = 'name'
        typec = pd.Series([x.attrib['type'] for x in root[col][0]], dtype='object')
        typec.name = 'type'
        d = pd.concat([namec, typec], 1)
        dfs.append(d)

    if len(df.columns) == 1:
        df.loc['children'] = dfs
    else:
        d2 = pd.DataFrame(dfs, dtype='object', columns=['children'])
        df = df.append(d2.T)
    return df


def relinfo_daterange(df, root):
    """
    Convert the date ranges from relinfo into a dataframe and attached to main result
    :param df:
    :param root:
    :return:
    """
    dfs = []

    for symbol in df.columns:
        cols = root[symbol].find('Columns')
        colnames = [x.attrib['cName'] for x in cols.getchildren()]
        s = []
        for col in cols:
            start = pd.to_datetime(col.getchildren()[0].text[:10])
            end = pd.to_datetime(col.getchildren()[1].text[:10])
            s.append([start, end])

        dr = pd.DataFrame(s, index=colnames, columns=['start', 'end'])
        dfs.append(dr)

    if len(df.columns) == 1:
        df.loc['daterange'] = dfs
    else:
        d2 = pd.DataFrame(dfs, dtype='object', columns=['daterange'])
        df = df.append(d2.T)
    return df


def determine_month(sample):
    if isinstance(sample, int) and sample in forwards.futures_month_conv:
        return forwards.futures_month_conv[sample]
    if isinstance(sample, str):
        if sample in forwards.futures_month_conv.values():
            return sample
        if sample.upper() == 'Q1':
            return ['F', 'G', 'H']
        if sample.upper() == 'Q2':
            return ['J', 'K', 'M']
        if sample.upper() == 'Q3':
            return ['N', 'Q', 'U']
        if sample.upper() == 'Q4':
            return ['V', 'X', 'Z']
        if sample.upper().startswith('CAL'):
            return list(forwards.futures_month_conv.values())


def determine_year(samples):
    res = []
    for sample in samples:
        if sample is not None and isinstance(sample, str):
            sample = sample.upper()
            if sample.startswith('CAL'):
                x = sample.replace('CAL', '')
                if len(x) == 2:
                    x = '20' + x
                res.append(x)

    return res


def filter_contracts_months(contracts, months):
    months_org = months
    if not isinstance(months, list):
        months = [months]

    months = [determine_month(x) for x in months]
    if None in months:
        months.remove(None)
    months = [item for sublist in months for item in sublist] # flatten list

    contracts = [x for x in contracts if x[-1] in months]

    # year limiter for cal
    year_filter = determine_year(months_org)
    if year_filter is not None and len(year_filter) > 0:
        contracts = [x for x in contracts if [y for y in year_filter if y in x]]

    return contracts


def filter_contracts(contracts, start_year=None, end_year=None, months=None):
    """
    Given list of contracts (eg FB_2020G) filter by start/end year and month
    :param contracts:
    :param start_year:
    :param end_year:
    :param months:
    :return:
    """
    if start_year is not None:
        contracts = [x for x in contracts if start_year <= int(x.split('_')[-1][:4])]
    if end_year is not None:
        contracts = [x for x in contracts if int(x.split('_')[-1][:4]) <= end_year]
    if months is not None:
        contracts = filter_contracts_months(contracts, months)
    return contracts


def pivots_contract_by_year(df):
    """
    Given a list of contracts eg 2020F, 2019F, average by year
    :param df:
    :return:(
    """
    dfs = []
    for year in set([re.search('\d{4}', x).group() for x in df.columns]):
        d = df[[x for x in df.columns if year in x]].mean(1)
        d.name = year
        dfs.append(d)

    df = pd.concat(dfs, 1)
    df = df.reindex(sorted(df.columns), axis=1)
    return df
