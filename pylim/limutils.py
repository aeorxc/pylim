import pandas as pd
import re


futures_contract_months = {
    1: "F",
    2: "G",
    3: "H",
    4: "J",
    5: "K",
    6: "M",
    7: "N",
    8: "Q",
    9: "U",
    10: "V",
    11: "X",
    12: "Z"
}


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
        if isinstance(months, int):
            months = [futures_contract_months[months]]
        if isinstance(months, list):
            months = [futures_contract_months[x] if isinstance(x, int) else x for x in months]
            if None in months:
                months.remove(None)
        contracts = [x for x in contracts if x[-1] in months]
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
    return df
