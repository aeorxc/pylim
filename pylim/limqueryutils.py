import re
import typing as t
from datetime import date, datetime, timedelta
from itertools import chain

import dateutil
import pandas as pd

from pylim import limutils

LIM_DATETIME_FORMAT = '%m/%d/%Y'


class LimQueryBuilder:
    let_keyword = 'LET'
    show_keyword = 'SHOW'
    when_keyword = 'WHEN'

    def __init__(self):
        self.lets = []
        self.shows = []
        self.whens = []

    def add_let(self, let: str):
        self.lets.append(let)

    def add_show(self, show: str):
        self.shows.append(show)

    def add_when(self, when: str):
        self.whens.append(when)

    def whens_to_or(self):
        """Convert existing buffer of 'whens' to a single OR statement."""
        or_statement = ' OR\n'.join(self.whens)
        or_statement = f'{{ {or_statement} }}'
        self.whens = [or_statement]

    def __str__(self) -> str:
        return '\n'.join(
            chain(
                (self.let_keyword,),
                self.lets,
                (self.show_keyword,),
                self.shows,
                (self.when_keyword,),
                self.whens,
            )
        )


def is_formula(symbol: str) -> bool:
    lowercase = symbol.lower()
    return lowercase.startswith("show") or lowercase.startswith("let")


def build_when_clause(start_date: t.Union[str, date]) -> str:
    if start_date:
        if isinstance(start_date, str):
            if 'date is within' in start_date.lower():
                return start_date
            else:
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
        previous_date = start_date - timedelta(days=1)
        return f'date is after {previous_date:{LIM_DATETIME_FORMAT}}'
    return ''


def build_series_query(
    symbols: t.Tuple[str, ...],
    metadata: t.Optional[pd.DataFrame] = None,
    start_date: t.Optional[t.Tuple[str, date]] = None,
) -> str:
    symbol_query_parts = ['Show']
    for symbol in symbols:
        qx = f'{symbol}: {symbol}'
        if limutils.check_pra_symbol(symbol):
            use_high_low = False
            if metadata is not None:
                meta = metadata[symbol]['daterange']
                if 'Low' in meta.index and 'High' in meta.index:
                    if 'Close' in meta.index and meta.start.Low < meta.start.Close:
                        use_high_low = True
                    if 'MidPoint' in meta.index and meta.start.Low <= meta.start.MidPoint:
                        use_high_low = True
            if use_high_low:
                qx = f'{symbol}: (High of {symbol} + Low of {symbol})/2'
        symbol_query_parts.append(qx)

    when = build_when_clause(start_date)
    if when:
        symbol_query_parts.append(f'when {when}')
    query = '\n'.join(symbol_query_parts)
    return query


def build_curve_query(
    symbols: t.Tuple[str, ...], curve_date: t.Optional[date] = None, column: str = 'Close', curve_formula: t.Optional[str] = None
) -> str:
    """
    Build query for multiple symbols and a single curve date.
    """
    builder = LimQueryBuilder()
    curve_date_filter = 'LAST' if curve_date is None else f'{curve_date:{LIM_DATETIME_FORMAT}}'
    for symbol in symbols:
        builder.add_let(f'ATTR x{symbol} = forward_curve({symbol},"{column}","{curve_date_filter}","","","days","",0 day ago)')
        builder.add_show(f'{symbol}: x{symbol}')
        builder.add_when(f'x{symbol} is DEFINED')
    builder.whens_to_or()

    if curve_formula is not None:
        if 'Show' in curve_formula or 'show' in curve_formula:
            curve_formula = curve_formula.replace('Show', '').replace('show', '')
        for symbol in symbols:
            curve_formula = curve_formula.replace(symbol, f'x{symbol}')
        builder.add_show(curve_formula)

    # When no curve date is specified we get a full history so filter it.
    if curve_date is None:
        last_month = datetime.now() - dateutil.relativedelta.relativedelta(months=1)
        builder.add_when(f'and date is after {last_month:{LIM_DATETIME_FORMAT}}')

    return str(builder)


def build_curve_history_query(
    symbols: t.Tuple[str], curve_dates: t.Tuple[date, ...], column: str = 'Close'
) -> str:
    """
    Build query for a single symbol and multiple curve dates.
    """
    builder = LimQueryBuilder()
    symbol = symbols[0]
    for counter, curve_date in enumerate(curve_dates, start=1):
        builder.add_let(f'ATTR x{counter} = forward_curve({symbol},"{column}","{curve_date:{LIM_DATETIME_FORMAT}}","","","days","",0 day ago)')
        builder.add_show(f'{curve_date:%Y/%m/%d}: x{counter}')
        builder.add_when(f'x{counter} is DEFINED')
    builder.whens_to_or()
    return str(builder)


def build_continuous_futures_rollover_query(
    symbol: str,
    months: t.Tuple[str, ...] = ('M1',),
    rollover_date: str = '5 days before expiration day',
    after_date: t.Optional[int] = None,
) -> str:
    if after_date is None:
        after_date = date.today().year - 1
    builder = LimQueryBuilder()
    builder.add_when(f'Date is after {after_date}')
    for month in months:
        m = int(month[1:])
        if m == 1:
            rollover_policy = 'actual prices'
        else:
            rollover_policy = f'{m} nearby actual prices'
        builder.add_let(f'M{m} = {symbol}(ROLLOVER_DATE = "{rollover_date}",ROLLOVER_POLICY = "{rollover_policy}")')
        builder.add_show(f'M{m}: M{m}')
    return str(builder)


def build_futures_contracts_formula_query(
    formula: str,
    matches: t.Tuple[str, ...],
    contracts: t.Tuple[str, ...],
    start_date: t.Optional[t.Union[str, date]] = None,
) -> str:
    builder = LimQueryBuilder()
    for contract in contracts:
        builder.add_show(f'{contract}: x{contract}')
        t = formula
        for vsym in matches:
            t = re.sub(fr'\b{vsym}\b', f'{vsym}_{contract}', t)
        if 'show' in t.lower():
            t = re.sub(r'\Show 1:', f'ATTR x{contract} = ', t)
        else:
            t = f'ATTR x{contract} = {t}'
        builder.add_let(f'{t}')
    builder.add_when(build_when_clause(start_date))
    return str(builder)


def continuous_convention(clause: str, symbol: str, mx: int) -> str:
    if mx != 1:
        return clause.replace(symbol, f'{symbol}_{mx:02d}')
    return clause


def build_structure_query(
    clause: str, symbols: t.Tuple[str, ...], mx: int, my: int, start_date: t.Optional[date] = None
) -> str:
    cx = clause
    cy = clause
    for match in symbols:
        cx = continuous_convention(cx, match, mx)
        cy = continuous_convention(cy, match, my)

    when = ''
    if start_date is not None:
        when = f' when date is after {start_date:{LIM_DATETIME_FORMAT}}'
    query = f'Show M{mx}-M{my}: ({cx}) - ({cy}){when}'
    return query


def extract_clause(query: str) -> str:
    """
    Given a string like "Show 1: x + y", return "x + y".
    """
    return re.sub(r'Show \w:', '', query)
