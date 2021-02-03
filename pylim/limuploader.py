from datetime import datetime
from lxml import etree
import lxml.builder
import time
import requests
import logging
import pandas as pd
from pylim import lim


upload_headers = {
    'Content-Type': 'text/xml',
}

default_column = 'TopColumn:Price:Close'


def check_upload_status(job_id):
    url = f'{lim.limServer}/rs/upload/jobreport/{job_id}'
    response = lim.session.get(url)
    try:
        response.raise_for_status()
    except requests.RequestException:
        logging.error(f'Received response: Code: {response.status_code} Msg: {response.text}')
        raise

    root = etree.fromstring(response.text.encode('utf-8'))
    status_el = root.find('status')
    if status_el is not None:
        code, msg = '', ''
        code_el = status_el.find('code')
        if code_el is not None:
            code = code_el.text
        message_el = status_el.find('message')
        if message_el is not None:
            msg = message_el.text
        if code not in ['200', '201', '300', '302']:
            logging.warning(f'job id {job_id}: code:{code} msg: {msg}')
        return code, msg


def build_upload_xml(df, dfmeta):
    """
    Converts a dataframe (column headings being the treepath) into an XML that the uploader takes.
    """
    E = lxml.builder.ElementMaker()
    ROOT = E.ExcelData
    ROWS = E.Rows
    xROW = E.Row
    xCOL = E.Col
    xCOLS = E.Cols

    entries = []
    count = 1
    for irow, row in df.iterrows():
        for col, val in row.iteritems():
            if pd.isna(val):
                continue
            tokens = col.split(';')
            treepath = tokens[0]
            column = default_column if len(tokens) == 1 else tokens[1]
            desc = dfmeta.get('description', '')
            if isinstance(irow, pd.Timestamp):
                irow = irow.date()
            erow = xROW(
                xCOLS(
                    xCOL(treepath, num="1"),
                    xCOL(column, num="2"),
                    xCOL(str((irow - datetime(1899, 12, 30).date()).days), num="3"), # excel dateformat
                    xCOL(str(val), num="4"),
                    xCOL(desc, num="5"),
                ),
                num=str(count)
            )
            count = count + 1
            entries.append(erow)

    irow = ROOT()
    xROWS = ROWS()
    [xROWS.append(x) for x in entries]
    irow.append(xROWS)

    res = lxml.etree.tostring(irow, pretty_print=True)
    return res


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def upload_chunk(df, dfmeta):
    url = f'{lim.limServer}/rs/upload'
    params = {
        'username': lim.limUserName,
        'parsername': 'DefaultParser',
    }
    res = build_upload_xml(df, dfmeta)
    logging.info(f'Uploading chunk to {url}')
    response = lim.session.post(url, data=res, headers=upload_headers, params=params)
    try:
        response.raise_for_status()
    except requests.RequestException:
        logging.error(f'Received response: Code: {response.status_code} Msg: {response.text}')
        logging.error(f'For chunk head: \n{df.head()}')
        logging.error(f'For chunk tail: \n{df.tail()}')
        raise
    root = etree.fromstring(response.text.encode('utf-8'))
    intStatus = root.attrib['intStatus']
    if intStatus == '202':
        job_id = root.attrib['jobID']
        logging.debug(f'Submitted job id: {job_id}')
        for i in range(0, lim.calltries):
            code, msg = check_upload_status(job_id)
            if code in ['200', '201', '300', '302']:
                return msg
            time.sleep(lim.sleep)


def upload_series(df, dfmeta, max_chunk_size: int = 1000):
    if not len(df.columns):
        return
    chunk_size = int(round(len(df) / max_chunk_size, 0)) or 1
    for chunk in chunks(df, chunk_size):
        upload_chunk(chunk, dfmeta)
