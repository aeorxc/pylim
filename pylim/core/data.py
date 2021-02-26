"""
Logic to interact with /rs/api/datarequests resource.
"""
import logging
import time
import typing as t

import pandas as pd
import requests
from lxml import etree

from pylim.core.session import get_lim_session
from pylim.limutils import build_dataframe

calltries = 50
sleep_seconds = 0.5
endpoint_url = '/rs/api/datarequests'


def query(query_text: str) -> pd.DataFrame:
    with get_lim_session() as session:
        response = session.post(
            endpoint_url,
            data=f'<DataRequest><Query><Text>{query_text}</Text></Query></DataRequest>',
        )
        attempt = 1
        while True:
            root = etree.fromstring(response.text.encode('utf-8'))
            status_code = int(root.attrib["status"])
            if status_code == 100:
                return build_dataframe(root[0])
            elif status_code == 130:
                logging.info('No data')
                return pd.DataFrame()
            elif status_code == 200:
                job_id = int(root.attrib['id'])
                logging.info(f'Job {job_id} not complete, starting to poll...')
                # Overwrite the response object for the next loop iteration.
                response = session.get(f'{endpoint_url}/{job_id}')
            else:
                status_message = root.attrib["statusMsg"]
                raise requests.HTTPError(
                    f"{status_code} LIM Client Error: {status_message} for url: {response.url}", response=response
                )
            if attempt >= calltries:
                break
            if attempt > 1:
                time.sleep(sleep_seconds)
            attempt += 1
            
    # Loop completed without returning any result.
    raise requests.exceptions.RetryError('Run out of tries')
