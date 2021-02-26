"""
Low level module for building and executing queries to MorningStar LIM API.

This module handles:
- Connecting and authentication to the LIM server,
- XML request/response schema specific to each LIM endpoint,
- Status codes returned by the API,
- Polling for results in case of long running jobs,
- Retry and exception handling logic.

To use this module, the clients have to make sure that following environment variables are set:
- LIMSERVER - URL to the MorningStar API, defaults to https://rwe.morningstarcommodity.com,
- LIMUSERNAME - login to the MorningStar account,
- LIMPASSWORD - password for the MorningStar account.
"""
from .data import query
from .session import get_lim_session
