import logging
from os import getenv
from urllib.parse import urljoin
from urllib.request import getproxies

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class BaseUrlSession(requests.Session):
    """
    A Session with a URL that all requests will use as a base.

    .. note::
        Trailing slashes provided in the base URL and the path are important,
        because this class will perform the join using the `urljoin` method. 
    
    Example #1: Calling GET method without a trailing slash in URL.
    .. code-block:: python
        >>> s = BaseUrlSession('https://example.com/resource/')
        >>> r = s.get('sub-resource/' params={'foo': 'bar'})
        >>> print(r.request.url)
        https://example.com/resource/sub-resource/?foo=bar

    Example #2: Calling GET method with a trailing slash in URL.
    .. code-block:: python
        >>> s = sessions.BaseUrlSession('https://example.com/resource/')
        >>> r = s.get('/sub-resource/' params={'foo': 'bar'})
        >>> print(r.request.url)
        https://example.com/sub-resource/?foo=bar
    """

    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url

    def request(self, method: str, url: str, *args, **kwargs):
        """Send the request after generating the complete URL."""
        url = self.create_url(url)
        return super().request(method, url, *args, **kwargs)

    def create_url(self, url: str) -> str:
        """Create the URL based off this partial path."""
        return urljoin(self.base_url, url)


def raise_for_status_and_log(response: requests.Response, *args, **kwargs):
    try:
        response.raise_for_status()
    except requests.RequestException:
        logger.error(f'Response error: Code: {response.status_code} Msg: {response.text}')
        raise


def get_lim_session() -> requests.Session:
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
    session = BaseUrlSession(
        getenv("LIMSERVER", "https://rwe.morningstarcommodity.com"),
    )
    session.auth = getenv("LIMUSERNAME", ""), getenv("LIMPASSWORD", "")
    session.headers = {"Content-Type": "application/xml"}
    session.proxies = getproxies()
    session.mount("http://", retry_adapter)
    session.mount("https://", retry_adapter)
    session.hooks["response"] = raise_for_status_and_log
    return session
