from tqdm import tqdm
import socket
from sys import exit
from typing import Any, Dict, List, Tuple, Union
import requests
from io import BytesIO
from urllib.parse import urlparse, parse_qs, quote_plus
from urllib3.util.retry import Retry
from lxml.etree import HTML, XML
import pandas as pd
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from requests.exceptions import HTTPError, Timeout, ConnectionError, ChunkedEncodingError, ProxyError
from rich.console import Console
from urllib.parse import urlencode as _urlencode
import json
from requests.structures import CaseInsensitiveDict


console = Console(log_path=False, record=True)


def check_alive(ip: str, port: int, timeout: int = 1):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    result = sock.connect_ex((ip, port))
    return result == 0


def ensure_alive(ip: str, port: int, timeout: int = 1, name: str = None):
    name = name if name else f'{ip}:{port}'
    if check_alive(ip, port, timeout):
        print(f'"{name}" responded successfully!')
    else:
        print(f'FATAL: "{name}" not reachable!')
        exit(1)


def quote_as_is(*args):
    return args[0]


def urlencode(url: str, query: dict):
    char = '&' if '?' in url else '?'
    return f'{url}{char}' + _urlencode(query, quote_via=quote_as_is)


def bar_download(url: str, filename: str = None, session: Union[None, requests.Session] = None, bar_prefix: str = None, proxies=None):
    def _resp_body_len(headers: CaseInsensitiveDict):
        stream = headers.get('transfer-encoding') == 'chunked'
        content_length = int(headers.get('content-length', 0))
        if content_length:
            return content_length
        elif stream:
            print('Unknown response length ("Transfer-Encoding: chunked" detected)')
            return 0
        else:
            print('Warning: No "Transfer-Encoding" or "Content-Length" in headers!')
            return 0

    # Why all these warnings? See this post:
    # https://blog.piaoruiqing.com/2019/09/08/do-you-know-content-length/


    # TODO: is it good practice to use toolbelt?
    # https://toolbelt.readthedocs.io/en/latest/downloadutils.html
    # requests_toolbelt.downloadutils.tee.tee

    _requests = session if session else requests
    r = _requests.get(url, stream=True, proxies=proxies)

    total_size = _resp_body_len(r.headers)
    block_size = 1024
    with BytesIO() as handler:
        with tqdm(total=total_size, unit='iB', unit_scale=True, desc=bar_prefix) as bar:
            for data in r.iter_content(block_size):
                bar.update(len(data))
                handler.write(data)
            if total_size != 0:
                if bar.n < total_size:
                    print("ERROR: Real data length < Content-Length")
                elif bar.n > total_size:
                    print("FATAL: Real data length > Content-Length")
                else:
                    # print("Real data length == Content-Length")
                    pass
        content = handler.getvalue()
    if filename:
        with open(filename, 'wb') as f:
            f.write(content)
    return content


class API:
    # TODO: store params as dict, stringfy when export
    # TODO: predefined slots
    def __init__(self, url, quote=False):
        self.url = url
        self.quote = quote

    def _encode(self, raw):
        if not isinstance(raw, str) and not isinstance(raw, int):
            console.log(f'[red]Warning: force convert to str "{raw}"')
        raw = str(raw)
        return quote_plus(raw) if self.quote else raw

    def append_param(self, key, value):
        if not value:
            return self
        if callable(value):
            key, value = value(self, key)
        url = self.url
        if '?' not in self.url:
            url += '?'
        elif not self.url.endswith('&'):
            url += '&'
        return API(url + f'{key}={self._encode(value)}')

    def replace_param(self, key, value):
        return API(self.url.replace(f'<{key}>', self._encode(value)))

    def group_params(self, item_dict, prefix=None, suffix=None):
        assert prefix or suffix
        prefix = prefix or ''
        suffix = suffix or ''
        return self.add_params({
            f'{prefix}{k}{suffix}': v
            for k, v in item_dict.items()
        })

    def add_param(self, key, value):
        if f'<{key}>' in self.url:
            return self.replace_param(key, value)
        return self.append_param(key, value)

    def add_params(self, params_dict):
        api = self
        for k, v in params_dict.items():
            api = api.append_param(k, v)
        return api

    def __getattr__(self, key):
        return lambda x: self.add_param(key, x)

    def __repr__(self):
        return self.url


def get_dom(url, mode='html', bar=True):
    parser = HTML if mode.lower() == 'html' else XML
    if bar:
        content = bar_download(url)
    else:
        content = requests.get(url).content
    return parser(content)


def get_json(url: str) -> Any:
    content = bar_download(url)
    return json.loads(content.decode('utf-8'))


def get_query_dict(url: str) -> Dict[str, List[str]]:
    return parse_qs(urlparse(url).query)


def get_csv(url: str, comment_prefix: str = None, **kwargs) -> Tuple[pd.DataFrame, Union[bytes, None]]:
    """Remove comments in csv-like files.
    But unlike pandas.read_csv, this function only removes comments at the beginning of the file.

    Args:
        url (str): Source URL.
        comment_prefix (str, optional): Line start character of a comment line. Defaults to None.

    Returns:
        Tuple[pd.DataFrame, Union[bytes, None]]: (DataFrame, comments in bytes)
    """
    content = bar_download(url)

    if comment_prefix:
        # remove comment
        comments = []
        comment_prefix_bytes = comment_prefix.encode('utf-8')
        lines = content.split(b'\n')
        clean_idx = 0
        while lines[clean_idx].startswith(comment_prefix_bytes):
            clean_idx += 1
            comments.append(lines[clean_idx])
        clean_content = b'\n'.join(lines[clean_idx:])
        comments = b'\n'.join(comments[:-1])
    else:
        clean_content = content
        comments = None

    df = pd.read_csv(BytesIO(clean_content), **kwargs)
    return df, comments  # type: ignore


SINGLE_REQUEST_MAX_WAIT = 20
RETRY_BACKOFF_FACTOR = 0.02


def parallel_requests(
    urls: Union[Dict[str, str], List[str]], method: str = 'GET', workers: int = 32,
    retries: int = 5, post_data: List = None, leave=True,
    progress_scale: int = 1, total: Union[None, int] = None, **kwargs
) -> List[requests.Response]:

    from math import log2

    # TODO: `assert_success` to retry on failed tasks
    # TODO: `yeild`
    # TODO: retry time upper limit, retry stuck warning

    results = {}
    # urls_type = 'dict' if isinstance(urls, dict) else 'list'
    if isinstance(urls, list):
        urls = {str(k): v for k, v in enumerate(urls)}
    assert isinstance(urls, dict)
    if post_data:
        method = 'POST'
    else:
        post_data = [None] * len(urls)

    bar = tqdm(total=total or len(urls)*progress_scale, leave=leave)

    max_retry = int(log2(SINGLE_REQUEST_MAX_WAIT / RETRY_BACKOFF_FACTOR))
    if retries > max_retry:
        console.log(
            f'Retry {retries} times will take up to more than {SINGLE_REQUEST_MAX_WAIT} seconds,')
        console.log(
            f'auto adjusted to {max_retry} times.')

    max_retries = Retry(
        total=min(retries, max_retry),
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=[403, 500, 501, 502, 503, 504, 505]
    )

    def _exception_cb(url_id):
        def _cb(future):
            if future._exception:
                console.log(
                    f'[red]Warning: failed after {retries} attempts: "{url_id}"')
        return _cb

    with FuturesSession(
        max_workers=workers,
        adapter_kwargs={'max_retries': max_retries}
    ) as session:
        futures = []
        for (id_, url), data in zip(urls.items(), post_data):
            future = session.request(
                method, url, headers={'X-META-ID': str(id_)}, data=data, **kwargs
            )
            # print(f'DEBUG: Starting {id_}: "{url}"')
            future.add_done_callback(_exception_cb(str(id_)))
            futures.append(future)
        for future in as_completed(futures):  # type: ignore
            try:
                resp = future.result()
            except (HTTPError, Timeout, ConnectionError, ChunkedEncodingError, ProxyError):
                pass
            else:
                id_ = resp.request.headers['X-META-ID']
                results[id_] = resp
                bar.update(min(1*progress_scale, bar.total-bar.n))

    # unfetched_keys = set(urls.keys()) - set(results.keys())
    # if unfetched_keys:
    #     console.log(f'Failed requests IDs:')
    #     console_list(unfetched_keys)

    # if urls_type == 'list':
    #     # Sort by original id
    #     console.log('Sorting results...')
    #     sorted_results = [results[key] for key in urls.keys() if key in results]
    #     return sorted_results
    # else:
    #     return results
    # Sort by original id
    sorted_results = []
    failed_requsts = []
    for key in urls.keys():
        if key in results:
            sorted_results.append(results[key])
        else:
            failed_requsts.append(key)
            sorted_results.append(None)
    if failed_requsts:
        console.log(f'Failed requests IDs: {", ".join(failed_requsts)}')
    return sorted_results


if __name__ == "__main__":
    api = API('http://www.baidu.com') \
        .k1('v1') \
        .group_params({
            'k2': 'v2',
            'k3': 'v3'
        }, prefix='p.')
    print(api)
