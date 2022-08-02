import asyncio
# from asyncio.log import logger
from utils.logger import logger
import json

from typing import Dict, Any, List, Tuple
from aiohttp import ClientSession

TIMEOUT = 5
VERBOSE = False
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',"Content-Type": "application/json"}

# async get content
async def http_get_content_aiohttp(
    session: ClientSession, 
    url: str, 
    headers: Dict = HEADERS, 
    proxy: str = None, 
    timeout: int = TIMEOUT,
) -> (int, bytes):

    # open http get connection
    try: res = await session.get(url=url, headers=headers, proxy=proxy, timeout=timeout)
    except Exception as e: 
        logger.warning(f'http_get_content_aiohttp.session.get: {e}')
        pass

    # return content
    response_content = None
    try: response_content = await res.read()
    except Exception as e: 
        logger.warning(f'http_get_content_aiohttp.res.read: {e}')
        pass

    return res.status, response_content

# async get json
async def http_get_json_aiohttp(
    session: ClientSession, 
    url: str, 
    headers: Dict = HEADERS, 
    proxy: str = None, 
    timeout: int = TIMEOUT,
) -> (int, Dict[str, Any]):

    # open http get connection
    try: res = await session.get(url=url, headers=headers, proxy=proxy, timeout=timeout)
    except Exception as e: 
        logger.warning(f'http_get_json_aiohttp.session.get: {e}')
        pass

    # return json
    response_json = None
    try: response_json = await res.json(content_type=None)
    except json.decoder.JSONDecodeError as e: 
        logger.warning(f'http_get_content_aiohttp.res.json: {e}')
        pass

    return res.status, response_json

# async get json
async def http_get_json_ordered_aiohttp(
    session: ClientSession, 
    ordered_url: List, 
    headers: Dict = HEADERS, 
    proxy: str = None, 
    timeout: int = TIMEOUT,
) -> (int, int, Dict[str, Any]):

    # open http get connection
    try: 
        sort_order, url = ordered_url
        res = await session.get(url=url, headers=headers, proxy=proxy, timeout=timeout)
        response_json = await res.json(content_type=None)
        if VERBOSE and (res.status != 200):
            logger.debug(f'{sort_order}: url {url}')
            logger.debug(f'session.get: status {res.status}; {response_json} (proxy {proxy}, timeout {timeout}, headers {headers}')
        return res.status, sort_order, response_json

    except json.decoder.JSONDecodeError as e: 
        logger.warning(f'http_get_json_ordered_aiohttp; json decoder: {e}')
        raise ValueError(f'http_get_json_ordered_aiohttp; json decoder: {e}')
        pass

    except Exception as e:
        logger.warning(f'http_get_json_ordered_aiohttp.main: {e}')
        raise ValueError(f'http_get_json_ordered_aiohttp; {e}')

# parallelize http get
async def get_json(
    urls: List[str], 
    headers: Dict = HEADERS, 
    proxy: str = None, 
    timeout: int = TIMEOUT
) -> (List[Tuple[int, Dict[str, Any]]], float):
    try:
        session = ClientSession()
        res = await asyncio.gather(*[http_get_json_aiohttp(session, url, headers, proxy, timeout) for url in urls])
        await session.close()
        return res
    except Exception as e: 
        logger.warning(f'get_json.session.get: {e}')
        pass

async def get_json_ordered(
    # session: ClientSession, 
    urls: List[Tuple[int, str]], 
    headers: Dict = HEADERS, 
    proxy: str = None, 
    timeout: int = TIMEOUT
) -> (List[Tuple[int, int, Dict[str, Any]]], float):
    try:
        session = ClientSession()
        res = await asyncio.gather(*[http_get_json_ordered_aiohttp(session, url, headers, proxy, timeout) for url in urls])
        await session.close()
        return res
    except Exception as e:
        logger.warning(f'get_json_ordered.gather: {e}')
        raise ValueError(f'get_json_ordered; {e}')
    finally:
        await session.close()
