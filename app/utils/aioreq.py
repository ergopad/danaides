import asyncio
from asyncio.log import logger
import json

from typing import Dict, Any, List, Tuple
from aiohttp import ClientSession

# async get content
async def http_get_content_aiohttp(
    session: ClientSession, 
    url: str, 
    headers: Dict = {}, 
    proxy: str = None, 
    timeout: int = 10,
) -> (int, bytes):

    # open http get connection
    try: res = await session.get(url=url, headers=headers, proxy=proxy, timeout=timeout)
    except: pass

    # return content
    response_content = None
    try: response_content = await res.read()
    except: pass

    return res.status, response_content

# async get json
async def http_get_json_aiohttp(
    session: ClientSession, 
    url: str, 
    headers: Dict = {}, 
    proxy: str = None, 
    timeout: int = 10,
) -> (int, Dict[str, Any]):

    # open http get connection
    try: res = await session.get(url=url, headers=headers, proxy=proxy, timeout=timeout)
    except: pass

    # return json
    response_json = None
    try: response_json = await res.json(content_type=None)
    except json.decoder.JSONDecodeError as e: pass

    return res.status, response_json

# async get json
async def http_get_json_ordered_aiohttp(
    session: ClientSession, 
    ordered_url: List, 
    headers: Dict = {}, 
    proxy: str = None, 
    timeout: int = 10,
) -> (int, int, Dict[str, Any]):

    # open http get connection
    try: 
        sort_order, url = ordered_url
        logger.debug(sort_order)
        res = await session.get(url=url, headers=headers, proxy=proxy, timeout=timeout)
        response_json = await res.json(content_type=None)
        return res.status, sort_order, response_json

    except json.decoder.JSONDecodeError as e: 
        raise ValueError(f'http_get_json_ordered_aiohttp; json decoder: {e}')
        pass

    except Exception as e:
        raise ValueError(f'http_get_json_ordered_aiohttp; {e}')

# parallelize http get
async def get_json(
    urls: List[str], 
    headers: Dict = {}, 
    proxy: str = None, 
    timeout: int = 10
) -> (List[Tuple[int, Dict[str, Any]]], float):
    session = ClientSession()
    res = await asyncio.gather(*[http_get_json_aiohttp(session, url, headers, proxy, timeout) for url in urls])
    await session.close()
    return res

async def get_json_ordered(
    # session: ClientSession, 
    urls: List[Tuple[int, str]], 
    headers: Dict = {}, 
    proxy: str = None, 
    timeout: int = 10
) -> (List[Tuple[int, int, Dict[str, Any]]], float):
    try:
        session = ClientSession()
        res = await asyncio.gather(*[http_get_json_ordered_aiohttp(session, url, headers, proxy, timeout) for url in urls])
        await session.close()
        return res
    except Exception as e:
        raise ValueError(f'get_json_ordered; {e}')
    finally:
        await session.close()
