import asyncio
import collections
import json
import logging
import os
import random
import time
from itertools import cycle

import aiohttp
import requests
from aiohttp import ClientSession, ClientTimeout, ClientResponseError
from aiohttp_socks import ProxyConnector
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

user_agent_list = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
]

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    'X-Forwarded-For': '127.0.0.1',
    'X-Forwarded-Host': '127.0.0.1',
    'X-Client-IP': '127.0.0.1',
    'X-Remote-IP': '127.0.0.1',
    'X-Remote-Addr': '127.0.0.1',
    'X-Host': '127.0.0.1'
}


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


async def filter_valid_proxy(proxy):
    try:
        connector = ProxyConnector.from_url(proxy)
        async with aiohttp.ClientSession(connector=connector, timeout=ClientTimeout(total=10)) as session:
            async with session.get('http://httpbin.org/ip') as response:
                if response.status == 200:
                    return proxy
    except Exception as e:
        logger.debug(e)
        return None


async def filter_valid_proxies(proxies):
    valid_proxies = []
    try:
        tasks = []
        for proxy in proxies:
            tasks.append(asyncio.create_task(filter_valid_proxy(proxy)))
        for coro in asyncio.as_completed(tasks):
            proxy = await coro
            if proxy is not None:
                valid_proxies.append(proxy)
    except Exception as e:
        logger.debug(e)
        return None
    return valid_proxies


async def extract_words(url, bank_of_words, proxy_pool, num_retries=3):
    try:
        if num_retries == 0:
            logger.error(f"Error for {url}: num_retries is 0")
            return None

        proxy = next(proxy_pool) if proxy_pool else None
        connector = ProxyConnector.from_url(proxy) if proxy else aiohttp.TCPConnector(ssl=False, limit=1000)
        async with ClientSession(headers={**headers, 'User-Agent': random.choice(user_agent_list)},
                                 connector=connector) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                html_content = await response.text()

            soup = BeautifulSoup(html_content, 'html.parser')
            article = soup.find('div', {'class': 'article-text'})
            if article is None:
                logger.debug(f"Error for {url}: article is None: {response.status} {html_content}")
                return None

            article_txt = article.text.split(' ')
            filtered_words = [word for word in article_txt if word.isalpha() and len(word) > 2 and word in bank_of_words]
            return filtered_words
    except ClientResponseError as e:
        logger.debug(f"Error retry={num_retries} for {url}: {e}")
        if e.status == 404:
            return None
        if e.status == 999:
            logger.error(f"Error retry={num_retries} for {url}: {e}")
            await asyncio.sleep(5)
            return {'url': url, 'retry': num_retries - 1}
        else:
            return {'url': url, 'retry': num_retries - 1}
    except Exception as e:
        logger.debug(f"Error retry={num_retries} for {url}: {e}")
        return {'url': url, 'retry': num_retries - 1}


async def extract_all_words(urls, bank_of_words, valid_proxies):
    counter = collections.Counter()
    chunk_size = 70*len(valid_proxies) if valid_proxies else 70
    proxy_pool = cycle(valid_proxies) if valid_proxies else None
    cycle_counter = 1
    for group in chunker(urls, chunk_size):
        start_time = time.time()
        tasks = []
        retries = []
        for url in group:
            tasks.append(asyncio.create_task(extract_words(url, bank_of_words, proxy_pool)))
        for coro in asyncio.as_completed(tasks):
            res = await coro
            if res is not None:
                if isinstance(res, dict):
                    retries.append(res)
                else:
                    counter.update(res)
        while retries:
            tasks = []
            for retry in retries:
                tasks.append(asyncio.create_task(extract_words(retry['url'], bank_of_words, proxy_pool, retry['retry'])))
            retries = []
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res is not None:
                    if isinstance(res, dict):
                        retries.append(res)
                    else:
                        counter.update(res)
        logger.info(f'Cycle number: {cycle_counter}/{int(len(urls) / chunk_size) + 1}, '
                    f'took {time.time() - start_time}, '
                    f'for {len(group)} urls')
        cycle_counter += 1
    return counter


async def main():
    logger.info('Starting scraping...')
    # Get bank of words
    url = "https://raw.githubusercontent.com/dwyl/english-words/master/words.txt"
    response = requests.get(url)
    data = response.text
    lines = data.splitlines()
    bank_of_words = set(line for line in lines if len(line) >= 3 and line.isalpha())

    # Get list of urls to scrape
    absolute_path = os.path.dirname(__file__)
    relative_path = "endg-urls"
    full_path = os.path.join(absolute_path, relative_path)
    with open(full_path, 'r') as file:
        urls = file.read().splitlines()

    # Get list of proxies
    proxies_req = requests.get('https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked'
                               '&sort_type=desc')
    proxies = proxies_req.json()['data']
    proxies = [f"{proxy['protocols'][0]}://{proxy['ip']}:{proxy['port']}" for proxy in proxies]
    valid_proxies = await filter_valid_proxies(proxies)

    counter = await extract_all_words(urls, bank_of_words, valid_proxies)

    json_obj = {}
    for word, count in counter.most_common(10):
        json_obj[word] = count

    logger.info(json.dumps(json_obj, indent=1))

if __name__ == '__main__':
    asyncio.run(main())
