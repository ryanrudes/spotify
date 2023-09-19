from disk import diskset, diskqueue, pagequeue
from aiohttp.client import ClientSession
from collections import defaultdict
from tqdm import tqdm

import argparse
import requests
import aiohttp
import asyncio
import time
import re
import os

domain = "https://open.spotify.com/"

page_expr = re.compile(r'https://open\.spotify\.com/(?!(?:intl|oembed)\b)([^\s"\']+)^\?')
slug_expr = re.compile(r'/(track|album|artist|playlist|concert|episode|show|genre)/([0-9a-zA-Z]{22})')
user_expr = re.compile(r'/user/([0-9a-z]{28})')

status_messages = {429: "Too many requests", 500: "Internal Server Error", 503: "Service unavailable", 504: "Gateway timeout"}

def parse_args():
    parser = argparse.ArgumentParser(description = "Scrape Spotify")
    parser.add_argument("--root", default = "run", help = "Root directory to store data")
    args = parser.parse_args()
    return args

def process(url, html, code):
    try:
        if code == 200:
            pages = set(re.findall(page_expr, html))
            results = defaultdict(set)

            for slug, id in re.findall(slug_expr, html):
                results[slug].add(id)

            for id in re.findall(user_expr, html):
                results["user"].add(id)

            for slug, identifiers in results.items():
                for id in identifiers:
                    page = slug + '/' + id
                    pages.add(page)

            return pages, results
        elif code in status_messages.keys():
            message = status_messages[code]
            print(f"{message} when accessing {url}. Retrying in 0.1 seconds...")
            time.sleep(0.1)
        else:
            print(f"Failed to access {url}. Status code: {code}")
            return set(), dict()
    except KeyboardInterrupt:
        raise
    except Exception as e:
        print(f"Error while scraping {url}: {str(e)}")
        return set(), dict()
    
async def request(page, session):
    url = domain + page
    async with session.get(url) as response:
        html = await response.text()
        return process(url, html, response.status)
    
async def scrape(pages):
    connection = aiohttp.TCPConnector(limit = 64)

    async with ClientSession(connector = connection) as session:
        tasks = []

        for page in pages:
            task = asyncio.ensure_future(request(page, session))
            tasks.append(task)

        pbar = tqdm(total = len(pages), desc = "Requesting URLs", unit = " tracks", leave = False)
        outputs = []

        for output in asyncio.as_completed(tasks):
            try:
                value = await output

                if value is not None:
                    outputs.append(value)
            except Exception as e:
                raise e
                continue
            finally:
                pbar.update(1)

    return outputs

if __name__ == "__main__":
    args = parse_args()

    os.makedirs(f"{args.root}/queues/pages", exist_ok = True)
    
    page_input_queue = pagequeue(f"{args.root}/queues/pages/input.db")
    page_output_queue = pagequeue(f"{args.root}/queues/pages/output.db")
    result_queue = diskqueue(f"{args.root}/queues/results.db")

    visited = diskset(f"{args.root}/pages.db")
    results = {key: diskset(f"{args.root}/{key}.db") for key in diskqueue.SLUG_MAPPING.keys()}

    if len(page_input_queue) == 0:
        page_input_queue.put("track/6fxbtIuYVYl37ynRqEfMcc")

    last_time = time.time()
    round = 0
    
    batch_size = 4096

    while True:
        length = len(page_input_queue)
        for _ in tqdm(range(length // batch_size + 1)):
            pages = page_input_queue.get_batch(min(batch_size, len(page_input_queue)))

            all_pages = []
            all_results = []

            for pages, outputs in asyncio.run(scrape(pages)):
                all_pages.extend(pages)

                for slug, identifiers in outputs.items():
                    for id in identifiers:
                        all_results.append((slug, id))

            page_output_queue.extend(all_pages)
            result_queue.extend(all_results)

        length = len(page_output_queue)
        for _ in tqdm(range(length // batch_size + 1)):
            urls = page_output_queue.get_batch(min(batch_size, len(page_output_queue)))
            new_urls = []
            for url in urls:
                if url not in visited:
                    new_urls.append(url)
            visited.extend(new_urls)
            page_input_queue.extend(new_urls)

        length = len(result_queue)
        for _ in tqdm(range(length // batch_size + 1)):
            slugs, hashes = result_queue.get_batch(min(batch_size, len(result_queue)))
            unique_items = defaultdict(set)
            for slug, hash in zip(slugs, hashes):
                if hash not in results[slug]:
                    unique_items[slug].add(hash)
            for slug, identifiers in unique_items.items():
                results[slug].extend(identifiers)

        last_time = time.time()
        message = ", ".join(key + ": " + str(len(results[key])) for key in diskqueue.SLUG_MAPPING.keys())
        print(f"(Round #{round})", message)
        print(len(page_input_queue), len(page_output_queue), len(result_queue))
        round += 1