#RobotCheck_WORKING.py
import os
import asyncio
import aiohttp
import urllib.parse
from multiprocessing import Pool, cpu_count

CACHE_FILENAME = "robot_cache.txt"
WEBSITES_FILENAME = "websites.txt"

def load_cache():
    """Load cached URL results from file into a dictionary."""
    cache = {}
    if os.path.exists(CACHE_FILENAME):
        with open(CACHE_FILENAME, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    # Expecting each line in format: url,True or url,False
                    try:
                        url, result = line.split(",", 1)
                        cache[url.strip()] = result.strip() == "True"
                    except ValueError:
                        continue
    return cache

def save_cache(cache):
    """Write the cache dictionary to file."""
    with open(CACHE_FILENAME, "w", encoding="utf-8") as f:
        for url, allowed in cache.items():
            f.write(f"{url},{allowed}\n")

async def fetch_robot(url, session):
    """
    Given a URL, build the robots.txt URL from the root domain,
    fetch its content, and decide if scraping is allowed.
    If robots.txt is not found (or any error occurs), return True.
    """
    parsed = urllib.parse.urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    robot_url = f"{base}/robots.txt"
    try:
        async with session.get(robot_url, timeout=10) as response:
            if response.status == 200:
                text = await response.text()
                # Simple parsing: for User-agent: * check if there is a blanket "Disallow: /"
                allowed = True
                user_agent_all = False
                for line in text.splitlines():
                    line = line.strip()
                    if line.lower().startswith("user-agent:"):
                        ua = line.split(":", 1)[1].strip()
                        # Check for the wildcard user-agent
                        if ua == "*" or ua.lower() == "all":
                            user_agent_all = True
                    elif user_agent_all and line.lower().startswith("disallow:"):
                        # If the rule disallows all scraping
                        dis = line.split(":", 1)[1].strip()
                        if dis == "/" or dis == "":
                            allowed = False
                            break
                return url, allowed
            else:
                # If robots.txt is not found or another status, default to True.
                return url, True
    except Exception:
        return url, True

async def process_urls_async(urls):
    """Asynchronously check a list of URLs for their robots.txt rules."""
    results = {}
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_robot(url, session) for url in urls]
        responses = await asyncio.gather(*tasks)
        for url, allowed in responses:
            results[url] = allowed
    return results

def check_url_chunk(urls):
    """Worker function to run the asynchronous checks for a chunk of URLs."""
    return asyncio.run(process_urls_async(urls))

def chunkify(lst, n):
    """Split list lst into n nearly equal chunks."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def main():
    # 1. Cache File Check and Creation
    cache = load_cache()
    print(f"Loaded {len(cache)} cached URLs.")

    # 2. URL Processing: read new URLs from websites.txt
    if not os.path.exists(WEBSITES_FILENAME):
        print(f"Error: {WEBSITES_FILENAME} not found.")
        return

    with open(WEBSITES_FILENAME, "r", encoding="utf-8") as f:
        websites = [line.strip() for line in f if line.strip()]

    # Filter out URLs already in cache.
    new_urls = [url for url in websites if url not in cache]
    print(f"Found {len(new_urls)} new URLs to process.")

    if new_urls:
        # Determine number of processes to use.
        num_processes = min(cpu_count(), len(new_urls))
        # Split new_urls into chunks for each process.
        # Calculate chunk size (ensure at least one URL per chunk)
        chunk_size = (len(new_urls) + num_processes - 1) // num_processes
        chunks = list(chunkify(new_urls, chunk_size))

        # Use multiprocessing Pool to check URLs concurrently.
        with Pool(processes=num_processes) as pool:
            results_list = pool.map(check_url_chunk, chunks)

        # Merge results into the main cache dictionary.
        for result in results_list:
            cache.update(result)

        # Write updated cache back to file.
        save_cache(cache)
        print("Cache updated with new URL results.")
    else:
        print("No new URLs to process.")

if __name__ == "__main__":
    main()
