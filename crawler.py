#WebCrawlingURLs5.0_filtering_threadpool2_perf_WORKING.py
import asyncio
import re
import os
import httpx
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import time

# Define parameters
MAX_DEPTH = 2         # Maximum recursion depth for crawling.
N_MULTIPLIER = 2     # Multiplier for ThreadPoolExecutor max_workers.

def clean_domain(domain):
    """Extract the hostname from the domain URL."""
    parsed = urlparse(domain)
    if parsed.netloc:
        return parsed.netloc
    else:
        return domain.strip().rstrip('/')

def extract_urls_from_html(html, base_url, domain):
    """
    Extract absolute URLs from HTML that belong to the same domain.
    Relative URLs are converted to absolute URLs.
    """
    urls = set()
    for link in re.findall(r'href=["\'](.*?)["\']', html):
        full_url = urljoin(base_url, link)
        link_host = clean_domain(httpx.URL(full_url).host or "")
        if link_host == domain:
            urls.add(full_url)
    return urls

def is_recipe_url(url):
    """Return True if the URL appears to be a recipe URL (contains 'recipe')."""
    return re.search(r"recipe", url, re.IGNORECASE) is not None

async def crawl_domain(domain, max_depth=MAX_DEPTH):
    """
    Recursively crawl the given domain up to max_depth using an async HTTP client.

    If the input domain URL already appears recipe-focused (i.e. it contains 'recipe'),
    then use that URL as the base URL (instead of forcing the root domain).
    Otherwise, use the cleaned root domain as base and, for depths ≥ 1,
    only follow links that include 'recipe'.
    """
    recipe_mode = is_recipe_url(domain)
    cleaned = clean_domain(domain)

    # If already recipe-focused, use the provided URL as base (adding scheme if missing)
    if recipe_mode:
        base_url = domain if domain.lower().startswith("http") else f"http://{domain}"
    else:
        base_url = f"http://{cleaned}"

    visited = set()
    collected_urls = set()

    print(f"Starting crawl for domain: {domain} (base: {base_url}, recipe_mode: {recipe_mode})")

    async with httpx.AsyncClient(
        http2=True,
        timeout=10,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; MyCrawler/1.0)"}
    ) as client:

        async def crawl(url, depth):
            if depth > max_depth or url in visited:
                return
            visited.add(url)
            print(f"Crawling URL: {url} at depth {depth}")
            # For non-base pages, check before making a request:
            if depth >= 1 and not is_recipe_url(url):
                print(f"Skipping URL early (non-recipe): {url}")
                return
            try:
                resp = await client.get(url)
                if resp.status_code != 200:
                    print(f"Skipped URL (status {resp.status_code}): {url}")
                    return
                html = resp.text
                new_urls = extract_urls_from_html(html, url, cleaned)
                for new_url in new_urls:
                    if new_url not in visited:
                        collected_urls.add(new_url)
                        await crawl(new_url, depth + 1)
            except Exception as e:
                print(f"Error crawling {url}: {e}")

        await crawl(base_url, 0)
    print(f"Finished crawl for domain: {domain} - collected {len(collected_urls)} URLs")
    return collected_urls

def crawl_domain_sync(domain):
    """Synchronous wrapper to run the asynchronous crawl for a given domain."""
    print(f"Process starting for domain: {domain}")
    urls = asyncio.run(crawl_domain(domain))
    print(f"Process finished for domain: {domain}")
    return urls

def main():
    start_time = time.time()
    input_file = "robot_cache.txt"
    # Determine number of cores and set the thread pool worker count
    num_cores = multiprocessing.cpu_count()
    max_workers = num_cores * N_MULTIPLIER

    # Create output file name based on parameters. We will fill in the URL count later.
    base_output_name = f"all_urls_depth{MAX_DEPTH}_n{N_MULTIPLIER}_cores{num_cores}"

    if not os.path.exists(input_file):
        print(f"{input_file} file not found!")
        return

    domains = []
    with open(input_file, "r") as f:
        for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 2:
                domain = parts[0].strip()
                should_crawl = parts[1].strip().lower() == "true"
                if should_crawl:
                    domains.append(domain)
                    print(f"Domain queued for crawl: {domain}")

    all_urls = set()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(crawl_domain_sync, domains)
        for url_set in results:
            all_urls.update(url_set)

    total_urls = len(all_urls)
    elapsed_time = time.time() - start_time
    # Build output file name with total URLs
    output_file = os.path.join(os.getcwd(), f"{base_output_name}_urls{total_urls}.txt")

    # Write the collected URLs to the output file
    with open(output_file, "w") as f:
        for url in sorted(all_urls):
            f.write(url + "\n")

    # Write a summary file containing the parameters and performance data
    summary_file = os.path.join(os.getcwd(), f"summary_{base_output_name}_urls{total_urls}.txt")
    with open(summary_file, "w") as f:
        f.write("Crawling Performance Summary\n")
        f.write("----------------------------\n")
        f.write(f"Max Depth: {MAX_DEPTH}\n")
        f.write(f"ThreadPool Multiplier (n): {N_MULTIPLIER}\n")
        f.write(f"Number of CPU Cores: {num_cores}\n")
        f.write(f"Total URLs Collected: {total_urls}\n")
        f.write(f"Elapsed Time: {elapsed_time:.2f} seconds\n")

    print(f"Total URLs collected: {total_urls}")
    print(f"Crawling complete. URLs saved to {output_file}")
    print(f"Summary saved to {summary_file}")

if __name__ == "__main__":
    main()
