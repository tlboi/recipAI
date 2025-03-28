#fetcher4.21.py
import asyncio
import aiohttp
import aiofiles
import os
import time
import re
import logging
from urllib.parse import urlparse, urljoin
from collections import defaultdict, Counter # Import Counter
from tqdm import tqdm  # Correct tqdm import (non-async)
from bs4 import BeautifulSoup
import random
import mimetypes
import multiprocessing

# ---------------- Configuration Variables ----------------
URL_FILE = "5000_random_urls.txt"
MAX_CONCURRENT_REQUESTS = 60  # Increased initial concurrency
DOMAIN_REQUEST_LIMIT = 4  # Increased domain limit (be careful!)
MAX_REDIRECTS = 2  # Maximum number of redirects to follow
KEYWORD_FILTER = r"(recipe|ingredient|cooking|bake|dish|food|cuisine|eat)"
LOG_LEVEL = logging.INFO

# ---------------- Script Version ----------------
script_version = "4.21" # Updated version

# ---------------- Generate Configuration String ----------------
# Use os.path.basename to handle potential paths in URL_FILE
config_string = f"MCR{MAX_CONCURRENT_REQUESTS}_DRL{DOMAIN_REQUEST_LIMIT}_MR{MAX_REDIRECTS}_{os.path.basename(URL_FILE)}"

# ---------------- Dynamic File and Folder Names ----------------
base_name = f"v{script_version}_{config_string}"  # Base name for files and folders

OUTPUT_DIR = f"html_files_{base_name}"
RESUME_FILE = f"download_status_{base_name}.txt" # Note: Resume file might not be perfectly accurate for failed/skipped in previous runs if errors change
ERROR_LOG_FILE = f"error_log_{base_name}.txt"
NON_HTML_SKIPPED_LOG_FILE = f"non_html_skipped_{base_name}.txt"
output_filename = f"summary_{base_name}.txt"  # Filename includes version and parameters

# ---------------- Niche User-Agent List ----------------
NICHE_USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux i686; U; en) Opera 9.80 (Linux i686; U; en) Presto/2.2.15 Version/10.10",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:45.0) Gecko/20100101 Firefox/45.0",
    "Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14",
    "Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko)",
    "Mozilla/5.0 (X11; U; FreeBSD; en-US; rv:1.8.1.6) Gecko/20070802 SeaMonkey/1.1.4"
]


def get_random_headers():
    """Return a headers dictionary with a random niche User-Agent."""
    return {
        "User-Agent": random.choice(NICHE_USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/"
    }


# ---------------- Custom Logging Handler using tqdm ----------------
class TqdmLoggingHandler(logging.Handler):

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
        except Exception:
            self.handleError(record)


# ---------------- Setup Logging ----------------
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
handler = TqdmLoggingHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.handlers = [handler] # Clears existing handlers and adds only the tqdm one

# Create output directories if they don't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


async def download_url(session, url, domain_semaphore, error_messages, non_html_skipped_urls, redirect_count=0):
    """
    Downloads a single URL and saves the relevant content.
    Populates error_messages and non_html_skipped_urls dictionaries on failure/skip.
    Returns True on completion (success, failure, or skip), False if unexpected error occurred before semaphore release.
    """
    try:
        async with domain_semaphore:
            headers = get_random_headers()  # Add random user agent
            try:
                async with session.get(url, timeout=30, headers=headers, allow_redirects=False) as response:
                    if response.status == 200:

                        # Check Content-Type
                        content_type = response.headers.get("Content-Type", "").lower()
                        if "text/html" in content_type:
                            try:
                                html = await response.text()  # Try decoding it as text first
                            except UnicodeDecodeError as e:
                                logging.warning(f"UnicodeDecodeError for {url}: {e}. Skipping.")
                                error_messages[url] = "unicode_error"
                                return True # Indicate completion (failure)

                            # Keyword filtering
                            if KEYWORD_FILTER and not re.search(KEYWORD_FILTER, html, re.IGNORECASE):
                                logging.info(f"Skipping {url}: No relevant keywords found.")
                                error_messages[url] = "skipped_keyword" # Specific reason for skipping
                                return True # Indicate completion (skipped)

                            # Extract body content using BeautifulSoup
                            soup = BeautifulSoup(html, 'html.parser')
                            body = soup.find('body')
                            if body:
                                # Get inner content of body, remove script/style
                                for tag in body.find_all(['script', 'style']):
                                    tag.decompose()
                                content_to_save = body.prettify() # Prettify preserves structure better
                            else:
                                # If no body tag, save the whole HTML after removing script/style
                                soup = BeautifulSoup(html, 'html.parser')
                                for tag in soup.find_all(['script', 'style']):
                                     tag.decompose()
                                content_to_save = soup.prettify()
                                logging.warning(f"No <body> tag found for {url}. Saving modified full HTML.")


                            # Sanitize the filename from URL and save
                            # Use more robust sanitization
                            domain = urlparse(url).netloc or "unknown_domain"
                            path = (urlparse(url).path or "").strip('/')
                            query = (urlparse(url).query or "")
                            filename_base = f"{domain}_{path}_{query}"
                            filename = re.sub(r'[^\w\-.]', '_', filename_base) # Allow alphanumeric, underscore, hyphen, dot
                            filename = re.sub(r'_+', '_', filename).strip('_') # Collapse multiple underscores
                            if len(filename) > 200: # Limit filename length
                                filename = filename[:200]
                            filename += ".html"
                            filepath = os.path.join(OUTPUT_DIR, filename)

                            async with aiofiles.open(filepath, "w", encoding="utf-8") as f:
                                await f.write(content_to_save)
                            logging.info(f"Downloaded and saved: {url}")
                            # Success: Don't add to error_messages
                            return True # Indicate completion (success)
                        else:
                            # Skip the response, add error to the file
                            logging.warning(f"Skipping non-HTML content from {url} (Content-Type: {content_type})")
                            error_key = f"skipped_non_html_{content_type.split(';')[0].replace('/','_')}" # More specific key
                            error_messages[url] = error_key
                            non_html_skipped_urls[url] = content_type
                            return True # Indicate completion (skipped)

                    elif response.status in [404, 403, 401, 406, 451, 500, 521, 202]:  # Handle specific status codes
                        error_key = f"status_code_{response.status}"
                        logging.warning(f"Error downloading {url}: {error_key}")
                        error_messages[url] = error_key
                        return True # Indicate completion (failure)

                    elif 300 <= response.status < 400:  # Handling redirects
                        if redirect_count < MAX_REDIRECTS:
                            redirect_url = response.headers.get('Location')
                            if redirect_url:
                                # Handle relative redirects
                                try:
                                    redirect_url = urljoin(url, redirect_url)
                                except ValueError:
                                    logging.warning(f"Invalid redirect URL '{redirect_url}' from {url}.")
                                    error_messages[url] = "invalid_redirect_url"
                                    return True # Indicate completion (failure)

                                logging.info(
                                    f"Redirecting from {url} to {redirect_url} (Attempt {redirect_count + 1})")
                                # Crucially, the recursive call *replaces* the current task result
                                return await download_url(session, redirect_url, domain_semaphore, error_messages, non_html_skipped_urls, redirect_count + 1)
                            else:
                                logging.warning(f"Redirect from {url} with no Location header.")
                                error_messages[url] = "redirect_no_location"
                                return True # Indicate completion (failure)
                        else:
                            logging.warning(
                                f"Too many redirects for {url}. Stopping after {MAX_REDIRECTS} attempts.")
                            error_messages[url] = f"max_redirects_{MAX_REDIRECTS}"
                            return True # Indicate completion (failure)
                    else:
                        # Generic status code error
                        error_key = f"status_code_{response.status}"
                        logging.error(f"Error downloading {url}: {error_key}")
                        error_messages[url] = error_key
                        return True # Indicate completion (failure)

            except aiohttp.ClientConnectorCertificateError as e:
                 logging.error(f"SSL Certificate error downloading {url}: {e}")
                 error_messages[url] = "ssl_certificate_error"
                 return True
            except aiohttp.ClientConnectorError as e:
                 logging.error(f"Connection error downloading {url}: {e}")
                 error_messages[url] = "connection_error"
                 return True
            except asyncio.TimeoutError:
                logging.error(f"Timeout error downloading {url}")
                error_messages[url] = "timeout_error"
                return True
            except aiohttp.ClientError as e:
                logging.error(f"Client error downloading {url}: {type(e).__name__} - {e}")
                error_messages[url] = f"client_error_{type(e).__name__}" # More specific client error
                return True # Indicate completion (failure)
            except Exception as e:
                logging.exception(f"Unexpected error downloading {url}: {e}") # Log full traceback
                error_messages[url] = f"unexpected_error_{type(e).__name__}"
                return True # Indicate completion (failure)

    except asyncio.CancelledError:
        logging.warning(f"Task for {url} cancelled.")
        # Don't add to error messages here, main loop handles cancellation if needed
        return False # Indicate cancellation/early exit
    except Exception as e:
        # This catches errors *outside* the session.get/domain_semaphore block, unlikely but possible
        logging.exception(f"Critical unexpected error in download_url wrapper for {url}: {e}")
        # Try to record the error if possible
        try:
             error_messages[url] = f"critical_wrapper_error_{type(e).__name__}"
        except Exception as e2:
             logging.error(f"Failed to record critical error for {url}: {e2}")
        return False # Indicate major failure


async def worker(queue, session, domain_semaphores, error_messages, non_html_skipped_urls, pbar):
    """Worker to process URLs from the queue."""
    while True:
        url = await queue.get()
        if url is None:
            queue.task_done() # Mark the None item as done
            break  # Signal to stop

        domain = urlparse(url).netloc
        if not domain:
            logging.warning(f"Skipping invalid URL (no domain): {url}")
            error_messages[url] = "invalid_url_no_domain"
            pbar.update(1) # Update progress even for invalid URLs taken from queue
            queue.task_done()
            continue

        # Ensure semaphore exists for the domain
        if domain not in domain_semaphores:
             domain_semaphores[domain] = asyncio.Semaphore(DOMAIN_REQUEST_LIMIT)

        task_completed = False
        try:
            # Pass the shared dictionaries directly
            task_completed = await download_url(session, url, domain_semaphores[domain],
                                                error_messages, non_html_skipped_urls)
        except Exception as e:
            # Catch unexpected errors during the download_url call itself
            logging.exception(f"Error occurred calling download_url for {url}: {e}")
            try:
                 # Attempt to record the error
                 error_messages[url] = f"worker_call_error_{type(e).__name__}"
            except Exception as e2:
                 logging.error(f"Failed to record worker call error for {url}: {e2}")
            task_completed = True # Treat as a completed task (failure) for progress bar

        finally:
             if task_completed: # Only update pbar if download_url ran to completion (success, fail, skip)
                 pbar.update(1)
             queue.task_done()


def load_urls_from_file(filename):
    """Loads URLs from a text file, one URL per line."""
    try:
        with open(filename, "r", encoding='utf-8') as f: # Specify encoding
            return [line.strip() for line in f if line.strip() and not line.startswith('#')] # Skip comments
    except FileNotFoundError:
        logging.error(f"Error: URL file '{filename}' not found.")
        return []
    except Exception as e:
        logging.error(f"Error reading URL file '{filename}': {e}")
        return []


def load_previously_completed_urls():
    """Loads the set of URLs corresponding to *existing* HTML files and resume file."""
    completed_urls = set()

    # 1. Load from resume file (URLs attempted in previous runs, regardless of outcome then)
    # This helps avoid retrying URLs that consistently failed or were skipped before.
    if os.path.exists(RESUME_FILE):
        try:
            with open(RESUME_FILE, "r", encoding='utf-8') as f:
                for line in f:
                    completed_urls.add(line.strip())
            logging.info(f"Loaded {len(completed_urls)} URLs from resume file {RESUME_FILE}")
        except Exception as e:
            logging.error(f"Error loading resume file {RESUME_FILE}: {e}")

    # 2. Load from existing HTML files (URLs successfully downloaded previously)
    # This is a fallback/complement, assuming filenames correctly map back to URLs (which is tricky)
    # Note: This filename-to-URL reconstruction is brittle. The RESUME_FILE is more reliable.
    # We keep it for now but prioritize the resume file.
    initial_count = len(completed_urls)
    try:
        if os.path.exists(OUTPUT_DIR):
            for filename in os.listdir(OUTPUT_DIR):
                if filename.endswith(".html"):
                    # Attempt to reconstruct URL - This is highly unreliable!
                    # Let's skip this unreliable reconstruction for now.
                    # Relying solely on the RESUME_FILE is cleaner.
                    pass
            # if len(completed_urls) > initial_count:
            #      logging.info(f"Added {len(completed_urls) - initial_count} URLs based on existing files in {OUTPUT_DIR} (use with caution).")

    except Exception as e:
        logging.error(f"Error listing files in {OUTPUT_DIR}: {e}")

    return completed_urls


def save_processed_urls(processed_urls):
    """Saves the set of URLs processed in this run to the resume file."""
    try:
        # Append mode might be better if running concurrently or resuming often
        # But 'w' ensures it reflects only the *latest* full run's attempts
        with open(RESUME_FILE, "w", encoding='utf-8') as f:
            for url in sorted(list(processed_urls)): # Sort for consistency
                f.write(url + "\n")
        logging.info(f"Saved {len(processed_urls)} processed URLs to {RESUME_FILE}")
    except Exception as e:
        logging.error(f"Error saving processed URLs to {RESUME_FILE}: {e}")


async def main():
    """Main function to orchestrate the download process."""
    start_time = time.time()
    urls = load_urls_from_file(URL_FILE)
    total_urls_in_file = len(urls)

    if not urls:
        logging.info("No URLs to download. Exiting.")
        return

    logging.info(f"Loaded {total_urls_in_file} URLs from {URL_FILE}")

    # Use Manager dicts for sharing state between potential future processes (though currently using threads via asyncio)
    # Works okay with asyncio tasks as well.
    manager = multiprocessing.Manager()
    error_messages = manager.dict()  # Stores URL -> error/skip reason for this run
    non_html_skipped_urls = manager.dict()  # Stores URL -> content_type for non-HTML skips this run

    # Load URLs that were processed (attempted) in previous runs to avoid re-processing
    previously_completed_urls = load_previously_completed_urls()

    urls_to_process = sorted(list(set(urls) - previously_completed_urls)) # Avoid duplicates and already processed
    num_urls_to_process = len(urls_to_process)

    if not urls_to_process:
        logging.info("All URLs from the list have been processed in previous runs (found in resume file or output dir).")
        # Optionally, still generate a summary based on existing files/logs
        num_html_files = 0
        if os.path.exists(OUTPUT_DIR):
             num_html_files = len([f for f in os.listdir(OUTPUT_DIR) if f.endswith(".html")])
        print("\n--- Summary (No new URLs processed) ---")
        print(f"Total URLs in list: {total_urls_in_file}")
        print(f"URLs processed in previous runs: {len(previously_completed_urls)}")
        print(f"Total HTML files found in {OUTPUT_DIR}: {num_html_files}")
        print(f"Script Version: {script_version}")
        print(f"Configuration: {config_string}")
        return

    logging.info(f"Preparing to process {num_urls_to_process} new URLs.")

    # Domain-based semaphore management - Use a standard dict as it's managed within the main async context
    domain_semaphores = defaultdict(lambda: asyncio.Semaphore(DOMAIN_REQUEST_LIMIT))

    queue = asyncio.Queue()
    processed_in_this_run = set() # Track URLs added to the queue for this run
    for url in urls_to_process:
        queue.put_nowait(url)
        processed_in_this_run.add(url)

    connector = aiohttp.TCPConnector(limit=None) # Allow session to manage concurrency via semaphores
    async with aiohttp.ClientSession(connector=connector) as session:

        workers = []
        # Setup tqdm progress bar
        with tqdm(total=num_urls_to_process, desc="Downloading URLs", unit="url",
                  dynamic_ncols=True) as pbar:

            for _ in range(MAX_CONCURRENT_REQUESTS):
                worker_task = asyncio.create_task(
                    worker(queue, session, domain_semaphores, error_messages, non_html_skipped_urls, pbar)
                    # Removed unused 'completed_urls' and 'error_counts' args from v4.20
                )
                workers.append(worker_task)

            # Wait for queue to be processed
            await queue.join()

            # Signal workers to stop
            for _ in range(MAX_CONCURRENT_REQUESTS):
                await queue.put(None)

            # Wait for all workers to finish
            await asyncio.gather(*workers, return_exceptions=True) # Handle potential worker exceptions

    end_time = time.time()
    elapsed_time = end_time - start_time
    # Iteration per second based on URLs processed *in this run*
    iteration_per_second = num_urls_to_process / elapsed_time if elapsed_time > 0 else 0

    # --- Post-processing and Summary ---

    # Save all URLs processed in *this* run to the resume file
    save_processed_urls(processed_in_this_run)

    # Count successful downloads by checking files created *during this run*
    # A simple way: count files in the output dir. Assumes no deletions during run.
    num_html_files_total = 0
    if os.path.exists(OUTPUT_DIR):
        num_html_files_total = len([f for f in os.listdir(OUTPUT_DIR) if f.endswith(".html")])


    # Analyze error_messages from this run
    final_error_counts = Counter()
    keyword_skipped_count = 0
    failed_downloads_count = 0 # Count actual failures (not keyword skips)

    # Convert manager dict proxy to regular dict for easier processing
    current_run_errors = dict(error_messages)

    for url, error_reason in current_run_errors.items():
        if error_reason == "skipped_keyword":
            keyword_skipped_count += 1
        else:
            final_error_counts[error_reason] += 1
            failed_downloads_count += 1 # Count this as a failure/issue

    # Save detailed error log for this run
    try:
        with open(ERROR_LOG_FILE, "w", encoding='utf-8') as f:
            if current_run_errors:
                 f.write("--- Errors and Skips Log ---\n")
                 for url, error in sorted(current_run_errors.items()):
                     f.write(f"{url}: {error}\n")
            else:
                 f.write("No errors or skips recorded in this run.\n")
        logging.info(f"Error/skip details for this run saved to {ERROR_LOG_FILE}")
    except Exception as e:
        logging.error(f"Error saving error log file: {e}")

    # Save non-HTML skipped log for this run
    try:
        # Convert manager dict proxy
        current_non_html = dict(non_html_skipped_urls)
        with open(NON_HTML_SKIPPED_LOG_FILE, "w", encoding='utf-8') as f:
             if current_non_html:
                 f.write("--- Non-HTML Skipped URLs Log ---\n")
                 for url, ctype in sorted(current_non_html.items()):
                     f.write(f"{url}: {ctype}\n")
             else:
                 f.write("No non-HTML URLs skipped in this run.\n")
        logging.info(f"Non-HTML skipped URLs for this run saved to {NON_HTML_SKIPPED_LOG_FILE}")
    except Exception as e:
        logging.error(f"Error saving non-HTML log file: {e}")


    # --- Generate Summary ---
    summary = f"""
--- Summary ---
Total URLs in list ({URL_FILE}): {total_urls_in_file}
URLs processed in this run: {num_urls_to_process} (Excluded {len(previously_completed_urls)} from resume/previous runs)
Total HTML files saved ({OUTPUT_DIR}): {num_html_files_total}

Breakdown for this run ({num_urls_to_process} attempts):
  - Skipped (no keywords): {keyword_skipped_count}
  - Failures/Other Skips: {failed_downloads_count}
  (Successful downloads in this run = {num_urls_to_process - keyword_skipped_count - failed_downloads_count})

Time elapsed: {elapsed_time:.2f} seconds
Processing rate: {iteration_per_second:.2f} URLs/second (for this run)

Error/Skip counts for this run:
"""
    # Add the detailed error counts
    if final_error_counts:
        # Sort by count descending, then by key alphabetically
        sorted_errors = sorted(final_error_counts.items(), key=lambda item: (-item[1], item[0]))
        for error_type, count in sorted_errors:
            summary += f"  - {error_type}: {count}\n"
    else:
        summary += "  - None\n"

    summary += f"\nError/skip details saved to: {ERROR_LOG_FILE}"
    summary += f"\nNon-HTML skipped URLs saved to: {NON_HTML_SKIPPED_LOG_FILE}"
    summary += f"\nProcessed URLs list saved to: {RESUME_FILE}"
    summary += f"\nScript Version: {script_version}"
    summary += f"\nConfiguration: {config_string}"

    print(summary) # Print summary to console

    # Save summary to text file
    try:
        with open(output_filename, "w", encoding='utf-8') as f:
            f.write(summary)
        logging.info(f"Summary saved to {output_filename}")
    except Exception as e:
        logging.error(f"Error saving summary to file: {e}")


if __name__ == "__main__":
    # freeze_support() is necessary for multiprocessing support when bundled (e.g., with PyInstaller) on Windows
    multiprocessing.freeze_support()
    # Use the default asyncio event loop policy unless specific issues arise
    asyncio.run(main())
