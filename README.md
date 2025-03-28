# recipAI 🥣 Recipe Scraper Pipeline

A modular and scalable Python-based pipeline for large-scale **recipe data collection**, from domain checking to full HTML parsing and structured extraction.

The end-goal of this (ongoing) project is to retrieve enough free-of-rights recipes from the web to fine tune an AI language model to cook and come up with its own original recipes! So far we have 7 million recipes scraped from the web. 

## 📁 Short summary

1. Prepare a .txt file with all your main domain URLs
2. Run robot.py to establish the list of websites that allow crawling
3. Run crawler.py to get all the recipe-related sub-urls
4. Run filter.py to discard those that are potentially not recipes
5. Run fetcher.py to get light-weight htmls
6. Run parser.py to get recipe-relevant entries into clean database files

## 📁 Project Structure

```
📦 project-root/
├── robot.py           # Checks if crawling is allowed via robots.txt
├── crawler.py         # Recursively collects recipe-related URLs from approved domains
├── filter.py          # Filters and classifies URLs using positive/negative keyword lists
├── fetcher.py         # Downloads and extracts HTML content with smart filtering and logging
├── parser.py          # Parses saved HTML pages and extracts structured recipe data
├── websites.txt       # Input: List of domains to process
├── robot_cache.txt    # Intermediate: Domains and whether they can be crawled
├── NEGATIVE_TERMS.txt # Input: Patterns to discard unwanted URLs
├── POSITIVE_TERMS.txt # Input: Patterns that indicate recipe relevance
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Recommended: Virtual environment (`venv`)
- Install dependencies:
  ```bash
  pip install aiohttp aiofiles httpx tqdm beautifulsoup4 unidecode lxml
  ```

---

## 🧭 Pipeline Overview

### 1. `robot.py` – Respectful Crawler Initiation

Checks `robots.txt` rules to see if crawling is allowed for a list of domains.

- **Input:** `websites.txt`
- **Output:** `robot_cache.txt`
- **Skips** domains that disallow crawling.

```bash
python robot.py
```

---

### 2. `crawler.py` – Recursive URL Discovery

Recursively crawls domains from `robot_cache.txt`, collecting only recipe-related links.

- Depth-limited crawl.
- Output: A domain-wide URL list (`all_urls_depth*_...txt`)
- Also generates a performance summary.

```bash
python crawler.py
```

---

### 3. `filter.py` – Recipe Link Filtering

Filters the collected URLs using custom keyword lists.

- Classifies links into:
  - `kept_urls.txt`
  - `discarded_urls.txt`
  - `uncategorized_urls.txt`
- Uses:
  - `POSITIVE_TERMS.txt` (e.g., `ingredient`, `recipe`)
  - `NEGATIVE_TERMS.txt` (e.g., `login`, `about`, `shop`)

```bash
python filter.py
```

---

### 4. `fetcher.py` – HTML Content Collector

Downloads HTML pages from `kept_urls.txt`, filtering for valid recipe content.

- Smart domain rate-limiting and concurrency
- Extracts `<body>` content while skipping non-relevant pages
- Resume support and detailed error logging

```bash
python fetcher.py
```

**Output:**

- HTML files: `html_files_*`
- Logs: `error_log_*.txt`, `summary_*.txt`, etc.

---

### 5. `parser.py` – Structured Recipe Extractor

Parses downloaded HTML pages to extract structured recipe data using JSON-LD and fallback heuristics.

- Output formats:
  - `recipes_database3.1.json`
  - `recipes_database3.1.db` (SQLite)
  - `recipes_database3.1.csv`

```bash
python parser.py
```

---

## 📊 Output Schema

Each extracted recipe includes:

- `title`
- `description`
- `ingredients` (list)
- `instructions` (list)
- `prep_time`, `cook_time`, `total_time` (ISO 8601 or plain text)
- `yield`
- `author`
- `image_url`
- `source_file`

---

## ⚙️ Customization Tips

- **Keyword tuning**: Edit `POSITIVE_TERMS.txt` and `NEGATIVE_TERMS.txt` to refine URL filtering
- **Concurrency & depth**: Adjust `MAX_CONCURRENT_REQUESTS`, `DOMAIN_REQUEST_LIMIT`, and `MAX_DEPTH` in scripts
- **HTML directory**: Change `INPUT_HTML_DIR` in `parser.py` to point to your HTML folder

---

## 🧠 Design Philosophy

- Respects `robots.txt`
- Highly parallelized (`asyncio`, `threading`, `multiprocessing`)
- Resilient to malformed HTML, timeouts, and connection errors
- Designed for scale and transparency

---

## 🧩 Future Ideas

- Automatic recipe deduplication
- RDF or JSON-LD export for semantic web compatibility
- Web interface for browsing recipes
- Dockerized deployment

---

## 📄 License
Copyright © 2025 Thierry Ludovic Boissière

This software and all accompanying files are provided for **personal, educational, and non-commercial use only**.

You may:
- Use, study, and modify this software for non-commercial purposes
- Share unmodified copies of the code for non-commercial use, with attribution

You may NOT:
- Use this software for any commercial purpose, including but not limited to services, websites, data collection, product development, or consulting
- Redistribute modified versions without written permission
- Sell, license, sublicense, or include this code in commercial offerings

All rights are reserved by the author. For commercial licensing, please contact: thierry.l.boissiere@gmail.com

THIS SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND.

```
