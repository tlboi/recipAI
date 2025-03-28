#filter_URL_for_recipe_step1_manual_4.0.py
import os
import re
import sys
from urllib.parse import unquote
from multiprocessing import Pool

try:
    from unidecode import unidecode
except ImportError:
    print("Error: The 'unidecode' library is required. Install it via 'pip install unidecode'.")
    sys.exit(1)

def read_terms(filename: str):
    """Read terms from a file, one term per line, and return a list of non-empty strings."""
    if not os.path.exists(filename):
        print(f"Error: {filename} does not exist.")
        sys.exit(1)
    with open(filename, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

# Load terms from their respective text files
NEGATIVE_TERMS = read_terms("NEGATIVE_TERMS.txt")
POSITIVE_TERMS = read_terms("POSITIVE_TERMS.txt")

def normalize_url(url: str) -> str:
    """
    Normalize a URL:
    - Strip leading/trailing whitespace.
    - Repeatedly decode percent-encoded characters until stable.
    - Convert to lower case.
    """
    url = url.strip()
    prev = None
    while url != prev:
        prev = url
        url = unquote(url)
    return url.lower()

def convert_to_readable(text: str) -> str:
    """
    Convert non-readable or non-ASCII characters into an ASCII approximation
    using the Unidecode library.
    """
    return unidecode(text)

def clean_url(url: str) -> str:
    """
    Normalize the URL and then convert it into a more readable ASCII form.
    """
    normalized = normalize_url(url)
    readable = convert_to_readable(normalized)
    return readable

def classify_url(url: str) -> (str, str):
    """
    Classify a URL into one of three categories based on its cleaned form:
    - 'discarded' if any negative term is found.
    - 'kept' if any positive term is found.
    - 'uncategorized' if neither is found.
    
    Returns a tuple: (cleaned_url, category)
    """
    cleaned = clean_url(url)

    # Check for negative terms first.
    for term in NEGATIVE_TERMS:
        if re.search(term, cleaned):
            return (cleaned, 'discarded')
    
    # Check for positive terms.
    for term in POSITIVE_TERMS:
        if re.search(term, cleaned):
            return (cleaned, 'kept')
    
    # Otherwise, mark as uncategorized.
    return (cleaned, 'uncategorized')

def process_urls(input_file: str):
    if not os.path.exists(input_file):
        print(f"Error: {input_file} does not exist.")
        sys.exit(1)

    # Read all URLs from the input file.
    with open(input_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    # Use multiprocessing to classify URLs concurrently.
    with Pool() as pool:
        results = pool.map(classify_url, urls)

    # Separate the URLs by their classification.
    discarded_urls = [url for url, cat in results if cat == 'discarded']
    kept_urls = [url for url, cat in results if cat == 'kept']
    uncategorized_urls = [url for url, cat in results if cat == 'uncategorized']

    # Write the results to separate files.
    with open("discarded_urls.txt", "w", encoding="utf-8") as f:
        for url in discarded_urls:
            f.write(url + "\n")
    
    with open("kept_urls.txt", "w", encoding="utf-8") as f:
        for url in kept_urls:
            f.write(url + "\n")
    
    with open("uncategorized_urls.txt", "w", encoding="utf-8") as f:
        for url in uncategorized_urls:
            f.write(url + "\n")

def main():
    input_file = "all_urls.txt"  # Ensure this file exists with your URLs.
    process_urls(input_file)
    print("URL classification complete. Check the output files.")

if __name__ == "__main__":
    main()
