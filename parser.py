#parser3.1WORKING.py
import os
import json
import re
import logging
from pathlib import Path
from bs4 import BeautifulSoup, NavigableString, Comment
import time
import sqlite3
import csv
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse, urljoin

# --- Configuration ---
INPUT_HTML_DIR = "./bodies"                 # Directory containing HTML files
OUTPUT_JSON_FILE = "recipes_database3.1.json"  # Output JSON filename
OUTPUT_DB_FILE = "recipes_database3.1.db"      # Output SQLite DB filename
OUTPUT_CSV_FILE = "recipes_database3.1.csv"    # Output CSV filename
MAX_WORKERS = os.cpu_count()                # Number of parallel processes (adjust if needed)
LOG_FILE = "recipe_extraction3.1.log"          # Log filename
LOG_LEVEL = logging.INFO                    # Logging level (INFO, DEBUG, WARNING, ERROR)

# --- Logging Setup ---
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s', # Added process ID
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'), # Ensure UTF-8 for logs
        logging.StreamHandler()
    ]
)

# --- Helper Functions ---

def clean_text(text: Optional[str]) -> str:
    """Strip whitespace, replace multiple spaces, remove common leading list markers."""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text).strip()
    # Remove common leading list markers/noise (non-word chars, except numbers/fractions)
    text = re.sub(r'^\s*[▢•*–-]\s*', '', text)
    return text.strip()

def parse_time(time_str: Optional[str]) -> Optional[str]:
    """Parse common time formats into ISO 8601 duration (PTxHxM) or return cleaned input."""
    if not time_str: return None
    time_str = clean_text(time_str)

    # Handle ISO 8601 Duration (e.g., "PT1H30M")
    iso_match = re.match(r'^P(T.+)$', time_str, re.IGNORECASE)
    if iso_match and any(c in iso_match.group(1) for c in 'HMS'):
        return time_str.upper()

    # Handle formats like "1 hr 30 min", "90 minutes", "1 hour", etc.
    hours = 0; minutes = 0
    hour_match = re.search(r'(\d+)\s*(?:hours?|hrs?|hr|h)', time_str, re.IGNORECASE)
    if hour_match: hours = int(hour_match.group(1))

    min_match = re.search(r'(\d+)\s*(?:minutes?|mins?|min|m)', time_str, re.IGNORECASE)
    if min_match:
        mins_val = int(min_match.group(1))
        # Handle cases like "90 minutes" without explicit hours
        if not hour_match and mins_val >= 60:
             hours = mins_val // 60; minutes = mins_val % 60
        else: minutes = mins_val

    # Handle cases like "90'"
    prime_min_match = re.search(r'(\d+)\'', time_str)
    if prime_min_match and not hour_match and not min_match:
         m = int(prime_min_match.group(1)); hours = m // 60; minutes = m % 60

    if hours > 0 or minutes > 0:
        duration = "PT"
        if hours > 0: duration += f"{hours}H"
        if minutes > 0: duration += f"{minutes}M"
        if duration != "PT": return duration

    # Fallback: return cleaned original string if no specific format matched well
    return time_str if time_str else None


def extract_list_items(element: Optional[BeautifulSoup]) -> List[str]:
    """Extracts text from direct child list items (li) within a given element."""
    items = []
    if not element: return items

    for item in element.find_all('li'):
        parent_list = item.find_parent(['ul', 'ol'])
        # Only extract if the li is a direct child of the target element (if it's a list)
        # or if the li is inside a list that is a direct child of the target element.
        is_direct_child_of_list_element = element and element.name in ['ul', 'ol'] and item.parent == element
        is_in_direct_child_list = parent_list and parent_list.parent == element

        if is_direct_child_of_list_element or is_in_direct_child_list:
            text = clean_text(item.get_text(separator=' ', strip=True))
            # Filter out items that are *only* noise after cleaning
            if text and (len(text) > 1 or text.isalnum()):
                # Attempt to remove common embedded notes
                text = re.sub(r'\s*\(Note:.*?\)\s*|\s*Note:.*', '', text, flags=re.IGNORECASE).strip()
                if text: items.append(text)
    return items

# --- Extraction Strategies ---

def _extract_from_json_ld(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """Extracts recipe data from JSON-LD script tags (Schema.org). Most reliable source."""
    recipe_data = {}
    json_ld_scripts = soup.find_all('script', type='application/ld+json')

    for script in json_ld_scripts:
        try:
            script_content = script.string
            if not script_content: continue
            # Remove JS comments which can break JSON parsing
            script_content = re.sub(r'//.*?\n|/\*.*?\*/', '', script_content, flags=re.S)
            data = json.loads(script_content)
        except (json.JSONDecodeError, TypeError):
            logging.debug("Failed to parse JSON-LD script content.", exc_info=True)
            continue

        # JSON-LD can be a list or a single object, potentially nested in @graph
        potential_recipes = []
        if isinstance(data, dict):
            if data.get('@graph'): potential_recipes.extend(item for item in data['@graph'] if isinstance(item, dict))
            else: potential_recipes.append(data)
        elif isinstance(data, list): potential_recipes.extend(item for item in data if isinstance(item, dict))

        for item in potential_recipes:
            item_type = item.get('@type')
            is_recipe = isinstance(item_type, str) and item_type.lower() == 'recipe' or \
                        isinstance(item_type, list) and 'Recipe' in item_type

            if is_recipe:
                # Extract fields, preferring JSON-LD if available
                recipe_data['title'] = clean_text(item.get('name')) or recipe_data.get('title')
                recipe_data['description'] = clean_text(item.get('description')) or recipe_data.get('description')

                # Handle image variations and filter data URIs
                img_data = item.get('image'); image_url = None
                if isinstance(img_data, str): image_url = img_data
                elif isinstance(img_data, dict): image_url = img_data.get('url')
                elif isinstance(img_data, list) and img_data:
                    first_img = img_data[0]
                    if isinstance(first_img, str): image_url = first_img
                    elif isinstance(first_img, dict): image_url = first_img.get('url')
                if image_url and not image_url.startswith('data:'):
                     recipe_data['image_url'] = image_url # Don't use 'or recipe_data.get' here to prioritize this source

                # Extract Ingredients
                ingredients = item.get('recipeIngredient', item.get('ingredients'))
                if ingredients and isinstance(ingredients, list):
                    clean_ingredients = [clean_text(ing) for ing in ingredients if isinstance(ing, str)]
                    recipe_data['ingredients'] = [ing for ing in clean_ingredients if ing] or recipe_data.get('ingredients')

                # Extract Instructions (handling text, list, HowToStep, HowToSection)
                instructions_data = item.get('recipeInstructions', item.get('instructions'))
                instruction_list = []
                if isinstance(instructions_data, str): instruction_list = [clean_text(step) for step in instructions_data.split('\n') if clean_text(step)]
                elif isinstance(instructions_data, list):
                    for step in instructions_data:
                        step_text = None
                        if isinstance(step, str): step_text = clean_text(step)
                        elif isinstance(step, dict):
                            step_type = step.get('@type')
                            if step_type == 'HowToStep': step_text = clean_text(step.get('text'))
                            elif step_type == 'HowToSection':
                                section_name = clean_text(step.get('name'))
                                if section_name: instruction_list.append(f"--- {section_name} ---")
                                section_items = step.get('itemListElement', [])
                                for section_step in section_items:
                                     sec_step_text = None
                                     if isinstance(section_step, str): sec_step_text = clean_text(section_step)
                                     elif isinstance(section_step, dict) and section_step.get('@type') == 'HowToStep': sec_step_text = clean_text(section_step.get('text'))
                                     if sec_step_text: instruction_list.append(sec_step_text) # Append inner steps
                        if step_text: instruction_list.append(step_text) # Append outer step
                if instruction_list: recipe_data['instructions'] = [step for step in instruction_list if step] or recipe_data.get('instructions')

                # Extract Times
                recipe_data['prep_time'] = parse_time(item.get('prepTime')) or recipe_data.get('prep_time')
                recipe_data['cook_time'] = parse_time(item.get('cookTime')) or recipe_data.get('cook_time')
                recipe_data['total_time'] = parse_time(item.get('totalTime')) or recipe_data.get('total_time')

                # Extract Yield (without fallback to prevent time mix-up)
                yield_val = item.get('recipeYield', item.get('yield'))
                if isinstance(yield_val, list): yield_val = yield_val[0] if yield_val else None
                if yield_val is not None:
                     yield_str = clean_text(str(yield_val))
                     # Final check: don't assign if yield looks like a duration string
                     if not re.match(r'^P?T?[\d.]+[HMS]', yield_str, re.I):
                         recipe_data['yield'] = yield_str

                # Extract Author
                author = item.get('author')
                if isinstance(author, dict): recipe_data['author'] = clean_text(author.get('name')) or recipe_data.get('author')
                elif isinstance(author, list) and author:
                     first_author = author[0]
                     if isinstance(first_author, dict): recipe_data['author'] = clean_text(first_author.get('name')) or recipe_data.get('author')
                     elif isinstance(first_author, str): recipe_data['author'] = clean_text(first_author) or recipe_data.get('author')
                elif isinstance(author, str): recipe_data['author'] = clean_text(author) or recipe_data.get('author')

                # If we found a recipe, return it (assuming first is the main one)
                return {k: v for k, v in recipe_data.items() if v is not None and v != ""}

    return None # No valid Recipe type found in JSON-LD

def _find_element_by_keywords(soup: BeautifulSoup, keywords: List[str], tag_names: List[str] = ['div', 'ul', 'ol', 'section', 'p']) -> Optional[BeautifulSoup]:
    """Finds an element possibly containing recipe info by keywords in itemprop, id, class, or nearby headers."""
    for keyword in keywords:
        # 1. Try itemprop (Schema.org microdata)
        itemprop_matches = soup.find_all(attrs={'itemprop': re.compile(rf'\b{keyword}\b', re.I)})
        if itemprop_matches:
            first_match = itemprop_matches[0];
            # If itemprop is on 'li', find its parent container
            if first_match.name == 'li': parent_container = first_match.find_parent(['ul', 'ol', 'div', 'section']);
            if parent_container: return parent_container
            return first_match

        # 2. Try ID (should be unique)
        id_matches = soup.find_all(tag_names, attrs={'id': re.compile(rf'\b{keyword}\b', re.I)})
        if id_matches: return id_matches[0]

        # 3. Try Class
        class_matches = soup.find_all(tag_names, attrs={'class': re.compile(rf'\b{keyword}\b', re.I)})
        if class_matches: return class_matches[0] # Return first match for simplicity

    # 4. Fallback: Search for Headers (h2-h5, strong, b, p) containing keywords
    for header_tag in ['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'p']:
        headers = soup.find_all(header_tag, string=re.compile(rf'\b{keyword}\b', re.I))
        for header in headers:
            # Check next sibling element
            potential_container = header.find_next_sibling(['ul', 'ol', 'div', 'section', 'p', 'table'])
            if potential_container:
                # Check if sibling looks relevant (has list items, enough text, or plugin classes)
                has_lis = potential_container.find('li')
                has_significant_text = len(clean_text(potential_container.get_text(" ", strip=True))) > 50
                has_plugin_class = potential_container.find(class_=re.compile(r'wprm|tasty|mv-create|recipe', re.I))
                if potential_container.name in ['ul', 'ol'] or has_lis or has_significant_text or has_plugin_class:
                    return potential_container
            # Check if header's parent container looks relevant (and not too broad)
            parent_container = header.find_parent(['div', 'section'])
            if parent_container:
                 has_lis = parent_container.find('li'); has_plugin_class = parent_container.find(class_=re.compile(r'wprm|tasty|mv-create|recipe', re.I))
                 # Avoid huge containers like body/main; check content > header content
                 if (has_lis or has_plugin_class) and len(parent_container.find_all()) < 150: # Increased limit slightly
                     if len(clean_text(parent_container.get_text())) > len(clean_text(header.get_text())) + 20:
                          return parent_container
    return None


def _extract_common_patterns(soup: BeautifulSoup, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts data based on common HTML patterns (classes, IDs, tags, itemprops) if not already found."""

    # --- Title ---
    if not extracted_data.get('title'):
        title_selectors = ['h1.entry-title', 'h1.post-title', 'h1.recipe-title', 'h1.recipe-hed', 'h1[itemprop="name"]', '.entry-title', '.post-title', '.recipe-title', '#recipe-name', '#recipe_title', '.recipe-hed', 'h1']
        for selector in title_selectors:
            title_element = soup.select_one(selector)
            if title_element: extracted_data['title'] = clean_text(title_element.get_text()); break
        # Fallback to HTML <title> tag, attempting to remove site name
        if not extracted_data.get('title') and soup.title and soup.title.string:
            title_text = clean_text(soup.title.string); title_text = re.split(r'\s*[|\-–—:»]\s*', title_text)[0].strip(); title_text = re.sub(r'\s+(?:Recipe|Recipes|Blog|Home|Page).*$', '', title_text, flags=re.I).strip()
            if len(title_text.split()) > 1 or len(title_text) > 10: extracted_data['title'] = title_text

    # --- Description ---
    if not extracted_data.get('description'):
         desc_selectors = ['[itemprop="description"]', '.wprm-recipe-summary', '.tasty-recipes-description', '.mv-create-description', '.recipe-summary', '.summary', '.description', '.dek', '.intro']
         found_desc = False
         for selector in desc_selectors:
              desc_element = soup.select_one(selector)
              if desc_element:
                  desc_text = clean_text(desc_element.get_text());
                  # Basic check to avoid common author bios/intros misidentified as description
                  if desc_text and 'follow my journey' not in desc_text.lower() and 'recipe developer' not in desc_text.lower():
                      extracted_data['description'] = desc_text; found_desc = True; break
         # Fallback to meta description tags
         if not found_desc:
             meta_desc = soup.find('meta', attrs={'name': re.compile(r'description', re.I)})
             if meta_desc and meta_desc.get('content'): extracted_data['description'] = clean_text(meta_desc['content'])
             else:
                 og_desc = soup.find('meta', property='og:description')
                 if og_desc and og_desc.get('content'): extracted_data['description'] = clean_text(og_desc['content'])

    # --- Ingredients ---
    if not extracted_data.get('ingredients'):
        ingredient_keywords = ['recipeIngredient', 'wprm-recipe-ingredients', 'tasty-recipes-ingredients', 'mv-create-ingredients', 'recipe-ingredients', 'ingredients-section', 'ingredient-list', 'ingredient', 'ingred', 'materials']
        ingredient_section = _find_element_by_keywords(soup, ingredient_keywords, ['div', 'ul', 'ol', 'section', 'p', 'table'])
        if ingredient_section:
            ingredients = []; plugin_item_selectors = ['.wprm-recipe-ingredient', '.tasty-recipes-ingredients-body li', '.mv-create-ingredients li']; found_via_plugin_item = False
            # 1. Try specific plugin item classes first
            for item_selector in plugin_item_selectors:
                 items = ingredient_section.select(item_selector)
                 if items: ingredients = [clean_text(item.get_text()) for item in items if clean_text(item.get_text())];
                 if ingredients: found_via_plugin_item = True; break
            # 2. Try generic list items (li)
            if not found_via_plugin_item: ingredients = extract_list_items(ingredient_section)
            # 3. Fallback to paragraphs (if section isn't a list)
            if not ingredients and ingredient_section.name not in ['ul', 'ol']:
                 p_tags = ingredient_section.find_all('p', recursive=False);
                 if not p_tags: p_tags = ingredient_section.find_all('p') # Try descendants if no direct children
                 p_ingredients = [clean_text(p.get_text()) for p in p_tags if clean_text(p.get_text())]
                 # Check if paragraphs look like ingredient lines
                 plausible_p = [p for p in p_ingredients if re.match(r'^(\d+|[\d½⅓⅔¼¾⅕⅖⅗⅘⅙⅚⅛⅜⅝⅞]|\*|•|-)', p, re.I) or (0 < len(p.split()) < 15)]
                 if len(plausible_p) >= 2: ingredients = plausible_p # Require multiple plausible lines
            # 4. Fallback to table rows (if section is a table)
            if not ingredients and ingredient_section.name == 'table':
                 rows = ingredient_section.find_all('tr'); td_ingredients = []
                 for row in rows:
                     cells = row.find_all('td'); row_text = " ".join(clean_text(cell.get_text()) for cell in cells[:2]) # Combine first few cells
                     if row_text: td_ingredients.append(row_text)
                 if len(td_ingredients) >= 2: ingredients = td_ingredients
            # Assign if found
            if ingredients: extracted_data['ingredients'] = [ing for ing in ingredients if ing] # Final check for empty strings

    # --- Instructions ---
    if not extracted_data.get('instructions'):
        instruction_keywords = ['recipeInstructions', 'recipeinstruction', 'wprm-recipe-instructions', 'tasty-recipes-instructions', 'mv-create-instructions', 'recipe-directions', 'instructions-section', 'method-section', 'directions-list', 'instruction', 'direction', 'method', 'procedure', 'steps', 'preparation']
        instruction_section = _find_element_by_keywords(soup, instruction_keywords, ['div', 'ol', 'ul', 'section', 'p', 'table'])
        if instruction_section:
            instructions = []; plugin_item_selectors = ['.wprm-recipe-instruction', '.tasty-recipes-instructions-body li', '.mv-create-instructions li', '.recipe-directions__item']; found_via_plugin_item = False
            # 1. Try specific plugin item classes first
            for item_selector in plugin_item_selectors:
                 items = instruction_section.select(item_selector)
                 if items: instructions = [clean_text(item.get_text(strip=True)) for item in items if clean_text(item.get_text(strip=True))];
                 if instructions: found_via_plugin_item = True; break
            # 2. Try ordered lists (ol > li)
            if not found_via_plugin_item:
                if instruction_section.name == 'ol': instructions = extract_list_items(instruction_section)
                else: ol_element = instruction_section.find('ol');
                if ol_element: instructions = extract_list_items(ol_element)
            # 3. Try unordered lists (ul > li)
            if not instructions:
                 if instruction_section.name == 'ul': instructions = extract_list_items(instruction_section)
                 else: ul_element = instruction_section.find('ul');
                 if ul_element: instructions = extract_list_items(ul_element)
            # 4. Fallback to paragraphs (if section isn't a list)
            if not instructions and instruction_section.name not in ['ul', 'ol']:
                 p_tags = instruction_section.find_all('p', recursive=False);
                 if not p_tags: p_tags = instruction_section.find_all('p')
                 p_instructions = [clean_text(p.get_text()) for p in p_tags if clean_text(p.get_text())]
                 # Check if paragraphs look like instruction steps
                 plausible_p = [p for p in p_instructions if re.match(r'^(?:Step\s*\d+|\d+\.|First|Next|Then|Finally|Meanwhile|Preheat|Combine|Mix|Stir|Bake|Cook|Serve)', p, re.I) or len(p.split()) >= 5]
                 # Avoid single short paragraph as instructions
                 if len(plausible_p) > 1 or (len(plausible_p) == 1 and len(plausible_p[0].split()) >= 10):
                     instructions = plausible_p
            # Assign if found
            if instructions: extracted_data['instructions'] = [step for step in instructions if step] # Final check for empty strings

    # --- Time & Yield --- (REVISED LOGIC)
    time_yield_keys = {
        'prep_time': ['prep_time', 'prep-time', 'preptime'],
        'cook_time': ['cook_time', 'cook-time', 'cooktime'],
        'total_time': ['total_time', 'total-time', 'totaltime', 'ready_in', 'readyin'],
        'yield': ['yield', 'servings', 'makes', 'recipe_yield']
    }
    plugin_classes = [ # Generic plugin classes for time/yield
        'wprm-recipe-prep_time', 'wprm-recipe-cook_time', 'wprm-recipe-total_time', 'wprm-recipe-servings',
        'tasty-recipes-prep-time', 'tasty-recipes-cook-time', 'tasty-recipes-total-time', 'tasty-recipes-yield',
        'mv-create-prep-time', 'mv-create-cook-time', 'mv-create-total-time', 'mv-create-yield'
    ]
    itemprop_map = {'prep_time': ['prepTime'], 'cook_time': ['cookTime'], 'total_time': ['totalTime'], 'yield': ['recipeYield']}

    # Search within common info/meta blocks first
    possible_areas = soup.find_all(['div', 'p', 'ul', 'span'], class_=re.compile(r'info|meta|details|summary|time|yield|servings|recipe-meta|entry-meta', re.I))
    search_soup = possible_areas if possible_areas else [soup] # Search specific areas or whole soup if none found

    for key, base_patterns in time_yield_keys.items():
         if not extracted_data.get(key):
             found = False
             # Build search list specific to this key
             relevant_itemprops = itemprop_map.get(key, [])
             search_patterns = base_patterns + plugin_classes + relevant_itemprops

             for pattern in search_patterns:
                 safe_pattern = re.escape(pattern) # For label removal regex
                 elements = []
                 for area in search_soup:
                     # Search by class, id, and relevant itemprop
                     elements.extend(area.find_all(['span', 'div', 'p', 'li', 'time', 'dt', 'dd'], attrs={'class': re.compile(rf'\b{pattern}\b', re.I)}))
                     elements.extend(area.find_all(['span', 'div', 'p', 'li', 'time', 'dt', 'dd'], attrs={'id': re.compile(rf'\b{pattern}\b', re.I)}))
                     if pattern in relevant_itemprops: elements.extend(area.find_all(attrs={'itemprop': re.compile(rf'\b{pattern}\b', re.I)}))
                 elements = list(dict.fromkeys(elements)) # Remove duplicates

                 for element in elements:
                     value = None
                     # 1. Check specific attributes
                     if element.name == 'time' and element.get('datetime') and 'time' in key: value = element['datetime']
                     elif element.has_attr('itemprop') and element.get('content'): value = element['content']
                     else: # 2. Extract from text content
                          text = clean_text(element.get_text(" ", strip=True))
                          # Try removing label (require separator)
                          label_pattern = re.compile(rf'^\s*(?:{key.replace("_", r"[ _-]?")}|{safe_pattern})\s*[:\-]\s+', re.I)
                          possible_value = label_pattern.sub('', text).strip()
                          if possible_value and possible_value != text: value = possible_value
                          # If label removal failed, check siblings/children only if text IS the label
                          elif text.lower() == pattern.lower() or text.lower() == key.replace('_',' ').lower():
                              value_el = element.find(['span', 'strong', 'time'], class_=re.compile(r'value|time|amount|data', re.I))
                              if value_el: value = clean_text(value_el.get_text())
                              else:
                                  next_sib = element.next_sibling;
                                  while next_sib and isinstance(next_sib, (NavigableString, Comment)): next_sib = next_sib.next_sibling
                                  if next_sib and next_sib.name: value = clean_text(next_sib.get_text())
                                  elif next_sib and isinstance(next_sib, NavigableString): value = clean_text(str(next_sib))
                          # Otherwise, use text content if it's not just the label
                          elif possible_value == text and not (text.lower() == pattern.lower() or text.lower() == key.replace('_',' ').lower()):
                               value = text

                     # 3. Validate and Assign
                     if value:
                         value = clean_text(value)
                         # Skip if value looks like a label for another field
                         if value.lower() in ['prep time', 'cook time', 'total time', 'yield', 'servings', 'makes']: continue

                         if 'time' in key:
                             parsed = parse_time(value)
                             if parsed: extracted_data[key] = parsed; found = True; break
                         else: # Yield/Servings
                             # CRITICAL FIX: Don't assign yield if it looks like a time duration
                             if not re.match(r'^P?T?[\d.]+[HMS]', value, re.I):
                                 extracted_data[key] = value; found = True; break
                 if found: break # Found value for this key

             # Fallback text search (using more specific regex)
             if not found:
                 patterns = {
                     'prep_time': r'(?:Prep(?:aration)?\sTime)\s*[:\-]?\s*([\d\s.\-]+(?:minutes?|mins?|min|hours?|hrs?|hr|h|m))',
                     'cook_time': r'(?:Cook(?:ing)?\sTime)\s*[:\-]?\s*([\d\s.\-]+(?:minutes?|mins?|min|hours?|hrs?|hr|h|m))',
                     'total_time': r'(?:Total\sTime|Ready\sIn)\s*[:\-]?\s*([\d\s.\-]+(?:minutes?|mins?|min|hours?|hrs?|hr|h|m))',
                     'yield': r'(?:Yield|Servings|Makes|Serves)\s*[:\-]?\s*((?:about|approx\.?)?\s*[\d\w\s\-]+(?:servings?|people|makes|yields|dozen)?\b)' # More flexible yield capture
                 }
                 if key in patterns:
                     search_text = " ".join(clean_text(area.get_text(" ", strip=True)) for area in possible_areas) or soup.get_text(" ", strip=True)
                     match = re.search(patterns[key], search_text, re.IGNORECASE)
                     if match:
                         value = clean_text(match.group(1))
                         if value:
                             if 'time' in key:
                                 parsed = parse_time(value);
                                 if parsed: extracted_data[key] = parsed
                             elif not re.match(r'^P?T?[\d.]+[HMS]', value, re.I): # Check yield again
                                  extracted_data[key] = value

    # --- Image --- (Keep enhanced filtering)
    if not extracted_data.get('image_url'):
        best_img_url = None; og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content') and not og_image['content'].startswith('data:'): best_img_url = og_image['content']
        if not best_img_url:
            itemprop_image = soup.find(attrs={'itemprop': 'image'})
            if itemprop_image:
                img_src = None
                if itemprop_image.name == 'img' and itemprop_image.get('src'): img_src = itemprop_image['src']
                elif itemprop_image.name == 'meta' and itemprop_image.get('content'): img_src = itemprop_image['content']
                elif itemprop_image.find('img') and itemprop_image.find('img').get('src'): img_src = itemprop_image.find('img')['src']
                if img_src and not img_src.startswith('data:'): best_img_url = img_src
        if not best_img_url:
            recipe_image_area = soup.find(['div', 'figure', 'p'], class_=re.compile(r'recipe-image|post-image|featured-image|wp-post-image|recipe-main-photo', re.I))
            if recipe_image_area: img_tag = recipe_image_area.find('img');
            if img_tag and img_tag.get('src') and not img_tag['src'].startswith('data:'): best_img_url = img_tag['src']
        if not best_img_url:
            all_imgs = soup.find_all('img'); candidate_imgs = []
            bad_keywords = ['ad', 'logo', 'icon', 'spinner', 'loading', 'avatar', 'gravatar', 'badge', 'button', 'social', 'pixel', 'spacer', 'header', 'footer', 'banner', 'placeholder', 'default', 'dummy', 'blank', 'transparent', 'captcha', 'stat', 'counter', 'shopify', 'rating', 'powered', 'profile']
            small_img_paths = ['/thumb/', 'small.', '_thumb.', '_tn.', '/icon/', '-sm.', '-small.', '_small.', '150x150', '100x100', '80x80', '75x75', '50x50']
            for img in all_imgs:
                 src = img.get('src')
                 if not src or src.startswith('data:') or '.svg' in src.lower() or '.gif' in src.lower(): continue
                 parent = img.find_parent(); parent_attrs = str(parent.attrs).lower() if parent else ''; img_attrs = str(img.attrs).lower(); combined_attrs = src.lower() + parent_attrs + img_attrs; src_lower = src.lower()
                 if any(kw in combined_attrs for kw in bad_keywords) or any(sp in src_lower for sp in small_img_paths): continue
                 try:
                     width = int(img.get('width', 0)); height = int(img.get('height', 0)); area = width * height; min_width=250; min_height=150; min_area=min_width*min_height
                     if area >= min_area and width >= min_width and height >= min_height: candidate_imgs.append({'url': src, 'area': area})
                     elif width == 0 and height == 0 and len(src) > 50: candidate_imgs.append({'url': src, 'area': 0})
                 except (ValueError, TypeError): continue
            if candidate_imgs: candidate_imgs.sort(key=lambda x: x['area'], reverse=True); best_img_url = candidate_imgs[0]['url']
        if best_img_url: extracted_data['image_url'] = best_img_url

    # --- Author ---
    if not extracted_data.get('author'):
        author_patterns = ['author', 'byline', 'recipe-author', 'wprm-recipe-author', 'tasty-recipes-author', 'mv-create-author']; found_author = False
        author_prop = soup.find(attrs={'itemprop': 'author'})
        if author_prop: # Try itemprop first
             if author_prop.name == 'meta' and author_prop.get('content'): extracted_data['author'] = clean_text(author_prop['content']); found_author = True
             elif author_prop.name in ['a', 'span']: extracted_data['author'] = clean_text(author_prop.get_text()); found_author = True
             elif author_prop.find(['a', 'span']): inner_el = author_prop.find(['a', 'span']); extracted_data['author'] = clean_text(inner_el.get_text()); found_author = True
        if not found_author: # Try common classes
            for pattern in author_patterns:
                author_element = soup.find(['span', 'div', 'p', 'a', 'li'], class_=re.compile(rf'\b{pattern}\b', re.I))
                if author_element:
                     link = author_element.find('a'); text = clean_text(link.get_text()) if link else clean_text(author_element.get_text())
                     text = re.sub(r'^(?:By|Author|Recipe\s+by)[:\s]*', '', text, flags=re.IGNORECASE).strip()
                     if text and len(text) > 1: extracted_data['author'] = text; found_author = True; break
        if not found_author: # Fallback to meta tag
            meta_author = soup.find('meta', attrs={'name': re.compile(r'author', re.I)})
            if meta_author and meta_author.get('content'): extracted_data['author'] = clean_text(meta_author['content'])

    return extracted_data


# --- Main Processing Function ---

def extract_recipe_info(html_content: str, file_path: str) -> Optional[Dict[str, Any]]:
    """Parses HTML, extracts recipe info via JSON-LD and common patterns, validates."""
    try: soup = BeautifulSoup(html_content, 'lxml')
    except Exception as e: logging.error(f"Failed to parse HTML file {file_path}: {e}"); return None

    recipe = {'source_file': str(file_path)}

    # 1. Try JSON-LD
    json_ld_data = _extract_from_json_ld(soup)
    if json_ld_data: logging.debug(f"Extracted primary data from JSON-LD: {file_path}"); recipe.update(json_ld_data)

    # 2. Try common patterns (merges/fills gaps)
    try: recipe = _extract_common_patterns(soup, recipe)
    except Exception as e: logging.error(f"Error during common pattern extraction for {file_path}: {e}", exc_info=True)

    # 3. Final Cleaning and Validation
    final_recipe = {}; required_fields = ['title', 'ingredients', 'instructions']; has_required = True

    # Clean up all extracted fields
    for key, value in recipe.items():
        if isinstance(value, str):
            cleaned_val = clean_text(value);
            if cleaned_val: final_recipe[key] = cleaned_val
        elif isinstance(value, list):
            # Final cleaning pass for list items
            cleaned_list = [clean_text(str(item)) for item in value if item]
            final_list = [item for item in cleaned_list if item and (len(item) > 1 or item.isalnum())]
            final_list = [re.sub(r'\s*\(Note:.*?\)\s*|\s*Note:.*', '', item, flags=re.IGNORECASE).strip() for item in final_list]
            final_list = [item for item in final_list if item] # Remove empty after note removal
            if final_list: final_recipe[key] = final_list
        elif value is not None: final_recipe[key] = value

    # Check required fields
    missing_required = [field for field in required_fields if not final_recipe.get(field)]
    if missing_required:
        logging.warning(f"Missing required field(s) '{', '.join(missing_required)}' in file: {file_path}")
        logging.warning(f"Skipping file due to missing required fields: {file_path}")
        return None

    # Plausibility checks
    min_ingredients = 2; min_instructions = 2; plausible = True; reasons = []
    if len(final_recipe.get('ingredients', [])) < min_ingredients: plausible = False; reasons.append(f"ingredients<{min_ingredients}")
    if len(final_recipe.get('instructions', [])) < min_instructions: plausible = False; reasons.append(f"instructions<{min_instructions}")
    if len(final_recipe.get('title', '')) < 5: plausible = False; reasons.append("title too short")

    # Check for non-recipe indicators (stricter if basic plausibility failed)
    non_recipe_indicators = ['category', 'tag', 'author', 'search', 'print', 'shop', 'account', 'login', 'contact', 'about', 'privacy', 'sweepstakes', 'collections', 'oembed', 'terms', 'policy', 'subscribe', 'cart', 'products', 'gallery']
    combined_check_str = (final_recipe.get('title', '') + file_path).lower()
    if any(indicator in combined_check_str for indicator in non_recipe_indicators) and not plausible:
        logging.warning(f"Skipping likely non-recipe page (indicator found + {', '.join(reasons)}): {file_path}")
        return None
    if not plausible:
        logging.warning(f"Skipping file due to implausible data ({', '.join(reasons)}): {file_path}")
        return None

    # Make image URL absolute if relative
    if 'image_url' in final_recipe and final_recipe['image_url'].startswith('/'):
        try:
            # Basic reconstruction of base URL from filename
            file_part = file_path.replace('bodies/', '', 1)
            # Try to find scheme://netloc part in the mangled filename
            url_match = re.match(r'^(https?___[^_/?#]+)', file_part)
            if url_match:
                 base_part = url_match.group(1).replace('___', '://')
                 final_recipe['image_url'] = urljoin(base_part + '/', final_recipe['image_url']) # Add trailing slash for urljoin
        except Exception as url_err:
             logging.debug(f"Could not make image URL absolute for {file_path}: {url_err}")

    logging.info(f"Successfully extracted recipe: {final_recipe.get('title', 'Untitled')} from {file_path}")
    return final_recipe

def process_file(file_path: Path) -> Optional[Dict[str, Any]]:
    """Reads HTML file and calls extraction function, handling file errors."""
    try:
        try: html_content = file_path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
             logging.warning(f"UTF-8 decode failed for {file_path}, trying with errors='ignore'")
             html_content = file_path.read_text(encoding='utf-8', errors='ignore')
        # Basic check for minimal HTML content
        if not html_content or len(html_content) < 150 or not ("<html" in html_content.lower() or "<body" in html_content.lower()):
            logging.warning(f"File is empty or lacks basic HTML structure: {file_path}"); return None
        return extract_recipe_info(html_content, str(file_path))
    except FileNotFoundError: logging.error(f"File not found: {file_path}"); return None
    except Exception as e: logging.error(f"Error processing file {file_path}: {e}", exc_info=True); return None


# --- Saving Functions ---

DB_COLUMNS = ['title', 'description', 'ingredients', 'instructions', 'prep_time', 'cook_time', 'total_time', 'yield', 'author', 'image_url', 'source_file']

def save_to_sqlite(recipes: List[Dict[str, Any]], db_path: Path):
    """Saves the list of recipes to an SQLite database."""
    logging.info(f"Attempting to save {len(recipes)} recipes to SQLite DB: {db_path}")
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Use TEXT for all columns for simplicity; lists stored as JSON strings
        column_defs = ', '.join([f'"{col}" TEXT' for col in DB_COLUMNS])
        create_table_sql = f'CREATE TABLE IF NOT EXISTS recipes (id INTEGER PRIMARY KEY AUTOINCREMENT, {column_defs})'
        cursor.execute(create_table_sql)
        # Delete existing data before inserting new batch (optional, prevents duplicates on reruns)
        # cursor.execute('DELETE FROM recipes')
        placeholders = ', '.join(['?'] * len(DB_COLUMNS))
        insert_sql = f'INSERT INTO recipes ({", ".join(f"{col}" for col in DB_COLUMNS)}) VALUES ({placeholders})'
        rows_inserted = 0
        for recipe in recipes:
            values = []
            for col in DB_COLUMNS:
                value = recipe.get(col)
                # Convert lists to JSON strings for TEXT column
                if isinstance(value, list): values.append(json.dumps(value))
                else: values.append(value if value is not None else None)
            try: cursor.execute(insert_sql, tuple(values)); rows_inserted += 1
            except sqlite3.Error as insert_err: logging.error(f"SQLite insert failed for recipe '{recipe.get('title', 'N/A')}': {insert_err}", exc_info=True); logging.debug(f"Failed row data: {values}")
        conn.commit(); logging.info(f"Successfully saved {rows_inserted} recipes to SQLite DB: {db_path}")
    except sqlite3.Error as e: logging.error(f"SQLite error: {e}", exc_info=True); print(f"ERROR: Failed to save to SQLite DB {db_path}: {e}")
    finally:
        if conn: conn.close()

def save_to_csv(recipes: List[Dict[str, Any]], csv_path: Path, headers: List[str]):
    """Saves the list of recipes to a CSV file."""
    logging.info(f"Attempting to save {len(recipes)} recipes to CSV: {csv_path}")
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers, quoting=csv.QUOTE_MINIMAL, extrasaction='ignore') # Ignore extra fields in dict
            writer.writeheader(); rows_written = 0
            for recipe in recipes:
                row_data = {}
                for header in headers:
                    value = recipe.get(header)
                    # Convert lists to newline-separated strings for CSV readability
                    if isinstance(value, list): row_data[header] = '\n'.join(map(str, value))
                    else: row_data[header] = value if value is not None else ''
                try: writer.writerow(row_data); rows_written += 1
                except csv.Error as write_err: logging.error(f"CSV write failed for recipe '{recipe.get('title', 'N/A')}': {write_err}", exc_info=True); logging.debug(f"Failed row data: {row_data}")
            logging.info(f"Successfully saved {rows_written} recipes to CSV: {csv_path}")
    except IOError as e: logging.error(f"Failed to write CSV file {csv_path}: {e}", exc_info=True); print(f"ERROR: Failed to save data to CSV {csv_path}: {e}")
    except csv.Error as e: logging.error(f"CSV error: {e}", exc_info=True); print(f"ERROR: CSV writing error for {csv_path}: {e}")


# --- Main Execution ---

def main():
    input_dir = Path(INPUT_HTML_DIR); output_json_file = Path(OUTPUT_JSON_FILE); output_db_file = Path(OUTPUT_DB_FILE); output_csv_file = Path(OUTPUT_CSV_FILE)

    if not input_dir.is_dir():
        logging.error(f"Input directory not found or is not a directory: {input_dir.resolve()}"); print(f"ERROR: Input directory not found: {input_dir.resolve()}"); return

    logging.info(f"Starting recipe extraction from: {input_dir.resolve()}")
    logging.info(f"JSON output: {output_json_file.resolve()}")
    logging.info(f"SQLite DB output: {output_db_file.resolve()}")
    logging.info(f"CSV output: {output_csv_file.resolve()}")
    logging.info(f"Using up to {MAX_WORKERS} processes.")

    # Find HTML files, case-insensitive
    html_files = list(input_dir.rglob('*.[hH][tT][mM]')) + list(input_dir.rglob('*.[hH][tT][mM][lL]'))
    logging.info(f"Found {len(html_files)} HTML files to process.")
    if not html_files: logging.warning("No HTML files found in the input directory."); return

    all_recipes = []; files_processed = 0; files_successful = 0; files_skipped = 0; files_failed = 0; start_time = time.time()

    # Process files in parallel
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_file, file_path): file_path for file_path in html_files}
        processed_count = 0; total_files = len(futures)
        for future in as_completed(futures):
            file_path = futures[future]; processed_count += 1
            try:
                result = future.result(); files_processed += 1
                if result: all_recipes.append(result); files_successful += 1
                else: files_skipped += 1 # Skipped by validation/plausibility checks
            except Exception as e: files_failed += 1; logging.error(f"Unhandled exception processing {file_path}: {e}", exc_info=True) # Catch errors from process_file
            # Log progress periodically
            if processed_count % 50 == 0 or processed_count == total_files: logging.info(f"Progress: Processed {processed_count}/{total_files} files...")

    end_time = time.time(); duration = end_time - start_time

    # Log summary
    logging.info("-" * 30); logging.info(f"Extraction finished in {duration:.2f} seconds."); logging.info(f"Total files processed: {files_processed}"); logging.info(f"Successfully extracted recipes: {files_successful}"); logging.info(f"Files skipped (missing data/implausible/not recipe): {files_skipped}"); logging.info(f"Files failed (errors during processing): {files_failed}"); logging.info("-" * 30)

    # Save results if any recipes were extracted
    if all_recipes:
        all_recipes.sort(key=lambda x: x.get('title', '').lower()) # Sort by title

        # Save JSON
        try:
            output_json_file.write_text(json.dumps(all_recipes, indent=4, ensure_ascii=False), encoding='utf-8')
            logging.info(f"Recipe database saved to JSON: {output_json_file.resolve()}")
        except IOError as e: logging.error(f"Failed to write JSON file {output_json_file.resolve()}: {e}", exc_info=True); print(f"ERROR: Failed to write JSON: {e}")

        # Save SQLite
        save_to_sqlite(all_recipes, output_db_file)

        # Save CSV
        save_to_csv(all_recipes, output_csv_file, DB_COLUMNS)

    else:
        logging.warning("No recipes were successfully extracted and validated. No output files created.")
        print("WARNING: No recipes were successfully extracted. No output files created.")

if __name__ == "__main__":
    # Ensure input directory exists before starting
    if not Path(INPUT_HTML_DIR).is_dir():
         print(f"ERROR: Input directory not found: {Path(INPUT_HTML_DIR).resolve()}")
         print("Please correct the INPUT_HTML_DIR variable in the script.")
    else:
         main()
