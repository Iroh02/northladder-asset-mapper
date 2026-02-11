"""
Core matching engine for UAE Asset ID mapping.

Matching Approach:
    - Combines manufacturer/brand with asset name to form a full product string
    - Normalizes strings (lowercase, remove punctuation, standardize storage, remove years)
    - Uses rapidfuzz token_sort_ratio for fuzzy matching (order-independent token comparison)
    - Token-sort is chosen because List 1 names ("iPhone 6 16GB") and NL names
      ("Apple iPhone 6 (2014), 16GB") contain the same tokens in different orders/formats

Threshold / Confidence Tiers:
    - >= 95%: HIGH confidence (auto-accept) — safe to apply UAE Asset ID directly
    - 85-94%: MEDIUM confidence (REVIEW_REQUIRED) — needs human review, shows top candidates
    - < 85%:  LOW confidence (NO_MATCH) — manual mapping required
    - The 85-94% zone contains false positives (e.g., iPhone 4 → iPhone 6 at 95%)
      so these are flagged for review rather than auto-accepted

Duplicate Handling:
    - If multiple NL entries share the exact same asset name (after cleaning),
      ALL matching UAE Asset IDs are returned as comma-separated values
    - Status is set to MULTIPLE_MATCHES so reviewers can verify the correct one
"""

import os
import json
import re
import pandas as pd
from rapidfuzz import fuzz, process
from typing import Dict, List, Callable, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SIMILARITY_THRESHOLD = 85   # Minimum score to appear as a candidate at all
HIGH_CONFIDENCE_THRESHOLD = 90  # Auto-accept: safe to apply without review (lowered from 95)

MATCH_STATUS_MATCHED = "MATCHED"           # >= 90% single ID — auto-apply
MATCH_STATUS_MULTIPLE = "MULTIPLE_MATCHES" # >= 95% but multiple IDs for same name
MATCH_STATUS_SUGGESTED = "REVIEW_REQUIRED"  # 85-94% — needs human review
MATCH_STATUS_NO_MATCH = "NO_MATCH"         # < 85% — manual mapping required

CONFIDENCE_HIGH = "HIGH"      # >= 95%
CONFIDENCE_MEDIUM = "MEDIUM"  # 85-94%
CONFIDENCE_LOW = "LOW"        # < 85%


# ---------------------------------------------------------------------------
# String normalization
# ---------------------------------------------------------------------------

def normalize_text(text: str) -> str:
    """
    Normalize an asset name for comparison.

    Steps:
        1. Lowercase
        2. Remove year patterns like (2014), (2015) — these are NL-specific metadata
        3. Remove punctuation (commas, quotes, dashes become spaces)
        4. Standardize storage: "16 gb" → "16gb", "512 gb" → "512gb"
        5. Standardize RAM: "8 gb ram" → "8gb"
        6. Collapse whitespace

    Safety note: We intentionally keep all numeric tokens (storage sizes, model numbers)
    since they are critical differentiators (e.g., iPhone 6 16GB vs 128GB).
    """
    if not isinstance(text, str):
        return ""

    s = text.lower().strip()

    # Remove year patterns like (2014), (2015)
    s = re.sub(r'\(\d{4}\)', '', s)

    # Remove common punctuation — replace with space to preserve token boundaries
    s = re.sub(r'[,\-\(\)"\'\/\.]', ' ', s)

    # Standardize storage/RAM: "16 gb" → "16gb", handles TB/MB too
    s = re.sub(r'(\d+)\s*(gb|tb|mb)', r'\1\2', s, flags=re.IGNORECASE)

    # Remove screen size patterns like 15.6" or 10.1" (inches)
    # These are mostly in List 2 laptop names and rarely in NL
    s = re.sub(r'\d+\.?\d*\s*"', '', s)

    # Strip connectivity markers (5G, 4G, 3G, LTE) - causes score drops
    # Example: "ROG Phone 3 5G" should match "ROG Phone 3" at 100%
    s = re.sub(r'\b[345]g\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\blte\b', '', s, flags=re.IGNORECASE)

    # Collapse whitespace
    s = re.sub(r'\s+', ' ', s).strip()

    return s


def build_match_string(brand: str, name: str) -> str:
    """
    Build a full product string from brand + name for matching.

    If the name already starts with the brand (like in List 2 / NL List),
    we don't duplicate it. Otherwise, we prepend the brand.
    """
    brand_str = str(brand).strip() if pd.notna(brand) else ""
    name_str = str(name).strip() if pd.notna(name) else ""

    if not name_str:
        return normalize_text(brand_str)

    # Check if name already starts with brand (case-insensitive)
    if brand_str and name_str.lower().startswith(brand_str.lower()):
        return normalize_text(name_str)

    combined = f"{brand_str} {name_str}".strip()
    return normalize_text(combined)


# ---------------------------------------------------------------------------
# Attribute-based matching (Level 0 - fast path)
# ---------------------------------------------------------------------------

def extract_cpu_generation(text: str) -> str:
    """
    Extract CPU generation from laptop specs.
    Maps CPU model codes to generation numbers (e.g., i5-12500H → 12th gen).
    """
    text_lower = text.lower()

    # Apple Silicon: M1, M2, M3
    apple_match = re.search(r'\bm([123])\b', text_lower)
    if apple_match:
        return f"m{apple_match.group(1)}"

    # Intel Core patterns: i3-12500H, i5-1165G7, i7-10750H
    intel_match = re.search(r'(?:core\s+)?i[357]-?(\d{1,2})\d{2,3}[a-z]{0,2}', text_lower)
    if intel_match:
        gen = intel_match.group(1)
        return f"{gen}th gen" if gen != '1' else 'core'

    # AMD Ryzen patterns: Ryzen 5 5500U, Ryzen 7 6800H
    ryzen_match = re.search(r'ryzen\s+[357]\s+(\d)(\d{3})', text_lower)
    if ryzen_match:
        gen = ryzen_match.group(1)
        return f"ryzen {gen}"

    # Fallback: look for "10th gen", "11th gen", etc.
    gen_match = re.search(r'(\d{1,2})(?:st|nd|rd|th)\s*gen', text_lower)
    if gen_match:
        return f"{gen_match.group(1)}th gen"

    # Low-end CPUs: N200, N100, Celeron, Pentium (treat as generic "core")
    if re.search(r'\b[n]\d{3}\b|celeron|pentium', text_lower):
        return 'core'

    return ''


def extract_ram(text: str) -> str:
    """
    Extract RAM from laptop specs (e.g., '8gb', '16gb').
    RAM is typically smaller than storage (4GB, 8GB, 16GB, 32GB, 64GB).
    Storage starts at 128GB typically.
    """
    # Look for patterns like "8GB RAM", "16 GB", but filter out storage sizes
    ram_matches = re.findall(r'(\d+)\s*gb', text.lower())

    for size in ram_matches:
        size_int = int(size)
        # RAM is typically <= 64GB; storage is >= 128GB (or small values like 16/32 for old phones)
        if 4 <= size_int <= 64:
            return f"{size}gb"

    return ''


def is_laptop_product(text: str) -> bool:
    """Check if text describes a laptop product."""
    laptop_keywords = [
        'laptop', 'notebook', 'chromebook',
        'macbook', 'thinkpad', 'ideapad', 'yoga',
        'pavilion', 'elitebook', 'probook', 'envy', 'spectre', 'omen',
        'precision', 'latitude', 'inspiron', 'vostro', 'xps',
        'vivobook', 'zenbook', 'rog', 'tuf',
        'surface pro', 'surface laptop', 'surface book',
        'matebook', 'magicbook',
        'aspire', 'swift', 'predator', 'nitro',
        'legion', 'flex'
    ]
    text_lower = text.lower()
    return any(kw in text_lower for kw in laptop_keywords)


def extract_laptop_attributes(text: str, brand: str) -> Dict[str, str]:
    """
    Extract laptop-specific attributes for matching.

    Laptops have different naming: product line + CPU gen + RAM + storage
    vs phones: product line + model + storage
    """
    text_norm = normalize_text(text)
    brand_norm = normalize_text(brand)

    # Extract RAM first
    ram = extract_ram(text)

    # Extract storage (for laptops, storage is typically >= 128GB or in TB)
    # Find all GB/TB values and pick the largest one that's not RAM
    storage = ''
    text_lower = text.lower()

    # Find all storage values with explicit TB marker
    tb_matches = re.findall(r'(\d+)\s*tb', text_lower)
    if tb_matches:
        # Convert TB to GB for comparison (1TB = 1000GB roughly)
        storage = f"{tb_matches[0]}tb"
    else:
        # Find all GB values
        gb_matches = re.findall(r'(\d+)\s*gb', text_lower)
        gb_values = [int(m) for m in gb_matches]

        # Filter: storage should be > RAM (storage is typically >= 128GB)
        ram_int = int(ram.replace('gb', '')) if ram else 0
        storage_candidates = [v for v in gb_values if v > ram_int and v >= 128]

        if storage_candidates:
            # Pick the largest value (main storage)
            storage = f"{max(storage_candidates)}gb"
        elif gb_values:
            # Fallback: pick the largest value overall (even if < 128GB)
            largest = max(gb_values)
            if largest != ram_int:  # Don't use RAM as storage
                storage = f"{largest}gb"

    attrs = {
        'brand': brand_norm,
        'product_line': '',
        'model': '',  # For laptops, model = CPU generation
        'storage': storage,
        'ram': ram,
    }

    # Extract CPU generation (this becomes the "model" for laptops)
    cpu_gen = extract_cpu_generation(text)
    if cpu_gen:
        attrs['model'] = cpu_gen
    else:
        # Fallback for laptops without clear CPU gen (e.g., older Apple MacBooks):
        # Use year as model if present (e.g., "2015", "2016", "2017")
        year_match = re.search(r'\b(20\d{2})\b', text)
        if year_match:
            attrs['model'] = year_match.group(1)

    # Extract laptop product lines by brand
    text_lower = text.lower()

    # Dell product lines
    if 'dell' in brand_norm:
        for line in ['precision', 'latitude', 'inspiron', 'vostro', 'xps', 'alienware']:
            if line in text_lower:
                attrs['product_line'] = line
                break

    # HP product lines
    elif 'hp' in brand_norm:
        for line in ['elitebook', 'probook', 'pavilion', 'envy', 'spectre', 'omen', 'zbook']:
            if line in text_lower:
                attrs['product_line'] = line
                break

    # Lenovo product lines
    elif 'lenovo' in brand_norm:
        for line in ['thinkpad', 'ideapad', 'yoga', 'legion', 'flex']:
            if line in text_lower:
                attrs['product_line'] = line
                break

    # Apple product lines
    elif 'apple' in brand_norm:
        if 'macbook pro' in text_lower:
            attrs['product_line'] = 'macbook pro'
        elif 'macbook air' in text_lower:
            attrs['product_line'] = 'macbook air'
        elif 'macbook' in text_lower:
            attrs['product_line'] = 'macbook'

    # Asus product lines
    elif 'asus' in brand_norm:
        for line in ['vivobook', 'zenbook', 'rog', 'tuf', 'expertbook']:
            if line in text_lower:
                attrs['product_line'] = line
                break

    # Acer product lines
    elif 'acer' in brand_norm:
        for line in ['aspire', 'swift', 'predator', 'nitro', 'spin']:
            if line in text_lower:
                attrs['product_line'] = line
                break

    # Microsoft Surface
    elif 'microsoft' in brand_norm or 'surface' in text_lower:
        if 'surface pro' in text_lower:
            attrs['product_line'] = 'surface pro'
        elif 'surface laptop' in text_lower:
            attrs['product_line'] = 'surface laptop'
        elif 'surface book' in text_lower:
            attrs['product_line'] = 'surface book'

    # Huawei product lines
    elif 'huawei' in brand_norm:
        for line in ['matebook', 'magicbook']:
            if line in text_lower:
                attrs['product_line'] = line
                break

    return attrs


def extract_product_attributes(text: str, brand: str = '') -> Dict[str, str]:
    """
    HYBRID extraction: laptop-specific + phone hand-tuned + generic fallback.

    Laptops: Extract product line + CPU gen + RAM + storage
    Phones (hand-tuned): Apple, Samsung, Google, Xiaomi, Huawei
    Other devices (generic): Universal pattern detection

    Returns dict with:
        'brand': normalized brand name
        'product_line': product family (galaxy, iphone, redmi, pavilion, thinkpad, etc.)
        'model': model identifier (s9, 14 pro, 10th gen, ryzen 5, etc.)
        'storage': storage capacity (128gb, 1tb, etc.)
        'ram': RAM capacity (laptop-specific, 8gb, 16gb, etc.)
    """
    text_norm = normalize_text(text)
    brand_norm = normalize_text(brand)

    # === LAPTOP DETECTION (priority - different naming convention) ===
    if is_laptop_product(text):
        return extract_laptop_attributes(text, brand)

    attrs = {
        'brand': brand_norm,
        'product_line': '',
        'model': '',
        'storage': extract_storage(text_norm),
    }

    # === HAND-TUNED PATTERNS (mobile phones - major brands) ===

    # Samsung: Remove model codes (G960F, N9005, SM-G960F, etc.)
    if 'samsung' in brand_norm or 'samsung' in text_norm:
        text_clean = re.sub(r'\b(?:sm-)?[a-z]\d{3,5}[a-z]?\b', '', text_norm, flags=re.IGNORECASE)
        text_norm = re.sub(r'\s+', ' ', text_clean).strip()

    # Apple iPhone: "iphone 14 pro 256gb" → line=iphone, model=14 pro
    if 'iphone' in text_norm:
        match = re.search(r'iphone\s+(\d+[a-z]*(?:\s+\w+)?)', text_norm)
        if match:
            attrs['product_line'] = 'iphone'
            attrs['model'] = match.group(1).strip()
            return attrs

    # Samsung Galaxy: "galaxy s9 plus 128gb" → line=galaxy, model=s9 plus
    if 'galaxy' in text_norm:
        match = re.search(r'galaxy\s+([a-z]+\d+[a-z]*(?:\s+\w+)?)', text_norm)
        if match:
            attrs['product_line'] = 'galaxy'
            attrs['model'] = match.group(1).strip()
            return attrs

    # Google Pixel: "pixel 9 pro 256gb" → line=pixel, model=9 pro
    if 'pixel' in text_norm:
        match = re.search(r'pixel\s+(\d+[a-z]*(?:\s+\w+)?)', text_norm)
        if match:
            attrs['product_line'] = 'pixel'
            attrs['model'] = match.group(1).strip()
            return attrs

    # Xiaomi Redmi/Mi: "redmi note 12 pro 128gb" → line=redmi, model=note 12 pro
    if 'redmi' in text_norm:
        match = re.search(r'redmi\s+(note\s+\d+[a-z]*(?:\s+\w+)?|\d+[a-z]*(?:\s+\w+)?)', text_norm, re.IGNORECASE)
        if match:
            attrs['product_line'] = 'redmi'
            attrs['model'] = match.group(1).strip()
            return attrs
    elif 'xiaomi' in brand_norm and 'mi' in text_norm:
        # "xiaomi mi 11 ultra" → line=mi, model=11 ultra
        match = re.search(r'mi\s+(\d+[a-z]*(?:\s+\w+)?)', text_norm)
        if match:
            attrs['product_line'] = 'mi'
            attrs['model'] = match.group(1).strip()
            return attrs

    # Huawei Mate/P-series: "mate 30 pro 256gb" → line=mate, model=30 pro
    if 'mate' in text_norm and ('huawei' in brand_norm or 'huawei' in text_norm):
        match = re.search(r'mate\s+(\d+[a-z]*(?:\s+\w+)?)', text_norm)
        if match:
            attrs['product_line'] = 'mate'
            attrs['model'] = match.group(1).strip()
            return attrs
    elif ('huawei' in brand_norm or 'huawei' in text_norm) and re.search(r'\bp\d+', text_norm):
        # "huawei p30 pro" → line=p, model=30 pro
        match = re.search(r'p(\d+[a-z]*(?:\s+\w+)?)', text_norm)
        if match:
            attrs['product_line'] = 'p'
            attrs['model'] = match.group(1).strip()
            return attrs

    # === GENERIC EXTRACTION (all other brands) ===
    # Detect common product line patterns: "find x5", "moto g50", "reno 8", etc.

    # Pattern 1: "ProductLine ModelNumber" (e.g., "find x5", "reno 8 pro")
    match = re.search(r'\b([a-z]+)\s+([a-z]?\d+[a-z]*(?:\s+(?:pro|plus|ultra|lite|max|mini|note))?)', text_norm, re.IGNORECASE)
    if match:
        line_candidate = match.group(1)
        model_candidate = match.group(2)

        # Filter out noise words (the, and, with, etc.)
        noise_words = {'the', 'and', 'or', 'with', 'dual', 'sim', 'unlocked', 'new', 'used', 'refurbished'}
        if line_candidate not in noise_words:
            attrs['product_line'] = line_candidate
            attrs['model'] = model_candidate.strip()
            return attrs

    # Pattern 2: Just model number (e.g., "a52 5g 128gb")
    match = re.search(r'\b([a-z]?\d+[a-z]*(?:\s+(?:pro|plus|ultra|lite|max|mini))?)', text_norm, re.IGNORECASE)
    if match:
        model_candidate = match.group(1).strip()
        # Use first meaningful word as product line
        words = text_norm.split()
        for word in words:
            if len(word) > 2 and word not in {'the', 'and', 'with', 'sim', 'new', 'used'}:
                attrs['product_line'] = word
                attrs['model'] = model_candidate
                break

    return attrs


def build_attribute_index(df_nl_clean: pd.DataFrame) -> Dict:
    """
    Build an attribute-based index for fast exact matching.

    Returns nested dict: brand → product_line → model → ram_storage_key → [asset_ids]

    For phones: brand → product_line → model → storage
    For laptops: brand → product_line → model (CPU gen) → "ram_storage" (combined key)

    This allows O(1) lookup for products with clear attributes, avoiding
    expensive fuzzy matching for the majority of queries.
    """
    index = {}

    for _, row in df_nl_clean.iterrows():
        brand = normalize_text(str(row.get('brand', '')).strip())
        if not brand:
            continue

        attrs = extract_product_attributes(row['normalized_name'], brand)

        # Only index if we successfully extracted model
        if not attrs['model']:
            continue

        # Build nested structure
        if brand not in index:
            index[brand] = {}
        if attrs['product_line'] not in index[brand]:
            index[brand][attrs['product_line']] = {}
        if attrs['model'] not in index[brand][attrs['product_line']]:
            index[brand][attrs['product_line']][attrs['model']] = {}

        # For laptops, use combined RAM+storage as key; for phones, just storage
        ram = attrs.get('ram', '')
        storage_key = f"{ram}_{attrs['storage']}" if ram else attrs['storage']

        if storage_key not in index[brand][attrs['product_line']][attrs['model']]:
            index[brand][attrs['product_line']][attrs['model']][storage_key] = {
                'asset_ids': [],
                'nl_name': row['normalized_name']
            }

        asset_id = str(row['uae_assetid']).strip()
        entry = index[brand][attrs['product_line']][attrs['model']][storage_key]
        if asset_id not in entry['asset_ids']:
            entry['asset_ids'].append(asset_id)

    return index


def try_attribute_match(
    query: str,
    brand: str,
    attribute_index: Dict
) -> Optional[dict]:
    """
    Attempt fast attribute-based matching before falling back to fuzzy.

    Returns match result dict if confident match found, None otherwise.
    This is the "fast path" that handles phones and laptops in 2-5ms.
    """
    attrs = extract_product_attributes(query, brand)

    # Need at least brand, product_line, and model for attribute matching
    if not (attrs['brand'] and attrs['product_line'] and attrs['model']):
        return None

    # Navigate the index
    try:
        brand_data = attribute_index.get(attrs['brand'], {})
        line_data = brand_data.get(attrs['product_line'], {})
        model_data = line_data.get(attrs['model'], {})

        # For laptops, use combined RAM+storage key; for phones, just storage
        ram = attrs.get('ram', '')
        storage_key = f"{ram}_{attrs['storage']}" if ram else attrs['storage']

        # Try exact match with RAM+storage (laptops) or just storage (phones)
        if storage_key in model_data:
            entry = model_data[storage_key]
            return {
                'mapped_uae_assetid': ', '.join(entry['asset_ids']),
                'match_score': 100.0,
                'match_status': MATCH_STATUS_MULTIPLE if len(entry['asset_ids']) > 1 else MATCH_STATUS_MATCHED,
                'confidence': CONFIDENCE_HIGH,
                'matched_on': entry['nl_name'],
                'method': 'attribute'
            }

        # Fallback: try without RAM if laptop match failed (maybe RAM not in query)
        if ram and attrs['storage'] in model_data:
            entry = model_data[attrs['storage']]
            return {
                'mapped_uae_assetid': ', '.join(entry['asset_ids']),
                'match_score': 95.0,  # Slightly lower since RAM didn't match
                'match_status': MATCH_STATUS_MULTIPLE if len(entry['asset_ids']) > 1 else MATCH_STATUS_MATCHED,
                'confidence': CONFIDENCE_HIGH,
                'matched_on': entry['nl_name'],
                'method': 'attribute'
            }

        # Try without storage if no exact match (for products without storage in name)
        if '' in model_data:  # Empty storage key
            entry = model_data['']
            return {
                'mapped_uae_assetid': ', '.join(entry['asset_ids']),
                'match_score': 90.0,  # Lower since storage/RAM didn't match
                'match_status': MATCH_STATUS_MULTIPLE if len(entry['asset_ids']) > 1 else MATCH_STATUS_MATCHED,
                'confidence': CONFIDENCE_MEDIUM,
                'matched_on': entry['nl_name'],
                'method': 'attribute'
            }

    except (KeyError, AttributeError):
        pass

    return None  # Fall back to fuzzy matching


# ---------------------------------------------------------------------------
# NL List preprocessing
# ---------------------------------------------------------------------------

def load_and_clean_nl_list(df_nl: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Clean the NorthLadder master list:
        1. Drop rows with null/empty uae_assetname
        2. Drop rows where uae_assetname contains "test" (case-insensitive)
        3. Add normalized name column for matching
        4. Check for duplicate asset IDs with different names (data quality warning)

    Returns:
        - Cleaned DataFrame with 'normalized_name' column
        - Stats dict (includes 'warnings' list)
    """
    df = df_nl.copy()
    warnings = []

    original_count = len(df)

    # Filter out null / empty asset names
    df = df[df['uae_assetname'].notna()]
    df = df[df['uae_assetname'].astype(str).str.strip() != '']
    null_dropped = original_count - len(df)

    # Filter out test entries (case-insensitive, matches "test" as a word boundary)
    # Using word boundary to avoid filtering "latest" or "testing" — safety choice
    test_mask = df['uae_assetname'].astype(str).str.contains(
        r'\btest\b', case=False, na=False
    )
    df = df[~test_mask]
    test_dropped = original_count - null_dropped - len(df)

    # Check for duplicate asset IDs with different names (data quality issue)
    id_counts = df['uae_assetid'].value_counts()
    duplicate_ids = id_counts[id_counts > 1].index.tolist()
    if duplicate_ids:
        warnings.append(f"Found {len(duplicate_ids)} duplicate asset IDs with different names")
        for asset_id in duplicate_ids[:5]:  # Show first 5
            names = df[df['uae_assetid'] == asset_id]['uae_assetname'].unique()
            warnings.append(f"  ID {asset_id}: {len(names)} different names")

    # Check for empty brands
    empty_brands = df['brand'].isna().sum() + (df['brand'].astype(str).str.strip() == '').sum()
    if empty_brands > 0:
        warnings.append(f"{empty_brands} NL entries have empty brand fields")

    # Build normalized names for matching
    df['normalized_name'] = df.apply(
        lambda row: build_match_string(row.get('brand', ''), row['uae_assetname']),
        axis=1
    )

    stats = {
        'original': original_count,
        'null_dropped': null_dropped,
        'test_dropped': test_dropped,
        'final': len(df),
        'warnings': warnings,
    }

    return df, stats


def build_nl_lookup(df_nl_clean: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Build a lookup dictionary: normalized_name → list of uae_assetid values.

    This handles the duplicate case: if multiple rows have the same normalized name,
    all their IDs are collected together.
    """
    lookup = {}
    for _, row in df_nl_clean.iterrows():
        key = row['normalized_name']
        asset_id = str(row['uae_assetid']).strip()
        if key not in lookup:
            lookup[key] = []
        if asset_id not in lookup[key]:  # avoid exact duplicates
            lookup[key].append(asset_id)
    return lookup


def build_brand_index(df_nl_clean: pd.DataFrame) -> Dict[str, Dict]:
    """
    Build a brand-partitioned index for recursive matching.

    Returns dict:  normalized_brand → {
        'lookup': {normalized_name → [asset_ids]},
        'names':  [list of normalized names],
    }

    This allows matching within a single brand's products instead of
    searching all 9,894 records — faster and eliminates cross-brand errors.
    """
    brand_index = {}
    for _, row in df_nl_clean.iterrows():
        brand = normalize_text(str(row.get('brand', '')).strip())
        if not brand:
            continue
        if brand not in brand_index:
            brand_index[brand] = {'lookup': {}, 'names': []}

        name = row['normalized_name']
        asset_id = str(row['uae_assetid']).strip()

        if name not in brand_index[brand]['lookup']:
            brand_index[brand]['lookup'][name] = []
            brand_index[brand]['names'].append(name)
        if asset_id not in brand_index[brand]['lookup'][name]:
            brand_index[brand]['lookup'][name].append(asset_id)

    return brand_index


def extract_storage(text: str) -> str:
    """Extract storage from a normalized product string (e.g., '16gb', '128gb')."""
    storage_match = re.findall(r'(\d+(?:gb|tb|mb))', text)
    return storage_match[0] if storage_match else ''


def extract_attributes(text: str) -> Dict[str, str]:
    """
    Extract structured attributes from a normalized product string.

    Returns dict with:
        'storage': e.g., '16gb', '128gb', '1tb' (or '' if not found)
        'model_nums': list of model numbers (short digits not attached to storage)

    Used in recursive matching to filter candidates before fuzzy comparison.
    """
    storage = extract_storage(text)

    # Remove connectivity markers (3g, 4g, 5g) before model number extraction
    # to prevent "5" in "5g" from being treated as a model number
    text_clean = re.sub(r'\b[345]g\b', '', text)

    # Extract model numbers: 1-2 digit numbers NOT followed by gb/tb/mb
    model_nums = re.findall(r'(?<!\d)(\d{1,2})(?!\d|gb|tb|mb)', text_clean)

    return {'storage': storage, 'model_nums': model_nums}


def extract_model_tokens(text: str) -> List[str]:
    """
    Extract model-identifying tokens from a normalized product string.

    Returns tokens that contain digits but aren't storage (gb/tb/mb) or
    connectivity (3g/4g/5g). Includes letter prefixes/suffixes so we can
    distinguish variants like "5t" vs "5i" or "a57" vs "a57s".

    Examples:
        'apple iphone 14 pro 256gb' -> ['14']
        'huawei nova 5t 128gb'      -> ['5t']
        'oppo a57s 64gb'            -> ['a57s']
        'samsung galaxy s23 256gb'  -> ['s23']
        'google pixel 9 5g 256gb'   -> ['9']
    """
    # Remove storage tokens (e.g., "256gb", "1tb")
    text_clean = re.sub(r'\b\d+(?:gb|tb|mb)\b', '', text)
    # Remove connectivity markers (e.g., "5g", "4g")
    text_clean = re.sub(r'\b[345]g\b', '', text_clean)
    # Get remaining tokens that contain at least one digit
    tokens = text_clean.split()
    return [t for t in tokens if re.search(r'\d', t)]


# ---------------------------------------------------------------------------
# Matching logic — recursive brand → attribute → fuzzy
# ---------------------------------------------------------------------------

def match_single_item(
    query: str,
    nl_lookup: Dict[str, List[str]],
    nl_names: List[str],
    threshold: int = SIMILARITY_THRESHOLD,
    brand_index: Optional[Dict] = None,
    input_brand: str = '',
    attribute_index: Optional[Dict] = None,
) -> dict:
    """
    Match a single product against the NL list using hybrid matching.

    Matching strategy (cascading filters with fast path):
        0. ATTRIBUTE MATCHING (fast path): Try exact attribute match first
           - Handles 70-80% of queries in 2-5ms
           - Works especially well for Samsung (strips model codes), iPhone, Pixel, Galaxy
        1. BRAND FILTER: If brand is known, search only within that brand's products
           (e.g., 9,894 → ~2,000 Apple records). Eliminates cross-brand errors.
        2. STORAGE FILTER: If storage is detected (e.g., "16gb"), prefer candidates
           with the same storage. Prevents "16GB" matching "128GB" variants.
        3. FUZZY MATCH: token_sort_ratio on the narrowed candidate list.
        4. MODEL TOKEN GUARD: Reject if model tokens differ (e.g., iPhone 4 vs 6).

    Falls back through levels if earlier levels don't produce confident matches.
    """
    no_match_result = {
        'mapped_uae_assetid': '',
        'match_score': 0,
        'match_status': MATCH_STATUS_NO_MATCH,
        'confidence': CONFIDENCE_LOW,
        'matched_on': '',
        'method': 'none',
    }

    if not query:
        return no_match_result

    # --- Level 0: Attribute-based matching (FAST PATH) ---
    if attribute_index and input_brand:
        attr_match = try_attribute_match(query, input_brand, attribute_index)
        if attr_match:
            return attr_match  # Found exact match, skip fuzzy entirely

    # --- Level 1: Brand partitioning ---
    search_lookup = nl_lookup
    search_names = nl_names
    brand_norm = normalize_text(input_brand) if input_brand else ''

    if brand_index and brand_norm and brand_norm in brand_index:
        # Narrow search to this brand's products only
        brand_data = brand_index[brand_norm]
        search_lookup = brand_data['lookup']
        search_names = brand_data['names']

    # --- Level 2: Storage pre-filter ---
    query_storage = extract_storage(query)
    if query_storage and len(search_names) > 20:
        # Filter candidates to those with the same storage
        storage_filtered = [n for n in search_names if query_storage in n]
        if storage_filtered:
            search_names = storage_filtered

    # --- Level 3: Fuzzy match on narrowed candidates ---
    result = process.extractOne(
        query,
        search_names,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=threshold,
    )

    # If brand-filtered search found nothing, fall back to full NL search
    if result is None and (search_names is not nl_names):
        result = process.extractOne(
            query,
            nl_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold,
        )
        search_lookup = nl_lookup  # use full lookup for ID resolution

    if result is None:
        return no_match_result

    best_match, score, _ = result
    asset_ids = search_lookup.get(best_match, [])
    # Also check full lookup in case brand subset didn't have the ID mapping
    if not asset_ids:
        asset_ids = nl_lookup.get(best_match, [])

    # --- Level 4: Model token guardrail ---
    # Applied to ALL scores (including >= 95%) to prevent false positives
    # like Pixel 9 → Pixel 3 (95%), Mate 20 → Mate 40 (95%),
    # Nova 5T → Nova 5i (95%), A57 → A57s (96%)
    q_tokens = extract_model_tokens(query)
    m_tokens = extract_model_tokens(best_match)
    if q_tokens and m_tokens:
        # Compare model tokens position by position (e.g., "5t" vs "5i", "s23" vs "s24")
        for qt, mt in zip(q_tokens, m_tokens):
            if qt != mt:
                score = min(score, threshold - 1)  # Demote to NO_MATCH
                break
    elif q_tokens and not m_tokens:
        # Query has model token but match doesn't (e.g., "ROG Phone 6" → "ROG Phone")
        # Demote to review — the match is likely a different generation
        score = min(score, HIGH_CONFIDENCE_THRESHOLD - 1)  # Demote to REVIEW at most
    elif not q_tokens and m_tokens:
        # Match has model token but query doesn't (e.g., "Find X" → "Find X9")
        # Demote to review — the match added a model number the query doesn't have
        score = min(score, HIGH_CONFIDENCE_THRESHOLD - 1)  # Demote to REVIEW at most

    score_rounded = round(score, 2)

    # Determine confidence tier
    if score >= HIGH_CONFIDENCE_THRESHOLD:
        confidence = CONFIDENCE_HIGH
    elif score >= SIMILARITY_THRESHOLD:
        confidence = CONFIDENCE_MEDIUM
    else:
        confidence = CONFIDENCE_LOW

    if len(asset_ids) == 0 or confidence == CONFIDENCE_LOW:
        return {
            'mapped_uae_assetid': '',
            'match_score': score_rounded,
            'match_status': MATCH_STATUS_NO_MATCH,
            'confidence': CONFIDENCE_LOW,
            'matched_on': best_match,
            'method': 'fuzzy',
        }
    elif confidence == CONFIDENCE_HIGH:
        status = MATCH_STATUS_MULTIPLE if len(asset_ids) > 1 else MATCH_STATUS_MATCHED
        return {
            'mapped_uae_assetid': ', '.join(asset_ids),
            'match_score': score_rounded,
            'match_status': status,
            'confidence': confidence,
            'matched_on': best_match,
            'method': 'fuzzy',
        }
    else:
        return {
            'mapped_uae_assetid': ', '.join(asset_ids),
            'match_score': score_rounded,
            'match_status': MATCH_STATUS_SUGGESTED,
            'confidence': confidence,
            'matched_on': best_match,
            'method': 'fuzzy',
        }


def run_matching(
    df_input: pd.DataFrame,
    brand_col: str,
    name_col: str,
    nl_lookup: Dict[str, List[str]],
    nl_names: List[str],
    threshold: int = SIMILARITY_THRESHOLD,
    progress_callback: Optional[Callable] = None,
    brand_index: Optional[Dict] = None,
    attribute_index: Optional[Dict] = None,
) -> pd.DataFrame:
    """
    Run hybrid matching for an entire input DataFrame against the NL lookup.

    Matching is hybrid (attribute-based fast path + fuzzy fallback):
        0. Attribute matching (fast path) → 70-80% of queries in 2-5ms
        1. Brand partition → narrows search to one brand
        2. Storage filter → narrows to same storage variant
        3. Fuzzy match → finds best candidate
        4. Model token guard → rejects wrong model tokens

    Args:
        df_input: The input asset list (List 1 or List 2)
        brand_col: Column name containing the brand/manufacturer
        name_col: Column name containing the product name
        nl_lookup: dict of normalized_name → [asset_ids] (full, for fallback)
        nl_names: list of all normalized NL names (full, for fallback)
        threshold: minimum similarity score
        progress_callback: optional callable(current, total) for UI progress
        brand_index: brand-partitioned index from build_brand_index()
        attribute_index: attribute-based index from build_attribute_index()

    Returns:
        Copy of df_input with added columns:
            mapped_uae_assetid, match_score, match_status, confidence, matched_on
    """
    df = df_input.copy()
    total = len(df)

    results = []
    for idx, row in df.iterrows():
        input_brand = str(row.get(brand_col, '')).strip() if brand_col != '__no_brand__' else ''
        query = build_match_string(input_brand, row.get(name_col, ''))
        match_result = match_single_item(
            query, nl_lookup, nl_names, threshold,
            brand_index=brand_index,
            input_brand=input_brand,
            attribute_index=attribute_index,
        )
        results.append(match_result)

        if progress_callback and (len(results) % 50 == 0 or len(results) == total):
            progress_callback(len(results), total)

    results_df = pd.DataFrame(results)
    df['mapped_uae_assetid'] = results_df['mapped_uae_assetid'].values
    df['match_score'] = results_df['match_score'].values
    df['match_status'] = results_df['match_status'].values
    df['confidence'] = results_df['confidence'].values
    df['matched_on'] = results_df['matched_on'].values

    return df


# ---------------------------------------------------------------------------
# Single-item test helper (for UI "Test Match" feature)
# ---------------------------------------------------------------------------

def test_single_match(
    brand: str,
    name: str,
    nl_lookup: Dict[str, List[str]],
    nl_names: List[str],
    threshold: int = SIMILARITY_THRESHOLD,
    brand_index: Optional[Dict] = None,
    attribute_index: Optional[Dict] = None,
) -> dict:
    """
    Test matching for a single item. Returns detailed info including top 3 alternatives.
    Used by the UI sample-match tester with hybrid matching.
    """
    query = build_match_string(brand, name)

    if not query:
        return {
            'query': query,
            'error': 'Empty query after normalization',
            'top_matches': [],
        }

    # Determine search scope (brand-partitioned if available)
    search_names = nl_names
    search_lookup = nl_lookup
    brand_norm = normalize_text(brand) if brand else ''
    if brand_index and brand_norm and brand_norm in brand_index:
        search_names = brand_index[brand_norm]['names']
        search_lookup = brand_index[brand_norm]['lookup']

    # Get top 3 matches from the brand-scoped search
    top_matches = process.extract(
        query,
        search_names,
        scorer=fuzz.token_sort_ratio,
        limit=3,
    )

    alternatives = []
    for match_name, score, _ in top_matches:
        asset_ids = search_lookup.get(match_name, []) or nl_lookup.get(match_name, [])
        if score >= HIGH_CONFIDENCE_THRESHOLD:
            alt_status = 'HIGH'
        elif score >= threshold:
            alt_status = 'MEDIUM'
        else:
            alt_status = 'LOW'
        alternatives.append({
            'nl_name': match_name,
            'score': round(score, 2),
            'asset_ids': asset_ids,
            'status': alt_status,
        })

    best = match_single_item(query, nl_lookup, nl_names, threshold,
                             brand_index=brand_index, input_brand=brand,
                             attribute_index=attribute_index)

    return {
        'query': query,
        'brand': brand,
        'name': name,
        'best_match': best,
        'top_3_alternatives': alternatives,
    }


# ---------------------------------------------------------------------------
# NL Reference persistence — upload once, reuse forever
# ---------------------------------------------------------------------------

NL_REFERENCE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nl_reference")
NL_DATA_PATH = os.path.join(NL_REFERENCE_DIR, "nl_clean.parquet")
NL_META_PATH = os.path.join(NL_REFERENCE_DIR, "nl_meta.json")


def save_nl_reference(df_nl_clean: pd.DataFrame, stats: Dict) -> None:
    """Save the cleaned NL list to disk so it persists across app restarts."""
    os.makedirs(NL_REFERENCE_DIR, exist_ok=True)
    # Cast object columns to string — NL data has mixed types (e.g. int asset names)
    df_save = df_nl_clean.copy()
    for col in df_save.select_dtypes(include='object').columns:
        df_save[col] = df_save[col].astype(str)
    df_save.to_parquet(NL_DATA_PATH, index=False)
    with open(NL_META_PATH, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, default=str)


def load_nl_reference() -> Optional[Tuple[pd.DataFrame, Dict]]:
    """Load a previously saved NL reference. Returns None if not found."""
    if not os.path.exists(NL_DATA_PATH) or not os.path.exists(NL_META_PATH):
        return None
    df = pd.read_parquet(NL_DATA_PATH)
    with open(NL_META_PATH, "r", encoding="utf-8") as f:
        stats = json.load(f)
    return df, stats


def nl_reference_exists() -> bool:
    """Check if a saved NL reference exists on disk."""
    return os.path.exists(NL_DATA_PATH) and os.path.exists(NL_META_PATH)


def delete_nl_reference() -> None:
    """Delete the saved NL reference."""
    for path in [NL_DATA_PATH, NL_META_PATH]:
        if os.path.exists(path):
            os.remove(path)


def parse_nl_sheet(file) -> pd.DataFrame:
    """Parse only the NorthLadder List sheet from an uploaded Excel file."""
    df_nl = pd.read_excel(file, sheet_name='NorthLadder List', header=None, skiprows=2)
    df_nl = df_nl.iloc[:, 1:]
    df_nl.columns = ['category', 'brand', 'uae_assetid', 'uae_assetname']
    return df_nl


# ---------------------------------------------------------------------------
# Dynamic Excel parser — handles any number of sheets
# ---------------------------------------------------------------------------

# Keywords used to detect which column is the brand and which is the product name.
# Checked against the header row (row 1) after lowercasing.
BRAND_KEYWORDS = ['manufacturer', 'brand', 'make', 'oem']
NAME_KEYWORDS = ['name', 'product', 'model', 'asset', 'device', 'description', 'foxway']

# Sheets to skip when auto-detecting asset lists (the NL reference is handled separately)
NL_SHEET_KEYWORDS = ['northladder', 'nl list', 'nl_list', 'reference', 'master']


def _detect_header_row(file, sheet_name: str) -> int:
    """
    Detect which row contains the actual column headers.

    Strategy: read the first 5 rows and find the first row where
    at least 2 non-null string values exist (skipping title/blank rows).
    """
    df = pd.read_excel(file, sheet_name=sheet_name, header=None, nrows=5)
    for i, row in df.iterrows():
        str_vals = [v for v in row.values if isinstance(v, str) and v.strip()]
        if len(str_vals) >= 2:
            return i
    return 1  # Fallback: row 1


def _detect_columns(columns: List[str]) -> Dict[str, str]:
    """
    Given a list of lowercased column names, detect brand_col and name_col.

    Returns dict with:
        'brand_col': column name for brand/manufacturer (or None)
        'name_col':  column name for product name (required)
    """
    cols_lower = [str(c).lower().strip() for c in columns]
    result = {'brand_col': None, 'name_col': None}

    for col_orig, col_low in zip(columns, cols_lower):
        if result['brand_col'] is None:
            if any(kw in col_low for kw in BRAND_KEYWORDS):
                result['brand_col'] = col_orig
                continue
        if result['name_col'] is None:
            if any(kw in col_low for kw in NAME_KEYWORDS):
                result['name_col'] = col_orig

    # Fallback: if no name column detected, use the last column
    if result['name_col'] is None and len(columns) > 0:
        result['name_col'] = columns[-1]

    return result


def _is_nl_sheet(sheet_name: str) -> bool:
    """Check if a sheet name looks like the NL reference list."""
    name_lower = sheet_name.lower().strip()
    return any(kw in name_lower for kw in NL_SHEET_KEYWORDS)


def _filter_duplicate_custom_configs(df: pd.DataFrame, name_col: str) -> pd.DataFrame:
    """
    Smart filter: Remove "Custom configuration" items ONLY if a specific config also exists.

    Keeps unique custom configs (might be real custom-built products).
    Removes duplicate custom configs (obvious placeholders).

    Example:
      Remove: "Acer Nitro V - Custom configuration" (duplicate exists below)
      Keep:   "Acer Nitro V - Core i5 / 16GB / 512GB" (specific config)
      Keep:   "HP Laptop X - Custom configuration" (if NO specific config exists)

    Returns: DataFrame with duplicate custom configs filtered out
    """
    if name_col not in df.columns:
        return df

    # Find all custom config items
    custom_mask = df[name_col].str.contains('custom configuration', case=False, na=False)
    custom_items = df[custom_mask]

    if len(custom_items) == 0:
        return df  # No custom configs, return as-is

    # Check each custom config item for duplicates
    keep_indices = []

    for idx in df.index:
        if not custom_mask[idx]:
            keep_indices.append(idx)  # Not a custom config, keep it
            continue

        # It's a custom config - check if a specific config exists for same model
        name = str(df.loc[idx, name_col])

        # Extract model base (everything before "- Custom configuration")
        if ' - custom' in name.lower():
            model_base = name.split(' - Custom')[0].strip()
        elif ' - CUSTOM' in name:
            model_base = name.split(' - CUSTOM')[0].strip()
        else:
            model_base = name.replace('custom configuration', '').replace('Custom Configuration', '').strip()

        # Find items with same model base but specific config (not custom)
        duplicates = df[
            (df[name_col].str.contains(model_base, regex=False, na=False, case=False)) &
            (~df[name_col].str.contains('custom configuration', case=False, na=False)) &
            (df.index != idx)
        ]

        if len(duplicates) == 0:
            # No duplicate found - this is a unique custom config, keep it
            keep_indices.append(idx)
        # else: Duplicate exists, don't add to keep_indices (filter it out)

    return df.loc[keep_indices].reset_index(drop=True)


def parse_asset_sheets(file) -> Dict[str, Dict]:
    """
    Parse all asset-list sheets from an uploaded Excel file.

    Automatically:
        - Skips sheets that look like the NL reference
        - Detects the header row (skips title rows)
        - Detects brand and product-name columns
        - Drops the leading empty index column if present

    Returns dict:  sheet_name → {
        'df': pd.DataFrame,
        'brand_col': str or None,
        'name_col': str,
    }
    """
    xls = pd.ExcelFile(file)
    results = {}

    for sheet_name in xls.sheet_names:
        if _is_nl_sheet(sheet_name):
            continue  # Skip NL reference sheets

        # Detect header row
        header_row = _detect_header_row(file, sheet_name)
        df = pd.read_excel(file, sheet_name=sheet_name, header=None, skiprows=header_row + 1)

        # Read header separately to get column names
        hdr = pd.read_excel(file, sheet_name=sheet_name, header=None, skiprows=header_row, nrows=1)
        raw_headers = [str(v).strip() if pd.notna(v) else '' for v in hdr.iloc[0].values]

        # Drop leading empty columns (common pattern: first col is NaN index)
        while raw_headers and raw_headers[0] in ('', 'nan', 'None'):
            raw_headers = raw_headers[1:]
            df = df.iloc[:, 1:]

        if len(raw_headers) == 0 or len(df.columns) == 0:
            continue  # Empty sheet

        # Ensure column count matches
        if len(raw_headers) > len(df.columns):
            raw_headers = raw_headers[:len(df.columns)]
        elif len(raw_headers) < len(df.columns):
            raw_headers += [f'col_{i}' for i in range(len(raw_headers), len(df.columns))]

        df.columns = raw_headers

        # Detect brand and name columns
        col_map = _detect_columns(raw_headers)

        if col_map['name_col'] is None:
            continue  # Can't match without a product name column

        # Drop rows where the name column is empty
        df = df.dropna(subset=[col_map['name_col']])

        # Smart filter: Remove duplicate "Custom configuration" entries
        # Keeps unique custom configs, removes obvious placeholders
        df = _filter_duplicate_custom_configs(df, col_map['name_col'])

        results[sheet_name] = {
            'df': df.reset_index(drop=True),
            'brand_col': col_map['brand_col'],
            'name_col': col_map['name_col'],
        }

    return results