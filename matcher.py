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
# Brand normalization
# ---------------------------------------------------------------------------

BRAND_ALIASES: Dict[str, str] = {
    # Apple variants
    'apple inc': 'apple', 'apple inc.': 'apple', 'apple computer': 'apple',
    # Samsung variants
    'samsung electronics': 'samsung', 'samsung electronics co': 'samsung',
    'samsung electronics co.': 'samsung', 'samsung electronics co ltd': 'samsung',
    # Huawei variants
    'huawei technologies': 'huawei', 'huawei technologies co': 'huawei',
    # HP variants
    'hp inc': 'hp', 'hp inc.': 'hp', 'hewlett packard': 'hp',
    'hewlett-packard': 'hp', 'hewlett packard enterprise': 'hp',
    # Dell variants
    'dell inc': 'dell', 'dell inc.': 'dell', 'dell technologies': 'dell',
    # Lenovo variants
    'lenovo group': 'lenovo', 'lenovo group ltd': 'lenovo',
    # Microsoft variants
    'microsoft corporation': 'microsoft', 'microsoft corp': 'microsoft',
    # Xiaomi variants
    'xiaomi corporation': 'xiaomi', 'xiaomi inc': 'xiaomi',
    # LG variants
    'lg electronics': 'lg', 'lg electronics inc': 'lg',
    # Sony variants
    'sony corporation': 'sony', 'sony mobile': 'sony',
    # Google variants
    'google llc': 'google', 'google inc': 'google',
    # Oppo variants
    'oppo electronics': 'oppo',
    # OnePlus variants
    'oneplus technology': 'oneplus',
    # Asus variants
    'asus computer': 'asus', 'asustek computer': 'asus', 'asustek': 'asus',
    # Acer variants
    'acer inc': 'acer', 'acer inc.': 'acer',
    # Nokia variants
    'nokia corporation': 'nokia', 'hmd global': 'nokia',
    # Motorola variants
    'motorola mobility': 'motorola',
    # Honor variants
    'honor device': 'honor',
    # Vivo variants
    'vivo communication': 'vivo', 'vivo mobile': 'vivo',
    # Realme variants
    'realme mobile': 'realme',
    # Nothing variants
    'nothing technology': 'nothing',
}

# Suffixes to strip from brand names before alias lookup
_BRAND_SUFFIXES = re.compile(
    r'\s+(?:inc\.?|ltd\.?|co\.?|corp\.?|corporation|electronics|technologies|'
    r'group|llc|gmbh|plc|pvt|private|limited|international)\s*$',
    re.IGNORECASE,
)


def normalize_brand(brand: str) -> str:
    """
    Normalize a brand name: lowercase, strip legal suffixes, apply alias lookup.

    Examples:
        'Apple Inc' -> 'apple'
        'Samsung Electronics Co' -> 'samsung'
        'HP Inc.' -> 'hp'
        'Hewlett Packard' -> 'hp'
        'XIAOMI' -> 'xiaomi'
    """
    if not isinstance(brand, str) or not brand.strip():
        return ''
    b = brand.strip().lower()
    # Check alias table first (handles multi-word aliases like "hewlett packard")
    if b in BRAND_ALIASES:
        return BRAND_ALIASES[b]
    # Strip legal suffixes and check again
    b_stripped = _BRAND_SUFFIXES.sub('', b).strip()
    if b_stripped in BRAND_ALIASES:
        return BRAND_ALIASES[b_stripped]
    return b_stripped if b_stripped else b


# Known brand names for inference (canonical → itself, for first-word lookup)
_KNOWN_BRANDS = {
    'apple', 'samsung', 'huawei', 'xiaomi', 'oppo', 'vivo', 'realme',
    'oneplus', 'motorola', 'nokia', 'honor', 'google', 'sony', 'lg',
    'asus', 'lenovo', 'dell', 'hp', 'acer', 'microsoft', 'nothing',
    'poco', 'tecno', 'infinix', 'itel', 'zte', 'alcatel', 'meizu',
    'blackberry', 'htc', 'nubia', 'iqoo',
}
# Also build reverse lookup: all alias keys → canonical brand
_BRAND_FROM_FIRST_WORD = {b: b for b in _KNOWN_BRANDS}
for alias, canonical in BRAND_ALIASES.items():
    first = alias.split()[0]
    if first not in _BRAND_FROM_FIRST_WORD:
        _BRAND_FROM_FIRST_WORD[first] = canonical


def _infer_brand_from_name(product_name: str) -> str:
    """
    Attempt to infer brand from the first word(s) of a product name.

    Only returns a brand if the match is unambiguous (exact known brand).
    Returns '' if no confident inference can be made.

    Examples:
        'Apple iPhone 14 128GB' -> 'apple'
        'Samsung Galaxy S23' -> 'samsung'
        'Unknown Device 128GB' -> ''
    """
    if not product_name:
        return ''
    words = product_name.lower().strip().split()
    if not words:
        return ''
    first = words[0]
    if first in _BRAND_FROM_FIRST_WORD:
        return _BRAND_FROM_FIRST_WORD[first]
    # Try first two words (e.g., "one plus")
    if len(words) >= 2:
        two_words = f"{words[0]} {words[1]}"
        if two_words in _BRAND_FROM_FIRST_WORD:
            return _BRAND_FROM_FIRST_WORD[two_words]
    return ''


# ---------------------------------------------------------------------------
# String normalization
# ---------------------------------------------------------------------------

def normalize_text(text: str) -> str:
    """
    Normalize an asset name for comparison with enhanced variant preservation.

    Steps:
        1. Lowercase
        2. Keep year patterns (2014), (2015) - different years = different products
        3. Keep variant identifiers (Max, Plus, XL, Pro, etc.) - critical differentiators
        4. Keep letter suffixes on models (7X vs 7C) - different variants
        5. Keep product type keywords (Tab, Watch, Fold) - different categories
        6. Remove punctuation (commas, quotes, dashes become spaces)
        7. Standardize storage/RAM: "16 gb" → "16gb"
        8. Remove connectivity markers (5G/LTE) - not product differentiators
        9. Collapse whitespace

    Variant preservation prevents false MULTIPLE_MATCHES:
    - iPhone 11 Pro vs Pro Max → different normalized names (different products!)
    - Honor 7 vs 7X → different normalized names (different models!)
    - Galaxy Tab vs Watch → different normalized names (different categories!)

    Safety note: We intentionally keep:
    - All numeric tokens (storage, RAM, model numbers, years)
    - Variant suffixes (Max, Plus, XL, Pro)
    - Letter model variants (X, C, S after numbers)
    - Product type keywords
    """
    if not isinstance(text, str):
        return ""

    s = text.lower().strip()

    # --- Generation / edition normalization ---
    # Normalize "mark ii", "mk2", "mk 2", "gen 2", "2nd gen", "2nd generation"
    # to canonical "mk2" / "gen2" forms BEFORE punctuation removal
    # Roman numerals: I→1, II→2, III→3, IV→4, V→5, VI→6, VII→7, VIII→8, IX→9, X→10
    _roman_map = {'i': '1', 'ii': '2', 'iii': '3', 'iv': '4', 'v': '5',
                  'vi': '6', 'vii': '7', 'viii': '8', 'ix': '9', 'x': '10'}
    # "mark ii" / "mark 2" → "mk2"
    def _replace_mark(m):
        val = m.group(1).strip().lower()
        num = _roman_map.get(val, val)  # roman → digit, or keep digit
        return f'mk{num}'
    s = re.sub(r'\b(?:mark|mk)\s*(i{1,3}v?|vi{0,3}|ix|x|\d+)\b', _replace_mark, s, flags=re.IGNORECASE)
    # "gen 2" / "gen ii" / "2nd gen" / "2nd generation" → "gen2"
    def _replace_gen_forward(m):
        val = m.group(1).strip().lower()
        num = _roman_map.get(val, val)
        return f'gen{num}'
    def _replace_gen_reverse(m):
        val = m.group(1).strip().lower()
        num = re.sub(r'(st|nd|rd|th)$', '', val)
        return f'gen{num}'
    s = re.sub(r'\bgen(?:eration)?\s*(i{1,3}v?|vi{0,3}|ix|x|\d+)\b', _replace_gen_forward, s, flags=re.IGNORECASE)
    s = re.sub(r'\b(\d+)(?:st|nd|rd|th)\s*gen(?:eration)?\b', _replace_gen_reverse, s, flags=re.IGNORECASE)

    # Model de-concatenation: split joined brand+model and variant patterns
    # Must happen early (before punctuation removal) but after lowercasing
    # Order matters: split compound variants first, then digit-based splits
    # Pattern: variant combos joined together → split (must be before digit splits)
    s = re.sub(r'promax', 'pro max', s)
    # Pattern: tab + model letter → add space (tabs8 → tab s8, taba7 → tab a7)
    s = re.sub(r'\b(tab)([a-z]\d)', r'\1 \2', s)
    # Pattern: known brand names directly followed by digits → add space
    s = re.sub(r'\b(iphone|ipad|galaxy|pixel|redmi|mate|nova|honor|poco|note)(\d)', r'\1 \2', s)
    # Pattern: digits directly followed by known variant keywords → add space
    s = re.sub(r'(\d)(pro|max|plus|ultra|lite|mini|se)\b', r'\1 \2', s)

    # --- Model concatenation: join separated model identifiers ---
    # "fold 3" → "fold3", "flip 4" → "flip4"
    # These are single model identifiers that should stay together for token matching
    s = re.sub(r'\b(fold|flip)\s+(\d+)\b', r'\1\2', s)
    # Galaxy S/A/Z series: "galaxy s 23" → "galaxy s23", "galaxy a 54" → "galaxy a54"
    # Only in galaxy context to avoid false positives (e.g., "Moto Z 32 GB" or "Mate S 32 GB")
    s = re.sub(r'(galaxy)\s+([saz])\s+(\d{2})\b', r'\1 \2\3', s)

    # Pre-normalize fractional TB to GB BEFORE punctuation removal (dot matters here)
    # "0.25tb" → "256gb", "0.5tb" → "512gb"
    s = re.sub(r'\b0\.25\s*tb\b', '256gb', s, flags=re.IGNORECASE)
    s = re.sub(r'\b0\.5\s*tb\b', '512gb', s, flags=re.IGNORECASE)

    # KEEP years - they're critical for distinguishing products
    # iPhone SE (2016) vs (2020) vs (2022) are DIFFERENT products
    # Years will be preserved as numbers after punctuation removal

    # Remove common punctuation — replace with space to preserve token boundaries
    # This converts "(2016)" to " 2016 " which keeps the year
    s = re.sub(r'[,\-\(\)"\'\/\.]', ' ', s)

    # Fix missing unit: "256g" → "256gb" (common typo in some datasets)
    # Only match plausible storage sizes (16+) to avoid converting "5g" connectivity
    s = re.sub(r'\b(1[6-9]|[2-9]\d|\d{3,})g\b', r'\1gb', s, flags=re.IGNORECASE)

    # Standardize storage/RAM: "16 gb" → "16gb", handles TB/MB too
    # This keeps RAM values distinct: "2gb" vs "3gb" vs "4gb"
    s = re.sub(r'(\d+)\s*(gb|tb|mb)', r'\1\2', s, flags=re.IGNORECASE)

    # Standardize watch case size: "40 mm" → "40mm"
    # Critical for watch matching: 42mm vs 46mm are DIFFERENT products
    s = re.sub(r'(\d+)\s*mm\b', r'\1mm', s, flags=re.IGNORECASE)

    # Remove screen size patterns like 15.6" or 10.1" (inches)
    # These are mostly in List 2 laptop names and rarely in NL
    s = re.sub(r'\d+\.?\d*\s*"', '', s)

    # Strip connectivity markers (5G, 4G, 3G, LTE) - these are NOT product differentiators
    # Z Fold2 5G vs Z Fold2 LTE are SAME base product (just different connectivity)
    # Example: "ROG Phone 3 5G" should match "ROG Phone 3" at 100%
    s = re.sub(r'\b[345]g\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\blte\b', '', s, flags=re.IGNORECASE)

    # Remove regional/SIM variants - these are NOT product differentiators
    # "Galaxy S10 Dual SIM" vs "Galaxy S10" are SAME base product
    # "iPhone 12 International" vs "iPhone 12" are SAME base product
    # Example: "Galaxy S10 DS" should match "Galaxy S10" at 100%
    s = re.sub(r'\b(dual\s*sim|ds|international|global)\b', '', s, flags=re.IGNORECASE)

    # KEEP variant suffixes - these indicate different physical products!
    # "Max", "Plus", "XL", "Pro" are already preserved (not removed)
    # Letter model variants (7X, 7C, 8X) are already preserved (part of tokens)
    # Product type keywords (Tab, Watch, Fold, Note) are already preserved

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


def extract_processor_tier(text: str) -> str:
    """
    Extract processor tier (i3, i5, i7, i9, m1, m2, etc.) from laptop name.

    Returns: 'i3', 'i5', 'i7', 'i9', 'm1', 'm2', 'm3', 'ryzen3', 'ryzen5', 'ryzen7', ''
    """
    text_lower = text.lower()

    # Apple Silicon
    if re.search(r'\bm1\b', text_lower):
        return 'm1'
    if re.search(r'\bm2\b', text_lower):
        return 'm2'
    if re.search(r'\bm3\b', text_lower):
        return 'm3'
    if re.search(r'\bm4\b', text_lower):
        return 'm4'

    # Intel Core
    if re.search(r'\bcore\s*i3\b|i3[-\s]', text_lower):
        return 'i3'
    if re.search(r'\bcore\s*i5\b|i5[-\s]', text_lower):
        return 'i5'
    if re.search(r'\bcore\s*i7\b|i7[-\s]', text_lower):
        return 'i7'
    if re.search(r'\bcore\s*i9\b|i9[-\s]', text_lower):
        return 'i9'

    # AMD Ryzen
    if re.search(r'ryzen\s*3\b', text_lower):
        return 'ryzen3'
    if re.search(r'ryzen\s*5\b', text_lower):
        return 'ryzen5'
    if re.search(r'ryzen\s*7\b', text_lower):
        return 'ryzen7'
    if re.search(r'ryzen\s*9\b', text_lower):
        return 'ryzen9'

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
    # Exclude ROG Phone — it's a gaming phone, not a laptop
    if 'rog' in text_lower and 'phone' in text_lower:
        return False
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

    # Extract processor tier (i3, i5, i7, i9, m1, m2, etc.)
    processor = extract_processor_tier(text)

    # Extract CPU generation
    cpu_gen = extract_cpu_generation(text)
    if not cpu_gen:
        # Fallback for laptops without clear CPU gen (e.g., older Apple MacBooks):
        # Use year as model if present (e.g., "2015", "2016", "2017")
        year_match = re.search(r'\b(20\d{2})\b', text)
        if year_match:
            cpu_gen = year_match.group(1)

    attrs = {
        'brand': brand_norm,
        'product_line': '',
        'processor': processor,      # NEW: i3, i5, i7, i9, m1, m2, etc.
        'generation': cpu_gen,        # NEW: 11th gen, 8th gen, m1, etc.
        'model': cpu_gen,             # DEPRECATED: kept for backward compatibility
        'storage': storage,
        'ram': ram,
    }

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
    HYBRID extraction: watch + laptop + phone hand-tuned + generic fallback.

    Watches: Extract series/gen + case size (mm) + connectivity
    Laptops: Extract product line + CPU gen + RAM + storage
    Phones (hand-tuned): Apple, Samsung, Google, Xiaomi, Huawei
    Other devices (generic): Universal pattern detection

    Returns dict with:
        'brand': normalized brand name
        'product_line': product family (galaxy, iphone, redmi, pavilion, thinkpad, etc.)
        'model': model identifier (s9, 14 pro, 10th gen, ryzen 5, etc.)
        'storage': storage capacity (128gb, 1tb, etc.)
        'ram': RAM capacity (laptop-specific, 8gb, 16gb, etc.)
        'watch_mm': case size for watches (40mm, 42mm, 44mm, 46mm, etc.)
        'connectivity': GPS vs Cellular for watches
    """
    _default_attrs = {'brand': '', 'product_line': '', 'model': '', 'storage': '', 'ram': '', 'watch_mm': '', 'connectivity': ''}
    if not isinstance(text, str) or not text.strip():
        return _default_attrs

    text_norm = normalize_text(text)
    brand_norm = normalize_text(brand) if isinstance(brand, str) else ''

    # === WATCH DETECTION (priority - critical attributes: mm, series, connectivity) ===
    if extract_category(text_norm) == 'watch':
        watch_mm = extract_watch_mm(text_norm)

        # Extract series/generation: "series 10", "ultra 2", "se"
        series = ''
        series_match = re.search(r'\b(series\s*\d+|ultra\s*\d+|se)\b', text_norm)
        if series_match:
            series = series_match.group(1).replace('  ', ' ').strip()

        # Extract connectivity: GPS vs Cellular
        connectivity = ''
        if 'cellular' in text_norm or 'lte' in text_norm.lower() or '4g' in text_norm.lower():
            connectivity = 'cellular'
        elif 'gps' in text_norm:
            connectivity = 'gps'

        return {
            'brand': brand_norm,
            'product_line': 'watch',
            'model': series or 'watch',
            'storage': '',           # Watches typically don't have storage variants
            'ram': '',
            'watch_mm': watch_mm,    # CRITICAL: case size
            'connectivity': connectivity,  # GPS vs Cellular
        }

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
    # CRITICAL: Capture ALL variant words (pro max, pro, plus, mini, etc.)
    if 'iphone' in text_norm:
        match = re.search(r'iphone\s+(\d+[a-z]*(?:\s+(?:pro|plus|max|mini|ultra|lite))*)', text_norm)
        if match:
            attrs['product_line'] = 'iphone'
            attrs['model'] = match.group(1).strip()
            return attrs

    # Samsung Galaxy: "galaxy s9 plus 128gb" → line=galaxy, model=s9 plus
    # CRITICAL: Capture ALL variant words (plus, ultra, note, fold, flip, etc.)
    if 'galaxy' in text_norm:
        match = re.search(r'galaxy\s+([a-z]+\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite|note|fold|flip|edge|active))*)', text_norm)
        if match:
            attrs['product_line'] = 'galaxy'
            attrs['model'] = match.group(1).strip()
            return attrs

    # Google Pixel: "pixel 9 pro 256gb" → line=pixel, model=9 pro
    # CRITICAL: Capture ALL variant words (pro xl, pro, a, etc.)
    if 'pixel' in text_norm:
        match = re.search(r'pixel\s+(\d+[a-z]*(?:\s+(?:pro|xl|max|ultra|lite|a))*)', text_norm)
        if match:
            attrs['product_line'] = 'pixel'
            attrs['model'] = match.group(1).strip()
            return attrs

    # Xiaomi Redmi/Mi: "redmi note 12 pro 128gb" → line=redmi, model=note 12 pro
    # CRITICAL: Capture ALL variant words (pro max, pro, plus, etc.)
    if 'redmi' in text_norm:
        match = re.search(r'redmi\s+(note\s+\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*|\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*)', text_norm, re.IGNORECASE)
        if match:
            attrs['product_line'] = 'redmi'
            attrs['model'] = match.group(1).strip()
            return attrs
    elif 'xiaomi' in brand_norm and 'mi' in text_norm:
        # "xiaomi mi 11 ultra" → line=mi, model=11 ultra
        match = re.search(r'mi\s+(\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*)', text_norm)
        if match:
            attrs['product_line'] = 'mi'
            attrs['model'] = match.group(1).strip()
            return attrs

    # Huawei Mate/P-series: "mate 30 pro 256gb" → line=mate, model=30 pro
    # CRITICAL: Capture ALL variant words
    if 'mate' in text_norm and ('huawei' in brand_norm or 'huawei' in text_norm):
        match = re.search(r'mate\s+(\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*)', text_norm)
        if match:
            attrs['product_line'] = 'mate'
            attrs['model'] = match.group(1).strip()
            return attrs
    elif ('huawei' in brand_norm or 'huawei' in text_norm) and re.search(r'\bp\d+', text_norm):
        # "huawei p30 pro" → line=p, model=30 pro
        match = re.search(r'p(\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*)', text_norm)
        if match:
            attrs['product_line'] = 'p'
            attrs['model'] = match.group(1).strip()
            return attrs

    # === GENERIC EXTRACTION (all other brands) ===
    # Detect common product line patterns: "find x5", "moto g50", "reno 8", etc.
    # CRITICAL: Capture ALL variant words (pro max, plus, etc.)

    # Pattern 1: "ProductLine ModelNumber" (e.g., "find x5", "reno 8 pro")
    match = re.search(r'\b([a-z]+)\s+([a-z]?\d+[a-z]*(?:\s+(?:pro|plus|ultra|lite|max|mini|note|xl|edge|active))*)', text_norm, re.IGNORECASE)
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
    match = re.search(r'\b([a-z]?\d+[a-z]*(?:\s+(?:pro|plus|ultra|lite|max|mini|xl))*)', text_norm, re.IGNORECASE)
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
        brand = normalize_brand(str(row.get('brand', '')).strip())
        if not brand:
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

        # Build storage key based on category
        # Watches: use mm + connectivity (CRITICAL: 42mm vs 46mm are different products!)
        # Laptops: use RAM + storage
        # Phones/Tablets: use storage only
        ram = attrs.get('ram', '')
        watch_mm = attrs.get('watch_mm', '')
        connectivity = attrs.get('connectivity', '')

        if attrs['product_line'] == 'watch':
            # Watch key: mm is CRITICAL, connectivity is important
            storage_key = f"{watch_mm}_{connectivity}".strip('_')
        elif ram:
            # Laptop key: RAM + storage
            storage_key = f"{ram}_{attrs['storage']}"
        else:
            # Phone/tablet key: storage only
            storage_key = attrs['storage']

        if storage_key not in index[brand][attrs['product_line']][attrs['model']]:
            index[brand][attrs['product_line']][attrs['model']][storage_key] = {
                'asset_ids': [],
                'nl_name': row['normalized_name']
            }

        asset_id = str(row['uae_assetid']).strip()
        entry = index[brand][attrs['product_line']][attrs['model']][storage_key]
        if asset_id not in entry['asset_ids']:
            entry['asset_ids'].append(asset_id)

        # Watch fallback: also index under mm-only key for connectivity-agnostic lookups
        if attrs['product_line'] == 'watch' and watch_mm and connectivity:
            mm_only_key = watch_mm  # e.g., "42mm" without "_gps"
            if mm_only_key != storage_key:
                if mm_only_key not in index[brand][attrs['product_line']][attrs['model']]:
                    index[brand][attrs['product_line']][attrs['model']][mm_only_key] = {
                        'asset_ids': [],
                        'nl_name': row['normalized_name'],
                        '_is_fallback': True,  # marker for fallback key
                    }
                fallback_entry = index[brand][attrs['product_line']][attrs['model']][mm_only_key]
                if asset_id not in fallback_entry['asset_ids']:
                    fallback_entry['asset_ids'].append(asset_id)

    return index


def try_attribute_match(
    query: str,
    brand: str,
    attribute_index: Dict,
    nl_catalog: Optional[pd.DataFrame] = None,
    original_input: str = ''
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

    # CATEGORY FILTERING: Extract query category to prevent cross-category matches
    query_category = extract_category(query)

    # Navigate the index
    try:
        brand_data = attribute_index.get(attrs['brand'], {})
        line_data = brand_data.get(attrs['product_line'], {})
        model_data = line_data.get(attrs['model'], {})

        # Build storage key based on category (must match build_attribute_index logic)
        # Watches: use mm + connectivity
        # Laptops: use RAM + storage
        # Phones/Tablets: use storage only
        ram = attrs.get('ram', '')
        watch_mm = attrs.get('watch_mm', '')
        connectivity = attrs.get('connectivity', '')

        if attrs['product_line'] == 'watch':
            storage_key = f"{watch_mm}_{connectivity}".strip('_')
        elif ram:
            storage_key = f"{ram}_{attrs['storage']}"
        else:
            storage_key = attrs['storage']

        # Try exact match with category-specific key
        if storage_key in model_data:
            entry = model_data[storage_key]
            asset_ids = entry['asset_ids']
            nl_name = entry['nl_name']

            # CATEGORY CHECK: Verify the matched product is in the same category
            nl_category = extract_category(nl_name)
            if query_category != 'other' and nl_category != query_category:
                # Cross-category match detected - reject it
                return None

            # Auto-select if multiple IDs and catalog provided
            if len(asset_ids) > 1 and nl_catalog is not None:
                user_input_for_auto_select = original_input if original_input else query
                selection = auto_select_matching_variant(user_input_for_auto_select, asset_ids, nl_catalog)
                return {
                    'mapped_uae_assetid': selection['selected_id'],
                    'match_score': 100.0,
                    'match_status': MATCH_STATUS_MATCHED,
                    'confidence': CONFIDENCE_HIGH,
                    'matched_on': entry['nl_name'],
                    'method': 'attribute_auto_selected',
                    'auto_selected': selection['auto_selected'],
                    'selection_reason': selection['reason'],
                    'alternatives': selection['alternatives'],
                }
            else:
                return {
                    'mapped_uae_assetid': ', '.join(asset_ids),
                    'match_score': 100.0,
                    'match_status': MATCH_STATUS_MULTIPLE if len(asset_ids) > 1 else MATCH_STATUS_MATCHED,
                    'confidence': CONFIDENCE_HIGH,
                    'matched_on': entry['nl_name'],
                    'method': 'attribute',
                    'auto_selected': False,
                    'selection_reason': '',
                    'alternatives': [],
                }

        # Watch fallback: try mm-only key if full mm+connectivity key missed
        if attrs['product_line'] == 'watch' and watch_mm and connectivity:
            mm_only_key = watch_mm
            if mm_only_key in model_data and mm_only_key != storage_key:
                entry = model_data[mm_only_key]
                asset_ids = entry['asset_ids']
                nl_name = entry['nl_name']
                nl_category = extract_category(nl_name)
                if query_category == 'other' or nl_category == query_category:
                    if len(asset_ids) > 1 and nl_catalog is not None:
                        user_input_for_auto_select = original_input if original_input else query
                        selection = auto_select_matching_variant(user_input_for_auto_select, asset_ids, nl_catalog)
                        return {
                            'mapped_uae_assetid': selection['selected_id'],
                            'match_score': 95.0,
                            'match_status': MATCH_STATUS_MATCHED if selection['auto_selected'] else MATCH_STATUS_MULTIPLE,
                            'confidence': CONFIDENCE_HIGH if selection['auto_selected'] else CONFIDENCE_MEDIUM,
                            'matched_on': entry['nl_name'],
                            'method': 'attribute_watch_mm_fallback',
                            'auto_selected': selection['auto_selected'],
                            'selection_reason': selection['reason'],
                            'alternatives': selection['alternatives'],
                        }
                    else:
                        return {
                            'mapped_uae_assetid': ', '.join(asset_ids),
                            'match_score': 95.0,
                            'match_status': MATCH_STATUS_MULTIPLE if len(asset_ids) > 1 else MATCH_STATUS_MATCHED,
                            'confidence': CONFIDENCE_HIGH if len(asset_ids) == 1 else CONFIDENCE_MEDIUM,
                            'matched_on': entry['nl_name'],
                            'method': 'attribute_watch_mm_fallback',
                            'auto_selected': False,
                            'selection_reason': '',
                            'alternatives': [],
                        }

        # Fallback: try without RAM if laptop match failed (maybe RAM not in query)
        # Skip this fallback for watches (watches don't have RAM/storage variants)
        if ram and attrs['storage'] in model_data and attrs['product_line'] != 'watch':
            entry = model_data[attrs['storage']]
            asset_ids = entry['asset_ids']
            nl_name = entry['nl_name']

            # CATEGORY CHECK: Verify the matched product is in the same category
            nl_category = extract_category(nl_name)
            if query_category != 'other' and nl_category != query_category:
                # Cross-category match detected - reject it
                return None

            if len(asset_ids) > 1 and nl_catalog is not None:
                user_input_for_auto_select = original_input if original_input else query
                selection = auto_select_matching_variant(user_input_for_auto_select, asset_ids, nl_catalog)
                return {
                    'mapped_uae_assetid': selection['selected_id'],
                    'match_score': 95.0,
                    'match_status': MATCH_STATUS_MATCHED,
                    'confidence': CONFIDENCE_HIGH,
                    'matched_on': entry['nl_name'],
                    'method': 'attribute_auto_selected',
                    'auto_selected': selection['auto_selected'],
                    'selection_reason': selection['reason'],
                    'alternatives': selection['alternatives'],
                }
            else:
                return {
                    'mapped_uae_assetid': ', '.join(asset_ids),
                    'match_score': 95.0,
                    'match_status': MATCH_STATUS_MULTIPLE if len(asset_ids) > 1 else MATCH_STATUS_MATCHED,
                    'confidence': CONFIDENCE_HIGH,
                    'matched_on': entry['nl_name'],
                    'method': 'attribute',
                    'auto_selected': False,
                    'selection_reason': '',
                    'alternatives': [],
                }

        # Try without storage if no exact match (for products without storage in name)
        if '' in model_data:  # Empty storage key
            entry = model_data['']
            asset_ids = entry['asset_ids']
            nl_name = entry['nl_name']

            # CATEGORY CHECK: Verify the matched product is in the same category
            nl_category = extract_category(nl_name)
            if query_category != 'other' and nl_category != query_category:
                # Cross-category match detected - reject it
                return None

            if len(asset_ids) > 1 and nl_catalog is not None:
                user_input_for_auto_select = original_input if original_input else query
                selection = auto_select_matching_variant(user_input_for_auto_select, asset_ids, nl_catalog)
                return {
                    'mapped_uae_assetid': selection['selected_id'],
                    'match_score': 90.0,
                    'match_status': MATCH_STATUS_MATCHED,
                    'confidence': CONFIDENCE_MEDIUM,
                    'matched_on': entry['nl_name'],
                    'method': 'attribute_auto_selected',
                    'auto_selected': selection['auto_selected'],
                    'selection_reason': selection['reason'],
                    'alternatives': selection['alternatives'],
                }
            else:
                return {
                    'mapped_uae_assetid': ', '.join(asset_ids),
                    'match_score': 90.0,
                    'match_status': MATCH_STATUS_MULTIPLE if len(asset_ids) > 1 else MATCH_STATUS_MATCHED,
                    'confidence': CONFIDENCE_MEDIUM,
                    'matched_on': entry['nl_name'],
                    'method': 'attribute',
                    'auto_selected': False,
                    'selection_reason': '',
                    'alternatives': [],
                }

        # --- TIER 2: Query has no storage → model has exactly 1 storage variant ---
        # Safe: If there's only one option, the product identity is unambiguous
        query_storage = attrs.get('storage', '')
        if not query_storage and model_data and attrs['product_line'] != 'watch':
            storage_keys = [k for k in model_data.keys() if k]  # non-empty keys
            if len(storage_keys) == 1:
                # Only one storage variant — safe to match
                entry = model_data[storage_keys[0]]
                asset_ids = entry['asset_ids']
                nl_name = entry['nl_name']
                nl_category = extract_category(nl_name)
                if query_category == 'other' or nl_category == query_category:
                    if len(asset_ids) > 1 and nl_catalog is not None:
                        user_input_for_auto_select = original_input if original_input else query
                        selection = auto_select_matching_variant(user_input_for_auto_select, asset_ids, nl_catalog)
                        return {
                            'mapped_uae_assetid': selection['selected_id'],
                            'match_score': 95.0,
                            'match_status': MATCH_STATUS_MATCHED,
                            'confidence': CONFIDENCE_HIGH,
                            'matched_on': entry['nl_name'],
                            'method': 'attribute_tier2_single_variant',
                            'auto_selected': selection['auto_selected'],
                            'selection_reason': selection['reason'],
                            'alternatives': selection['alternatives'],
                        }
                    elif len(asset_ids) == 1:
                        return {
                            'mapped_uae_assetid': asset_ids[0],
                            'match_score': 95.0,
                            'match_status': MATCH_STATUS_MATCHED,
                            'confidence': CONFIDENCE_HIGH,
                            'matched_on': entry['nl_name'],
                            'method': 'attribute_tier2_single_variant',
                            'auto_selected': False,
                            'selection_reason': '',
                            'alternatives': [],
                        }
            elif len(storage_keys) > 1:
                # Multiple storage variants — return MULTIPLE_MATCHES with auto-select
                all_ids = []
                first_nl_name = ''
                for sk in storage_keys:
                    e = model_data[sk]
                    if not first_nl_name:
                        first_nl_name = e['nl_name']
                    all_ids.extend(e['asset_ids'])
                nl_category = extract_category(first_nl_name)
                if query_category == 'other' or nl_category == query_category:
                    all_ids = list(dict.fromkeys(all_ids))  # deduplicate preserving order
                    if len(all_ids) > 1 and nl_catalog is not None:
                        user_input_for_auto_select = original_input if original_input else query
                        selection = auto_select_matching_variant(user_input_for_auto_select, all_ids, nl_catalog)
                        return {
                            'mapped_uae_assetid': selection['selected_id'],
                            'match_score': 90.0,
                            'match_status': MATCH_STATUS_MATCHED if selection['auto_selected'] else MATCH_STATUS_MULTIPLE,
                            'confidence': CONFIDENCE_HIGH if selection['auto_selected'] else CONFIDENCE_MEDIUM,
                            'matched_on': first_nl_name,
                            'method': 'attribute_tier2_multi_variant',
                            'auto_selected': selection['auto_selected'],
                            'selection_reason': selection['reason'],
                            'alternatives': selection['alternatives'],
                        }
                    elif len(all_ids) == 1:
                        return {
                            'mapped_uae_assetid': all_ids[0],
                            'match_score': 90.0,
                            'match_status': MATCH_STATUS_MATCHED,
                            'confidence': CONFIDENCE_MEDIUM,
                            'matched_on': first_nl_name,
                            'method': 'attribute_tier2_multi_variant',
                            'auto_selected': False,
                            'selection_reason': '',
                            'alternatives': [],
                        }

        # --- TIER 3: Query has storage but no exact key → fuzzy match storage keys ---
        if query_storage and model_data and attrs['product_line'] != 'watch':
            available_keys = [k for k in model_data.keys() if k]
            if available_keys:
                from rapidfuzz import fuzz as _fuzz
                best_key = None
                best_score = 0
                for k in available_keys:
                    score = _fuzz.ratio(storage_key, k)
                    if score > best_score:
                        best_score = score
                        best_key = k
                if best_key and best_score >= 80:
                    entry = model_data[best_key]
                    asset_ids = entry['asset_ids']
                    nl_name = entry['nl_name']
                    nl_category = extract_category(nl_name)
                    if query_category == 'other' or nl_category == query_category:
                        if len(asset_ids) > 1 and nl_catalog is not None:
                            user_input_for_auto_select = original_input if original_input else query
                            selection = auto_select_matching_variant(user_input_for_auto_select, asset_ids, nl_catalog)
                            return {
                                'mapped_uae_assetid': selection['selected_id'],
                                'match_score': 90.0,
                                'match_status': MATCH_STATUS_MATCHED,
                                'confidence': CONFIDENCE_MEDIUM,
                                'matched_on': entry['nl_name'],
                                'method': 'attribute_tier3_fuzzy_storage',
                                'auto_selected': selection['auto_selected'],
                                'selection_reason': selection['reason'],
                                'alternatives': selection['alternatives'],
                            }
                        else:
                            return {
                                'mapped_uae_assetid': ', '.join(asset_ids),
                                'match_score': 90.0,
                                'match_status': MATCH_STATUS_MULTIPLE if len(asset_ids) > 1 else MATCH_STATUS_MATCHED,
                                'confidence': CONFIDENCE_MEDIUM,
                                'matched_on': entry['nl_name'],
                                'method': 'attribute_tier3_fuzzy_storage',
                                'auto_selected': False,
                                'selection_reason': '',
                                'alternatives': [],
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
        brand = normalize_brand(str(row.get('brand', '')).strip())
        if not brand:
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


def _normalize_storage_value(val: str) -> str:
    """Canonicalize storage: 1024gb→1tb, 2048gb→2tb. Passthrough for normal values."""
    if not val:
        return val
    m = re.match(r'^(\d+)(gb|tb|mb)$', val, re.IGNORECASE)
    if not m:
        return val
    num, unit = int(m.group(1)), m.group(2).lower()
    if unit == 'gb' and num == 1024:
        return '1tb'
    if unit == 'gb' and num == 2048:
        return '2tb'
    return val


def extract_storage(text: str) -> str:
    """
    Extract storage from a normalized product string (e.g., '16gb', '128gb', '1tb').
    Filters out RAM-sized values (typically <= 12GB for phones/tablets) when multiple
    matches are found, to avoid confusing '4gb' RAM with '64gb' storage.
    """
    if not isinstance(text, str):
        return ''
    matches = re.findall(r'(\d+(?:gb|tb|mb))', text)
    if not matches:
        return ''
    if len(matches) == 1:
        return _normalize_storage_value(matches[0])

    # Prefer TB values (definitely storage)
    tb_matches = [m for m in matches if 'tb' in m.lower()]
    if tb_matches:
        return _normalize_storage_value(tb_matches[0])

    # For GB values, filter out likely RAM (<= 12GB) and prefer larger values
    gb_values = [(m, int(re.search(r'\d+', m).group())) for m in matches if 'gb' in m.lower()]
    storage_values = [(m, size) for m, size in gb_values if size >= 16]
    if storage_values:
        return _normalize_storage_value(max(storage_values, key=lambda x: x[1])[0])

    # Fallback: return first match
    return _normalize_storage_value(matches[0])


def extract_watch_mm(text: str) -> str:
    """
    Extract watch case size in mm.

    Returns: '40mm', '42mm', '44mm', '46mm', '49mm', etc.
    Handles: "40mm", "40 mm", "40MM"

    Critical for distinguishing watch variants - 42mm vs 46mm are different products!
    """
    if not isinstance(text, str) or not text:
        return ''
    # Match 38-55mm range (covers all Apple Watch, Galaxy Watch, etc.)
    match = re.search(r'\b(3[89]|4[0-9]|5[0-5])\s*mm\b', text, re.IGNORECASE)
    return f"{match.group(1)}mm" if match else ''


def extract_category(text: str) -> str:
    """
    Extract product category from normalized text.

    Returns one of: 'mobile', 'tablet', 'watch', 'laptop', 'other'

    Used for category filtering to prevent cross-category false matches
    (e.g., Galaxy Tab should NOT match Galaxy Watch).
    """
    if not isinstance(text, str) or not text.strip():
        return 'other'
    text_lower = text.lower()

    # Tablets: Must check before "phone" (some products have both keywords)
    # Use word boundary for 'tab' to prevent false matches in 'stable', 'collaboration', etc.
    if (re.search(r'\btab(?:let)?\b', text_lower) or
        'ipad' in text_lower or
        'matepad' in text_lower or
        re.search(r'\bpad\b', text_lower)):
        return 'tablet'

    # Smartwatches: Must check before "phone"
    # Covers: Apple Watch, Galaxy Watch, Samsung Gear, Huawei Watch GT, etc.
    if 'watch' in text_lower or re.search(r'\bgear\b', text_lower):
        return 'watch'

    # Laptops: Check before mobile (MacBook, ThinkPad, etc.)
    if is_laptop_product(text):
        return 'laptop'

    # Mobile phones: Most common category
    # Use word boundaries for 'phone' to avoid 'headphones', and for short keywords
    # to prevent false matches in 'climate', 'ultimate', 'innovation', 'finder', etc.
    if any(kw in text_lower for kw in ['iphone', 'mobile', 'smartphone', 'galaxy s', 'galaxy a', 'galaxy z', 'pixel', 'redmi']) or \
       any(re.search(rf'\b{kw}\b', text_lower) for kw in ['phone', 'mi', 'mate', 'nova', 'find', 'reno']):
        return 'mobile'

    # Phone-only brands: These manufacturers make almost exclusively phones.
    # If the brand name appears, it's safe to classify as mobile.
    # Word boundaries prevent false substring matches (e.g., 'nothing' in a sentence).
    phone_only_brands = [
        'honor', 'motorola', 'moto', 'oneplus', 'one plus',
        'nokia', 'vivo', 'realme', 'nothing',
        'oppo', 'xiaomi', 'poco', 'tecno', 'infinix', 'itel',
        'zte', 'alcatel', 'meizu', 'umidigi', 'doogee',
        'blackview', 'cubot', 'oukitel', 'ulefone',
        'cat phone', 'fairphone', 'sharp aquos',
        'sony xperia', 'xperia',
        'iqoo', 'nubia',
    ]
    if any(re.search(rf'\b{re.escape(kw)}\b', text_lower) for kw in phone_only_brands):
        return 'mobile'

    # LG phone series: "LG V60", "LG G8" — word boundary after V/G fails when followed by digit
    if re.search(r'\blg\s+[vg]\d', text_lower):
        return 'mobile'

    return 'other'


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

    Returns tokens that contain digits OR are variant keywords (max, plus, xl, pro, etc).
    This ensures Pro vs Pro Max are distinguished by the model token guardrail.

    Extracts:
    - Tokens with digits: "14", "5t", "a57s", "s23"
    - Variant keywords: "max", "plus", "xl", "mini", "lite", "ultra"
    - Product type keywords: "tab", "watch", "fold", "flip", "note"
    - Letter suffixes: "7x", "7c", "8x" (already captured if they have digits)

    Examples:
        'apple iphone 14 pro 256gb' -> ['14', 'pro']
        'apple iphone 11 pro max 256gb' -> ['11', 'pro', 'max']
        'huawei nova 5t 128gb'      -> ['5t']
        'honor 7 series honor 7x 32gb' -> ['7', '7x']
        'samsung galaxy tab s8 128gb' -> ['tab', 's8']
        'google pixel 9 pro xl 512gb' -> ['9', 'pro', 'xl']
    """
    if not isinstance(text, str) or not text.strip():
        return []
    # Remove storage tokens (e.g., "256gb", "1tb")
    text_clean = re.sub(r'\b\d+(?:gb|tb|mb)\b', '', text)
    # Remove connectivity markers (e.g., "5g", "4g")
    text_clean = re.sub(r'\b[345]g\b', '', text_clean)

    # Variant keywords that distinguish different products
    # These are critical identifiers that must match for products to be the same
    variant_keywords = {
        # Size variants
        'max', 'plus', 'mini', 'xl', 'ultra', 'lite', 'pro',
        # Product types (different categories!)
        'tab', 'watch', 'fold', 'flip', 'note', 'pad', 'book',
        # Generation markers that matter
        'edge', 'active', 'prime',
    }

    tokens = text_clean.split()
    model_tokens = []

    for token in tokens:
        # Include if token contains a digit (existing logic)
        if re.search(r'\d', token):
            model_tokens.append(token)
        # Also include if token is a variant keyword (NEW!)
        elif token in variant_keywords:
            model_tokens.append(token)

    return model_tokens


def extract_model_variant_keywords(text: str) -> Dict[str, any]:
    """
    Extract model variant keywords that distinguish different products.

    Returns dict with:
        'fold_gen': Generation number for Fold (e.g., 'fold2', 'fold3', 'fold6')
        'flip_gen': Generation number for Flip (e.g., 'flip3', 'flip4', 'flip6')
        'has_fold': Boolean - is this a Fold product?
        'has_flip': Boolean - is this a Flip product?
        'has_pro_max': Boolean - is this Pro Max variant?
        'has_pro': Boolean - is this Pro (but NOT Pro Max)?
        'has_plus': Boolean - is this Plus variant?
        'has_ultra': Boolean - is this Ultra variant?
        'has_lite': Boolean - is this Lite variant?

    Critical for preventing errors like:
    - Fold2 matching with Fold4 (different generations!)
    - Flip matching with Fold (completely different product lines!)
    - Pro matching with Pro Max (different models!)
    """
    text_lower = text.lower()
    result = {
        'fold_gen': None,
        'flip_gen': None,
        'has_fold': False,
        'has_flip': False,
        'has_pro_max': False,
        'has_pro': False,
        'has_plus': False,
        'has_ultra': False,
        'has_lite': False,
    }

    # Fold generation (Fold2, Fold3, Fold4, Fold6, Fold7, etc.)
    if 'fold' in text_lower:
        result['has_fold'] = True
        # Look for generation number: "fold 2", "fold2", "z fold 3", "zfold3"
        fold_match = re.search(r'fold\s*(\d+)', text_lower)
        if fold_match:
            result['fold_gen'] = f"fold{fold_match.group(1)}"
        else:
            result['fold_gen'] = 'fold'  # Generic Fold without generation

    # Flip generation (Flip3, Flip4, Flip5, Flip6, Flip7, etc.)
    if 'flip' in text_lower:
        result['has_flip'] = True
        # Look for generation number: "flip 3", "flip3", "z flip 4", "zflip4"
        flip_match = re.search(r'flip\s*(\d+)', text_lower)
        if flip_match:
            result['flip_gen'] = f"flip{flip_match.group(1)}"
        else:
            result['flip_gen'] = 'flip'  # Generic Flip without generation

    # Pro vs Pro Max (CRITICAL: Check "pro max" first!)
    if 'pro max' in text_lower:
        result['has_pro_max'] = True
    elif 'pro' in text_lower:
        result['has_pro'] = True

    # Other variants
    if 'plus' in text_lower:
        result['has_plus'] = True
    if 'ultra' in text_lower:
        result['has_ultra'] = True
    if 'lite' in text_lower:
        result['has_lite'] = True

    return result


def auto_select_matching_variant(
    user_input: str,
    asset_ids: List[str],
    nl_catalog: pd.DataFrame
) -> dict:
    """
    Automatically select the best variant from MULTIPLE_MATCHES based on user's exact specs.

    For recommerce: Match what user HAS, not what's 'better'.

    Priority order (CRITICAL - DO NOT SKIP ANY!):
    1. Year matching (2024, 2023, etc.)
    1.5. MODEL VARIANT matching (Fold vs Flip, Fold2 vs Fold3, Pro vs Pro Max) <- ADDED TO FIX ERRORS!
    2. Connectivity matching (5G vs 4G)
    3. First ID if truly identical

    Returns dict:
        'selected_id': The chosen asset ID
        'auto_selected': True if auto-selected, False if manual selection needed
        'reason': Human-readable explanation of selection logic
        'alternatives': List of other asset IDs (for manual override)
    """
    if len(asset_ids) == 0:
        return {
            'selected_id': '',
            'auto_selected': False,
            'reason': 'No variants found',
            'alternatives': []
        }

    if len(asset_ids) == 1:
        return {
            'selected_id': asset_ids[0],
            'auto_selected': False,
            'reason': 'Single match',
            'alternatives': []
        }

    # Get all variant details
    variants = nl_catalog[nl_catalog['uae_assetid'].isin(asset_ids)]

    if len(variants) == 0:
        return {
            'selected_id': asset_ids[0],
            'auto_selected': False,
            'reason': 'Variants not found in catalog',
            'alternatives': asset_ids[1:]
        }

    # === PRIORITY 1: Year matching (most specific) ===
    user_year = re.search(r'\b(20\d{2})\b', user_input)
    if user_year:
        year = user_year.group(1)
        match_year = variants[variants['uae_assetname'].str.contains(year, na=False)]
        if len(match_year) > 0:
            # Continue to Priority 1.5 with year-filtered variants
            variants = match_year

    # === PRIORITY 1.5: MODEL VARIANT matching (CRITICAL FIX!) ===
    # This prevents Fold2 from matching Fold4, and Flip from matching Fold!
    user_variants = extract_model_variant_keywords(user_input)

    # CRITICAL ERROR PREVENTION 1: Fold vs Flip (completely different product lines!)
    if user_variants['has_fold'] or user_variants['has_flip']:
        # Filter to ONLY Fold or ONLY Flip based on what user has
        filtered = []
        for _, row in variants.iterrows():
            nl_variants = extract_model_variant_keywords(row['uae_assetname'])

            # If user has Fold, NL must have Fold (not Flip!)
            if user_variants['has_fold'] and not nl_variants['has_fold']:
                continue
            # If user has Flip, NL must have Flip (not Fold!)
            if user_variants['has_flip'] and not nl_variants['has_flip']:
                continue

            filtered.append(row['uae_assetid'])

        if len(filtered) > 0:
            variants = nl_catalog[nl_catalog['uae_assetid'].isin(filtered)]

    # CRITICAL ERROR PREVENTION 2: Fold/Flip generation matching (Fold2 ≠ Fold3 ≠ Fold4!)
    if user_variants['fold_gen'] or user_variants['flip_gen']:
        filtered = []
        for _, row in variants.iterrows():
            nl_variants = extract_model_variant_keywords(row['uae_assetname'])

            # If user has specific Fold generation, NL must match EXACTLY
            if user_variants['fold_gen'] and nl_variants['fold_gen'] != user_variants['fold_gen']:
                continue
            # If user has specific Flip generation, NL must match EXACTLY
            if user_variants['flip_gen'] and nl_variants['flip_gen'] != user_variants['flip_gen']:
                continue

            filtered.append(row['uae_assetid'])

        if len(filtered) > 0:
            variants = nl_catalog[nl_catalog['uae_assetid'].isin(filtered)]

    # ERROR PREVENTION 3: Pro vs Pro Max (different models!)
    if user_variants['has_pro_max'] or user_variants['has_pro']:
        filtered = []
        for _, row in variants.iterrows():
            nl_variants = extract_model_variant_keywords(row['uae_assetname'])

            # If user has Pro Max, NL must have Pro Max (not just Pro)
            if user_variants['has_pro_max'] and not nl_variants['has_pro_max']:
                continue
            # If user has Pro (not Max), NL must NOT have Pro Max
            if user_variants['has_pro'] and nl_variants['has_pro_max']:
                continue

            filtered.append(row['uae_assetid'])

        if len(filtered) > 0:
            variants = nl_catalog[nl_catalog['uae_assetid'].isin(filtered)]

    # ERROR PREVENTION 4: Plus variant matching
    if user_variants['has_plus']:
        filtered = []
        for _, row in variants.iterrows():
            nl_variants = extract_model_variant_keywords(row['uae_assetname'])

            # If user has Plus, prefer NL with Plus
            if nl_variants['has_plus']:
                filtered.append(row['uae_assetid'])

        if len(filtered) > 0:
            variants = nl_catalog[nl_catalog['uae_assetid'].isin(filtered)]

    # If model variant filtering narrowed down to 1 option, select it!
    if len(variants) == 1:
        selected = variants.iloc[0]['uae_assetid']
        alternatives = [aid for aid in asset_ids if aid != selected]

        # Build reason based on what was matched
        reason_parts = []
        if user_year:
            reason_parts.append(f'year {user_year.group(1)}')
        if user_variants['fold_gen']:
            reason_parts.append(f'{user_variants["fold_gen"]}')
        elif user_variants['flip_gen']:
            reason_parts.append(f'{user_variants["flip_gen"]}')
        if user_variants['has_pro_max']:
            reason_parts.append('Pro Max')
        elif user_variants['has_pro']:
            reason_parts.append('Pro')
        if user_variants['has_plus']:
            reason_parts.append('Plus')

        reason = f'Matched {", ".join(reason_parts)}' if reason_parts else 'Matched model variant'

        return {
            'selected_id': selected,
            'auto_selected': True,
            'reason': reason,
            'alternatives': alternatives
        }

    # === PRIORITY 2: Connectivity matching (5G vs 4G/LTE) ===
    user_has_5g = '5g' in user_input.lower()
    user_has_4g = any(x in user_input.lower() for x in ['4g', 'lte'])

    if user_has_5g:
        # User has 5G -> select 5G variant
        match_5g = variants[variants['uae_assetname'].str.contains('5G|5g', na=False, regex=True)]
        if len(match_5g) > 0:
            selected = match_5g.iloc[0]['uae_assetid']
            alternatives = [aid for aid in asset_ids if aid != selected]
            return {
                'selected_id': selected,
                'auto_selected': True,
                'reason': 'Matched 5G (user has 5G)',
                'alternatives': alternatives
            }

    if user_has_4g:
        # User has 4G/LTE -> select non-5G variant
        match_4g = variants[~variants['uae_assetname'].str.contains('5G|5g', na=False, regex=True)]
        if len(match_4g) > 0:
            selected = match_4g.iloc[0]['uae_assetid']
            alternatives = [aid for aid in asset_ids if aid != selected]
            return {
                'selected_id': selected,
                'auto_selected': True,
                'reason': 'Matched 4G/LTE (user has 4G/LTE)',
                'alternatives': alternatives
            }

    # Check if NL has connectivity difference but user doesn't specify
    has_5g_variant = any('5g' in str(v).lower() for v in variants['uae_assetname'])
    has_4g_variant = any(not ('5g' in str(v).lower()) for v in variants['uae_assetname'])

    if has_5g_variant and has_4g_variant:
        # User didn't specify, default to non-5G (more common in recommerce inventory)
        match_4g = variants[~variants['uae_assetname'].str.contains('5G|5g', na=False, regex=True)]
        if len(match_4g) > 0:
            selected = match_4g.iloc[0]['uae_assetid']
            alternatives = [aid for aid in asset_ids if aid != selected]
            return {
                'selected_id': selected,
                'auto_selected': True,
                'reason': 'Defaulted to 4G (user unspecified)',
                'alternatives': alternatives
            }

    # === PRIORITY 3: Truly identical variants -> pick first ===
    selected = variants.iloc[0]['uae_assetid']
    alternatives = asset_ids[1:] if len(asset_ids) > 1 else []
    return {
        'selected_id': selected,
        'auto_selected': True,
        'reason': 'First ID (variants identical)',
        'alternatives': alternatives
    }


def verify_critical_attributes(query: str, matched: str) -> bool:
    """
    Verify that critical attributes match between query and matched product.

    Used for REVIEW items (85-94% scores) to determine if they should be
    auto-upgraded to MATCHED status.

    Critical attributes that MUST match:
        - Model tokens: e.g., "14" in "iPhone 14", "5t" in "Nova 5T", "s23" in "Galaxy S23"
        - Storage: e.g., "128gb", "256gb", "1tb"

    Non-critical attributes (can differ):
        - Connectivity: "5G", "Dual SIM", "LTE"
        - Year: "2020" vs "2022"
        - Descriptors: "Pro", "Plus", "Limited Edition"

    Args:
        query: Normalized query string (original product name)
        matched: Normalized matched string (NL catalog product name)

    Returns:
        True if all critical attributes match (safe to upgrade to MATCHED)
        False if any critical attribute differs (keep as REVIEW_REQUIRED)

    Examples:
        ✓ verify("rog phone 5 dual 128gb", "asus rog phone 5 128gb") -> True
          (Model "5" matches, storage "128gb" matches, "dual" is non-critical)

        ✓ verify("iphone se 2020 128gb", "apple iphone se 128gb") -> True
          (Model "se" matches, storage matches, year "2020" is non-critical)

        ✗ verify("galaxy s23 256gb", "samsung galaxy s24 256gb") -> False
          (Model "s23" vs "s24" differs - different products)

        ✗ verify("iphone 14 128gb", "apple iphone 14 256gb") -> False
          (Storage "128gb" vs "256gb" differs - different SKUs)
    """
    if not isinstance(query, str) or not isinstance(matched, str):
        return False
    try:
        return _verify_critical_attributes_inner(query, matched)
    except Exception:
        return False


def _verify_critical_attributes_inner(query: str, matched: str) -> bool:
    """Inner implementation of verify_critical_attributes (wrapped by try/except)."""
    # CATEGORY CROSS-MATCH RULE: Tablet vs phone guard
    # "galaxy tab s10 plus" must NEVER match "galaxy s10 plus" (phone)
    query_cat = extract_category(query)
    matched_cat = extract_category(matched)
    if query_cat != matched_cat and query_cat != 'other' and matched_cat != 'other':
        return False  # Different known categories -> different product type

    # Extract critical attributes from both strings
    query_model = extract_model_tokens(query)
    matched_model = extract_model_tokens(matched)

    query_storage = extract_storage(query)
    matched_storage = extract_storage(matched)

    # MM SIZE RULE: Case size (mm) must match exactly
    # 42mm vs 46mm are DIFFERENT products! Run unconditionally since
    # extract_watch_mm only matches 38-55mm (watch-specific range)
    query_mm = extract_watch_mm(query)
    matched_mm = extract_watch_mm(matched)
    if query_mm and matched_mm and query_mm != matched_mm:
            return False  # Different case size -> different product

    # RULE 1: Storage must match exactly if both have storage specified
    # (128GB vs 256GB are different SKUs)
    if query_storage and matched_storage:
        if query_storage != matched_storage:
            return False  # Different storage -> different SKU

    # RULE 2: Model tokens must match (order doesn't matter, but values must)
    # This catches: iPhone 14 vs 15, Galaxy S23 vs S24, Nova 5T vs 5i
    if query_model and matched_model:
        # Both have model tokens - they must overlap significantly
        # Using set comparison: if query tokens are subset of matched tokens, it's OK
        # (matched might have extra tokens like year, but core model must match)
        query_set = set(query_model)
        matched_set = set(matched_model)

        # Check if core model tokens match
        # At least one model token from query must be in matched
        if not query_set.intersection(matched_set):
            return False  # No common model tokens -> different products

        # If query has primary model token (first one), matched must have it too
        # This catches: iPhone "14" vs "15", Galaxy "s23" vs "s24"
        if query_model[0] not in matched_set:
            return False  # Primary model differs

    elif query_model and not matched_model:
        # Query has model token but match doesn't
        # This might be OK if it's just a generic base model
        # e.g., "ROG Phone 6" -> "ROG Phone" (generic)
        # Let's be conservative and allow this
        pass

    elif not query_model and matched_model:
        # Match has model token but query doesn't
        # This is suspicious - match is more specific than query
        # e.g., "Find X" -> "Find X9" (match added specificity)
        # Be conservative and allow this (might be variant)
        pass

    # All critical checks passed
    return True


# ---------------------------------------------------------------------------
# Matching logic — recursive brand → attribute → fuzzy
# ---------------------------------------------------------------------------

def compute_confidence_breakdown(query: str, matched: str) -> dict:
    """
    Compute a diagnostic confidence breakdown for a query→matched pair.

    Purely diagnostic — does NOT change any match decisions.
    Useful for debugging why a match was accepted or rejected.

    Returns dict with:
        model_match: bool — do model tokens match?
        storage_match: bool — does storage match?
        category_match: bool — same category?
        watch_mm_match: bool — same watch mm? (or N/A)
        brand_match: bool — same brand?
        composite_score: float — weighted composite (0-100)
        risk_flags: list[str] — potential issues
    """
    risk_flags = []

    # Category
    q_cat = extract_category(query)
    m_cat = extract_category(matched)
    category_match = (q_cat == m_cat) or q_cat == 'other' or m_cat == 'other'
    if not category_match:
        risk_flags.append(f'category_mismatch:{q_cat}→{m_cat}')

    # Model tokens — set-based comparison (order-independent, matching token_sort_ratio)
    q_tokens = extract_model_tokens(query)
    m_tokens = extract_model_tokens(matched)
    model_match = True
    if q_tokens and m_tokens:
        q_set = set(q_tokens)
        m_set = set(m_tokens)
        common = q_set & m_set
        # Primary numeric token: first token with a digit (e.g., "14", "s23", "fold3")
        q_primary = next((t for t in q_tokens if any(c.isdigit() for c in t)), None)
        m_primary = next((t for t in m_tokens if any(c.isdigit() for c in t)), None)
        if not common:
            # No overlap at all
            model_match = False
            risk_flags.append(f'model_no_overlap:{q_tokens}→{m_tokens}')
        elif q_primary and m_primary and q_primary != m_primary:
            # Primary numeric token differs (e.g., "14" vs "15", "s23" vs "s24")
            model_match = False
            risk_flags.append(f'model_primary_mismatch:{q_primary}→{m_primary}')
        elif q_set != m_set:
            # Sets differ — check if difference is significant
            diff = (q_set - m_set) | (m_set - q_set)
            # Filter out year tokens (2014-2026) which are not core model identifiers
            _year_re = re.compile(r'^20[12]\d$')
            significant_diff = {t for t in diff if not _year_re.match(t)}
            if significant_diff:
                # Meaningful model difference (e.g., Pro vs Pro Max — extra "max" token)
                model_match = False
                risk_flags.append(f'model_set_diff:{q_set - m_set}|{m_set - q_set}')
            else:
                # Only year tokens differ — not a real model mismatch
                risk_flags.append(f'model_year_diff:{diff}')
    elif q_tokens and not m_tokens:
        risk_flags.append('query_has_model_but_match_doesnt')
    elif not q_tokens and m_tokens:
        risk_flags.append('match_has_model_but_query_doesnt')

    # Storage
    q_storage = extract_storage(query)
    m_storage = extract_storage(matched)
    storage_match = True
    if q_storage and m_storage and q_storage != m_storage:
        storage_match = False
        risk_flags.append(f'storage_mismatch:{q_storage}→{m_storage}')

    # Watch mm
    q_mm = extract_watch_mm(query)
    m_mm = extract_watch_mm(matched)
    watch_mm_match = True
    if q_mm and m_mm and q_mm != m_mm:
        watch_mm_match = False
        risk_flags.append(f'watch_mm_mismatch:{q_mm}→{m_mm}')

    # Brand (simple check)
    q_words = query.lower().split()
    m_words = matched.lower().split()
    brand_match = bool(set(q_words[:2]) & set(m_words[:2])) if q_words and m_words else True

    # Composite score (weighted)
    composite = 100.0
    if not category_match:
        composite -= 50
    if not model_match:
        composite -= 30
    if not storage_match:
        composite -= 15
    if not watch_mm_match:
        composite -= 20
    if not brand_match:
        composite -= 10
    composite = max(0.0, composite)

    return {
        'model_match': model_match,
        'storage_match': storage_match,
        'category_match': category_match,
        'watch_mm_match': watch_mm_match,
        'brand_match': brand_match,
        'composite_score': composite,
        'risk_flags': risk_flags,
    }


def verification_gate(query_norm: str, cand_norm: str) -> Tuple[bool, List[str]]:
    """
    Strict verification gate applied before returning MATCHED for any fuzzy match.

    Checks four hard constraints:
        1. Category cross-match: both known & different → reject
        2. Storage mismatch: both present & different → reject
        3. Watch mm mismatch: both present & different → reject
        4. Primary model token mismatch: both present & different → reject

    Returns:
        (pass_gate: bool, reasons: list[str])
        If pass_gate is False, the match must NOT be returned as MATCHED.
    """
    reasons = []

    # 1. Category cross-match
    q_cat = extract_category(query_norm)
    m_cat = extract_category(cand_norm)
    if q_cat != 'other' and m_cat != 'other' and q_cat != m_cat:
        reasons.append(f'category_cross:{q_cat}→{m_cat}')

    # 2. Storage mismatch
    q_storage = extract_storage(query_norm)
    m_storage = extract_storage(cand_norm)
    if q_storage and m_storage and q_storage != m_storage:
        reasons.append(f'storage_mismatch:{q_storage}→{m_storage}')

    # 3. Watch mm mismatch
    q_mm = extract_watch_mm(query_norm)
    m_mm = extract_watch_mm(cand_norm)
    if q_mm and m_mm and q_mm != m_mm:
        reasons.append(f'watch_mm_mismatch:{q_mm}→{m_mm}')

    # 4. Model token mismatch (set-based with primary token check)
    q_tokens = extract_model_tokens(query_norm)
    m_tokens = extract_model_tokens(cand_norm)
    if q_tokens and m_tokens:
        # Filter out year tokens (2012, 2023, etc.) — these are catalog metadata, not model identifiers
        _year_re = re.compile(r'^20[012]\d$')
        q_filtered = [t for t in q_tokens if not _year_re.match(t)]
        m_filtered = [t for t in m_tokens if not _year_re.match(t)]
        # Only compare non-year tokens
        if q_filtered and m_filtered:
            if len(q_filtered) != len(m_filtered):
                reasons.append(f'model_token_count:{q_filtered}→{m_filtered}')
            else:
                for qt, mt in zip(q_filtered, m_filtered):
                    if qt != mt:
                        reasons.append(f'model_token_mismatch:{qt}→{mt}')
                        break

    # 5. Material mismatch (watches: aluminium vs steel vs titanium)
    _MATERIAL_GROUPS = {
        'aluminium': ('aluminium', 'aluminum', 'alum'),
        'steel': ('steel', 'stainless'),
        'titanium': ('titanium', 'titan'),
        'ceramic': ('ceramic',),
        'plastic': ('plastic', 'polycarbonate'),
    }

    def _detect_material(text):
        t = text.lower()
        for mat, keywords in _MATERIAL_GROUPS.items():
            if any(kw in t for kw in keywords):
                return mat
        return None

    q_mat = _detect_material(query_norm)
    m_mat = _detect_material(cand_norm)
    if q_mat and m_mat and q_mat != m_mat:
        reasons.append(f'material_mismatch:{q_mat}→{m_mat}')

    passed = len(reasons) == 0
    return passed, reasons


def match_laptop_by_attributes(
    query: str,
    input_brand: str,
    original_input: str,
    search_names: List[str],
    search_lookup: Dict[str, List[str]],
    nl_catalog: Optional[pd.DataFrame] = None,
) -> Optional[dict]:
    """
    Match laptops by attributes instead of model numbers.

    For Windows laptops, ignore model numbers (SP513-55N, UX325, etc.) and match by:
    - Brand (already filtered)
    - Series (Spin 5, ZenBook, VivoBook)
    - Processor tier (i3, i5, i7, i9)
    - Generation (11th Gen, 8th Gen, etc.)
    - RAM (8GB, 16GB, etc.)
    - Storage (256GB SSD, 512GB SSD, etc.)

    Returns match dict or None if no good match found.
    """
    # Extract attributes from query
    query_attrs = extract_laptop_attributes(query, input_brand)

    # Required attributes for matching
    query_processor = query_attrs.get('processor', '')
    query_gen = query_attrs.get('generation', '')
    query_ram = query_attrs.get('ram', '')
    query_storage = query_attrs.get('storage', '')
    query_line = query_attrs.get('product_line', '')

    if not (query_processor and query_ram and query_storage):
        # Missing critical attributes, fall back to fuzzy matching
        return None

    # Score each candidate by attribute matching
    best_score = 0
    best_match = None
    best_match_name = ''

    for nl_name in search_names:
        # Skip non-laptops
        if not is_laptop_product(nl_name):
            continue

        # Extract attributes from NL candidate
        nl_attrs = extract_laptop_attributes(nl_name, input_brand)
        nl_processor = nl_attrs.get('processor', '')
        nl_gen = nl_attrs.get('generation', '')
        nl_ram = nl_attrs.get('ram', '')
        nl_storage = nl_attrs.get('storage', '')
        nl_line = nl_attrs.get('product_line', '')

        # Attribute-based scoring (0-100 scale)
        score = 0

        # CRITICAL: Processor tier must match exactly (i5 != i7)
        if query_processor != nl_processor:
            continue  # Skip this candidate entirely
        else:
            score += 30  # Processor match is critical

        # CRITICAL: RAM must match exactly (8GB != 16GB)
        if query_ram != nl_ram:
            continue  # Skip this candidate entirely
        else:
            score += 25  # RAM match is critical

        # CRITICAL: Storage must match exactly (256GB != 512GB)
        if query_storage != nl_storage:
            continue  # Skip this candidate entirely
        else:
            score += 25  # Storage match is critical

        # Generation: Exact match preferred, ±1 generation acceptable
        if query_gen and nl_gen:
            if query_gen == nl_gen:
                score += 15  # Exact generation match
            else:
                # Try to extract numeric generation for tolerance check
                query_gen_num = re.search(r'(\d+)', query_gen)
                nl_gen_num = re.search(r'(\d+)', nl_gen)
                if query_gen_num and nl_gen_num:
                    diff = abs(int(query_gen_num.group(1)) - int(nl_gen_num.group(1)))
                    if diff == 1:
                        score += 10  # ±1 generation tolerance
                    else:
                        continue  # Too far apart, skip
                else:
                    continue  # Can't compare generations
        elif query_gen or nl_gen:
            # One has generation, other doesn't - skip
            continue
        else:
            # Neither has generation (older laptops)
            score += 5

        # Product line: CRITICAL - Must match if both specified
        # Prevents: MacBook Air→Pro, Aspire→Predator, etc.
        if query_line and nl_line:
            # Check for exact or partial match (e.g., "macbook pro" matches "macbook pro 13")
            if query_line == nl_line or query_line in nl_line or nl_line in query_line:
                score += 15  # Product line match is critical for laptops
            else:
                # Different series (Air vs Pro, Aspire vs Predator) - skip entirely
                continue
        elif query_line or nl_line:
            # One has series, other doesn't - allow with reduced confidence
            score += 5

        if score > best_score:
            best_score = score
            best_match_name = nl_name

    if best_score >= 85:  # Minimum 85% attribute match (processor + RAM + storage + series = 95 points base)
        asset_ids = search_lookup.get(best_match_name, [])

        # Auto-select if multiple variants
        if len(asset_ids) > 1 and nl_catalog is not None:
            selection = auto_select_matching_variant(original_input, asset_ids, nl_catalog)
            return {
                'mapped_uae_assetid': selection['selected_id'],
                'match_score': round(best_score, 2),
                'match_status': MATCH_STATUS_MATCHED,
                'confidence': CONFIDENCE_HIGH,
                'matched_on': best_match_name,
                'method': 'laptop_attribute_match',
                'auto_selected': selection['auto_selected'],
                'selection_reason': selection['reason'],
                'alternatives': selection['alternatives'],
            }
        elif len(asset_ids) == 1:
            return {
                'mapped_uae_assetid': asset_ids[0],
                'match_score': round(best_score, 2),
                'match_status': MATCH_STATUS_MATCHED,
                'confidence': CONFIDENCE_HIGH,
                'matched_on': best_match_name,
                'method': 'laptop_attribute_match',
                'auto_selected': False,
                'selection_reason': '',
                'alternatives': [],
            }
        elif len(asset_ids) > 1:
            # Multiple IDs but no catalog
            return {
                'mapped_uae_assetid': ', '.join(asset_ids),
                'match_score': round(best_score, 2),
                'match_status': MATCH_STATUS_MULTIPLE_MATCHES,
                'confidence': CONFIDENCE_MEDIUM,
                'matched_on': best_match_name,
                'method': 'laptop_attribute_match',
                'auto_selected': False,
                'selection_reason': '',
                'alternatives': [],
            }

    return None  # No good match found


def match_single_item(
    query: str,
    nl_lookup: Dict[str, List[str]],
    nl_names: List[str],
    threshold: int = SIMILARITY_THRESHOLD,
    brand_index: Optional[Dict] = None,
    input_brand: str = '',
    attribute_index: Optional[Dict] = None,
    nl_catalog: Optional[pd.DataFrame] = None,
    original_input: str = '',
    input_category: str = '',
) -> dict:
    """
    Match a single product against the NL list using hybrid matching.

    Matching strategy (cascading filters with fast path):
        0. ATTRIBUTE MATCHING (fast path): Try exact attribute match first
           - Handles 70-80% of queries in 2-5ms
           - Works especially well for Samsung (strips model codes), iPhone, Pixel, Galaxy
        1. BRAND FILTER: If brand is known, search only within that brand's products
           (e.g., 9,894 → ~2,000 Apple records). Eliminates cross-brand errors.
        2. CATEGORY FILTER: Prevent cross-category matches (Tab vs Watch, Mobile vs Laptop)
        3. STORAGE FILTER: If storage is detected (e.g., "16gb"), prefer candidates
           with the same storage. Prevents "16GB" matching "128GB" variants.
        4. FUZZY MATCH: token_sort_ratio on the narrowed candidate list.
        5. MODEL TOKEN GUARD: Reject if model tokens differ (e.g., iPhone 4 vs 6).
        6. AUTO-SELECT: If multiple IDs found, automatically select best variant based on
           user's exact specs (year, connectivity, etc.)

    Falls back through levels if earlier levels don't produce confident matches.
    """
    no_match_result = {
        'mapped_uae_assetid': '',
        'match_score': 0,
        'match_status': MATCH_STATUS_NO_MATCH,
        'confidence': CONFIDENCE_LOW,
        'matched_on': '',
        'method': 'none',
        'auto_selected': False,
        'selection_reason': '',
        'alternatives': [],
    }

    if not isinstance(query, str) or not query.strip():
        return no_match_result

    try:
        return _match_single_item_inner(
            query, nl_lookup, nl_names, threshold, brand_index,
            input_brand, attribute_index, nl_catalog, original_input,
            input_category, no_match_result,
        )
    except Exception:
        return no_match_result


def _match_single_item_inner(
    query, nl_lookup, nl_names, threshold, brand_index,
    input_brand, attribute_index, nl_catalog, original_input,
    input_category, no_match_result,
) -> dict:
    """Inner implementation of match_single_item (wrapped by try/except)."""
    # --- Level 0: Attribute-based matching (FAST PATH) ---
    if attribute_index and input_brand:
        attr_match = try_attribute_match(query, input_brand, attribute_index, nl_catalog, original_input)
        if attr_match:
            return attr_match  # Found exact match, skip fuzzy entirely

    # --- Level 1: Brand partitioning ---
    search_lookup = nl_lookup
    search_names = nl_names
    brand_norm = normalize_brand(input_brand) if input_brand else ''
    if not brand_norm:
        brand_norm = normalize_text(input_brand) if input_brand else ''

    if brand_index and brand_norm and brand_norm in brand_index:
        # Narrow search to this brand's products only
        brand_data = brand_index[brand_norm]
        search_lookup = brand_data['lookup']
        search_names = brand_data['names']

    # --- Level 2: Category filtering (MANDATORY & STRICT) ---
    # CRITICAL FIX: Always apply category filtering to prevent cross-category errors
    # (Tablet → Phone, Watch → Phone, etc.)
    # ENHANCEMENT: Use actual uploaded category if provided, otherwise extract from query
    if input_category:
        # Normalize uploaded category to match NL catalog categories
        input_cat_lower = input_category.lower().strip()
        if input_cat_lower in ['mobile', 'mobile phone', 'phone']:
            query_category = 'mobile'
        elif input_cat_lower in ['tablet', 'tab']:
            query_category = 'tablet'
        elif input_cat_lower in ['laptop']:
            query_category = 'laptop'
        elif input_cat_lower in ['smartwatch', 'watch']:
            query_category = 'watch'
        else:
            query_category = input_cat_lower
    else:
        # Fall back to extracting category from product name
        query_category = extract_category(query)

    if query_category != 'other':
        # Filter candidates to same category (prevent Tab matching Watch, etc.)
        category_filtered = [n for n in search_names if extract_category(n) == query_category]
        if category_filtered:
            search_names = category_filtered
        else:
            # NO matches in the same category → product doesn't exist in NL catalog
            # Return NO_MATCH instead of allowing cross-category fallback
            # This prevents Tablet→Phone, Watch→Phone errors
            return no_match_result

    # --- Level 2.5: Laptop attribute-based matching (SPECIAL PATH FOR LAPTOPS) ---
    # For Windows laptops, use attribute matching instead of fuzzy matching
    # to ignore model numbers (SP513-55N, UX325, etc.)
    if query_category == 'laptop' and is_laptop_product(query):
        laptop_match = match_laptop_by_attributes(
            query, input_brand, original_input,
            search_names, search_lookup, nl_catalog
        )
        if laptop_match:
            return laptop_match  # Found good attribute match, skip fuzzy matching

    # --- Level 3: Storage pre-filter ---
    query_storage = extract_storage(query)
    if query_storage and len(search_names) > 20:
        # Filter candidates to those with the same storage
        storage_filtered = [n for n in search_names if query_storage in n]
        if storage_filtered:
            search_names = storage_filtered

    # --- Level 4: Fuzzy match on narrowed candidates ---
    # Safety: raise threshold for fully unscoped searches (no brand, no category)
    # to prevent generic queries from false-matching against the full 10K catalog
    effective_threshold = threshold
    if not brand_norm and query_category == 'other':
        effective_threshold = max(threshold, HIGH_CONFIDENCE_THRESHOLD)

    result = process.extractOne(
        query,
        search_names,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=effective_threshold,
    )

    # If brand-filtered search found nothing, fall back to full NL search
    # BUT re-apply category filtering to prevent cross-category matches
    if result is None and (search_names is not nl_names):
        # Re-apply category filtering to full NL catalog
        fallback_names = nl_names
        if query_category != 'other':
            category_filtered = [n for n in fallback_names if extract_category(n) == query_category]
            if category_filtered:
                fallback_names = category_filtered
            else:
                # No same-category products in entire catalog → return NO_MATCH
                return no_match_result

        result = process.extractOne(
            query,
            fallback_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=effective_threshold,
        )
        search_lookup = nl_lookup  # use full lookup for ID resolution

    if result is None:
        # --- Near-miss recovery: 80-84 score band → REVIEW_REQUIRED if gate passes ---
        # Only attempt if threshold is the default (don't override raised thresholds)
        near_miss_cutoff = 80
        if effective_threshold <= SIMILARITY_THRESHOLD:
            near_miss_result = process.extractOne(
                query, search_names,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=near_miss_cutoff,
            )
            if near_miss_result is not None:
                nm_match, nm_score, _ = near_miss_result
                nm_ids = search_lookup.get(nm_match, [])
                if not nm_ids:
                    nm_ids = nl_lookup.get(nm_match, [])
                gate_pass, gate_reasons = verification_gate(query, nm_match)
                if gate_pass and nm_ids:
                    # Gate passed: surface as REVIEW_REQUIRED (never auto-MATCHED)
                    # Get top3 candidates for human reviewer
                    top3 = process.extract(
                        query, search_names,
                        scorer=fuzz.token_sort_ratio,
                        limit=3,
                    )
                    alternatives = [{'name': n, 'score': round(s, 2)} for n, s, _ in top3]
                    return {
                        'mapped_uae_assetid': ', '.join(nm_ids),
                        'match_score': round(nm_score, 2),
                        'match_status': MATCH_STATUS_SUGGESTED,
                        'confidence': CONFIDENCE_LOW,
                        'matched_on': nm_match,
                        'method': 'fuzzy_near_miss_recovery',
                        'auto_selected': False,
                        'selection_reason': f'near_miss_recovery(score={round(nm_score, 2)})',
                        'alternatives': alternatives,
                    }
        return no_match_result

    best_match, score, _ = result
    asset_ids = search_lookup.get(best_match, [])
    # Also check full lookup in case brand subset didn't have the ID mapping
    if not asset_ids:
        asset_ids = nl_lookup.get(best_match, [])

    # --- Level 5: Model token guardrail ---
    # Applied to ALL scores (including >= 95%) to prevent false positives
    # like Pixel 9 → Pixel 3 (95%), Mate 20 → Mate 40 (95%),
    # Nova 5T → Nova 5i (95%), A57 → A57s (96%)
    # Pro vs Pro Max (different products!), Plus variants, XL variants
    q_tokens = extract_model_tokens(query)
    m_tokens = extract_model_tokens(best_match)
    if q_tokens and m_tokens:
        # CRITICAL: First check if token counts differ (catches Pro vs Pro Max!)
        # zip() only compares overlapping tokens, so we'd miss the 'max' difference
        if len(q_tokens) != len(m_tokens):
            score = min(score, threshold - 1)  # Demote to NO_MATCH
        else:
            # Same count → compare position by position (e.g., "5t" vs "5i", "s23" vs "s24")
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

    # Watch mm guardrail: demote if mm values differ (38-55mm range is watch-specific)
    q_mm = extract_watch_mm(query)
    m_mm = extract_watch_mm(best_match)
    if q_mm and m_mm and q_mm != m_mm:
        score = min(score, threshold - 1)  # Demote to NO_MATCH

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
            'auto_selected': False,
            'selection_reason': '',
            'alternatives': [],
        }
    elif confidence == CONFIDENCE_HIGH:
        # --- Verification gate: strict check before allowing MATCHED ---
        gate_pass, gate_reasons = verification_gate(query, best_match)
        if not gate_pass:
            # Gate failed: demote HIGH to REVIEW_REQUIRED (never auto-accept)
            return {
                'mapped_uae_assetid': ', '.join(asset_ids),
                'match_score': score_rounded,
                'match_status': MATCH_STATUS_SUGGESTED,
                'confidence': CONFIDENCE_MEDIUM,
                'matched_on': best_match,
                'method': 'fuzzy_gate_blocked',
                'auto_selected': False,
                'selection_reason': f'gate_fail: {"; ".join(gate_reasons)}',
                'alternatives': [],
            }
        # --- Level 6: Auto-select for MULTIPLE_MATCHES ---
        if len(asset_ids) > 1 and nl_catalog is not None:
            # Auto-select best variant based on user's exact specs
            # Use original_input (before normalization) to detect 5G/4G/years correctly
            user_input_for_auto_select = original_input if original_input else query
            selection = auto_select_matching_variant(user_input_for_auto_select, asset_ids, nl_catalog)

            return {
                'mapped_uae_assetid': selection['selected_id'],
                'match_score': score_rounded,
                'match_status': MATCH_STATUS_MATCHED,  # Auto-selected -> MATCHED
                'confidence': confidence,
                'matched_on': best_match,
                'method': 'fuzzy_auto_selected',
                'auto_selected': selection['auto_selected'],
                'selection_reason': selection['reason'],
                'alternatives': selection['alternatives'],
            }
        else:
            # Single match or no catalog provided
            return {
                'mapped_uae_assetid': ', '.join(asset_ids),
                'match_score': score_rounded,
                'match_status': MATCH_STATUS_MULTIPLE if len(asset_ids) > 1 else MATCH_STATUS_MATCHED,
                'confidence': confidence,
                'matched_on': best_match,
                'method': 'fuzzy',
                'auto_selected': False,
                'selection_reason': '',
                'alternatives': [],
            }
    else:
        # MEDIUM confidence (85-94%): Apply attribute verification
        # If critical attributes match, upgrade to MATCHED
        # Otherwise, keep as REVIEW_REQUIRED for human verification

        # --- Soft Similarity Upgrade ---
        # Score >= 88 with ALL key attributes matching → safe to upgrade to MATCHED
        # This recovers items in the 88-89 band that are clearly correct matches
        # but fall just below the 90 HIGH_CONFIDENCE_THRESHOLD
        SOFT_UPGRADE_THRESHOLD = 88
        if score >= SOFT_UPGRADE_THRESHOLD:
            gate_pass_soft, gate_reasons_soft = verification_gate(query, best_match)
            if gate_pass_soft:
                # All 4 gate checks passed (category, storage, mm, model tokens)
                # Safe to upgrade — the match is correct, just scored slightly below 90
                if len(asset_ids) > 1 and nl_catalog is not None:
                    user_input_for_auto_select = original_input if original_input else query
                    selection = auto_select_matching_variant(user_input_for_auto_select, asset_ids, nl_catalog)
                    return {
                        'mapped_uae_assetid': selection['selected_id'],
                        'match_score': score_rounded,
                        'match_status': MATCH_STATUS_MATCHED,
                        'confidence': CONFIDENCE_MEDIUM,
                        'matched_on': best_match,
                        'method': 'fuzzy_soft_upgrade_auto_selected',
                        'auto_selected': selection['auto_selected'],
                        'selection_reason': selection['reason'],
                        'alternatives': selection['alternatives'],
                    }
                else:
                    return {
                        'mapped_uae_assetid': ', '.join(asset_ids),
                        'match_score': score_rounded,
                        'match_status': MATCH_STATUS_MULTIPLE if len(asset_ids) > 1 else MATCH_STATUS_MATCHED,
                        'confidence': CONFIDENCE_MEDIUM,
                        'matched_on': best_match,
                        'method': 'fuzzy_soft_upgrade',
                        'auto_selected': False,
                        'selection_reason': f'soft_upgrade(score={score_rounded}>=88,gate=pass)',
                        'alternatives': [],
                    }

        verified = verify_critical_attributes(query, best_match)

        if verified:
            # Additional gate: even if attributes verify, run strict gate
            gate_pass, gate_reasons = verification_gate(query, best_match)
            if not gate_pass:
                # Gate failed: keep as REVIEW, don't upgrade to MATCHED
                return {
                    'mapped_uae_assetid': ', '.join(asset_ids),
                    'match_score': score_rounded,
                    'match_status': MATCH_STATUS_SUGGESTED,
                    'confidence': confidence,
                    'matched_on': best_match,
                    'method': 'fuzzy_verified_gate_blocked',
                    'auto_selected': False,
                    'selection_reason': f'gate_fail: {"; ".join(gate_reasons)}',
                    'alternatives': [],
                }
            verified = True  # gate passed, continue to MATCHED upgrade
            # All critical attributes match -> safe to auto-accept
            # Check for auto-select if multiple IDs
            if len(asset_ids) > 1 and nl_catalog is not None:
                user_input_for_auto_select = original_input if original_input else query
                selection = auto_select_matching_variant(user_input_for_auto_select, asset_ids, nl_catalog)

                return {
                    'mapped_uae_assetid': selection['selected_id'],
                    'match_score': score_rounded,
                    'match_status': MATCH_STATUS_MATCHED,
                    'confidence': confidence,
                    'matched_on': best_match,
                    'method': 'fuzzy_verified_auto_selected',
                    'auto_selected': selection['auto_selected'],
                    'selection_reason': selection['reason'],
                    'alternatives': selection['alternatives'],
                }
            else:
                return {
                    'mapped_uae_assetid': ', '.join(asset_ids),
                    'match_score': score_rounded,
                    'match_status': MATCH_STATUS_MULTIPLE if len(asset_ids) > 1 else MATCH_STATUS_MATCHED,
                    'confidence': confidence,
                    'matched_on': best_match,
                    'method': 'fuzzy_verified',
                    'auto_selected': False,
                    'selection_reason': '',
                    'alternatives': [],
                }
        else:
            # Critical attributes differ -> needs human review
            return {
                'mapped_uae_assetid': ', '.join(asset_ids),
                'match_score': score_rounded,
                'match_status': MATCH_STATUS_SUGGESTED,
                'confidence': confidence,
                'matched_on': best_match,
                'method': 'fuzzy',
                'auto_selected': False,
                'selection_reason': '',
                'alternatives': [],
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
    nl_catalog: Optional[pd.DataFrame] = None,
    diagnostic: bool = False,
) -> pd.DataFrame:
    """
    Run hybrid matching for an entire input DataFrame against the NL lookup.

    Matching is hybrid (attribute-based fast path + fuzzy fallback):
        0. Attribute matching (fast path) → 70-80% of queries in 2-5ms
        1. Brand partition → narrows search to one brand
        2. Category filter → prevents cross-category errors
        3. Storage filter → narrows to same storage variant
        4. Fuzzy match → finds best candidate
        5. Model token guard → rejects wrong model tokens
        6. Auto-select → automatically selects best variant from multiple matches

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
        nl_catalog: NL catalog DataFrame for auto-select (optional)

    Returns:
        Copy of df_input with added columns:
            mapped_uae_assetid, match_score, match_status, confidence, matched_on,
            auto_selected, selection_reason, alternatives
    """
    df = df_input.copy()
    total = len(df)

    # Strip whitespace from column names (common issue: "Foxway Product Name " trailing space)
    df.columns = [str(c).strip() for c in df.columns]
    # Also strip the caller-provided column names to match
    brand_col = brand_col.strip() if brand_col else brand_col
    name_col = name_col.strip() if name_col else name_col

    # Detect category and storage columns using role-based detection
    # Handles variations: 'type', 'Category', 'DEVICE TYPE', 'device_type', etc.
    category_col = _detect_category_column(df.columns.tolist())
    storage_col = _detect_storage_column(df.columns.tolist())

    results = []
    for idx, row in df.iterrows():
        try:
            input_brand = str(row.get(brand_col, '')).strip() if brand_col != '__no_brand__' else ''
            original_product_name = str(row.get(name_col, '')).strip()

            # Brand inference: if brand is missing, try to extract from product name
            if not input_brand or input_brand.lower() in ('nan', 'none', ''):
                inferred = _infer_brand_from_name(original_product_name)
                if inferred:
                    input_brand = inferred

            # Extract category from uploaded data if available
            input_category = str(row.get(category_col, '')).strip() if category_col else ''

            # ENHANCEMENT: If storage/capacity column exists, combine it with product name
            # This improves matching for datasets that separate model and capacity
            # Example: "iPad Pro 2022 11" + "128GB" → "iPad Pro 2022 11 128GB"
            if storage_col:
                storage_value = str(row.get(storage_col, '')).strip()
                if storage_value:
                    # Combine name + storage for better matching
                    original_product_name = f"{original_product_name} {storage_value}"

            query = build_match_string(input_brand, original_product_name)
            match_result = match_single_item(
                query, nl_lookup, nl_names, threshold,
                brand_index=brand_index,
                input_brand=input_brand,
                attribute_index=attribute_index,
                nl_catalog=nl_catalog,
                original_input=original_product_name,
                input_category=input_category,  # NEW - pass actual category from uploaded data
            )
        except Exception:
            match_result = {
                'mapped_uae_assetid': '',
                'match_score': 0,
                'match_status': MATCH_STATUS_NO_MATCH,
                'confidence': CONFIDENCE_LOW,
                'matched_on': '',
                'method': 'error',
                'auto_selected': False,
                'selection_reason': '',
                'alternatives': [],
            }

        # --- Verification columns (always included for Excel export) ---
        matched_on = match_result.get('matched_on', '')
        if matched_on:
            gate_p, gate_r = verification_gate(query, matched_on)
            match_result['verification_pass'] = gate_p
            match_result['verification_reasons'] = '; '.join(gate_r) if gate_r else ''
        else:
            match_result['verification_pass'] = True
            match_result['verification_reasons'] = ''

        # --- Diagnostic columns (optional, off by default for performance) ---
        if diagnostic:
            match_result['query_category'] = extract_category(query)
            match_result['matched_category'] = extract_category(matched_on) if matched_on else ''
            match_result['query_storage'] = extract_storage(query)
            match_result['matched_storage'] = extract_storage(matched_on) if matched_on else ''
            match_result['query_model_tokens'] = str(extract_model_tokens(query))
            match_result['matched_model_tokens'] = str(extract_model_tokens(matched_on)) if matched_on else '[]'
            # verification_pass and verification_reasons already set above (unconditional)
            # Top3 candidates for REVIEW/NO_MATCH only (expensive)
            if match_result.get('match_status') in (MATCH_STATUS_SUGGESTED, MATCH_STATUS_NO_MATCH):
                top3 = process.extract(query, nl_names, scorer=fuzz.token_sort_ratio, limit=3)
                for i, (name, sc, _) in enumerate(top3, 1):
                    match_result[f'top{i}_name'] = name
                    match_result[f'top{i}_score'] = round(sc, 2)
                # Pad if fewer than 3
                for i in range(len(top3) + 1, 4):
                    match_result[f'top{i}_name'] = ''
                    match_result[f'top{i}_score'] = 0.0
            else:
                for i in range(1, 4):
                    match_result[f'top{i}_name'] = ''
                    match_result[f'top{i}_score'] = 0.0

        results.append(match_result)

        if progress_callback and (len(results) % 50 == 0 or len(results) == total):
            progress_callback(len(results), total)

    results_df = pd.DataFrame(results)
    df['mapped_uae_assetid'] = results_df['mapped_uae_assetid'].values
    df['match_score'] = results_df['match_score'].values
    df['match_status'] = results_df['match_status'].values
    df['confidence'] = results_df['confidence'].values
    df['matched_on'] = results_df['matched_on'].values
    df['method'] = results_df['method'].values
    df['auto_selected'] = results_df['auto_selected'].values
    df['selection_reason'] = results_df['selection_reason'].values
    df['alternatives'] = results_df['alternatives'].values
    df['verification_pass'] = results_df['verification_pass'].values
    df['verification_reasons'] = results_df['verification_reasons'].values

    if diagnostic:
        for col in ['query_category', 'matched_category', 'query_storage', 'matched_storage',
                     'query_model_tokens', 'matched_model_tokens',
                     'top1_name', 'top1_score', 'top2_name',
                     'top2_score', 'top3_name', 'top3_score']:
            if col in results_df.columns:
                df[col] = results_df[col].values

    return df


# ---------------------------------------------------------------------------
# Coverage Dashboard Metrics
# ---------------------------------------------------------------------------

def compute_coverage_metrics(df_results: pd.DataFrame) -> Dict[str, any]:
    """
    Compute coverage dashboard metrics from a completed matching result DataFrame.

    Returns a dict with:
        total_rows: int — total items processed
        matched_count / matched_rate: MATCHED items
        review_count / review_rate: REVIEW_REQUIRED items
        no_match_count / no_match_rate: NO_MATCH items
        multiple_count: MULTIPLE_MATCHES items
        near_miss_count: NO_MATCH items where top candidate scored 80-84
        false_positive_risk_count: MATCHED items where verification_gate would fail
        avg_match_score: average score of MATCHED items
        method_breakdown: dict of method → count
    """
    total = len(df_results)
    if total == 0:
        return {'total_rows': 0, 'matched_count': 0, 'matched_rate': 0.0,
                'review_count': 0, 'review_rate': 0.0,
                'no_match_count': 0, 'no_match_rate': 0.0,
                'multiple_count': 0, 'near_miss_count': 0,
                'false_positive_risk_count': 0, 'avg_match_score': 0.0,
                'method_breakdown': {}}

    status_col = 'match_status'
    matched = df_results[df_results[status_col] == MATCH_STATUS_MATCHED]
    review = df_results[df_results[status_col] == MATCH_STATUS_SUGGESTED]
    no_match = df_results[df_results[status_col] == MATCH_STATUS_NO_MATCH]
    multiple = df_results[df_results[status_col] == MATCH_STATUS_MULTIPLE]

    # Near-miss: NO_MATCH items with score >= 80
    near_miss = no_match[no_match['match_score'] >= 80] if 'match_score' in no_match.columns else pd.DataFrame()

    # False-positive risk: MATCHED items where verification gate would fail
    fp_risk = 0
    if len(matched) > 0 and 'matched_on' in matched.columns:
        for _, row in matched.iterrows():
            query_norm = str(row.get('matched_on', ''))
            # We can't re-derive query easily here, so check verification_pass if available
            if 'verification_pass' in row and row['verification_pass'] == False:
                fp_risk += 1

    # Method breakdown
    method_breakdown = {}
    if 'method' in df_results.columns:
        method_breakdown = df_results['method'].value_counts().to_dict()

    avg_score = round(matched['match_score'].mean(), 2) if len(matched) > 0 else 0.0

    return {
        'total_rows': total,
        'matched_count': len(matched),
        'matched_rate': round(len(matched) / total * 100, 1),
        'review_count': len(review),
        'review_rate': round(len(review) / total * 100, 1),
        'no_match_count': len(no_match),
        'no_match_rate': round(len(no_match) / total * 100, 1),
        'multiple_count': len(multiple),
        'near_miss_count': len(near_miss),
        'false_positive_risk_count': fp_risk,
        'avg_match_score': avg_score,
        'method_breakdown': method_breakdown,
    }


# ---------------------------------------------------------------------------
# Catalog Gap Detector
# ---------------------------------------------------------------------------

def detect_catalog_gaps(
    df_results: pd.DataFrame,
    nl_catalog: Optional[pd.DataFrame] = None,
) -> Dict[str, any]:
    """
    Analyze NO_MATCH items to identify catalog gaps and improvement opportunities.

    Returns a dict with:
        unmatched_brands: dict of brand → count for NO_MATCH items
        high_volume_unmatched: list of product names appearing >= 3 times as NO_MATCH
        near_miss_candidates: list of dicts with query, top_candidate, score for 80-84 band
        brand_coverage: dict of brand → {matched, total, rate} for each brand
        category_coverage: dict of category → {matched, total, rate}
    """
    total = len(df_results)
    if total == 0:
        return {'unmatched_brands': {}, 'high_volume_unmatched': [],
                'near_miss_candidates': [], 'brand_coverage': {},
                'category_coverage': {}}

    status_col = 'match_status'
    no_match = df_results[df_results[status_col] == MATCH_STATUS_NO_MATCH]

    # --- Unmatched brands ---
    unmatched_brands = {}
    brand_col_candidates = [c for c in df_results.columns
                            if c.lower().strip() in ('brand', 'manufacturer', 'make', 'oem')]
    brand_col = brand_col_candidates[0] if brand_col_candidates else None
    if brand_col and brand_col in no_match.columns:
        unmatched_brands = no_match[brand_col].astype(str).str.strip().str.lower().value_counts().to_dict()

    # --- High-volume unmatched ---
    # Find product names that appear multiple times as NO_MATCH
    name_col_candidates = [c for c in df_results.columns
                           if any(kw in c.lower() for kw in ['name', 'product', 'model', 'foxway'])]
    name_col = name_col_candidates[0] if name_col_candidates else None
    high_volume = []
    if name_col and name_col in no_match.columns:
        name_counts = no_match[name_col].astype(str).str.strip().str.lower().value_counts()
        high_volume = [
            {'product_name': name, 'count': int(count)}
            for name, count in name_counts.items()
            if count >= 3
        ]
        high_volume.sort(key=lambda x: x['count'], reverse=True)

    # --- Near-miss candidates (80-84 score band) ---
    near_miss_candidates = []
    nm_rows = no_match[(no_match['match_score'] >= 80) & (no_match['match_score'] < 85)]
    for _, row in nm_rows.head(50).iterrows():  # Cap at 50 for performance
        near_miss_candidates.append({
            'matched_on': str(row.get('matched_on', '')),
            'score': row.get('match_score', 0),
        })

    # --- Brand coverage ---
    brand_coverage = {}
    if brand_col and brand_col in df_results.columns:
        for brand, group in df_results.groupby(df_results[brand_col].astype(str).str.strip().str.lower()):
            if brand in ('nan', 'none', ''):
                continue
            matched_count = len(group[group[status_col] == MATCH_STATUS_MATCHED])
            brand_coverage[brand] = {
                'matched': matched_count,
                'total': len(group),
                'rate': round(matched_count / len(group) * 100, 1) if len(group) > 0 else 0.0,
            }

    # --- Category coverage ---
    category_coverage = {}
    cat_col_candidates = [c for c in df_results.columns
                          if c.lower().strip() in ('type', 'category', 'device type', 'device_type')]
    cat_col = cat_col_candidates[0] if cat_col_candidates else None
    if cat_col and cat_col in df_results.columns:
        for cat, group in df_results.groupby(df_results[cat_col].astype(str).str.strip().str.lower()):
            if cat in ('nan', 'none', ''):
                continue
            matched_count = len(group[group[status_col] == MATCH_STATUS_MATCHED])
            category_coverage[cat] = {
                'matched': matched_count,
                'total': len(group),
                'rate': round(matched_count / len(group) * 100, 1) if len(group) > 0 else 0.0,
            }

    return {
        'unmatched_brands': unmatched_brands,
        'high_volume_unmatched': high_volume,
        'near_miss_candidates': near_miss_candidates,
        'brand_coverage': brand_coverage,
        'category_coverage': category_coverage,
    }


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
    nl_catalog: Optional[pd.DataFrame] = None,
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
    brand_norm = normalize_brand(brand) if brand else ''
    if not brand_norm:
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
                             attribute_index=attribute_index, nl_catalog=nl_catalog,
                             original_input=name)

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

# Column role detection keywords (role-based approach for better accuracy)
# Separate keywords for each column role to prevent conflicts
BRAND_KEYWORDS = ['manufacturer', 'brand', 'make', 'oem', 'vendor']
CATEGORY_KEYWORDS = ['type', 'category', 'device type', 'device_type', 'devicetype', 'product type', 'product_type']
NAME_KEYWORDS = ['name', 'product', 'model', 'description', 'desc', 'foxway', 'device', 'item', 'asset', 'equipment']
STORAGE_KEYWORDS = ['capacity', 'storage', 'size', 'memory']
# Columns to EXCLUDE from name detection (these are IDs, not product names)
NAME_EXCLUDE_KEYWORDS = ['id', 'serial', 'imei', 'barcode', 'sku', 'code', 'number']

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


def _detect_brand_column(columns: List[str]) -> str:
    """Detect the brand/manufacturer column."""
    for col in columns:
        col_lower = col.lower().strip()
        if any(kw in col_lower for kw in BRAND_KEYWORDS):
            return col
    return None


def _detect_category_column(columns: List[str]) -> str:
    """
    Detect the category/type column.
    Handles variations like 'type', 'Category', 'DEVICE TYPE', 'device_type'.
    """
    for col in columns:
        # Normalize: lowercase and replace spaces with underscores
        col_normalized = col.lower().strip().replace(' ', '_')
        if any(kw.replace(' ', '_') in col_normalized for kw in CATEGORY_KEYWORDS):
            return col
    return None


def _detect_name_column(columns: List[str]) -> str:
    """
    Detect the product name column.
    Priority: model > name > product > description
    Excludes category columns to prevent conflicts.
    """
    # Priority 1: Look for "model" keyword first
    for col in columns:
        col_lower = col.lower().strip()
        if 'model' in col_lower and 'type' not in col_lower:
            # Exclude ID-like columns (e.g., "Model Number", "Model ID")
            if any(excl in col_lower for excl in NAME_EXCLUDE_KEYWORDS):
                continue
            return col

    # Priority 2: Look for other name keywords, but exclude category and ID columns
    for col in columns:
        col_lower = col.lower().strip()
        col_normalized = col_lower.replace(' ', '_')

        # Skip if this looks like a category column
        if any(kw.replace(' ', '_') in col_normalized for kw in CATEGORY_KEYWORDS):
            continue

        # Skip if this looks like an ID column (e.g., "Asset ID", "Serial Number")
        if any(excl in col_lower for excl in NAME_EXCLUDE_KEYWORDS):
            continue

        # Check name keywords
        if any(kw in col_lower for kw in NAME_KEYWORDS):
            return col

    return None


def _detect_storage_column(columns: List[str]) -> str:
    """Detect the storage/capacity column."""
    for col in columns:
        col_lower = col.lower().strip()
        if any(kw in col_lower for kw in STORAGE_KEYWORDS):
            return col
    return None


def _detect_columns(columns: List[str]) -> Dict[str, str]:
    """
    Role-based column detection (more accurate than keyword matching).

    Detects each column role separately to prevent conflicts:
    - brand_col: Brand, Manufacturer, Make
    - name_col: Model, Product Name, Description
    - category_col: type, Category, DEVICE TYPE (for category filtering)

    Returns dict with:
        'brand_col': column name for brand/manufacturer (or None)
        'name_col':  column name for product name (required)
        'category_col': column name for category/type (optional, for filtering)
    """
    result = {
        'brand_col': _detect_brand_column(columns),
        'category_col': _detect_category_column(columns),
        'name_col': _detect_name_column(columns),
        'storage_col': _detect_storage_column(columns),
    }

    # Fallback: if no name column detected, use the last non-category column
    if result['name_col'] is None and len(columns) > 0:
        # Use the last column that isn't the category column
        for col in reversed(columns):
            if col != result['category_col']:
                result['name_col'] = col
                break

        # If still None, just use the last column
        if result['name_col'] is None:
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
    Parse all asset-list sheets from an uploaded Excel or CSV file.

    Automatically:
        - Handles both .xlsx and .csv files
        - Skips sheets that look like the NL reference (Excel only)
        - Detects the header row (skips title rows)
        - Detects brand and product-name columns
        - Drops the leading empty index column if present

    Returns dict:  sheet_name → {
        'df': pd.DataFrame,
        'brand_col': str or None,
        'name_col': str,
    }
    """
    results = {}

    # Check if file is CSV or Excel
    file_name = getattr(file, 'name', '')
    is_csv = file_name.lower().endswith('.csv')

    if is_csv:
        # Handle CSV file (single sheet)
        try:
            # Try to read CSV file
            df = pd.read_csv(file)

            # Get headers
            raw_headers = [str(v).strip() if pd.notna(v) else '' for v in df.columns]

            # Drop leading empty columns
            while raw_headers and raw_headers[0] in ('', 'nan', 'None', 'Unnamed: 0'):
                raw_headers = raw_headers[1:]
                df = df.iloc[:, 1:]

            if len(raw_headers) == 0 or len(df.columns) == 0:
                return results  # Empty file

            # Update column names
            df.columns = raw_headers

            # Detect brand and name columns
            col_map = _detect_columns(raw_headers)

            if col_map['name_col'] is None:
                return results  # Can't match without a product name column

            # Drop rows where the name column is empty
            df = df.dropna(subset=[col_map['name_col']])

            # Smart filter: Remove duplicate "Custom configuration" entries
            df = _filter_duplicate_custom_configs(df, col_map['name_col'])

            # Use filename (without extension) as sheet name
            sheet_name = file_name.rsplit('.', 1)[0] if '.' in file_name else 'Sheet 1'

            results[sheet_name] = {
                'df': df.reset_index(drop=True),
                'brand_col': col_map['brand_col'],
                'name_col': col_map['name_col'],
                'category_col': col_map.get('category_col'),
                'storage_col': col_map.get('storage_col'),
            }

        except Exception as e:
            # If CSV parsing fails, return empty results
            return results

    else:
        # Handle Excel file (multiple sheets)
        xls = pd.ExcelFile(file)

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
                'category_col': col_map.get('category_col'),
                'storage_col': col_map.get('storage_col'),
            }

    return results