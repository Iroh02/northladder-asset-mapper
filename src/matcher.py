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
from functools import lru_cache
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

# Variant tokens that must match exactly between query and candidate
VARIANT_TOKENS = {"pro", "max", "ultra", "plus", "fold", "flip", "fe", "mini", "lite", "note", "edge"}

# Hardware model code pattern (e.g., ZE552KL, SM-G960F, A2172)
# Requires 3+ digits to avoid matching normal model numbers like "s23", "a52"
MODEL_CODE_PATTERN = re.compile(r'\b[a-z]{1,3}\d{3,6}[a-z]{0,3}\b', re.IGNORECASE)


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
    'oneplus technology': 'oneplus', 'one plus': 'oneplus',
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
    # NL catalog OLD/New brand splits → canonical brand
    'dell old': 'dell', 'dell new': 'dell',
    'hp old': 'hp', 'hp new': 'hp',
    'lenovo old': 'lenovo', 'lenovo new': 'lenovo',
    'samsung (old)': 'samsung',
    # NL sub-brands → parent brand
    'legion': 'lenovo', 'omen': 'hp', 'alienware': 'dell',
    'macbooks': 'apple',
    # MSI alias
    "msi - micro star int'l": 'msi', 'msi': 'msi',
    # Garmin (already canonical, just ensure consistent)
    'microsoft surface': 'microsoft',
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

@lru_cache(maxsize=50000)
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
    # Reverse pattern MUST run first: "7th gen 10.4" → "gen7 10.4" before forward
    # pattern can greedily match "gen 10" from the screen size that follows
    s = re.sub(r'\b(\d+)(?:st|nd|rd|th)\s*gen(?:eration)?\b', _replace_gen_reverse, s, flags=re.IGNORECASE)
    s = re.sub(r'\bgen(?:eration)?\s*(i{1,3}v?|vi{0,3}|ix|x|\d+)\b', _replace_gen_forward, s, flags=re.IGNORECASE)

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

    # Strip Thunderbolt port designators BEFORE storage parsing
    # "2 TBT3" means "2 Thunderbolt 3 ports", NOT "2 TB" storage
    # "4 TBT3" means "4 Thunderbolt 3 ports", NOT "4 TB" storage
    s = re.sub(r'\b(\d+)\s*tbt\d?\b', r'\1tbt', s, flags=re.IGNORECASE)

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
    # Only convert true storage sizes (64g, 128g, 256g, 512g, 1024g, 2048g)
    # Do NOT convert small numbers like 16g/20g (MacBook GPU cores like 14c/20g)
    # Safe rule: only convert when number is >=64 OR has 3+ digits
    s = re.sub(r'\b(6[4-9]|[7-9]\d|\d{3,})g\b', r'\1gb', s, flags=re.IGNORECASE)

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
    # Also handles normalized text where dash is stripped: "i5 1245u"
    # Extract full model number, then determine gen from digit count + value
    intel_match = re.search(r'(?:core\s+)?i[3579][\s\-]?(\d{4,5})[a-z]{0,2}', text_lower)
    if intel_match:
        model_digits = intel_match.group(1)
        if len(model_digits) == 5:
            # 5-digit: first 2 digits = gen (i7-10750H -> 10, i5-12500H -> 12)
            gen = model_digits[:2]
        elif model_digits[0] == '1':
            # 4-digit starting with 1: gen 10+ (i5-1245U -> 12, i5-1065G7 -> 10)
            gen = model_digits[:2]
        else:
            # 4-digit, gen 2-9: first digit = gen (i5-8350U -> 8, i7-7600U -> 7)
            gen = model_digits[0]
        return f"{gen}th gen"

    # AMD Ryzen patterns: Ryzen 5 5500U, Ryzen 7 6800H
    ryzen_match = re.search(r'ryzen\s+[357]\s+(\d)(\d{3})', text_lower)
    if ryzen_match:
        gen = ryzen_match.group(1)
        return f"ryzen {gen}"

    # Fallback: look for "10th gen", "11th gen", etc.
    gen_match = re.search(r'(\d{1,2})(?:st|nd|rd|th)\s*gen', text_lower)
    if gen_match:
        return f"{gen_match.group(1)}th gen"

    # Normalized text fallback: "gen8", "gen11" (from normalize_text converting "8th gen" → "gen8")
    gen_norm_match = re.search(r'\bgen(\d{1,2})\b', text_lower)
    if gen_norm_match:
        return f"{gen_norm_match.group(1)}th gen"

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
        'aspire', 'swift', 'predator', 'nitro', 'spin',
        'legion', 'flex', 'travelmate', 'extensa',
        'alienware', 'zbook'
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
    # Use \b boundary to avoid matching "tbt3" (Thunderbolt 3 ports)
    tb_matches = re.findall(r'(\d+)\s*tb\b', text_lower)
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


def extract_watch_material(text_norm: str) -> str:
    """
    Canonical watch material extractor.

    Detects real-world material variants and abbreviations commonly found in
    asset lists and NL catalogs, mapping them to one of the four canonical values.

    Returns one of:
        'aluminum'
        'stainless'
        'titanium'
        'ceramic'
        ''  (empty string if no material detected)
    """
    t = text_norm.lower()

    # Aluminum variants: aluminum, aluminium, alumin, alum, alu
    if re.search(r'\b(alumin(?:um|ium)?|alu|alum)\b', t):
        return 'aluminum'

    # Stainless variants: stainless, stainlesssteel, stainless steel, st steel, ss, steel
    # Note: "steel" alone is safe here because this function is ONLY called for watches
    if re.search(r'\b(stainless(?:\s*steel)?|st\s*steel|steel|ss)\b', t):
        return 'stainless'

    # Titanium variants: titanium, titan, ti
    if re.search(r'\b(titanium|titan|ti)\b', t):
        return 'titanium'

    # Ceramic
    if re.search(r'\bceramic\b', t):
        return 'ceramic'

    return ''


def extract_watch_edition(text_norm: str) -> str:
    """
    Detect special watch editions: Nike, Hermes, Black Unity, Special Edition.

    Returns one of: 'nike', 'hermes', 'black_unity', 'edition', ''
    Only called for watches — cannot affect phones/tablets/laptops.
    """
    t = text_norm.lower()
    if re.search(r'\b(black\s*unity|unity)\b', t):
        return 'black_unity'
    if re.search(r'\b(herm[eè]s)\b', t):
        return 'hermes'
    if re.search(r'\bnike\b', t):
        return 'nike'
    if re.search(r'\b(special\s+edition|edition)\b', t):
        return 'edition'
    return ''


def extract_tablet_generation(text_norm: str) -> str:
    """
    Extract iPad / tablet generation from text: '7th gen' → '7', 'gen5' → '5'.
    Only matches ordinal-gen patterns — cannot affect phones/watches/laptops.
    """
    if not isinstance(text_norm, str):
        return ''
    t = text_norm.lower()
    # "7th gen", "5th generation"
    m = re.search(r'(\d+)(?:st|nd|rd|th)\s*gen', t)
    if m:
        return m.group(1)
    # normalize_text already converts "7th generation" → "gen7", "gen 5" → "gen5"
    m2 = re.search(r'\bgen(\d+)\b', t)
    if m2:
        return m2.group(1)
    return ''


def extract_screen_inches(text_norm: str) -> str:
    """
    Extract screen size in inches from text: '8.3"' → '8.3', '10.4 inch' → '10.4'.
    Also handles normalize_text output where dots become spaces: '10 4' → '10.4'.
    Only returns plausible tablet/laptop screen sizes (7–15 inches).
    """
    if not isinstance(text_norm, str):
        return ''
    t = text_norm.lower()
    # Space-separated decimal + inch suffix: "7 9 inch" → "7.9" (must run BEFORE simple inch match)
    # This handles normalize_text converting "7.9 inch" → "7 9 inch"
    m_sp_inch = re.search(r'(?<!gen)(?<!\d)\b(\d{1,2})\s(\d)\s*(?:"|inch)', t)
    if m_sp_inch:
        reconstructed = f'{m_sp_inch.group(1)}.{m_sp_inch.group(2)}'
        val = float(reconstructed)
        if 7.0 <= val <= 15.0:
            return reconstructed
    # "8.3"", "10.4 inch", "11 inch"
    m = re.search(r'(\d+(?:\.\d+)?)\s*(?:"|inch)', t)
    if m:
        val = float(m.group(1))
        if 7.0 <= val <= 15.0:
            return m.group(1)
    # Bare decimal in tablet range: "10.4", "8.3" (no unit suffix)
    m2 = re.search(r'\b(\d{1,2}\.\d{1,2})\b', t)
    if m2:
        val = float(m2.group(1))
        if 7.0 <= val <= 15.0:
            return m2.group(1)
    # Space-separated decimal without suffix: "10 4" → "10.4", "8 3" → "8.3"
    # Negative lookbehind prevents matching "gen7 8" as "7.8" (gen prefix = generation, not screen)
    m3 = re.search(r'(?<!gen)(?<!\d)\b(\d{1,2})\s(\d)\b', t)
    if m3:
        reconstructed = f'{m3.group(1)}.{m3.group(2)}'
        val = float(reconstructed)
        if 7.0 <= val <= 15.0:
            return reconstructed
    return ''


@lru_cache(maxsize=50000)
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

    # Save original text before normalization (for connectivity detection: "lte" gets stripped)
    text_orig = text.lower()
    text_norm = normalize_text(text)
    brand_norm = normalize_text(brand) if isinstance(brand, str) else ''

    # === WATCH DETECTION (priority - critical attributes: mm, series, connectivity) ===
    if extract_category(text_norm) == 'watch':
        watch_mm = extract_watch_mm(text_norm)

        # Extract series/generation: "series 10", "ultra 2", "se"
        series = ''
        series_match = re.search(r'\b(series\s*\d+(?:\s*(?:pro|ultra|se))?|ultra\s*\d+|se)\b', text_norm)
        if series_match:
            series = series_match.group(1).replace('  ', ' ').strip()

        # Extract connectivity: GPS vs Cellular
        connectivity = ''
        if 'cellular' in text_norm or 'lte' in text_norm.lower() or '4g' in text_norm.lower():
            connectivity = 'cellular'
        elif 'gps' in text_norm:
            connectivity = 'gps'

        # Extract material: aluminum, stainless steel, titanium, ceramic
        material = extract_watch_material(text_norm)

        # Extract edition: Nike, Hermes, Black Unity, Special Edition
        edition = extract_watch_edition(text_norm)

        return {
            'brand': brand_norm,
            'product_line': 'watch',
            'model': series or 'watch',
            'storage': '',           # Watches typically don't have storage variants
            'ram': '',
            'watch_mm': watch_mm,    # CRITICAL: case size
            'connectivity': connectivity,  # GPS vs Cellular
            'material': material,    # aluminum, stainless, titanium
            'edition': edition,      # nike, hermes, black_unity, edition
        }

    # === LAPTOP DETECTION (priority - different naming convention) ===
    if is_laptop_product(text):
        laptop_attrs = extract_laptop_attributes(text, brand)
        # Add year if not already captured as generation
        if not laptop_attrs.get('generation'):
            year_m = re.search(r'\b(20[12]\d)\b', text_norm)
            if year_m:
                laptop_attrs['year'] = year_m.group(1)
        return laptop_attrs

    # === TABLET DETECTION (iPad, Galaxy Tab, MatePad, etc.) ===
    category = extract_category(text_norm)
    if category == 'tablet':
        tablet_attrs = {
            'brand': brand_norm,
            'product_line': '',
            'model': '',
            'storage': extract_storage(text_norm),
            'screen_size': '',
            'screen_inches': '',   # from extract_screen_inches()
            'generation': '',      # from extract_tablet_generation()
            'year': '',
            'tablet_line': '',     # pro, se, lite, air, or '' (base)
            'tablet_family': '',   # "ipad pro", "ipad mini", "matepad pro", etc.
            'connectivity': '',    # "wifi" or "cellular"
            'variant_tokens': set(),  # {pro, air, mini, se, lite, kids, paper, plus}
            'model_number': '',    # hardware model code if present
            'chip': '',            # Apple M-series chip: m1, m2, m4
        }

        # Extract screen size: "10.4"", "10.4''", "10.4 inch", "11"", bare "10.4"
        screen_m = re.search(r'\b(\d{1,2}(?:\.\d{1,2})?)\s*(?:inch|in|"|\'\')', text_norm)
        if not screen_m:
            # Bare decimal in tablet-range: "10.4", "11.0", "8.3" (no unit suffix)
            screen_m = re.search(r'\b(\d{1,2}\.\d{1,2})\b', text_norm)
            if screen_m:
                val = float(screen_m.group(1))
                if not (7.0 <= val <= 13.0):
                    screen_m = None  # outside tablet range, probably not a screen size
        if screen_m:
            tablet_attrs['screen_size'] = screen_m.group(1)

        # Also populate screen_inches from the canonical extractor
        tablet_attrs['screen_inches'] = extract_screen_inches(text_norm)

        # Extract tablet generation: "7th gen" → "7", "gen5" → "5"
        tablet_attrs['generation'] = extract_tablet_generation(text_norm)

        # Extract year
        year_m = re.search(r'\b(20[12]\d)\b', text_norm)
        if year_m:
            tablet_attrs['year'] = year_m.group(1)

        # Extract tablet_line (pro/se/lite/air — shared across brands)
        _TABLET_VARIANT_KW = {'pro', 'se', 'lite', 'air', 'mini', 'kids', 'paper', 'plus', 'ultra', 'fe'}
        for kw in _TABLET_VARIANT_KW:
            if re.search(r'\b' + kw + r'\b', text_norm):
                tablet_attrs['variant_tokens'].add(kw)
        tl_m = re.search(r'\b(pro|se|lite|air)\b', text_norm)
        if tl_m:
            tablet_attrs['tablet_line'] = tl_m.group(1)

        # Connectivity: wifi vs cellular (lte/5g/cellular → "cellular", wifi-only → "wifi")
        # Check both text_norm and text_orig (normalize_text strips "lte")
        _conn_text = f'{text_norm} {text_orig}'
        if re.search(r'\b(?:cellular|lte|5g|4g)\b', _conn_text):
            tablet_attrs['connectivity'] = 'cellular'
        elif re.search(r'\bwifi\b', _conn_text):
            tablet_attrs['connectivity'] = 'wifi'

        # Apple M-series chip: m1, m2, m4, m5
        chip_m = re.search(r'\bm([1-9])\b', text_norm)
        if chip_m:
            tablet_attrs['chip'] = f'm{chip_m.group(1)}'

        # Hardware model code (e.g., A2588, SM-X700)
        hw_m = re.search(r'\b([a-z]{1,3}\d{3,5}[a-z]{0,2})\b', text_norm)
        if hw_m:
            code = hw_m.group(1)
            # Exclude storage-like tokens (128gb, 256gb) and generation tokens (gen5)
            if not re.match(r'^\d+[gt]b?$', code) and not code.startswith('gen'):
                tablet_attrs['model_number'] = code

        # iPad: "ipad pro 12.9 2022 256gb" or NL: "apple ipad pro ipad pro gen1 2015 12 9 wifi 256gb"
        if 'ipad' in text_norm:
            tablet_attrs['product_line'] = 'ipad'
            # Determine tablet_family: "ipad pro", "ipad air", "ipad mini", or "ipad"
            variant_m = re.search(r'ipad\s+(?:ipad\s+)?(pro|air|mini)', text_norm)
            if variant_m:
                tablet_attrs['tablet_family'] = f"ipad {variant_m.group(1)}"
            else:
                tablet_attrs['tablet_family'] = 'ipad'
            # Extract variant and optional generation (gen1, gen5, etc.)
            ipad_m = re.search(r'ipad\s+(?:ipad\s+)?(?:pro|air|mini)\s+(?:ipad\s+(?:pro|air|mini)\s+)?(gen\d+)', text_norm)
            if ipad_m:
                variant = variant_m.group(1) if variant_m else ''
                gen = ipad_m.group(1)
                tablet_attrs['model'] = f"{variant} {gen}".strip()
            else:
                if variant_m:
                    tablet_attrs['model'] = variant_m.group(1)
                else:
                    gen_m = re.search(r'ipad\s+(?:ipad\s+)?(gen\d+)', text_norm)
                    if gen_m:
                        tablet_attrs['model'] = gen_m.group(1)
            # Screen size: NL uses space-separated "12 9" for 12.9", "9 7" for 9.7"
            if not tablet_attrs['screen_size']:
                screen_m2 = re.search(r'\b(\d{1,2})\s+(\d)\b', text_norm)
                if screen_m2:
                    size = f"{screen_m2.group(1)}.{screen_m2.group(2)}"
                    if 7.0 <= float(size) <= 13.0:
                        tablet_attrs['screen_size'] = size
                else:
                    screen_m3 = re.search(r'\b(\d{1,2}\.\d)\b', text_norm)
                    if screen_m3 and 7.0 <= float(screen_m3.group(1)) <= 13.0:
                        tablet_attrs['screen_size'] = screen_m3.group(1)
            return tablet_attrs

        # Samsung Galaxy Tab: "galaxy tab s8 ultra 256gb"
        if 'tab' in text_norm:
            tablet_attrs['product_line'] = 'tab'
            tab_m = re.search(r'tab\s+([a-z]\d+[a-z]*(?:\s+(?:plus|ultra|lite|fe))*)', text_norm)
            if tab_m:
                tablet_attrs['model'] = tab_m.group(1).strip()
            # tablet_family: "tab s8", "tab a8" (series letter + number)
            tab_fam = re.search(r'tab\s+([a-z]\d+)', text_norm)
            if tab_fam:
                tablet_attrs['tablet_family'] = f"tab {tab_fam.group(1)}"
            else:
                tablet_attrs['tablet_family'] = 'tab'
            return tablet_attrs

        # Huawei MatePad / MediaPad
        if 'mediapad' in text_norm:
            tablet_attrs['product_line'] = 'mediapad'
            mp_m = re.search(r'mediapad\s+((?:t\d+|m\d+|lite)\s*(?:lite)?)', text_norm)
            if mp_m:
                tablet_attrs['model'] = mp_m.group(1).strip()
            tablet_attrs['tablet_family'] = 'mediapad'
            if tablet_attrs['tablet_line']:
                tablet_attrs['tablet_family'] = f"mediapad {tablet_attrs['tablet_line']}"
            return tablet_attrs

        if 'matepad' in text_norm:
            tablet_attrs['product_line'] = 'matepad'
            mp_m = re.search(r'matepad\s+(pro|air|t\d+|se)?', text_norm)
            if mp_m and mp_m.group(1):
                tablet_attrs['model'] = mp_m.group(1).strip()
            # tablet_family: "matepad pro", "matepad se", "matepad"
            tablet_attrs['tablet_family'] = 'matepad'
            if tablet_attrs['tablet_line']:
                tablet_attrs['tablet_family'] = f"matepad {tablet_attrs['tablet_line']}"
            return tablet_attrs

        # Generic tablet — fall through to phone path below
        # (will be handled by generic extraction)

    def _finalize_mobile_attrs(a):
        """Enrich mobile attrs with variant and generation from model string."""
        _m = a.get('model', '')
        if _m and not a.get('variant'):
            _vt = VARIANT_TOKENS & set(_m.lower().split())
            if _vt:
                a['variant'] = ' '.join(sorted(_vt))
        if _m and not a.get('generation'):
            _gm = re.search(r'(\d+)', _m)
            if _gm:
                a['generation'] = _gm.group(1)
        return a

    attrs = {
        'brand': brand_norm,
        'product_line': '',
        'model': '',
        'storage': extract_storage(text_norm),
        'model_number': '',   # Hardware code: ZE552KL, SM-S918B, A2172
        'variant': '',        # Variant tokens: ultra, fe, plus, max, mini, fold, flip
        'generation': '',     # Numeric generation if detectable
        'screen_size': '',    # Screen size if present
    }

    # Extract hardware model number from ORIGINAL text (before Samsung strips it)
    _hw_code = MODEL_CODE_PATTERN.search(text_norm)
    if _hw_code:
        attrs['model_number'] = _hw_code.group(0).lower()

    # Extract screen size if present (for phablets, large phones)
    _screen_m = re.search(r'\b(\d{1,2}(?:\.\d{1,2})?)\s*(?:inch|in|"|\'\')', text_norm)
    if _screen_m:
        attrs['screen_size'] = _screen_m.group(1)

    # Extract year for phones
    year_m = re.search(r'\b(20[12]\d)\b', text_norm)
    if year_m:
        attrs['year'] = year_m.group(1)

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
            return _finalize_mobile_attrs(attrs)

    # Samsung Galaxy: "galaxy s9 plus 128gb" → line=galaxy, model=s9 plus
    # CRITICAL: Capture ALL variant words (plus, ultra, note, fold, flip, edge, active)
    # Also handle "galaxy z fold5", "galaxy z flip5" where model is "z fold5" / "z flip5"
    if 'galaxy' in text_norm:
        # Try Z Fold/Flip pattern first (e.g., "galaxy z fold5 256gb", "galaxy z flip 5")
        z_match = re.search(r'galaxy\s+(z\s+(?:fold|flip)\s*\d*(?:\s+(?:pro|plus|max|ultra|lite|5g))*)', text_norm)
        if z_match:
            attrs['product_line'] = 'galaxy'
            attrs['model'] = re.sub(r'\s+', ' ', z_match.group(1)).strip()
            return _finalize_mobile_attrs(attrs)
        # Standard pattern (e.g., "galaxy s23 ultra", "galaxy a54")
        match = re.search(r'galaxy\s+([a-z]+\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite|fe|note|fold|flip|edge|active))*)', text_norm)
        if match:
            attrs['product_line'] = 'galaxy'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)

    # Google Pixel: "pixel 9 pro 256gb", "pixel 9 pro fold" → line=pixel, model=9 pro / 9 pro fold
    # CRITICAL: Capture ALL variant words including fold (pro xl, pro fold, fold, pro, a, etc.)
    if 'pixel' in text_norm:
        match = re.search(r'pixel\s+(\d+[a-z]*(?:\s+(?:pro\s+fold|pro\s+xl|fold|pro|xl|max|ultra|lite|a))*)', text_norm)
        if match:
            attrs['product_line'] = 'pixel'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)

    # Xiaomi Redmi/Mi: "redmi note 12 pro 128gb" → line=redmi, model=note 12 pro
    # CRITICAL: Capture ALL variant words (pro max, pro, plus, etc.)
    if 'redmi' in text_norm:
        match = re.search(r'redmi\s+(note\s+\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*|\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*)', text_norm, re.IGNORECASE)
        if match:
            attrs['product_line'] = 'redmi'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)
    elif 'xiaomi' in brand_norm and 'mi' in text_norm:
        # "xiaomi mi 11 ultra" → line=mi, model=11 ultra
        match = re.search(r'mi\s+(\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*)', text_norm)
        if match:
            attrs['product_line'] = 'mi'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)

    # Huawei Mate/P-series: "mate 30 pro 256gb" → line=mate, model=30 pro
    # CRITICAL: Capture ALL variant words
    if 'mate' in text_norm and ('huawei' in brand_norm or 'huawei' in text_norm):
        match = re.search(r'mate\s+(\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*)', text_norm)
        if match:
            attrs['product_line'] = 'mate'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)
    elif ('huawei' in brand_norm or 'huawei' in text_norm) and re.search(r'\bp\d+', text_norm):
        # "huawei p30 pro" → line=p, model=30 pro
        match = re.search(r'p(\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*)', text_norm)
        if match:
            attrs['product_line'] = 'p'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)

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
            return _finalize_mobile_attrs(attrs)

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

    return _finalize_mobile_attrs(attrs)


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

        material = attrs.get('material', '')

        # Detect tablet for tablet-specific key
        _is_tablet_entry = extract_category(row['normalized_name']) == 'tablet'

        if attrs['product_line'] == 'watch':
            # Watch key: mm + connectivity + material (all critical for unique identification)
            storage_key = f"{watch_mm}_{connectivity}_{material}".strip('_')
        elif ram:
            # Laptop key: RAM + storage
            storage_key = f"{ram}_{attrs['storage']}"
        elif _is_tablet_entry:
            # Tablet key: screen_inches + generation + storage (prevents size/gen collisions)
            _t_screen = attrs.get('screen_inches', '') or attrs.get('screen_size', '')
            _t_gen = attrs.get('generation', '')
            _t_parts = [p for p in [_t_screen, f'gen{_t_gen}' if _t_gen else '', attrs['storage']] if p]
            storage_key = '_'.join(_t_parts) if _t_parts else attrs['storage']
        else:
            # Phone key: storage only
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

        # Watch fallback keys: index under less-specific keys for graceful degradation
        if attrs['product_line'] == 'watch' and watch_mm:
            model_bucket = index[brand][attrs['product_line']][attrs['model']]

            # Fallback 1: mm + connectivity (no material)
            if connectivity and material:
                mm_conn_key = f"{watch_mm}_{connectivity}"
                if mm_conn_key != storage_key and mm_conn_key not in model_bucket:
                    model_bucket[mm_conn_key] = {
                        'asset_ids': [],
                        'nl_name': row['normalized_name'],
                        '_is_fallback': True,
                    }
                if mm_conn_key != storage_key:
                    fb_entry = model_bucket[mm_conn_key]
                    if asset_id not in fb_entry['asset_ids']:
                        fb_entry['asset_ids'].append(asset_id)

            # Fallback 2: mm only (no connectivity, no material)
            mm_only_key = watch_mm
            if mm_only_key != storage_key:
                if mm_only_key not in model_bucket:
                    model_bucket[mm_only_key] = {
                        'asset_ids': [],
                        'nl_name': row['normalized_name'],
                        '_is_fallback': True,
                    }
                fb_entry = model_bucket[mm_only_key]
                if asset_id not in fb_entry['asset_ids']:
                    fb_entry['asset_ids'].append(asset_id)

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
        # Watches: use mm + connectivity + material
        # Laptops: use RAM + storage
        # Tablets: use screen_inches + generation + storage
        # Phones: use storage only
        ram = attrs.get('ram', '')
        watch_mm = attrs.get('watch_mm', '')
        connectivity = attrs.get('connectivity', '')
        material = attrs.get('material', '')

        if attrs['product_line'] == 'watch':
            storage_key = f"{watch_mm}_{connectivity}_{material}".strip('_')
        elif ram:
            storage_key = f"{ram}_{attrs['storage']}"
        elif query_category == 'tablet':
            _t_screen = attrs.get('screen_inches', '') or attrs.get('screen_size', '')
            _t_gen = attrs.get('generation', '')
            _t_parts = [p for p in [_t_screen, f'gen{_t_gen}' if _t_gen else '', attrs['storage']] if p]
            storage_key = '_'.join(_t_parts) if _t_parts else attrs['storage']
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

        # Watch fallback tiers: try progressively less-specific keys
        if attrs['product_line'] == 'watch' and watch_mm:
            query_material = attrs.get('material', '')

            # STRICT MATERIAL ENFORCEMENT
            if query_material:
                # Query specifies material → DO NOT allow fallback
                return None

            # Query has no material → fallback allowed
            fallback_keys = []
            if connectivity:
                fallback_keys.append(f"{watch_mm}_{connectivity}")
            fallback_keys.append(watch_mm)

            for fb_key in fallback_keys:
                if fb_key in model_data and fb_key != storage_key:
                    entry = model_data[fb_key]
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
                                'method': 'attribute_watch_fallback',
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
                                'method': 'attribute_watch_fallback',
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
        # SAFETY: For composite keys (ram_storage), RAM must match exactly.
        # Only the storage portion is allowed to fuzzy-match.
        if query_storage and model_data and attrs['product_line'] != 'watch':
            available_keys = [k for k in model_data.keys() if k]
            if available_keys:
                from rapidfuzz import fuzz as _fuzz
                best_key = None
                best_score = 0
                for k in available_keys:
                    # If query has RAM (composite key), candidate must have same RAM
                    if ram:
                        k_parts = k.split('_', 1)
                        if len(k_parts) == 2 and k_parts[0] != ram:
                            continue  # RAM mismatch — skip this key entirely
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
# Variant Signature Matching
# ---------------------------------------------------------------------------

def build_variant_signature(attrs: Dict[str, str]) -> str:
    """
    Build a deterministic variant signature from extracted product attributes.

    Signature format (underscore-joined, lowercase, empty fields skipped):
        Watch:  apple_watch_series9_45mm_gps_aluminum
        Phone:  apple_iphone_14_pro_128gb
        Laptop: apple_macbook_air_m1_8gb_256gb

    Returns empty string if insufficient attributes for a meaningful signature.
    """
    product_line = attrs.get('product_line', '')
    if not product_line:
        return ''

    parts = []

    # Brand
    brand = attrs.get('brand', '')
    if brand:
        parts.append(brand)

    # Product line
    parts.append(product_line)

    # Category-specific fields
    processor = attrs.get('processor', '')
    if product_line == 'watch':
        # Watch: model + mm + connectivity + material
        model = attrs.get('model', '')
        if model and model != product_line:
            parts.append(model)
        watch_mm = attrs.get('watch_mm', '')
        if watch_mm:
            parts.append(watch_mm)
        connectivity = attrs.get('connectivity', '')
        if connectivity:
            parts.append(connectivity)
        material = attrs.get('material', '')
        if material:
            parts.append(material)
        edition = attrs.get('edition', '')
        if edition:
            parts.append(edition)
    elif processor or attrs.get('ram'):
        # Laptop: processor + ram + storage (skip model if same as processor)
        model = attrs.get('model', '')
        if model and model != product_line and model != processor:
            parts.append(model)
        if processor:
            parts.append(processor)
        ram = attrs.get('ram', '')
        if ram:
            parts.append(ram)
        storage = attrs.get('storage', '')
        if storage:
            parts.append(storage)
    elif attrs.get('screen_size') or attrs.get('tablet_line') or attrs.get('generation') or attrs.get('tablet_family'):
        # Tablet: tablet_family + model + generation + screen + year + connectivity + storage
        # tablet_family is more specific than tablet_line (e.g., "ipad pro" vs just "pro")
        tablet_family = attrs.get('tablet_family', '')
        tablet_line = attrs.get('tablet_line', '')
        if tablet_family:
            # tablet_family already includes line (e.g., "ipad pro"), don't duplicate
            parts.append(tablet_family.replace(' ', '_'))
        elif tablet_line:
            parts.append(tablet_line)
        model = attrs.get('model', '')
        if model and model != product_line and model != tablet_line:
            # Don't duplicate if model matches family suffix (e.g., model="pro", family="ipad pro")
            if not tablet_family or model not in tablet_family:
                parts.append(model)
        generation = attrs.get('generation', '')
        if generation and f'gen{generation}' not in (model or ''):
            parts.append(f'{generation}thgen')
        screen = attrs.get('screen_inches') or attrs.get('screen_size', '')
        if screen:
            parts.append(screen)
        year = attrs.get('year', '')
        if year:
            parts.append(year)
        connectivity = attrs.get('connectivity', '')
        if connectivity:
            parts.append(connectivity)
        chip = attrs.get('chip', '')
        if chip:
            parts.append(chip)
        storage = attrs.get('storage', '')
        if storage:
            parts.append(storage)
    else:
        # Phone/generic: model + storage
        model = attrs.get('model', '')
        if model and model != product_line:
            parts.append(model)
        storage = attrs.get('storage', '')
        if storage:
            parts.append(storage)

    # Need at least 3 parts for a meaningful signature (brand + line + something)
    if len(parts) < 3:
        return ''

    sig = '_'.join(parts).lower().replace(' ', '_')
    # Collapse multiple underscores
    sig = re.sub(r'_+', '_', sig).strip('_')
    return sig


def build_signature_index(df_nl_clean: pd.DataFrame) -> Dict[str, Dict]:
    """
    Build a deterministic variant signature index from the NL catalog.

    For each row, extracts product attributes, builds a signature, and indexes it.

    Returns:
        dict mapping signature → {
            'asset_ids': [list of asset IDs],
            'nl_name': normalized name of first entry
        }
    """
    sig_index: Dict[str, Dict] = {}

    for _, row in df_nl_clean.iterrows():
        nl_name = str(row.get('normalized_name', ''))
        brand = str(row.get('brand', ''))
        asset_id = str(row.get('uae_assetid', ''))

        if not nl_name or not asset_id:
            continue

        attrs = extract_product_attributes(nl_name, brand)
        sig = build_variant_signature(attrs)

        if not sig:
            continue

        if sig in sig_index:
            if asset_id not in sig_index[sig]['asset_ids']:
                sig_index[sig]['asset_ids'].append(asset_id)
        else:
            sig_index[sig] = {
                'asset_ids': [asset_id],
                'nl_name': nl_name,
            }

    return sig_index


def try_signature_match(
    query: str,
    brand: str,
    signature_index: Dict[str, Dict],
    nl_catalog: Optional[pd.DataFrame] = None,
    original_input: str = '',
) -> Optional[dict]:
    """
    Attempt deterministic variant signature matching.

    Builds a signature from the query's extracted attributes and looks it up
    in the pre-built signature index. This catches variant mismatches that
    attribute matching misses (e.g., aluminum vs stainless, M1 vs M2).

    Returns match result dict if found, None otherwise.
    """
    attrs = extract_product_attributes(query, brand)
    sig = build_variant_signature(attrs)

    if not sig or sig not in signature_index:
        return None

    entry = signature_index[sig]
    asset_ids = entry['asset_ids']
    nl_name = entry['nl_name']

    # Category safety check
    query_cat = extract_category(query)
    nl_cat = extract_category(nl_name)
    if query_cat != 'other' and nl_cat != 'other' and query_cat != nl_cat:
        return None

    if len(asset_ids) > 1 and nl_catalog is not None:
        user_input = original_input if original_input else query
        selection = auto_select_matching_variant(user_input, asset_ids, nl_catalog)
        return {
            'mapped_uae_assetid': selection['selected_id'],
            'match_score': 100.0,
            'match_status': MATCH_STATUS_MATCHED,
            'confidence': CONFIDENCE_HIGH,
            'matched_on': nl_name,
            'method': 'signature',
            'auto_selected': selection['auto_selected'],
            'selection_reason': selection['reason'],
            'alternatives': selection['alternatives'],
        }
    elif len(asset_ids) == 1:
        return {
            'mapped_uae_assetid': asset_ids[0],
            'match_score': 100.0,
            'match_status': MATCH_STATUS_MATCHED,
            'confidence': CONFIDENCE_HIGH,
            'matched_on': nl_name,
            'method': 'signature',
            'auto_selected': False,
            'selection_reason': '',
            'alternatives': [],
        }
    else:
        return {
            'mapped_uae_assetid': ', '.join(asset_ids),
            'match_score': 100.0,
            'match_status': MATCH_STATUS_MULTIPLE,
            'confidence': CONFIDENCE_HIGH,
            'matched_on': nl_name,
            'method': 'signature',
            'auto_selected': False,
            'selection_reason': '',
            'alternatives': [],
        }


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

    # Filter out promo/placeholder brands that pollute fuzzy search space
    _EXCLUDE_BRANDS = {
        'promo', 'others', 'bts laptops', 'dsf 2021 promotion', 're-cycle',
        'windows', 'other laptops',
    }
    pre_promo = len(df)
    promo_mask = df['brand'].astype(str).str.strip().str.lower().isin(_EXCLUDE_BRANDS)
    df = df[~promo_mask]
    promo_dropped = pre_promo - len(df)

    # Filter out "All/Any" catchall placeholder entries
    pre_catchall = len(df)
    catchall_mask = df['uae_assetname'].astype(str).str.contains(
        r'\b(?:all\s+(?:models|storage|other|ram)|any\s+(?:storage|brand|model|ram))\b',
        case=False, na=False
    )
    df = df[~catchall_mask]
    catchall_dropped = pre_catchall - len(df)

    # Filter out junk single-character/digit-only names (e.g., "1", "11")
    pre_junk = len(df)
    junk_mask = df['uae_assetname'].astype(str).str.strip().str.match(r'^\d{1,2}$')
    df = df[~junk_mask]
    junk_dropped = pre_junk - len(df)

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
        'promo_dropped': promo_dropped,
        'catchall_dropped': catchall_dropped,
        'junk_dropped': junk_dropped,
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


@lru_cache(maxsize=50000)
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
        'mediapad' in text_lower or
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
        'has_mini': False,
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
    if re.search(r'\bmini\b', text_lower):
        result['has_mini'] = True

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

    # === PRIORITY 0: Material matching (FIRST — aluminum vs stainless vs titanium) ===
    # For watches especially, material is the most critical differentiator
    user_input_lower = user_input.lower()
    user_material = ''
    if 'alumin' in user_input_lower:
        user_material = 'aluminum'
    elif 'stainless' in user_input_lower:
        user_material = 'stainless'
    elif 'titanium' in user_input_lower or 'titan' in user_input_lower:
        user_material = 'titanium'
    elif 'ceramic' in user_input_lower:
        user_material = 'ceramic'

    if user_material:
        filtered = []
        for _, row in variants.iterrows():
            nl_name_lower = str(row['uae_assetname']).lower()
            if user_material == 'aluminum' and 'alumin' in nl_name_lower:
                filtered.append(row['uae_assetid'])
            elif user_material == 'stainless' and 'stainless' in nl_name_lower:
                filtered.append(row['uae_assetid'])
            elif user_material == 'titanium' and ('titanium' in nl_name_lower or 'titan' in nl_name_lower):
                filtered.append(row['uae_assetid'])
            elif user_material == 'ceramic' and 'ceramic' in nl_name_lower:
                filtered.append(row['uae_assetid'])
        if len(filtered) > 0:
            variants = nl_catalog[nl_catalog['uae_assetid'].isin(filtered)]

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

    # ERROR PREVENTION 5: Ultra variant matching
    # Ultra is a distinct product (Galaxy S23 Ultra != Galaxy S23)
    filtered = []
    for _, row in variants.iterrows():
        nl_variants = extract_model_variant_keywords(row['uae_assetname'])
        if user_variants['has_ultra'] and not nl_variants['has_ultra']:
            continue  # User has Ultra, NL must too
        if not user_variants['has_ultra'] and nl_variants['has_ultra']:
            continue  # User does NOT have Ultra, skip Ultra NL entries
        filtered.append(row['uae_assetid'])
    if len(filtered) > 0:
        variants = nl_catalog[nl_catalog['uae_assetid'].isin(filtered)]

    # ERROR PREVENTION 6: Lite variant matching
    # Lite is a distinct product (P40 Lite != P40)
    filtered = []
    for _, row in variants.iterrows():
        nl_variants = extract_model_variant_keywords(row['uae_assetname'])
        if user_variants['has_lite'] and not nl_variants['has_lite']:
            continue  # User has Lite, NL must too
        if not user_variants['has_lite'] and nl_variants['has_lite']:
            continue  # User does NOT have Lite, skip Lite NL entries
        filtered.append(row['uae_assetid'])
    if len(filtered) > 0:
        variants = nl_catalog[nl_catalog['uae_assetid'].isin(filtered)]

    # ERROR PREVENTION 7: Mini variant matching
    # Mini is a distinct product (iPhone 13 Mini != iPhone 13)
    filtered = []
    for _, row in variants.iterrows():
        nl_variants = extract_model_variant_keywords(row['uae_assetname'])
        if user_variants['has_mini'] and not nl_variants['has_mini']:
            continue
        if not user_variants['has_mini'] and nl_variants['has_mini']:
            continue
        filtered.append(row['uae_assetid'])
    if len(filtered) > 0:
        variants = nl_catalog[nl_catalog['uae_assetid'].isin(filtered)]

    # If model variant filtering narrowed down to 1 option, select it!
    if len(variants) == 1:
        selected = variants.iloc[0]['uae_assetid']
        alternatives = [aid for aid in asset_ids if aid != selected]

        # Build reason based on what was matched
        reason_parts = []
        if user_material:
            reason_parts.append(f'material:{user_material}')
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
        if user_variants['has_ultra']:
            reason_parts.append('Ultra')
        if user_variants['has_lite']:
            reason_parts.append('Lite')
        if user_variants['has_mini']:
            reason_parts.append('Mini')

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


def variant_exact_match(query_attrs: Dict, candidate_attrs: Dict) -> Tuple[bool, List[str]]:
    """
    Compare extracted attributes between query and candidate for variant-level exactness.

    Checks:
        - material: if both present, must match exactly
        - model: must match exactly (series 9 != series 10)
        - variant tokens: pro/max/ultra/fold/flip/etc must match
        - generation: if both present, must match (fold3 != fold4)

    Returns:
        (match: bool, mismatches: list[str])
    """
    mismatches = []

    # Material check
    q_mat = query_attrs.get('material', '')
    c_mat = candidate_attrs.get('material', '')
    if q_mat and c_mat and q_mat != c_mat:
        mismatches.append(f'material:{q_mat}!={c_mat}')

    # Model check (series 9 vs series 10, etc.)
    q_model = query_attrs.get('model', '')
    c_model = candidate_attrs.get('model', '')
    if q_model and c_model and q_model != c_model:
        # Skip if one is generic ('watch', 'phone', etc.)
        if q_model not in ('watch', 'mobile', 'tablet', 'laptop') and \
           c_model not in ('watch', 'mobile', 'tablet', 'laptop'):
            mismatches.append(f'model:{q_model}!={c_model}')

    # Variant tokens (pro, max, ultra, fold, flip, etc.)
    q_text = ' '.join([
        query_attrs.get('product_line', ''),
        query_attrs.get('model', ''),
    ])
    c_text = ' '.join([
        candidate_attrs.get('product_line', ''),
        candidate_attrs.get('model', ''),
    ])
    q_vtokens = extract_variant_tokens(q_text)
    c_vtokens = extract_variant_tokens(c_text)
    if q_vtokens != c_vtokens:
        mismatches.append(f'variant_tokens:{q_vtokens}!={c_vtokens}')

    # Watch mm check
    q_mm = query_attrs.get('watch_mm', '')
    c_mm = candidate_attrs.get('watch_mm', '')
    if q_mm and c_mm and q_mm != c_mm:
        mismatches.append(f'watch_mm:{q_mm}!={c_mm}')

    # Connectivity check (for watches)
    q_conn = query_attrs.get('connectivity', '')
    c_conn = candidate_attrs.get('connectivity', '')
    if q_conn and c_conn and q_conn != c_conn:
        mismatches.append(f'connectivity:{q_conn}!={c_conn}')

    return len(mismatches) == 0, mismatches


def tablet_variant_exact_match(query_attrs: Dict, candidate_attrs: Dict) -> Tuple[bool, List[str]]:
    """
    Strict tablet-specific gate: MATCHED only if core tablet attributes are identical.

    Checks tablet_family, size_inches, generation, year, variant_tokens, model_number.
    Any mismatch → REVIEW_REQUIRED, never MATCHED.
    """
    mismatches = []

    # tablet_family must match (e.g., "ipad pro" != "ipad mini", "matepad" != "matepad pro")
    q_fam = query_attrs.get('tablet_family', '').lower().strip()
    c_fam = candidate_attrs.get('tablet_family', '').lower().strip()
    if q_fam and c_fam and q_fam != c_fam:
        mismatches.append(f'tablet_family:{q_fam}!={c_fam}')

    # size_inches must match exactly (10.4 != 11, 12.9 != 13)
    q_size = query_attrs.get('screen_inches', '') or query_attrs.get('screen_size', '')
    c_size = candidate_attrs.get('screen_inches', '') or candidate_attrs.get('screen_size', '')
    if q_size and c_size:
        try:
            if abs(float(q_size) - float(c_size)) > 0.15:
                mismatches.append(f'tablet_size:{q_size}!={c_size}')
        except ValueError:
            if q_size != c_size:
                mismatches.append(f'tablet_size:{q_size}!={c_size}')

    # generation must match exactly
    q_gen = query_attrs.get('generation', '')
    c_gen = candidate_attrs.get('generation', '')
    if q_gen and c_gen and q_gen != c_gen:
        mismatches.append(f'tablet_generation:{q_gen}!={c_gen}')

    # year must match exactly
    q_year = query_attrs.get('year', '')
    c_year = candidate_attrs.get('year', '')
    if q_year and c_year and q_year != c_year:
        mismatches.append(f'tablet_year:{q_year}!={c_year}')

    # variant_tokens must match (pro/lite/se/air present on one side but not the other)
    q_vt = query_attrs.get('variant_tokens', set())
    c_vt = candidate_attrs.get('variant_tokens', set())
    if isinstance(q_vt, (list, tuple)):
        q_vt = set(q_vt)
    if isinstance(c_vt, (list, tuple)):
        c_vt = set(c_vt)
    # Only check _TABLET_CRITICAL_VARIANTS — these always distinguish products
    _TABLET_CRITICAL_VARIANTS = {'pro', 'air', 'mini', 'se', 'lite', 'plus', 'ultra', 'fe', 'kids', 'paper'}
    q_crit = q_vt & _TABLET_CRITICAL_VARIANTS
    c_crit = c_vt & _TABLET_CRITICAL_VARIANTS
    if q_crit != c_crit:
        mismatches.append(f'tablet_variant:{q_crit}!={c_crit}')

    # tablet_line (pro/se/lite/air) must match — backup check in case variant_tokens missed
    q_tl = query_attrs.get('tablet_line', '')
    c_tl = candidate_attrs.get('tablet_line', '')
    if q_tl and c_tl and q_tl != c_tl:
        if f'tablet_variant:' not in '|'.join(mismatches):
            mismatches.append(f'tablet_line:{q_tl}!={c_tl}')
    elif q_tl and not c_tl:
        if f'tablet_variant:' not in '|'.join(mismatches):
            mismatches.append(f'tablet_line_missing:{q_tl}')

    # model_number: if present on both sides, must match exactly
    q_mn = query_attrs.get('model_number', '').lower().strip()
    c_mn = candidate_attrs.get('model_number', '').lower().strip()
    if q_mn and c_mn and q_mn != c_mn:
        mismatches.append(f'tablet_model_number:{q_mn}!={c_mn}')

    # chip: if present on both sides, must match (M1 != M2)
    q_chip = query_attrs.get('chip', '')
    c_chip = candidate_attrs.get('chip', '')
    if q_chip and c_chip and q_chip != c_chip:
        mismatches.append(f'tablet_chip:{q_chip}!={c_chip}')

    # storage: if present on both sides, must match
    q_stor = query_attrs.get('storage', '')
    c_stor = candidate_attrs.get('storage', '')
    if q_stor and c_stor and q_stor != c_stor:
        mismatches.append(f'tablet_storage:{q_stor}!={c_stor}')

    return len(mismatches) == 0, mismatches


def laptop_variant_exact_match(query_attrs: Dict, candidate_attrs: Dict) -> Tuple[bool, List[str]]:
    """
    Strict laptop-specific gate: MATCHED only if core laptop attributes match.

    For Windows laptops, we do NOT rely on model numbers (SF314-511, etc.).
    We map based on: brand, series/product_line, processor, generation, RAM, storage.

    Checks:
    - processor family (i5 != i7, ryzen5 != ryzen7)
    - generation (11th != 10th, m1 != m2)
    - RAM (16gb != 8gb)
    - storage (512gb != 256gb)
    - product_line/series (latitude != inspiron, thinkpad != ideapad)

    If query has an attribute but candidate lacks it → reject.
    If both have an attribute and they differ → reject.

    Returns (match: bool, mismatches: list[str])
    """
    mismatches = []

    # Brand must match
    q_brand = query_attrs.get('brand', '').lower().strip()
    c_brand = candidate_attrs.get('brand', '').lower().strip()
    if q_brand and c_brand and q_brand != c_brand:
        mismatches.append(f'laptop_brand:{q_brand}!={c_brand}')

    # Product line (series/model family) must match
    # latitude != inspiron, thinkpad != ideapad, swift != aspire
    q_line = query_attrs.get('product_line', '').lower().strip()
    c_line = candidate_attrs.get('product_line', '').lower().strip()
    if q_line and c_line and q_line != c_line:
        mismatches.append(f'laptop_series:{q_line}!={c_line}')
    elif q_line and not c_line:
        # Query specifies series but candidate doesn't → reject
        mismatches.append(f'laptop_series_missing:{q_line}')

    # Processor family must match (i3/i5/i7/i9, m1/m2/m4, ryzen3/5/7/9)
    q_proc = query_attrs.get('processor', '').lower().strip()
    c_proc = candidate_attrs.get('processor', '').lower().strip()
    if q_proc and c_proc and q_proc != c_proc:
        mismatches.append(f'laptop_processor:{q_proc}!={c_proc}')
    elif q_proc and not c_proc:
        mismatches.append(f'laptop_processor_missing:{q_proc}')

    # CPU generation must match (11th != 10th, 8th != 12th, m1 != m2)
    q_gen = query_attrs.get('generation', '').lower().strip()
    c_gen = candidate_attrs.get('generation', '').lower().strip()
    if q_gen and c_gen and q_gen != c_gen:
        mismatches.append(f'laptop_generation:{q_gen}!={c_gen}')
    elif q_gen and not c_gen:
        mismatches.append(f'laptop_generation_missing:{q_gen}')

    # RAM must match exactly (16gb != 8gb, 32gb != 16gb)
    q_ram = query_attrs.get('ram', '').lower().strip()
    c_ram = candidate_attrs.get('ram', '').lower().strip()
    if q_ram and c_ram and q_ram != c_ram:
        mismatches.append(f'laptop_ram:{q_ram}!={c_ram}')
    elif q_ram and not c_ram:
        mismatches.append(f'laptop_ram_missing:{q_ram}')

    # Storage must match exactly (512gb != 256gb, 1tb != 512gb)
    q_stor = query_attrs.get('storage', '').lower().strip()
    c_stor = candidate_attrs.get('storage', '').lower().strip()
    if q_stor and c_stor and q_stor != c_stor:
        mismatches.append(f'laptop_storage:{q_stor}!={c_stor}')
    elif q_stor and not c_stor:
        mismatches.append(f'laptop_storage_missing:{q_stor}')

    return len(mismatches) == 0, mismatches


def _extract_galaxy_s_number(model_str: str) -> str:
    """Extract Samsung Galaxy s/a/z/m number from model string, e.g. 's23', 'a54', 'z fold5'."""
    if not model_str:
        return ''
    m = re.search(r'\b([sazm])(\d{1,3})\b', model_str.lower())
    return f'{m.group(1)}{m.group(2)}' if m else ''


def _extract_galaxy_variant(model_str: str, full_text: str = '') -> str:
    """Extract Samsung Galaxy variant: ultra, plus, fe, or 'base'.
    Uses both model string and full text to catch variant tokens."""
    combined = f'{model_str} {full_text}'.lower()
    for v in ('ultra', 'fe', 'plus', 'lite', 'note', 'fold', 'flip', 'edge', 'active'):
        if v in combined.split():
            return v
    return 'base'


def mobile_variant_exact_match(query_attrs: Dict, candidate_attrs: Dict) -> Tuple[bool, List[str]]:
    """
    Strict mobile-specific gate: MATCHED only if core attributes are identical.

    Checks brand, product_line, model (normalized), storage, and variant tokens.
    Any mismatch → REVIEW_REQUIRED, never MATCHED.
    """
    mismatches = []

    # Brand must match
    q_brand = query_attrs.get('brand', '')
    c_brand = candidate_attrs.get('brand', '')
    if q_brand and c_brand and q_brand != c_brand:
        mismatches.append(f'mobile_brand:{q_brand}!={c_brand}')

    # Product line must match (iphone, galaxy, pixel, redmi, etc.)
    q_line = query_attrs.get('product_line', '')
    c_line = candidate_attrs.get('product_line', '')
    if q_line and c_line and q_line != c_line:
        mismatches.append(f'mobile_product_line:{q_line}!={c_line}')

    # Model must match exactly after normalization (14 pro max != 14 pro, s23 != s24)
    q_model = normalize_text(query_attrs.get('model', ''))
    c_model = normalize_text(candidate_attrs.get('model', ''))
    if q_model and c_model and q_model != c_model:
        mismatches.append(f'mobile_model:{q_model}!={c_model}')

    # Storage must match
    q_storage = query_attrs.get('storage', '')
    c_storage = candidate_attrs.get('storage', '')
    if q_storage and c_storage and q_storage != c_storage:
        mismatches.append(f'mobile_storage:{q_storage}!={c_storage}')

    # Model number (hardware code) must match if present on both sides
    q_model_num = query_attrs.get('model_number', '').lower().strip()
    c_model_num = candidate_attrs.get('model_number', '').lower().strip()
    if q_model_num and c_model_num and q_model_num != c_model_num:
        mismatches.append(f'mobile_model_number:{q_model_num}!={c_model_num}')

    # Variant tokens must match exactly (pro, max, ultra, mini, lite, fe, fold, flip)
    q_text = ' '.join([q_line, query_attrs.get('model', '')])
    c_text = ' '.join([c_line, candidate_attrs.get('model', '')])
    q_vtokens = extract_variant_tokens(q_text)
    c_vtokens = extract_variant_tokens(c_text)
    if q_vtokens != c_vtokens:
        mismatches.append(f'mobile_variant:{q_vtokens}!={c_vtokens}')

    # --- Samsung Galaxy strict enforcement (Part 4) ---
    # For Samsung Galaxy, enforce exact s-number match (s23 != s24)
    # and strict variant distinction (ultra/plus/fe/base)
    if q_line and 'galaxy' in q_line.lower():
        q_snum = _extract_galaxy_s_number(query_attrs.get('model', ''))
        c_snum = _extract_galaxy_s_number(candidate_attrs.get('model', ''))
        if q_snum and c_snum and q_snum != c_snum:
            mismatches.append(f'samsung_s_number:{q_snum}!={c_snum}')
        # Variant distinction: "fe" vs base, "ultra" vs "plus" etc.
        q_galaxy_var = _extract_galaxy_variant(query_attrs.get('model', ''), q_text)
        c_galaxy_var = _extract_galaxy_variant(candidate_attrs.get('model', ''), c_text)
        if q_galaxy_var != c_galaxy_var:
            mismatches.append(f'samsung_variant:{q_galaxy_var}!={c_galaxy_var}')

    # --- ASUS Zenfone strict model number enforcement (Part 5) ---
    # If either side has a model_number (hardware code like ZE552KL), require exact match
    if q_line and 'zenfone' in q_line.lower():
        if q_model_num and c_model_num and q_model_num != c_model_num:
            # Already caught above, but ensure it's flagged specifically
            if f'mobile_model_number:{q_model_num}!={c_model_num}' not in mismatches:
                mismatches.append(f'zenfone_model_number:{q_model_num}!={c_model_num}')
        # If query has model_number but candidate doesn't (or vice versa), flag it
        if q_model_num and not c_model_num:
            mismatches.append(f'zenfone_model_number_missing:candidate_has_none')
        elif c_model_num and not q_model_num:
            pass  # Query without model code is OK — still use other checks

    return len(mismatches) == 0, mismatches


def _enforce_gate(result: dict, query: str) -> dict:
    """
    Enforce verification_gate before allowing MATCHED status.
    If gate fails, downgrades to REVIEW_REQUIRED.
    Applied to all match paths (attribute, signature, fuzzy).

    Category-specific rules:
    - MOBILE: strict mobile_variant_exact_match required; fuzzy → always REVIEW_REQUIRED
    - LAPTOP: model tokens/codes relaxed; fuzzy → always REVIEW_REQUIRED
    - TABLET: screen_inches and generation must match
    - ALL: fuzzy method → always REVIEW_REQUIRED (never MATCHED)
    """
    if result.get('match_status') != MATCH_STATUS_MATCHED:
        return result  # Only enforce on MATCHED

    matched_on = result.get('matched_on', '')
    if not matched_on:
        return result

    method = result.get('method', '')

    # Detect category from both query text and explicit input_category
    input_cat = result.get('_input_category', '').lower().strip()
    query_cat = extract_category(query)
    is_laptop = (query_cat == 'laptop' or input_cat == 'laptop')
    is_mobile = (query_cat == 'mobile' or input_cat in ('mobile', 'mobile phone', 'phone'))
    is_tablet = (query_cat == 'tablet' or input_cat in ('tablet', 'tab'))

    # -----------------------------------------------------------------------
    # PART 6: Fuzzy matches → ALWAYS REVIEW_REQUIRED, never MATCHED
    # Only attribute and signature matches are allowed to be MATCHED.
    # -----------------------------------------------------------------------
    if 'fuzzy' in method:
        result['match_status'] = MATCH_STATUS_SUGGESTED
        result['confidence'] = CONFIDENCE_MEDIUM
        result['verification_pass'] = False
        result['verification_reasons'] = 'fuzzy_downgrade: fuzzy matches require human review'
        result['method'] = method + '_fuzzy_downgrade'
        return result

    # -----------------------------------------------------------------------
    # Standard verification gate (category, storage, model tokens, variant, etc.)
    # -----------------------------------------------------------------------
    gate_pass, gate_reasons = verification_gate(query, matched_on)

    # Additional variant_exact_match check using extracted attributes
    try:
        q_brand = result.get('_query_brand', '')
        q_attrs = extract_product_attributes(query, q_brand)
        c_attrs = extract_product_attributes(matched_on, '')
        vem_pass, vem_mismatches = variant_exact_match(q_attrs, c_attrs)
        if not vem_pass:
            gate_pass = False
            gate_reasons.extend([f'vem_{m}' for m in vem_mismatches])
    except Exception:
        pass  # Don't break gate on extraction failures

    # -----------------------------------------------------------------------
    # PART 1: MOBILE strict gate — enforce mobile_variant_exact_match
    # -----------------------------------------------------------------------
    if is_mobile:
        try:
            q_brand = result.get('_query_brand', '')
            q_attrs_m = extract_product_attributes(query, q_brand)
            c_attrs_m = extract_product_attributes(matched_on, '')
            mobile_pass, mobile_reasons = mobile_variant_exact_match(q_attrs_m, c_attrs_m)
            if not mobile_pass:
                gate_pass = False
                gate_reasons.extend(mobile_reasons)
        except Exception:
            pass

    # -----------------------------------------------------------------------
    # PART 4: TABLET strict gate — tablet_variant_exact_match (family, size,
    # generation, year, variant_tokens, model_number, chip, storage)
    # -----------------------------------------------------------------------
    if is_tablet:
        try:
            q_brand_t = result.get('_query_brand', '')
            q_attrs_t = extract_product_attributes(query, q_brand_t)
            c_attrs_t = extract_product_attributes(matched_on, '')
            tablet_pass, tablet_reasons = tablet_variant_exact_match(q_attrs_t, c_attrs_t)
            if not tablet_pass:
                gate_pass = False
                gate_reasons.extend(tablet_reasons)
        except Exception:
            pass

    # -----------------------------------------------------------------------
    # LAPTOP STRICT GATE: laptop_variant_exact_match (brand, series, processor,
    # generation, RAM, storage). No model number dependency for Windows laptops.
    # -----------------------------------------------------------------------
    if is_laptop:
        try:
            q_brand_l = result.get('_query_brand', '')
            q_attrs_l = extract_laptop_attributes(query, q_brand_l)
            c_attrs_l = extract_laptop_attributes(matched_on, '')
            laptop_pass, laptop_reasons = laptop_variant_exact_match(q_attrs_l, c_attrs_l)
            if not laptop_pass:
                gate_pass = False
                gate_reasons.extend(laptop_reasons)
        except Exception:
            pass

        # Old laptop relaxation: filter out model token/code checks from verification_gate
        # (still applies on top of strict gate above — removes false negatives from generic checks)
        if gate_reasons:
            laptop_valid_prefixes = ('category_cross:', 'storage_mismatch:', 'variant_mismatch:',
                                     'vem_storage', 'laptop_')
            gate_reasons = [r for r in gate_reasons if r.startswith(laptop_valid_prefixes)]
            gate_pass = len(gate_reasons) == 0

    if gate_pass:
        result['verification_pass'] = True
        result['verification_reasons'] = ''
        return result

    # Gate failed: downgrade to REVIEW_REQUIRED
    result['match_status'] = MATCH_STATUS_SUGGESTED
    result['confidence'] = CONFIDENCE_MEDIUM
    result['verification_pass'] = False
    result['verification_reasons'] = '; '.join(gate_reasons)
    result['method'] = method + '_gate_blocked'
    return result


def extract_variant_tokens(text: str) -> set:
    """Extract variant-identifying tokens (pro, max, ultra, fold, etc.) from text."""
    tokens = set(normalize_text(text).split())
    return tokens & VARIANT_TOKENS


def extract_model_code(text: str) -> Optional[str]:
    """Extract hardware model codes like ZE552KL, SM-G960F, A2172.
    Only matches codes with 3+ digits to avoid false positives on normal model numbers."""
    m = MODEL_CODE_PATTERN.search(normalize_text(text))
    return m.group(0).lower() if m else None


def verification_gate(query_norm: str, cand_norm: str) -> Tuple[bool, List[str]]:
    """
    Strict verification gate applied before returning MATCHED for any match path.

    Checks hard constraints:
        1. Category cross-match: both known & different → reject
        2. Storage mismatch: both present & different → reject
        3. Watch mm mismatch: both present & different → reject
        4. Primary model token mismatch: both present & different → reject
        5. Material mismatch (watches): aluminium vs steel vs titanium → reject
        6. Variant token mismatch: pro vs pro max, fold vs non-fold → reject
        7. Hardware model code mismatch: ZE552KL vs ZE520KL → reject

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
        # Filter out year tokens (2012, 2023, etc.) — catalog metadata, not model IDs
        _year_re = re.compile(r'^20[012]\d$')
        q_filtered = [t for t in q_tokens if not _year_re.match(t)]
        m_filtered = [t for t in m_tokens if not _year_re.match(t)]
        # Filter out hardware model codes (covered separately by Check 7)
        # e.g., ZE552KL, ZS630KL, SM-G960F — these have 3+ digits
        q_filtered = [t for t in q_filtered if not MODEL_CODE_PATTERN.fullmatch(t)]
        m_filtered = [t for t in m_filtered if not MODEL_CODE_PATTERN.fullmatch(t)]
        # Deduplicate tokens (NL names often repeat: "Pixel 9, Pixel 9" -> ['9','9'])
        # Use dict.fromkeys to preserve order while deduplicating
        q_filtered = list(dict.fromkeys(q_filtered))
        m_filtered = list(dict.fromkeys(m_filtered))
        # Only compare non-year, non-model-code, deduplicated tokens
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
    # Strict watch material gate: if query is a watch with material, candidate must also have
    # matching material (prevents aluminum watch matching stainless steel variant)
    if q_cat == 'watch' and m_cat == 'watch':
        if q_mat and not m_mat:
            reasons.append(f'watch_material_missing_in_candidate:{q_mat}')
        elif m_mat and not q_mat:
            # Candidate has material but query doesn't specify — allow (not strict in this direction)
            pass

    # 5b. Watch edition mismatch (Nike vs base, Hermes vs base, etc.)
    # Only fires for watches — extract_watch_edition returns '' for non-watches
    if q_cat == 'watch' or m_cat == 'watch':
        q_edition = extract_watch_edition(query_norm)
        m_edition = extract_watch_edition(cand_norm)
        if q_edition and m_edition and q_edition != m_edition:
            reasons.append(f'watch_edition_mismatch:{q_edition}→{m_edition}')
        elif q_edition and not m_edition:
            reasons.append(f'watch_edition_missing_in_candidate:{q_edition}')
        elif m_edition and not q_edition:
            reasons.append(f'watch_edition_missing_in_query:{m_edition}')

    # 6. Variant token mismatch (pro vs pro max, fold vs non-fold, etc.)
    q_variants = extract_variant_tokens(query_norm)
    m_variants = extract_variant_tokens(cand_norm)
    if q_variants != m_variants:
        reasons.append(f'variant_mismatch:{q_variants}→{m_variants}')

    # 7. Hardware model code mismatch (ZE552KL vs ZE520KL, etc.)
    q_code = extract_model_code(query_norm)
    m_code = extract_model_code(cand_norm)
    if q_code and m_code and q_code != m_code:
        reasons.append(f'model_code_mismatch:{q_code}→{m_code}')

    # 8. Tablet screen size mismatch (10.4 vs 11.0 — different products)
    # Only fires for tablets — cannot affect phones/watches/laptops
    if q_cat == 'tablet' and m_cat == 'tablet':
        q_screen_m = re.search(r'\b(\d{1,2}(?:\.\d{1,2})?)\s*(?:inch|in|"|\'\')', query_norm)
        m_screen_m = re.search(r'\b(\d{1,2}(?:\.\d{1,2})?)\s*(?:inch|in|"|\'\')', cand_norm)
        if not q_screen_m:
            q_screen_m = re.search(r'\b(\d{1,2}\.\d{1,2})\b', query_norm)
        if not m_screen_m:
            m_screen_m = re.search(r'\b(\d{1,2}\.\d{1,2})\b', cand_norm)
        if q_screen_m and m_screen_m:
            q_size = float(q_screen_m.group(1))
            m_size = float(m_screen_m.group(1))
            if 7.0 <= q_size <= 13.0 and 7.0 <= m_size <= 13.0:
                if abs(q_size - m_size) > 0.15:  # tolerance for 10.4 vs 10.5 rounding
                    reasons.append(f'tablet_screen_mismatch:{q_size}→{m_size}')

    # 9. Tablet line mismatch (pro vs base, se vs pro — different products)
    # Only fires for tablets
    if q_cat == 'tablet' and m_cat == 'tablet':
        _TABLET_LINES = {'pro', 'se', 'lite', 'air'}
        q_tl = set()
        m_tl = set()
        for kw in _TABLET_LINES:
            if re.search(r'\b' + kw + r'\b', query_norm):
                q_tl.add(kw)
            if re.search(r'\b' + kw + r'\b', cand_norm):
                m_tl.add(kw)
        if q_tl and m_tl and q_tl != m_tl:
            reasons.append(f'tablet_line_mismatch:{q_tl}→{m_tl}')
        elif q_tl and not m_tl:
            reasons.append(f'tablet_line_missing_in_candidate:{q_tl}')

    # 9b. Tablet generation mismatch (7th gen vs 5th gen — different products)
    # Only fires for tablets — extract_tablet_generation returns '' for non-tablets
    if q_cat == 'tablet' and m_cat == 'tablet':
        q_gen = extract_tablet_generation(query_norm)
        m_gen = extract_tablet_generation(cand_norm)
        if q_gen and m_gen and q_gen != m_gen:
            reasons.append(f'tablet_generation_mismatch:{q_gen}→{m_gen}')

    # 9c. Tablet/laptop screen inches mismatch (8.3 vs 10.9 — different products)
    # Only fires for tablets — uses extract_screen_inches for canonical extraction
    if q_cat == 'tablet' and m_cat == 'tablet':
        q_screen = extract_screen_inches(query_norm)
        m_screen = extract_screen_inches(cand_norm)
        if q_screen and m_screen:
            q_val = float(q_screen)
            m_val = float(m_screen)
            if abs(q_val - m_val) > 0.15:
                reasons.append(f'screen_inches_mismatch:{q_screen}→{m_screen}')

    # 10. Year mismatch (2023 vs 2024 — different model years)
    # Applies to tablets and laptops (especially MacBooks)
    q_year_m = re.search(r'\b(20[12]\d)\b', query_norm)
    m_year_m = re.search(r'\b(20[12]\d)\b', cand_norm)
    if q_year_m and m_year_m and q_year_m.group(1) != m_year_m.group(1):
        # Only enforce for categories where year distinguishes product generations
        if q_cat in ('tablet', 'laptop') or m_cat in ('tablet', 'laptop'):
            reasons.append(f'year_mismatch:{q_year_m.group(1)}→{m_year_m.group(1)}')

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

        # CRITICAL: Generation must match EXACTLY (11th != 10th, m1 != m2)
        # No tolerance — even ±1 generation can mean different CPUs/performance
        if query_gen and nl_gen:
            if query_gen == nl_gen:
                score += 15  # Exact generation match
            else:
                # Different generation → skip entirely (no ±1 tolerance)
                continue
        elif query_gen or nl_gen:
            # One has generation, other doesn't → skip
            continue
        else:
            # Neither has generation (older laptops without clear gen marking)
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
                'match_status': MATCH_STATUS_MULTIPLE,
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
    signature_index: Optional[Dict] = None,
) -> dict:
    """
    Match a single product against the NL list using hybrid matching.

    Matching strategy (cascading filters with fast path):
        0. ATTRIBUTE MATCHING (fast path): Try exact attribute match first
           - Handles 70-80% of queries in 2-5ms
           - Works especially well for Samsung (strips model codes), iPhone, Pixel, Galaxy
        0.5. SIGNATURE MATCHING: Deterministic variant resolution
           - Catches material/CPU/storage variant mismatches attribute matching misses
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
        no_match_result['method'] = 'empty_input'
        return no_match_result

    # Guard: reject NaN-like, whitespace-only, or sub-3-char queries
    # These produce spurious fuzzy matches (e.g., blank Foxway Product Name → iPad)
    query_clean = normalize_text(query)
    if not query_clean or len(query_clean) < 3 or query_clean in ('nan', 'none', 'na', 'n/a'):
        no_match_result['method'] = 'empty_input'
        return no_match_result

    # PART 5: Brand inference + empty brand guard
    # If brand is empty, try to infer from product name
    if not input_brand or input_brand.lower().strip() in ('nan', 'none', ''):
        inferred = _infer_brand_from_name(original_input or query)
        if inferred:
            input_brand = inferred
        else:
            # Cannot determine brand → REVIEW_REQUIRED, never MATCHED
            no_match_result['method'] = 'missing_brand'
            no_match_result['match_status'] = MATCH_STATUS_SUGGESTED
            no_match_result['confidence'] = CONFIDENCE_LOW
            no_match_result['verification_reasons'] = 'brand_unknown: cannot match without brand'
            return no_match_result

    try:
        result = _match_single_item_inner(
            query, nl_lookup, nl_names, threshold, brand_index,
            input_brand, attribute_index, nl_catalog, original_input,
            input_category, no_match_result, signature_index=signature_index,
        )
        result['_input_category'] = input_category or ''
        return _enforce_gate(result, query)
    except Exception:
        return no_match_result


def _match_single_item_inner(
    query, nl_lookup, nl_names, threshold, brand_index,
    input_brand, attribute_index, nl_catalog, original_input,
    input_category, no_match_result, signature_index=None,
) -> dict:
    """Inner implementation of match_single_item (wrapped by try/except)."""
    # --- Level 0: Attribute-based matching (FAST PATH) ---
    if attribute_index and input_brand:
        attr_match = try_attribute_match(query, input_brand, attribute_index, nl_catalog, original_input)
        if attr_match:
            return attr_match  # Found exact match, skip fuzzy entirely

    # --- Level 0.5: Signature-based matching (deterministic variant resolution) ---
    if signature_index and input_brand:
        sig_match = try_signature_match(query, input_brand, signature_index, nl_catalog, original_input)
        if sig_match:
            return sig_match

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
    # PART 3: Laptops NEVER use fuzzy — attribute-only or no match.
    if query_category == 'laptop' and is_laptop_product(query):
        laptop_match = match_laptop_by_attributes(
            query, input_brand, original_input,
            search_names, search_lookup, nl_catalog
        )
        if laptop_match:
            return laptop_match  # Found good attribute match
        # PART 3: No fuzzy fallback for laptops — return NO_MATCH
        return no_match_result

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
    signature_index: Optional[Dict] = None,
) -> pd.DataFrame:
    """
    Run hybrid matching for an entire input DataFrame against the NL lookup.

    Matching is hybrid (attribute-based fast path + signature + fuzzy fallback):
        0. Attribute matching (fast path) → 70-80% of queries in 2-5ms
        0.5. Signature matching → deterministic variant resolution
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
                input_category=input_category,
                signature_index=signature_index,
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

        # --- Original input fields (always included for Excel export) ---
        # Attach the original product name so export code never has to guess column names
        match_result['original_input'] = original_product_name

        # --- Category column (always included for Excel export) ---
        match_result['category'] = extract_category(query) if query else ''

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
            # Canonical/signature diagnostic columns
            q_attrs = extract_product_attributes(query, input_brand)
            q_sig = build_variant_signature(q_attrs)
            match_result['canonical_key_query'] = q_sig
            if matched_on:
                m_attrs = extract_product_attributes(matched_on, input_brand)
                m_sig = build_variant_signature(m_attrs)
                match_result['canonical_key_match'] = m_sig
            else:
                match_result['canonical_key_match'] = ''
            match_result['canonical_match_used'] = match_result.get('method', '') == 'signature'
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
    df['original_input'] = results_df['original_input'].values
    df['mapped_uae_assetid'] = results_df['mapped_uae_assetid'].values
    df['match_score'] = results_df['match_score'].values
    df['match_status'] = results_df['match_status'].values
    df['confidence'] = results_df['confidence'].values
    df['matched_on'] = results_df['matched_on'].values
    df['method'] = results_df['method'].values
    df['auto_selected'] = results_df['auto_selected'].values
    df['selection_reason'] = results_df['selection_reason'].values
    df['alternatives'] = results_df['alternatives'].values
    df['category'] = results_df['category'].values
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
    signature_index: Optional[Dict] = None,
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
                             original_input=name, signature_index=signature_index)

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


# ---------------------------------------------------------------------------
# Self-test: verification gate correctness
# ---------------------------------------------------------------------------

def self_test_verification() -> List[str]:
    """
    Run built-in sanity checks for the verification gate.

    Returns a list of failure messages (empty list = all passed).
    """
    failures: List[str] = []

    cases = [
        # (query, candidate, expected_pass, description)

        # --- PHONE VARIANT TESTS ---
        # 1. Pixel Fold must NOT match Pixel Pro
        ('google pixel 9 pro fold 256gb', 'google pixel 9 pro 256gb', False,
         'Pixel Pro Fold vs Pro should fail'),
        # 2. Pro Max must NOT match Pro
        ('apple iphone 15 pro max 256gb', 'apple iphone 15 pro 256gb', False,
         'Pro Max vs Pro should fail'),
        # 3. Galaxy S23 Ultra must NOT match S23
        ('samsung galaxy s23 ultra 256gb', 'samsung galaxy s23 256gb', False,
         'S23 Ultra vs S23 should fail'),
        # 4. Flip vs Fold (different product lines)
        ('samsung galaxy z flip5 256gb', 'samsung galaxy z fold5 256gb', False,
         'Flip vs Fold should fail'),
        # 5. Lite vs non-Lite
        ('huawei p40 128gb', 'huawei p40 lite 128gb', False,
         'P40 vs P40 Lite should fail'),
        # 6. Mini vs non-Mini
        ('apple iphone 13 128gb', 'apple iphone 13 mini 128gb', False,
         'iPhone 13 vs iPhone 13 Mini should fail'),
        # 7. Plus vs non-Plus
        ('samsung galaxy s21 128gb', 'samsung galaxy s21 plus 128gb', False,
         'Galaxy S21 vs S21 Plus should fail'),
        # 8. FE vs non-FE
        ('samsung galaxy s21 128gb', 'samsung galaxy s21 fe 128gb', False,
         'Galaxy S21 vs S21 FE should fail'),

        # --- STORAGE TESTS ---
        # 9. Storage mismatch should fail
        ('apple iphone 14 128gb', 'apple iphone 14 256gb', False,
         'Storage 128gb vs 256gb should fail'),
        # 10. Exact match should pass
        ('apple iphone 14 128gb', 'apple iphone 14 128gb', True,
         'Exact phone match should pass'),

        # --- WATCH TESTS ---
        # 11. Watch mm mismatch should fail
        ('apple watch series 9 45mm gps', 'apple watch series 9 41mm gps', False,
         'Watch 45mm vs 41mm should fail'),
        # 12. Watch material mismatch should fail
        ('apple watch series 9 45mm gps stainless steel',
         'apple watch series 9 45mm gps aluminum', False,
         'Watch stainless vs aluminum should fail'),
        # 13. Exact watch match should pass
        ('apple watch series 9 45mm gps aluminum',
         'apple watch series 9 45mm gps aluminum', True,
         'Exact watch match should pass'),

        # --- TABLET TESTS ---
        # 14. Category cross-match: tablet vs phone
        ('samsung galaxy tab s8 128gb', 'samsung galaxy s8 128gb', False,
         'Galaxy Tab S8 (tablet) vs Galaxy S8 (phone) should fail'),

        # --- LAPTOP TESTS ---
        # 15. Model code mismatch should fail
        ('asus zenfone ze552kl 64gb', 'asus zenfone ze520kl 64gb', False,
         'Model code ZE552KL vs ZE520KL should fail'),

        # --- NORMALIZATION TESTS ---
        # 16. OnePlus Nord exact match should pass
        ('oneplus nord 128gb', 'oneplus nord 128gb', True,
         'OnePlus Nord exact match should pass'),
        # 17. Different model numbers should fail
        ('apple iphone 14 128gb', 'apple iphone 15 128gb', False,
         'iPhone 14 vs iPhone 15 should fail'),
        # 18. Mate X3 vs Mate X3 Fold
        ('huawei mate x3 256gb', 'huawei mate x3 fold 256gb', False,
         'Mate X3 vs Mate X3 Fold should fail'),
        # 19. Galaxy Note vs Galaxy (different product line)
        ('samsung galaxy note 10 256gb', 'samsung galaxy s10 256gb', False,
         'Galaxy Note 10 vs Galaxy S10 should fail'),
        # 20. Exact laptop match should pass
        ('dell latitude core i5 11th gen 16gb 512gb',
         'dell latitude core i5 11th gen 16gb 512gb', True,
         'Exact laptop match should pass'),
    ]

    for query, candidate, expected_pass, desc in cases:
        q_norm = normalize_text(query)
        c_norm = normalize_text(candidate)
        actual_pass, reasons = verification_gate(q_norm, c_norm)
        if actual_pass != expected_pass:
            failures.append(
                f'FAIL: {desc} — expected {"pass" if expected_pass else "fail"}, '
                f'got {"pass" if actual_pass else "fail"} (reasons: {reasons})'
            )

    # --- Normalization / extraction sanity checks ---
    # 21. TBT3 must NOT be extracted as TB storage
    attrs = extract_product_attributes(
        'Apple MacBook Pro (13" 2020, 2 TBT3) - Core i5 / 8GB / 256GB SSD', 'Apple'
    )
    if attrs.get('storage') != '256gb':
        failures.append(
            f'FAIL: TBT3 storage extraction — expected "256gb", '
            f'got "{attrs.get("storage")}"'
        )

    # 22. Normal TB must still extract correctly
    attrs = extract_product_attributes('Dell XPS 15 1TB SSD 16GB RAM', 'Dell')
    if attrs.get('storage') != '1tb':
        failures.append(
            f'FAIL: Normal TB extraction — expected "1tb", '
            f'got "{attrs.get("storage")}"'
        )

    # 23. Brand normalization: "One Plus" -> "oneplus"
    if normalize_brand('One Plus') != 'oneplus':
        failures.append(
            f'FAIL: Brand "One Plus" — expected "oneplus", '
            f'got "{normalize_brand("One Plus")}"'
        )

    # 24. Brand normalization: "Dell OLD" -> "dell"
    if normalize_brand('Dell OLD') != 'dell':
        failures.append(
            f'FAIL: Brand "Dell OLD" — expected "dell", '
            f'got "{normalize_brand("Dell OLD")}"'
        )

    # --- Signature material isolation tests ---
    # 25. Watch signatures with different materials must produce distinct keys
    sig_al = build_variant_signature(
        extract_product_attributes('apple watch series 9 45mm gps aluminum', 'apple'))
    sig_ss = build_variant_signature(
        extract_product_attributes('apple watch series 9 45mm gps stainless steel', 'apple'))
    if sig_al == sig_ss:
        failures.append(
            f'FAIL: Aluminum vs Stainless signature collision — '
            f'both produced "{sig_al}"'
        )

    # 26. Aluminum signature must contain "aluminum", stainless must contain "stainless"
    if 'aluminum' not in sig_al:
        failures.append(
            f'FAIL: Aluminum signature missing material — got "{sig_al}"'
        )
    if 'stainless' not in sig_ss:
        failures.append(
            f'FAIL: Stainless signature missing material — got "{sig_ss}"'
        )

    # 27. Phone signature must NOT be affected by watch material logic
    sig_phone = build_variant_signature(
        extract_product_attributes('apple iphone 14 pro 128gb', 'apple'))
    if not sig_phone or 'iphone' not in sig_phone:
        failures.append(
            f'FAIL: Phone signature broken — got "{sig_phone}"'
        )

    # --- Watch material abbreviation tests ---
    # 28. "SS" abbreviation must resolve to stainless
    mat_ss = extract_watch_material('apple watch series 9 45mm gps ss')
    if mat_ss != 'stainless':
        failures.append(
            f'FAIL: "SS" material — expected "stainless", got "{mat_ss}"'
        )

    # 29. "alu" abbreviation must resolve to aluminum
    mat_alu = extract_watch_material('apple watch ultra 2 49mm cellular alu')
    if mat_alu != 'aluminum':
        failures.append(
            f'FAIL: "alu" material — expected "aluminum", got "{mat_alu}"'
        )

    # 30. Full "titanium" must still resolve correctly
    mat_ti = extract_watch_material('apple watch series 8 41mm gps titanium')
    if mat_ti != 'titanium':
        failures.append(
            f'FAIL: "titanium" material — expected "titanium", got "{mat_ti}"'
        )

    # 31. "ceramic" must still resolve correctly
    mat_cer = extract_watch_material('apple watch series 7 45mm gps ceramic')
    if mat_cer != 'ceramic':
        failures.append(
            f'FAIL: "ceramic" material — expected "ceramic", got "{mat_cer}"'
        )

    # --- REGRESSION TESTS (production integrity audit) ---

    # 32. Watch material separation: aluminum gate blocks stainless
    q32 = normalize_text('apple watch series 9 45mm gps aluminum')
    c32 = normalize_text('apple watch series 9 45mm gps stainless steel')
    pass32, _ = verification_gate(q32, c32)
    if pass32:
        failures.append('FAIL: Watch aluminum vs stainless should be rejected by gate')

    # 33. Watch material: titanium gate blocks aluminum
    q33 = normalize_text('apple watch ultra 2 49mm cellular titanium')
    c33 = normalize_text('apple watch ultra 2 49mm cellular aluminum')
    pass33, _ = verification_gate(q33, c33)
    if pass33:
        failures.append('FAIL: Watch titanium vs aluminum should be rejected by gate')

    # 34. Fold vs non-fold: signature collision regression
    sig_fold = build_variant_signature(
        extract_product_attributes('samsung galaxy z fold5 256gb', 'samsung'))
    sig_flip = build_variant_signature(
        extract_product_attributes('samsung galaxy z flip5 256gb', 'samsung'))
    if sig_fold == sig_flip:
        failures.append(
            f'FAIL: Fold5 vs Flip5 signature collision — both "{sig_fold}"')
    if not sig_fold or 'fold' not in sig_fold:
        failures.append(
            f'FAIL: Fold5 signature missing "fold" — got "{sig_fold}"')
    if not sig_flip or 'flip' not in sig_flip:
        failures.append(
            f'FAIL: Flip5 signature missing "flip" — got "{sig_flip}"')

    # 35. Fold vs Flip gate rejection
    q35 = normalize_text('samsung galaxy z fold5 256gb')
    c35 = normalize_text('samsung galaxy z flip5 256gb')
    pass35, _ = verification_gate(q35, c35)
    if pass35:
        failures.append('FAIL: Galaxy Z Fold5 vs Flip5 should be rejected by gate')

    # 36. Ultra vs non-ultra gate rejection
    q36 = normalize_text('samsung galaxy s23 ultra 256gb')
    c36 = normalize_text('samsung galaxy s23 256gb')
    pass36, _ = verification_gate(q36, c36)
    if pass36:
        failures.append('FAIL: Galaxy S23 Ultra vs S23 should be rejected by gate')

    # 37. Storage mismatch gate rejection
    q37 = normalize_text('apple iphone 15 pro 128gb')
    c37 = normalize_text('apple iphone 15 pro 256gb')
    pass37, _ = verification_gate(q37, c37)
    if pass37:
        failures.append('FAIL: iPhone 15 Pro 128gb vs 256gb should be rejected by gate')

    # 38. Pro vs Pro Max gate rejection (variant mismatch)
    q38 = normalize_text('apple iphone 15 pro 256gb')
    c38 = normalize_text('apple iphone 15 pro max 256gb')
    pass38, _ = verification_gate(q38, c38)
    if pass38:
        failures.append('FAIL: iPhone 15 Pro vs Pro Max should be rejected by gate')

    # 39. variant_exact_match: material mismatch
    vem_q = extract_product_attributes('apple watch series 9 45mm gps aluminum', 'apple')
    vem_c = extract_product_attributes('apple watch series 9 45mm gps stainless steel', 'apple')
    vem_pass, _ = variant_exact_match(vem_q, vem_c)
    if vem_pass:
        failures.append('FAIL: variant_exact_match should reject aluminum vs stainless')

    # 40. variant_exact_match: fold vs flip model mismatch
    vem_fold = extract_product_attributes('samsung galaxy z fold5 256gb', 'samsung')
    vem_flip = extract_product_attributes('samsung galaxy z flip5 256gb', 'samsung')
    vem_pass2, _ = variant_exact_match(vem_fold, vem_flip)
    if vem_pass2:
        failures.append('FAIL: variant_exact_match should reject fold5 vs flip5')

    # === TASK A: Watch edition regression tests ===

    # 41. Nike edition vs base watch → reject
    p41, _ = verification_gate(
        normalize_text('apple watch series 9 45mm gps nike aluminum'),
        normalize_text('apple watch series 9 45mm gps aluminum'))
    if p41:
        failures.append('FAIL: Watch Nike vs base should be rejected')

    # 42. Black Unity vs base watch → reject
    p42, _ = verification_gate(
        normalize_text('apple watch series 9 45mm black unity'),
        normalize_text('apple watch series 9 45mm'))
    if p42:
        failures.append('FAIL: Watch Black Unity vs base should be rejected')

    # 43. Hermes vs base watch → reject
    p43, _ = verification_gate(
        normalize_text('apple watch series 9 45mm hermes'),
        normalize_text('apple watch series 9 45mm'))
    if p43:
        failures.append('FAIL: Watch Hermes vs base should be rejected')

    # 44. Edition vs base watch → reject
    p44, _ = verification_gate(
        normalize_text('apple watch series 9 45mm special edition'),
        normalize_text('apple watch series 9 45mm'))
    if p44:
        failures.append('FAIL: Watch Special Edition vs base should be rejected')

    # 45. Nike vs Hermes → reject
    p45, _ = verification_gate(
        normalize_text('apple watch series 9 45mm nike'),
        normalize_text('apple watch series 9 45mm hermes'))
    if p45:
        failures.append('FAIL: Watch Nike vs Hermes should be rejected')

    # 46. Matching Nike editions → pass
    p46, _ = verification_gate(
        normalize_text('apple watch series 9 45mm nike aluminum'),
        normalize_text('apple watch series 9 45mm nike aluminum'))
    if not p46:
        failures.append('FAIL: Identical Nike watch should pass gate')

    # === TASK B: Tablet size + line regression tests ===

    # 47. MatePad 10.4 vs MatePad Pro 11.0 → reject (size + line mismatch)
    p47, r47 = verification_gate(
        normalize_text('huawei matepad 10.4 2022 128gb'),
        normalize_text('huawei matepad pro 11.0 2022 128gb'))
    if p47:
        failures.append(f'FAIL: MatePad 10.4 vs MatePad Pro 11.0 should be rejected: {r47}')

    # 48. iPad Pro 11 vs iPad Pro 12.9 → reject (size mismatch)
    p48, _ = verification_gate(
        normalize_text('apple ipad pro 11 2022 256gb'),
        normalize_text('apple ipad pro 12.9 2022 256gb'))
    if p48:
        failures.append('FAIL: iPad Pro 11 vs 12.9 should be rejected')

    # 49. MatePad base vs MatePad Pro → reject (tablet_line mismatch)
    p49, _ = verification_gate(
        normalize_text('huawei matepad 10.4 128gb'),
        normalize_text('huawei matepad pro 10.4 128gb'))
    if p49:
        failures.append('FAIL: MatePad base vs MatePad Pro should be rejected')

    # === TASK C: MacBook year strictness ===

    # 50. MacBook Pro 2023 vs MacBook Pro 2024 → reject
    p50, _ = verification_gate(
        normalize_text('apple macbook pro 2023 m3 16gb 512gb'),
        normalize_text('apple macbook pro 2024 m3 16gb 512gb'))
    if p50:
        failures.append('FAIL: MacBook Pro 2023 vs 2024 should be rejected')

    # 51. MacBook Pro 2023 vs MacBook Pro 2023 → pass
    p51, _ = verification_gate(
        normalize_text('apple macbook pro 2023 m3 16gb 512gb'),
        normalize_text('apple macbook pro 2023 m3 16gb 512gb'))
    if not p51:
        failures.append('FAIL: Identical MacBook Pro 2023 should pass gate')

    # === TASK D: Empty input guard ===

    # 52. extract_watch_edition extraction test
    if extract_watch_edition('apple watch series 9 45mm nike') != 'nike':
        failures.append('FAIL: extract_watch_edition should detect "nike"')

    # 53. extract_watch_edition extraction for hermes
    if extract_watch_edition('apple watch ultra 2 49mm hermes') != 'hermes':
        failures.append('FAIL: extract_watch_edition should detect "hermes"')

    # 54. Empty/NaN query guard: empty string
    empty_result = match_single_item('', {}, [], 85)
    if empty_result['method'] != 'empty_input':
        failures.append(f'FAIL: Empty string should return empty_input, got {empty_result["method"]}')

    # 55. Empty/NaN query guard: whitespace
    ws_result = match_single_item('   ', {}, [], 85)
    if ws_result['method'] != 'empty_input':
        failures.append(f'FAIL: Whitespace should return empty_input, got {ws_result["method"]}')

    # 56. Empty/NaN query guard: "nan"
    nan_result = match_single_item('nan', {}, [], 85)
    if nan_result['method'] != 'empty_input':
        failures.append(f'FAIL: "nan" should return empty_input, got {nan_result["method"]}')

    # === PATCH 8: New regression tests ===

    # 57. iPad Mini 7th gen vs iPad Mini 5th gen → reject (generation mismatch)
    p57, r57 = verification_gate(
        normalize_text('apple ipad mini 7th gen 256gb'),
        normalize_text('apple ipad mini 5th gen 256gb'))
    if p57:
        failures.append(f'FAIL: iPad Mini 7th gen vs 5th gen should be rejected: {r57}')

    # 58. MatePad 10.4 vs MatePad 11 → reject (screen inches mismatch)
    p58, r58 = verification_gate(
        normalize_text('huawei matepad 10.4 128gb'),
        normalize_text('huawei matepad 11 inch 128gb'))
    if p58:
        failures.append(f'FAIL: MatePad 10.4 vs 11 should be rejected: {r58}')

    # 59. Watch Nike vs standard → reject (edition mismatch)
    p59, r59 = verification_gate(
        normalize_text('apple watch series 9 45mm gps nike'),
        normalize_text('apple watch series 9 45mm gps'))
    if p59:
        failures.append(f'FAIL: Watch Nike vs standard should be rejected: {r59}')

    # 60. Empty input guard: match_single_item returns NO_MATCH for "  "
    empty2_result = match_single_item('  ', {}, [], 85)
    if empty2_result['match_status'] != MATCH_STATUS_NO_MATCH:
        failures.append(f'FAIL: Empty input should return NO_MATCH, got {empty2_result["match_status"]}')

    # 61. extract_tablet_generation: "7th gen" → "7"
    if extract_tablet_generation('apple ipad mini 7th gen 256gb') != '7':
        failures.append(
            f'FAIL: extract_tablet_generation("...7th gen...") — expected "7", '
            f'got "{extract_tablet_generation("apple ipad mini 7th gen 256gb")}"')

    # 62. extract_tablet_generation: "gen5" (from normalize_text) → "5"
    if extract_tablet_generation('apple ipad gen5 wifi 128gb') != '5':
        failures.append(
            f'FAIL: extract_tablet_generation("...gen5...") — expected "5", '
            f'got "{extract_tablet_generation("apple ipad gen5 wifi 128gb")}"')

    # 63. extract_screen_inches: "8.3 inch" → "8.3"
    if extract_screen_inches('apple ipad mini 8.3 inch 256gb') != '8.3':
        failures.append(
            f'FAIL: extract_screen_inches("...8.3 inch...") — expected "8.3", '
            f'got "{extract_screen_inches("apple ipad mini 8.3 inch 256gb")}"')

    # 64. extract_screen_inches: bare "10.4" → "10.4"
    if extract_screen_inches('huawei matepad 10.4 128gb') != '10.4':
        failures.append(
            f'FAIL: extract_screen_inches("...10.4...") — expected "10.4", '
            f'got "{extract_screen_inches("huawei matepad 10.4 128gb")}"')

    # 65. Signature includes generation: iPad mini 7th gen
    # Generation is encoded as "gen7" in the model part (from normalize_text "7th gen" → "gen7")
    sig65 = build_variant_signature(
        extract_product_attributes('apple ipad mini 7th gen 256gb', 'apple'))
    if 'gen7' not in sig65:
        failures.append(f'FAIL: iPad mini 7th gen signature missing generation — got "{sig65}"')

    # === Laptop attribute matching tests ===

    # 66. extract_cpu_generation: normalized "gen8" format (from "8th gen")
    gen66 = extract_cpu_generation('dell latitude core i5 gen8 16gb 5490 14 inch 256gb ssd')
    if gen66 != '8th gen':
        failures.append(f'FAIL: extract_cpu_generation("gen8") should return "8th gen", got "{gen66}"')

    # 67. extract_cpu_generation: normalized "i5 1245u" (dash stripped by normalize_text)
    gen67 = extract_cpu_generation('dell latitude 5530 15 6 core i5 1245u 16gb 256gb ssd')
    if gen67 != '12th gen':
        failures.append(f'FAIL: extract_cpu_generation("i5 1245u") should return "12th gen", got "{gen67}"')

    # 68. extract_cpu_generation: original format "i5-1245U" still works
    gen68 = extract_cpu_generation('Dell Latitude 5530 Core i5-1245U 16GB 256GB SSD')
    if gen68 != '12th gen':
        failures.append(f'FAIL: extract_cpu_generation("i5-1245U") should return "12th gen", got "{gen68}"')

    # 69. extract_cpu_generation: normalized "gen11" format
    gen69 = extract_cpu_generation('acer predator helios 300 gen11 intel 17 3 core i5 11400h 8gb 512gb ssd')
    if gen69 != '11th gen':
        failures.append(f'FAIL: extract_cpu_generation("i5 11400h") should return "11th gen", got "{gen69}"')

    # 70. Laptop attribute extraction: processor + generation + ram + storage
    laptop_attrs = extract_laptop_attributes(
        'dell latitude core i5 gen8 16gb 5490 14 inch 256gb ssd', 'dell')
    if laptop_attrs['processor'] != 'i5':
        failures.append(f'FAIL: Laptop processor should be "i5", got "{laptop_attrs["processor"]}"')
    if laptop_attrs['generation'] != '8th gen':
        failures.append(f'FAIL: Laptop generation should be "8th gen", got "{laptop_attrs["generation"]}"')
    if laptop_attrs['ram'] != '16gb':
        failures.append(f'FAIL: Laptop RAM should be "16gb", got "{laptop_attrs["ram"]}"')
    if laptop_attrs['storage'] != '256gb':
        failures.append(f'FAIL: Laptop storage should be "256gb", got "{laptop_attrs["storage"]}"')
    if laptop_attrs['product_line'] != 'latitude':
        failures.append(f'FAIL: Laptop product_line should be "latitude", got "{laptop_attrs["product_line"]}"')

    # 71. Laptop generation mismatch prevents match (12th gen query vs 8th gen NL)
    nl_names_71 = ['dell latitude core i5 gen8 16gb 5490 14 inch 256gb ssd']
    nl_lookup_71 = {nl_names_71[0]: ['NL-DELL-001']}
    laptop_result_71 = match_laptop_by_attributes(
        'dell latitude 5530 15 6 core i5 1245u 16gb 256gb ssd',
        'dell', 'Dell Latitude 5530 Core i5-1245U 16GB 256GB SSD',
        nl_names_71, nl_lookup_71, None)
    if laptop_result_71 is not None:
        failures.append(f'FAIL: 12th gen should NOT match 8th gen laptop, got match')

    # 72. Laptop exact attribute match succeeds (same gen, same specs)
    nl_names_72 = ['dell latitude core i5 gen8 16gb 5490 14 inch 256gb ssd']
    nl_lookup_72 = {nl_names_72[0]: ['NL-DELL-002']}
    laptop_result_72 = match_laptop_by_attributes(
        'dell latitude core i5 gen8 16gb 256gb ssd',
        'dell', 'Dell Latitude Core i5 8th Gen 16GB 256GB SSD',
        nl_names_72, nl_lookup_72, None)
    if laptop_result_72 is None:
        failures.append(f'FAIL: Same-gen same-specs laptop should match')
    elif laptop_result_72['mapped_uae_assetid'] != 'NL-DELL-002':
        failures.append(f'FAIL: Laptop match should return NL-DELL-002, got {laptop_result_72["mapped_uae_assetid"]}')

    # === PART 7: Safety regression tests ===

    # 73. iPhone 14 Pro must NOT match iPhone 14 (mobile_variant_exact_match)
    q73 = extract_product_attributes('apple iphone 14 pro 256gb', 'apple')
    c73 = extract_product_attributes('apple iphone 14 256gb', 'apple')
    pass73, _ = mobile_variant_exact_match(q73, c73)
    if pass73:
        failures.append('FAIL: iPhone 14 Pro should NOT match iPhone 14 (variant mismatch)')

    # 74. Galaxy S23 Ultra must NOT match Galaxy S23 (mobile_variant_exact_match)
    q74 = extract_product_attributes('samsung galaxy s23 ultra 256gb', 'samsung')
    c74 = extract_product_attributes('samsung galaxy s23 256gb', 'samsung')
    pass74, _ = mobile_variant_exact_match(q74, c74)
    if pass74:
        failures.append('FAIL: Galaxy S23 Ultra should NOT match Galaxy S23 (variant mismatch)')

    # 75. MatePad Pro 10.4 must NOT match MatePad Pro 11 (tablet screen mismatch)
    q_attrs75 = {'screen_inches': '10.4', 'screen_size': '10.4', 'generation': '', 'brand': 'huawei', 'product_line': 'matepad', 'model': 'pro'}
    c_attrs75 = {'screen_inches': '11', 'screen_size': '11', 'generation': '', 'brand': 'huawei', 'product_line': 'matepad', 'model': 'pro'}
    if q_attrs75['screen_inches'] == c_attrs75['screen_inches']:
        failures.append('FAIL: MatePad Pro 10.4 should NOT match MatePad Pro 11 (screen mismatch)')

    # 76. Laptop generation mismatch must NOT match
    nl_names_76 = ['dell latitude core i5 gen12 16gb 256gb ssd']
    nl_lookup_76 = {nl_names_76[0]: ['NL-DELL-GEN12']}
    laptop_result_76 = match_laptop_by_attributes(
        'dell latitude core i5 gen8 16gb 256gb ssd',
        'dell', 'Dell Latitude Core i5 8th Gen 16GB 256GB SSD',
        nl_names_76, nl_lookup_76, None)
    if laptop_result_76 is not None:
        failures.append('FAIL: Laptop gen 8 should NOT match gen 12')

    # 77. Laptop missing RAM must not match (returns None from match_laptop_by_attributes)
    nl_names_77 = ['dell latitude core i5 gen8 16gb 256gb ssd']
    nl_lookup_77 = {nl_names_77[0]: ['NL-DELL-RAM']}
    laptop_result_77 = match_laptop_by_attributes(
        'dell latitude core i5 gen8 256gb ssd',   # no RAM
        'dell', 'Dell Latitude Core i5 8th Gen 256GB SSD',
        nl_names_77, nl_lookup_77, None)
    if laptop_result_77 is not None:
        failures.append('FAIL: Laptop missing RAM should return None (incomplete attrs)')

    # 78. iPhone 14 Pro Max must NOT match iPhone 14 Pro (variant tokens differ)
    q78 = extract_product_attributes('apple iphone 14 pro max 256gb', 'apple')
    c78 = extract_product_attributes('apple iphone 14 pro 256gb', 'apple')
    pass78, _ = mobile_variant_exact_match(q78, c78)
    if pass78:
        failures.append('FAIL: iPhone 14 Pro Max should NOT match iPhone 14 Pro')

    # 79. Same mobile with exact attributes should pass mobile gate
    q79 = extract_product_attributes('apple iphone 14 pro 256gb', 'apple')
    c79 = extract_product_attributes('apple iphone 14 pro 256gb', 'apple')
    pass79, _ = mobile_variant_exact_match(q79, c79)
    if not pass79:
        failures.append('FAIL: iPhone 14 Pro 256GB should match itself')

    # 80. Fuzzy method results must be downgraded to REVIEW_REQUIRED by _enforce_gate
    fuzzy_result_80 = {
        'match_status': MATCH_STATUS_MATCHED,
        'method': 'fuzzy',
        'matched_on': 'apple iphone 14 pro 256gb',
        'confidence': CONFIDENCE_HIGH,
    }
    gated_80 = _enforce_gate(fuzzy_result_80, 'apple iphone 14 pro 256gb')
    if gated_80['match_status'] != MATCH_STATUS_SUGGESTED:
        failures.append(f'FAIL: Fuzzy MATCHED should be downgraded to REVIEW_REQUIRED, got {gated_80["match_status"]}')

    # === PART 7 (Mobile Hardening): Safety regression tests ===

    # 81. Galaxy S23 vs Galaxy S23 FE must NOT match (samsung_variant: base != fe)
    q81 = extract_product_attributes('samsung galaxy s23 256gb', 'samsung')
    c81 = extract_product_attributes('samsung galaxy s23 fe 256gb', 'samsung')
    pass81, reasons81 = mobile_variant_exact_match(q81, c81)
    if pass81:
        failures.append('FAIL: Galaxy S23 should NOT match Galaxy S23 FE (variant mismatch)')

    # 82. Galaxy S23 FE vs Galaxy S23 FE should match (identical)
    q82 = extract_product_attributes('samsung galaxy s23 fe 128gb', 'samsung')
    c82 = extract_product_attributes('samsung galaxy s23 fe 128gb', 'samsung')
    pass82, _ = mobile_variant_exact_match(q82, c82)
    if not pass82:
        failures.append('FAIL: Galaxy S23 FE should match itself')

    # 83. Galaxy S23 vs Galaxy S24 must NOT match (samsung_s_number: s23 != s24)
    q83 = extract_product_attributes('samsung galaxy s23 256gb', 'samsung')
    c83 = extract_product_attributes('samsung galaxy s24 256gb', 'samsung')
    pass83, reasons83 = mobile_variant_exact_match(q83, c83)
    if pass83:
        failures.append('FAIL: Galaxy S23 should NOT match Galaxy S24 (s-number mismatch)')

    # 84. Galaxy S23 Ultra vs Galaxy S23 Plus must NOT match (samsung_variant: ultra != plus)
    q84 = extract_product_attributes('samsung galaxy s23 ultra 256gb', 'samsung')
    c84 = extract_product_attributes('samsung galaxy s23 plus 256gb', 'samsung')
    pass84, reasons84 = mobile_variant_exact_match(q84, c84)
    if pass84:
        failures.append('FAIL: Galaxy S23 Ultra should NOT match Galaxy S23 Plus')

    # 85. iPad Mini 5th gen vs iPad Mini 7th gen must NOT match (verification gate)
    p85, r85 = verification_gate(
        normalize_text('apple ipad mini 5th gen 64gb'),
        normalize_text('apple ipad mini 7th gen 64gb'))
    if p85:
        failures.append(f'FAIL: iPad Mini 5th gen vs 7th gen should be rejected: {r85}')

    # 86. MatePad 10.4 vs MatePad 11 must NOT match (screen inches mismatch)
    p86, r86 = verification_gate(
        normalize_text('huawei matepad 10.4 2022 128gb'),
        normalize_text('huawei matepad 11 2022 128gb'))
    if p86:
        failures.append(f'FAIL: MatePad 10.4 vs MatePad 11 should be rejected: {r86}')

    # 87. _extract_galaxy_s_number: "s23" from "galaxy s23 ultra"
    snum87 = _extract_galaxy_s_number('s23 ultra')
    if snum87 != 's23':
        failures.append(f'FAIL: _extract_galaxy_s_number("s23 ultra") should be "s23", got "{snum87}"')

    # 88. _extract_galaxy_variant: "fe" from "galaxy s23 fe"
    gvar88 = _extract_galaxy_variant('s23 fe', 'samsung galaxy s23 fe 256gb')
    if gvar88 != 'fe':
        failures.append(f'FAIL: _extract_galaxy_variant for S23 FE should be "fe", got "{gvar88}"')

    # 89. _extract_galaxy_variant: "base" from "galaxy s23" (no variant)
    gvar89 = _extract_galaxy_variant('s23', 'samsung galaxy s23 256gb')
    if gvar89 != 'base':
        failures.append(f'FAIL: _extract_galaxy_variant for S23 base should be "base", got "{gvar89}"')

    # 90. Model number enforcement: different model codes should fail
    q90 = {'brand': 'asus', 'product_line': 'zenfone', 'model': '3', 'storage': '64gb',
           'model_number': 'ze552kl', 'variant': '', 'generation': '3'}
    c90 = {'brand': 'asus', 'product_line': 'zenfone', 'model': '3', 'storage': '64gb',
           'model_number': 'ze520kl', 'variant': '', 'generation': '3'}
    pass90, reasons90 = mobile_variant_exact_match(q90, c90)
    if pass90:
        failures.append('FAIL: Zenfone ZE552KL should NOT match ZE520KL (model_number mismatch)')

    # === PART E: Tablet hardening regression tests ===

    # 91. iPad Mini 5th gen vs iPad Mini 7th gen must NOT match (generation)
    t91q = extract_product_attributes(normalize_text('apple ipad mini 5th gen 64gb wifi'), 'apple')
    t91c = extract_product_attributes(normalize_text('apple ipad mini 7th gen 64gb wifi'), 'apple')
    pass91, r91 = tablet_variant_exact_match(t91q, t91c)
    if pass91:
        failures.append(f'FAIL: iPad Mini 5th gen vs 7th gen should NOT match: {r91}')

    # 92. iPad Pro 11 vs iPad Pro 12.9 must NOT match (size)
    t92q = extract_product_attributes(normalize_text('apple ipad pro 11 inch 2022 256gb'), 'apple')
    t92c = extract_product_attributes(normalize_text('apple ipad pro 12.9 inch 2022 256gb'), 'apple')
    pass92, r92 = tablet_variant_exact_match(t92q, t92c)
    if pass92:
        failures.append(f'FAIL: iPad Pro 11 vs 12.9 should NOT match: {r92}')

    # 93. iPad Pro 12.9 vs iPad Pro 13 must NOT match (12.9 != 13)
    t93q = extract_product_attributes(normalize_text('apple ipad pro 12.9 inch 2022 256gb'), 'apple')
    t93c = extract_product_attributes(normalize_text('apple ipad pro 13 inch 2024 256gb'), 'apple')
    pass93, r93 = tablet_variant_exact_match(t93q, t93c)
    if pass93:
        failures.append(f'FAIL: iPad Pro 12.9 vs 13 should NOT match: {r93}')

    # 94. MatePad 10.4 vs MatePad 11 must NOT match (size)
    t94q = extract_product_attributes(normalize_text('huawei matepad 10.4 2022 128gb'), 'huawei')
    t94c = extract_product_attributes(normalize_text('huawei matepad 11 inch 2022 128gb'), 'huawei')
    pass94, r94 = tablet_variant_exact_match(t94q, t94c)
    if pass94:
        failures.append(f'FAIL: MatePad 10.4 vs 11 should NOT match: {r94}')

    # 95. MediaPad Lite 8 vs generic 8" must NOT match (variant "lite")
    t95q = extract_product_attributes(normalize_text('huawei mediapad lite 8 inch 32gb'), 'huawei')
    t95c = extract_product_attributes(normalize_text('huawei mediapad 8 inch 32gb'), 'huawei')
    pass95, r95 = tablet_variant_exact_match(t95q, t95c)
    if pass95:
        failures.append(f'FAIL: MediaPad Lite 8 vs generic 8" should NOT match: {r95}')

    # 96. iPad SE vs iPad (non-SE) must NOT match (variant "se")
    t96q = extract_product_attributes(normalize_text('apple ipad se 10.9 inch 256gb'), 'apple')
    t96c = extract_product_attributes(normalize_text('apple ipad 10.9 inch 256gb'), 'apple')
    pass96, r96 = tablet_variant_exact_match(t96q, t96c)
    if pass96:
        failures.append(f'FAIL: iPad SE vs iPad should NOT match: {r96}')

    # 97. Positive: iPad Mini 7th gen matches itself
    t97q = extract_product_attributes(normalize_text('apple ipad mini 7th gen 256gb wifi'), 'apple')
    t97c = extract_product_attributes(normalize_text('apple ipad mini 7th gen 256gb wifi'), 'apple')
    pass97, _ = tablet_variant_exact_match(t97q, t97c)
    if not pass97:
        failures.append('FAIL: iPad Mini 7th gen 256gb should match itself')

    # 98. Positive: iPad Pro 11 2022 matches itself
    t98q = extract_product_attributes(normalize_text('apple ipad pro 11 inch 2022 256gb'), 'apple')
    t98c = extract_product_attributes(normalize_text('apple ipad pro 11 inch 2022 256gb'), 'apple')
    pass98, _ = tablet_variant_exact_match(t98q, t98c)
    if not pass98:
        failures.append('FAIL: iPad Pro 11 2022 256gb should match itself')

    # 99. Positive: MatePad Pro 11 matches itself
    t99q = extract_product_attributes(normalize_text('huawei matepad pro 11 inch 128gb'), 'huawei')
    t99c = extract_product_attributes(normalize_text('huawei matepad pro 11 inch 128gb'), 'huawei')
    pass99, _ = tablet_variant_exact_match(t99q, t99c)
    if not pass99:
        failures.append('FAIL: MatePad Pro 11 128gb should match itself')

    # 100. iPad Pro vs iPad Air must NOT match (family mismatch)
    t100q = extract_product_attributes(normalize_text('apple ipad pro 11 inch 256gb'), 'apple')
    t100c = extract_product_attributes(normalize_text('apple ipad air 11 inch 256gb'), 'apple')
    pass100, r100 = tablet_variant_exact_match(t100q, t100c)
    if pass100:
        failures.append(f'FAIL: iPad Pro vs iPad Air should NOT match: {r100}')

    # 101. Year mismatch: iPad 2019 vs iPad 2022 must NOT match
    t101q = extract_product_attributes(normalize_text('apple ipad 10.2 inch 2019 128gb'), 'apple')
    t101c = extract_product_attributes(normalize_text('apple ipad 10.2 inch 2022 128gb'), 'apple')
    pass101, r101 = tablet_variant_exact_match(t101q, t101c)
    if pass101:
        failures.append(f'FAIL: iPad 2019 vs iPad 2022 should NOT match: {r101}')

    # 102. Chip mismatch: iPad Pro M1 vs iPad Pro M2 must NOT match
    t102q = extract_product_attributes(normalize_text('apple ipad pro 11 inch m1 256gb'), 'apple')
    t102c = extract_product_attributes(normalize_text('apple ipad pro 11 inch m2 256gb'), 'apple')
    pass102, r102 = tablet_variant_exact_match(t102q, t102c)
    if pass102:
        failures.append(f'FAIL: iPad Pro M1 vs M2 should NOT match: {r102}')

    # 103. tablet_family extraction: iPad Pro → "ipad pro"
    t103 = extract_product_attributes(normalize_text('apple ipad pro 12.9 inch 256gb'), 'apple')
    if t103.get('tablet_family') != 'ipad pro':
        failures.append(f'FAIL: tablet_family for iPad Pro should be "ipad pro", got "{t103.get("tablet_family")}"')

    # 104. tablet_family extraction: MatePad Pro → "matepad pro"
    t104 = extract_product_attributes(normalize_text('huawei matepad pro 11 128gb'), 'huawei')
    if t104.get('tablet_family') != 'matepad pro':
        failures.append(f'FAIL: tablet_family for MatePad Pro should be "matepad pro", got "{t104.get("tablet_family")}"')

    # 105. connectivity extraction: "wifi" detected
    t105 = extract_product_attributes('apple ipad mini 7th gen 256gb wifi', 'apple')
    if t105.get('connectivity') != 'wifi':
        failures.append(f'FAIL: connectivity for "...wifi" should be "wifi", got "{t105.get("connectivity")}"')

    # 106. connectivity extraction: "lte" → "cellular" (pass original, not normalized)
    t106 = extract_product_attributes('apple ipad pro 11 inch lte 256gb', 'apple')
    if t106.get('connectivity') != 'cellular':
        failures.append(f'FAIL: connectivity for "...lte" should be "cellular", got "{t106.get("connectivity")}"')

    # === LAPTOP HARDENING regression tests ===

    # 107. normalize_text: "14c/20g" should NOT convert 20g to 20gb (GPU cores, not storage)
    norm_gpu = normalize_text('macbook pro 14c/20g 16gb 512g')
    if '20gb' in norm_gpu:
        failures.append(f'FAIL: normalize_text should NOT convert 20g->20gb (GPU cores), got "{norm_gpu}"')
    if '512gb' not in norm_gpu:
        failures.append(f'FAIL: normalize_text should convert 512g->512gb, got "{norm_gpu}"')

    # 108. laptop_variant_exact_match: i5 11th gen vs i5 10th gen must NOT match
    l108q = extract_laptop_attributes('dell latitude core i5 11th gen 8gb 512gb', 'dell')
    l108c = extract_laptop_attributes('dell latitude core i5 10th gen 8gb 512gb', 'dell')
    pass108, r108 = laptop_variant_exact_match(l108q, l108c)
    if pass108:
        failures.append(f'FAIL: i5 11th gen should NOT match i5 10th gen: {r108}')

    # 109. laptop_variant_exact_match: 32gb vs 16gb must NOT match
    l109q = extract_laptop_attributes('hp elitebook i7 11th gen 32gb 512gb', 'hp')
    l109c = extract_laptop_attributes('hp elitebook i7 11th gen 16gb 512gb', 'hp')
    pass109, r109 = laptop_variant_exact_match(l109q, l109c)
    if pass109:
        failures.append(f'FAIL: 32gb should NOT match 16gb: {r109}')

    # 110. laptop_variant_exact_match: 512gb vs 256gb must NOT match
    l110q = extract_laptop_attributes('lenovo thinkpad i5 11th gen 16gb 512gb', 'lenovo')
    l110c = extract_laptop_attributes('lenovo thinkpad i5 11th gen 16gb 256gb', 'lenovo')
    pass110, r110 = laptop_variant_exact_match(l110q, l110c)
    if pass110:
        failures.append(f'FAIL: 512gb should NOT match 256gb: {r110}')

    # 111. laptop_variant_exact_match: exact match should pass
    l111q = extract_laptop_attributes('dell latitude core i5 11th gen 16gb 512gb', 'dell')
    l111c = extract_laptop_attributes('dell latitude core i5 11th gen 16gb 512gb', 'dell')
    pass111, _ = laptop_variant_exact_match(l111q, l111c)
    if not pass111:
        failures.append('FAIL: Exact laptop spec should match itself')

    # 112. laptop processor mismatch: i5 vs i7 must NOT match
    l112q = extract_laptop_attributes('acer aspire i5 11th gen 8gb 256gb', 'acer')
    l112c = extract_laptop_attributes('acer aspire i7 11th gen 8gb 256gb', 'acer')
    pass112, r112 = laptop_variant_exact_match(l112q, l112c)
    if pass112:
        failures.append(f'FAIL: i5 should NOT match i7: {r112}')

    # 113. laptop series mismatch: latitude vs inspiron must NOT match
    l113q = extract_laptop_attributes('dell latitude i5 11th gen 16gb 512gb', 'dell')
    l113c = extract_laptop_attributes('dell inspiron i5 11th gen 16gb 512gb', 'dell')
    pass113, r113 = laptop_variant_exact_match(l113q, l113c)
    if pass113:
        failures.append(f'FAIL: Latitude should NOT match Inspiron: {r113}')

    # 114. MacBook M1 vs M2 must NOT match (chip generation)
    l114q = extract_laptop_attributes('apple macbook pro m1 16gb 512gb', 'apple')
    l114c = extract_laptop_attributes('apple macbook pro m2 16gb 512gb', 'apple')
    pass114, r114 = laptop_variant_exact_match(l114q, l114c)
    if pass114:
        failures.append(f'FAIL: M1 should NOT match M2: {r114}')

    # 115. MacBook Air vs MacBook Pro must NOT match (product_line)
    l115q = extract_laptop_attributes('apple macbook air m1 8gb 256gb', 'apple')
    l115c = extract_laptop_attributes('apple macbook pro m1 8gb 256gb', 'apple')
    pass115, r115 = laptop_variant_exact_match(l115q, l115c)
    if pass115:
        failures.append(f'FAIL: MacBook Air should NOT match MacBook Pro: {r115}')

    # 116. match_laptop_by_attributes should reject gen mismatch
    # Build minimal test environment
    nl_names_116 = ['dell latitude core i5 gen10 16gb 512gb ssd']
    nl_lookup_116 = {nl_names_116[0]: ['NL-LAPTOP-GEN10']}
    laptop_result_116 = match_laptop_by_attributes(
        'dell latitude core i5 gen11 16gb 512gb ssd',
        'dell', 'Dell Latitude Core i5 11th Gen 16GB 512GB SSD',
        nl_names_116, nl_lookup_116, None)
    if laptop_result_116 is not None:
        failures.append('FAIL: match_laptop_by_attributes should reject 11th gen query vs 10th gen NL')

    # 117. match_laptop_by_attributes should accept exact gen match
    nl_names_117 = ['dell latitude core i5 gen11 16gb 512gb ssd']
    nl_lookup_117 = {nl_names_117[0]: ['NL-LAPTOP-GEN11']}
    laptop_result_117 = match_laptop_by_attributes(
        'dell latitude core i5 gen11 16gb 512gb ssd',
        'dell', 'Dell Latitude Core i5 11th Gen 16GB 512GB SSD',
        nl_names_117, nl_lookup_117, None)
    if laptop_result_117 is None:
        failures.append('FAIL: match_laptop_by_attributes should accept exact gen match')
    elif laptop_result_117.get('mapped_uae_assetid') != 'NL-LAPTOP-GEN11':
        failures.append(f'FAIL: Expected NL-LAPTOP-GEN11, got {laptop_result_117.get("mapped_uae_assetid")}')

    return failures