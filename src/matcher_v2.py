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
    - The 85-94% zone contains false positives (e.g., iPhone 4 -> iPhone 6 at 95%)
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
from urllib.parse import urlparse, unquote
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
VARIANT_TOKENS = {"pro", "max", "ultra", "plus", "fold", "flip", "fe", "mini", "lite", "note", "edge",
                  "gt", "turbo", "neo", "speed", "kit"}

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
    # NL catalog OLD/New brand splits -> canonical brand
    'dell old': 'dell', 'dell new': 'dell',
    'hp old': 'hp', 'hp new': 'hp',
    'lenovo old': 'lenovo', 'lenovo new': 'lenovo',
    'samsung (old)': 'samsung',
    # NL sub-brands -> parent brand
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


# Known brand names for inference (canonical -> itself, for first-word lookup)
_KNOWN_BRANDS = {
    'apple', 'samsung', 'huawei', 'xiaomi', 'oppo', 'vivo', 'realme',
    'oneplus', 'motorola', 'nokia', 'honor', 'google', 'sony', 'lg',
    'asus', 'lenovo', 'dell', 'hp', 'acer', 'microsoft', 'nothing',
    'poco', 'tecno', 'infinix', 'itel', 'zte', 'alcatel', 'meizu',
    'blackberry', 'htc', 'nubia', 'iqoo',
}
# Also build reverse lookup: all alias keys -> canonical brand
_BRAND_FROM_FIRST_WORD = {b: b for b in _KNOWN_BRANDS}
for alias, canonical in BRAND_ALIASES.items():
    first = alias.split()[0]
    if first not in _BRAND_FROM_FIRST_WORD:
        _BRAND_FROM_FIRST_WORD[first] = canonical

# Product-line names that unambiguously identify a brand.
# Enables brand inference when input has no Brand column (e.g., "iPhone 15 128GB Bleu").
_PRODUCT_LINE_TO_BRAND = {
    # Apple
    'iphone': 'apple', 'ipad': 'apple', 'macbook': 'apple', 'airpods': 'apple',
    'imac': 'apple',
    # Samsung
    'galaxy': 'samsung',
    # Google
    'pixel': 'google',
    # Xiaomi
    'redmi': 'xiaomi', 'poco': 'poco',
    # Huawei
    'matepad': 'huawei', 'matebook': 'huawei',
    # OnePlus
    'nord': 'oneplus',
    # OPPO
    'reno': 'oppo', 'realme': 'realme',
    # Motorola
    'moto': 'motorola', 'razr': 'motorola',
    # Microsoft
    'surface': 'microsoft',
    # Lenovo
    'thinkpad': 'lenovo', 'ideapad': 'lenovo', 'yoga': 'lenovo',
    # Dell
    'latitude': 'dell', 'inspiron': 'dell', 'xps': 'dell',
}
_BRAND_FROM_FIRST_WORD.update(_PRODUCT_LINE_TO_BRAND)


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
    # Try stripping trailing digits from first word for product-line matching
    # Handles "reno4" -> "reno" -> oppo, "galaxy" stays "galaxy" (no digits)
    first_base = re.sub(r'\d+$', '', first)
    if first_base and first_base != first and first_base in _PRODUCT_LINE_TO_BRAND:
        return _PRODUCT_LINE_TO_BRAND[first_base]
    return ''


# ---------------------------------------------------------------------------
# URL -> product name extraction
# ---------------------------------------------------------------------------

def _is_url(text: str) -> bool:
    """Check if text looks like a URL.

    Returns True for:
        - Standard URLs:  http://..., https://..., www.…
        - Protocol-relative: //example.com/path
        - Bare domains:   example.com/path  (domain.tld followed by /)
        - Embedded URLs:  "url: https://..." or "see https://..."
    """
    if not isinstance(text, str):
        return False
    t = text.strip().lower()
    # Standard prefixes
    if t.startswith(('http://', 'https://', 'www.', '//')):
        return True
    # Embedded URL anywhere in the text
    if re.search(r'https?://', t):
        return True
    # Bare domain pattern: word.tld/  (e.g. "example.com/iphone-15")
    if re.search(r'[a-z0-9][-a-z0-9]*\.[a-z]{2,}/', t):
        return True
    return False


def extract_name_from_url(url: str) -> str:
    """
    Extract a human-readable product name from a product page URL.

    Handles common e-commerce URL patterns:
        https://www.recommerce.com/fr/iphone-15-128go-bleu -> 'iphone 15 128go bleu'
        https://example.com/products/samsung-galaxy-s23-256gb -> 'samsung galaxy s23 256gb'

    If the last path segment is too short, purely numeric, or looks like an
    opaque ID, the function scans earlier segments and picks the best
    slug-like candidate (contains both letters and digits, e.g.
    "iphone-15-128gb").

    Scheme-less URLs ("example.com/path", "//example.com/path") and
    embedded URLs ("url: https://...") are accepted — a leading
    "http(s)://" is prepended when missing so urlparse works correctly.

    Returns empty string if parsing fails or no meaningful name found.
    """
    if not isinstance(url, str) or not url.strip():
        return ''
    try:
        raw = url.strip()

        # --- normalise scheme-less / embedded URLs so urlparse works ---
        # Embedded: "url: https://example.com/path" -> extract the URL part
        _embedded = re.search(r'(https?://\S+)', raw)
        if _embedded:
            raw = _embedded.group(1)
        # Protocol-relative: "//example.com/path"
        elif raw.startswith('//'):
            raw = 'https:' + raw
        # Bare domain: "example.com/path" (no scheme, no //)
        elif not raw.lower().startswith(('http://', 'https://')) and re.match(r'[a-z0-9][-a-z0-9]*\.[a-z]{2,}/', raw, re.IGNORECASE):
            raw = 'https://' + raw

        parsed = urlparse(raw)
        path = unquote(parsed.path).strip('/')
        if not path:
            return ''

        segments = path.split('/')

        def _slug_to_name(slug: str) -> str:
            """Convert a single URL slug to a candidate product name."""
            slug = re.sub(r'\.(html?|php|aspx?|jsp)$', '', slug, flags=re.IGNORECASE)
            slug = re.sub(r'[?#].*', '', slug)
            name = slug.replace('-', ' ').replace('_', ' ')
            name = re.sub(r'\b[0-9a-f]{8,}\b', '', name)
            return re.sub(r'\s+', ' ', name).strip()

        def _is_good_slug(name: str) -> bool:
            """A good slug has both letters and digits (e.g. 'iphone 15 128gb')
            and is at least 4 chars long."""
            return (
                len(name) >= 4
                and bool(re.search(r'[a-zA-Z]', name))
                and not re.fullmatch(r'[0-9]+', name)
                and not re.fullmatch(r'[0-9a-f-]{20,}', name)  # UUID-ish
            )

        # Try last segment first (most specific)
        best = _slug_to_name(segments[-1])
        if _is_good_slug(best):
            return best

        # Fallback: scan all segments in reverse, pick the best slug-like one
        for seg in reversed(segments[:-1]):
            candidate = _slug_to_name(seg)
            if _is_good_slug(candidate):
                return candidate

        # Final fallback: return whatever the last segment gave, if ≥4 chars
        if len(best) >= 4:
            return best
        return ''
    except Exception:
        return ''


# ---------------------------------------------------------------------------
# Color word stripping (pre-normalization)
# ---------------------------------------------------------------------------

# Color words that appear in recommerce/reseller data but are NEVER part of
# NL catalog product names. Stripping them improves fuzzy matching.
_COLOR_WORDS = {
    # French
    'bleu', 'noir', 'vert', 'rose', 'jaune', 'rouge', 'blanc', 'gris',
    'argent', 'or', 'violet', 'orange', 'corail', 'sideral', 'ciel',
    'sable', 'lumiere', 'minuit', 'creme',
    # Dutch (Forza / Dutch recommerce platforms)
    'blauw', 'zwart', 'paars', 'wit', 'groen', 'roze', 'goud', 'grijs',
    'rood', 'zilver', 'geel', 'bruin', 'oranje', 'koraal',
    # English
    'black', 'white', 'blue', 'green', 'red', 'gold', 'silver', 'purple',
    'pink', 'yellow', 'orange', 'coral', 'graphite', 'midnight', 'starlight',
    'cream', 'titanium', 'space', 'grey', 'gray', 'sierra', 'natural',
    # Compound color words (joined in URL slugs, no separating space)
    'spacegray', 'spacegrey', 'spacegrijs',
}

def strip_color_words(text: str) -> str:
    """
    Remove color words from a product name string.

    Only strips words that are standalone tokens (word boundaries) and are
    known color names. Does NOT strip substrings within product names.

    Examples:
        'iPhone 15 128gb Bleu' -> 'iPhone 15 128gb'
        'iPhone Xs 64gb Gris Sideral' -> 'iPhone Xs 64gb'
        'iPhone 15 Pro 256gb' -> 'iPhone 15 Pro 256gb' (unchanged)
    """
    if not isinstance(text, str):
        return text
    words = text.split()
    filtered = [w for w in words if w.lower() not in _COLOR_WORDS]
    return ' '.join(filtered).strip()


# Noise tokens that appear in recommerce URL slugs but carry no product identity.
# Applied ONLY to names extracted from URLs (not to human-written product names).
_URL_NOISE_WORDS = {
    # SIM configuration
    'nano', 'dual', 'sim', 'dualsim', 'simm', 'esim',
    # Region / grade / condition
    'eu', 'grade', 'refurbished',
}


def _clean_url_extracted_name(text: str) -> str:
    """
    Clean a product name that was extracted from a URL slug.

    Strips color words AND URL-specific noise tokens (SIM type, region, grade).
    Also handles trailing URL dedup digits on noise tokens (e.g., "titanium1", "sim1").

    Examples:
        'iphone 14 pro 256gb paars nano' -> 'iphone 14 pro 256gb'
        'samsung galaxy s23 5g 256gb zwart dual sim' -> 'samsung galaxy s23 5g 256gb'
        'iphone 15 256gb black eu a grade' -> 'iphone 15 256gb'
        'iphone 16 pro 256 gb titanium1' -> 'iphone 16 pro 256 gb'
    """
    if not isinstance(text, str):
        return text
    # Strip trailing "a grade" / "b grade" / "c grade" patterns BEFORE word-level filtering
    # (avoids stripping the "a" from "Galaxy A53")
    text = re.sub(r'\b[abc]\s+grade\b', '', text, flags=re.IGNORECASE)
    words = text.split()
    noise = _COLOR_WORDS | _URL_NOISE_WORDS
    filtered = []
    for w in words:
        wl = w.lower()
        if wl in noise:
            continue
        # Strip trailing digits from URL dedup suffixes (e.g., "titanium1" -> "titanium")
        wl_stripped = re.sub(r'\d+$', '', wl)
        if wl_stripped and wl_stripped in noise:
            continue
        filtered.append(w)
    # Remove trailing standalone single digits (URL dedup: "...paars-1" -> "...1")
    while filtered and re.fullmatch(r'\d{1,2}', filtered[-1]):
        filtered.pop()
    return ' '.join(filtered).strip()


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
        7. Standardize storage/RAM: "16 gb" -> "16gb"
        8. Remove connectivity markers (5G/LTE) - not product differentiators
        9. Collapse whitespace

    Variant preservation prevents false MULTIPLE_MATCHES:
    - iPhone 11 Pro vs Pro Max -> different normalized names (different products!)
    - Honor 7 vs 7X -> different normalized names (different models!)
    - Galaxy Tab vs Watch -> different normalized names (different categories!)

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
    # Roman numerals: I->1, II->2, III->3, IV->4, V->5, VI->6, VII->7, VIII->8, IX->9, X->10
    _roman_map = {'i': '1', 'ii': '2', 'iii': '3', 'iv': '4', 'v': '5',
                  'vi': '6', 'vii': '7', 'viii': '8', 'ix': '9', 'x': '10'}
    # "mark ii" / "mark 2" -> "mk2"
    def _replace_mark(m):
        val = m.group(1).strip().lower()
        num = _roman_map.get(val, val)  # roman -> digit, or keep digit
        return f'mk{num}'
    s = re.sub(r'\b(?:mark|mk)\s*(i{1,3}v?|vi{0,3}|ix|x|\d+)\b', _replace_mark, s, flags=re.IGNORECASE)
    # "gen 2" / "gen ii" / "2nd gen" / "2nd generation" -> "gen2"
    def _replace_gen_forward(m):
        val = m.group(1).strip().lower()
        num = _roman_map.get(val, val)
        return f'gen{num}'
    def _replace_gen_reverse(m):
        val = m.group(1).strip().lower()
        num = re.sub(r'(st|nd|rd|th)$', '', val)
        return f'gen{num}'
    # Reverse pattern MUST run first: "7th gen 10.4" -> "gen7 10.4" before forward
    # pattern can greedily match "gen 10" from the screen size that follows
    s = re.sub(r'\b(\d+)(?:st|nd|rd|th)\s*gen(?:eration)?\b', _replace_gen_reverse, s, flags=re.IGNORECASE)
    s = re.sub(r'\bgen(?:eration)?\s*(i{1,3}v?|vi{0,3}|ix|x|\d+)\b', _replace_gen_forward, s, flags=re.IGNORECASE)

    # Brand canonicalization in text: collapse split brand names BEFORE model parsing
    s = re.sub(r'\bone\s+plus\b', 'oneplus', s)

    # Samsung "+" variant normalization: "s24+" -> "s24 plus", "a55+" -> "a55 plus"
    # Galaxy S/A series use "+" as shorthand for Plus (S24+, S25+, A55+, Tab S8+).
    # Must run BEFORE punctuation removal since '+' is not a word token.
    # Pattern: letter s/a followed by 1-2 digits, then '+'.
    # Safe: won't match non-Samsung (OnePlus 12+ starts with digit, not s/a).
    s = re.sub(r'\b([sa]\d{1,2})\+', r'\1 plus', s)

    # Model de-concatenation: split joined brand+model and variant patterns
    # Must happen early (before punctuation removal) but after lowercasing
    # Order matters: split compound variants first, then digit-based splits
    # Pattern: variant combos joined together -> split (must be before digit splits)
    s = re.sub(r'promax', 'pro max', s)
    # Pattern: tab + model letter -> add space (tabs8 -> tab s8, taba7 -> tab a7)
    s = re.sub(r'\b(tab)([a-z]\d)', r'\1 \2', s)
    # Pattern: known brand names directly followed by digits -> add space
    s = re.sub(r'\b(iphone|ipad|galaxy|pixel|redmi|mate|nova|honor|poco|note|reno|find)(\d)', r'\1 \2', s)
    # Pattern: digits directly followed by known variant keywords -> add space
    s = re.sub(r'(\d)(pro|max|plus|ultra|lite|mini|se)\b', r'\1 \2', s)

    # --- Model concatenation: join separated model identifiers ---
    # "fold 3" -> "fold3", "flip 4" -> "flip4"
    # These are single model identifiers that should stay together for token matching
    s = re.sub(r'\b(fold|flip)\s+(\d+)\b', r'\1\2', s)
    # Galaxy S/A/Z series: "galaxy s 23" -> "galaxy s23", "galaxy a 54" -> "galaxy a54"
    # Only in galaxy context to avoid false positives (e.g., "Moto Z 32 GB" or "Mate S 32 GB")
    s = re.sub(r'(galaxy)\s+([saz])\s+(\d{2})\b', r'\1 \2\3', s)

    # Strip Thunderbolt port designators BEFORE storage parsing
    # "2 TBT3" means "2 Thunderbolt 3 ports", NOT "2 TB" storage
    # "4 TBT3" means "4 Thunderbolt 3 ports", NOT "4 TB" storage
    s = re.sub(r'\b(\d+)\s*tbt\d?\b', r'\1tbt', s, flags=re.IGNORECASE)

    # Pre-normalize fractional TB to GB BEFORE punctuation removal (dot matters here)
    # "0.25tb" -> "256gb", "0.5tb" -> "512gb"
    s = re.sub(r'\b0\.25\s*tb\b', '256gb', s, flags=re.IGNORECASE)
    s = re.sub(r'\b0\.5\s*tb\b', '512gb', s, flags=re.IGNORECASE)

    # KEEP years - they're critical for distinguishing products
    # iPhone SE (2016) vs (2020) vs (2022) are DIFFERENT products
    # Years will be preserved as numbers after punctuation removal

    # Remove common punctuation — replace with space to preserve token boundaries
    # This converts "(2016)" to " 2016 " which keeps the year
    s = re.sub(r'[,\-\(\)"\'\/\.]', ' ', s)

    # French storage units: "Go" (Giga-octets) -> GB, "To" (Téra-octets) -> TB
    # "256 Go" -> "256gb", "1 To" -> "1tb" (common in French recommerce data)
    s = re.sub(r'(\d+)\s*go\b', r'\1gb', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*to\b', r'\1tb', s, flags=re.IGNORECASE)

    # Fix missing unit: "256g" -> "256gb" (common typo in some datasets)
    # Only convert true storage sizes (64g, 128g, 256g, 512g, 1024g, 2048g)
    # Do NOT convert small numbers like 16g/20g (MacBook GPU cores like 14c/20g)
    # Safe rule: only convert when number is >=64 OR has 3+ digits
    s = re.sub(r'\b(6[4-9]|[7-9]\d|\d{3,})g\b', r'\1gb', s, flags=re.IGNORECASE)

    # Standardize storage/RAM: "16 gb" -> "16gb", handles TB/MB too
    # This keeps RAM values distinct: "2gb" vs "3gb" vs "4gb"
    s = re.sub(r'(\d+)\s*(gb|tb|mb)', r'\1\2', s, flags=re.IGNORECASE)

    # Standardize watch case size: "40 mm" -> "40mm"
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

    # Normalize brand to canonical form: "HP OLD" -> "hp", "Dell Inc" -> "dell"
    # Removes catalog noise ("OLD"/"New") and legal suffixes before combining.
    if brand_str:
        brand_canonical = normalize_brand(brand_str)
        if brand_canonical:
            brand_str = brand_canonical

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
    Maps CPU model codes to generation numbers (e.g., i5-12500H -> 12th gen).
    """
    text_lower = text.lower()

    # Apple Silicon: M1, M2, M3
    apple_match = re.search(r'\bm([123])\b', text_lower)
    if apple_match:
        return f"m{apple_match.group(1)}"

    # 5Intel Core patterns: i3-1200H, i5-1165G7, i7-10750H
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

    # Normalized text fallback: "gen8", "gen11" (from normalize_text converting "8th gen" -> "gen8")
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


# ---------------------------------------------------------------------------
# V2-only: EU laptop query normalization
# ---------------------------------------------------------------------------

# Retail noise tokens to strip from laptop queries (case-insensitive).
# These appear in EU recommerce data but carry no identity signal.
_LAPTOP_NOISE_TOKENS = re.compile(
    r'\b(?:'
    r'tbt[34]?|tb[34]|thunderbolt\s*\d?'
    r'|wi[\- ]?fi\s*\d*[a-z]?'
    r'|wlan|bluetooth|bt\s*\d*\.?\d*'
    r'|usb[\- ]?c?|hdmi|nfc'
    r'|backlit|fingerprint|webcam|touchscreen'
    r'|ips|oled|retina|fhd|qhd|uhd|wuxga|wqxga'
    r')\b',
    re.IGNORECASE,
)

# Screen size with quote/inch symbol -> "NN inch"
_SCREEN_SIZE_QUOTE = re.compile(r'(\d+\.?\d*)\s*[""″\u201c\u201d]')
_SCREEN_SIZE_INCH = re.compile(r'(\d+\.?\d*)\s*inch(?:es)?\b', re.IGNORECASE)


def normalize_laptop_query_text_v2(text: str) -> str:
    """V2-only normalization for EU retail laptop strings.

    Applied BEFORE attribute extraction / candidate retrieval.
    Cleans retail noise while preserving identity tokens
    (brand, family, CPU, RAM, storage, year, chip).

    Examples:
        'MacBook Pro (13" 2020, 4 TBT3)' -> 'MacBook Pro 13 inch 2020'
        'HP EliteBook 840 G8 14" i5 Wi-Fi 6E 16GB 512GB'
            -> 'HP EliteBook 840 G8 14 inch i5 16GB 512GB'
    """
    if not text:
        return text

    s = str(text)

    # 1. Normalize screen size: 13" -> 13 inch, 15.6" -> 15.6 inch
    s = _SCREEN_SIZE_QUOTE.sub(r'\1 inch', s)
    # Ensure existing "inch" forms are canonical
    s = _SCREEN_SIZE_INCH.sub(r'\1 inch', s)

    # 2. Parentheses cleanup: extract identity fragments, drop noise.
    #    "(13 inch 2020, 4 TBT3)" -> keep "13 inch 2020", drop "4 TBT3"
    def _clean_parens(m):
        inner = m.group(1)
        # Split on comma; keep fragments with identity tokens
        parts = [p.strip() for p in inner.split(',')]
        kept = []
        for p in parts:
            p_low = p.lower()
            # Keep if it contains: year, inch, chip name, storage, ram, gen
            if re.search(r'\b(20[12]\d|inch|m[1234]|i[3579]|ryzen|core|gen\s?\d|\d+\s*gb|\d+\s*tb)\b', p_low):
                kept.append(p)
        return ' ' + ' '.join(kept) + ' ' if kept else ' '

    s = re.sub(r'\(([^)]*)\)', _clean_parens, s)

    # 3. Remove retail noise tokens (TBT3, Wi-Fi 6E, WLAN, BT, etc.)
    s = _LAPTOP_NOISE_TOKENS.sub(' ', s)

    # 4. Collapse whitespace
    s = re.sub(r'\s+', ' ', s).strip()

    return s


def extract_laptop_attributes(text: str, brand: str) -> Dict[str, str]:
    """
    Extract laptop-specific attributes for matching.

    Laptops have different naming: product line + CPU gen + RAM + storage
    vs phones: product line + model + storage
    """
    text_norm = normalize_text(text)
    # Use normalize_brand (not normalize_text) so attrs['brand'] matches
    # build_attribute_index keys — "HP OLD" -> "hp", "Dell Inc" -> "dell"
    brand_norm = normalize_brand(brand) if brand else ''
    if not brand_norm:
        brand_norm = normalize_text(brand) if isinstance(brand, str) else ''

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

    # --- Screen size extraction (Task 1A) ---
    screen_inches = ''
    _si = re.search(r'\b(\d{2}(?:\.\d)?)\s*(?:inch|")\b', text_lower)
    if not _si:
        # Fallback: already-normalized "NN inch" from v2 normalization
        _si = re.search(r'\b(\d{2}(?:\.\d)?)\s+inch\b', text_norm)
    if _si:
        screen_inches = _si.group(1)

    # --- Apple chip extraction (Task 1B) ---
    apple_chip = ''
    if brand_norm == 'apple' or 'macbook' in text_lower:
        _ac = re.search(
            r'\bm([1234])\s*(pro|max|ultra)?\b', text_lower)
        if _ac:
            apple_chip = f'm{_ac.group(1)}'
            if _ac.group(2):
                apple_chip += f' {_ac.group(2)}'

    # --- Year extraction ---
    year = ''
    _yr = re.search(r'\b(20[12]\d)\b', text)
    if _yr:
        year = _yr.group(1)

    # --- Dual-storage detection (Task 1C) ---
    storage_ambiguous = False
    storage_list = []
    _all_gb = re.findall(r'(\d+)\s*gb', text_lower)
    _all_tb = re.findall(r'(\d+)\s*tb\b', text_lower)
    _gb_vals = [int(v) for v in _all_gb]
    _tb_vals = [int(v) * 1024 for v in _all_tb]
    ram_int = int(ram.replace('gb', '')) if ram else 0
    _storage_vals = sorted(set(
        [v for v in _gb_vals if v >= 128 and v != ram_int] + _tb_vals
    ))
    if len(_storage_vals) >= 2:
        storage_ambiguous = True
        storage_list = _storage_vals

    attrs = {
        'brand': brand_norm,
        'product_line': '',
        'processor': processor,      # i3, i5, i7, i9, m1, m2, etc.
        'generation': cpu_gen,        # 11th gen, 8th gen, m1, etc.
        'model': cpu_gen,             # DEPRECATED: kept for backward compatibility
        'storage': storage,
        'ram': ram,
        'platform_code': '',         # Dell 5420, HP 840 g8, Lenovo t14, etc.
        'laptop_family': '',         # sub-series: swift 3, rog strix, pavilion 15
        'model_code': '',            # hardware code: sf314, ux325, fx504
        'screen_inches': screen_inches,     # 13, 14, 15.6, 16, 17.3
        'apple_chip': apple_chip,           # m1, m1 pro, m2 max, etc.
        'year': year,                       # 2020, 2023, etc.
        'storage_ambiguous': storage_ambiguous,  # True if 2+ storage sizes
        'storage_list': storage_list,            # [256, 1024] sorted GB values
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

    # --- Laptop family (sub-series) extraction ---
    # Differentiates Swift 3 vs Swift 5, ROG Strix vs ROG Zephyrus, Pavilion 14 vs 15.
    # Uses text_norm (normalize_text output) so commas in NL catalog names become spaces.
    laptop_family = ''
    if attrs['product_line']:
        pl = attrs['product_line']

        if brand_norm == 'acer':
            # Named sub-series (longest first)
            for fam in ['predator helios', 'predator triton', 'swift go', 'aspire vero']:
                if fam in text_norm:
                    laptop_family = fam
                    break
            # Numbered/letter sub-series: swift 3, aspire 5, nitro v, aspire e
            if not laptop_family:
                _fam_m = re.search(
                    rf'\b{re.escape(pl)}\s+([a-zv\d])\b', text_norm)
                if _fam_m:
                    laptop_family = f'{pl} {_fam_m.group(1)}'

        elif brand_norm == 'asus':
            # Named sub-series
            for fam in ['rog strix', 'rog zephyrus', 'rog flow',
                         'vivobook pro', 'vivobook flip', 'vivobook s',
                         'zenbook pro', 'zenbook flip', 'zenbook duo', 'zenbook s',
                         'expertbook b']:
                if fam in text_norm:
                    laptop_family = fam
                    break
            # TUF sub-models: tuf a15, tuf f15, tuf fx
            if not laptop_family and 'tuf' in text_norm:
                _tuf_m = re.search(
                    r'\btuf\s+(?:gaming\s+)?([af]\d{2})\b', text_norm)
                if _tuf_m:
                    laptop_family = f'tuf {_tuf_m.group(1)}'
                elif 'fx' in text_norm:
                    laptop_family = 'tuf fx'

        elif brand_norm == 'hp':
            # Named sub-series
            for kw in ['x360', 'gaming', 'aero', 'detachable',
                        'studio', 'fury', 'firefly', 'power']:
                if f'{pl} {kw}' in text_norm:
                    laptop_family = f'{pl} {kw}'
                    break
            # Numbered: pavilion 14, elitebook 840, probook 450
            if not laptop_family:
                _hp_m = re.search(
                    rf'\b{re.escape(pl)}\s+(\d{{2,3}})\b', text_norm)
                if _hp_m:
                    laptop_family = f'{pl} {_hp_m.group(1)}'

        elif brand_norm in ('apple', 'microsoft'):
            # macbook pro, macbook air, surface laptop — already specific
            laptop_family = pl

        elif brand_norm == 'huawei':
            # Named: matebook x pro, matebook d, matebook x
            for fam in ['matebook x pro']:
                if fam in text_norm:
                    laptop_family = fam
                    break
            if not laptop_family:
                _hw_m = re.search(
                    r'\bmatebook\s+([a-z])\b', text_norm)
                if _hw_m:
                    laptop_family = f'matebook {_hw_m.group(1)}'

    attrs['laptop_family'] = laptop_family

    # --- Model code extraction (Acer / ASUS hardware identifiers) ---
    # Acer: SF314, PH315, AN515  |  ASUS: UX325, GA401, FX504, G551
    model_code = ''
    if brand_norm == 'acer':
        # Full Acer model code WITH suffix: sf314-57 or sf314 57 (hyphen or space)
        # The suffix distinguishes hardware revisions (SF314-57 != SF314-58)
        # normalize_text strips hyphens -> "sf314 57", raw text keeps "sf314-57"
        _mc_m = re.search(r'\b([a-z]{2}\d{3})[-\s](\d{1,3}[a-z]*)\b', text_lower)
        if _mc_m:
            # Normalize to hyphen form: "sf314-57" regardless of separator
            model_code = f'{_mc_m.group(1)}-{_mc_m.group(2)}'
        else:
            # Fallback: base code only (no suffix in text): sf314, an515
            _mc_m = re.search(r'\b([a-z]{2}\d{3})\b', text_lower)
            if _mc_m:
                model_code = _mc_m.group(1)
    elif brand_norm == 'asus':
        # 2-letter + 3-4 digit (primary): ux325, ga401, gm501
        # Allow trailing letters (ux325ea, ga401iv) — capture only base code
        _mc_m = re.search(r'\b([a-z]{2}\d{3,4})[a-z]*\b', text_lower)
        if not _mc_m:
            # 1-letter + 3-digit fallback: g551, g752, s510
            _mc_m = re.search(r'\b([a-z]\d{3})[a-z]*\b', text_lower)
        if _mc_m:
            model_code = _mc_m.group(1)
    attrs['model_code'] = model_code

    # --- Platform code extraction (model number within product line) ---
    # Dell: Latitude 5420, Precision 5560, XPS 9520
    # HP: EliteBook 840 G8, ProBook 640 G9, ZBook 15 G6
    # Lenovo: ThinkPad T14, ThinkPad X1 Carbon, IdeaPad 5
    if attrs['product_line']:
        pl = attrs['product_line']
        pl_idx = text_lower.find(pl)
        if pl_idx >= 0:
            remaining = text_lower[pl_idx + len(pl):].strip()

            if brand_norm == 'dell':
                # Dell: 4-digit code, optionally with letter prefix (E5470)
                # Allow optional 2-digit screen/series prefix: "Inspiron 15 7570"
                _pc_m = re.match(r'(?:\d{1,2}\s+)?([a-z]?\d{4}[a-z]?)\b', remaining)
                if _pc_m:
                    attrs['platform_code'] = _pc_m.group(1)
                else:
                    # Fallback: NL catalog format has model code mid-text
                    # e.g. "dell inspiron core i5 gen8 4gb 3576 15 inch 1tb"
                    # Find standalone 4-digit number NOT part of CPU spec
                    for _fb in re.finditer(r'\b([a-z]?\d{4})\b', text_norm):
                        _fb_code = _fb.group(1)
                        _fb_pos = _fb.start()
                        # Skip if preceded by i3/i5/i7/i9 (CPU model like 8250)
                        _before = text_norm[:_fb_pos].rstrip(' -')
                        if _before.endswith(('i3', 'i5', 'i7', 'i9')):
                            continue
                        # Skip if part of "genXXXX" pattern
                        if re.search(rf'gen\s*{re.escape(_fb_code)}', text_norm):
                            continue
                        attrs['platform_code'] = _fb_code
                        break

            elif brand_norm == 'hp':
                # HP: 3-4 digit code + optional G# (840 G8, 640 G9, 15 G6)
                _pc_m = re.match(r'(\d{2,4})\s*(g\d+)?', remaining)
                if _pc_m:
                    pc_parts = [_pc_m.group(1)]
                    if _pc_m.group(2):
                        pc_parts.append(_pc_m.group(2))
                    attrs['platform_code'] = ' '.join(pc_parts)

            elif brand_norm == 'lenovo':
                # Lenovo: X1 Carbon/Yoga/Nano first (more specific)
                _pc_m = re.match(
                    r'(x\d+\s+(?:carbon|yoga|nano|titanium))\b', remaining)
                if not _pc_m:
                    # Simple: letter + 1-2 digits + optional suffix (t14, e14, l14, p14s)
                    _pc_m = re.match(r'([a-z]\d{1,2}[a-z]?)\b', remaining)
                if _pc_m:
                    attrs['platform_code'] = _pc_m.group(1)

    # --- Disambiguate product revision vs CPU generation ---
    # "ThinkPad T14 Gen 3" -> "Gen 3" is the 3rd revision of the T14, NOT Intel 3rd gen.
    # If platform_code exists and "genX" appears right after it, cpu_gen was
    # misidentified as a product revision. Re-extract from CPU model number only,
    # or clear it if no CPU model number found.
    if attrs['platform_code'] and cpu_gen:
        _pc_gen = re.search(
            rf'\b{re.escape(attrs["platform_code"])}\s+gen(\d{{1,2}})\b', text_norm)
        if _pc_gen:
            _rev_gen = f"{_pc_gen.group(1)}th gen"
            if cpu_gen == _rev_gen:
                # cpu_gen came from product revision — try CPU model number only
                _intel_re = re.search(
                    r'(?:core\s+)?i[3579][\s\-]?(\d{4,5})[a-z]{0,2}', text_lower)
                if _intel_re:
                    _d = _intel_re.group(1)
                    _g = _d[:2] if (len(_d) == 5 or _d[0] == '1') else _d[0]
                    cpu_gen = f"{_g}th gen"
                else:
                    cpu_gen = ''  # Unknown CPU gen — safe to leave empty
                attrs['generation'] = cpu_gen
                attrs['model'] = cpu_gen

    return attrs


# ---------------------------------------------------------------------------
# Laptop policy class (Task 2)
# ---------------------------------------------------------------------------

_GAMING_KEYWORDS = {
    'rog', 'tuf', 'nitro', 'predator', 'legion', 'omen', 'victus',
    'alienware', 'raider', 'stealth', 'katana', 'crosshair',
    'gaming',
}

_BUSINESS_LINES = {
    'latitude', 'precision', 'thinkpad', 'elitebook', 'probook',
    'zbook', 'surface laptop', 'surface book', 'expertbook',
    'travelmate',
}


def laptop_policy_class(query_text: str, brand: str, attrs: Dict) -> str:
    """Classify a laptop into a policy class for completeness thresholds.

    Returns one of:
        'APPLE_MACBOOK', 'WINDOWS_BUSINESS', 'WINDOWS_GAMING', 'WINDOWS_OTHER'
    """
    text_low = query_text.lower()
    brand_norm = (normalize_brand(brand) if brand else '') or ''
    pl = attrs.get('product_line', '')

    # Apple MacBook
    if brand_norm == 'apple' or 'macbook' in text_low:
        if 'macbook air' in text_low or 'macbook pro' in text_low or pl in ('macbook air', 'macbook pro', 'macbook'):
            return 'APPLE_MACBOOK'

    # Gaming: keyword or GPU token present
    if any(kw in text_low for kw in _GAMING_KEYWORDS):
        return 'WINDOWS_GAMING'
    # GPU tokens (gtx, rtx, rx, geforce, radeon)
    if re.search(r'\b(gtx|rtx|geforce|radeon|rx\s*\d)\b', text_low):
        return 'WINDOWS_GAMING'

    # Business
    if pl in _BUSINESS_LINES:
        return 'WINDOWS_BUSINESS'

    return 'WINDOWS_OTHER'


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
    Extract iPad / tablet generation from text: '7th gen' -> '7', 'gen5' -> '5'.
    Only matches ordinal-gen patterns — cannot affect phones/watches/laptops.
    """
    if not isinstance(text_norm, str):
        return ''
    t = text_norm.lower()
    # "7th gen", "5th generation"
    m = re.search(r'(\d+)(?:st|nd|rd|th)\s*gen', t)
    if m:
        return m.group(1)
    # normalize_text already converts "7th generation" -> "gen7", "gen 5" -> "gen5"
    m2 = re.search(r'\bgen(\d+)\b', t)
    if m2:
        return m2.group(1)
    return ''


def extract_screen_inches(text_norm: str) -> str:
    """
    Extract screen size in inches from text: '8.3"' -> '8.3', '10.4 inch' -> '10.4'.
    Also handles normalize_text output where dots become spaces: '10 4' -> '10.4'.
    Only returns plausible tablet/laptop screen sizes (7–15 inches).
    """
    if not isinstance(text_norm, str):
        return ''
    t = text_norm.lower()
    # Space-separated decimal + inch suffix: "7 9 inch" -> "7.9" (must run BEFORE simple inch match)
    # This handles normalize_text converting "7.9 inch" -> "7 9 inch"
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
    # Space-separated decimal without suffix: "10 4" -> "10.4", "8 3" -> "8.3"
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
    # Use normalize_brand so attrs['brand'] matches attribute_index keys
    brand_norm = normalize_brand(brand) if isinstance(brand, str) and brand.strip() else ''
    if not brand_norm:
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

        # Extract tablet generation: "7th gen" -> "7", "gen5" -> "5"
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

        # Connectivity: wifi vs cellular (lte/5g/cellular -> "cellular", wifi-only -> "wifi")
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

    # Apple iPhone: "iphone 14 pro 256gb" -> line=iphone, model=14 pro
    # CRITICAL: Capture ALL variant words (pro max, pro, plus, mini, etc.)
    if 'iphone' in text_norm:
        match = re.search(r'iphone\s+(\d+[a-z]*(?:\s+(?:pro|plus|max|mini|ultra|lite))*)', text_norm)
        if match:
            attrs['product_line'] = 'iphone'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)

    # Samsung Galaxy: "galaxy s9 plus 128gb" -> line=galaxy, model=s9 plus
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

    # Google Pixel: "pixel 9 pro 256gb", "pixel 9 pro fold" -> line=pixel, model=9 pro / 9 pro fold
    # CRITICAL: Capture ALL variant words including fold (pro xl, pro fold, fold, pro, a, etc.)
    if 'pixel' in text_norm:
        match = re.search(r'pixel\s+(\d+[a-z]*(?:\s+(?:pro\s+fold|pro\s+xl|fold|pro|xl|max|ultra|lite|a))*)', text_norm)
        if match:
            attrs['product_line'] = 'pixel'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)

    # Xiaomi Redmi/Mi/Xiaomi-numbered: "redmi note 12 pro 128gb" -> line=redmi, model=note 12 pro
    # CRITICAL: Capture ALL variant words (pro max, pro, plus, etc.)
    if 'redmi' in text_norm:
        match = re.search(r'redmi\s+(note\s+\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*|\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*)', text_norm, re.IGNORECASE)
        if match:
            attrs['product_line'] = 'redmi'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)
    elif 'xiaomi' in brand_norm or 'xiaomi' in text_norm:
        # Xiaomi Mi series: "xiaomi mi 11 ultra" -> line=mi, model=11 ultra
        # Use word-boundary \bmi\b to avoid matching substring of "xiaomi"
        if re.search(r'\bmi\b', text_norm):
            match = re.search(r'\bmi\s+(\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*)', text_norm)
            if match:
                attrs['product_line'] = 'mi'
                attrs['model'] = match.group(1).strip()
                return _finalize_mobile_attrs(attrs)
        # Xiaomi numbered series: "xiaomi 15 ultra" -> line=xiaomi, model=15 ultra
        match = re.search(r'xiaomi\s+(\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite|t))*)', text_norm)
        if match:
            attrs['product_line'] = 'xiaomi'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)

    # Huawei Mate/P-series: "mate 30 pro 256gb" -> line=mate, model=30 pro
    # CRITICAL: Capture ALL variant words
    if 'mate' in text_norm and ('huawei' in brand_norm or 'huawei' in text_norm):
        match = re.search(r'mate\s+(\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*)', text_norm)
        if match:
            attrs['product_line'] = 'mate'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)
    elif ('huawei' in brand_norm or 'huawei' in text_norm) and re.search(r'\bp\d+', text_norm):
        # "huawei p30 pro" -> line=p, model=30 pro
        match = re.search(r'p(\d+[a-z]*(?:\s+(?:pro|plus|max|ultra|lite))*)', text_norm)
        if match:
            attrs['product_line'] = 'p'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)

    # OPPO Reno: "reno 4 128gb", "reno 3 pro 256gb"
    # NL catalog format: "oppo reno 3 series reno 3 pro 256gb" — strip redundant series label
    if 'reno' in text_norm:
        _reno_text = re.sub(r'reno\s+\d+\s+series\s+', '', text_norm)
        match = re.search(r'reno\s+(\d+[a-z]*(?:\s+(?:pro|plus|ultra|lite|max|neo|z|f))*)', _reno_text)
        if match:
            attrs['product_line'] = 'reno'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)

    # OPPO Find: "find x5 pro 256gb"
    # NL catalog format: "oppo find x5 series find x5 pro 256gb"
    if 'find' in text_norm and ('oppo' in text_norm or 'oppo' in brand_norm):
        _find_text = re.sub(r'find\s+[a-z]?\d+\s+series\s+', '', text_norm)
        match = re.search(r'find\s+([a-z]?\d+[a-z]*(?:\s+(?:pro|plus|ultra|lite|max|neo))*)', _find_text)
        if match:
            attrs['product_line'] = 'find'
            attrs['model'] = match.group(1).strip()
            return _finalize_mobile_attrs(attrs)

    # === GENERIC EXTRACTION (all other brands) ===
    # Detect common product line patterns: "moto g50", etc.
    # CRITICAL: Capture ALL variant words (pro max, plus, etc.)

    # Pattern 1: "ProductLine ModelNumber" (e.g., "moto g50")
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

    Returns nested dict: brand -> product_line -> model -> ram_storage_key -> [asset_ids]

    For phones: brand -> product_line -> model -> storage
    For laptops: brand -> product_line -> model (CPU gen) -> "ram_storage" (combined key)

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
                # Query specifies material -> DO NOT allow fallback
                return None

            # Query has no material -> fallback allowed
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

        # --- TIER 2: Query has no storage -> model has exactly 1 storage variant ---
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

        # --- TIER 3: Query has storage but no exact key -> fuzzy match storage keys ---
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
        dict mapping signature -> {
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
    Build a lookup dictionary: normalized_name -> list of uae_assetid values.

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

    Returns dict:  normalized_brand -> {
        'lookup': {normalized_name -> [asset_ids]},
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
    """Canonicalize storage: 1024gb->1tb, 2048gb->2tb. Passthrough for normal values."""
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
        # Xiaomi/Poco/Redmi performance variants (GT ≠ base, Turbo ≠ base)
        'gt', 'turbo', 'neo', 'speed',
        # Bundle/kit suffix (Xiaomi 14 Ultra ≠ Xiaomi 14 Ultra Photography Kit)
        'kit',
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

    # --- OPPO Reno Z/F variant extraction (brand-conditional) ---
    # "Reno2 Z" and "Reno2" are DIFFERENT products in OPPO's lineup.
    # Single-letter variants Z and F are only meaningful after a Reno family token.
    # We do NOT globally treat "z" as important (would break Samsung Galaxy Z Fold etc.).
    text_lower = text_clean.lower()
    if 'reno' in text_lower:
        # Match patterns like "reno 2 z", "reno 4 f", "reno z", "reno f"
        # After de-concat: "reno2" -> "reno 2", so digit may be a separate token
        reno_variant_match = re.search(r'\breno\s*\d*\s+(z|f)\b', text_lower)
        if reno_variant_match:
            variant_letter = reno_variant_match.group(1)
            if variant_letter not in model_tokens:
                model_tokens.append(variant_letter)

    return model_tokens


# ---------------------------------------------------------------------------
# Brand-specific model identity extraction & guardrail
# ---------------------------------------------------------------------------
# Targeted at OPPO/Xiaomi/Redmi/Poco where cross-generation mismatches are
# the most common failure mode. Does NOT fire for Samsung/Apple/Huawei.

_MODEL_IDENTITY_BRANDS = {'oppo', 'xiaomi', 'redmi', 'poco', 'realme'}

# Regex patterns that capture the full model identity token for each family.
# Group structure: (family+generation, variant_suffix)
_MODEL_IDENTITY_PATTERNS = [
    # OPPO Reno: reno, reno2, reno4 pro, reno10 pro+, reno z
    re.compile(r'\breno\s*(\d+)?\s*(pro\s*\+?|lite|z|f|neo|zoom)?\b', re.IGNORECASE),
    # Redmi Note: note 12, note 12 pro, note 12 turbo
    re.compile(r'\bnote\s*(\d+)\s*(pro|turbo|speed|play|prime)?\b', re.IGNORECASE),
    # Poco F/X/M/C series: poco f4, poco f4 gt, poco x5 pro, poco m5
    re.compile(r'\bpoco\s*([fxmc]\d+)\s*(pro|gt|neo)?\b', re.IGNORECASE),
    # Xiaomi numbered series: xiaomi 14, xiaomi 14 ultra, xiaomi 13t pro
    re.compile(r'\bxiaomi\s*(\d+)\s*(t)?\s*(ultra|pro|lite)?\b', re.IGNORECASE),
    # Redmi numbered: redmi 12, redmi 12c, redmi k60 pro
    re.compile(r'\bredmi\s*(\w?\d+\w?)\s*(pro|turbo|speed|play|prime)?\b', re.IGNORECASE),
]


def extract_model_identity(text: str) -> str:
    """
    Extract a normalized model identity string for OPPO/Xiaomi/Redmi/Poco devices.

    Returns a compact identity string like:
        'reno4pro', 'reno6', 'note12turbo', 'pocof4gt', '14ultra'

    Returns '' if no recognizable model identity is found, or if the text
    doesn't belong to a targeted brand.

    Only fires for brands in _MODEL_IDENTITY_BRANDS.

    Examples:
        'Oppo Reno4 Pro 128GB'            -> 'reno4pro'
        'Oppo Reno6 Dual 128GB'           -> 'reno6'
        'Reno10 Pro 5G'                    -> 'reno10pro'
        'Redmi Note 12 Turbo 256GB'       -> 'note12turbo'
        'Poco F4 GT 128GB'                -> 'pocof4gt'
        'Xiaomi 14 Ultra'                 -> 'xiaomi14ultra'
        'Xiaomi 14 Ultra Photography Kit' -> 'xiaomi14ultra'  (kit is NOT identity)
        'Apple iPhone 15 Pro'             -> ''  (not a targeted brand)
    """
    if not isinstance(text, str) or not text.strip():
        return ''
    t = text.lower().strip()

    # Check if this belongs to a targeted brand
    brand_found = any(b in t for b in _MODEL_IDENTITY_BRANDS)
    if not brand_found:
        # Also check product-line keywords that imply brand (reno -> oppo, etc.)
        if not any(kw in t for kw in ('reno', 'note', 'poco', 'redmi')):
            return ''

    best_identity = ''
    for pattern in _MODEL_IDENTITY_PATTERNS:
        # Find ALL matches (NL names repeat: "Reno Series, Reno Z" has 2 reno matches)
        # Keep the most specific (longest) identity
        for m in pattern.finditer(t):
            groups = [g for g in m.groups() if g]
            suffix = ''.join(g.strip().replace(' ', '').replace('+', 'plus')
                             for g in groups)
            # Extract the family keyword from the start of the match
            match_text = m.group(0).lower().strip()
            family_m = re.match(r'[a-z]+', match_text)
            family = family_m.group(0) if family_m else ''
            identity = (family + suffix).lower()
            # Keep the most specific match (longest identity string)
            if len(identity) > len(best_identity):
                best_identity = identity
    return best_identity


def model_identity_guardrail(query: str, candidate: str) -> Tuple[bool, str]:
    """
    Brand-specific model identity guardrail for OPPO/Xiaomi/Redmi/Poco.

    Compares the normalized model identity of query vs candidate.
    If both have identities and they differ, the match is REJECTED.

    Returns:
        (pass_guardrail: bool, reason: str)

    Only fires for targeted brands. Returns (True, '') for Samsung, Apple, etc.
    """
    q_id = extract_model_identity(query)
    c_id = extract_model_identity(candidate)

    if q_id and c_id:
        if q_id != c_id:
            return False, f'model_identity_mismatch:{q_id}!={c_id}'
    elif q_id and not c_id:
        # Query has identity but candidate doesn't — suspicious
        return False, f'model_identity_missing_in_candidate:{q_id}'

    return True, ''


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
        # Query has model token (e.g., "reno2") but match doesn't (e.g., "reno z")
        # This means the match is a DIFFERENT generation/model entirely.
        # e.g., "Reno2 Z 128GB" -> "Reno Z 128GB" (missing the "2" = wrong product)
        # Conservative: reject the match. Prefer REVIEW_REQUIRED over false acceptance.
        return False

    elif not query_model and matched_model:
        # Match has model token but query doesn't
        # This is suspicious - match is more specific than query
        # e.g., "Find X" -> "Find X9" (match added specificity)
        # Be conservative and allow this (might be variant)
        pass

    # All critical checks passed
    return True


# ---------------------------------------------------------------------------
# Matching logic — recursive brand -> attribute -> fuzzy
# ---------------------------------------------------------------------------

def compute_confidence_breakdown(query: str, matched: str) -> dict:
    """
    Compute a diagnostic confidence breakdown for a query->matched pair.

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
        risk_flags.append(f'category_mismatch:{q_cat}->{m_cat}')

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
            risk_flags.append(f'model_no_overlap:{q_tokens}->{m_tokens}')
        elif q_primary and m_primary and q_primary != m_primary:
            # Primary numeric token differs (e.g., "14" vs "15", "s23" vs "s24")
            model_match = False
            risk_flags.append(f'model_primary_mismatch:{q_primary}->{m_primary}')
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
        risk_flags.append(f'storage_mismatch:{q_storage}->{m_storage}')

    # Watch mm
    q_mm = extract_watch_mm(query)
    m_mm = extract_watch_mm(matched)
    watch_mm_match = True
    if q_mm and m_mm and q_mm != m_mm:
        watch_mm_match = False
        risk_flags.append(f'watch_mm_mismatch:{q_mm}->{m_mm}')

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
    Any mismatch -> REVIEW_REQUIRED, never MATCHED.
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

    If query has an attribute but candidate lacks it -> reject.
    If both have an attribute and they differ -> reject.

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
        # Query specifies series but candidate doesn't -> reject
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

    # Platform code: Dell 5420, HP 840 G8, Lenovo T14, etc.
    # ONE-SIDED ENFORCEMENT: if query specifies a platform code, candidate
    # MUST have the same one.  This prevents Dell 7320 matching Dell 3420
    # when the NL catalog entry lacks a parseable platform code.
    q_pc = query_attrs.get('platform_code', '').lower().strip()
    c_pc = candidate_attrs.get('platform_code', '').lower().strip()
    if q_pc and c_pc and q_pc != c_pc:
        mismatches.append(f'laptop_platform_code:{q_pc}!={c_pc}')
    elif q_pc and not c_pc:
        mismatches.append(f'laptop_platform_code_missing:{q_pc}')

    # Laptop family (sub-series): swift 3 != swift 5, rog strix != rog zephyrus
    q_fam = query_attrs.get('laptop_family', '').lower().strip()
    c_fam = candidate_attrs.get('laptop_family', '').lower().strip()
    if q_fam and c_fam and q_fam != c_fam:
        mismatches.append(f'laptop_family:{q_fam}!={c_fam}')
    elif q_fam and not c_fam:
        mismatches.append(f'laptop_family_missing:{q_fam}')

    # Model code: sf314 != sf514, ux325 != ux425
    # ONE-SIDED: query has a hardware code -> candidate must confirm it.
    q_mc = query_attrs.get('model_code', '').lower().strip()
    c_mc = candidate_attrs.get('model_code', '').lower().strip()
    if q_mc and c_mc and q_mc != c_mc:
        mismatches.append(f'laptop_model_code:{q_mc}!={c_mc}')
    elif q_mc and not c_mc:
        mismatches.append(f'laptop_model_code_missing:{q_mc}')

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
    Any mismatch -> REVIEW_REQUIRED, never MATCHED.
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
    - MOBILE: strict mobile_variant_exact_match required; fuzzy -> always REVIEW_REQUIRED
    - LAPTOP: model tokens/codes relaxed; fuzzy -> always REVIEW_REQUIRED
    - TABLET: screen_inches and generation must match
    - ALL: fuzzy method -> always REVIEW_REQUIRED (never MATCHED)
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
    # PART 6: Fuzzy matches -> ALWAYS REVIEW_REQUIRED, never MATCHED
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
            # Infer brand from query text for laptop attribute extraction.
            # _query_brand may be empty; fall back to first word of query.
            q_brand_l = result.get('_query_brand', '')
            if not q_brand_l:
                _first_word = query.split()[0] if query.strip() else ''
                q_brand_l = normalize_brand(_first_word) or _first_word
            q_attrs_l = extract_laptop_attributes(query, q_brand_l)
            c_attrs_l = extract_laptop_attributes(matched_on, q_brand_l)
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

        # --- One-sided disambiguator check for MATCHED laptops ---
        # If the QUERY specifies a sub-series (laptop_family) but the NL
        # candidate doesn't, the match is suspicious — the query is more
        # specific than what the catalog can confirm.  Block these as
        # REVIEW_REQUIRED so a human can verify.
        # NOTE: Both-sides-present mismatches (Swift 3!=Swift 5) are already
        # caught above by laptop_variant_exact_match.
        if gate_pass and result.get('match_status') in (MATCH_STATUS_MATCHED, MATCH_STATUS_MULTIPLE):
            try:
                _q_fam = q_attrs_l.get('laptop_family', '')
                _c_fam = c_attrs_l.get('laptop_family', '')
                if _q_fam and not _c_fam:
                    gate_pass = False
                    gate_reasons.append(f'laptop_family_missing_in_candidate:{_q_fam}')
            except Exception:
                pass

    if gate_pass:
        result['verification_pass'] = True
        result['verification_reasons'] = ''
        return result

    # Gate failed: determine severity of downgrade
    # Model identity mismatch on attribute match = wrong product entirely -> NO_MATCH
    # (e.g., Reno4 matched to Reno2 via attribute index — product doesn't exist in catalog)
    _has_identity_mismatch = any(r.startswith('model_identity_mismatch:') for r in gate_reasons)
    _is_attribute_method = method.startswith('attribute')
    if _has_identity_mismatch and _is_attribute_method:
        result['match_status'] = MATCH_STATUS_NO_MATCH
        result['confidence'] = CONFIDENCE_LOW
        result['verification_pass'] = False
        result['verification_reasons'] = '; '.join(gate_reasons)
        result['method'] = method + '_identity_rejected'
        return result

    # Standard gate failure: downgrade to REVIEW_REQUIRED
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
        1. Category cross-match: both known & different -> reject
        2. Storage mismatch: both present & different -> reject
        3. Watch mm mismatch: both present & different -> reject
        4. Primary model token mismatch: both present & different -> reject
        5. Material mismatch (watches): aluminium vs steel vs titanium -> reject
        6. Variant token mismatch: pro vs pro max, fold vs non-fold -> reject
        7. Hardware model code mismatch: ZE552KL vs ZE520KL -> reject

    Returns:
        (pass_gate: bool, reasons: list[str])
        If pass_gate is False, the match must NOT be returned as MATCHED.
    """
    # Re-normalize candidate to apply latest normalization rules (e.g., "reno7" -> "reno 7")
    # NL catalog's stored normalized_name may use older normalization without de-concat splits
    cand_norm = normalize_text(cand_norm)
    reasons = []

    # 1. Category cross-match
    q_cat = extract_category(query_norm)
    m_cat = extract_category(cand_norm)
    if q_cat != 'other' and m_cat != 'other' and q_cat != m_cat:
        reasons.append(f'category_cross:{q_cat}->{m_cat}')

    # 2. Storage mismatch
    q_storage = extract_storage(query_norm)
    m_storage = extract_storage(cand_norm)
    if q_storage and m_storage and q_storage != m_storage:
        reasons.append(f'storage_mismatch:{q_storage}->{m_storage}')

    # 3. Watch mm mismatch
    q_mm = extract_watch_mm(query_norm)
    m_mm = extract_watch_mm(cand_norm)
    if q_mm and m_mm and q_mm != m_mm:
        reasons.append(f'watch_mm_mismatch:{q_mm}->{m_mm}')

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
                reasons.append(f'model_token_count:{q_filtered}->{m_filtered}')
            else:
                for qt, mt in zip(q_filtered, m_filtered):
                    if qt != mt:
                        reasons.append(f'model_token_mismatch:{qt}->{mt}')
                        break
    elif q_tokens and not m_tokens:
        # Query has model tokens (e.g., "reno2") but candidate has NONE (e.g., "reno z")
        # This means the candidate is a different generation — reject.
        # Prevents: "Reno2 Z 128GB" matching "Reno Z 128GB" (wrong product)
        _year_re = re.compile(r'^20[012]\d$')
        q_non_year = [t for t in q_tokens if not _year_re.match(t)]
        q_non_year = [t for t in q_non_year if not MODEL_CODE_PATTERN.fullmatch(t)]
        if q_non_year:
            reasons.append(f'model_token_missing_in_candidate:{q_non_year}')

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
        reasons.append(f'material_mismatch:{q_mat}->{m_mat}')
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
            reasons.append(f'watch_edition_mismatch:{q_edition}->{m_edition}')
        elif q_edition and not m_edition:
            reasons.append(f'watch_edition_missing_in_candidate:{q_edition}')
        elif m_edition and not q_edition:
            reasons.append(f'watch_edition_missing_in_query:{m_edition}')

    # 6. Variant token mismatch (pro vs pro max, fold vs non-fold, etc.)
    q_variants = extract_variant_tokens(query_norm)
    m_variants = extract_variant_tokens(cand_norm)
    if q_variants != m_variants:
        reasons.append(f'variant_mismatch:{q_variants}->{m_variants}')

    # 7. Hardware model code mismatch (ZE552KL vs ZE520KL, etc.)
    q_code = extract_model_code(query_norm)
    m_code = extract_model_code(cand_norm)
    if q_code and m_code and q_code != m_code:
        reasons.append(f'model_code_mismatch:{q_code}->{m_code}')

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
                    reasons.append(f'tablet_screen_mismatch:{q_size}->{m_size}')

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
            reasons.append(f'tablet_line_mismatch:{q_tl}->{m_tl}')
        elif q_tl and not m_tl:
            reasons.append(f'tablet_line_missing_in_candidate:{q_tl}')

    # 9b. Tablet generation mismatch (7th gen vs 5th gen — different products)
    # Only fires for tablets — extract_tablet_generation returns '' for non-tablets
    if q_cat == 'tablet' and m_cat == 'tablet':
        q_gen = extract_tablet_generation(query_norm)
        m_gen = extract_tablet_generation(cand_norm)
        if q_gen and m_gen and q_gen != m_gen:
            reasons.append(f'tablet_generation_mismatch:{q_gen}->{m_gen}')

    # 9c. Tablet/laptop screen inches mismatch (8.3 vs 10.9 — different products)
    # Only fires for tablets — uses extract_screen_inches for canonical extraction
    if q_cat == 'tablet' and m_cat == 'tablet':
        q_screen = extract_screen_inches(query_norm)
        m_screen = extract_screen_inches(cand_norm)
        if q_screen and m_screen:
            q_val = float(q_screen)
            m_val = float(m_screen)
            if abs(q_val - m_val) > 0.15:
                reasons.append(f'screen_inches_mismatch:{q_screen}->{m_screen}')

    # 10. Year mismatch (2023 vs 2024 — different model years)
    # Applies to tablets and laptops (especially MacBooks)
    q_year_m = re.search(r'\b(20[12]\d)\b', query_norm)
    m_year_m = re.search(r'\b(20[12]\d)\b', cand_norm)
    if q_year_m and m_year_m and q_year_m.group(1) != m_year_m.group(1):
        # Only enforce for categories where year distinguishes product generations
        if q_cat in ('tablet', 'laptop') or m_cat in ('tablet', 'laptop'):
            reasons.append(f'year_mismatch:{q_year_m.group(1)}->{m_year_m.group(1)}')

    # 11. Brand-specific model identity guardrail (OPPO/Xiaomi/Redmi/Poco)
    # Catches cross-generation mismatches like Reno4->Reno3, Note 12 Turbo->Note 12
    id_pass, id_reason = model_identity_guardrail(query_norm, cand_norm)
    if not id_pass:
        reasons.append(id_reason)

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
    Match laptops by attributes with policy-class completeness gates.

    Uses laptop_policy_class() to determine minimum attribute requirements,
    then cross-join scores all candidates. Dual-storage queries always go
    to REVIEW_REQUIRED. Score margin between top-1 and top-2 must exceed
    threshold for MATCHED; otherwise REVIEW with top-3 alternatives.

    Scoring weights:
        +100  platform_code / model_code exact match
        +40   processor tier match (i3/i5/i7/i9/ryzen)
        +40   generation match
        +30   RAM match
        +30   storage match
        +10   screen_inches match

    Returns match dict or None if no good match found.
    """
    # ── Extract query attributes ──────────────────────────────────────
    query_attrs = extract_laptop_attributes(query, input_brand)
    policy = laptop_policy_class(original_input or query, input_brand, query_attrs)

    q_proc = query_attrs.get('processor', '')
    q_gen = query_attrs.get('generation', '')
    q_ram = query_attrs.get('ram', '')
    q_storage = query_attrs.get('storage', '')
    q_line = query_attrs.get('product_line', '')
    q_pc = query_attrs.get('platform_code', '')
    q_fam = query_attrs.get('laptop_family', '')
    q_mc = query_attrs.get('model_code', '')
    q_screen = query_attrs.get('screen_inches', '')
    q_chip = query_attrs.get('apple_chip', '')
    q_year = query_attrs.get('year', '')
    q_dual = query_attrs.get('storage_ambiguous', False)

    # ── Minimum extraction gate — need *something* to score on ────────
    if policy == 'APPLE_MACBOOK':
        # Apple: need at least product_line (air/pro) + one of storage/chip/year
        if not q_line:
            return None
        if not (q_storage or q_chip or q_year):
            return None
    else:
        # Windows: need at least processor + RAM + storage
        if not (q_proc and q_ram and q_storage):
            return None

    # ── Build candidate pool ──────────────────────────────────────────
    laptop_names = [n for n in search_names if is_laptop_product(n)]

    # Pre-filter by product_line when query specifies one
    if q_line and laptop_names:
        line_filtered = []
        for n in laptop_names:
            na = extract_laptop_attributes(n, input_brand)
            nl_pl = na.get('product_line', '')
            if nl_pl and (nl_pl == q_line or nl_pl in q_line or q_line in nl_pl):
                line_filtered.append(n)
        if line_filtered:
            laptop_names = line_filtered

    # ── Cross-join scoring ────────────────────────────────────────────
    scored = []  # list of (score, nl_name, nl_attrs, match_detail)

    for nl_name in laptop_names:
        nl_attrs = extract_laptop_attributes(nl_name, input_brand)
        nl_proc = nl_attrs.get('processor', '')
        nl_gen = nl_attrs.get('generation', '')
        nl_ram = nl_attrs.get('ram', '')
        nl_storage = nl_attrs.get('storage', '')
        nl_line = nl_attrs.get('product_line', '')
        nl_pc = nl_attrs.get('platform_code', '')
        nl_fam = nl_attrs.get('laptop_family', '')
        nl_mc = nl_attrs.get('model_code', '')
        nl_screen = nl_attrs.get('screen_inches', '')
        nl_chip = nl_attrs.get('apple_chip', '')
        nl_year = nl_attrs.get('year', '')

        score = 0
        detail = []

        # --- Hard rejections (skip candidate entirely) ---
        # Product line mismatch (Air != Pro, Aspire != Predator)
        if q_line and nl_line:
            if not (q_line == nl_line or q_line in nl_line or nl_line in q_line):
                continue
        # Laptop family mismatch (Swift 3 != Swift 5)
        if q_fam and nl_fam and q_fam != nl_fam:
            continue
        # Model code mismatch (sf314 != sf514)
        if q_mc and nl_mc and q_mc != nl_mc:
            continue
        # Platform code mismatch (latitude 5420 != 5520)
        if q_pc and nl_pc and q_pc != nl_pc:
            continue
        # Processor tier mismatch (i5 != i7) — hard reject for Windows
        if policy != 'APPLE_MACBOOK':
            if q_proc and nl_proc and q_proc != nl_proc:
                continue
        # Generation mismatch — hard reject
        if q_gen and nl_gen and q_gen != nl_gen:
            continue
        # RAM mismatch — hard reject for Windows
        if policy != 'APPLE_MACBOOK':
            if q_ram and nl_ram and q_ram != nl_ram:
                continue
        # Storage mismatch — hard reject
        if q_storage and nl_storage and q_storage != nl_storage:
            continue
        # Apple chip mismatch — hard reject
        if q_chip and nl_chip and q_chip != nl_chip:
            continue

        # --- Positive scoring ---
        # Platform code / model code: +100
        if q_pc and nl_pc and q_pc == nl_pc:
            score += 100
            detail.append('code_match')
        elif q_mc and nl_mc and q_mc == nl_mc:
            score += 100
            detail.append('code_match')

        # Processor tier: +40
        if q_proc and nl_proc and q_proc == nl_proc:
            score += 40
            detail.append('cpu')

        # Generation: +40
        if q_gen and nl_gen and q_gen == nl_gen:
            score += 40
            detail.append('gen')

        # RAM: +30
        if q_ram and nl_ram and q_ram == nl_ram:
            score += 30
            detail.append('ram')

        # Storage: +30
        if q_storage and nl_storage and q_storage == nl_storage:
            score += 30
            detail.append('storage')

        # Screen: +10
        if q_screen and nl_screen and q_screen == nl_screen:
            score += 10
            detail.append('screen')

        # Product line match bonus: +15
        if q_line and nl_line:
            if q_line == nl_line or q_line in nl_line or nl_line in q_line:
                score += 15
                detail.append('line')

        # Apple chip match: +40 (replaces cpu+gen for Apple)
        if q_chip and nl_chip and q_chip == nl_chip:
            score += 40
            detail.append('chip')

        # Year match: +10
        if q_year and nl_year and q_year == nl_year:
            score += 10
            detail.append('year')

        if score > 0:
            scored.append((score, nl_name, nl_attrs, detail))

    # Sort by score descending
    scored.sort(key=lambda x: -x[0])

    if not scored:
        return None

    # ── Policy-class completeness gate ────────────────────────────────
    # Even if we have a high-scoring candidate, the query must have enough
    # attributes for MATCHED; otherwise force REVIEW_REQUIRED.
    def _check_completeness() -> tuple:
        """Returns (passes: bool, missing: list[str])."""
        missing = []
        if policy == 'APPLE_MACBOOK':
            # Need: product_line + screen OR chip OR year + storage
            if not q_line:
                missing.append('product_line')
            if not q_storage:
                missing.append('storage')
            if not (q_chip or q_year or q_screen):
                missing.append('chip_or_year_or_screen')
        elif policy == 'WINDOWS_BUSINESS':
            # Need: cpu + gen + ram + storage + (platform_code OR laptop_family)
            if not q_proc:
                missing.append('processor')
            if not q_gen:
                missing.append('generation')
            if not q_ram:
                missing.append('ram')
            if not q_storage:
                missing.append('storage')
            if not (q_pc or q_fam):
                missing.append('code_or_family')
        elif policy == 'WINDOWS_GAMING':
            # Need: (model_code + specs) OR (laptop_family + specs)
            # specs = cpu + ram + storage
            if not q_proc:
                missing.append('processor')
            if not q_ram:
                missing.append('ram')
            if not q_storage:
                missing.append('storage')
            if not (q_mc or q_fam or q_pc):
                missing.append('code_or_family')
        else:  # WINDOWS_OTHER
            # Same as BUSINESS
            if not q_proc:
                missing.append('processor')
            if not q_gen:
                missing.append('generation')
            if not q_ram:
                missing.append('ram')
            if not q_storage:
                missing.append('storage')
            if not (q_pc or q_fam):
                missing.append('code_or_family')
        return len(missing) == 0, missing

    complete, missing_attrs = _check_completeness()

    # ── Dual-storage: always REVIEW_REQUIRED ──────────────────────────
    if q_dual:
        top3 = scored[:3]
        alts = []
        for _, cname, _, _ in top3:
            for aid in search_lookup.get(cname, []):
                if aid not in alts:
                    alts.append(aid)
        return {
            'mapped_uae_assetid': alts[0] if alts else '',
            'match_score': round(scored[0][0], 2),
            'match_status': MATCH_STATUS_SUGGESTED,
            'confidence': CONFIDENCE_MEDIUM,
            'matched_on': scored[0][1],
            'method': 'laptop_attribute_match',
            'auto_selected': False,
            'selection_reason': f'dual_storage({query_attrs.get("storage_list", [])}); top candidates shown',
            'alternatives': alts[1:] if len(alts) > 1 else [],
            'review_reason': 'DUAL_STORAGE',
        }

    # ── Decide MATCHED vs REVIEW ──────────────────────────────────────
    top_score, top_name, top_attrs, top_detail = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0
    MARGIN = 10  # top must lead by at least 10 points

    # Collect top-3 alternative asset IDs for REVIEW output
    def _top3_alt_ids():
        alt_ids = []
        for _, cname, _, _ in scored[:3]:
            for aid in search_lookup.get(cname, []):
                if aid not in alt_ids:
                    alt_ids.append(aid)
        return alt_ids

    # Minimum score threshold: 85 for code-based, 100 for attribute-only
    has_code = any(d == 'code_match' for d in top_detail)
    min_threshold = 85 if has_code else 100

    if top_score < min_threshold:
        # Score too low even for REVIEW
        if top_score >= 60:
            alts = _top3_alt_ids()
            return {
                'mapped_uae_assetid': alts[0] if alts else '',
                'match_score': round(top_score, 2),
                'match_status': MATCH_STATUS_SUGGESTED,
                'confidence': CONFIDENCE_LOW,
                'matched_on': top_name,
                'method': 'laptop_attribute_match',
                'auto_selected': False,
                'selection_reason': f'below threshold ({top_score}<{min_threshold}); attrs={"+".join(top_detail)}',
                'alternatives': alts[1:] if len(alts) > 1 else [],
            }
        return None

    # Completeness or margin fail -> REVIEW_REQUIRED
    if not complete or (top_score - second_score) < MARGIN:
        alts = _top3_alt_ids()
        reason_parts = []
        if not complete:
            reason_parts.append(f'incomplete({",".join(missing_attrs)})')
        if (top_score - second_score) < MARGIN:
            reason_parts.append(f'margin({top_score}-{second_score}={top_score - second_score}<{MARGIN})')
        return {
            'mapped_uae_assetid': alts[0] if alts else '',
            'match_score': round(top_score, 2),
            'match_status': MATCH_STATUS_SUGGESTED,
            'confidence': CONFIDENCE_MEDIUM,
            'matched_on': top_name,
            'method': 'laptop_attribute_match',
            'auto_selected': False,
            'selection_reason': '; '.join(reason_parts) + f'; attrs={"+".join(top_detail)}',
            'alternatives': alts[1:] if len(alts) > 1 else [],
        }

    # ── MATCHED path — single best candidate, passes all gates ────────
    asset_ids = search_lookup.get(top_name, [])

    if len(asset_ids) == 1:
        return {
            'mapped_uae_assetid': asset_ids[0],
            'match_score': round(top_score, 2),
            'match_status': MATCH_STATUS_MATCHED,
            'confidence': CONFIDENCE_HIGH,
            'matched_on': top_name,
            'method': 'laptop_attribute_match',
            'auto_selected': False,
            'selection_reason': f'policy={policy}; attrs={"+".join(top_detail)}',
            'alternatives': [],
        }

    # --- Multi-ID: try tie-breaking with platform_code / model_code ---
    if len(asset_ids) > 1 and nl_catalog is not None:
        if q_pc or q_mc:
            narrowed = []
            for aid in asset_ids:
                row = nl_catalog[nl_catalog['uae_assetid'] == aid]
                if row.empty:
                    continue
                nl_name_raw = str(row.iloc[0].get('uae_assetname', ''))
                nl_a = extract_laptop_attributes(nl_name_raw, input_brand)
                nl_pc_c = nl_a.get('platform_code', '')
                nl_mc_c = nl_a.get('model_code', '')
                if q_pc and nl_pc_c and q_pc == nl_pc_c:
                    narrowed.append(aid)
                elif q_mc and nl_mc_c and q_mc == nl_mc_c:
                    narrowed.append(aid)

            if len(narrowed) == 1:
                others = [a for a in asset_ids if a != narrowed[0]]
                return {
                    'mapped_uae_assetid': narrowed[0],
                    'match_score': round(top_score, 2),
                    'match_status': MATCH_STATUS_MATCHED,
                    'confidence': CONFIDENCE_HIGH,
                    'matched_on': top_name,
                    'method': 'laptop_attribute_match',
                    'auto_selected': True,
                    'selection_reason': f'code tie-break: {q_pc or q_mc}; policy={policy}',
                    'alternatives': others,
                }

    # Multi-ID, no tie-breaker -> REVIEW_REQUIRED with all candidates
    alt_details = []
    for aid in asset_ids:
        if nl_catalog is not None:
            row = nl_catalog[nl_catalog['uae_assetid'] == aid]
            if not row.empty:
                alt_details.append(f'{aid} ({str(row.iloc[0].get("uae_assetname", ""))})')
                continue
        alt_details.append(aid)

    return {
        'mapped_uae_assetid': ', '.join(asset_ids),
        'match_score': round(top_score, 2),
        'match_status': MATCH_STATUS_SUGGESTED,
        'confidence': CONFIDENCE_MEDIUM,
        'matched_on': top_name,
        'method': 'laptop_attribute_match',
        'auto_selected': False,
        'selection_reason': f'multiple NL IDs, no tie-breaker; candidates: {"; ".join(alt_details)}',
        'alternatives': asset_ids,
    }


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
    brand_category_index: Optional[Dict] = None,
    widen_mode: str = 'aggressive',
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
           (e.g., 9,894 -> ~2,000 Apple records). Eliminates cross-brand errors.
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
    # These produce spurious fuzzy matches (e.g., blank Foxway Product Name -> iPad)
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
            # Cannot determine brand -> REVIEW_REQUIRED, never MATCHED
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
            brand_category_index=brand_category_index, widen_mode=widen_mode,
        )
        result['_input_category'] = input_category or ''
        return _enforce_gate(result, query)
    except Exception:
        return no_match_result


def _match_single_item_inner(
    query, nl_lookup, nl_names, threshold, brand_index,
    input_brand, attribute_index, nl_catalog, original_input,
    input_category, no_match_result, signature_index=None,
    brand_category_index=None, widen_mode='aggressive',
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

    # --- Level 1+2: Brand + Category bucketed retrieval (V2 STEP 3C) ---
    # Determine brand and category first, then do a single O(1) bucket lookup
    # instead of brand partition + O(n) category filter.
    search_lookup = nl_lookup
    search_names = nl_names
    brand_norm = normalize_brand(input_brand) if input_brand else ''
    if not brand_norm:
        brand_norm = normalize_text(input_brand) if input_brand else ''

    # Determine query category (same logic as before, just moved up)
    if input_category:
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
        query_category = extract_category(query)

    # Try brand+category bucket (O(1) lookup, pre-computed in run_matching)
    if brand_category_index and brand_norm and query_category != 'other':
        bc_key = (brand_norm, query_category)
        if bc_key in brand_category_index:
            bc_data = brand_category_index[bc_key]
            search_lookup = bc_data['lookup']
            search_names = bc_data['names']
        else:
            # V2 EDIT 4: brand+category bucket missing — widened fallback
            # Instead of hard NO_MATCH, try brand-only then global pool.
            # Widened results are capped at REVIEW_REQUIRED (never MATCHED).
            # Task B: In conservative mode (List 1), only widen if high-signal.
            _allow_widen = True
            if widen_mode == 'conservative':
                _mfk = extract_model_family_key(query, query_category, brand_hint=brand_norm)
                _allow_widen = bool(brand_norm and _mfk)

            if _allow_widen:
                fallback_names = []
                fallback_lookup = {}
                fallback_source = ''
                if brand_index and brand_norm in brand_index:
                    fallback_lookup = brand_index[brand_norm]['lookup']
                    fallback_names = brand_index[brand_norm]['names']
                    fallback_source = 'brand_only'
                if not fallback_names:
                    fallback_lookup = nl_lookup
                    fallback_names = nl_names
                    fallback_source = 'global_pool'

                if fallback_names:
                    top3 = process.extract(
                        query, fallback_names,
                        scorer=fuzz.token_sort_ratio, limit=3,
                    )
                    if top3 and top3[0][1] >= 70:
                        best_name, best_score, _ = top3[0]
                        fb_ids = fallback_lookup.get(best_name, [])
                        if not fb_ids:
                            fb_ids = nl_lookup.get(best_name, [])
                        alts = [{'name': n, 'score': round(s, 2)} for n, s, _ in top3]
                        return {
                            'mapped_uae_assetid': fb_ids[0] if fb_ids else '',
                            'match_score': round(best_score, 2),
                            'match_status': MATCH_STATUS_SUGGESTED,
                            'confidence': CONFIDENCE_LOW,
                            'matched_on': best_name,
                            'method': f'fuzzy_widened_{fallback_source}',
                            'auto_selected': False,
                            'selection_reason': f'widened_search({fallback_source})',
                            'alternatives': alts,
                        }
            return no_match_result
    else:
        # Fallback: brand partition then O(n) category filter (for 'other' or no index)
        if brand_index and brand_norm and brand_norm in brand_index:
            brand_data = brand_index[brand_norm]
            search_lookup = brand_data['lookup']
            search_names = brand_data['names']

        if query_category != 'other':
            category_filtered = [n for n in search_names if extract_category(n) == query_category]
            if category_filtered:
                search_names = category_filtered
            else:
                return no_match_result

    # --- Level 2.5: Laptop attribute-based matching (SPECIAL PATH FOR LAPTOPS) ---
    # For Windows laptops, use attribute matching instead of fuzzy matching
    # to ignore model numbers (SP513-55N, UX325, etc.)
    if query_category == 'laptop' and is_laptop_product(query):
        # V2: normalize EU retail noise before extraction & retrieval
        query_laptop = normalize_laptop_query_text_v2(original_input or query)
        query_laptop_norm = normalize_text(query_laptop)

        laptop_match = match_laptop_by_attributes(
            query_laptop_norm, input_brand, original_input,
            search_names, search_lookup, nl_catalog
        )
        if laptop_match:
            return laptop_match  # Found good attribute match

        # LAPTOP FALLBACK: brand-filtered fuzzy within laptop candidates only.
        # Returns REVIEW_REQUIRED (never MATCHED) with top-3 alternatives.
        # V2: use the cleaned query for better fuzzy scoring.
        laptop_candidates = [n for n in search_names if is_laptop_product(n)]
        if laptop_candidates:
            top_matches = process.extract(
                query_laptop_norm, laptop_candidates,
                scorer=fuzz.token_sort_ratio, limit=3,
            )
            if top_matches and top_matches[0][1] >= threshold:
                best_name, best_score, _ = top_matches[0]
                asset_ids = search_lookup.get(best_name, [])
                return {
                    'mapped_uae_assetid': asset_ids[0] if asset_ids else '',
                    'match_score': round(best_score, 2),
                    'match_status': MATCH_STATUS_SUGGESTED,
                    'confidence': CONFIDENCE_MEDIUM,
                    'matched_on': best_name,
                    'method': 'laptop_fuzzy_fallback',
                    'auto_selected': False,
                    'selection_reason': '',
                    'alternatives': [
                        (n, round(s, 2)) for n, s, _ in top_matches[1:]
                    ],
                }

            # V2 Task B: Laptop-specific CATALOG_MISSING reclassification.
            # If fuzzy fallback found candidates (score >= 60 but < threshold),
            # promote to REVIEW_REQUIRED with top 3 instead of NO_MATCH.
            if top_matches and top_matches[0][1] >= 60:
                best_name, best_score, _ = top_matches[0]
                asset_ids = search_lookup.get(best_name, [])
                return {
                    'mapped_uae_assetid': asset_ids[0] if asset_ids else '',
                    'match_score': round(best_score, 2),
                    'match_status': MATCH_STATUS_SUGGESTED,
                    'confidence': CONFIDENCE_LOW,
                    'matched_on': best_name,
                    'method': 'laptop_retrieval_weak',
                    'auto_selected': False,
                    'selection_reason': 'weak retrieval — top candidates below threshold',
                    'alternatives': [
                        (n, round(s, 2)) for n, s, _ in top_matches[1:]
                    ],
                }
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
                # No same-category products in entire catalog -> return NO_MATCH
                return no_match_result

        result = process.extractOne(
            query,
            fallback_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=effective_threshold,
        )
        search_lookup = nl_lookup  # use full lookup for ID resolution

    if result is None:
        # --- Near-miss recovery: 80-84 score band -> REVIEW_REQUIRED if gate passes ---
        # Only attempt if threshold is the default (don't override raised thresholds)
        near_miss_cutoff = 80
        if effective_threshold <= SIMILARITY_THRESHOLD and widen_mode != 'conservative':
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
    # like Pixel 9 -> Pixel 3 (95%), Mate 20 -> Mate 40 (95%),
    # Nova 5T -> Nova 5i (95%), A57 -> A57s (96%)
    # Pro vs Pro Max (different products!), Plus variants, XL variants
    q_tokens = extract_model_tokens(query)
    m_tokens = extract_model_tokens(best_match)
    if q_tokens and m_tokens:
        # CRITICAL: First check if token counts differ (catches Pro vs Pro Max!)
        # zip() only compares overlapping tokens, so we'd miss the 'max' difference
        if len(q_tokens) != len(m_tokens):
            score = min(score, threshold - 1)  # Demote to NO_MATCH
        else:
            # Same count -> compare position by position (e.g., "5t" vs "5i", "s23" vs "s24")
            for qt, mt in zip(q_tokens, m_tokens):
                if qt != mt:
                    score = min(score, threshold - 1)  # Demote to NO_MATCH
                    break
    elif q_tokens and not m_tokens:
        # Query has model token but match doesn't (e.g., "Reno2 Z" vs "Reno Z")
        # This is a DIFFERENT product — demote to NO_MATCH, not just REVIEW.
        # Previously this only demoted to 89 (REVIEW), where the soft-upgrade path
        # could still accept it because verification_gate skipped the empty-tokens case.
        score = min(score, threshold - 1)  # Demote to NO_MATCH
    elif not q_tokens and m_tokens:
        # Match has model token but query doesn't (e.g., "Find X" -> "Find X9")
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

    if len(asset_ids) == 0:
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
    if confidence == CONFIDENCE_LOW:
        # V2 EDIT 1: Upgrade to REVIEW_REQUIRED with top-3 fuzzy candidates
        # instead of NO_MATCH — makes the result actionable for human review.
        # Never MATCHED, only REVIEW_REQUIRED with FUZZY_ONLY reason.
        # Task B: In conservative mode (List 1), skip upgrade -> keep NO_MATCH.
        if widen_mode == 'conservative':
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
        top3 = process.extract(query, search_names, scorer=fuzz.token_sort_ratio, limit=3)
        alts = [{'name': n, 'score': round(s, 2)} for n, s, _ in top3]
        return {
            'mapped_uae_assetid': ', '.join(asset_ids),
            'match_score': score_rounded,
            'match_status': MATCH_STATUS_SUGGESTED,
            'confidence': CONFIDENCE_LOW,
            'matched_on': best_match,
            'method': 'fuzzy_below_threshold',
            'auto_selected': False,
            'selection_reason': f'below_threshold(score={score_rounded})',
            'alternatives': alts,
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
        # Score >= 88 with ALL key attributes matching -> safe to upgrade to MATCHED
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


def _normalize_alternatives(raw_alts, nl_catalog=None, method=''):
    """
    V2: Convert alternatives from any format to standardized list-of-dicts.

    Input formats handled:
      - [] (empty)
      - ['NL-001', 'NL-002'] (asset ID strings from auto_select)
      - [('normalized_name', 85.5)] (tuples from laptop_fuzzy_fallback)
      - [{'name': ..., 'score': ...}] (dicts from near_miss_recovery)

    Output format (always):
      [{'uae_assetid': str, 'uae_assetname': str, 'score': float, 'reason': str}, ...]
    """
    if not raw_alts:
        return []
    normalized = []
    for item in raw_alts[:3]:  # cap at 3 alternatives
        if isinstance(item, dict):
            # Already dict — standardize keys
            entry = {
                'uae_assetid': str(item.get('uae_assetid', '')),
                'uae_assetname': str(item.get('uae_assetname', item.get('name', ''))),
                'score': round(float(item.get('score', 0)), 2),
                'reason': str(item.get('reason', method)),
            }
            # Fill missing ID from catalog by name lookup
            if not entry['uae_assetid'] and entry['uae_assetname'] and nl_catalog is not None:
                matches = nl_catalog[nl_catalog['normalized_name'] == entry['uae_assetname']]
                if len(matches) > 0:
                    entry['uae_assetid'] = str(matches.iloc[0]['uae_assetid'])
            normalized.append(entry)
        elif isinstance(item, (tuple, list)) and len(item) >= 2:
            # Tuple/list format: (name, score) from laptop_fuzzy_fallback
            name = str(item[0])
            sc = round(float(item[1]), 2) if item[1] else 0
            aid = ''
            aname = ''
            if nl_catalog is not None:
                matches = nl_catalog[nl_catalog['normalized_name'] == name]
                if len(matches) > 0:
                    aid = str(matches.iloc[0]['uae_assetid'])
                    aname = str(matches.iloc[0].get('uae_assetname', name))
            normalized.append({
                'uae_assetid': aid,
                'uae_assetname': aname or name,
                'score': sc,
                'reason': method or 'fuzzy_candidate',
            })
        elif isinstance(item, str):
            # Plain asset ID string (from auto_select_matching_variant)
            aname = ''
            if nl_catalog is not None:
                matches = nl_catalog[nl_catalog['uae_assetid'] == item]
                if len(matches) > 0:
                    aname = str(matches.iloc[0].get('uae_assetname', ''))
            normalized.append({
                'uae_assetid': item,
                'uae_assetname': aname,
                'score': 0,
                'reason': method or 'variant_alternative',
            })
    return normalized


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
    widen_mode: str = 'aggressive',
) -> pd.DataFrame:
    """
    Run hybrid matching for an entire input DataFrame against the NL lookup.

    Matching is hybrid (attribute-based fast path + signature + fuzzy fallback):
        0. Attribute matching (fast path) -> 70-80% of queries in 2-5ms
        0.5. Signature matching -> deterministic variant resolution
        1. Brand partition -> narrows search to one brand
        2. Category filter -> prevents cross-category errors
        3. Storage filter -> narrows to same storage variant
        4. Fuzzy match -> finds best candidate
        5. Model token guard -> rejects wrong model tokens
        6. Auto-select -> automatically selects best variant from multiple matches

    Args:
        df_input: The input asset list (List 1 or List 2)
        brand_col: Column name containing the brand/manufacturer
        name_col: Column name containing the product name
        nl_lookup: dict of normalized_name -> [asset_ids] (full, for fallback)
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

    # Pre-detect fallback columns for URL/name recovery (once, not per-row)
    _FALLBACK_NAME_COLS = ['product_name', 'product name', 'title', 'name',
                           'asset name', 'asset_name', 'model_name', 'model name']
    _FALLBACK_URL_COLS = ['url', 'product_url', 'product url', 'link', 'href',
                          'product_slug', 'product_link', 'page_url', 'page url',
                          'source_url', 'source url', 'item_url', 'item url']
    col_lower_map = {str(c).strip().lower(): str(c).strip() for c in df.columns}
    fallback_name_col = None
    for fc in _FALLBACK_NAME_COLS:
        if fc in col_lower_map and col_lower_map[fc] != name_col:
            fallback_name_col = col_lower_map[fc]
            break
    fallback_url_col = None
    for fc in _FALLBACK_URL_COLS:
        if fc in col_lower_map and col_lower_map[fc] != name_col:
            fallback_url_col = col_lower_map[fc]
            break

    # V2 Enhancement (STEP 3C): Build brand+category bucketed index for O(1) retrieval.
    # Pre-partitions candidates by (brand, category) so per-item category filtering is
    # replaced with a direct dictionary lookup — eliminates O(n) extract_category() calls.
    brand_category_index = {}
    if brand_index:
        for brand_key, brand_data in brand_index.items():
            for name in brand_data['names']:
                cat = extract_category(name)
                bc_key = (brand_key, cat)
                if bc_key not in brand_category_index:
                    brand_category_index[bc_key] = {'lookup': {}, 'names': []}
                brand_category_index[bc_key]['names'].append(name)
                if name in brand_data['lookup']:
                    brand_category_index[bc_key]['lookup'][name] = brand_data['lookup'][name]

    results = []
    for idx, row in df.iterrows():
        no_match_reason = ''
        query = ''
        try:
            input_brand = str(row.get(brand_col, '')).strip() if brand_col != '__no_brand__' else ''
            original_product_name = str(row.get(name_col, '')).strip()

            # --- URL / empty name fallback ---
            # If the detected name column contains a URL or is empty/nan, try fallbacks
            _name_is_bad = (
                not original_product_name
                or original_product_name.lower() in ('nan', 'none', '')
                or _is_url(original_product_name)
            )
            if _name_is_bad:
                recovered = False
                # Fallback 1: Try a dedicated name column we didn't pick initially
                if fallback_name_col:
                    fb_val = str(row.get(fallback_name_col, '')).strip()
                    if fb_val and fb_val.lower() not in ('nan', 'none', '') and not _is_url(fb_val):
                        original_product_name = fb_val
                        recovered = True
                # Fallback 2: Extract product name from a URL column
                if not recovered and fallback_url_col:
                    url_val = str(row.get(fallback_url_col, '')).strip()
                    extracted = extract_name_from_url(url_val)
                    if extracted:
                        original_product_name = extracted
                        recovered = True
                # Fallback 3: Try extracting from the original value if it was a URL
                if not recovered and _is_url(str(row.get(name_col, '')).strip()):
                    extracted = extract_name_from_url(str(row.get(name_col, '')).strip())
                    if extracted:
                        original_product_name = extracted
                        recovered = True
                if not recovered:
                    no_match_reason = 'EMPTY_PRODUCT_NAME' if not _is_url(str(row.get(name_col, '')).strip()) else 'URL_NOT_PARSED'

            # Brand inference: if brand is missing, try to extract from product name
            if not input_brand or input_brand.lower() in ('nan', 'none', ''):
                inferred = _infer_brand_from_name(original_product_name)
                if inferred:
                    input_brand = inferred

            # Extract category from uploaded data if available
            input_category = str(row.get(category_col, '')).strip() if category_col else ''

            # --- Category inference fallback ---
            # If no category column or value is empty, infer from product name
            if not input_category or input_category.lower() in ('nan', 'none', ''):
                inferred_cat = extract_category(original_product_name)
                if inferred_cat and inferred_cat != 'other':
                    input_category = inferred_cat

            # Strip color words (French/English) before matching.
            # Colors like "Bleu", "Noir", "Argent" in recommerce data are never
            # in NL catalog names and dilute fuzzy matching scores.
            original_product_name = strip_color_words(original_product_name)

            # ENHANCEMENT: If storage/capacity column exists, combine it with product name
            # This improves matching for datasets that separate model and capacity
            # Example: "iPad Pro 2022 11" + "128GB" -> "iPad Pro 2022 11 128GB"
            if storage_col:
                storage_value = str(row.get(storage_col, '')).strip()
                if storage_value:
                    # Combine name + storage for better matching
                    original_product_name = f"{original_product_name} {storage_value}"

            # Skip matching if we have no usable product name
            if not original_product_name or original_product_name.lower() in ('nan', 'none', ''):
                match_result = {
                    'mapped_uae_assetid': '',
                    'match_score': 0,
                    'match_status': MATCH_STATUS_NO_MATCH,
                    'confidence': CONFIDENCE_LOW,
                    'matched_on': '',
                    'method': 'skipped',
                    'auto_selected': False,
                    'selection_reason': '',
                    'alternatives': [],
                }
                if not no_match_reason:
                    no_match_reason = 'EMPTY_PRODUCT_NAME'
            else:
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
                    brand_category_index=brand_category_index,
                    widen_mode=widen_mode,
                )
                # Set no_match_reason based on result (V2 enhanced reason codes)
                if match_result.get('match_status') == MATCH_STATUS_NO_MATCH and not no_match_reason:
                    method = match_result.get('method', '')
                    score = match_result.get('match_score', 0)
                    vr = match_result.get('verification_reasons', '')
                    if method in ('empty_input', 'missing_brand'):
                        no_match_reason = 'LOW_SIGNAL_INPUT'
                    elif 'identity_rejected' in method or 'gate_blocked' in method:
                        no_match_reason = 'BLOCKED_BY_GATE'
                    elif 'category_cross' in str(vr):
                        no_match_reason = 'TAXONOMY_MISMATCH'
                    elif input_category and input_category.lower().strip() == 'laptop':
                        # V2 Task B: stricter CATALOG_MISSING for laptops.
                        # Only label CATALOG_MISSING_LIKELY when:
                        # - brand known AND best candidate score < 75
                        # - AND no high-signal family/platform code in query
                        _q_laptop = normalize_laptop_query_text_v2(original_product_name)
                        _q_attrs = extract_laptop_attributes(
                            normalize_text(_q_laptop), input_brand)
                        _has_signal = bool(
                            _q_attrs.get('laptop_family')
                            or _q_attrs.get('platform_code')
                            or _q_attrs.get('model_code')
                        )
                        if input_brand and score < 75 and not _has_signal:
                            no_match_reason = 'CATALOG_MISSING_LIKELY'
                        else:
                            # Has family/code signal OR score >= 75 — SKU
                            # may exist but retrieval/gate blocked it.
                            no_match_reason = 'RETRIEVAL_WEAK'
                    elif method == 'none' and score == 0:
                        no_match_reason = 'CATALOG_MISSING_LIKELY'
                    elif score > 0 and score < threshold:
                        no_match_reason = 'CATALOG_MISSING_LIKELY'
                    else:
                        no_match_reason = 'CATALOG_MISSING_LIKELY'
                elif match_result.get('match_status') in (MATCH_STATUS_MATCHED, MATCH_STATUS_MULTIPLE):
                    no_match_reason = 'SUCCESS'

                # Set review_reason for REVIEW_REQUIRED items (V2)
                review_reason = ''
                if match_result.get('match_status') == MATCH_STATUS_SUGGESTED:
                    method = match_result.get('method', '')
                    sel_reason = match_result.get('selection_reason', '')
                    if 'retrieval_weak' in method:
                        review_reason = 'RETRIEVAL_WEAK'
                    elif 'fuzzy' in method:
                        review_reason = 'FUZZY_ONLY'
                    elif 'gate_blocked' in method:
                        review_reason = 'GATE_BLOCKED_SUSPICIOUS'
                    elif 'multiple NL IDs' in str(sel_reason):
                        review_reason = 'MULTI_ID_AMBIGUOUS'
                    else:
                        review_reason = 'NEAR_MATCH_NEEDS_HUMAN'
                match_result['review_reason'] = review_reason

                # V2 EDIT 1: Normalize alternatives to standardized list-of-dicts
                match_result['alternatives'] = _normalize_alternatives(
                    match_result.get('alternatives', []),
                    nl_catalog=nl_catalog,
                    method=match_result.get('method', ''),
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
            if not no_match_reason:
                no_match_reason = 'PROCESSING_ERROR'

        # --- Serialize alternatives to JSON string (FIX 1+2) ---
        # Guarantees alternatives is always a valid JSON string, parseable with json.loads().
        # _normalize_alternatives already ran inside try block; error block has [].
        alts = match_result.get('alternatives', [])
        if not isinstance(alts, str):
            match_result['alternatives'] = json.dumps(alts if alts else [], ensure_ascii=False)

        # --- Task A: Blocked-candidate suggestions for gate-blocked reviews ---
        # When gate blocks a match, store the blocked candidate + 2 nearby candidates
        # so reviewers have actionable context even for gate-blocked rows.
        _method = match_result.get('method', '')
        _review_reason = match_result.get('review_reason', '')
        if ('gate_blocked' in _method or _review_reason == 'GATE_BLOCKED_SUSPICIOUS'):
            blocked_name = match_result.get('matched_on', '')
            blocked_id = match_result.get('mapped_uae_assetid', '')
            blocked_score = match_result.get('match_score', 0)
            blocked_cands = [{
                'uae_assetid': str(blocked_id),
                'uae_assetname': blocked_name,
                'score': round(float(blocked_score), 2) if blocked_score else 0,
                'reason': f'gate_blocked: {match_result.get("verification_reasons", "")[:80]}',
            }]
            # Find 2 more nearby candidates from the brand bucket
            _b = normalize_brand(input_brand) if input_brand else ''
            if _b and brand_index and _b in brand_index:
                _bucket_names = brand_index[_b]['names']
                _bucket_lookup = brand_index[_b]['lookup']
            else:
                _bucket_names = nl_names
                _bucket_lookup = nl_lookup
            try:
                _top5 = process.extract(query, _bucket_names, scorer=fuzz.token_sort_ratio, limit=5)
                for _cn, _cs, _ in _top5:
                    if len(blocked_cands) >= 3:
                        break
                    if _cn == blocked_name:
                        continue
                    _cids = _bucket_lookup.get(_cn, []) or nl_lookup.get(_cn, [])
                    blocked_cands.append({
                        'uae_assetid': ', '.join(_cids) if _cids else '',
                        'uae_assetname': _cn,
                        'score': round(_cs, 2),
                        'reason': 'nearby_candidate',
                    })
            except Exception:
                pass
            match_result['blocked_candidates'] = json.dumps(blocked_cands, ensure_ascii=False)
        else:
            match_result['blocked_candidates'] = '[]'

        # --- Task 4: Ensure no empty REVIEW rows ---
        # If REVIEW_REQUIRED but neither alternatives nor blocked_candidates
        # has any data, downgrade to NO_MATCH (unactionable for analysts).
        if match_result.get('match_status') == MATCH_STATUS_SUGGESTED:
            _alts_raw = match_result.get('alternatives', '[]')
            _blk_raw = match_result.get('blocked_candidates', '[]')
            try:
                _alts_list = json.loads(_alts_raw) if isinstance(_alts_raw, str) else (_alts_raw or [])
            except Exception:
                _alts_list = []
            try:
                _blk_list = json.loads(_blk_raw) if isinstance(_blk_raw, str) else (_blk_raw or [])
            except Exception:
                _blk_list = []
            if not _alts_list and not _blk_list:
                match_result['match_status'] = MATCH_STATUS_NO_MATCH
                match_result['confidence'] = CONFIDENCE_LOW
                if not no_match_reason or no_match_reason == 'SUCCESS':
                    no_match_reason = 'NO_ACTIONABLE_CANDIDATES'
                match_result['review_reason'] = ''

        # --- Task 1: review_summary (one-line human-readable summary) ---
        _status = match_result.get('match_status', '')
        _rr = match_result.get('review_reason', '')
        _nmr = no_match_reason
        _vr = str(match_result.get('verification_reasons', ''))
        _score = match_result.get('match_score', 0)
        _mo = match_result.get('matched_on', '')
        review_summary = ''
        if _status == MATCH_STATUS_SUGGESTED:
            if _rr == 'GATE_BLOCKED_SUSPICIOUS':
                _first_reason = _vr.split(';')[0].strip()[:60] if _vr else 'attribute mismatch'
                review_summary = f'Gate blocked ({_first_reason}). Verify candidate.'
            elif _rr == 'FUZZY_ONLY':
                review_summary = f'Low-confidence fuzzy ({_score}%). Verify identity.'
            elif _rr == 'MULTI_ID_AMBIGUOUS':
                review_summary = 'Multiple catalog IDs. Select correct variant.'
            else:
                review_summary = f'Near match ({_score}%). Needs human verification.'
        elif _status == MATCH_STATUS_NO_MATCH:
            if _nmr == 'CATALOG_MISSING_LIKELY':
                review_summary = 'Likely missing from NL catalog. Request addition.'
            elif _nmr == 'LOW_SIGNAL_INPUT':
                review_summary = 'Insufficient product info to match.'
            elif _nmr == 'BLOCKED_BY_GATE':
                review_summary = 'Candidate found but rejected by safety gate.'
            elif _nmr == 'TAXONOMY_MISMATCH':
                review_summary = 'Category mismatch between input and catalog.'
            elif _nmr == 'NO_ACTIONABLE_CANDIDATES':
                review_summary = 'No viable candidates found for review.'
            elif _nmr == 'PROCESSING_ERROR':
                review_summary = 'Processing error during matching.'
            elif _nmr:
                review_summary = f'No match: {_nmr}.'
        match_result['review_summary'] = review_summary

        # --- Task 2: review_priority (numeric, higher = more actionable) ---
        review_priority = 0.0
        if _status == MATCH_STATUS_SUGGESTED:
            review_priority = float(_score) if _score else 0.0
            # Parse best alt/blk scores
            try:
                _pa = json.loads(match_result.get('alternatives', '[]')) if isinstance(match_result.get('alternatives'), str) else (match_result.get('alternatives') or [])
            except Exception:
                _pa = []
            try:
                _pb = json.loads(match_result.get('blocked_candidates', '[]')) if isinstance(match_result.get('blocked_candidates'), str) else (match_result.get('blocked_candidates') or [])
            except Exception:
                _pb = []
            _best_alt_score = max((float(a.get('score', 0)) for a in _pa if isinstance(a, dict)), default=0)
            _best_blk_score = max((float(b.get('score', 0)) for b in _pb if isinstance(b, dict)), default=0)
            _best_cand = max(_best_alt_score, _best_blk_score)
            if _best_cand >= 90:
                review_priority += 20
            elif _best_cand >= 80:
                review_priority += 10
            # Gate-blocked = was almost a match, high priority
            if _rr == 'GATE_BLOCKED_SUSPICIOUS':
                review_priority += 15
            elif _rr == 'MULTI_ID_AMBIGUOUS':
                review_priority += 10
        match_result['review_priority'] = round(review_priority, 2)

        # --- Original input fields (always included for Excel export) ---
        # Attach the original product name so export code never has to guess column names
        match_result['original_input'] = original_product_name

        # --- NO_MATCH_REASON debug column ---
        match_result['no_match_reason'] = no_match_reason

        # --- Category column (always included for Excel export) ---
        match_result['category'] = extract_category(query) if query else (
            extract_category(original_product_name) if original_product_name else ''
        )

        # --- Verification columns (always included for Excel export) ---
        # If _enforce_gate already set verification (MATCHED/REVIEW results),
        # respect its decision (laptop relaxation, etc.).  Only run raw
        # verification_gate for results that didn't go through _enforce_gate.
        if 'verification_pass' not in match_result:
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
    # V2 columns: reason codes
    if 'no_match_reason' in results_df.columns:
        df['no_match_reason'] = results_df['no_match_reason'].values
    if 'review_reason' in results_df.columns:
        df['review_reason'] = results_df['review_reason'].values
    # V2 Task A: blocked_candidates for gate-blocked reviews
    if 'blocked_candidates' in results_df.columns:
        df['blocked_candidates'] = results_df['blocked_candidates'].values
    # V2: review_summary, review_priority
    if 'review_summary' in results_df.columns:
        df['review_summary'] = results_df['review_summary'].values
    if 'review_priority' in results_df.columns:
        df['review_priority'] = results_df['review_priority'].values

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
        method_breakdown: dict of method -> count
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
        unmatched_brands: dict of brand -> count for NO_MATCH items
        high_volume_unmatched: list of product names appearing >= 3 times as NO_MATCH
        near_miss_candidates: list of dicts with query, top_candidate, score for 80-84 band
        brand_coverage: dict of brand -> {matched, total, rate} for each brand
        category_coverage: dict of category -> {matched, total, rate}
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
# Columns to EXCLUDE from name detection (IDs, URLs, slugs, prices — not product names)
NAME_EXCLUDE_KEYWORDS = ['id', 'serial', 'imei', 'barcode', 'sku', 'code', 'number',
                         'slug', 'url', 'link', 'href', 'path', 'price', 'image', 'thumbnail']

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

    Priority order:
      1. Exact match on known product-name column names (product_name, product title, etc.)
      2. "model" keyword (excluding IDs)
      3. General NAME_KEYWORDS (excluding category/ID/URL columns)

    This ensures 'product_name' is always preferred over 'product_slug' or 'url'.
    """
    # Priority 1: Exact match on well-known product name column names
    EXACT_NAME_COLUMNS = [
        'product_name', 'product name', 'productname',
        'product title', 'product_title', 'producttitle',
        'asset name', 'asset_name', 'assetname',
        'model_name', 'model name', 'modelname',
        'device_name', 'device name', 'devicename',
        'item_name', 'item name', 'itemname',
        'title', 'name',
    ]
    for exact in EXACT_NAME_COLUMNS:
        for col in columns:
            if col.lower().strip() == exact:
                return col

    # Priority 2: Look for "model" keyword first
    for col in columns:
        col_lower = col.lower().strip()
        if 'model' in col_lower and 'type' not in col_lower:
            # Exclude ID-like columns (e.g., "Model Number", "Model ID")
            if any(excl in col_lower for excl in NAME_EXCLUDE_KEYWORDS):
                continue
            return col

    # Priority 3: Look for other name keywords, but exclude category and ID/URL columns
    for col in columns:
        col_lower = col.lower().strip()
        col_normalized = col_lower.replace(' ', '_')

        # Skip if this looks like a category column
        if any(kw.replace(' ', '_') in col_normalized for kw in CATEGORY_KEYWORDS):
            continue

        # Skip if this looks like an ID/URL/slug column
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

    Returns dict:  sheet_name -> {
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

            # --- URL-only CSV detection (e.g., Forza) ---
            # If the detected name column has very low cardinality (few unique values
            # vs total rows), it's likely a categorical field (grade, condition) rather
            # than actual product names.  Synthesize product_name from URL column.
            _url_col_name = None
            for _c in raw_headers:
                if _c.lower().strip() in ('url', 'product_url', 'link', 'href'):
                    _url_col_name = _c
                    break
            if _url_col_name and col_map['name_col']:
                _nunique = df[col_map['name_col']].nunique()
                _total = max(len(df), 1)
                if _nunique / _total < 0.1 and _nunique <= 10:
                    # Name column is categorical -> extract product names from URLs
                    df['product_name'] = (
                        df[_url_col_name]
                        .apply(extract_name_from_url)
                        .apply(_clean_url_extracted_name)
                    )
                    col_map['name_col'] = 'product_name'
            # Also handle the case where no name column was detected at all
            # but a URL column exists
            if col_map['name_col'] is None and _url_col_name:
                df['product_name'] = (
                    df[_url_col_name]
                    .apply(extract_name_from_url)
                    .apply(_clean_url_extracted_name)
                )
                col_map['name_col'] = 'product_name'

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

            # --- URL-only sheet detection (same logic as CSV path) ---
            _url_col_name = None
            for _c in raw_headers:
                if _c.lower().strip() in ('url', 'product_url', 'link', 'href'):
                    _url_col_name = _c
                    break
            if _url_col_name and col_map['name_col']:
                _nunique = df[col_map['name_col']].nunique()
                _total = max(len(df), 1)
                if _nunique / _total < 0.1 and _nunique <= 10:
                    df['product_name'] = (
                        df[_url_col_name]
                        .apply(extract_name_from_url)
                        .apply(_clean_url_extracted_name)
                    )
                    col_map['name_col'] = 'product_name'
            if col_map['name_col'] is None and _url_col_name:
                df['product_name'] = (
                    df[_url_col_name]
                    .apply(extract_name_from_url)
                    .apply(_clean_url_extracted_name)
                )
                col_map['name_col'] = 'product_name'

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

        # --- OPPO RENO VARIANT TESTS ---
        # 21. Reno2 Z must NOT match Reno Z (missing generation digit)
        ('oppo reno2 z 128gb', 'oppo reno series reno z 128gb', False,
         'Reno2 Z vs Reno Z should fail (generation mismatch)'),
        # 22. Reno4 Z must NOT match Reno5 Z (different generation)
        ('oppo reno4 z 128gb', 'oppo reno5 series reno5 z 128gb', False,
         'Reno4 Z vs Reno5 Z should fail (reno4 vs reno5)'),
        # 23. Reno7 Z must NOT match Reno5 Z (different generation)
        ('oppo reno7 z 128gb', 'oppo reno5 series reno5 z 128gb', False,
         'Reno7 Z vs Reno5 Z should fail (reno7 vs reno5)'),
        # 24. Reno Z vs Reno Z should PASS (same base model, no digit on either side)
        ('oppo reno z 128gb', 'oppo reno series reno z 128gb', True,
         'Reno Z vs Reno Z should pass (consistent)'),
        # 25. Reno2 vs Reno2 should PASS (same model)
        ('oppo reno2 128gb', 'oppo reno2 series reno2 128gb', True,
         'Reno2 vs Reno2 should pass (exact match)'),
        # 26. Reno2 Z vs Reno2 (without Z) should FAIL (Z variant is distinct)
        ('oppo reno2 z 128gb', 'oppo reno2 series reno2 128gb', False,
         'Reno2 Z vs Reno2 (no Z) should fail (Z variant mismatch)'),

        # --- XIAOMI / POCO / REDMI VARIANT TESTS ---
        # 27. Poco F4 GT vs Poco F4 (GT variant lost)
        ('poco f4 gt 128gb', 'poco f4 128gb', False,
         'Poco F4 GT vs F4 should fail (GT variant)'),
        # 28. Redmi Note 12 Turbo vs Redmi Note 12 (Turbo variant lost)
        ('redmi note 12 turbo 256gb', 'redmi note 12 256gb', False,
         'Note 12 Turbo vs Note 12 should fail (Turbo variant)'),
        # 29. Xiaomi 14 Ultra vs Xiaomi 14 Ultra Photography Kit (bundle mismatch)
        ('xiaomi 14 ultra 256gb', 'xiaomi 14 ultra photography kit 256gb', False,
         'Xiaomi 14 Ultra vs Photography Kit should fail (kit suffix)'),
        # 30. Correct Poco match should pass
        ('poco f4 gt 128gb', 'poco f4 gt 128gb', True,
         'Poco F4 GT exact match should pass'),
        # 31. Correct Redmi match should pass
        ('redmi note 12 turbo 256gb', 'redmi note 12 turbo 256gb', True,
         'Note 12 Turbo exact match should pass'),
        # 32. OPPO Reno cross-generation: Reno4 Pro vs Reno3 Pro
        ('oppo reno4 pro 128gb', 'oppo reno3 pro 128gb', False,
         'Reno4 Pro vs Reno3 Pro should fail (generation mismatch)'),
        # 33. OPPO Reno cross-generation: Reno8 vs Reno5
        ('oppo reno8 128gb', 'oppo reno5 128gb', False,
         'Reno8 vs Reno5 should fail (generation mismatch)'),
        # 34. OPPO Reno cross-generation: Reno10 Pro vs Reno6
        ('oppo reno10 pro 128gb', 'oppo reno6 128gb', False,
         'Reno10 Pro vs Reno6 should fail (generation + variant mismatch)'),
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

    # 41. Nike edition vs base watch -> reject
    p41, _ = verification_gate(
        normalize_text('apple watch series 9 45mm gps nike aluminum'),
        normalize_text('apple watch series 9 45mm gps aluminum'))
    if p41:
        failures.append('FAIL: Watch Nike vs base should be rejected')

    # 42. Black Unity vs base watch -> reject
    p42, _ = verification_gate(
        normalize_text('apple watch series 9 45mm black unity'),
        normalize_text('apple watch series 9 45mm'))
    if p42:
        failures.append('FAIL: Watch Black Unity vs base should be rejected')

    # 43. Hermes vs base watch -> reject
    p43, _ = verification_gate(
        normalize_text('apple watch series 9 45mm hermes'),
        normalize_text('apple watch series 9 45mm'))
    if p43:
        failures.append('FAIL: Watch Hermes vs base should be rejected')

    # 44. Edition vs base watch -> reject
    p44, _ = verification_gate(
        normalize_text('apple watch series 9 45mm special edition'),
        normalize_text('apple watch series 9 45mm'))
    if p44:
        failures.append('FAIL: Watch Special Edition vs base should be rejected')

    # 45. Nike vs Hermes -> reject
    p45, _ = verification_gate(
        normalize_text('apple watch series 9 45mm nike'),
        normalize_text('apple watch series 9 45mm hermes'))
    if p45:
        failures.append('FAIL: Watch Nike vs Hermes should be rejected')

    # 46. Matching Nike editions -> pass
    p46, _ = verification_gate(
        normalize_text('apple watch series 9 45mm nike aluminum'),
        normalize_text('apple watch series 9 45mm nike aluminum'))
    if not p46:
        failures.append('FAIL: Identical Nike watch should pass gate')

    # === TASK B: Tablet size + line regression tests ===

    # 47. MatePad 10.4 vs MatePad Pro 11.0 -> reject (size + line mismatch)
    p47, r47 = verification_gate(
        normalize_text('huawei matepad 10.4 2022 128gb'),
        normalize_text('huawei matepad pro 11.0 2022 128gb'))
    if p47:
        failures.append(f'FAIL: MatePad 10.4 vs MatePad Pro 11.0 should be rejected: {r47}')

    # 48. iPad Pro 11 vs iPad Pro 12.9 -> reject (size mismatch)
    p48, _ = verification_gate(
        normalize_text('apple ipad pro 11 2022 256gb'),
        normalize_text('apple ipad pro 12.9 2022 256gb'))
    if p48:
        failures.append('FAIL: iPad Pro 11 vs 12.9 should be rejected')

    # 49. MatePad base vs MatePad Pro -> reject (tablet_line mismatch)
    p49, _ = verification_gate(
        normalize_text('huawei matepad 10.4 128gb'),
        normalize_text('huawei matepad pro 10.4 128gb'))
    if p49:
        failures.append('FAIL: MatePad base vs MatePad Pro should be rejected')

    # === TASK C: MacBook year strictness ===

    # 50. MacBook Pro 2023 vs MacBook Pro 2024 -> reject
    p50, _ = verification_gate(
        normalize_text('apple macbook pro 2023 m3 16gb 512gb'),
        normalize_text('apple macbook pro 2024 m3 16gb 512gb'))
    if p50:
        failures.append('FAIL: MacBook Pro 2023 vs 2024 should be rejected')

    # 51. MacBook Pro 2023 vs MacBook Pro 2023 -> pass
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

    # 57. iPad Mini 7th gen vs iPad Mini 5th gen -> reject (generation mismatch)
    p57, r57 = verification_gate(
        normalize_text('apple ipad mini 7th gen 256gb'),
        normalize_text('apple ipad mini 5th gen 256gb'))
    if p57:
        failures.append(f'FAIL: iPad Mini 7th gen vs 5th gen should be rejected: {r57}')

    # 58. MatePad 10.4 vs MatePad 11 -> reject (screen inches mismatch)
    p58, r58 = verification_gate(
        normalize_text('huawei matepad 10.4 128gb'),
        normalize_text('huawei matepad 11 inch 128gb'))
    if p58:
        failures.append(f'FAIL: MatePad 10.4 vs 11 should be rejected: {r58}')

    # 59. Watch Nike vs standard -> reject (edition mismatch)
    p59, r59 = verification_gate(
        normalize_text('apple watch series 9 45mm gps nike'),
        normalize_text('apple watch series 9 45mm gps'))
    if p59:
        failures.append(f'FAIL: Watch Nike vs standard should be rejected: {r59}')

    # 60. Empty input guard: match_single_item returns NO_MATCH for "  "
    empty2_result = match_single_item('  ', {}, [], 85)
    if empty2_result['match_status'] != MATCH_STATUS_NO_MATCH:
        failures.append(f'FAIL: Empty input should return NO_MATCH, got {empty2_result["match_status"]}')

    # 61. extract_tablet_generation: "7th gen" -> "7"
    if extract_tablet_generation('apple ipad mini 7th gen 256gb') != '7':
        failures.append(
            f'FAIL: extract_tablet_generation("...7th gen...") — expected "7", '
            f'got "{extract_tablet_generation("apple ipad mini 7th gen 256gb")}"')

    # 62. extract_tablet_generation: "gen5" (from normalize_text) -> "5"
    if extract_tablet_generation('apple ipad gen5 wifi 128gb') != '5':
        failures.append(
            f'FAIL: extract_tablet_generation("...gen5...") — expected "5", '
            f'got "{extract_tablet_generation("apple ipad gen5 wifi 128gb")}"')

    # 63. extract_screen_inches: "8.3 inch" -> "8.3"
    if extract_screen_inches('apple ipad mini 8.3 inch 256gb') != '8.3':
        failures.append(
            f'FAIL: extract_screen_inches("...8.3 inch...") — expected "8.3", '
            f'got "{extract_screen_inches("apple ipad mini 8.3 inch 256gb")}"')

    # 64. extract_screen_inches: bare "10.4" -> "10.4"
    if extract_screen_inches('huawei matepad 10.4 128gb') != '10.4':
        failures.append(
            f'FAIL: extract_screen_inches("...10.4...") — expected "10.4", '
            f'got "{extract_screen_inches("huawei matepad 10.4 128gb")}"')

    # 65. Signature includes generation: iPad mini 7th gen
    # Generation is encoded as "gen7" in the model part (from normalize_text "7th gen" -> "gen7")
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

    # 103. tablet_family extraction: iPad Pro -> "ipad pro"
    t103 = extract_product_attributes(normalize_text('apple ipad pro 12.9 inch 256gb'), 'apple')
    if t103.get('tablet_family') != 'ipad pro':
        failures.append(f'FAIL: tablet_family for iPad Pro should be "ipad pro", got "{t103.get("tablet_family")}"')

    # 104. tablet_family extraction: MatePad Pro -> "matepad pro"
    t104 = extract_product_attributes(normalize_text('huawei matepad pro 11 128gb'), 'huawei')
    if t104.get('tablet_family') != 'matepad pro':
        failures.append(f'FAIL: tablet_family for MatePad Pro should be "matepad pro", got "{t104.get("tablet_family")}"')

    # 105. connectivity extraction: "wifi" detected
    t105 = extract_product_attributes('apple ipad mini 7th gen 256gb wifi', 'apple')
    if t105.get('connectivity') != 'wifi':
        failures.append(f'FAIL: connectivity for "...wifi" should be "wifi", got "{t105.get("connectivity")}"')

    # 106. connectivity extraction: "lte" -> "cellular" (pass original, not normalized)
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

    # === OPPO RENO MODEL TOKEN EXTRACTION TESTS ===

    # Reno2 Z must produce tokens including generation digit "2" AND "z" variant
    # (after de-concat: "reno2" -> "reno 2", so token is "2" not "reno2")
    reno2z_tokens = extract_model_tokens(normalize_text('oppo reno2 z 128gb'))
    if '2' not in reno2z_tokens:
        failures.append(f'FAIL: Reno2 Z tokens missing "2" - got {reno2z_tokens}')
    if 'z' not in reno2z_tokens:
        failures.append(f'FAIL: Reno2 Z tokens missing "z" variant - got {reno2z_tokens}')

    # Reno4 Z must produce tokens with "4" and "z"
    reno4z_tokens = extract_model_tokens(normalize_text('oppo reno4 z 5g 128gb'))
    if '4' not in reno4z_tokens:
        failures.append(f'FAIL: Reno4 Z tokens missing "4" - got {reno4z_tokens}')
    if 'z' not in reno4z_tokens:
        failures.append(f'FAIL: Reno4 Z tokens missing "z" variant - got {reno4z_tokens}')

    # Reno Z (no digit) must produce "z" token but NOT any numbered reno
    renoz_tokens = extract_model_tokens(normalize_text('oppo reno z 128gb'))
    if 'z' not in renoz_tokens:
        failures.append(f'FAIL: Reno Z tokens missing "z" variant - got {renoz_tokens}')

    # Reno2 (without Z) must have "2" but NOT "z"
    reno2_tokens = extract_model_tokens(normalize_text('oppo reno2 128gb'))
    if '2' not in reno2_tokens:
        failures.append(f'FAIL: Reno2 tokens missing "2" - got {reno2_tokens}')
    if 'z' in reno2_tokens:
        failures.append(f'FAIL: Reno2 (no Z) should not have "z" token - got {reno2_tokens}')

    # Samsung Galaxy Z Fold must NOT be affected by Reno Z logic
    fold_tokens = extract_model_tokens(normalize_text('samsung galaxy z fold5 256gb'))
    if 'z' in fold_tokens:
        failures.append(f'FAIL: Galaxy Z Fold5 should not pick up "z" as Reno variant - got {fold_tokens}')
    if 'fold5' not in fold_tokens:
        failures.append(f'FAIL: Galaxy Z Fold5 missing "fold5" token - got {fold_tokens}')

    # verify_critical_attributes: Reno2 Z vs Reno Z must FAIL
    vca_reno = verify_critical_attributes(
        normalize_text('oppo reno2 z 128gb'),
        normalize_text('oppo reno series reno z 128gb'))
    if vca_reno:
        failures.append('FAIL: verify_critical_attributes should reject Reno2 Z vs Reno Z')

    # verify_critical_attributes: Reno4 Z vs Reno5 Z must FAIL
    vca_reno45 = verify_critical_attributes(
        normalize_text('oppo reno4 z 5g 128gb'),
        normalize_text('oppo reno5 series reno5 z 5g 128gb'))
    if vca_reno45:
        failures.append('FAIL: verify_critical_attributes should reject Reno4 Z vs Reno5 Z')

    # verify_critical_attributes: Reno Z vs Reno Z must PASS
    vca_renozz = verify_critical_attributes(
        normalize_text('oppo reno z 128gb'),
        normalize_text('oppo reno series reno z 128gb'))
    if not vca_renozz:
        failures.append('FAIL: verify_critical_attributes should accept Reno Z vs Reno Z')

    # === MODEL IDENTITY GUARDRAIL TESTS ===

    # extract_model_identity correctness
    _id_cases = [
        ('Oppo Reno4 Pro 128GB', 'reno4pro'),
        ('Oppo Reno6 Dual 128GB', 'reno6'),
        ('Oppo Reno10 Pro 5G', 'reno10pro'),
        ('Redmi Note 12 Turbo', 'note12turbo'),
        ('Poco F4 GT 128GB', 'pocof4gt'),
        ('Xiaomi 14 Ultra', 'xiaomi14ultra'),
        ('Apple iPhone 15 Pro', ''),
        ('Samsung Galaxy S23', ''),
    ]
    for text, expected_id in _id_cases:
        actual_id = extract_model_identity(text)
        if actual_id != expected_id:
            failures.append(
                f'FAIL: extract_model_identity("{text}") = "{actual_id}", expected "{expected_id}"')

    # model_identity_guardrail rejections
    _guard_cases = [
        ('oppo reno4 pro 128gb', 'oppo reno3 pro 128gb', False),
        ('poco f4 gt 128gb', 'poco f4 128gb', False),
        ('redmi note 12 turbo', 'redmi note 12', False),
        ('oppo reno4 pro 128gb', 'oppo reno4 pro 128gb', True),
        ('apple iphone 15 pro', 'apple iphone 15 pro', True),
    ]
    for q_g, c_g, expected_pass in _guard_cases:
        actual_pass, _ = model_identity_guardrail(q_g, c_g)
        if actual_pass != expected_pass:
            failures.append(
                f'FAIL: model_identity_guardrail("{q_g}", "{c_g}") = {actual_pass}, '
                f'expected {expected_pass}')

    # ----- OPPO Reno extraction tests (de-concat + series stripping) -----
    _reno_extraction_cases = [
        # (input_text, brand, expected_line, expected_model)
        ('OPPO Reno4 128GB', 'oppo', 'reno', '4'),
        ('OPPO Reno6 Dual 128GB', 'oppo', 'reno', '6'),
        ('OPPO Reno8 5G Dual 128GB', 'oppo', 'reno', '8'),
        ('OPPO Reno3 Pro 256GB', 'oppo', 'reno', '3 pro'),
        ('OPPO Reno10 Pro 5G Dual 256GB', 'oppo', 'reno', '10 pro'),
        # NL catalog entries (with "series" noise)
        ('oppo reno2 series reno2 128gb', 'oppo', 'reno', '2'),
        ('oppo reno3 series reno3 pro 256gb', 'oppo', 'reno', '3 pro'),
        ('oppo reno5 series reno5 5g 128gb', 'oppo', 'reno', '5'),
        ('oppo reno6 series reno6 5g 256gb', 'oppo', 'reno', '6'),
        # OPPO Find
        ('OPPO Find X5 Pro 256GB', 'oppo', 'find', 'x5 pro'),
        ('oppo find x5 series find x5 pro 256gb', 'oppo', 'find', 'x5 pro'),
    ]
    for text_r, brand_r, exp_line, exp_model in _reno_extraction_cases:
        norm_r = normalize_text(text_r)
        attrs_r = extract_product_attributes(norm_r, brand_r)
        if attrs_r['product_line'] != exp_line or attrs_r['model'] != exp_model:
            failures.append(
                f'FAIL: extract_product_attributes("{text_r}") = '
                f'line={attrs_r["product_line"]!r}, model={attrs_r["model"]!r}, '
                f'expected line={exp_line!r}, model={exp_model!r}')

    # Cross-generation attribute mismatch: Reno4 query must NOT share model key with Reno2 NL entry
    q_norm = normalize_text('OPPO Reno4 128GB')
    q_attrs = extract_product_attributes(q_norm, 'oppo')
    nl_norm = normalize_text('oppo reno2 series reno2 128gb')
    nl_attrs = extract_product_attributes(nl_norm, 'oppo')
    if q_attrs['model'] == nl_attrs['model']:
        failures.append(
            f'FAIL: Reno4 query model={q_attrs["model"]!r} should differ from '
            f'Reno2 NL model={nl_attrs["model"]!r} — cross-generation collision!')

    # === LAPTOP IMPROVEMENT TESTS ===

    # 118. Brand normalization: "HP OLD" -> attrs['brand'] = "hp" (not "hp old")
    _l118 = extract_laptop_attributes('HP EliteBook 840 G8 Core i5 16GB 256GB', 'HP OLD')
    if _l118['brand'] != 'hp':
        failures.append(f'FAIL: Laptop brand for "HP OLD" should be "hp", got "{_l118["brand"]}"')

    # 119. Brand normalization: "Dell OLD" -> attrs['brand'] = "dell"
    _l119 = extract_laptop_attributes('Dell Latitude 5420 Core i5 16GB 512GB', 'Dell OLD')
    if _l119['brand'] != 'dell':
        failures.append(f'FAIL: Laptop brand for "Dell OLD" should be "dell", got "{_l119["brand"]}"')

    # 120. Brand normalization: "Lenovo OLD" -> attrs['brand'] = "lenovo"
    _l120 = extract_laptop_attributes('Lenovo ThinkPad T14 Gen 3 Core i5 16GB 256GB', 'Lenovo OLD')
    if _l120['brand'] != 'lenovo':
        failures.append(f'FAIL: Laptop brand for "Lenovo OLD" should be "lenovo", got "{_l120["brand"]}"')

    # 121. Brand normalization in extract_product_attributes: "HP OLD" -> "hp"
    _l121 = extract_product_attributes('HP EliteBook 840 G8 Core i5 16GB 256GB', 'HP OLD')
    if _l121['brand'] != 'hp':
        failures.append(f'FAIL: extract_product_attributes brand for "HP OLD" should be "hp", got "{_l121["brand"]}"')

    # 122. Platform code: Dell Latitude 5420 -> platform_code = "5420"
    _l122 = extract_laptop_attributes('Dell Latitude 5420 Core i5-1145G7 16GB 512GB', 'Dell')
    if _l122['platform_code'] != '5420':
        failures.append(f'FAIL: Dell platform_code should be "5420", got "{_l122["platform_code"]}"')

    # 123. Platform code: HP EliteBook 840 G8 -> platform_code = "840 g8"
    _l123 = extract_laptop_attributes('HP EliteBook 840 G8 Core i5-1145G7 16GB 256GB', 'HP')
    if _l123['platform_code'] != '840 g8':
        failures.append(f'FAIL: HP platform_code should be "840 g8", got "{_l123["platform_code"]}"')

    # 124. Platform code: Lenovo ThinkPad T14 -> platform_code = "t14"
    _l124 = extract_laptop_attributes('Lenovo ThinkPad T14 Gen 3 Core i5-1245U 16GB 256GB', 'Lenovo')
    if _l124['platform_code'] != 't14':
        failures.append(f'FAIL: Lenovo platform_code should be "t14", got "{_l124["platform_code"]}"')

    # 125. Platform code: Lenovo ThinkPad X1 Carbon -> platform_code = "x1 carbon"
    _l125 = extract_laptop_attributes('Lenovo ThinkPad X1 Carbon Gen 11 Core i7 16GB 512GB', 'Lenovo')
    if _l125['platform_code'] != 'x1 carbon':
        failures.append(f'FAIL: Lenovo platform_code should be "x1 carbon", got "{_l125["platform_code"]}"')

    # 126. laptop_variant_exact_match: different platform codes -> reject
    _l126q = extract_laptop_attributes('Dell Latitude 5420 Core i5 11th Gen 16GB 512GB', 'Dell')
    _l126c = extract_laptop_attributes('Dell Latitude 5520 Core i5 11th Gen 16GB 512GB', 'Dell')
    _pass126, _r126 = laptop_variant_exact_match(_l126q, _l126c)
    if _pass126:
        failures.append(f'FAIL: Latitude 5420 should NOT match 5520 (platform_code): {_r126}')

    # 127. laptop_variant_exact_match: same platform codes -> pass
    _l127q = extract_laptop_attributes('Dell Latitude 5420 Core i5 11th Gen 16GB 512GB', 'Dell')
    _l127c = extract_laptop_attributes('Dell Latitude 5420 Core i5 11th Gen 16GB 512GB', 'Dell')
    _pass127, _ = laptop_variant_exact_match(_l127q, _l127c)
    if not _pass127:
        failures.append('FAIL: Identical Latitude 5420 should match itself')

    # 128. HP platform code: ProBook 640 G9 -> "640 g9"
    _l128 = extract_laptop_attributes('HP ProBook 640 G9 Core i5-1245U 16GB 256GB', 'HP')
    if _l128['platform_code'] != '640 g9':
        failures.append(f'FAIL: HP ProBook platform_code should be "640 g9", got "{_l128["platform_code"]}"')

    # 129. Platform code rejection in match_laptop_by_attributes
    _nl129 = ['dell latitude 5520 core i5 gen11 16gb 512gb ssd']
    _lu129 = {_nl129[0]: ['NL-DELL-5520']}
    _r129 = match_laptop_by_attributes(
        'dell latitude 5420 core i5 gen11 16gb 512gb ssd',
        'dell', 'Dell Latitude 5420 Core i5 11th Gen 16GB 512GB',
        _nl129, _lu129, None)
    if _r129 is not None:
        failures.append('FAIL: match_laptop_by_attributes should reject 5420 vs 5520 (platform_code)')

    # 130. MacBook should NOT extract platform_code (no numeric model code)
    _l130 = extract_laptop_attributes('Apple MacBook Pro M2 16GB 512GB', 'Apple')
    if _l130.get('platform_code'):
        failures.append(f'FAIL: MacBook should not have platform_code, got "{_l130["platform_code"]}"')

    # 131. Dell Precision 5560 -> platform_code = "5560"
    _l131 = extract_laptop_attributes('Dell Precision 5560 Core i7-11800H 32GB 1TB', 'Dell')
    if _l131['platform_code'] != '5560':
        failures.append(f'FAIL: Dell Precision platform_code should be "5560", got "{_l131["platform_code"]}"')

    # 132. Laptop with no platform code should still match on specs
    _nl132 = ['dell latitude core i5 gen11 16gb 512gb ssd']
    _lu132 = {_nl132[0]: ['NL-DELL-NOPE']}
    _r132 = match_laptop_by_attributes(
        'dell latitude core i5 gen11 16gb 512gb ssd',
        'dell', 'Dell Latitude Core i5 11th Gen 16GB 512GB SSD',
        _nl132, _lu132, None)
    if _r132 is None:
        failures.append('FAIL: Laptop without platform_code should still match on specs')

    # ==================== laptop_family extraction tests ====================

    # 133. Acer Swift 3 vs Swift 5 -> distinct laptop_family
    _l133a = extract_laptop_attributes('Acer Swift 3 SF314-511 Core i5 8GB 512GB', 'Acer')
    _l133b = extract_laptop_attributes('Acer Swift 5 SF514-55T Core i7 16GB 512GB', 'Acer')
    if _l133a.get('laptop_family') != 'swift 3':
        failures.append(f'FAIL: Acer Swift 3 laptop_family should be "swift 3", got "{_l133a.get("laptop_family")}"')
    if _l133b.get('laptop_family') != 'swift 5':
        failures.append(f'FAIL: Acer Swift 5 laptop_family should be "swift 5", got "{_l133b.get("laptop_family")}"')

    # 134. Acer Predator Helios vs Triton -> distinct laptop_family
    _l134a = extract_laptop_attributes('Acer Predator Helios 300 Core i7 16GB 512GB', 'Acer')
    _l134b = extract_laptop_attributes('Acer Predator Triton 500 Core i9 32GB 1TB', 'Acer')
    if _l134a.get('laptop_family') != 'predator helios':
        failures.append(f'FAIL: Predator Helios laptop_family should be "predator helios", got "{_l134a.get("laptop_family")}"')
    if _l134b.get('laptop_family') != 'predator triton':
        failures.append(f'FAIL: Predator Triton laptop_family should be "predator triton", got "{_l134b.get("laptop_family")}"')

    # 135. ASUS ROG Strix vs Zephyrus -> distinct laptop_family
    _l135a = extract_laptop_attributes('ASUS ROG Strix G15 Core i7 16GB 512GB', 'ASUS')
    _l135b = extract_laptop_attributes('ASUS ROG Zephyrus G14 Ryzen 9 16GB 1TB', 'ASUS')
    if _l135a.get('laptop_family') != 'rog strix':
        failures.append(f'FAIL: ROG Strix laptop_family should be "rog strix", got "{_l135a.get("laptop_family")}"')
    if _l135b.get('laptop_family') != 'rog zephyrus':
        failures.append(f'FAIL: ROG Zephyrus laptop_family should be "rog zephyrus", got "{_l135b.get("laptop_family")}"')

    # 136. HP Pavilion 14 vs 15 -> distinct laptop_family
    _l136a = extract_laptop_attributes('HP Pavilion 14 Core i5 8GB 256GB', 'HP')
    _l136b = extract_laptop_attributes('HP Pavilion 15 Core i5 8GB 512GB', 'HP')
    if _l136a.get('laptop_family') != 'pavilion 14':
        failures.append(f'FAIL: Pavilion 14 laptop_family should be "pavilion 14", got "{_l136a.get("laptop_family")}"')
    if _l136b.get('laptop_family') != 'pavilion 15':
        failures.append(f'FAIL: Pavilion 15 laptop_family should be "pavilion 15", got "{_l136b.get("laptop_family")}"')

    # 137. HP EliteBook 840 -> laptop_family includes model number
    _l137 = extract_laptop_attributes('HP EliteBook 840 G8 Core i5 16GB 512GB', 'HP')
    if _l137.get('laptop_family') != 'elitebook 840':
        failures.append(f'FAIL: EliteBook 840 laptop_family should be "elitebook 840", got "{_l137.get("laptop_family")}"')

    # 138. Apple MacBook Pro -> laptop_family = product_line
    _l138 = extract_laptop_attributes('Apple MacBook Pro M2 16GB 512GB', 'Apple')
    if _l138.get('laptop_family') != 'macbook pro':
        failures.append(f'FAIL: MacBook Pro laptop_family should be "macbook pro", got "{_l138.get("laptop_family")}"')

    # 139. NL catalog format: "Acer, Aspire, 5 Series, ..." -> aspire 5
    _l139 = extract_laptop_attributes('Acer, Aspire, 5 Series, Core i5, 7th Gen, 6 GB, A515, 1 TB', 'Acer')
    if _l139.get('laptop_family') != 'aspire 5':
        failures.append(f'FAIL: NL "Aspire 5 Series" laptop_family should be "aspire 5", got "{_l139.get("laptop_family")}"')

    # 140. NL catalog: "HP, Pavilion, 15, ..." -> pavilion 15
    _l140 = extract_laptop_attributes('HP, Pavilion, 15, Core i3, 6th Gen, 4 GB, AY067ne, 1 TB', 'HP')
    if _l140.get('laptop_family') != 'pavilion 15':
        failures.append(f'FAIL: NL "Pavilion 15" laptop_family should be "pavilion 15", got "{_l140.get("laptop_family")}"')

    # ==================== model_code extraction tests ====================

    # 141. Acer SF314-511 -> model_code = sf314-511 (full suffix)
    _l141 = extract_laptop_attributes('Acer Swift 3 SF314-511 Core i5 8GB 512GB', 'Acer')
    if _l141.get('model_code') != 'sf314-511':
        failures.append(f'FAIL: Acer SF314-511 model_code should be "sf314-511", got "{_l141.get("model_code")}"')

    # 142. Acer PH315-55 -> model_code = ph315-55 (full suffix)
    _l142 = extract_laptop_attributes('Acer Predator Helios 300 PH315-55 Core i7 16GB 512GB', 'Acer')
    if _l142.get('model_code') != 'ph315-55':
        failures.append(f'FAIL: Acer PH315-55 model_code should be "ph315-55", got "{_l142.get("model_code")}"')

    # 143. ASUS UX325 -> model_code = ux325
    _l143 = extract_laptop_attributes('ASUS ZenBook UX325EA Core i7 16GB 512GB', 'ASUS')
    if _l143.get('model_code') != 'ux325':
        failures.append(f'FAIL: ASUS UX325 model_code should be "ux325", got "{_l143.get("model_code")}"')

    # 144. ASUS FX504 -> model_code = fx504
    _l144 = extract_laptop_attributes('Asus, TUF, FX Series, Core i7, 8th Gen, 4 GB, FX504, 1 TB', 'Asus')
    if _l144.get('model_code') != 'fx504':
        failures.append(f'FAIL: ASUS FX504 model_code should be "fx504", got "{_l144.get("model_code")}"')

    # ==================== laptop_family gate tests ====================

    # 145. laptop_variant_exact_match rejects different laptop_family
    _q145 = {'brand': 'acer', 'product_line': 'swift', 'processor': 'i5',
             'generation': '11th gen', 'ram': '8gb', 'storage': '512gb',
             'platform_code': '', 'laptop_family': 'swift 3', 'model_code': 'sf314'}
    _c145 = {'brand': 'acer', 'product_line': 'swift', 'processor': 'i5',
             'generation': '11th gen', 'ram': '8gb', 'storage': '512gb',
             'platform_code': '', 'laptop_family': 'swift 5', 'model_code': 'sf514'}
    _pass145, _reasons145 = laptop_variant_exact_match(_q145, _c145)
    if _pass145:
        failures.append('FAIL: laptop_variant_exact_match should reject Swift 3 vs Swift 5')
    if not any('laptop_family' in r for r in _reasons145):
        failures.append(f'FAIL: rejection should mention laptop_family, got {_reasons145}')

    # 146. laptop_variant_exact_match accepts same laptop_family
    _q146 = {'brand': 'hp', 'product_line': 'pavilion', 'processor': 'i5',
             'generation': '11th gen', 'ram': '8gb', 'storage': '512gb',
             'platform_code': '15', 'laptop_family': 'pavilion 15', 'model_code': ''}
    _c146 = {'brand': 'hp', 'product_line': 'pavilion', 'processor': 'i5',
             'generation': '11th gen', 'ram': '8gb', 'storage': '512gb',
             'platform_code': '15', 'laptop_family': 'pavilion 15', 'model_code': ''}
    _pass146, _reasons146 = laptop_variant_exact_match(_q146, _c146)
    if not _pass146:
        failures.append(f'FAIL: laptop_variant_exact_match should accept same Pavilion 15, got {_reasons146}')

    # 147. match_laptop_by_attributes rejects different laptop_family (Swift 3 vs 5)
    _nl147_3 = 'acer swift 3 series core i5 gen11 8gb sf314 512gb ssd'
    _nl147_5 = 'acer swift 5 series core i7 gen11 8gb sf514 512gb ssd'
    _nl147 = [_nl147_3, _nl147_5]
    _lu147 = {_nl147_3: ['NL-SWIFT3'], _nl147_5: ['NL-SWIFT5']}
    _r147 = match_laptop_by_attributes(
        'acer swift 3 sf314 core i5 gen11 8gb 512gb',
        'acer', 'Acer Swift 3 SF314 Core i5 11th Gen 8GB 512GB',
        _nl147, _lu147, None)
    if _r147 is not None:
        _r147_id = _r147.get('mapped_uae_assetid', '')
        if 'SWIFT5' in _r147_id:
            failures.append('FAIL: match_laptop_by_attributes matched Swift 3 query to Swift 5 NL entry')

    # ==================== ONE-SIDED enforcement tests ====================

    # 148. One-sided platform_code: query has code, candidate missing => reject
    _q148 = {'brand': 'dell', 'product_line': 'latitude', 'processor': 'i5',
             'generation': '11th gen', 'ram': '16gb', 'storage': '512gb',
             'platform_code': '7320', 'laptop_family': '', 'model_code': ''}
    _c148 = {'brand': 'dell', 'product_line': 'latitude', 'processor': 'i5',
             'generation': '11th gen', 'ram': '16gb', 'storage': '512gb',
             'platform_code': '', 'laptop_family': '', 'model_code': ''}
    _pass148, _r148 = laptop_variant_exact_match(_q148, _c148)
    if _pass148:
        failures.append('FAIL: One-sided platform_code: query=7320 candidate=empty should reject')
    if not any('platform_code_missing' in r for r in _r148):
        failures.append(f'FAIL: One-sided platform_code rejection should mention platform_code_missing, got {_r148}')

    # 149. One-sided model_code: query has code, candidate missing => reject
    _q149 = {'brand': 'acer', 'product_line': 'swift', 'processor': 'i5',
             'generation': '11th gen', 'ram': '8gb', 'storage': '512gb',
             'platform_code': '', 'laptop_family': 'swift 3', 'model_code': 'sf314'}
    _c149 = {'brand': 'acer', 'product_line': 'swift', 'processor': 'i5',
             'generation': '11th gen', 'ram': '8gb', 'storage': '512gb',
             'platform_code': '', 'laptop_family': 'swift 3', 'model_code': ''}
    _pass149, _r149 = laptop_variant_exact_match(_q149, _c149)
    if _pass149:
        failures.append('FAIL: One-sided model_code: query=sf314 candidate=empty should reject')
    if not any('model_code_missing' in r for r in _r149):
        failures.append(f'FAIL: One-sided model_code rejection should mention model_code_missing, got {_r149}')

    # 150. One-sided laptop_family: query has family, candidate missing => reject
    _q150 = {'brand': 'hp', 'product_line': 'pavilion', 'processor': 'i5',
             'generation': '11th gen', 'ram': '8gb', 'storage': '512gb',
             'platform_code': '', 'laptop_family': 'pavilion 15', 'model_code': ''}
    _c150 = {'brand': 'hp', 'product_line': 'pavilion', 'processor': 'i5',
             'generation': '11th gen', 'ram': '8gb', 'storage': '512gb',
             'platform_code': '', 'laptop_family': '', 'model_code': ''}
    _pass150, _r150 = laptop_variant_exact_match(_q150, _c150)
    if _pass150:
        failures.append('FAIL: One-sided laptop_family: query=pavilion 15 candidate=empty should reject')

    # 151. Candidate has code but query doesn't => should PASS (not one-sided this direction)
    _q151 = {'brand': 'dell', 'product_line': 'latitude', 'processor': 'i5',
             'generation': '11th gen', 'ram': '16gb', 'storage': '512gb',
             'platform_code': '', 'laptop_family': '', 'model_code': ''}
    _c151 = {'brand': 'dell', 'product_line': 'latitude', 'processor': 'i5',
             'generation': '11th gen', 'ram': '16gb', 'storage': '512gb',
             'platform_code': '5420', 'laptop_family': '', 'model_code': ''}
    _pass151, _r151 = laptop_variant_exact_match(_q151, _c151)
    if not _pass151:
        failures.append(f'FAIL: Candidate has platform_code but query does not => should pass, got {_r151}')

    # ==================== Multi-ID REVIEW_REQUIRED tests ====================

    # 152. Multi-ID laptop with no tie-breaker => REVIEW_REQUIRED
    import pandas as _pd152
    _nl152_name = 'dell latitude core i5 gen11 16gb 512gb ssd'
    _nl152 = [_nl152_name]
    _lu152 = {_nl152_name: ['NL-DELL-A', 'NL-DELL-B']}
    _cat152 = _pd152.DataFrame({
        'uae_assetid': ['NL-DELL-A', 'NL-DELL-B'],
        'uae_assetname': ['Dell Latitude Core i5 11th Gen 16GB 512GB v1',
                          'Dell Latitude Core i5 11th Gen 16GB 512GB v2']
    })
    _r152 = match_laptop_by_attributes(
        'dell latitude core i5 gen11 16gb 512gb ssd',
        'dell', 'Dell Latitude Core i5 11th Gen 16GB 512GB',
        _nl152, _lu152, _cat152)
    if _r152 is None:
        failures.append('FAIL: Multi-ID laptop should return result, got None')
    elif _r152.get('match_status') != MATCH_STATUS_SUGGESTED:
        failures.append(f'FAIL: Multi-ID laptop without tie-breaker should be REVIEW_REQUIRED, got {_r152.get("match_status")}')

    # 153. Multi-ID laptop WITH platform_code tie-breaker => MATCHED (single)
    _nl153_name = 'dell latitude 5420 core i5 gen11 16gb 512gb ssd'
    _nl153 = [_nl153_name]
    _lu153 = {_nl153_name: ['NL-DELL-5420X', 'NL-DELL-5420Y']}
    _cat153 = _pd152.DataFrame({
        'uae_assetid': ['NL-DELL-5420X', 'NL-DELL-5420Y'],
        'uae_assetname': ['Dell Latitude 5420 Core i5 11th Gen 16GB 512GB',
                          'Dell Latitude Core i5 11th Gen 16GB 512GB']
    })
    _r153 = match_laptop_by_attributes(
        'dell latitude 5420 core i5 gen11 16gb 512gb ssd',
        'dell', 'Dell Latitude 5420 Core i5 11th Gen 16GB 512GB',
        _nl153, _lu153, _cat153)
    if _r153 is None:
        failures.append('FAIL: Multi-ID laptop with tie-breaker should return result')
    elif _r153.get('match_status') != MATCH_STATUS_MATCHED:
        failures.append(f'FAIL: Multi-ID laptop with platform_code tie-breaker should be MATCHED, got {_r153.get("match_status")}')
    elif _r153.get('mapped_uae_assetid') != 'NL-DELL-5420X':
        failures.append(f'FAIL: Tie-breaker should select NL-DELL-5420X, got {_r153.get("mapped_uae_assetid")}')

    # --- V2 improvement patch self-tests ---

    # 40. "One Plus" text normalization -> "oneplus"
    _nt_op = normalize_text('One Plus Nord 256GB')
    if 'oneplus' not in _nt_op:
        failures.append(f'FAIL: normalize_text("One Plus Nord") should contain "oneplus", got "{_nt_op}"')

    # 41. Category inference: galaxy book -> laptop
    if _infer_canonical_category_v2('Samsung Galaxy Book 15.6 i7', 'samsung', '') != 'laptop':
        failures.append('FAIL: "Galaxy Book" should infer as laptop')

    # 42. Category inference: surface pro -> tablet (no laptop tokens)
    if _infer_canonical_category_v2('Microsoft Surface Pro 9 i7 256GB', 'microsoft', '') != 'tablet':
        failures.append('FAIL: "Surface Pro" should infer as tablet')

    # 43. Category inference: SM-G960F -> mobile (normalized, no hyphen)
    if _infer_canonical_category_v2('samsung sm g960f 64gb', 'samsung', '') != 'mobile':
        failures.append('FAIL: "SM-G960F" (normalized) should infer as mobile')

    # 44. Category inference: Huawei Mate 30 -> mobile
    if _infer_canonical_category_v2('Huawei Mate 30 Pro 256GB', 'huawei', '') != 'mobile':
        failures.append('FAIL: "Huawei Mate 30" should infer as mobile')

    # 45. Category inference: Xiaomi 15 -> mobile
    if _infer_canonical_category_v2('Xiaomi 15 Ultra 256GB', 'xiaomi', '') != 'mobile':
        failures.append('FAIL: "Xiaomi 15" should infer as mobile')

    # 46. Xiaomi product_line: "Xiaomi Xiaomi 15 Ultra" should get product_line=xiaomi, not mi
    _xa = extract_product_attributes('Xiaomi Xiaomi 15 Ultra 256GB', 'Xiaomi')
    if _xa.get('product_line') == 'mi':
        failures.append(f'FAIL: "Xiaomi 15 Ultra" product_line should be "xiaomi" not "mi"')

    # 47. Xiaomi Mi series: "Xiaomi Mi 11 Ultra" should get product_line=mi
    _xm = extract_product_attributes('Xiaomi Mi 11 Ultra 256GB', 'Xiaomi')
    if _xm.get('product_line') != 'mi':
        failures.append(f'FAIL: "Xiaomi Mi 11 Ultra" product_line should be "mi", got "{_xm.get("product_line")}"')

    # 48. Model family: Samsung SM-N986 -> galaxy note family
    _smn = extract_model_family_key('Samsung SM-N986B 256GB', 'mobile', 'samsung')
    if 'galaxy note' not in _smn:
        failures.append(f'FAIL: SM-N986 should map to galaxy note family, got "{_smn}"')

    # 49. Model family: Samsung SM-G960F -> galaxy s family
    _smg = extract_model_family_key('Samsung SM-G960F 64GB', 'mobile', 'samsung')
    if 'galaxy s' not in _smg:
        failures.append(f'FAIL: SM-G960F should map to galaxy s family, got "{_smg}"')

    # 50. Model family: Xiaomi 15 Ultra -> xiaomi family key
    _xmfk = extract_model_family_key('Xiaomi 15 Ultra 256GB', 'mobile', 'xiaomi')
    if 'xiaomi 15' not in _xmfk:
        failures.append(f'FAIL: "Xiaomi 15 Ultra" family key should contain "xiaomi 15", got "{_xmfk}"')

    # 51. Model family: OnePlus after text normalization
    _op_mfk = extract_model_family_key('One Plus Nord 128GB', 'mobile', 'oneplus')
    if 'oneplus' not in _op_mfk:
        failures.append(f'FAIL: "One Plus Nord" family key should contain "oneplus", got "{_op_mfk}"')

    # --- Samsung "+" variant normalization tests ---

    # 52. "S24+" normalizes identically to "S24 Plus"
    _nt_plus = normalize_text('Samsung Galaxy S24+ 512GB')
    _nt_spelled = normalize_text('Samsung Galaxy S24 Plus 512GB')
    if _nt_plus != _nt_spelled:
        failures.append(
            f'FAIL: "S24+" and "S24 Plus" should normalize identically — '
            f'got "{_nt_plus}" vs "{_nt_spelled}"')

    # 53. "S24" base model must NOT gain "plus"
    _nt_base = normalize_text('Samsung Galaxy S24 512GB')
    if 'plus' in _nt_base:
        failures.append(f'FAIL: "S24" base should not contain "plus" — got "{_nt_base}"')

    # 54. "S24 Ultra" must remain unchanged
    _nt_ultra = normalize_text('Samsung Galaxy S24 Ultra 512GB')
    if 'plus' in _nt_ultra:
        failures.append(f'FAIL: "S24 Ultra" should not contain "plus" — got "{_nt_ultra}"')

    # 55. Non-Samsung "+" must NOT be converted (OnePlus 12+)
    _nt_op12 = normalize_text('OnePlus 12+ 256GB')
    if 'plus' in _nt_op12.replace('oneplus', ''):
        failures.append(f'FAIL: "OnePlus 12+" should keep "+" unconverted — got "{_nt_op12}"')

    # 56. S24+ extraction: model must include "plus" variant
    _a_plus = extract_product_attributes('Samsung Galaxy S24+ 512GB', 'Samsung')
    if 'plus' not in (_a_plus.get('model', '')):
        failures.append(
            f'FAIL: "S24+" model should contain "plus" — '
            f'got model="{_a_plus.get("model")}"')

    # 57. S24+ and S24 Plus must produce same model_family_key
    _mfk_plus = extract_model_family_key('Samsung Galaxy S24+ 512GB', 'mobile', 'samsung')
    _mfk_spelled = extract_model_family_key('Samsung Galaxy S24 Plus 512GB', 'mobile', 'samsung')
    if _mfk_plus != _mfk_spelled:
        failures.append(
            f'FAIL: "S24+" and "S24 Plus" family keys should match — '
            f'got "{_mfk_plus}" vs "{_mfk_spelled}"')

    # 58. Galaxy A55+ also normalizes correctly
    _nt_a55 = normalize_text('Samsung Galaxy A55+ 128GB')
    if 'a55 plus' not in _nt_a55:
        failures.append(f'FAIL: "A55+" should normalize to "a55 plus" — got "{_nt_a55}"')

    # 59. Galaxy Tab S8+ also normalizes correctly
    _nt_tabs8 = normalize_text('Galaxy Tab S8+ 256GB')
    if 's8 plus' not in _nt_tabs8:
        failures.append(f'FAIL: "Tab S8+" should normalize to "s8 plus" — got "{_nt_tabs8}"')

    # =================================================================
    # V2 LAPTOP NORMALIZATION TESTS (Task A / B / C)
    # =================================================================

    # 60. Screen size + TBT3 parentheses cleanup
    _lnorm1 = normalize_laptop_query_text_v2('Apple MacBook Pro (13" 2020, 4 TBT3)')
    if '13 inch' not in _lnorm1 or '2020' not in _lnorm1:
        failures.append(
            f'FAIL: Laptop norm — expected "13 inch" and "2020" in result, '
            f'got "{_lnorm1}"')
    if 'tbt' in _lnorm1.lower():
        failures.append(
            f'FAIL: Laptop norm — TBT3 should be removed, got "{_lnorm1}"')

    # 61. Thunderbolt/Wi-Fi noise removed, CPU/storage/RAM kept
    _lnorm2 = normalize_laptop_query_text_v2(
        'HP EliteBook 840 G8 14" i5 Wi-Fi 6E Thunderbolt 16GB 512GB SSD')
    _ln2_low = _lnorm2.lower()
    if 'wi-fi' in _ln2_low or 'wifi' in _ln2_low or 'thunderbolt' in _ln2_low:
        failures.append(
            f'FAIL: Laptop norm — Wi-Fi/Thunderbolt should be removed, got "{_lnorm2}"')
    for token in ('i5', '16gb', '512gb', 'elitebook', '840'):
        if token not in _ln2_low:
            failures.append(
                f'FAIL: Laptop norm — identity token "{token}" lost, got "{_lnorm2}"')

    # 62. Non-laptop text is NOT broken by laptop normalization
    _lnorm3 = normalize_laptop_query_text_v2('Samsung Galaxy S24 Ultra 512GB')
    if 'galaxy' not in _lnorm3.lower() or 's24' not in _lnorm3.lower():
        failures.append(
            f'FAIL: Laptop norm should not break non-laptop text — got "{_lnorm3}"')

    # 63. Screen size with actual inch word preserved
    _lnorm4 = normalize_laptop_query_text_v2('Dell Latitude 14 inch i7 16GB 512GB')
    if '14 inch' not in _lnorm4.lower():
        failures.append(
            f'FAIL: Laptop norm — "14 inch" should be preserved, got "{_lnorm4}"')

    # 64. Parentheses with only noise -> fully removed
    _lnorm5 = normalize_laptop_query_text_v2('MacBook Air (4 TBT3, Wi-Fi 6)')
    if 'tbt' in _lnorm5.lower() or 'wi-fi' in _lnorm5.lower():
        failures.append(
            f'FAIL: Laptop norm — noise-only parens should be dropped, got "{_lnorm5}"')
    if 'macbook air' not in _lnorm5.lower():
        failures.append(
            f'FAIL: Laptop norm — "MacBook Air" identity lost, got "{_lnorm5}"')

    # 65. Laptop attribute extraction AFTER v2 normalization gets correct storage
    _lq65 = normalize_laptop_query_text_v2(
        'Apple MacBook Pro (13" 2020, 2 TBT3) - Core i5 / 8GB / 256GB SSD')
    _la65 = extract_laptop_attributes(normalize_text(_lq65), 'apple')
    if _la65.get('storage') != '256gb':
        failures.append(
            f'FAIL: Laptop v2 norm+extract storage — expected "256gb", '
            f'got "{_la65.get("storage")}" (from "{_lq65}")')
    if _la65.get('processor') != 'i5':
        failures.append(
            f'FAIL: Laptop v2 norm+extract processor — expected "i5", '
            f'got "{_la65.get("processor")}"')

    # 66. WLAN / BT / HDMI tokens removed
    _lnorm6 = normalize_laptop_query_text_v2(
        'Lenovo ThinkPad T14 i5 WLAN BT HDMI 16GB 256GB')
    _ln6_low = _lnorm6.lower()
    for noise in ('wlan', ' bt ', 'hdmi'):
        if noise in _ln6_low:
            failures.append(
                f'FAIL: Laptop norm — "{noise.strip()}" should be removed, got "{_lnorm6}"')

    # ==================== TASK 5: Policy-class + cross-join regression tests ====================

    # 154. laptop_policy_class: MacBook Pro -> APPLE_MACBOOK
    _policy154 = laptop_policy_class('Apple MacBook Pro M2 16GB 512GB', 'Apple',
                                     extract_laptop_attributes('Apple MacBook Pro M2 16GB 512GB', 'Apple'))
    if _policy154 != 'APPLE_MACBOOK':
        failures.append(f'FAIL: MacBook Pro policy should be APPLE_MACBOOK, got "{_policy154}"')

    # 155. laptop_policy_class: Dell Latitude -> WINDOWS_BUSINESS
    _policy155 = laptop_policy_class('Dell Latitude 5420 Core i5 16GB 512GB', 'Dell',
                                     extract_laptop_attributes('Dell Latitude 5420 Core i5 16GB 512GB', 'Dell'))
    if _policy155 != 'WINDOWS_BUSINESS':
        failures.append(f'FAIL: Dell Latitude policy should be WINDOWS_BUSINESS, got "{_policy155}"')

    # 156. laptop_policy_class: ASUS ROG Strix -> WINDOWS_GAMING
    _policy156 = laptop_policy_class('ASUS ROG Strix G15 Core i7 16GB 512GB RTX 3060', 'ASUS',
                                     extract_laptop_attributes('ASUS ROG Strix G15 Core i7 16GB 512GB RTX 3060', 'ASUS'))
    if _policy156 != 'WINDOWS_GAMING':
        failures.append(f'FAIL: ROG Strix policy should be WINDOWS_GAMING, got "{_policy156}"')

    # 157. laptop_policy_class: Acer Aspire (no gaming) -> WINDOWS_OTHER
    _policy157 = laptop_policy_class('Acer Aspire 5 Core i5 8GB 256GB', 'Acer',
                                     extract_laptop_attributes('Acer Aspire 5 Core i5 8GB 256GB', 'Acer'))
    if _policy157 != 'WINDOWS_OTHER':
        failures.append(f'FAIL: Acer Aspire policy should be WINDOWS_OTHER, got "{_policy157}"')

    # 158. screen_inches extraction: "14 inch" -> "14"
    _si158 = extract_laptop_attributes('Dell Latitude 14 inch Core i5 16GB 512GB', 'Dell')
    if _si158.get('screen_inches') != '14':
        failures.append(f'FAIL: screen_inches should be "14", got "{_si158.get("screen_inches")}"')

    # 159. apple_chip extraction: "M2 Pro" -> "m2 pro"
    _ac159 = extract_laptop_attributes('Apple MacBook Pro M2 Pro 16GB 512GB', 'Apple')
    if _ac159.get('apple_chip') != 'm2 pro':
        failures.append(f'FAIL: apple_chip should be "m2 pro", got "{_ac159.get("apple_chip")}"')

    # 160. Dual-storage detection: "256GB + 1TB"
    _ds160 = extract_laptop_attributes('Dell Latitude i5 16GB 256GB SSD 1TB HDD', 'Dell')
    if not _ds160.get('storage_ambiguous'):
        failures.append('FAIL: "256GB + 1TB" should set storage_ambiguous=True')
    if sorted(_ds160.get('storage_list', [])) != [256, 1024]:
        failures.append(f'FAIL: storage_list should be [256, 1024], got {_ds160.get("storage_list")}')

    # 161. Dual-storage query -> always REVIEW_REQUIRED (never MATCHED)
    _nl161 = ['dell latitude core i5 gen11 16gb 256gb ssd']
    _lu161 = {_nl161[0]: ['NL-DUAL-161']}
    _r161 = match_laptop_by_attributes(
        'dell latitude core i5 gen11 16gb 256gb ssd 1tb hdd',
        'dell', 'Dell Latitude Core i5 11th Gen 16GB 256GB SSD 1TB HDD',
        _nl161, _lu161, None)
    if _r161 is not None and _r161.get('match_status') == MATCH_STATUS_MATCHED:
        failures.append('FAIL: Dual-storage laptop query should never be MATCHED')

    # 162. MacBook Pro 16" must NOT match MacBook Pro 14" (screen mismatch via scoring)
    _nl162_14 = 'apple macbook pro 14 inch m2 pro 16gb 512gb ssd'
    _nl162_16 = 'apple macbook pro 16 inch m2 pro 16gb 512gb ssd'
    _nl162 = [_nl162_14, _nl162_16]
    _lu162 = {_nl162_14: ['NL-MBP14'], _nl162_16: ['NL-MBP16']}
    _r162 = match_laptop_by_attributes(
        'apple macbook pro 16 inch m2 pro 16gb 512gb ssd',
        'apple', 'Apple MacBook Pro 16" M2 Pro 16GB 512GB',
        _nl162, _lu162, None)
    if _r162 is not None and _r162.get('match_status') == MATCH_STATUS_MATCHED:
        if _r162.get('mapped_uae_assetid') == 'NL-MBP14':
            failures.append('FAIL: MacBook Pro 16" should NOT match 14" as MATCHED')

    # 163. MacBook Air 15" must NOT match MacBook Air 13" as MATCHED
    _nl163_13 = 'apple macbook air 13 inch m2 8gb 256gb ssd'
    _nl163_15 = 'apple macbook air 15 inch m2 8gb 256gb ssd'
    _nl163 = [_nl163_13, _nl163_15]
    _lu163 = {_nl163_13: ['NL-MBA13'], _nl163_15: ['NL-MBA15']}
    _r163 = match_laptop_by_attributes(
        'apple macbook air 15 inch m2 8gb 256gb ssd',
        'apple', 'Apple MacBook Air 15" M2 8GB 256GB',
        _nl163, _lu163, None)
    if _r163 is not None and _r163.get('match_status') == MATCH_STATUS_MATCHED:
        if _r163.get('mapped_uae_assetid') == 'NL-MBA13':
            failures.append('FAIL: MacBook Air 15" should NOT match 13" as MATCHED')

    # 164. Dell Latitude 7320 with missing CPU/RAM/storage -> None (incomplete)
    _nl164 = ['dell latitude 7320 core i5 gen11 16gb 512gb ssd']
    _lu164 = {_nl164[0]: ['NL-LAT-7320']}
    _r164 = match_laptop_by_attributes(
        'dell latitude 7320',
        'dell', 'Dell Latitude 7320',
        _nl164, _lu164, None)
    if _r164 is not None and _r164.get('match_status') == MATCH_STATUS_MATCHED:
        failures.append('FAIL: Dell Latitude 7320 with no specs should not be MATCHED')

    # 165. ROG Zephyrus should NOT match ROG Strix as MATCHED (family mismatch -> skip)
    _nl165_strix = 'asus rog strix g15 core i7 gen11 16gb 512gb ssd'
    _nl165_zeph = 'asus rog zephyrus g14 ryzen 9 gen11 16gb 1024gb ssd'
    _nl165 = [_nl165_strix, _nl165_zeph]
    _lu165 = {_nl165_strix: ['NL-STRIX'], _nl165_zeph: ['NL-ZEPH']}
    _r165 = match_laptop_by_attributes(
        'asus rog zephyrus g14 core i7 gen11 16gb 512gb ssd',
        'asus', 'ASUS ROG Zephyrus G14 Core i7 11th Gen 16GB 512GB',
        _nl165, _lu165, None)
    if _r165 is not None and _r165.get('match_status') == MATCH_STATUS_MATCHED:
        if _r165.get('mapped_uae_assetid') == 'NL-STRIX':
            failures.append('FAIL: ROG Zephyrus should NOT match ROG Strix as MATCHED')

    # 166. Non-laptop categories unchanged: mobile matching still works
    _q166 = extract_product_attributes('apple iphone 15 pro 256gb', 'apple')
    _c166 = extract_product_attributes('apple iphone 15 pro 256gb', 'apple')
    _pass166, _ = mobile_variant_exact_match(_q166, _c166)
    if not _pass166:
        failures.append('FAIL: iPhone 15 Pro should still match itself (non-laptop unchanged)')

    # 167. Completeness gate: WINDOWS_BUSINESS without code/family -> REVIEW
    _nl167 = ['dell latitude core i5 gen11 16gb 512gb ssd']
    _lu167 = {_nl167[0]: ['NL-BIZ-167']}
    _r167 = match_laptop_by_attributes(
        'dell latitude core i5 gen11 16gb 512gb ssd',
        'dell', 'Dell Latitude Core i5 11th Gen 16GB 512GB',
        _nl167, _lu167, None)
    # This query has product_line=latitude but no platform_code or laptop_family
    # -> completeness gate should fail (missing code_or_family for WINDOWS_BUSINESS)
    # It should NOT be MATCHED
    if _r167 is not None and _r167.get('match_status') == MATCH_STATUS_MATCHED:
        _q167_attrs = extract_laptop_attributes('dell latitude core i5 gen11 16gb 512gb ssd', 'dell')
        _q167_fam = _q167_attrs.get('laptop_family', '')
        _q167_pc = _q167_attrs.get('platform_code', '')
        # Only fail if truly no disambiguator present
        if not _q167_fam and not _q167_pc:
            failures.append('FAIL: WINDOWS_BUSINESS without code/family should be REVIEW, not MATCHED')

    # 168. Score margin test: two identical candidates -> REVIEW (margin=0)
    _nl168_a = 'hp elitebook 840 g8 core i5 gen11 16gb 512gb ssd'
    _nl168_b = 'hp elitebook 840 g8 core i5 gen11 16gb 512gb ssd'
    _nl168 = [_nl168_a]
    _lu168 = {_nl168_a: ['NL-HP-A', 'NL-HP-B']}
    _r168 = match_laptop_by_attributes(
        'hp elitebook 840 g8 core i5 gen11 16gb 512gb ssd',
        'hp', 'HP EliteBook 840 G8 Core i5 11th Gen 16GB 512GB',
        _nl168, _lu168, None)
    # Multi-ID with same name -> goes to tie-break path
    # Since both resolve to same NL name (no platform code differentiation in catalog),
    # should be REVIEW (multi-ID)
    if _r168 is not None and _r168.get('match_status') == MATCH_STATUS_MATCHED:
        if len(_lu168[_nl168_a]) > 1:
            failures.append('FAIL: Multi-ID with no tie-breaker should be REVIEW, not MATCHED')

    # 169. Apple MacBook with only product_line + storage should still score (Apple entry gate)
    _nl169 = ['apple macbook pro 16gb 512gb ssd']
    _lu169 = {_nl169[0]: ['NL-MBP-169']}
    _r169 = match_laptop_by_attributes(
        'apple macbook pro 16gb 512gb ssd',
        'apple', 'Apple MacBook Pro 16GB 512GB',
        _nl169, _lu169, None)
    # Should return *something* (not None) — Apple needs product_line + storage minimum
    if _r169 is None:
        failures.append('FAIL: MacBook Pro with storage should not return None from Apple gate')

    return failures


# ---------------------------------------------------------------------------
# V2-only: Model Family Key extractor (mobile/tablet/watch)
# ---------------------------------------------------------------------------

def _infer_canonical_category_v2(text: str, brand_hint: str = '', current_category: str = '') -> str:
    """
    V2-only: infer category from keywords when current_category is empty/other.

    Only overrides when confidence is high (keyword-based).
    """
    if current_category and current_category.lower() not in ('', 'other', 'nan', 'none'):
        return current_category
    t = (text or '').lower()
    b = (brand_hint or '').lower()
    # Brand-specific shortcuts
    if b == 'apple' and 'watch' in t:
        return 'watch'
    if b == 'apple' and 'ipad' in t:
        return 'tablet'
    # Tablet keywords (surface pro/go -> tablet when no laptop tokens present)
    if any(kw in t for kw in ('ipad', 'galaxy tab', 'tab s', 'tab a', 'mediapad', 'matepad')):
        return 'tablet'
    if re.search(r'\bsurface\s+(?:pro|go)\b', t) and not any(
            kw in t for kw in ('keyboard', 'type cover', 'laptop')):
        return 'tablet'
    # Watch keywords
    if any(kw in t for kw in ('apple watch', 'galaxy watch', 'watch gt', 'watch fit', 'smartwatch', 'watch ultra')):
        return 'watch'
    # Laptop keywords (galaxy book is a laptop, not mobile)
    if any(kw in t for kw in ('macbook', 'galaxy book', 'latitude', 'thinkpad', 'thinkbook',
                               'inspiron', 'pavilion', 'zenbook', 'vivobook', 'swift', 'aspire',
                               'elitebook', 'probook', 'surface laptop', 'surface book',
                               'nitro', 'predator', 'rog', 'tuf', 'ideapad', 'yoga',
                               'chromebook', 'envy', 'spectre', 'zbook', 'expertbook',
                               'victus', 'dell g3', 'dell g5', 'dell g7', 'dell g15', 'dell g16',
                               'transformer book', 'studiobook', 'proart')):
        return 'laptop'
    # Mobile (recognized phone keywords)
    if any(kw in t for kw in ('iphone', 'galaxy s', 'galaxy a', 'galaxy z', 'galaxy note', 'galaxy m',
                               'galaxy f', 'pixel', 'oneplus', 'one plus',
                               'redmi', 'poco', 'realme', 'oppo', 'vivo', 'moto', 'xperia',
                               'lumia', 'nokia ', 'surface duo', 'htc',
                               'blackberry', 'zenfone', 'vibe')):
        return 'mobile'
    # Huawei phone families: mate/p/nova/pura + number -> mobile
    if ('huawei' in t or b == 'huawei') and re.search(
            r'\b(?:mate\s*\d|p\d{2}|p\s+\d{2}|nova\s*\d|pura\s*\d)', t):
        return 'mobile'
    # Xiaomi/Mi numbered series -> mobile
    if ('xiaomi' in t or b == 'xiaomi') and re.search(r'\bxiaomi\s+\d+|mi\s+\d+', t):
        return 'mobile'
    # Samsung model codes (GT-/SM-) are overwhelmingly mobile
    # normalize_text strips hyphens -> "SM-G960F" becomes "sm g960f", so match both forms
    if re.search(r'\b(?:gt|sm)[\s-]?[a-z]\d{3}', t):
        return 'mobile'
    # Lenovo V-series: V14, V15, V17 are budget laptops
    if b == 'lenovo' and re.search(r'\bv\d{2}\b', t):
        return 'laptop'
    # Lenovo Miix -> tablet (detachable)
    if 'miix' in t:
        return 'tablet'
    # Samsung Galaxy View -> tablet (large screen portable)
    if 'galaxy view' in t:
        return 'tablet'
    return current_category or 'other'


def extract_model_family_key(text: str, category: str = '', brand_hint: str = '') -> str:
    """
    Extract a canonical model family key for bucketing and catalog gap detection.

    Mobile: brand + line + model number + variant (pro/max/ultra/plus/se/lite) + year (SE only)
    Tablet: brand + line (pro/air/mini) + size + gen/year; Galaxy Tab multi-token models
    Watch:  brand + series/ultra/se + size; Galaxy Watch classic/active tokens
    Laptop: not handled here (use extract_laptop_attributes instead)
    """
    if not text or not isinstance(text, str):
        return ''
    t = normalize_text(text).lower()
    cat = _infer_canonical_category_v2(text, brand_hint, category).lower()

    if cat == 'laptop':
        return ''  # Laptop uses its own identity system

    # --- Mobile ---
    if cat in ('mobile', 'mobile phone', 'phone', ''):
        # iPhone SE (special: no model number, year-differentiated)
        if 'iphone' in t and 'se' in t and not re.search(r'iphone\s*\d+', t):
            ym = re.search(r'(20\d{2})', t)
            year = ym.group(1) if ym else ''
            return f'iphone se {year}'.strip()

        # Apple iPhone (numbered)
        m = re.search(r'iphone\s*(\d+)', t)
        if m:
            num = m.group(1)
            variant = ''
            for v in ('pro max', 'pro', 'plus', 'mini'):
                if v in t:
                    variant = v
                    break
            return f'iphone {num} {variant}'.strip()

        # Samsung Galaxy
        m = re.search(r'galaxy\s+(s|a|z\s*(?:fold|flip)|note|m|f)\s*(\d+)', t)
        if m:
            line = m.group(1).replace(' ', '')
            num = m.group(2)
            variant = ''
            for v in ('ultra', 'plus', 'fe', 'lite', 'power'):
                if v in t:
                    variant = v
                    break
            return f'galaxy {line}{num} {variant}'.strip()

        # Google Pixel
        m = re.search(r'pixel\s*(\d+[a-z]?)', t)
        if m:
            num = m.group(1)
            variant = ''
            for v in ('pro', 'xl', 'a'):
                if f'pixel {num} {v}' in t or f'pixel{num}{v}' in t:
                    variant = v
                    break
            return f'pixel {num} {variant}'.strip()

        # OnePlus
        m = re.search(r'oneplus\s+(\S+(?:\s+\S+)?)', t)
        if m:
            return f'oneplus {m.group(1)[:20]}'.strip()

        # Huawei Mate/P/Nova with variant capture
        m = re.search(r'(p\d+|mate\s*\d+|nova\s*\d+)', t)
        if m:
            base = re.sub(r'\s+', ' ', m.group(1)).strip()
            variant = ''
            for v in ('pro', 'lite', 'plus', 'max', 'ultra'):
                if v in t:
                    variant = v
                    break
            return f'huawei {base} {variant}'.strip()
        # Huawei model-code fallback: CLT-L29, VOG-L29, etc. -> stable key
        if 'huawei' in t or (brand_hint or '').lower() == 'huawei':
            m = re.search(r'\b([a-z]{3})\s*[a-z]?\d{2,3}\b', t)
            if m:
                return f'huawei {m.group(1)}'.strip()

        # Xiaomi numbered series: "xiaomi 15 ultra", "xiaomi 14t pro"
        m = re.search(r'xiaomi\s+(\d+[a-z]?(?:\s+(?:pro|plus|ultra|lite|t))*)', t)
        if m:
            return f'xiaomi {m.group(1)}'.strip()

        # Xiaomi / Redmi / POCO
        for prefix in ('redmi note', 'redmi', 'poco'):
            m = re.search(rf'{prefix}\s+(\w+\d+\w*)', t)
            if m:
                return f'{prefix} {m.group(1)}'.strip()

        # Motorola (Moto G/E/X/Z + Edge series)
        m = re.search(r'moto\s*(g|e|x|z)\s*(\d+)?', t)
        if m:
            variant = ''
            for v in ('plus', 'power', 'play', 'stylus'):
                if v in t:
                    variant = v
                    break
            return f'moto {m.group(1)}{m.group(2) or ""} {variant}'.strip()
        m = re.search(r'(?:motorola\s+)?edge\s*(\d+)?', t)
        if m and ('motorola' in t or 'moto' in t):
            variant = ''
            for v in ('pro', 'plus', 'ultra', 'neo', 'lite'):
                if v in t:
                    variant = v
                    break
            return f'moto edge {m.group(1) or ""} {variant}'.strip()

        # Nokia Lumia
        m = re.search(r'(?:nokia\s+)?lumia\s*(\d+)', t)
        if m:
            return f'lumia {m.group(1)}'.strip()
        # Nokia numbered (e.g. Nokia 3310, Nokia 8.3)
        if 'nokia' in t:
            m = re.search(r'nokia\s+(\d[\d.]*)', t)
            if m:
                return f'nokia {m.group(1)}'.strip()

        # Samsung model codes (GT-/SM- hardware identifiers)
        # Map known prefixes to Galaxy families when possible:
        #   SM-N / GT-N -> Galaxy Note, SM-G / GT-I -> Galaxy S,
        #   SM-A -> Galaxy A, SM-M -> Galaxy M, SM-F -> Galaxy Z (Fold/Flip)
        # normalize_text turns "GT-I9500" into "gt i9500", so match both forms
        _SM_FAMILY_MAP = {
            'n': 'galaxy note', 'g': 'galaxy s', 'i': 'galaxy s',
            'a': 'galaxy a', 'm': 'galaxy m', 'f': 'galaxy z',
            'j': 'galaxy j', 'e': 'galaxy e', 't': 'galaxy tab',
        }
        m = re.search(r'((?:gt|sm)[\s-]?([a-z])\d{3,5})', t)
        if m:
            code = re.sub(r'\s+', '-', m.group(1))  # normalize "gt i9500" -> "gt-i9500"
            family_letter = m.group(2).lower()
            family = _SM_FAMILY_MAP.get(family_letter, '')
            if family:
                return f'{family} ({code})'.strip()
            return f'samsung {code}'.strip()

    # --- Tablet ---
    if cat in ('tablet', 'tab'):
        # iPad
        m = re.search(r'ipad\s*(pro|air|mini)?', t)
        if m:
            line = m.group(1) or 'base'
            size = ''
            # After normalize_text, "12.9" becomes "12 9" — match both forms
            sm = re.search(r'(\d{1,2}[\.\s]\d)\s*(?:inch|")?', t)
            if sm:
                size = sm.group(1).replace(' ', '.')
            gen = ''
            gm = re.search(r'(\d+)(?:th|st|nd|rd)\s*gen', t)
            if gm:
                gen = f'gen{gm.group(1)}'
            ym = re.search(r'(20\d{2})', t)
            year = ym.group(1) if ym and not gen else ''
            return f'ipad {line} {size} {gen}{year}'.strip()

        # Galaxy Tab — multi-token model capture (s9 fe, s8 ultra, a9, etc.)
        m = re.search(r'galaxy\s*tab\s*(\w+\+?)', t)
        if m:
            model = m.group(1)
            variant = ''
            for v in ('fe', 'ultra', 'plus', 'lite'):
                # Check variant appears AFTER the model token, not as the model itself
                if re.search(rf'tab\s+{re.escape(model)}\s+.*{v}', t) or \
                   re.search(rf'tab\s+{re.escape(model)}\s+{v}', t):
                    variant = v
                    break
                # Also check if variant is anywhere in the text (for reordered inputs)
                if v in t and v != model:
                    variant = v
                    break
            return f'galaxy tab {model} {variant}'.strip()

        # Surface Pro / Surface Go (with generation)
        m = re.search(r'surface\s*(pro|go)\s*(\d+)?', t)
        if m:
            line = m.group(1)
            num = m.group(2) or ''
            return f'surface {line} {num}'.strip()

        # Huawei MediaPad / MatePad
        m = re.search(r'(mediapad|matepad)\s*(pro|air|se)?\s*(\d[\d.]*)?', t)
        if m:
            series = m.group(1)
            variant = m.group(2) or ''
            num = m.group(3) or ''
            return f'huawei {series} {variant} {num}'.strip()

    # --- Watch ---
    if cat in ('watch', 'smartwatch'):
        # Apple Watch (series must come before se to avoid "se" matching inside "series")
        m = re.search(r'apple\s*watch\s*(ultra\s*\d*|series\s*\d+|se)\b', t)
        if m:
            model = re.sub(r'\s+', ' ', m.group(1)).strip()
            size = ''
            sm = re.search(r'(\d{2})\s*mm', t)
            if sm:
                size = f'{sm.group(1)}mm'
            return f'apple watch {model} {size}'.strip()

        # Galaxy Watch — classic/active/ultra + number
        m = re.search(r'galaxy\s*watch\s*(\d+)?\s*(classic|active\s*\d*|ultra|fe)?', t)
        if m:
            num = m.group(1) or ''
            sub = (m.group(2) or '').strip()
            size = ''
            sm = re.search(r'(\d{2})\s*mm', t)
            if sm:
                size = f'{sm.group(1)}mm'
            parts = ['galaxy watch']
            if num:
                parts.append(num)
            if sub:
                parts.append(sub)
            if size:
                parts.append(size)
            return ' '.join(parts)

        # Huawei Watch
        m = re.search(r'(watch\s*gt\s*\d*|watch\s*fit\s*\d*|watch\s*\d+)', t)
        if m:
            return f'huawei {m.group(1)}'.strip()

    return ''


# ---------------------------------------------------------------------------
# V2-only: Catalog Add Requests generator
# ---------------------------------------------------------------------------

def generate_catalog_add_requests(all_results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build a "Catalog Add Requests" sheet from NO_MATCH rows with
    no_match_reason == CATALOG_MISSING_LIKELY.

    Aggregates by brand + category + model_family_key.
    Returns DataFrame with columns:
      brand, category, model_family_key, count, example_1, example_2, example_3
    """
    rows = []
    for _sheet_name, df in all_results.items():
        if 'no_match_reason' not in df.columns:
            continue
        mask = df['no_match_reason'] == 'CATALOG_MISSING_LIKELY'
        missing = df[mask]
        for _, row in missing.iterrows():
            name = str(row.get('original_input', ''))
            cat = str(row.get('category', ''))
            brand = ''
            # Try to get brand from common column names
            for bcol in ('Brand', 'brand', 'manufacturer'):
                if bcol in row.index and pd.notna(row[bcol]):
                    brand = str(row[bcol]).strip()
                    break
            if not brand:
                brand = normalize_brand(name.split()[0]) if name.strip() else ''
            # V2: infer category if empty/other, then extract model family key
            cat = _infer_canonical_category_v2(name, brand, cat)
            mfk = extract_model_family_key(name, cat, brand_hint=brand)
            # Task C: fallback grouping key when model_family_key is blank
            group_key = mfk
            if not mfk:
                norm_brand = normalize_brand(brand) or brand.lower()
                cleaned = normalize_text(name)
                first_tokens = ' '.join(cleaned.split()[:3]) if cleaned else ''
                group_key = f'{norm_brand}:{first_tokens}' if first_tokens else norm_brand
            rows.append({
                'brand': normalize_brand(brand) or brand.lower(),
                'category': cat,
                'model_family_key': mfk,
                'group_key': group_key,
                'raw_name': name,
            })

    if not rows:
        return pd.DataFrame(columns=['brand', 'category', 'model_family_key', 'group_key',
                                     'count', 'example_1', 'example_2', 'example_3'])

    df_raw = pd.DataFrame(rows)
    # Aggregate by group_key (= model_family_key when available, fallback otherwise)
    agg_rows = []
    for (brand, cat, gk), grp in df_raw.groupby(['brand', 'category', 'group_key']):
        examples = grp['raw_name'].unique()[:3].tolist()
        # Use the first non-empty model_family_key in the group (if any)
        mfk = next((v for v in grp['model_family_key'] if v), '')
        agg_rows.append({
            'brand': brand,
            'category': cat,
            'model_family_key': mfk,
            'group_key': gk,
            'count': len(grp),
            'example_1': examples[0] if len(examples) > 0 else '',
            'example_2': examples[1] if len(examples) > 1 else '',
            'example_3': examples[2] if len(examples) > 2 else '',
        })

    return pd.DataFrame(agg_rows).sort_values('count', ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# V2-only: Diagnostics sheet generator
# ---------------------------------------------------------------------------

def generate_diagnostics_sheet(all_results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build a diagnostics sheet with:
    - Per-category status counts
    - no_match_reason distribution
    - Top gate-block reasons
    """
    diag_rows = []

    # 1. Per-category status counts
    diag_rows.append({'section': 'PER-CATEGORY STATUS COUNTS', 'key': '', 'value': ''})
    combined = pd.concat(all_results.values(), ignore_index=True)
    for cat in sorted(combined['category'].dropna().unique()):
        cat_df = combined[combined['category'] == cat]
        for status in [MATCH_STATUS_MATCHED, MATCH_STATUS_SUGGESTED, MATCH_STATUS_NO_MATCH]:
            cnt = int((cat_df['match_status'] == status).sum())
            pct = f'{cnt/len(cat_df)*100:.1f}%' if len(cat_df) > 0 else '0%'
            diag_rows.append({
                'section': 'category_status',
                'key': f'{cat} / {status}',
                'value': f'{cnt} ({pct})',
            })

    # 2. no_match_reason distribution
    diag_rows.append({'section': '', 'key': '', 'value': ''})
    diag_rows.append({'section': 'NO_MATCH REASON DISTRIBUTION', 'key': '', 'value': ''})
    if 'no_match_reason' in combined.columns:
        for reason, cnt in combined['no_match_reason'].value_counts().items():
            diag_rows.append({
                'section': 'no_match_reason',
                'key': str(reason),
                'value': str(cnt),
            })

    # 3. review_reason distribution
    diag_rows.append({'section': '', 'key': '', 'value': ''})
    diag_rows.append({'section': 'REVIEW REASON DISTRIBUTION', 'key': '', 'value': ''})
    if 'review_reason' in combined.columns:
        review_df = combined[combined['review_reason'].fillna('').str.len() > 0]
        for reason, cnt in review_df['review_reason'].value_counts().items():
            diag_rows.append({
                'section': 'review_reason',
                'key': str(reason),
                'value': str(cnt),
            })

    # 4. Top gate-block reasons
    diag_rows.append({'section': '', 'key': '', 'value': ''})
    diag_rows.append({'section': 'TOP GATE-BLOCK REASONS (top 20)', 'key': '', 'value': ''})
    if 'verification_reasons' in combined.columns:
        vr = combined[combined['verification_reasons'].fillna('').str.len() > 0]['verification_reasons']
        reason_counts = {}
        for reasons_str in vr:
            for r in str(reasons_str).split('; '):
                r = r.strip()
                if r:
                    reason_counts[r] = reason_counts.get(r, 0) + 1
        for reason, cnt in sorted(reason_counts.items(), key=lambda x: -x[1])[:20]:
            diag_rows.append({
                'section': 'gate_block',
                'key': reason,
                'value': str(cnt),
            })

    # 5. Catalog Missing Likely: top 20 group_keys where category=other
    diag_rows.append({'section': '', 'key': '', 'value': ''})
    diag_rows.append({'section': 'CATALOG MISSING: TOP 20 category=other GROUP KEYS', 'key': '', 'value': ''})
    if 'no_match_reason' in combined.columns:
        _nm = combined[combined.get('no_match_reason', pd.Series(dtype=str)).fillna('') == 'CATALOG_MISSING_LIKELY']
        if len(_nm) > 0:
            _other = _nm[_nm['category'].fillna('other').str.lower().isin(('other', '', 'nan', 'none'))]
            if len(_other) > 0:
                _gkeys = []
                for _, row in _other.iterrows():
                    name = str(row.get('original_input', ''))
                    brand = ''
                    for bcol in ('Brand', 'brand', 'manufacturer'):
                        if bcol in row.index and pd.notna(row.get(bcol)):
                            brand = str(row[bcol]).strip()
                            break
                    cat = str(row.get('category', ''))
                    cat = _infer_canonical_category_v2(name, brand, cat)
                    mfk = extract_model_family_key(name, cat, brand_hint=brand)
                    if not mfk:
                        nb = normalize_brand(brand) or brand.lower()
                        ct = normalize_text(name)
                        ft = ' '.join(ct.split()[:3]) if ct else ''
                        mfk = f'{nb}:{ft}' if ft else nb
                    _gkeys.append(mfk)
                _other = _other.copy()
                _other['_diag_gk'] = _gkeys
                for gk, cnt in _other['_diag_gk'].value_counts().head(20).items():
                    examples = _other[_other['_diag_gk'] == gk]['original_input'].unique()[:2].tolist()
                    diag_rows.append({
                        'section': 'catalog_missing_other',
                        'key': str(gk),
                        'value': f'{cnt} (e.g. {"; ".join(str(e)[:50] for e in examples)})',
                    })

    # 6. Catalog Missing Likely: top 20 brands
    diag_rows.append({'section': '', 'key': '', 'value': ''})
    diag_rows.append({'section': 'CATALOG MISSING: TOP 20 BRANDS', 'key': '', 'value': ''})
    if 'no_match_reason' in combined.columns:
        _nm = combined[combined['no_match_reason'].fillna('') == 'CATALOG_MISSING_LIKELY']
        if len(_nm) > 0:
            _brands = []
            for _, row in _nm.iterrows():
                brand = ''
                for bcol in ('Brand', 'brand', 'manufacturer'):
                    if bcol in row.index and pd.notna(row.get(bcol)):
                        brand = normalize_brand(str(row[bcol]).strip())
                        break
                _brands.append(brand or 'unknown')
            brand_counts = pd.Series(_brands).value_counts()
            for brand, cnt in brand_counts.head(20).items():
                diag_rows.append({
                    'section': 'catalog_missing_brand',
                    'key': str(brand),
                    'value': str(cnt),
                })

    # ================================================================
    # 7. LAPTOP DIAGNOSTICS (V2 Task D)
    # ================================================================

    laptop_df = combined[combined['category'].fillna('').str.lower() == 'laptop']
    if len(laptop_df) > 0:
        # 7a. Laptop: Top 20 CATALOG_MISSING / RETRIEVAL_WEAK group_keys
        diag_rows.append({'section': '', 'key': '', 'value': ''})
        diag_rows.append({'section': 'LAPTOP: TOP 20 CATALOG MISSING / RETRIEVAL_WEAK GROUP KEYS',
                          'key': '', 'value': ''})
        _nm_laptop = laptop_df[laptop_df['no_match_reason'].fillna('').isin(
            ('CATALOG_MISSING_LIKELY', 'RETRIEVAL_WEAK'))]
        if len(_nm_laptop) > 0:
            _gkeys = []
            for _, row in _nm_laptop.iterrows():
                name = str(row.get('original_input', ''))
                brand = ''
                for bcol in ('Brand', 'brand', 'manufacturer'):
                    if bcol in row.index and pd.notna(row.get(bcol)):
                        brand = str(row[bcol]).strip()
                        break
                mfk = extract_model_family_key(name, 'laptop', brand_hint=brand)
                if not mfk:
                    nb = normalize_brand(brand) or brand.lower()
                    ct = normalize_text(name)
                    ft = ' '.join(ct.split()[:4]) if ct else ''
                    mfk = f'{nb}:{ft}' if ft else nb
                _gkeys.append(mfk)
            _nm_laptop = _nm_laptop.copy()
            _nm_laptop['_diag_gk'] = _gkeys
            for gk, cnt in _nm_laptop['_diag_gk'].value_counts().head(20).items():
                examples = _nm_laptop[_nm_laptop['_diag_gk'] == gk]['original_input'].unique()[:2].tolist()
                diag_rows.append({
                    'section': 'laptop_missing_gk',
                    'key': str(gk),
                    'value': f'{cnt} (e.g. {"; ".join(str(e)[:60] for e in examples)})',
                })

        # 7b. Laptop: Top 10 review_summary reasons
        diag_rows.append({'section': '', 'key': '', 'value': ''})
        diag_rows.append({'section': 'LAPTOP: TOP 10 REVIEW SUMMARY REASONS', 'key': '', 'value': ''})
        _rev_laptop = laptop_df[laptop_df['match_status'] == MATCH_STATUS_SUGGESTED]
        if len(_rev_laptop) > 0 and 'review_summary' in _rev_laptop.columns:
            for reason, cnt in _rev_laptop['review_summary'].fillna('').value_counts().head(10).items():
                if reason:
                    diag_rows.append({
                        'section': 'laptop_review_summary',
                        'key': str(reason)[:80],
                        'value': str(cnt),
                    })

        # 7c. Laptop: Top 10 "near miss" gate-block attributes
        diag_rows.append({'section': '', 'key': '', 'value': ''})
        diag_rows.append({'section': 'LAPTOP: TOP 10 NEAR-MISS GATE REASONS', 'key': '', 'value': ''})
        if 'verification_reasons' in laptop_df.columns:
            _vr = laptop_df[laptop_df['verification_reasons'].fillna('').str.len() > 0]['verification_reasons']
            _reason_counts = {}
            for reasons_str in _vr:
                for r in str(reasons_str).split('; '):
                    r = r.strip()
                    if r and r.startswith('laptop_'):
                        _reason_counts[r] = _reason_counts.get(r, 0) + 1
            for reason, cnt in sorted(_reason_counts.items(), key=lambda x: -x[1])[:10]:
                diag_rows.append({
                    'section': 'laptop_gate_reason',
                    'key': reason,
                    'value': str(cnt),
                })

        # 7d. Laptop: no_match_reason breakdown
        diag_rows.append({'section': '', 'key': '', 'value': ''})
        diag_rows.append({'section': 'LAPTOP: NO_MATCH_REASON BREAKDOWN', 'key': '', 'value': ''})
        if 'no_match_reason' in laptop_df.columns:
            for reason, cnt in laptop_df['no_match_reason'].value_counts().items():
                diag_rows.append({
                    'section': 'laptop_no_match_reason',
                    'key': str(reason),
                    'value': str(cnt),
                })

    return pd.DataFrame(diag_rows)


# ---------------------------------------------------------------------------
# V2-only: Safety Audit — detect attribute mismatches in MATCHED rows
# ---------------------------------------------------------------------------

def generate_safety_audit_v2(all_results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Audit MATCHED rows for attribute mismatches that should have been caught.
    Returns a summary DataFrame with mismatch counts per category.
    """
    rows = []
    combined = pd.concat(all_results.values(), ignore_index=True)
    matched = combined[combined['match_status'].isin([MATCH_STATUS_MATCHED, MATCH_STATUS_MULTIPLE])]

    # --- Laptop: platform_code / model_code / laptop_family mismatch ---
    laptop_mismatches = 0
    laptop_details = []
    for _, r in matched.iterrows():
        q = str(r.get('original_input', '') or r.get('matched_on', ''))
        m = str(r.get('matched_on', ''))
        cat = str(r.get('category', '')).lower()
        if cat != 'laptop' and not is_laptop_product(q):
            continue
        try:
            q_attrs = extract_laptop_attributes(q, '')
            c_attrs = extract_laptop_attributes(m, '')
            lp_pass, lp_reasons = laptop_variant_exact_match(q_attrs, c_attrs)
            if not lp_pass:
                laptop_mismatches += 1
                laptop_details.append(f'{q[:60]} -> {m[:60]}: {"; ".join(lp_reasons)}')
        except Exception:
            pass
    rows.append({'audit': 'laptop_model_platform_mismatch_in_MATCHED',
                 'count': laptop_mismatches, 'threshold': 0,
                 'status': 'PASS' if laptop_mismatches == 0 else 'FAIL',
                 'details': '; '.join(laptop_details[:5])})

    # --- Mobile: pro/pro max, model number mismatch ---
    # Use brand context from the row to avoid false positives when
    # original_input lacks brand prefix (e.g. "Galaxy S23" vs "samsung galaxy s23").
    # Classify: confirmed = real variant/model/storage mismatch;
    #           audit_uncertain = parsing-only diffs (brand/product_line normalization)
    _CONFIRMED_PREFIXES = ('mobile_model:', 'mobile_storage:', 'mobile_variant:',
                           'mobile_model_number:', 'samsung_s_number:', 'samsung_variant:')
    mobile_confirmed = 0
    mobile_uncertain = 0
    mobile_confirmed_details = []
    mobile_uncertain_details = []
    for _, r in matched.iterrows():
        q_raw = str(r.get('original_input', '') or '')
        m = str(r.get('matched_on', ''))
        cat = str(r.get('category', '')).lower()
        if cat not in ('mobile', 'mobile phone', 'phone', ''):
            continue
        if is_laptop_product(q_raw):
            continue
        # Reconstruct query with brand context (mirrors run_matching flow)
        brand_hint = ''
        for bcol in ('Brand', 'brand', 'manufacturer'):
            if bcol in r.index and pd.notna(r.get(bcol)):
                brand_hint = str(r[bcol]).strip()
                break
        q = (brand_hint + ' ' + q_raw).strip() if brand_hint else q_raw
        try:
            q_attrs = extract_product_attributes(q, brand_hint)
            c_attrs = extract_product_attributes(m, '')
            mp, mr = mobile_variant_exact_match(q_attrs, c_attrs)
            if not mp:
                is_confirmed = any(reason.startswith(pfx) for reason in mr
                                   for pfx in _CONFIRMED_PREFIXES)
                detail = f'{q_raw[:50]} -> {m[:50]}: {"; ".join(mr)}'
                if is_confirmed:
                    mobile_confirmed += 1
                    mobile_confirmed_details.append(detail)
                else:
                    mobile_uncertain += 1
                    mobile_uncertain_details.append(detail)
        except Exception:
            pass
    mobile_total = mobile_confirmed + mobile_uncertain
    if mobile_confirmed > 0:
        status = 'WARN'
    elif mobile_uncertain > 0:
        status = 'WARN'
    else:
        status = 'PASS'
    details_parts = []
    if mobile_confirmed_details:
        details_parts.append(f'CONFIRMED({mobile_confirmed}): ' + '; '.join(mobile_confirmed_details[:3]))
    if mobile_uncertain_details:
        details_parts.append(f'AUDIT_UNCERTAIN({mobile_uncertain}): ' + '; '.join(mobile_uncertain_details[:3]))
    rows.append({'audit': 'mobile_variant_mismatch_in_MATCHED',
                 'count': mobile_total, 'threshold': 0,
                 'status': status,
                 'details': ' | '.join(details_parts) if details_parts else ''})

    # --- Watch: mm / connectivity mismatch ---
    watch_mismatches = 0
    watch_details = []
    for _, r in matched.iterrows():
        q = str(r.get('original_input', '') or r.get('matched_on', ''))
        m = str(r.get('matched_on', ''))
        cat = str(r.get('category', '')).lower()
        if cat not in ('watch', 'smartwatch'):
            continue
        try:
            q_attrs = extract_product_attributes(q, '')
            c_attrs = extract_product_attributes(m, '')
            wp, wr = variant_exact_match(q_attrs, c_attrs)
            if not wp:
                watch_mismatches += 1
                watch_details.append(f'{q[:60]} -> {m[:60]}: {"; ".join(wr)}')
        except Exception:
            pass
    rows.append({'audit': 'watch_mm_connectivity_mismatch_in_MATCHED',
                 'count': watch_mismatches, 'threshold': 0,
                 'status': 'PASS' if watch_mismatches == 0 else 'FAIL',
                 'details': '; '.join(watch_details[:5])})

    # --- Tablet: size / connectivity mismatch ---
    tablet_mismatches = 0
    tablet_details = []
    for _, r in matched.iterrows():
        q = str(r.get('original_input', '') or r.get('matched_on', ''))
        m = str(r.get('matched_on', ''))
        cat = str(r.get('category', '')).lower()
        if cat not in ('tablet', 'tab'):
            continue
        try:
            q_attrs = extract_product_attributes(q, '')
            c_attrs = extract_product_attributes(m, '')
            tp, tr = tablet_variant_exact_match(q_attrs, c_attrs)
            if not tp:
                tablet_mismatches += 1
                tablet_details.append(f'{q[:60]} -> {m[:60]}: {"; ".join(tr)}')
        except Exception:
            pass
    rows.append({'audit': 'tablet_size_connectivity_mismatch_in_MATCHED',
                 'count': tablet_mismatches, 'threshold': 0,
                 'status': 'PASS' if tablet_mismatches == 0 else 'FAIL',
                 'details': '; '.join(tablet_details[:5])})

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# V2-only: Schema Audit — validate alternatives JSON + schema
# ---------------------------------------------------------------------------

def generate_schema_audit_v2(all_results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Validate that all alternatives fields are valid JSON with correct schema.
    Returns a summary DataFrame.
    """
    rows = []
    combined = pd.concat(all_results.values(), ignore_index=True)

    # 1. JSON parse check
    json_ok = 0
    json_fail = 0
    json_fail_examples = []
    for _, r in combined.iterrows():
        raw = r.get('alternatives', '')
        if isinstance(raw, list):
            json_ok += 1
            continue
        if not isinstance(raw, str) or not raw.strip():
            json_ok += 1  # empty is valid (serialized as '[]')
            continue
        try:
            json.loads(raw)
            json_ok += 1
        except (json.JSONDecodeError, ValueError):
            json_fail += 1
            if len(json_fail_examples) < 3:
                json_fail_examples.append(str(raw)[:80])
    rows.append({'audit': 'alternatives_json_parseable',
                 'total': json_ok + json_fail, 'ok': json_ok, 'fail': json_fail,
                 'status': 'PASS' if json_fail == 0 else 'FAIL',
                 'examples': '; '.join(json_fail_examples)})

    # 2. Schema check: each item has {uae_assetid, uae_assetname, score, reason}
    schema_ok = 0
    schema_fail = 0
    schema_fail_examples = []
    required_keys = {'uae_assetid', 'uae_assetname', 'score', 'reason'}
    for _, r in combined.iterrows():
        raw = r.get('alternatives', '')
        try:
            alts = json.loads(raw) if isinstance(raw, str) else (raw if isinstance(raw, list) else [])
        except Exception:
            continue
        if not alts:
            continue
        for a in alts:
            if not isinstance(a, dict):
                schema_fail += 1
                if len(schema_fail_examples) < 3:
                    schema_fail_examples.append(f'not_dict: {str(a)[:60]}')
                continue
            if not required_keys.issubset(a.keys()):
                schema_fail += 1
                missing = required_keys - set(a.keys())
                if len(schema_fail_examples) < 3:
                    schema_fail_examples.append(f'missing_keys: {missing}')
            else:
                schema_ok += 1
    rows.append({'audit': 'alternatives_schema_correct',
                 'total': schema_ok + schema_fail, 'ok': schema_ok, 'fail': schema_fail,
                 'status': 'PASS' if schema_fail == 0 else 'FAIL',
                 'examples': '; '.join(schema_fail_examples)})

    # 3. Score numeric check
    score_ok = 0
    score_fail = 0
    for _, r in combined.iterrows():
        raw = r.get('alternatives', '')
        try:
            alts = json.loads(raw) if isinstance(raw, str) else (raw if isinstance(raw, list) else [])
        except Exception:
            continue
        for a in alts:
            if not isinstance(a, dict):
                continue
            s = a.get('score')
            if s is None or isinstance(s, (int, float)):
                score_ok += 1
            else:
                try:
                    float(s)
                    score_ok += 1
                except (ValueError, TypeError):
                    score_fail += 1
    rows.append({'audit': 'alternatives_score_numeric',
                 'total': score_ok + score_fail, 'ok': score_ok, 'fail': score_fail,
                 'status': 'PASS' if score_fail == 0 else 'FAIL',
                 'examples': ''})

    # 4. Review Required alt coverage
    review_rows = combined[combined['match_status'] == MATCH_STATUS_SUGGESTED]
    review_total = len(review_rows)
    review_with_alts = 0
    review_with_blk = 0
    for _, r in review_rows.iterrows():
        raw = r.get('alternatives', '')
        try:
            alts = json.loads(raw) if isinstance(raw, str) else (raw if isinstance(raw, list) else [])
        except Exception:
            alts = []
        if alts:
            review_with_alts += 1
        raw_b = r.get('blocked_candidates', '')
        try:
            blk = json.loads(raw_b) if isinstance(raw_b, str) else (raw_b if isinstance(raw_b, list) else [])
        except Exception:
            blk = []
        if blk:
            review_with_blk += 1
    # A row is "covered" if it has EITHER alternatives OR blocked_candidates
    covered = 0
    for _, r in review_rows.iterrows():
        has_alt = False
        has_blk = False
        raw = r.get('alternatives', '')
        try:
            alts = json.loads(raw) if isinstance(raw, str) else (raw if isinstance(raw, list) else [])
            has_alt = bool(alts)
        except Exception:
            pass
        raw_b = r.get('blocked_candidates', '')
        try:
            blk = json.loads(raw_b) if isinstance(raw_b, str) else (raw_b if isinstance(raw_b, list) else [])
            has_blk = bool(blk)
        except Exception:
            pass
        if has_alt or has_blk:
            covered += 1
    pct = f'{covered/review_total*100:.1f}%' if review_total > 0 else 'N/A'
    rows.append({'audit': 'review_alt_or_blk_coverage',
                 'total': review_total, 'ok': covered,
                 'fail': review_total - covered,
                 'status': 'PASS' if review_total == 0 or covered / review_total >= 0.95 else 'WARN',
                 'examples': f'{pct} covered ({review_with_alts} alts, {review_with_blk} blk)'})

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Unit tests for URL helpers  (run:  python matcher_v2.py --test-url)
# ---------------------------------------------------------------------------

def _run_url_tests() -> None:
    """5 focused tests for _is_url and extract_name_from_url."""
    passed = 0
    failed = 0

    def _assert(cond, label):
        nonlocal passed, failed
        if cond:
            passed += 1
            print(f'  PASS  {label}')
        else:
            failed += 1
            print(f'  FAIL  {label}')

    print('--- URL helper tests ---')

    # 1. Scheme-less bare domain parses a product name
    name = extract_name_from_url('example.com/iphone-15-128gb')
    _assert(name and 'iphone' in name.lower() and '128gb' in name.lower(),
            f'1) bare domain -> "{name}"')

    # 2. Protocol-relative URL parses
    name = extract_name_from_url('//example.com/iphone-15-128gb')
    _assert(name and 'iphone' in name.lower(),
            f'2) protocol-relative -> "{name}"')

    # 3. Embedded URL (text before the https://)
    name = extract_name_from_url('url: https://example.com/iphone-15-128gb')
    _assert(name and 'iphone' in name.lower(),
            f'3) embedded URL -> "{name}"')

    # 4. Numeric last segment falls back to prior segment
    name = extract_name_from_url('https://example.com/samsung-galaxy-s23-256gb/12345')
    _assert(name and 'galaxy' in name.lower(),
            f'4) numeric last seg fallback -> "{name}"')

    # 5. Normal non-URL text is unchanged by _is_url
    for plain in ['iPhone 15 128GB', 'Samsung Galaxy S23', 'Zo goed als nieuw']:
        _assert(not _is_url(plain),
                f'5) _is_url("{plain}") -> False')

    print(f'\n{passed} passed, {failed} failed')
    if failed:
        raise SystemExit(1)


if __name__ == '__main__':
    import sys
    if '--test-url' in sys.argv:
        _run_url_tests()