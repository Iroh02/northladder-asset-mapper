"""
Core matching engine for UAE Asset ID mapping.

Matching Approach:
    - Combines manufacturer/brand with asset name to form a full product string
    - Normalizes strings (lowercase, remove punctuation, standardize storage, remove years)
    - Uses rapidfuzz token_sort_ratio for fuzzy matching (order-independent token comparison)
    - Token-sort is chosen because List 1 names ("iPhone 6 16GB") and NL names
      ("Apple iPhone 6 (2014), 16GB") contain the same tokens in different orders/formats

Threshold Choice:
    - 85% was chosen as a balance between precision and recall
    - Below 85%, false positives increase significantly (e.g., "iPhone 6" matching "iPhone 6S")
    - Above 85%, legitimate matches with minor formatting differences get missed

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
SIMILARITY_THRESHOLD = 85  # Minimum score for a valid match

MATCH_STATUS_MATCHED = "MATCHED"
MATCH_STATUS_MULTIPLE = "MULTIPLE_MATCHES"
MATCH_STATUS_NO_MATCH = "NO_MATCH"


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


# ---------------------------------------------------------------------------
# Matching logic
# ---------------------------------------------------------------------------

def match_single_item(
    query: str,
    nl_lookup: Dict[str, List[str]],
    nl_names: List[str],
    threshold: int = SIMILARITY_THRESHOLD,
) -> dict:
    """
    Match a single normalized query string against the NL list.

    Returns dict with:
        - mapped_uae_assetid: matched ID(s) or empty string
        - match_score: best similarity score (0-100)
        - match_status: MATCHED / MULTIPLE_MATCHES / NO_MATCH
        - matched_on: the normalized NL name that was matched (for explainability)

    Uses token_sort_ratio which:
        - Splits strings into tokens, sorts them, then compares
        - This handles word-order differences well
        - "apple iphone 6 16gb" matches "iphone 6 16gb apple" equally
    """
    if not query:
        return {
            'mapped_uae_assetid': '',
            'match_score': 0,
            'match_status': MATCH_STATUS_NO_MATCH,
            'matched_on': '',
        }

    # Find the best match using token_sort_ratio
    result = process.extractOne(
        query,
        nl_names,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=threshold,
    )

    if result is None:
        return {
            'mapped_uae_assetid': '',
            'match_score': 0,
            'match_status': MATCH_STATUS_NO_MATCH,
            'matched_on': '',
        }

    best_match, score, _ = result
    asset_ids = nl_lookup.get(best_match, [])

    if len(asset_ids) == 0:
        return {
            'mapped_uae_assetid': '',
            'match_score': round(score, 2),
            'match_status': MATCH_STATUS_NO_MATCH,
            'matched_on': best_match,
        }
    elif len(asset_ids) == 1:
        return {
            'mapped_uae_assetid': asset_ids[0],
            'match_score': round(score, 2),
            'match_status': MATCH_STATUS_MATCHED,
            'matched_on': best_match,
        }
    else:
        return {
            'mapped_uae_assetid': ', '.join(asset_ids),
            'match_score': round(score, 2),
            'match_status': MATCH_STATUS_MULTIPLE,
            'matched_on': best_match,
        }


def run_matching(
    df_input: pd.DataFrame,
    brand_col: str,
    name_col: str,
    nl_lookup: Dict[str, List[str]],
    nl_names: List[str],
    threshold: int = SIMILARITY_THRESHOLD,
    progress_callback: Optional[Callable] = None,
) -> pd.DataFrame:
    """
    Run fuzzy matching for an entire input DataFrame against the NL lookup.

    Args:
        df_input: The input asset list (List 1 or List 2)
        brand_col: Column name containing the brand/manufacturer
        name_col: Column name containing the product name
        nl_lookup: dict of normalized_name → [asset_ids]
        nl_names: list of all normalized NL names (for rapidfuzz)
        threshold: minimum similarity score
        progress_callback: optional callable(current, total) for UI progress

    Returns:
        Copy of df_input with added columns:
            mapped_uae_assetid, match_score, match_status, matched_on
    """
    df = df_input.copy()
    total = len(df)

    results = []
    for idx, row in df.iterrows():
        query = build_match_string(
            row.get(brand_col, ''),
            row.get(name_col, ''),
        )
        match_result = match_single_item(query, nl_lookup, nl_names, threshold)
        results.append(match_result)

        if progress_callback and (len(results) % 50 == 0 or len(results) == total):
            progress_callback(len(results), total)

    results_df = pd.DataFrame(results)
    df['mapped_uae_assetid'] = results_df['mapped_uae_assetid'].values
    df['match_score'] = results_df['match_score'].values
    df['match_status'] = results_df['match_status'].values
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
) -> dict:
    """
    Test matching for a single item. Returns detailed info including top 3 alternatives.
    Used by the UI sample-match tester.
    """
    query = build_match_string(brand, name)

    if not query:
        return {
            'query': query,
            'error': 'Empty query after normalization',
            'top_matches': [],
        }

    # Get top 3 matches
    top_matches = process.extract(
        query,
        nl_names,
        scorer=fuzz.token_sort_ratio,
        limit=3,
    )

    alternatives = []
    for match_name, score, _ in top_matches:
        asset_ids = nl_lookup.get(match_name, [])
        alternatives.append({
            'nl_name': match_name,
            'score': round(score, 2),
            'asset_ids': asset_ids,
            'status': 'MATCHED' if score >= threshold else 'BELOW_THRESHOLD',
        })

    best = match_single_item(query, nl_lookup, nl_names, threshold)

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

        results[sheet_name] = {
            'df': df.reset_index(drop=True),
            'brand_col': col_map['brand_col'],
            'name_col': col_map['name_col'],
        }

    return results