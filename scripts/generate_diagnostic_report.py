"""
Full Diagnostic Match Report Generator

Generates detailed CSV/Excel reports for ALL input sheets (List 1, List 2, Auction List)
with per-row attribute comparison, safety diagnostics, and missed-match debugging.

Usage:
    python generate_diagnostic_report.py

Inputs:
    - Asset Mapping Lists.xlsx  (List 1, List 2)
    - Auction List.xlsx         (Auction List)
    - NL reference catalog (from NorthLadder List sheet or saved reference)

Outputs:
    - match_diagnostic_report_ALL.csv           (combined, sorted by problems-first)
    - match_diagnostic_report__List_1.csv        (per-sheet)
    - match_diagnostic_report__List_2.csv        (per-sheet)
    - match_diagnostic_report__Auction_List.csv  (per-sheet)
    - match_diagnostic_report_ALL.xlsx           (one tab per sheet)
"""

import sys, os
import time
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.join(_SCRIPT_DIR, '..')
sys.path.insert(0, os.path.join(_PROJECT_ROOT, 'src'))
DATA_DIR = os.path.join(_PROJECT_ROOT, 'data')
OUTPUT_DIR = os.path.join(_PROJECT_ROOT, 'outputs')
os.makedirs(OUTPUT_DIR, exist_ok=True)

import pandas as pd
from rapidfuzz import fuzz, process

from matcher import (
    parse_nl_sheet, parse_asset_sheets, load_and_clean_nl_list,
    build_nl_lookup, build_brand_index, build_attribute_index,
    build_match_string, normalize_text, normalize_brand,
    extract_category, extract_storage, extract_watch_mm, extract_model_tokens,
    compute_confidence_breakdown, match_single_item,
    SIMILARITY_THRESHOLD, HIGH_CONFIDENCE_THRESHOLD,
    MATCH_STATUS_MATCHED, MATCH_STATUS_MULTIPLE, MATCH_STATUS_SUGGESTED, MATCH_STATUS_NO_MATCH,
    CONFIDENCE_HIGH, CONFIDENCE_MEDIUM, CONFIDENCE_LOW,
    _detect_header_row, _detect_columns, _is_nl_sheet,
)


def load_nl_catalog(excel_path=None):
    if excel_path is None:
        excel_path = os.path.join(DATA_DIR, "Asset Mapping Lists.xlsx")
    """Load and prepare the NL catalog with all indexes."""
    print("Loading NL catalog...")
    nl_df = parse_nl_sheet(excel_path)
    nl_clean, stats = load_and_clean_nl_list(nl_df)
    print(f"  NL catalog: {stats['final']} entries (dropped {stats['null_dropped']} null, {stats['test_dropped']} test)")

    nl_lookup = build_nl_lookup(nl_clean)
    nl_names = list(nl_lookup.keys())
    brand_index = build_brand_index(nl_clean)
    attribute_index = build_attribute_index(nl_clean)

    print(f"  Indexes: {len(nl_names)} unique names, {len(brand_index)} brands")
    return nl_clean, nl_lookup, nl_names, brand_index, attribute_index


def load_all_sheets():
    """Load all asset sheets from both Excel files."""
    sheets = {}

    # Asset Mapping Lists.xlsx (List 1, List 2)
    print("\nLoading Asset Mapping Lists.xlsx...")
    parsed = parse_asset_sheets(os.path.join(DATA_DIR, "Asset Mapping Lists.xlsx"))
    for name, config in parsed.items():
        print(f"  Sheet '{name}': {len(config['df'])} rows, brand='{config['brand_col']}', name='{config['name_col']}'")
        sheets[name] = config

    # Auction List.xlsx
    print("\nLoading Auction List.xlsx...")
    try:
        parsed_auction = parse_asset_sheets(os.path.join(DATA_DIR, "Auction List.xlsx"))
        for name, config in parsed_auction.items():
            print(f"  Sheet '{name}': {len(config['df'])} rows, brand='{config['brand_col']}', name='{config['name_col']}'")
            sheets[name] = config
    except FileNotFoundError:
        print("  WARNING: Auction List.xlsx not found, skipping.")

    return sheets


def get_top3_candidates(query, nl_names, nl_lookup, brand_index, brand_norm):
    """Get top 3 fuzzy candidates for NO_MATCH debugging."""
    search_names = nl_names
    search_lookup = nl_lookup
    if brand_index and brand_norm and brand_norm in brand_index:
        search_names = brand_index[brand_norm]['names']
        search_lookup = brand_index[brand_norm]['lookup']

    results = process.extract(
        query, search_names,
        scorer=fuzz.token_sort_ratio,
        limit=3,
    )

    candidates = []
    for match_name, score, _ in results:
        candidates.append((match_name, round(score, 2)))

    # Pad to 3
    while len(candidates) < 3:
        candidates.append(('', 0.0))

    return candidates


def process_sheet(sheet_name, config, nl_clean, nl_lookup, nl_names, brand_index, attribute_index):
    """Process a single sheet and return diagnostic rows."""
    df = config['df']
    brand_col = config['brand_col']
    name_col = config['name_col']
    category_col = config.get('category_col')
    storage_col = config.get('storage_col')

    rows = []
    total = len(df)

    for idx, row in df.iterrows():
        # Extract raw values
        input_brand = str(row.get(brand_col, '')).strip() if brand_col and brand_col != '__no_brand__' else ''
        original_product_name = str(row.get(name_col, '')).strip()
        original_category = str(row.get(category_col, '')).strip() if category_col else ''
        original_storage = str(row.get(storage_col, '')).strip() if storage_col else ''

        # Combine storage if separate column
        combined_name = original_product_name
        if original_storage:
            combined_name = f"{original_product_name} {original_storage}"

        # Build normalized query
        normalized_query = build_match_string(input_brand, combined_name)

        # Run matching
        match_result = match_single_item(
            normalized_query, nl_lookup, nl_names, SIMILARITY_THRESHOLD,
            brand_index=brand_index,
            input_brand=input_brand,
            attribute_index=attribute_index,
            nl_catalog=nl_clean,
            original_input=combined_name,
            input_category=original_category,
        )

        matched_on = match_result.get('matched_on', '')

        # Extract query attributes
        q_category = extract_category(normalized_query)
        q_storage = extract_storage(normalized_query)
        q_watch_mm = extract_watch_mm(normalized_query)
        q_model_tokens = extract_model_tokens(normalized_query)

        # Extract matched attributes
        m_category = extract_category(matched_on) if matched_on else ''
        m_storage = extract_storage(matched_on) if matched_on else ''
        m_watch_mm = extract_watch_mm(matched_on) if matched_on else ''
        m_model_tokens = extract_model_tokens(matched_on) if matched_on else []

        # Compute confidence breakdown
        if matched_on:
            breakdown = compute_confidence_breakdown(normalized_query, matched_on)
        else:
            breakdown = {
                'model_match': None, 'storage_match': None,
                'category_match': None, 'watch_mm_match': None,
                'brand_match': None, 'composite_score': 0.0,
                'risk_flags': [],
            }

        # Count NL variants for matched name
        nl_variant_count = len(nl_lookup.get(matched_on, [])) if matched_on else 0

        # Alternatives
        alternatives = match_result.get('alternatives', [])
        alt_str = ', '.join(str(a) for a in alternatives) if alternatives else ''

        # Top 3 candidates for NO_MATCH debugging
        top1_name, top1_score = '', 0.0
        top2_name, top2_score = '', 0.0
        top3_name, top3_score = '', 0.0

        if match_result['match_status'] == MATCH_STATUS_NO_MATCH:
            brand_norm = normalize_brand(input_brand) if input_brand else ''
            if not brand_norm:
                brand_norm = normalize_text(input_brand) if input_brand else ''
            candidates = get_top3_candidates(normalized_query, nl_names, nl_lookup, brand_index, brand_norm)
            top1_name, top1_score = candidates[0]
            top2_name, top2_score = candidates[1]
            top3_name, top3_score = candidates[2]

        # Risk flags as string
        risk_flags_str = '; '.join(str(f) for f in breakdown.get('risk_flags', []))

        diag_row = {
            # Sheet tracking
            'source_sheet_name': sheet_name,
            'source_row_index': idx,

            # Raw & normalized
            'original_brand': input_brand,
            'original_product_name': original_product_name,
            'original_category': original_category,
            'original_storage': original_storage,
            'normalized_query': normalized_query,

            # Match output
            'matched_on': matched_on,
            'mapped_uae_assetid': match_result.get('mapped_uae_assetid', ''),
            'match_status': match_result.get('match_status', ''),
            'match_score': match_result.get('match_score', 0),
            'confidence': match_result.get('confidence', ''),
            'method': match_result.get('method', ''),
            'auto_selected': match_result.get('auto_selected', False),
            'selection_reason': match_result.get('selection_reason', ''),
            'alternatives': alt_str,
            'nl_variant_count': nl_variant_count,

            # Critical attribute comparison
            'query_category': q_category,
            'matched_category': m_category,
            'query_storage': q_storage,
            'matched_storage': m_storage,
            'query_watch_mm': q_watch_mm,
            'matched_watch_mm': m_watch_mm,
            'query_model_tokens': str(q_model_tokens),
            'matched_model_tokens': str(m_model_tokens),

            # Diagnostic safety
            'category_match': breakdown.get('category_match'),
            'storage_match': breakdown.get('storage_match'),
            'watch_mm_match': breakdown.get('watch_mm_match'),
            'model_tokens_match': breakdown.get('model_match'),
            'risk_flags': risk_flags_str,
            'composite_score': breakdown.get('composite_score', 0.0),

            # Missed match debugging (NO_MATCH only)
            'top1_candidate_name': top1_name,
            'top1_candidate_score': top1_score,
            'top2_candidate_name': top2_name,
            'top2_candidate_score': top2_score,
            'top3_candidate_name': top3_name,
            'top3_candidate_score': top3_score,
        }

        rows.append(diag_row)

        # Progress
        if (idx + 1) % 200 == 0 or (idx + 1) == total:
            pct = (idx + 1) / total * 100
            print(f"    [{sheet_name}] {idx + 1}/{total} ({pct:.0f}%)")

    return rows


def print_summary(all_rows, per_sheet_rows):
    """Print summary statistics."""
    print("\n" + "=" * 70)
    print("DIAGNOSTIC REPORT SUMMARY")
    print("=" * 70)

    def summarize(rows, label):
        df = pd.DataFrame(rows)
        total = len(df)
        if total == 0:
            print(f"\n  {label}: 0 rows (empty)")
            return

        matched = (df['match_status'] == MATCH_STATUS_MATCHED).sum()
        review = (df['match_status'] == MATCH_STATUS_SUGGESTED).sum()
        no_match = (df['match_status'] == MATCH_STATUS_NO_MATCH).sum()
        multiple = (df['match_status'] == MATCH_STATUS_MULTIPLE).sum()
        match_rate = matched / total * 100

        # Potential false positives: MATCHED but composite_score < 80 or risk_flags not empty
        matched_rows = df[df['match_status'] == MATCH_STATUS_MATCHED]
        false_pos = matched_rows[
            (matched_rows['composite_score'] < 80) |
            (matched_rows['risk_flags'].astype(str).str.len() > 0)
        ]

        # Potential missed matches: NO_MATCH but top candidate >= 80
        no_match_rows = df[df['match_status'] == MATCH_STATUS_NO_MATCH]
        missed = no_match_rows[no_match_rows['top1_candidate_score'] >= 80]

        print(f"\n  {label} ({total} rows):")
        print(f"    MATCHED:         {matched:>5} ({matched/total*100:.1f}%)")
        print(f"    REVIEW_REQUIRED: {review:>5} ({review/total*100:.1f}%)")
        print(f"    NO_MATCH:        {no_match:>5} ({no_match/total*100:.1f}%)")
        print(f"    MULTIPLE_MATCHES:{multiple:>5} ({multiple/total*100:.1f}%)")
        print(f"    Match rate:      {match_rate:.1f}%")
        print(f"    Potential false positives: {len(false_pos)} (MATCHED with composite<80 or risk_flags)")
        print(f"    Potential missed matches:  {len(missed)} (NO_MATCH with top candidate>=80)")

    # Overall
    summarize(all_rows, "OVERALL")

    # Per-sheet
    for sheet_name, rows in per_sheet_rows.items():
        summarize(rows, sheet_name)


def main():
    start_time = time.time()

    # Load NL catalog
    nl_clean, nl_lookup, nl_names, brand_index, attribute_index = load_nl_catalog()

    # Load all sheets
    sheets = load_all_sheets()

    if not sheets:
        print("ERROR: No sheets found to process!")
        return

    # Process each sheet
    all_rows = []
    per_sheet_rows = {}

    for sheet_name, config in sheets.items():
        print(f"\nProcessing '{sheet_name}' ({len(config['df'])} rows)...")
        rows = process_sheet(
            sheet_name, config,
            nl_clean, nl_lookup, nl_names, brand_index, attribute_index,
        )
        per_sheet_rows[sheet_name] = rows
        all_rows.extend(rows)

    # Sort combined file: problems first
    status_order = {
        MATCH_STATUS_MULTIPLE: 0,
        MATCH_STATUS_SUGGESTED: 1,
        MATCH_STATUS_NO_MATCH: 2,
        MATCH_STATUS_MATCHED: 3,
    }
    all_df = pd.DataFrame(all_rows)
    all_df['_status_order'] = all_df['match_status'].map(status_order).fillna(4)
    all_df = all_df.sort_values(
        by=['_status_order', 'composite_score', 'match_score'],
        ascending=[True, True, True],
    ).drop(columns=['_status_order']).reset_index(drop=True)

    # Write combined CSV
    combined_csv = os.path.join(OUTPUT_DIR, "match_diagnostic_report_ALL.csv")
    all_df.to_csv(combined_csv, index=False, encoding='utf-8-sig')
    print(f"\nWrote {combined_csv} ({len(all_df)} rows)")

    # Write per-sheet CSVs
    for sheet_name, rows in per_sheet_rows.items():
        safe_name = sheet_name.replace(' ', '_').replace('/', '_')
        csv_path = os.path.join(OUTPUT_DIR, f"match_diagnostic_report__{safe_name}.csv")
        pd.DataFrame(rows).to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"Wrote {csv_path} ({len(rows)} rows)")

    # Write combined Excel with one tab per sheet
    xlsx_path = os.path.join(OUTPUT_DIR, "match_diagnostic_report_ALL.xlsx")
    with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
        # Combined sheet first
        all_df.to_excel(writer, sheet_name='ALL_Combined', index=False)
        # Per-sheet tabs
        for sheet_name, rows in per_sheet_rows.items():
            safe_name = sheet_name.replace(' ', '_')[:31]  # Excel tab name limit
            pd.DataFrame(rows).to_excel(writer, sheet_name=safe_name, index=False)
    print(f"Wrote {xlsx_path}")

    # Print summary
    print_summary(all_rows, per_sheet_rows)

    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed:.1f}s")
    print(f"\nFiles generated:")
    print(f"  1. {combined_csv}")
    for sheet_name in per_sheet_rows:
        safe_name = sheet_name.replace(' ', '_').replace('/', '_')
        print(f"  2. match_diagnostic_report__{safe_name}.csv")
    print(f"  3. {xlsx_path}")


if __name__ == '__main__':
    main()
