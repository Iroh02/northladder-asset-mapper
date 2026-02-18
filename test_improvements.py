"""
Test auto-select + category filtering improvements locally.

Runs full mapping with new features and compares results.
"""
import pandas as pd
from matcher import (
    load_and_clean_nl_list,
    build_nl_lookup,
    build_brand_index,
    build_attribute_index,
    run_matching,
    parse_nl_sheet,
    parse_asset_sheets,
)

print("=" * 80)
print("TESTING AUTO-SELECT + CATEGORY FILTERING IMPROVEMENTS")
print("=" * 80)

# Load NL catalog
print("\n[1/4] Loading NL catalog...")
asset_file = r"c:\Users\nandi\Desktop\internship northladder docs\Asset Mapping Lists.xlsx"

df_nl_raw = pd.read_excel(asset_file, sheet_name='NorthLadder List', header=None, skiprows=2)
df_nl_raw.columns = ['empty', 'category', 'brand', 'uae_assetid', 'uae_assetname']
df_nl_raw = df_nl_raw[['category', 'brand', 'uae_assetid', 'uae_assetname']].dropna(subset=['uae_assetid'])

df_nl_clean, nl_stats = load_and_clean_nl_list(df_nl_raw)
nl_lookup = build_nl_lookup(df_nl_clean)
nl_names = list(nl_lookup.keys())
nl_brand_index = build_brand_index(df_nl_clean)
nl_attribute_index = build_attribute_index(df_nl_clean)

print(f"   Loaded {nl_stats['final']:,} NL products")
print(f"   Brands: {len(nl_brand_index)}")
print(f"   Attribute index size: {len(nl_attribute_index)}")

# Parse asset sheets
print("\n[2/4] Parsing asset lists...")
asset_sheets = parse_asset_sheets(asset_file)
print(f"   Found {len(asset_sheets)} sheets")
for sheet_name, info in asset_sheets.items():
    print(f"   - {sheet_name}: {len(info['df']):,} rows")

# Run matching on both lists
print("\n[3/4] Running matching with AUTO-SELECT + CATEGORY FILTERING...")

all_results = {}

for sheet_name, info in asset_sheets.items():
    print(f"\n   Matching {sheet_name}...")

    def progress_cb(current, total):
        if current % 200 == 0 or current == total:
            print(f"      Progress: {current:,}/{total:,} ({current/total*100:.1f}%)")

    df_result = run_matching(
        df_input=info['df'],
        brand_col=info['brand_col'] or '__no_brand__',
        name_col=info['name_col'],
        nl_lookup=nl_lookup,
        nl_names=nl_names,
        threshold=85,
        progress_callback=progress_cb,
        brand_index=nl_brand_index,
        attribute_index=nl_attribute_index,
        nl_catalog=df_nl_clean,
    )

    all_results[sheet_name] = df_result

# Analyze results
print("\n[4/4] Analyzing results...")
print("\n" + "=" * 80)
print("RESULTS BREAKDOWN")
print("=" * 80)

for sheet_name, df_result in all_results.items():
    print(f"\n{sheet_name}:")
    print(f"   Total items: {len(df_result):,}")

    # Count by status
    matched = (df_result['match_status'] == 'MATCHED').sum()
    multiple = (df_result['match_status'] == 'MULTIPLE_MATCHES').sum()
    review = (df_result['match_status'] == 'REVIEW_REQUIRED').sum()
    no_match = (df_result['match_status'] == 'NO_MATCH').sum()

    print(f"\n   Status breakdown:")
    print(f"      MATCHED:           {matched:4,} ({matched/len(df_result)*100:5.1f}%)")
    print(f"      MULTIPLE_MATCHES:  {multiple:4,} ({multiple/len(df_result)*100:5.1f}%)")
    print(f"      REVIEW_REQUIRED:   {review:4,} ({review/len(df_result)*100:5.1f}%)")
    print(f"      NO_MATCH:          {no_match:4,} ({no_match/len(df_result)*100:5.1f}%)")

    # Auto-select analysis
    auto_selected = df_result['auto_selected'].sum()
    if auto_selected > 0:
        print(f"\n   Auto-selection stats:")
        print(f"      Items auto-selected: {auto_selected:,}")

        # Breakdown by selection reason
        reasons = df_result[df_result['auto_selected'] == True]['selection_reason'].value_counts()
        for reason, count in reasons.items():
            print(f"         - {reason}: {count} ({count/auto_selected*100:.1f}%)")

    # Method breakdown
    print(f"\n   Method breakdown:")
    method_counts = df_result['method'].value_counts()
    for method, count in method_counts.items():
        print(f"      {method:30s}: {count:4,} ({count/len(df_result)*100:5.1f}%)")

# Overall summary
print("\n" + "=" * 80)
print("OVERALL SUMMARY")
print("=" * 80)

total_items = sum(len(df) for df in all_results.values())
total_matched = sum((df['match_status'] == 'MATCHED').sum() for df in all_results.values())
total_auto_selected = sum(df['auto_selected'].sum() for df in all_results.values())
total_usable = total_matched

print(f"\nTotal items processed: {total_items:,}")
print(f"Total MATCHED:         {total_matched:,} ({total_matched/total_items*100:.1f}%)")
print(f"   - Auto-selected:    {total_auto_selected:,} ({total_auto_selected/total_items*100:.1f}%)")
print(f"\nUsable mappings: {total_usable:,} / {total_items:,} ({total_usable/total_items*100:.1f}%)")

# Compare with expected results
print("\n" + "=" * 80)
print("COMPARISON WITH PREVIOUS RESULTS")
print("=" * 80)

print("\nBefore improvements (Results 15):")
print("   MATCHED:           ~2,806")
print("   MULTIPLE_MATCHES:  ~1,881")
print("   Total usable:      ~2,806 (only MATCHED)")

print("\nAfter improvements (Current):")
print(f"   MATCHED:           {total_matched:,}")
print(f"   MULTIPLE_MATCHES:  {sum((df['match_status'] == 'MULTIPLE_MATCHES').sum() for df in all_results.values()):,}")
print(f"   Total usable:      {total_usable:,} (MATCHED includes auto-selected)")

improvement = total_usable - 2806
improvement_pct = (improvement / 2806) * 100 if improvement > 0 else 0

print(f"\nImprovement: +{improvement:,} usable mappings ({improvement_pct:.1f}% increase)")

# Save results to Excel for inspection
output_path = r"c:\Users\nandi\Desktop\internship northladder docs\test_improvements_results_fixed.xlsx"
print(f"\n[SAVING] Results to: {output_path}")

with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    for sheet_name, df_result in all_results.items():
        safe_name = f"{sheet_name} - Mapped"[:31]
        df_result.to_excel(writer, sheet_name=safe_name, index=False)

    # Summary sheet
    summary_rows = []
    for sheet_name, df_result in all_results.items():
        total = len(df_result)
        matched = int((df_result['match_status'] == 'MATCHED').sum())
        multiple = int((df_result['match_status'] == 'MULTIPLE_MATCHES').sum())
        review = int((df_result['match_status'] == 'REVIEW_REQUIRED').sum())
        no_match = int((df_result['match_status'] == 'NO_MATCH').sum())
        auto_sel = int(df_result['auto_selected'].sum())

        summary_rows.append({
            'Sheet': sheet_name,
            'Total': total,
            'MATCHED': matched,
            'MULTIPLE_MATCHES': multiple,
            'REVIEW_REQUIRED': review,
            'NO_MATCH': no_match,
            'Auto-Selected': auto_sel,
            'Usable Rate': f"{matched/total*100:.1f}%",
        })

    pd.DataFrame(summary_rows).to_excel(writer, sheet_name='Summary', index=False)

    # Auto-select details
    auto_select_details = []
    for sheet_name, df_result in all_results.items():
        auto_selected_items = df_result[df_result['auto_selected'] == True]
        for idx, row in auto_selected_items.iterrows():
            name_col = 'name' if 'name' in row else 'Foxway Product Name'
            auto_select_details.append({
                'Sheet': sheet_name,
                'Product': row[name_col],
                'Selected ID': row['mapped_uae_assetid'],
                'Selection Reason': row['selection_reason'],
                'Alternatives': ', '.join(row['alternatives']) if row['alternatives'] else '',
                'Match Score': f"{row['match_score']:.1f}%",
            })

    if auto_select_details:
        pd.DataFrame(auto_select_details).to_excel(writer, sheet_name='Auto-Selected Details', index=False)

print(f"\n[SUCCESS] Results saved!")

print("\n" + "=" * 80)
print("NEXT STEPS")
print("=" * 80)
print("""
1. Review test_improvements_results.xlsx to verify:
   - Auto-select logic is working correctly
   - Category filtering prevented cross-category errors
   - No regression in match quality

2. If results look good:
   - Commit changes to GitHub
   - Push to trigger Streamlit Cloud redeploy
   - Test on Streamlit Cloud

3. Expected improvements:
   - MULTIPLE_MATCHES converted to MATCHED via auto-select
   - ~19 cross-category errors prevented
   - Total usable mappings increased by ~67%
""")
