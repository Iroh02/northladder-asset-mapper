"""Rebuild NL catalog with current normalization (including years!)"""
import pandas as pd
from matcher import normalize_text
import json
from pathlib import Path

print("=== REBUILDING NL CATALOG ===\n")

# Load from Asset Mapping Lists.xlsx
asset_file = r"c:\Users\nandi\Desktop\internship northladder docs\Asset Mapping Lists.xlsx"

if not Path(asset_file).exists():
    print(f"[ERROR] File not found: {asset_file}")
    exit(1)

print(f"Loading NL List from: {asset_file}")

# Load NL list from "NorthLadder List" sheet
df_nl = pd.read_excel(asset_file, sheet_name='NorthLadder List', header=None, skiprows=2)
df_nl.columns = ['empty', 'category', 'brand', 'uae_assetid', 'uae_assetname']
df_nl = df_nl[['category', 'brand', 'uae_assetid', 'uae_assetname']].dropna(subset=['uae_assetid'])

print(f"Loaded {len(df_nl)} products from NL List")

# Apply current normalization (WITH year preservation!)
print("\nApplying current normalization (with years)...")
df_nl['normalized_name'] = df_nl['uae_assetname'].apply(lambda x: normalize_text(str(x)))

# Show some iPhone SE examples
print("\nSample iPhone SE normalizations:")
se_examples = df_nl[df_nl['uae_assetname'].str.contains('iPhone SE', case=False, na=False)].head(6)
for _, row in se_examples.iterrows():
    print(f"  {row['uae_assetname']}")
    print(f"    -> {row['normalized_name']}")

# Check for duplicates
duplicates = df_nl[df_nl.duplicated(subset=['normalized_name'], keep=False)]
print(f"\nProducts with duplicate normalized names: {len(duplicates)}")

if len(duplicates) > 0:
    print("\nSample duplicates:")
    for norm_name in duplicates['normalized_name'].unique()[:5]:
        dupes = df_nl[df_nl['normalized_name'] == norm_name]
        print(f"\n  '{norm_name}' ({len(dupes)} entries):")
        for _, row in dupes.iterrows():
            print(f"    - {row['uae_assetid']}: {row['uae_assetname']}")

# Save to parquet
output_path = 'nl_reference/nl_clean.parquet'
print(f"\nSaving to {output_path}...")

# Cast all object columns to string to avoid mixed type issues
for col in df_nl.select_dtypes(include=['object']).columns:
    df_nl[col] = df_nl[col].astype(str)

df_nl.to_parquet(output_path, index=False, engine='pyarrow')

# Save metadata
meta = {
    'total_products': len(df_nl),
    'unique_normalized': df_nl['normalized_name'].nunique(),
    'duplicates': len(df_nl) - df_nl['normalized_name'].nunique(),
}

with open('nl_reference/nl_meta.json', 'w') as f:
    json.dump(meta, f, indent=2)

print("\n" + "="*70)
print("SUCCESS!")
print("="*70)
print(f"""
NL catalog rebuilt successfully!
- Total products: {meta['total_products']:,}
- Unique normalized names: {meta['unique_normalized']:,}
- Duplicates: {meta['duplicates']:,}

Normalization now includes:
- Years preserved (2016 vs 2020 vs 2022)
- Variant keywords (Pro Max, Plus, XL, etc.)
- All improvements from recent fixes

Next step: Run fresh mapping to see the improvement!
""")
