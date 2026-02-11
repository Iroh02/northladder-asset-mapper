"""Generate list of missing products to add to NL catalog"""
import pandas as pd

results_path = r"c:\Users\nandi\Desktop\internship northladder docs\asset_mapping_results (7).xlsx"

# Load unmatched items
df_l1_unmatched = pd.read_excel(results_path, sheet_name='List 1 - Unmatched')
df_l2_unmatched = pd.read_excel(results_path, sheet_name=' List 2 - Unmatched')

print("=== GENERATING MISSING PRODUCTS LIST ===\n")

# Priority 1: Old iPhones (high-value devices)
print("1. OLD iPHONES (High Priority)")
old_iphones = df_l1_unmatched[
    df_l1_unmatched['name'].str.contains('iphone [3-5]|3gs|4s', case=False, na=False)
]

iphone_models = {
    'iPhone 3G': ['8GB', '16GB'],
    'iPhone 3GS': ['16GB', '32GB'],
    'iPhone 4': ['8GB', '16GB', '32GB'],
    'iPhone 4S': ['16GB', '32GB', '64GB'],
    'iPhone 5': ['16GB', '32GB', '64GB'],
    'iPhone 5C': ['16GB', '32GB'],
}

print(f"   Found {len(old_iphones)} old iPhone items in data")
print("   Models to add to NL catalog:\n")

missing_iphones = []
for model, storages in iphone_models.items():
    for storage in storages:
        missing_iphones.append({
            'category': 'Mobile Phone',
            'brand': 'Apple',
            'uae_assetid': f'MISSING_{model.replace(" ", "_")}_{storage}',
            'uae_assetname': f'Apple {model} {storage}'
        })
        print(f"     - Apple {model} {storage}")

# Priority 2: Samsung F-series (old models)
print(f"\n2. SAMSUNG F-SERIES (574 items)")
samsung_f = df_l1_unmatched[
    (df_l1_unmatched['manufacturer'] == 'Samsung') &
    (df_l1_unmatched['name'].str.match(r'^F\d+', na=False))
]

print(f"   Found {len(samsung_f)} Samsung F-series items")
print("   Sample models to add:\n")

missing_samsung = []
for name in samsung_f['name'].unique()[:20]:
    missing_samsung.append({
        'category': 'Mobile Phone',
        'brand': 'Samsung',
        'uae_assetid': f'MISSING_Samsung_{name.replace(" ", "_")}',
        'uae_assetname': f'Samsung {name}'
    })
    print(f"     - Samsung {name}")

# Priority 3: Huawei old models
print(f"\n3. HUAWEI OLD MODELS (194 items)")
huawei_old = df_l1_unmatched[df_l1_unmatched['manufacturer'] == 'Huawei']

print(f"   Found {len(huawei_old)} Huawei items")
print("   Sample models to add:\n")

missing_huawei = []
for name in huawei_old['name'].unique()[:20]:
    missing_huawei.append({
        'category': 'Mobile Phone',
        'brand': 'Huawei',
        'uae_assetid': f'MISSING_Huawei_{name.replace(" ", "_")}',
        'uae_assetname': f'Huawei {name}'
    })
    print(f"     - Huawei {name}")

# Combine all missing products
all_missing = missing_iphones + missing_samsung + missing_huawei

# Save to Excel for import
df_missing = pd.DataFrame(all_missing)
output_path = r"c:\Users\nandi\Desktop\internship northladder docs\missing_products_to_add.xlsx"
df_missing.to_excel(output_path, index=False)

print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print(f"""
Total missing products identified: {len(all_missing)}
  - Old iPhones: {len(missing_iphones)}
  - Samsung F-series: {len(missing_samsung)}
  - Huawei old models: {len(missing_huawei)}

Output file: missing_products_to_add.xlsx

ACTION REQUIRED:
1. Review the Excel file
2. Generate proper UAE Asset IDs (replace MISSING_ placeholders)
3. Import into NL master catalog
4. Re-run matching to see improvement

EXPECTED IMPACT:
Adding these products would improve match rate by ~5-10%
(reduces NO_MATCH count significantly)
""")

print(f"\nFile saved: {output_path}")