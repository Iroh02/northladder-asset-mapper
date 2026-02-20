"""
Follow-up deep analysis with corrected column mappings.
"""
import pandas as pd
import re
from collections import defaultdict

FILE = r"c:\Users\nandi\Desktop\internship northladder docs\data\Asset Mapping Lists.xlsx"

xls = pd.ExcelFile(FILE, engine='openpyxl')

# Read NL List with correct header
nl = pd.read_excel(xls, sheet_name='NorthLadder List', header=1)
nl = nl.drop(columns=['Unnamed: 0'], errors='ignore')

# Read mapping lists
list1 = pd.read_excel(xls, sheet_name='List 1', header=1)
list1 = list1.drop(columns=['Unnamed: 0'], errors='ignore')

list2 = pd.read_excel(xls, sheet_name='List 2', header=1)
list2 = list2.drop(columns=['Unnamed: 0'], errors='ignore')

ID_COL = 'uae_assetid'
NAME_COL = 'uae_assetname'
BRAND_COL = 'brand'
CAT_COL = 'category'

print("=" * 100)
print("CORRECTED DEEP ANALYSIS")
print("=" * 100)

# ── NL LIST: BASIC STATS ──────────────────────────────────────────────────
print(f"\n{'='*80}")
print("NL LIST: BASIC STATS")
print(f"{'='*80}")
print(f"  Total rows: {len(nl)}")
print(f"  Unique asset IDs: {nl[ID_COL].nunique()}")
print(f"  Unique asset names: {nl[NAME_COL].nunique()}")
print(f"  Null IDs: {nl[ID_COL].isna().sum()}")
print(f"  Null names: {nl[NAME_COL].isna().sum()}")

# ── DUPLICATE NAMES (same name, different IDs) ────────────────────────────
print(f"\n{'='*80}")
print("NL LIST: DUPLICATE NAMES (exact same name, different IDs)")
print(f"{'='*80}")
name_groups = nl.groupby(NAME_COL)[ID_COL].apply(list)
dup_names = name_groups[name_groups.apply(len) > 1]
print(f"  Names appearing with multiple IDs: {len(dup_names)}")
for name, ids in dup_names.head(25).items():
    print(f"    '{name}' -> {len(ids)} IDs: {ids[:5]}")

# ── NEAR-DUPLICATE NAMES ──────────────────────────────────────────────────
print(f"\n{'='*80}")
print("NL LIST: NEAR-DUPLICATE NAMES (differ by case/spaces/punctuation)")
print(f"{'='*80}")

def normalize(n):
    if pd.isna(n): return ''
    s = str(n).lower().strip()
    s = re.sub(r'[^a-z0-9]', '', s)
    return s

nl['_norm'] = nl[NAME_COL].apply(normalize)
norm_groups = nl.groupby('_norm')[NAME_COL].apply(lambda x: sorted(x.unique()))
near_dups = norm_groups[norm_groups.apply(len) > 1]
print(f"  Near-duplicate groups: {len(near_dups)}")
for norm_key, variants in near_dups.head(30).items():
    print(f"    Normalized='{norm_key[:60]}': {variants}")

# ── CUSTOM CONFIGURATION / PLACEHOLDER ─────────────────────────────────────
print(f"\n{'='*80}")
print("NL LIST: CUSTOM CONFIGURATION / PLACEHOLDER / TEST ENTRIES")
print(f"{'='*80}")
patterns = {
    'custom config': r'custom\s*config',
    'test / dummy': r'^test\b|^dummy\b',
    'all other / all storage / all ram': r'all\s+(other|storage|ram|models|mobiles)',
    'other brand entries': r'^other\b',
    'placeholder / tbd / n/a': r'placeholder|tbd|n/a',
    'Promo brand entries': None,  # handled separately
}
for label, pat in patterns.items():
    if pat:
        mask = nl[NAME_COL].astype(str).str.lower().str.contains(pat, na=False, regex=True)
        count = mask.sum()
        if count > 0:
            print(f"\n  '{label}': {count} rows")
            for _, row in nl[mask].head(8).iterrows():
                print(f"    [{row[CAT_COL]}] [{row[BRAND_COL]}] {row[NAME_COL]}")

# Promo brand
promo = nl[nl[BRAND_COL] == 'Promo']
print(f"\n  'Promo' brand entries: {len(promo)} rows")
for _, row in promo.head(8).iterrows():
    print(f"    [{row[CAT_COL]}] {row[NAME_COL]}")

# Others brand
others = nl[nl[BRAND_COL] == 'Others']
print(f"\n  'Others' brand entries: {len(others)} rows")
for _, row in others.head(8).iterrows():
    print(f"    [{row[CAT_COL]}] {row[NAME_COL]}")

# ── STORAGE ENCODING ──────────────────────────────────────────────────────
print(f"\n{'='*80}")
print("NL LIST: STORAGE ENCODING PATTERNS IN ASSET NAMES")
print(f"{'='*80}")

storage_pat = re.compile(r'(\d+(?:\.\d+)?)\s*(GB|TB)', re.IGNORECASE)
storage_spacing = defaultdict(lambda: defaultdict(int))
for name in nl[NAME_COL].dropna():
    for m in storage_pat.finditer(str(name)):
        full = m.group(0)
        num = m.group(1)
        unit = m.group(2).upper()
        # Check the space between num and unit
        space = full[len(num):-len(unit)]
        key = f"{num}{unit}"
        form = f"{num}{space}{unit}"
        storage_spacing[key][form] += 1

inconsistent = {k: v for k, v in storage_spacing.items() if len(v) > 1}
print(f"  Total distinct storage values: {len(storage_spacing)}")
print(f"  Inconsistent (multiple formats for same value): {len(inconsistent)}")
for key in sorted(inconsistent, key=lambda k: -sum(inconsistent[k].values())):
    variants = inconsistent[key]
    print(f"\n    {key}:")
    for form, count in sorted(variants.items(), key=lambda x: -x[1]):
        print(f"      '{form}': {count}")

# ── MODEL TOKENIZATION ────────────────────────────────────────────────────
print(f"\n{'='*80}")
print("NL LIST: MODEL TOKENIZATION INCONSISTENCIES")
print(f"{'='*80}")

names_series = nl[NAME_COL].astype(str)

# Pro Max
promax_variants = defaultdict(int)
for n in names_series:
    nl_lower = n.lower()
    if 'pro max' in nl_lower:
        promax_variants['Pro Max'] += 1
    elif 'promax' in nl_lower:
        promax_variants['ProMax'] += 1
print(f"\n  Pro Max variants: {dict(promax_variants)}")

# Z Fold
zfold_variants = defaultdict(int)
for n in names_series:
    nl_lower = n.lower()
    if 'z fold' in nl_lower:
        zfold_variants['Z Fold'] += 1
    elif 'zfold' in nl_lower:
        zfold_variants['ZFold'] += 1
    elif 'z  fold' in nl_lower:
        zfold_variants['Z  Fold'] += 1
print(f"  Z Fold variants: {dict(zfold_variants)}")

# Z Flip
zflip_variants = defaultdict(int)
for n in names_series:
    nl_lower = n.lower()
    if 'z flip' in nl_lower:
        zflip_variants['Z Flip'] += 1
    elif 'zflip' in nl_lower:
        zflip_variants['ZFlip'] += 1
print(f"  Z Flip variants: {dict(zflip_variants)}")

# S series: S23, S24, etc.
s_series = defaultdict(lambda: defaultdict(int))
for n in names_series:
    for m in re.finditer(r'\b[Ss]\s*(\d{2})\b', n):
        num = m.group(1)
        full = m.group(0)
        s_series[num][full] += 1
print(f"\n  S-series model tokenization:")
for num, variants in sorted(s_series.items()):
    if len(variants) > 1:
        print(f"    S{num}: {dict(variants)}")

# Galaxy A/M series
a_series = defaultdict(lambda: defaultdict(int))
for n in names_series:
    for m in re.finditer(r'\b[AaMm]\s*(\d{2})\b', n):
        num = m.group(1)
        full = m.group(0)
        a_series[f"{full[0].upper()}{num}"][full] += 1
for num, variants in sorted(a_series.items()):
    if len(variants) > 1:
        print(f"    {num}: {dict(variants)}")

# iPad Pro, iPad Air, iPad Mini
ipad_variants = defaultdict(int)
for n in names_series:
    if re.search(r'ipad\s*pro', n, re.IGNORECASE): ipad_variants['iPad Pro'] += 1
    if re.search(r'ipad\s*air', n, re.IGNORECASE): ipad_variants['iPad Air'] += 1
    if re.search(r'ipad\s*mini', n, re.IGNORECASE): ipad_variants['iPad Mini'] += 1
print(f"\n  iPad variants in NL list: {dict(ipad_variants)}")

# MacBook variants
mac_variants = defaultdict(int)
for n in names_series:
    if re.search(r'macbook\s*pro', n, re.IGNORECASE): mac_variants['MacBook Pro'] += 1
    if re.search(r'macbook\s*air', n, re.IGNORECASE): mac_variants['MacBook Air'] += 1
print(f"  MacBook variants: {dict(mac_variants)}")

# ── OLD vs NEW BRANDS ─────────────────────────────────────────────────────
print(f"\n{'='*80}")
print("NL LIST: BRAND ISSUES - 'OLD' vs 'New' vs PLAIN BRANDS")
print(f"{'='*80}")
brand_counts = nl[BRAND_COL].value_counts()
old_brands = {b: c for b, c in brand_counts.items() if 'OLD' in str(b).upper() or 'New' in str(b)}
plain_brands = {}
for b in old_brands:
    base = re.sub(r'\s*(OLD|New|old|new)\s*', '', str(b)).strip()
    if base in brand_counts.index:
        plain_brands[base] = brand_counts[base]
print(f"  OLD/New brand variants:")
for b, c in sorted(old_brands.items()):
    base = re.sub(r'\s*(OLD|New|old|new)\s*', '', str(b)).strip()
    plain_c = plain_brands.get(base, 0)
    print(f"    '{b}': {c} rows   (plain '{base}': {plain_c} rows)")

# Full brand distribution
print(f"\n  Full brand distribution (all {brand_counts.shape[0]} brands):")
for b, c in brand_counts.items():
    print(f"    {str(b):40s}: {c}")

# ── LIST 1 ANALYSIS ───────────────────────────────────────────────────────
print(f"\n{'='*80}")
print("LIST 1 ANALYSIS")
print(f"{'='*80}")
print(f"  Shape: {list1.shape}")
print(f"  Columns: {list(list1.columns)}")
print(f"  Type distribution:")
for t, c in list1['type'].value_counts(dropna=False).items():
    print(f"    {str(t):20s}: {c}")

L1_NAME = 'name'
L1_BRAND = 'manufacturer'

print(f"\n  Brand distribution (top 25):")
for b, c in list1[L1_BRAND].value_counts().head(25).items():
    print(f"    {str(b):20s}: {c}")

# Variant naming in List 1
print(f"\n  Variant naming examples:")
variant_pats = {
    'Fold': r'fold', 'Flip': r'flip', 'Pro Max': r'pro\s*max',
    'Ultra': r'ultra', 'Plus': r'plus', 'Lite': r'lite',
    'Mini': r'mini', 'SE': r'\bse\b', 'FE': r'\bfe\b',
    'Note': r'note', 'Edge': r'edge', 'Neo': r'neo',
}
for vname, vpat in variant_pats.items():
    matches = list1[list1[L1_NAME].astype(str).str.lower().str.contains(vpat, na=False, regex=True)]
    if len(matches) > 0:
        examples = matches[L1_NAME].head(4).tolist()
        print(f"    '{vname}': {len(matches)} entries | e.g. {examples}")

# ── LIST 2 ANALYSIS ───────────────────────────────────────────────────────
print(f"\n{'='*80}")
print("LIST 2 ANALYSIS")
print(f"{'='*80}")
L2_NAME = 'Foxway Product Name '
L2_BRAND = 'Brand'
L2_CAT = 'Category'
print(f"  Shape: {list2.shape}")
print(f"  Columns: {list(list2.columns)}")
print(f"\n  Category distribution:")
for cat, c in list2[L2_CAT].value_counts(dropna=False).items():
    print(f"    {str(cat):20s}: {c}")

print(f"\n  Brand distribution (top 30):")
for b, c in list2[L2_BRAND].value_counts().head(30).items():
    print(f"    {str(b):30s}: {c}")

# Custom configuration in List 2
custom_mask = list2[L2_NAME].astype(str).str.lower().str.contains('custom config', na=False)
print(f"\n  'Custom configuration' entries: {custom_mask.sum()}")
for _, row in list2[custom_mask].head(10).iterrows():
    print(f"    [{row[L2_CAT]}] [{row[L2_BRAND]}] {row[L2_NAME]}")

# Variant naming in List 2
print(f"\n  Variant naming examples:")
for vname, vpat in variant_pats.items():
    matches = list2[list2[L2_NAME].astype(str).str.lower().str.contains(vpat, na=False, regex=True)]
    if len(matches) > 0:
        examples = matches[L2_NAME].head(4).tolist()
        print(f"    '{vname}': {len(matches)} entries | e.g. {examples}")

# Brand inconsistencies between lists
print(f"\n{'='*80}")
print("CROSS-LIST BRAND NAME MISMATCHES")
print(f"{'='*80}")
nl_brands = set(nl[BRAND_COL].dropna().str.strip().unique())
l1_brands = set(list1[L1_BRAND].dropna().str.strip().unique())
l2_brands = set(list2[L2_BRAND].dropna().str.strip().unique())

print(f"  NL brands: {sorted(nl_brands)}")
print(f"\n  List 1 brands: {sorted(l1_brands)}")
print(f"\n  List 2 brands: {sorted(l2_brands)}")

print(f"\n  Brands in List 1 NOT in NL (exact match): {sorted(l1_brands - nl_brands)}")
print(f"  Brands in List 2 NOT in NL (exact match): {sorted(l2_brands - nl_brands)}")
print(f"  Brands in NL NOT in List 1: {sorted(nl_brands - l1_brands)}")
print(f"  Brands in NL NOT in List 2: {sorted(nl_brands - l2_brands)}")

# Case-insensitive brand comparison
def norm_brand(b):
    return str(b).lower().strip().replace("'", "").replace("-", " ")

nl_brands_norm = {norm_brand(b): b for b in nl_brands}
l1_brands_norm = {norm_brand(b): b for b in l1_brands}
l2_brands_norm = {norm_brand(b): b for b in l2_brands}

print(f"\n  Brand name variants (same normalized, different surface):")
all_norms = set(nl_brands_norm) | set(l1_brands_norm) | set(l2_brands_norm)
for nb in sorted(all_norms):
    forms = []
    if nb in nl_brands_norm: forms.append(f"NL:'{nl_brands_norm[nb]}'")
    if nb in l1_brands_norm: forms.append(f"L1:'{l1_brands_norm[nb]}'")
    if nb in l2_brands_norm: forms.append(f"L2:'{l2_brands_norm[nb]}'")
    if len(forms) >= 2:
        # Check if they differ
        surface_forms = []
        if nb in nl_brands_norm: surface_forms.append(nl_brands_norm[nb])
        if nb in l1_brands_norm: surface_forms.append(l1_brands_norm[nb])
        if nb in l2_brands_norm: surface_forms.append(l2_brands_norm[nb])
        if len(set(surface_forms)) > 1:
            print(f"    {nb}: {', '.join(forms)}")

# OnePlus naming issue specifically
print(f"\n  OnePlus naming check:")
for src, brands in [('NL', nl_brands), ('L1', l1_brands), ('L2', l2_brands)]:
    for b in sorted(brands):
        if 'one' in b.lower() or 'plus' in b.lower():
            print(f"    {src}: '{b}'")

# ── CATEGORY MAPPING ACROSS SHEETS ────────────────────────────────────────
print(f"\n{'='*80}")
print("CATEGORY MAPPING ACROSS SHEETS")
print(f"{'='*80}")
nl_cats = set(nl[CAT_COL].dropna().unique())
l1_types = set(list1['type'].dropna().unique())
l2_cats = set(list2[L2_CAT].dropna().unique())
print(f"  NL categories: {sorted(nl_cats)}")
print(f"  List 1 types: {sorted(l1_types)}")
print(f"  List 2 categories: {sorted(l2_cats)}")

# ── SAMPLE NAME PATTERNS FOR MATCHING DIFFICULTY ──────────────────────────
print(f"\n{'='*80}")
print("SAMPLE NL NAMES BY CATEGORY (first 10 each)")
print(f"{'='*80}")
for cat in sorted(nl[CAT_COL].dropna().unique()):
    subset = nl[nl[CAT_COL] == cat]
    print(f"\n  --- {cat} ({len(subset)} rows) ---")
    for _, row in subset.head(10).iterrows():
        print(f"    [{row[BRAND_COL]:15s}] {row[NAME_COL]}")

# ── SPECIAL CHARACTERS IN NAMES ───────────────────────────────────────────
print(f"\n{'='*80}")
print("SPECIAL CHARACTERS / UNUSUAL PATTERNS IN NL NAMES")
print(f"{'='*80}")
# Year patterns like (2014)
year_pattern = nl[NAME_COL].astype(str).str.contains(r'\(\d{4}\)', na=False)
print(f"  Names with year in parentheses e.g. '(2014)': {year_pattern.sum()}")

# Comma-separated parts
comma_pattern = nl[NAME_COL].astype(str).str.contains(',', na=False)
print(f"  Names containing commas: {comma_pattern.sum()}")

# Slash-separated
slash_pattern = nl[NAME_COL].astype(str).str.contains('/', na=False)
print(f"  Names containing slashes: {slash_pattern.sum()}")

# Double spaces
dbl_space = nl[NAME_COL].astype(str).str.contains(r'  ', na=False)
print(f"  Names with double spaces: {dbl_space.sum()}")

# Show some examples of each
print(f"\n  Examples with commas:")
for n in nl[comma_pattern][NAME_COL].head(5).values:
    print(f"    {n}")

print(f"\n  Examples with slashes:")
for n in nl[slash_pattern][NAME_COL].head(5).values:
    print(f"    {n}")

print(f"\n  Examples with double spaces:")
for n in nl[dbl_space][NAME_COL].head(5).values:
    print(f"    {n}")

# ── NL NAME STRUCTURE ANALYSIS ────────────────────────────────────────────
print(f"\n{'='*80}")
print("NL NAME STRUCTURE ANALYSIS")
print(f"{'='*80}")

# Analyze typical name structure: Brand + Model + Year + Storage
# Count how many have brand prefix
brand_prefix = 0
no_brand_prefix = 0
for _, row in nl.iterrows():
    name = str(row[NAME_COL])
    brand = str(row[BRAND_COL])
    if name.lower().startswith(brand.lower()):
        brand_prefix += 1
    else:
        no_brand_prefix += 1
print(f"  Names starting with brand: {brand_prefix}")
print(f"  Names NOT starting with brand: {no_brand_prefix}")

# Show examples where name doesn't start with brand
no_prefix = nl[~nl.apply(lambda r: str(r[NAME_COL]).lower().startswith(str(r[BRAND_COL]).lower()), axis=1)]
print(f"\n  Examples where name doesn't start with brand:")
for _, row in no_prefix.head(15).iterrows():
    print(f"    Brand='{row[BRAND_COL]}', Name='{row[NAME_COL]}'")

# ── LAST 20 ROWS OF NL (check for junk) ──────────────────────────────────
print(f"\n{'='*80}")
print("LAST 20 ROWS OF NL LIST (check for trailing junk)")
print(f"{'='*80}")
for _, row in nl.tail(20).iterrows():
    print(f"  [{row[CAT_COL]}] [{row[BRAND_COL]}] [{row[ID_COL][:12]}...] {row[NAME_COL]}")

nl.drop(columns=['_norm'], inplace=True, errors='ignore')
print(f"\n{'='*80}")
print("ANALYSIS COMPLETE")
print(f"{'='*80}")
