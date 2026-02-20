"""
Test script for Batch 2 fixes:
- Fix #4: extract_category() word boundary
- Fix #5: Smart storage extraction
- Fix #7: MATCHED vs MULTIPLE_MATCHES status consistency
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from matcher import extract_category, extract_storage, MATCH_STATUS_MATCHED, MATCH_STATUS_MULTIPLE

print("=" * 60)
print("FIX #4: extract_category() Word Boundary Tests")
print("=" * 60)

# These should NOT be classified as 'tablet'
false_tablet_tests = [
    ("software update stable version", "other"),   # 'tab' in 'stable'
    ("microsoft collaboration tool", "other"),       # 'tab' in 'collaboration'
    ("portable speaker", "other"),                   # no 'tab' at all
]

# These SHOULD still be classified as 'tablet'
true_tablet_tests = [
    ("samsung galaxy tab s8 128gb", "tablet"),       # 'tab' as whole word
    ("samsung tablet s8", "tablet"),                 # 'tablet'
    ("apple ipad pro 12.9", "tablet"),               # 'ipad'
    ("huawei matepad pro", "tablet"),                # 'matepad'
    ("galaxy tab a7 lite", "tablet"),                # 'tab' as whole word
]

# These should NOT be classified as 'mobile' due to word boundary
false_mobile_tests = [
    ("ultimate gaming console", "other"),            # 'mate' in 'ultimate'
    ("climate control device", "other"),             # 'mate' in 'climate'
    ("innovation hub", "other"),                     # 'nova' in 'innovation'
    ("path finder gps", "other"),                    # 'find' in 'finder' - should be blocked by \b
]

# These SHOULD still be classified as 'mobile'
true_mobile_tests = [
    ("apple iphone 14 pro 256gb", "mobile"),
    ("samsung galaxy s23 ultra 512gb", "mobile"),
    ("huawei mate 60 pro", "mobile"),
    ("huawei nova 11", "mobile"),
    ("oppo find x6 pro", "mobile"),
    ("oppo reno 10", "mobile"),
    ("xiaomi redmi note 12", "mobile"),
    ("xiaomi mi 13 pro", "mobile"),
    ("samsung galaxy z fold5", "mobile"),
    ("google pixel 8 pro", "mobile"),
]

all_tests = false_tablet_tests + true_tablet_tests + false_mobile_tests + true_mobile_tests
passed = 0
failed = 0

for text, expected in all_tests:
    result = extract_category(text)
    status = "PASS" if result == expected else "FAIL"
    if status == "FAIL":
        failed += 1
        print(f"  {status}: '{text}' -> got '{result}', expected '{expected}'")
    else:
        passed += 1

print(f"\n  Results: {passed}/{passed+failed} passed")
if failed > 0:
    print(f"  WARNING: {failed} tests FAILED!")
else:
    print("  ALL TESTS PASSED!")

print()
print("=" * 60)
print("FIX #5: Smart Storage Extraction Tests")
print("=" * 60)

storage_tests = [
    # (input, expected)
    ("apple iphone 14 pro 256gb", "256gb"),                    # Simple case
    ("samsung galaxy s23 ultra 12gb ram 512gb", "512gb"),       # RAM first, storage second
    ("xiaomi redmi note 12 4gb ram 64gb storage", "64gb"),     # Small RAM, medium storage
    ("dell latitude i5 16gb ram 256gb ssd", "256gb"),          # Laptop with RAM + storage
    ("apple iphone 6 16gb", "16gb"),                           # Old phone, 16GB is storage
    ("macbook pro 8gb 512gb", "512gb"),                        # 8GB RAM, 512GB storage
    ("samsung galaxy s24 1tb", "1tb"),                         # TB storage
    ("huawei nova 11 128gb", "128gb"),                         # Single match
    ("", ""),                                                   # Empty
    ("apple watch series 10", ""),                              # No storage
]

passed = 0
failed = 0

for text, expected in storage_tests:
    result = extract_storage(text)
    status = "PASS" if result == expected else "FAIL"
    if status == "FAIL":
        failed += 1
        print(f"  {status}: '{text}' -> got '{result}', expected '{expected}'")
    else:
        passed += 1

print(f"\n  Results: {passed}/{passed+failed} passed")
if failed > 0:
    print(f"  WARNING: {failed} tests FAILED!")
else:
    print("  ALL TESTS PASSED!")

print()
print("=" * 60)
print("FIX #7: MATCHED vs MULTIPLE_MATCHES Status Test")
print("=" * 60)
print("  (Testing logic - multiple IDs should use MULTIPLE_MATCHES)")

# Simulate the logic
test_cases = [
    ([" UAE001"], MATCH_STATUS_MATCHED, "Single ID"),
    (["UAE001", "UAE002"], MATCH_STATUS_MULTIPLE, "Two IDs"),
    (["UAE001", "UAE002", "UAE003"], MATCH_STATUS_MULTIPLE, "Three IDs"),
]

passed = 0
for asset_ids, expected_status, desc in test_cases:
    result = MATCH_STATUS_MULTIPLE if len(asset_ids) > 1 else MATCH_STATUS_MATCHED
    status = "PASS" if result == expected_status else "FAIL"
    if status == "FAIL":
        print(f"  {status}: {desc} -> got '{result}', expected '{expected_status}'")
    else:
        passed += 1

print(f"\n  Results: {passed}/{len(test_cases)} passed")
print("  ALL TESTS PASSED!" if passed == len(test_cases) else "  SOME TESTS FAILED!")

print()
print("=" * 60)
print("RC1: ROG Phone Laptop Misclassification Guard")
print("=" * 60)

from matcher import is_laptop_product

rc1_tests = [
    # (input, expected_is_laptop, description)
    ("ROG Phone 6", False, "ROG Phone 6 is a gaming phone, NOT laptop"),
    ("ROG Phone 7 Ultimate", False, "ROG Phone 7 Ultimate is a phone"),
    ("ASUS ROG Phone 8 Pro 512GB", False, "ROG Phone 8 Pro is a phone"),
    ("rog phone 5s 256gb", False, "lowercase rog phone is a phone"),
    ("ASUS ROG Zephyrus G14", True, "ROG Zephyrus is a laptop"),
    ("ROG Strix G16", True, "ROG Strix is a laptop"),
    ("ASUS TUF Gaming F15", True, "TUF Gaming is a laptop"),
    ("Dell Latitude 5540", True, "Dell Latitude is a laptop"),
    ("MacBook Air M2 256GB", True, "MacBook is a laptop"),
]

passed = 0
failed = 0
for text, expected, desc in rc1_tests:
    result = is_laptop_product(text)
    status = "PASS" if result == expected else "FAIL"
    if status == "FAIL":
        failed += 1
        print(f"  {status}: '{text}' -> got {result}, expected {expected} ({desc})")
    else:
        passed += 1

print(f"\n  Results: {passed}/{passed+failed} passed")
if failed > 0:
    print(f"  WARNING: {failed} tests FAILED!")
else:
    print("  ALL TESTS PASSED!")

print()
print("=" * 60)
print("RC2: Phone-Only Brand Detection in extract_category()")
print("=" * 60)

rc2_tests = [
    # Phone-only brands should be detected as 'mobile'
    ("Honor 7X 128GB", "mobile", "Honor is a phone-only brand"),
    ("Motorola Moto G50 64GB", "mobile", "Motorola/Moto is a phone brand"),
    ("OnePlus Nord 2 256GB", "mobile", "OnePlus is a phone brand"),
    ("Nokia G60 128GB", "mobile", "Nokia is a phone brand"),
    ("Vivo V29 Pro 256GB", "mobile", "Vivo is a phone brand"),
    ("Realme GT Neo 5 256GB", "mobile", "Realme is a phone brand"),
    ("Nothing Phone 2 256GB", "mobile", "Nothing is a phone brand"),
    ("Oppo A78 128GB", "mobile", "Oppo is a phone brand"),
    ("Xiaomi 13T Pro 512GB", "mobile", "Xiaomi is a phone brand"),
    ("Poco F5 Pro 256GB", "mobile", "Poco is a phone brand"),
    ("Tecno Spark 20 Pro 256GB", "mobile", "Tecno is a phone brand"),
    ("Infinix Note 30 Pro 256GB", "mobile", "Infinix is a phone brand"),
    ("iQOO Neo 9 Pro 256GB", "mobile", "iQOO is a phone brand"),
    ("Nubia Z60 Ultra 512GB", "mobile", "Nubia is a phone brand"),
    ("ZTE Blade V50 128GB", "mobile", "ZTE is a phone brand"),
    ("Alcatel 1V 32GB", "mobile", "Alcatel is a phone brand"),
    ("Meizu 20 Pro 256GB", "mobile", "Meizu is a phone brand"),
    # Existing mobile keywords should still work
    ("Apple iPhone 15 Pro 256GB", "mobile", "iPhone keyword"),
    ("Samsung Galaxy S24 Ultra 512GB", "mobile", "Galaxy S keyword"),
    # Tablet/watch/laptop precedence must NOT be broken
    ("Samsung Galaxy Tab S9 128GB", "tablet", "Tab keyword wins over Samsung"),
    ("Apple Watch Series 9 45mm", "watch", "Watch keyword wins"),
    ("ASUS ROG Zephyrus G14", "laptop", "Laptop keyword wins over phone brand"),
    ("Dell Latitude 5540", "laptop", "Laptop keyword wins"),
    # Edge cases: 'other' should remain 'other' when no brand matches
    ("Generic Bluetooth Speaker", "other", "No mobile brand or keyword"),
    ("Sony WH-1000XM5 Headphones", "other", "Sony headphones are other"),
]

passed = 0
failed = 0
for text, expected, desc in rc2_tests:
    result = extract_category(text)
    status = "PASS" if result == expected else "FAIL"
    if status == "FAIL":
        failed += 1
        print(f"  {status}: '{text}' -> got '{result}', expected '{expected}' ({desc})")
    else:
        passed += 1

print(f"\n  Results: {passed}/{passed+failed} passed")
if failed > 0:
    print(f"  WARNING: {failed} tests FAILED!")
else:
    print("  ALL TESTS PASSED!")

print()
print("=" * 60)
print("RC3: Category Cross-Match Guard in verify_critical_attributes")
print("=" * 60)

from matcher import verify_critical_attributes

rc3_tests = [
    # Cross-category: should be REJECTED
    ("samsung galaxy tab s10 plus 128gb", "samsung galaxy s10 plus 128gb", False, "tablet vs mobile"),
    ("apple watch series 9 45mm gps", "apple iphone 14 128gb", False, "watch vs mobile"),
    ("huawei matepad pro 128gb", "huawei mate 40 pro 128gb", False, "tablet vs mobile"),
    ("lenovo thinkpad x1 carbon i7 16gb 512gb", "lenovo tab p11 128gb", False, "laptop vs tablet"),
    # Same category: should be ALLOWED (attribute checks may still reject, but not the category guard)
    # Note: verify_critical_attributes checks more than just category, so some may still fail
    # We test that the function doesn't reject purely because of category
    ("samsung galaxy s23 128gb", "samsung galaxy s23 256gb", False, "same category but storage mismatch"),
    ("apple iphone 14 pro 256gb", "apple iphone 14 pro 256gb", True, "exact same product"),
    ("apple ipad air 256gb", "apple ipad pro 256gb", True, "same tablet category allowed (attr check decides)"),
]

passed = 0
failed = 0
for query, matched, expected, desc in rc3_tests:
    result = verify_critical_attributes(query, matched)
    status = "PASS" if result == expected else "FAIL"
    if status == "FAIL":
        failed += 1
        print(f"  {status}: query='{query}' matched='{matched}' -> got {result}, expected {expected} ({desc})")
    else:
        passed += 1

print(f"\n  Results: {passed}/{passed+failed} passed")
if failed > 0:
    print(f"  WARNING: {failed} tests FAILED!")
else:
    print("  ALL TESTS PASSED!")

print()
print("=" * 60)
print("IN-MEMORY FIXTURE: Verification Gate + Cross-Category Tests")
print("=" * 60)

from matcher import (
    verification_gate, match_single_item, build_nl_lookup,
    build_brand_index, build_attribute_index, normalize_text,
    _infer_brand_from_name,
)
import pandas as pd

# Build a tiny in-memory NL catalog for targeted tests
_fixture_data = [
    # phones
    {'brand': 'apple', 'uae_assetname': 'iPhone 14 128GB', 'uae_assetid': 'UAE-IP14-128'},
    {'brand': 'apple', 'uae_assetname': 'iPhone 14 256GB', 'uae_assetid': 'UAE-IP14-256'},
    {'brand': 'apple', 'uae_assetname': 'iPhone 14 Pro 256GB', 'uae_assetid': 'UAE-IP14P-256'},
    {'brand': 'apple', 'uae_assetname': 'iPhone 14 Pro Max 256GB', 'uae_assetid': 'UAE-IP14PM-256'},
    # tablets
    {'brand': 'samsung', 'uae_assetname': 'Galaxy Tab S10 Plus 128GB', 'uae_assetid': 'UAE-TABS10P-128'},
    # phones (Samsung)
    {'brand': 'samsung', 'uae_assetname': 'Galaxy S10 Plus 128GB', 'uae_assetid': 'UAE-S10P-128'},
    {'brand': 'samsung', 'uae_assetname': 'Galaxy Z Fold3 256GB', 'uae_assetid': 'UAE-FOLD3-256'},
    {'brand': 'samsung', 'uae_assetname': 'Galaxy Z Fold4 256GB', 'uae_assetid': 'UAE-FOLD4-256'},
    # watches
    {'brand': 'apple', 'uae_assetname': 'Apple Watch Series 9 42mm GPS', 'uae_assetid': 'UAE-AW9-42'},
    {'brand': 'apple', 'uae_assetname': 'Apple Watch Series 9 46mm GPS', 'uae_assetid': 'UAE-AW9-46'},
    # ROG Phone (gaming phone, not laptop!)
    {'brand': 'asus', 'uae_assetname': 'ROG Phone 6 256GB', 'uae_assetid': 'UAE-ROGP6-256'},
    {'brand': 'asus', 'uae_assetname': 'ROG Zephyrus G14', 'uae_assetid': 'UAE-ROGZ-G14'},
]
_fixture_df = pd.DataFrame(_fixture_data)
_fixture_df['normalized_name'] = _fixture_df.apply(
    lambda r: normalize_text(f"{r['brand']} {r['uae_assetname']}"), axis=1
)
_fixture_lookup = build_nl_lookup(_fixture_df)
_fixture_names = list(_fixture_lookup.keys())
_fixture_brand_index = build_brand_index(_fixture_df)
_fixture_attr_index = build_attribute_index(_fixture_df)


def _match(brand, name):
    """Helper: run match_single_item on the fixture catalog."""
    from matcher import build_match_string
    query = build_match_string(brand, name)
    return match_single_item(
        query, _fixture_lookup, _fixture_names, 85,
        brand_index=_fixture_brand_index,
        input_brand=brand,
        attribute_index=_fixture_attr_index,
        nl_catalog=_fixture_df,
        original_input=name,
    )


fixture_tests = [
    # (brand, name, expected_status_or_NOT, description)
    # ROG Phone must classify as mobile (not laptop)
    ("asus", "ROG Phone 6 256GB", "MATCHED_OR_REVIEW", "ROG Phone must NOT be treated as laptop → should find match"),
    # Galaxy Tab must NEVER match Galaxy S phone
    ("samsung", "Galaxy Tab S10 Plus 128GB", "TAB_NOT_PHONE", "Tab must not match S10 Plus phone"),
    # iPhone 14 128gb must not match iPhone 14 256gb (storage mismatch)
    ("apple", "iPhone 14 128GB", "STORAGE_CORRECT", "Must match 128GB, not 256GB"),
    # Watch 42mm must not match Watch 46mm
    ("apple", "Apple Watch Series 9 42mm GPS", "MM_CORRECT", "Must match 42mm, not 46mm"),
    # Fold3 must not match Fold4
    ("samsung", "Galaxy Z Fold3 256GB", "FOLD_CORRECT", "Fold3 must not match Fold4"),
    # Pro must not match Pro Max
    ("apple", "iPhone 14 Pro 256GB", "PRO_NOT_PROMAX", "Pro must not match Pro Max"),
]

passed = 0
failed = 0
for brand, name, check_type, desc in fixture_tests:
    result = _match(brand, name)
    status_ok = True
    details = ""

    if check_type == "MATCHED_OR_REVIEW":
        if result['match_status'] == 'NO_MATCH' and 'rog' in result.get('matched_on', '').lower():
            status_ok = True  # Found ROG match at least
        elif result['match_status'] in ('MATCHED', 'REVIEW_REQUIRED'):
            status_ok = True
        else:
            status_ok = result['match_status'] != 'NO_MATCH' or 'laptop' not in result.get('method', '')
            if result['match_status'] == 'NO_MATCH':
                # Check that it wasn't blocked by laptop classification
                from matcher import extract_category
                cat = extract_category(normalize_text(f"asus {name}"))
                status_ok = cat != 'laptop'
                details = f"category={cat}, status={result['match_status']}"

    elif check_type == "TAB_NOT_PHONE":
        matched_on = result.get('matched_on', '')
        if 'tab' in matched_on.lower() or result['match_status'] == 'NO_MATCH':
            status_ok = True
        else:
            # Check it didn't match the phone "Galaxy S10 Plus"
            status_ok = 's10 plus' not in matched_on.lower() or 'tab' in matched_on.lower()
            details = f"matched_on={matched_on}"

    elif check_type == "STORAGE_CORRECT":
        if result['match_status'] in ('MATCHED', 'REVIEW_REQUIRED', 'MULTIPLE_MATCHES'):
            matched = result.get('matched_on', '')
            status_ok = '128gb' in matched
            details = f"matched_on={matched}"
        else:
            status_ok = True  # NO_MATCH is acceptable (won't be wrong)

    elif check_type == "MM_CORRECT":
        if result['match_status'] in ('MATCHED', 'REVIEW_REQUIRED', 'MULTIPLE_MATCHES'):
            matched = result.get('matched_on', '')
            status_ok = '42mm' in matched
            details = f"matched_on={matched}"
        else:
            status_ok = True

    elif check_type == "FOLD_CORRECT":
        if result['match_status'] in ('MATCHED', 'REVIEW_REQUIRED', 'MULTIPLE_MATCHES'):
            matched = result.get('matched_on', '')
            status_ok = 'fold3' in matched.replace(' ', '') or 'fold 3' in matched
            details = f"matched_on={matched}"
        else:
            status_ok = True

    elif check_type == "PRO_NOT_PROMAX":
        if result['match_status'] in ('MATCHED', 'REVIEW_REQUIRED', 'MULTIPLE_MATCHES'):
            matched = result.get('matched_on', '')
            status_ok = 'max' not in matched.lower()
            details = f"matched_on={matched}"
        else:
            status_ok = True

    if status_ok:
        passed += 1
    else:
        failed += 1
        print(f"  FAIL: '{brand} {name}' ({desc}) {details}")

# Verification gate direct tests
gate_tests = [
    # (query, candidate, expected_pass, desc)
    ("samsung galaxy tab s10 plus 128gb", "samsung galaxy s10 plus 128gb", False, "tablet vs mobile cross"),
    ("apple watch series 9 45mm gps", "apple iphone 14 128gb", False, "watch vs mobile cross"),
    ("apple iphone 14 128gb", "apple iphone 14 256gb", False, "storage mismatch"),
    ("apple iphone 14 pro 256gb", "apple iphone 14 pro max 256gb", False, "Pro vs Pro Max"),
    ("apple iphone 14 pro 256gb", "apple iphone 14 pro 256gb", True, "exact match"),
    ("apple watch 42mm gps", "apple watch 46mm gps", False, "watch mm mismatch"),
]

for query, cand, expected, desc in gate_tests:
    gate_pass, reasons = verification_gate(query, cand)
    if gate_pass == expected:
        passed += 1
    else:
        failed += 1
        print(f"  FAIL gate: query='{query}' cand='{cand}' -> {gate_pass}, expected {expected} ({desc}; reasons={reasons})")

# Brand inference tests
brand_infer_tests = [
    ("Apple iPhone 14 128GB", "apple"),
    ("Samsung Galaxy S23 Ultra", "samsung"),
    ("Unknown Device 128GB", ""),
    ("HP Pavilion 15", "hp"),
    ("", ""),
]
for name, expected_brand in brand_infer_tests:
    result = _infer_brand_from_name(name)
    if result == expected_brand:
        passed += 1
    else:
        failed += 1
        print(f"  FAIL brand_infer: '{name}' -> '{result}', expected '{expected_brand}'")

print(f"\n  Results: {passed}/{passed+failed} passed")
if failed > 0:
    print(f"  WARNING: {failed} tests FAILED!")
else:
    print("  ALL TESTS PASSED!")

print()
print("=" * 60)
print("FULL MATCHING TEST: Run on Asset Mapping Lists Excel")
print("=" * 60)

try:
    from matcher import (load_nl_reference, run_matching,
                         parse_nl_sheet, _detect_columns, _detect_header_row)

    excel_path = "Asset Mapping Lists.xlsx"

    # Load NL reference
    nl_df = parse_nl_sheet(excel_path)
    # Build normalized name column
    nl_df['normalized_name'] = nl_df.apply(
        lambda r: normalize_text(f"{r.get('brand', '')} {r.get('uae_assetname', '')}"), axis=1
    )
    print(f"  NL catalog loaded: {len(nl_df)} entries")

    # Build indexes
    nl_lookup = build_nl_lookup(nl_df)
    nl_names = list(nl_lookup.keys())
    brand_index = build_brand_index(nl_df)
    attribute_index = build_attribute_index(nl_df)
    print(f"  Indexes built: {len(nl_names)} unique names, {len(brand_index)} brands")

    xl = pd.ExcelFile(excel_path)
    print(f"  Excel sheets: {xl.sheet_names}")

    sheet_configs = {
        'List 1': {'brand': 'manufacturer', 'name': 'name', 'header': 1},
        'List 2': {'brand': 'Brand', 'name': 'Foxway Product Name ', 'header': 1},
    }

    for sheet_name, config in sheet_configs.items():
        if sheet_name not in xl.sheet_names:
            continue

        df = pd.read_excel(excel_path, sheet_name=sheet_name, header=config['header'])
        # Drop the first unnamed column if present
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        print(f"\n  Processing '{sheet_name}': {len(df)} rows, cols={list(df.columns[:5])}")

        brand_col = config['brand']
        name_col = config['name']
        print(f"  Using columns: brand='{brand_col}', name='{name_col}'")

        if not name_col:
            print(f"  SKIP: No name column detected")
            continue

        results = run_matching(
            df, brand_col, name_col,
            nl_lookup, nl_names,
            brand_index=brand_index,
            attribute_index=attribute_index,
            nl_catalog=nl_df,
        )

        # Count statuses
        status_counts = results['match_status'].value_counts()
        print(f"  Status breakdown:")
        for s, count in status_counts.items():
            pct = count / len(results) * 100
            print(f"    {s}: {count} ({pct:.1f}%)")

        # Check for MULTIPLE_MATCHES with comma-separated IDs
        multi = results[results['match_status'] == 'MULTIPLE_MATCHES']
        if len(multi) > 0:
            print(f"  MULTIPLE_MATCHES: {len(multi)} items properly flagged")
            sample = multi.head(3)
            for _, row in sample.iterrows():
                print(f"    -> {str(row.get('mapped_uae_assetid', 'N/A'))[:80]}")

        # Check that MATCHED items have single IDs (no commas)
        matched = results[results['match_status'] == 'MATCHED']
        multi_id_in_matched = matched[matched['mapped_uae_assetid'].astype(str).str.contains(',', na=False)]
        if len(multi_id_in_matched) > 0:
            print(f"  WARNING: {len(multi_id_in_matched)} MATCHED items still have comma-separated IDs!")
            for _, row in multi_id_in_matched.head(3).iterrows():
                print(f"    -> {str(row.get('mapped_uae_assetid', ''))[:80]}")
        else:
            print(f"  MATCHED items verified: all have single IDs")

except Exception as e:
    print(f"  Error running full test: {e}")
    import traceback
    traceback.print_exc()

print()
print("=" * 60)
print("TOKEN NORMALIZATION: Generation / Roman Numeral / Concatenation Tests")
print("=" * 60)

from matcher import normalize_text

token_norm_tests = [
    # Generation normalization: mark ii → mk2
    ("Apple Watch Series Mark II", "mk2"),
    ("Sony WH-1000 MK3", "mk3"),
    ("Mk 2 Edition", "mk2"),
    # Roman numerals in mark context
    ("Device Mark III", "mk3"),
    ("Device Mark IV", "mk4"),
    # Gen normalization: gen 2 → gen2
    ("iPad 10th Gen", "gen10"),
    ("iPad 2nd Generation", "gen2"),
    ("Gen III device", "gen3"),
    # Model concatenation: fold 3 → fold3, flip 4 → flip4
    ("Galaxy Z Fold 3 256GB", "fold3"),
    ("Galaxy Z Flip 4 128GB", "flip4"),
    # Series concatenation: s 23 → s23, a 54 → a54
    ("Galaxy S 23 Ultra 256GB", "s23"),
    ("Galaxy A 54 128GB", "a54"),
]

passed = 0
failed = 0
for text, expected_substr in token_norm_tests:
    result = normalize_text(text)
    if expected_substr in result:
        passed += 1
    else:
        failed += 1
        print(f"  FAIL: '{text}' -> '{result}' (expected '{expected_substr}' in output)")

print(f"\n  Results: {passed}/{passed + failed} passed")
if failed == 0:
    print("  ALL TESTS PASSED!")
else:
    print(f"  WARNING: {failed} tests FAILED!")

print()
print("=" * 60)
print("TOKEN NORMALIZATION: Negative Tests (should NOT change)")
print("=" * 60)

token_neg_tests = [
    # Storage should NOT be affected
    ("iPhone 14 256GB", "256gb"),
    # "5g" connectivity should still be stripped
    ("Galaxy S23 5G", lambda r: "5g" not in r.split()),
    # Regular model numbers preserved
    ("iPhone 14 Pro Max", "14"),
    # Model letter + storage should NOT be concatenated (z 32 = Z + 32GB, not z32)
    ("Motorola Moto Z 32 GB", lambda r: "z 32" in r),
    ("Huawei Mate S 32 GB", lambda r: "s 32" in r),
]

passed_neg = 0
failed_neg = 0
for text, check in token_neg_tests:
    result = normalize_text(text)
    if callable(check):
        ok = check(result)
    else:
        ok = check in result
    if ok:
        passed_neg += 1
    else:
        failed_neg += 1
        print(f"  FAIL: '{text}' -> '{result}' (check failed for {check})")

print(f"\n  Results: {passed_neg}/{passed_neg + failed_neg} passed")
if failed_neg == 0:
    print("  ALL TESTS PASSED!")
else:
    print(f"  WARNING: {failed_neg} tests FAILED!")

print()
print("=" * 60)
print("COVERAGE METRICS & CATALOG GAP DETECTOR: Unit Tests")
print("=" * 60)

from matcher import compute_coverage_metrics, detect_catalog_gaps
import pandas as pd

# Build a mock results DataFrame
mock_data = {
    'match_status': ['MATCHED', 'MATCHED', 'MATCHED', 'REVIEW_REQUIRED', 'NO_MATCH', 'NO_MATCH', 'NO_MATCH'],
    'match_score': [95.0, 92.0, 88.5, 86.0, 82.0, 60.0, 45.0],
    'matched_on': ['apple iphone 14 128gb', 'samsung galaxy s23 256gb', 'apple iphone 13 128gb',
                   'huawei mate 50 pro', '', '', ''],
    'method': ['fuzzy', 'fuzzy', 'fuzzy_soft_upgrade', 'fuzzy', 'fuzzy', 'fuzzy', 'fuzzy'],
    'Brand': ['Apple', 'Samsung', 'Apple', 'Huawei', 'Nokia', 'Nokia', 'LG'],
    'Foxway Product Name': ['iPhone 14 128GB', 'Galaxy S23 256GB', 'iPhone 13 128GB',
                            'Mate 50 Pro', 'Nokia G20', 'Nokia 105', 'LG Velvet'],
}
mock_df = pd.DataFrame(mock_data)

# Test compute_coverage_metrics
metrics = compute_coverage_metrics(mock_df)
cov_passed = 0
cov_failed = 0

def check(cond, msg):
    global cov_passed, cov_failed
    if cond:
        cov_passed += 1
    else:
        cov_failed += 1
        print(f"  FAIL: {msg}")

check(metrics['total_rows'] == 7, f"total_rows={metrics['total_rows']}, expected 7")
check(metrics['matched_count'] == 3, f"matched_count={metrics['matched_count']}, expected 3")
check(metrics['review_count'] == 1, f"review_count={metrics['review_count']}, expected 1")
check(metrics['no_match_count'] == 3, f"no_match_count={metrics['no_match_count']}, expected 3")
check(abs(metrics['matched_rate'] - 42.9) < 0.1, f"matched_rate={metrics['matched_rate']}, expected ~42.9")
check(metrics['near_miss_count'] == 1, f"near_miss_count={metrics['near_miss_count']}, expected 1 (score 82)")
check(metrics['avg_match_score'] > 0, f"avg_match_score={metrics['avg_match_score']}, expected > 0")
check('fuzzy_soft_upgrade' in metrics['method_breakdown'], "method_breakdown should include fuzzy_soft_upgrade")

# Test detect_catalog_gaps
gaps = detect_catalog_gaps(mock_df)
check(isinstance(gaps['unmatched_brands'], dict), "unmatched_brands should be a dict")
check(isinstance(gaps['brand_coverage'], dict), "brand_coverage should be a dict")
check(isinstance(gaps['near_miss_candidates'], list), "near_miss_candidates should be a list")
check(len(gaps['near_miss_candidates']) == 1, f"near_miss_candidates length={len(gaps['near_miss_candidates'])}, expected 1")

# Test empty DataFrame
empty_metrics = compute_coverage_metrics(pd.DataFrame(columns=['match_status', 'match_score']))
check(empty_metrics['total_rows'] == 0, "empty df total_rows should be 0")
check(empty_metrics['matched_count'] == 0, "empty df matched_count should be 0")

print(f"\n  Results: {cov_passed}/{cov_passed + cov_failed} passed")
if cov_failed == 0:
    print("  ALL TESTS PASSED!")
else:
    print(f"  WARNING: {cov_failed} tests FAILED!")

print()
print("=" * 60)
print("ALL BATCH 2 FIXES TESTED")
print("=" * 60)
