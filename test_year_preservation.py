"""Test that keeping years in normalization prevents false duplicates"""
from matcher import normalize_text

print("=== TESTING YEAR PRESERVATION ===\n")

test_cases = [
    # iPhone SE variants (should be DIFFERENT after normalization)
    ("Apple iPhone SE (2016), 64GB", "apple iphone se 2016 64gb"),
    ("Apple iPhone SE (2020), 64GB", "apple iphone se 2020 64gb"),
    ("Apple iPhone SE (2022), 64GB", "apple iphone se 2022 64gb"),

    # User input format (should match corresponding NL entries)
    ("iPhone SE 2016 64GB", "apple iphone se 2016 64gb"),
    ("iPhone SE 2020 64GB", "apple iphone se 2020 64gb"),
    ("iPhone SE 2022 64GB", "apple iphone se 2022 64gb"),

    # MacBook examples
    ("Apple, MacBook Air, 2015, 13 Inch, Retina, Core i5, 8 GB, 256 GB",
     "apple macbook air 2015 13 inch retina core i5 8gb 256gb"),
    ("Apple, MacBook Air, 2017, 13 Inch, Retina, Core i5, 8 GB, 128 GB",
     "apple macbook air 2017 13 inch retina core i5 8gb 128gb"),

    # Huawei P Smart
    ("Huawei, P Smart Series, P Smart (2017), 64 GB", "huawei p smart series p smart 2017 64gb"),
    ("Huawei, P Smart Series, P Smart (2019), 64 GB", "huawei p smart series p smart 2019 64gb"),
]

print("Testing normalization with year preservation:\n")
all_pass = True
for input_text, expected in test_cases:
    result = normalize_text(input_text)
    status = "PASS" if result == expected else "FAIL"
    if status == "FAIL":
        all_pass = False

    print(f"{status}: \"{input_text[:60]}\"")
    print(f"       -> \"{result}\"")
    if result != expected:
        print(f"       Expected: \"{expected}\"")
    print()

print("="*70)
print("VERIFICATION")
print("="*70)

if all_pass:
    print("\nSUCCESS: All tests passed!")
    print("\nBenefit:")
    print("  - iPhone SE 2016/2020/2022 now normalize to DIFFERENT strings")
    print("  - Each year variant will get its own unique match")
    print("  - No more false MULTIPLE_MATCHES for different year models")
else:
    print("\nFAILURE: Some tests failed - check normalization logic")

# Test distinctness
print("\n" + "="*70)
print("DISTINCTNESS TEST")
print("="*70)

iphone_se_variants = [
    normalize_text("Apple iPhone SE (2016), 64GB"),
    normalize_text("Apple iPhone SE (2020), 64GB"),
    normalize_text("Apple iPhone SE (2022), 64GB"),
]

print(f"\niPhone SE variants after normalization:")
for variant in iphone_se_variants:
    print(f"  - {variant}")

if len(set(iphone_se_variants)) == 3:
    print(f"\nPASS: All 3 variants are DISTINCT (different normalized strings)")
    print("This means they will match to different UAE Asset IDs!")
else:
    print(f"\nFAIL: Variants are not distinct - they normalize to same string")

# Expected impact
print("\n" + "="*70)
print("EXPECTED IMPACT ON MULTIPLE_MATCHES")
print("="*70)
print("""
Before (year stripping):
  - iPhone SE (2016/2020/2022) all normalize to "apple iphone se 64gb"
  - Matching returns 3 IDs for all variants
  - Results: MULTIPLE_MATCHES

After (year preservation):
  - iPhone SE (2016) → "apple iphone se 2016 64gb" → 1 ID
  - iPhone SE (2020) → "apple iphone se 2020 64gb" → 1 ID
  - iPhone SE (2022) → "apple iphone se 2022 64gb" → 1 ID
  - Results: MATCHED (unique ID for each year)

Estimated reduction in MULTIPLE_MATCHES:
  - ~20 products with year variants (from duplicate analysis)
  - Each matched by ~30-50 input items
  - Total: ~600-1000 fewer MULTIPLE_MATCHES items!
""")