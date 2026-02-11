"""Test attribute verification on current REVIEW items from results (8)"""
import pandas as pd
from matcher import verify_critical_attributes, normalize_text

results_path = r"c:\Users\nandi\Desktop\internship northladder docs\asset_mapping_results (8).xlsx"

print("=== TESTING ATTRIBUTE VERIFICATION ===\n")

# Load REVIEW items
df_l1_review = pd.read_excel(results_path, sheet_name='List 1 - Review')
df_l2_review = pd.read_excel(results_path, sheet_name=' List 2 - Review')

print(f"List 1 Review: {len(df_l1_review)} items")
print(f"List 2 Review: {len(df_l2_review)} items")
print(f"Total Review: {len(df_l1_review) + len(df_l2_review)} items\n")

# Test verification on List 1 Review items
print("="*70)
print("LIST 1 REVIEW - VERIFICATION TEST")
print("="*70)

l1_upgradable = 0
l1_keep_review = 0
l1_examples_upgraded = []
l1_examples_kept = []

for idx, row in df_l1_review.iterrows():
    original = row['name']
    matched = row['matched_on']
    score = row['match_score']

    # Normalize both strings (same as in matching)
    query_norm = normalize_text(f"{row['manufacturer']} {original}")
    matched_norm = normalize_text(matched)

    # Verify attributes
    verified = verify_critical_attributes(query_norm, matched_norm)

    if verified:
        l1_upgradable += 1
        if len(l1_examples_upgraded) < 5:
            l1_examples_upgraded.append((score, original, matched))
    else:
        l1_keep_review += 1
        if len(l1_examples_kept) < 5:
            l1_examples_kept.append((score, original, matched))

print(f"\nResults:")
print(f"  Would upgrade to MATCHED: {l1_upgradable} ({l1_upgradable/len(df_l1_review)*100:.1f}%)")
print(f"  Keep as REVIEW_REQUIRED: {l1_keep_review} ({l1_keep_review/len(df_l1_review)*100:.1f}%)")

print(f"\nExamples that WOULD be upgraded (first 5):")
for score, orig, match in l1_examples_upgraded:
    print(f"  [{score:.1f}%] {orig[:50]}")
    print(f"         -> {match[:50]}")
    print()

print(f"Examples that WOULD stay in review (first 5):")
for score, orig, match in l1_examples_kept:
    print(f"  [{score:.1f}%] {orig[:50]}")
    print(f"         -> {match[:50]}")
    print()

# Test verification on List 2 Review items
print("="*70)
print("LIST 2 REVIEW - VERIFICATION TEST")
print("="*70)

l2_upgradable = 0
l2_keep_review = 0
l2_examples_upgraded = []
l2_examples_kept = []

for idx, row in df_l2_review.iterrows():
    original = row['Foxway Product Name']
    matched = row['matched_on']
    score = row['match_score']

    # Normalize both strings
    query_norm = normalize_text(f"{row['Brand']} {original}")
    matched_norm = normalize_text(matched)

    # Verify attributes
    verified = verify_critical_attributes(query_norm, matched_norm)

    if verified:
        l2_upgradable += 1
        if len(l2_examples_upgraded) < 5:
            l2_examples_upgraded.append((score, original, matched))
    else:
        l2_keep_review += 1
        if len(l2_examples_kept) < 5:
            l2_examples_kept.append((score, original, matched))

print(f"\nResults:")
print(f"  Would upgrade to MATCHED: {l2_upgradable} ({l2_upgradable/len(df_l2_review)*100:.1f}%)")
print(f"  Keep as REVIEW_REQUIRED: {l2_keep_review} ({l2_keep_review/len(df_l2_review)*100:.1f}%)")

print(f"\nExamples that WOULD be upgraded (first 5):")
for score, orig, match in l2_examples_upgraded:
    print(f"  [{score:.1f}%] {orig[:50]}")
    print(f"         -> {match[:50]}")
    print()

print(f"Examples that WOULD stay in review (first 5):")
for score, orig, match in l2_examples_kept:
    print(f"  [{score:.1f}%] {orig[:50]}")
    print(f"         -> {match[:50]}")
    print()

# Overall summary
print("="*70)
print("OVERALL IMPACT")
print("="*70)

total_review = len(df_l1_review) + len(df_l2_review)
total_upgradable = l1_upgradable + l2_upgradable
total_keep = l1_keep_review + l2_keep_review

print(f"\nTotal REVIEW items: {total_review}")
print(f"  Would upgrade to MATCHED: {total_upgradable} ({total_upgradable/total_review*100:.1f}%)")
print(f"  Keep as REVIEW_REQUIRED: {total_keep} ({total_keep/total_review*100:.1f}%)")

print(f"\nBenefit:")
print(f"  - Reduces manual review workload by {total_upgradable} items")
print(f"  - Auto-accepts safe matches based on attribute verification")
print(f"  - Only keeps truly ambiguous matches for human review")

print(f"\nNext step:")
print(f"  Run a fresh mapping with the updated matcher to see the impact!")