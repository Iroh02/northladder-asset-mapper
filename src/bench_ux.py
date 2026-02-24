"""Benchmark: V2 UX patch — review_summary, review_priority, empty-row downgrade, analyst sheets."""
import sys, os, json
sys.path.insert(0, os.path.dirname(__file__))
import pandas as pd
from matcher_v2 import (
    run_matching, build_nl_lookup, build_brand_index, build_attribute_index,
    build_signature_index, load_nl_reference, generate_safety_audit_v2,
    generate_schema_audit_v2, self_test_verification,
    MATCH_STATUS_MATCHED, MATCH_STATUS_SUGGESTED, MATCH_STATUS_NO_MATCH,
)
from matcher_v1 import run_matching as run_matching_v1
from matcher_v1 import (
    build_nl_lookup as v1_build_nl_lookup, build_brand_index as v1_build_brand_index,
    build_attribute_index as v1_build_attr_index, build_signature_index as v1_build_sig_index,
)

print("=" * 70)
print("V2 UX PATCH BENCHMARK")
print("=" * 70)

nl_clean, _ = load_nl_reference()
nl_lookup = build_nl_lookup(nl_clean)
nl_names = list(nl_lookup.keys())
brand_index = build_brand_index(nl_clean)
attr_index = build_attribute_index(nl_clean)
sig_index = build_signature_index(nl_clean)

v1_lookup = v1_build_nl_lookup(nl_clean)
v1_names = list(v1_lookup.keys())
v1_brand = v1_build_brand_index(nl_clean)
v1_attr = v1_build_attr_index(nl_clean)
v1_sig = v1_build_sig_index(nl_clean)

DATA = os.path.join(os.path.dirname(__file__), '..', 'data', 'Asset Mapping Lists.xlsx')
df2 = pd.read_excel(DATA, sheet_name='List 2', header=1)
df1 = pd.read_excel(DATA, sheet_name='List 1', skiprows=2)
fails = []

# TEST 0: Self-tests
print("\n--- TEST 0: Self-tests ---")
sf = self_test_verification()
print(f"  {'PASS' if not sf else f'FAIL ({len(sf)})'}")
if sf:
    fails.append(f"Self-test: {len(sf)} failures")

# TEST 1: V1 unchanged
print("\n--- TEST 1: V1 MATCHED unchanged ---")
v1 = run_matching_v1(df2, 'Brand', 'Foxway Product Name ', v1_lookup, v1_names,
    brand_index=v1_brand, attribute_index=v1_attr, nl_catalog=nl_clean, signature_index=v1_sig)
v1m = int((v1['match_status'] == MATCH_STATUS_MATCHED).sum())
print(f"  V1 MATCHED: {v1m}")
if v1m != 2353:
    fails.append(f"V1 MATCHED: {v1m} != 2353")

# TEST 2: V2 run
print("\n--- TEST 2: V2 aggressive ---")
v2 = run_matching(df2, 'Brand', 'Foxway Product Name ', nl_lookup, nl_names,
    brand_index=brand_index, attribute_index=attr_index, nl_catalog=nl_clean,
    signature_index=sig_index, widen_mode='aggressive')
v2m = int((v2['match_status'] == MATCH_STATUS_MATCHED).sum())
v2r = int((v2['match_status'] == MATCH_STATUS_SUGGESTED).sum())
v2n = int((v2['match_status'] == MATCH_STATUS_NO_MATCH).sum())
print(f"  V2: MATCHED={v2m}, REVIEW={v2r}, NO_MATCH={v2n}")
# V2 may have MORE correct matches than V1 due to parsing fixes (Xiaomi, OnePlus).
# Allow up to +50 over V1 for legitimate improvements; flag >50 as suspicious.
if v2m > v1m + 50:
    fails.append(f"V2 MATCHED ({v2m}) > V1+50 ({v1m + 50})")
    print(f"  FAIL: V2 MATCHED exceeds V1 by {v2m - v1m} (threshold: 50)")
else:
    delta = v2m - v1m
    print(f"  PASS (V2-V1 delta: {'+' if delta >= 0 else ''}{delta})")

# TEST 3: review_summary populated for all REVIEW rows
print("\n--- TEST 3: review_summary ---")
review = v2[v2['match_status'] == MATCH_STATUS_SUGGESTED]
has_col = 'review_summary' in v2.columns
summary_populated = 0
if has_col:
    summary_populated = int(review['review_summary'].fillna('').str.len().gt(0).sum())
print(f"  Column present: {has_col}")
print(f"  Review rows: {len(review)}, with summary: {summary_populated}")
if has_col and len(review) > 0:
    pct = summary_populated / len(review) * 100
    print(f"  Coverage: {pct:.1f}%")
    if pct < 95:
        fails.append(f"review_summary coverage {pct:.1f}%")
else:
    if not has_col:
        fails.append("review_summary column missing")

# TEST 4: review_priority populated
print("\n--- TEST 4: review_priority ---")
has_pri = 'review_priority' in v2.columns
if has_pri:
    pri_vals = review['review_priority']
    print(f"  Min: {pri_vals.min():.1f}, Max: {pri_vals.max():.1f}, Mean: {pri_vals.mean():.1f}")
    print(f"  PASS")
else:
    fails.append("review_priority column missing")
    print(f"  FAIL: column missing")

# TEST 5: No empty review rows (Task 4)
print("\n--- TEST 5: No empty REVIEW rows ---")
empty_review = 0
for _, r in review.iterrows():
    has = False
    for col in ('alternatives', 'blocked_candidates'):
        raw = r.get(col, '[]')
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else (raw or [])
            if parsed:
                has = True
                break
        except Exception:
            pass
    if not has:
        empty_review += 1
print(f"  Empty REVIEW rows: {empty_review}")
if empty_review > 0:
    fails.append(f"{empty_review} empty REVIEW rows remain")
    print(f"  FAIL")
else:
    print(f"  PASS: all REVIEW rows have candidates")

# TEST 6: JSON validity
print("\n--- TEST 6: JSON validity ---")
jf = 0
for col in ('alternatives', 'blocked_candidates'):
    if col in v2.columns:
        for val in v2[col]:
            if isinstance(val, str) and val.strip():
                try:
                    json.loads(val)
                except Exception:
                    jf += 1
print(f"  JSON failures: {jf}")
if jf:
    fails.append(f"JSON failures: {jf}")

# TEST 7: Safety audit
print("\n--- TEST 7: Safety audit ---")
sa = generate_safety_audit_v2({'test': v2})
for _, r in sa.iterrows():
    name = r['audit']
    status = r['status']
    count = r['count']
    print(f"  {name}: count={count}, status={status}")
    if status == 'FAIL':
        fails.append(f"Safety: {name}={count}")

# TEST 8: NO_MATCH with review_summary
print("\n--- TEST 8: NO_MATCH review_summary ---")
nomatch = v2[v2['match_status'] == MATCH_STATUS_NO_MATCH]
if 'review_summary' in nomatch.columns:
    nm_with_summary = int(nomatch['review_summary'].fillna('').str.len().gt(0).sum())
    print(f"  NO_MATCH rows: {len(nomatch)}, with summary: {nm_with_summary}")
    pct = nm_with_summary / len(nomatch) * 100 if len(nomatch) > 0 else 100
    print(f"  Coverage: {pct:.1f}%")
    if pct < 90:
        fails.append(f"NO_MATCH summary coverage {pct:.1f}%")

# TEST 9: V2 conservative List 1
print("\n--- TEST 9: V2 conservative (List 1) ---")
v1_l1 = run_matching_v1(df1, df1.columns[0], df1.columns[1], v1_lookup, v1_names,
    brand_index=v1_brand, attribute_index=v1_attr, nl_catalog=nl_clean, signature_index=v1_sig)
v1_l1_rev = int((v1_l1['match_status'] == MATCH_STATUS_SUGGESTED).sum())
v2_l1 = run_matching(df1, df1.columns[0], df1.columns[1], nl_lookup, nl_names,
    brand_index=brand_index, attribute_index=attr_index, nl_catalog=nl_clean,
    signature_index=sig_index, widen_mode='conservative')
v2_l1_rev = int((v2_l1['match_status'] == MATCH_STATUS_SUGGESTED).sum())
delta = v2_l1_rev - v1_l1_rev
print(f"  V1 L1 REVIEW={v1_l1_rev}, V2 conservative L1 REVIEW={v2_l1_rev} (delta={delta})")
if delta > 100:
    fails.append(f"L1 review jump: {delta}")

print("\n" + "=" * 70)
if fails:
    print(f"RESULT: {len(fails)} FAILURES")
    for f in fails:
        print(f"  - {f}")
else:
    print("RESULT: ALL PASS")
print("=" * 70)
