# Matcher.py Quick Reference Guide

**Fast lookup for common operations and debugging**

---

## 🚀 Quick Start: How It Works

```
User uploads Excel → app.py parses → matcher.py matches → Results Excel downloaded
                         ↓
         Columns detected: Brand, Product Name
                         ↓
         For each row: match_single_item()
                         ↓
         Try: Attribute → Signature → Fuzzy
                         ↓
         Apply verification gate
                         ↓
         Return: UAE Asset ID + confidence
```

---

## 📊 Match Status Quick Reference

| Status | Score | Meaning | Action |
|--------|-------|---------|--------|
| **MATCHED** | ≥90% + gate passes | High confidence, auto-apply | ✅ Use directly |
| **REVIEW_REQUIRED** | 85-94% or gate fails | Needs verification | ⚠️ Manual review |
| **NO_MATCH** | <85% | No confident match | ❌ Manual mapping |

---

## 🔍 Matching Methods

| Method | Speed | Coverage | When Used |
|--------|-------|----------|-----------|
| **attribute** | 2-5ms | 70-80% | Exact brand+model+storage match |
| **signature** | 1-2ms | 5-10% | Variant-specific match (M1 vs M2) |
| **fuzzy** | 10-200ms | 10-20% | String similarity fallback |

---

## 🎯 Verification Gate Checklist

**10 checks - ANY failure → REVIEW_REQUIRED**

✓ Category match (mobile ≠ tablet)
✓ Storage match (128gb ≠ 256gb)
✓ Watch mm match (38mm ≠ 46mm)
✓ Model token count ([pro] ≠ [pro, max])
✓ Material match (aluminum ≠ stainless)
✓ Edition match (Nike ≠ base)
✓ Variant match ({pro, max} ≠ {pro})
✓ Model code match (ZE552KL ≠ ZE520KL)
✓ Screen size match (10.4" ≠ 11.0")
✓ Generation match (5th ≠ 7th)

---

## 🏷️ Auto-Selection Priority

When multiple IDs match, select by:

1. **Material** (watches) → aluminum > stainless > titanium
2. **Year** → 2024 > 2023 > 2022
3. **Model Variant** → Pro Max ≠ Pro ≠ base
4. **Connectivity** → 5G > 4G
5. **First ID** → If all identical

---

## 🔧 Debugging Common Issues

### Issue: "Pro" matching "Pro Max"

**Root cause:** Token count not checked
**Fix location:** Line 3283 - `extract_model_tokens()`
**Verification:** Lines 3319-3338 - Token count check

### Issue: Year not preserved (iPhone SE 2016 = 2020)

**Root cause:** Year not extracted in attributes
**Fix location:** Line 737 - `extract_product_attributes()`
**Rebuild:** `rebuild_nl_catalog.py` to update index

### Issue: Tablet matching Phone

**Root cause:** Category filter not applied
**Fix location:** Line 2658 - Category filtering in fuzzy match
**Verification:** Line 3167 - `verification_gate()` category check

### Issue: Storage "1024gb" not matching "1tb"

**Root cause:** Storage not normalized
**Fix location:** Line 555 - `extract_storage()`
**Normalization:** "1024gb" → "1tb", "2048gb" → "2tb"

---

## 📁 File Structure

```
src/
├── matcher.py         # Main matching engine (2,400 lines)
├── app.py            # Streamlit UI
└── nl_reference/     # Pre-built NL catalog cache
    ├── nl_clean.parquet
    └── nl_meta.json

docs/
├── MATCHER_LOGIC_COMPLETE.md      # Full technical doc (this)
└── MATCHER_QUICK_REFERENCE.md     # Quick lookup guide
```

---

## 🔑 Key Constants (Lines 34-51)

```python
SIMILARITY_THRESHOLD = 85              # Min score to consider
HIGH_CONFIDENCE_THRESHOLD = 90         # Auto-accept threshold

VARIANT_TOKENS = {                     # Must match exactly
    'pro', 'max', 'ultra', 'plus',
    'fold', 'flip', 'fe', 'mini',
    'lite', 'note', 'edge'
}

MODEL_CODE_PATTERN = r'\b[a-z]{1,3}\d{3,6}[a-z]{0,3}\b'
# Matches: ZE552KL, SM-G960F, A2172
# Requires 3+ digits to avoid false matches
```

---

## 📞 Function Call Chain

```
app.py
  └─ run_matching() [Line 3953]
      └─ match_single_item() [Line 3499]
          ├─ try_attribute_match() [Line 1952]
          │   └─ extract_product_attributes() [Line 737]
          ├─ try_signature_match() [Line 2093]
          ├─ Fuzzy matching (rapidfuzz)
          ├─ verification_gate() [Line 3167]
          │   ├─ mobile_variant_exact_match() [Line 2951]
          │   ├─ tablet_variant_exact_match() [Line 2778]
          │   └─ laptop_variant_exact_match() [Line 2861]
          └─ auto_select_matching_variant() [Line 2203]
```

---

## 🎨 Category Detection

```python
extract_category(text) → str

Returns: 'mobile' | 'tablet' | 'laptop' | 'watch' | 'other'

Examples:
- "iPhone 14"          → 'mobile'
- "Galaxy Tab S8"      → 'tablet'
- "MacBook Air"        → 'laptop'
- "Apple Watch Series" → 'watch'
- "AirPods Pro"        → 'other'
```

---

## 💾 Index Structures

### Attribute Index
```python
index[brand][product_line][model][storage_key] = {
    'asset_ids': ['NL-12345'],
    'nl_name': 'apple iphone 14 128gb'
}
```

### Signature Index
```python
index['apple_watch_series9_45mm_gps_aluminum'] = {
    'asset_ids': ['NL-88888'],
    'nl_name': 'apple watch series 9...'
}
```

### Brand Index
```python
index['apple'] = {
    'lookup': {'apple iphone 14 128gb': ['NL-12345']},
    'names': ['apple iphone 14 128gb', ...]
}
```

---

## 🧪 Testing Commands

```bash
# Run all tests
python -m pytest tests/ -v

# Test attribute extraction
python -m pytest tests/test_attribute_extraction.py -v

# Test verification gates
python -m pytest tests/test_verification_gates.py -v

# Test auto-selection
python -m pytest tests/test_auto_selection.py -v

# Interactive test (single match)
streamlit run app.py
# → Use "Test Single Match" feature (removed in manager demo)
```

---

## 📈 Performance Benchmarks

**1,000 items (typical asset list):**
- Fast path (70%): 700 items × 3ms = 2.1 seconds
- Fuzzy (30%): 300 items × 25ms = 7.5 seconds
- **Total: ~10 seconds** (1-2 seconds on production server)

**Index building (9,894 NL products):**
- Attribute index: ~500ms
- Signature index: ~200ms
- Brand index: ~300ms
- **Total: ~1 second** (cached after first load)

---

## 🚨 Critical Code Sections

### Must-Read Lines
- **3577:** Gate enforcement point (all MATCHED paths go through here)
- **3060:** Fuzzy downgrade (ALWAYS demotes to REVIEW_REQUIRED)
- **3319-3338:** Token count check (prevents Pro vs Pro Max)
- **2203:** Auto-selection entry point
- **1952:** Attribute matching entry point

### Danger Zones (High False Positive Risk)
- **Fuzzy matching without brand filter** - Line 2745
- **Cross-category matching** - Prevented at Line 2658
- **zip() without length check** - Fixed at Line 3323

---

## 🎓 Learning Path

**For new developers:**

1. **Start here:** Read `MATCHER_LOGIC_COMPLETE.md` sections 1-3
2. **Understand indices:** Section 4 (Index Building)
3. **Trace a match:** Section 9 (End-to-End Examples)
4. **Study gates:** Section 6 (Verification Gates)
5. **Debug issues:** Section 11 (Common Pitfalls)

**For debugging:**

1. Find error in app.py UI
2. Search this doc for error message
3. Check function reference (Appendix)
4. Read relevant code section
5. Test fix with single match

---

## 🔗 Related Documentation

- `MEMORY.md` - Project memory (past bugs, fixes)
- `README.md` - Setup instructions
- `tests/` - Test cases with examples
- Plan file in `~/.claude/plans/` - Current work

---

**Last Updated:** February 2026
**For Questions:** Refer to `MATCHER_LOGIC_COMPLETE.md` for detailed explanations
