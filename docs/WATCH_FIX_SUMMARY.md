# Smartwatch Matching Fix - Complete Implementation

**Date**: February 16, 2026
**Issue**: Apple Watch Series 10 42mm matching to 46mm with 100% confidence (CRITICAL BUG)

---

## Root Cause Analysis

**Problem**: Attribute matching had NO watch-specific attributes
- All Apple Watch Series 10 watches (regardless of case size) collapsed into same bucket:
  - `brand=apple, product_line=watch, model=series 10, storage_key=''`
- Attribute fast-path returned 100% match for 42mm → 46mm
- Bypassed fuzzy matching and model token guardrail entirely

**Impact**: 42mm vs 46mm watches treated as identical products (different SKUs worth hundreds of dollars difference)

---

## Fixes Implemented

### Fix 1: Extract Watch Case Size (mm)
**File**: [matcher.py](matcher.py#L842-L855)

Added `extract_watch_mm()` function:
```python
def extract_watch_mm(text: str) -> str:
    """
    Extract watch case size in mm.
    Returns: '40mm', '42mm', '44mm', '46mm', '49mm', etc.
    Critical for distinguishing watch variants - 42mm vs 46mm are different products!
    """
    match = re.search(r'\b(3[89]|4[0-9]|5[0-5])\s*mm\b', text, re.IGNORECASE)
    return f"{match.group(1)}mm" if match else ''
```

**Coverage**: 38-55mm range (covers all Apple Watch, Galaxy Watch, etc.)

---

### Fix 2: Normalize MM Spacing
**File**: [matcher.py](matcher.py#L95-L97)

Added normalization in `normalize_text()`:
```python
# Standardize watch case size: "40 mm" → "40mm"
# Critical for watch matching: 42mm vs 46mm are DIFFERENT products
s = re.sub(r'(\d+)\s*mm\b', r'\1mm', s, flags=re.IGNORECASE)
```

**Benefit**: Consistent matching regardless of spacing ("40 mm" vs "40mm")

---

### Fix 3: Watch-Aware Attribute Extraction
**File**: [matcher.py](matcher.py#L410-L448)

Added watch detection in `extract_product_attributes()`:
```python
# === WATCH DETECTION (priority - critical attributes: mm, series, connectivity) ===
if extract_category(text_norm) == 'watch':
    watch_mm = extract_watch_mm(text_norm)

    # Extract series/generation: "series 10", "ultra 2", "se"
    series = ''
    series_match = re.search(r'\b(series\s*\d+|ultra\s*\d+|se)\b', text_norm)
    if series_match:
        series = series_match.group(1).replace('  ', ' ').strip()

    # Extract connectivity: GPS vs Cellular
    connectivity = ''
    if 'cellular' in text_norm or 'lte' in text_norm.lower() or '4g' in text_norm.lower():
        connectivity = 'cellular'
    elif 'gps' in text_norm:
        connectivity = 'gps'

    return {
        'brand': brand_norm,
        'product_line': 'watch',
        'model': series or 'watch',
        'storage': '',           # Watches typically don't have storage variants
        'ram': '',
        'watch_mm': watch_mm,    # CRITICAL: case size
        'connectivity': connectivity,  # GPS vs Cellular
    }
```

**Attributes Extracted**:
- Series/generation (Series 10, Ultra 2, SE)
- **Case size in mm** (CRITICAL - 40mm, 42mm, 44mm, 46mm, 49mm)
- Connectivity (GPS vs Cellular)

---

### Fix 4: Use MM in Attribute Index
**Files**:
- [matcher.py](matcher.py#L582-L601) - `build_attribute_index()`
- [matcher.py](matcher.py#L637-L650) - `try_attribute_match()`

**Before**:
```python
storage_key = f"{ram}_{attrs['storage']}" if ram else attrs['storage']
# All watches → storage_key = '' (no differentiation!)
```

**After**:
```python
if attrs['product_line'] == 'watch':
    # Watch key: mm is CRITICAL, connectivity is important
    storage_key = f"{watch_mm}_{connectivity}".strip('_')
elif ram:
    # Laptop key: RAM + storage
    storage_key = f"{ram}_{attrs['storage']}"
else:
    # Phone/tablet key: storage only
    storage_key = attrs['storage']
```

**Result**:
- Apple Watch Series 10 42mm GPS → key = `42mm_gps`
- Apple Watch Series 10 46mm Cellular → key = `46mm_cellular`
- **Different keys = different products** ✅

---

### Fix 5: Watch-Specific Verification
**File**: [matcher.py](matcher.py#L1356-L1362)

Added watch check in `verify_critical_attributes()`:
```python
# WATCH-SPECIFIC RULE: Case size (mm) must match exactly
# 42mm vs 46mm are DIFFERENT products!
if extract_category(query) == 'watch':
    query_mm = extract_watch_mm(query)
    matched_mm = extract_watch_mm(matched)
    if query_mm and matched_mm and query_mm != matched_mm:
        return False  # Different case size -> different product
```

**Applies to**: Fuzzy matches in 85-94% range (prevents upgrade to MATCHED if mm differs)

---

## Testing

**Test File**: [test_watch_fix.py](test_watch_fix.py)

**Test Results**: ✅ All tests pass

1. **MM Extraction**: 6/6 tests pass (40mm, 42mm, 44mm, 46mm, 49mm, with/without spaces)
2. **MM Normalization**: 2/2 tests pass ("40 mm" → "40mm")
3. **Attribute Extraction**: 3/3 tests pass (Series 10, Ultra 2, GPS/Cellular)

---

## Impact

### Before Fix:
- Apple Watch Series 10 42mm → 46mm: **100% MATCHED** ❌
- All Series 10 watches treated as identical
- Attribute fast-path bypassed fuzzy + guardrails

### After Fix:
- Apple Watch Series 10 42mm → 46mm: **NO_MATCH** ✅
- 42mm vs 46mm recognized as different products
- Case size is now a first-class critical attribute

---

## Category-Specific Attribute Priority

| Category | Critical Attributes | Used in Index Key |
|----------|-------------------|-------------------|
| **Watch** | Series + **MM** + Connectivity | `{mm}_{connectivity}` |
| **Laptop** | CPU tier + Gen + **RAM** + **Storage** | `{ram}_{storage}` |
| **Phone** | Model tokens + **Storage** | `{storage}` |
| **Tablet** | Model + Screen size + **Storage** | `{storage}` |

---

## Recommendations

### 1. Future Category-Specific Enhancements

**Tablets**: Add screen size extraction (11", 12.9")
- iPad Pro 11" vs 12.9" are different products
- Current code only uses storage

**Phones**: Consider adding screen size for flagship models
- iPhone 14 vs 14 Plus (different screen sizes)
- Currently handled by model tokens ("plus") but size could be explicit

### 2. Training ML Model?

**Answer**: Not yet. Rule-based is better for this use case because:
- Need deterministic correctness on critical attributes (mm, storage, RAM)
- Catalog naming is structured enough
- ML requires labeled data + ongoing maintenance

**When ML makes sense**:
- If you have thousands of human-reviewed mappings
- Want a reranker to reduce review workload after hard filters
- Even then: keep hard constraints (mm, storage must match)

---

## Files Modified

1. **matcher.py** (5 changes):
   - Added `extract_watch_mm()` function
   - Added mm normalization in `normalize_text()`
   - Added watch detection in `extract_product_attributes()`
   - Updated `build_attribute_index()` to use mm for watches
   - Updated `try_attribute_match()` to use mm for watches
   - Added watch verification in `verify_critical_attributes()`

2. **test_watch_fix.py** (new file):
   - Comprehensive test suite for watch matching

---

## Conclusion

✅ **Watch bug is FIXED**
✅ **42mm vs 46mm now correctly recognized as different products**
✅ **Attribute matching is now category-aware**
✅ **All tests pass**

The fix implements a **category-specific attribute policy** where each category (watch, laptop, phone, tablet) has its own critical attributes that must match for products to be considered identical.
