# Asset Mapping Accuracy Report
**Date**: February 14, 2026
**Results File**: asset_mapping_results (19).xlsx
**Total Matched**: 4,581 items (List 1: 1,910, List 2: 2,671)

---

## Priority 1: Cross-Category Mapping Errors ❌

### Summary
- **List 1 (Mobiles)**: ✅ 0 errors - all correct!
- **List 2 (Mixed Categories)**: ❌ 15 errors (all in Tablet category)

### Tablet Category Errors (15 total)

#### 14 Tablets Mapped to Mobile Phones:

1. **Huawei MatePad → Huawei Mate S (phone)**
   - Uploaded: Huawei MatePad T10s 32GB LTE
   - Mapped: Huawei Mate S 32 GB (Mobile Phone)
   - Issue: "MatePad" matching "Mate S" due to fuzzy similarity

2. **Samsung Galaxy Tab Active → Samsung Xcover Pro / Z Flip / Z Fold (phones)**
   - Uploaded: Samsung Galaxy Tab Active5 Pro 128GB
   - Mapped: Samsung Galaxy Xcover Pro 128GB (Mobile Phone)
   - Uploaded: Samsung Galaxy Tab Active5 128GB
   - Mapped: Samsung Galaxy Z Flip3 5G 128GB (Mobile Phone)
   - Uploaded: Samsung Galaxy Tab Active5 256GB
   - Mapped: Samsung Galaxy Z Fold2 5G 256GB (Mobile Phone)
   - Issue: "Tab Active" matching "Xcover", "Active" matching "Active 2 Watch"

3. **Xiaomi Redmi Pad Pro → Xiaomi Mi CC9 Pro (phone)**
   - Uploaded: Xiaomi Redmi Pad Pro 128GB
   - Mapped: Xiaomi MI CC9 Pro 128GB (Mobile Phone)
   - Issue: "Pad Pro" matching "CC9 Pro"

#### 1 Tablet Mapped to Smartwatch:

4. **Samsung Galaxy Tab Active 2 → Samsung Galaxy Watch Active 2 (watch)**
   - Uploaded: Samsung Galaxy Tab Active 2 T395 4G (Tablet)
   - Mapped: Samsung Galaxy Watch Active 2 44mm (Smartwatch)
   - Issue: Very similar names, fuzzy matching ignores "Tab" vs "Watch"

### Root Cause
**Missing category filtering in matching logic.** The fuzzy matcher is matching across categories, leading to cross-category errors.

### Recommended Fix
Add category filtering to matching logic:
```python
# Match only within same category
if uploaded_category == 'Tablet':
    candidates = nl_catalog[nl_catalog['category'] == 'Tablet']
```

---

## Priority 2: Windows Laptop Mapping Issues ⚠️

### Current Behavior
Laptops are being matched based on **fuzzy string matching** including model numbers (SP513-55N, UX533, etc.), leading to incorrect attribute matches.

### Examples of Wrong Attribute Matching:

**Example 1: Wrong Processor Tier + Generation + Storage**
- **Uploaded**: Acer Spin 5 SP513-55N - **Core i5**-1135G7 (**11th Gen**) / 8GB / **512GB SSD**
- **Mapped**: Acer Spin 5 Series - **Core i7** (**7th Gen**) / 8GB / **256GB SSD**
- ❌ Processor: i5 → i7 (wrong tier)
- ❌ Generation: 11th → 7th (wrong generation)
- ❌ Storage: 512GB → 256GB (wrong storage)
- ✅ Brand, Series, RAM: Correct

**Example 2: Wrong Processor Tier**
- **Uploaded**: Asus Zenbook 13 UX325 - **Core i3**-1115G4 / 8GB / 512GB SSD
- **Mapped**: Asus ZenBook UX Series - **Core i5** 11th Gen / 8GB / 512GB
- ❌ Processor: i3 → i5 (wrong tier)
- ✅ Generation, RAM, Storage: Correct

**Example 3: Wrong Processor Tier + RAM**
- **Uploaded**: Asus Vivobook Pro 14 - Core i5-11300H / **8GB** / 256GB SSD
- **Mapped**: Asus VivoBook 15 - Core i7 11th Gen / **8GB** / 256GB SSD
- ❌ Processor: i5 → i7 (wrong tier)
- ✅ Generation, Storage: Correct

### Manager Requirement
**"For Windows laptops, no need to map model number. Map based on attributes:"**
1. Category: Laptop
2. Brand: Acer, Asus, HP, Dell, Lenovo
3. Series/Model: Spin 5, ZenBook UX, VivoBook
4. **Processor**: Core i3, i5, i7, i9
5. **Generation**: 8th Gen, 10th Gen, 11th Gen, etc.
6. **RAM**: 8GB, 16GB, 32GB
7. **Storage**: 256GB SSD, 512GB SSD, 1TB SSD

### Current Issue
Model numbers like "SP513-55N" and "UX325" are being used in fuzzy matching, but they don't help match the correct attributes. The matcher is prioritizing series name similarity over attribute accuracy.

### Recommended Approach
For Windows laptops, implement **attribute-based matching** instead of fuzzy string matching:

1. **Extract attributes** from uploaded laptop name:
   - Processor tier: i3, i5, i7, i9
   - Generation: Parse from processor model (1135G7 = 11th gen, 8250U = 8th gen)
   - RAM: 8GB, 16GB, etc.
   - Storage: 256GB, 512GB, 1TB

2. **Match by attribute priority**:
   - Brand (exact match)
   - Series/Model (fuzzy match on series name only, ignore model number)
   - Processor tier (exact match: i5 only matches i5)
   - Generation (exact or ±1 generation tolerance)
   - RAM (exact match)
   - Storage (exact match)

3. **Example**:
   ```
   Uploaded: Acer Spin 5 SP513-55N - Core i5-1135G7 / 8GB / 512GB SSD

   Extract:
   - Brand: Acer
   - Series: Spin 5
   - Processor: Core i5, 11th Gen (from 1135G7)
   - RAM: 8GB
   - Storage: 512GB SSD

   Match to NL:
   - Acer + Spin 5 + Core i5 + 11th Gen + 8GB + 512GB SSD
   - NOT: Acer + Spin 5 + Core i7 + 7th Gen + 8GB + 256GB SSD
   ```

---

## Action Items

### Immediate Fixes Required:

1. **Add Category Filtering** (Priority 1)
   - Prevent cross-category matches
   - Tablet should ONLY match Tablet in NL catalog
   - Estimated impact: Fix 15 errors

2. **Implement Attribute-Based Laptop Matching** (Priority 2)
   - Extract processor, generation, RAM, storage from product name
   - Match based on attributes, not model numbers
   - Estimated impact: Fix ~10-15 laptop mapping errors

### Files to Check:
- `list2_category_errors.csv` - Full list of 15 cross-category errors
- `asset_mapping_results (19).xlsx` - Current results file

---

## Current Accuracy

**List 1 (Mobile only)**: 100% category accuracy
**List 2 (Mixed categories)**:
- Mobile Phone: 2,118 matched (0 errors) - 100%
- Tablet: 229 matched (15 errors) - 93.4%
- Smartwatch: 200 matched (0 errors) - 100%
- Laptop: 124 matched (~10 attribute errors) - Needs attribute validation

**Overall**: 99.7% category accuracy (15 errors out of 4,581)
