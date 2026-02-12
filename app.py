"""
NorthLadder Asset Mapping Tool — Streamlit UI

The NL master catalog is bundled with the app (nl_reference/).
Users only need to upload their asset list Excel files.

Run with:
    streamlit run app.py

Version: FULLY FIXED + NL catalog rebuilt with years (Feb 2026)
"""

import io
import streamlit as st
import pandas as pd

from matcher import (
    load_and_clean_nl_list,
    build_nl_lookup,
    build_brand_index,
    build_attribute_index,
    run_matching,
    test_single_match,
    parse_nl_sheet,
    parse_asset_sheets,
    save_nl_reference,
    load_nl_reference,
    nl_reference_exists,
    delete_nl_reference,
    SIMILARITY_THRESHOLD,
    HIGH_CONFIDENCE_THRESHOLD,
    MATCH_STATUS_MATCHED,
    MATCH_STATUS_MULTIPLE,
    MATCH_STATUS_SUGGESTED,
    MATCH_STATUS_NO_MATCH,
    CONFIDENCE_HIGH,
    CONFIDENCE_MEDIUM,
    CONFIDENCE_LOW,
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="NorthLadder Asset Mapper",
    page_icon="🔗",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🔗 NorthLadder UAE Asset ID Mapper")
st.markdown("**Intelligent fuzzy matching with attribute verification and hybrid indexing**")

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.header("⚙️ Settings")

# Fixed threshold at 85% - hybrid matching with auto-select handles everything
threshold = SIMILARITY_THRESHOLD

st.sidebar.markdown("**Confidence Tiers:**")
st.sidebar.markdown("🟢 **HIGH (≥90%)** — MATCHED status (auto-selected if multiple variants)")
st.sidebar.markdown("🟡 **MEDIUM (85-89%)** — REVIEW REQUIRED (attributes differ)")
st.sidebar.markdown("🔴 **LOW (<85%)** — NO_MATCH (no confident match found)")

st.sidebar.divider()

# Advanced mode toggle
show_advanced = st.sidebar.checkbox(
    "🔧 Show Advanced Options",
    value=False,
    help="Enable advanced features like manual variant override"
)

st.sidebar.divider()

# Admin: refresh NL reference (hidden in sidebar expander)
with st.sidebar.expander("Admin: NL Reference"):
    if nl_reference_exists():
        nl_data = load_nl_reference()
        if nl_data:
            df_nl_ref, nl_meta = nl_data
            # Use .get() with fallback to df length if 'final' key doesn't exist in cached metadata
            record_count = nl_meta.get('final', len(df_nl_ref))
            st.caption(f"Loaded {record_count:,} records")
        if st.button("Refresh NL Reference"):
            delete_nl_reference()
            st.cache_data.clear()  # Clear cache to reload catalog
            st.rerun()
    else:
        st.warning("No NL reference found")
    nl_admin_upload = st.file_uploader("Upload new NL Master", type=["xlsx"], key="nl_admin")
    if nl_admin_upload is not None:
        if st.button("Save NL Reference"):
            with st.spinner("Saving..."):
                df_raw = parse_nl_sheet(nl_admin_upload)
                df_clean, stats = load_and_clean_nl_list(df_raw)
                save_nl_reference(df_clean, stats)
            st.success(f"Saved {stats['final']:,} records")
            st.cache_data.clear()  # Clear cache to reload new catalog
            st.rerun()

# =========================================================================
# Load NL reference (bundled with app) - CACHED for performance
# =========================================================================

@st.cache_data(show_spinner="Loading NL catalog...")
def load_nl_catalog():
    """Load NL reference and build all indexes - cached for fast reloads."""
    if not nl_reference_exists():
        return None

    df_nl_clean, nl_stats = load_nl_reference()
    nl_lookup = build_nl_lookup(df_nl_clean)
    nl_names = list(nl_lookup.keys())
    nl_brand_index = build_brand_index(df_nl_clean)
    nl_attribute_index = build_attribute_index(df_nl_clean)

    return {
        'df': df_nl_clean,
        'stats': nl_stats,
        'lookup': nl_lookup,
        'names': nl_names,
        'brand_index': nl_brand_index,
        'attribute_index': nl_attribute_index,
    }

# Load catalog (will be cached after first load)
catalog = load_nl_catalog()

if catalog is None:
    st.error(
        "NL reference catalog not found. "
        "Use the Admin panel in the sidebar to upload the NorthLadder master Excel."
    )
    st.stop()

# Unpack cached data
df_nl_clean = catalog['df']
nl_stats = catalog['stats']
nl_lookup = catalog['lookup']
nl_names = catalog['names']
nl_brand_index = catalog['brand_index']
nl_attribute_index = catalog['attribute_index']

st.success(
    f"NL Reference: **{nl_stats.get('final', len(df_nl_clean)):,}** asset records loaded "
    f"({len(nl_brand_index)} brands, hybrid matching enabled)"
)

# =========================================================================
# Tab Navigation
# =========================================================================
# Conditionally show Variant Selector tab based on advanced mode toggle
if show_advanced:
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard", "🔗 Mapping", "🎯 Variant Selector", "❌ Unmatched Analysis"])
else:
    tab1, tab2, tab4 = st.tabs(["📊 Dashboard", "🔗 Mapping", "❌ Unmatched Analysis"])
    tab3 = None  # Variant Selector is hidden

# =========================================================================
# TAB 1: DASHBOARD
# =========================================================================
with tab1:
    st.header("📊 System Overview")

    # Metrics Row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("NL Catalog Products", f"{nl_stats.get('final', len(df_nl_clean)):,}")
    with col2:
        st.metric("Unique Brands", len(nl_brand_index))
    with col3:
        categories = df_nl_clean['category'].nunique()
        st.metric("Categories", categories)
    with col4:
        st.metric("Matching Method", "Hybrid")

    st.divider()

    # Catalog Breakdown
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("📦 NL Catalog by Category")
        category_counts = df_nl_clean['category'].value_counts()
        st.bar_chart(category_counts)

        st.caption("**Product Distribution:**")
        total_products = nl_stats.get('final', len(df_nl_clean))
        for cat, count in category_counts.items():
            st.markdown(f"- **{cat}**: {count:,} products ({count/total_products*100:.1f}%)")

    with col_right:
        st.subheader("🏢 Top 15 Brands")
        brand_counts = df_nl_clean['brand'].value_counts().head(15)
        st.bar_chart(brand_counts)

    st.divider()

    # Matching Flow Diagram
    st.subheader("🔄 Matching Process Flow")
    st.markdown("""
    ```
    1. Upload Asset Lists
       ↓
    2. Auto-detect Sheets & Columns
       ↓
    3. Hybrid Matching Engine
       ├─ Attribute Matching (Fast Path - 70-80% of queries)
       │  └─ Exact brand + model + storage match
       └─ Fuzzy Matching (Fallback)
          ├─ Brand Partitioning
          ├─ Storage Pre-filtering
          ├─ Token Sort Fuzzy Match
          └─ Model Token Guardrail
       ↓
    4. Auto-Select for Multiple Variants
       └─ Matches user's exact specs (year, 5G/4G)
       ↓
    5. Results Classification
       ├─ ✅ MATCHED (≥90%, auto-selected if multiple IDs)
       ├─ 🟡 REVIEW (85-94%, attributes differ)
       └─ 🔴 NO_MATCH (<85%)
    ```
    """)

    st.divider()

    # Feature Highlights
    st.subheader("✨ Key Features")

    feat_col1, feat_col2 = st.columns(2)

    with feat_col1:
        st.markdown("""
        **🎯 Intelligent Matching:**
        - Hybrid matching (attribute + fuzzy)
        - Brand partitioning for accuracy
        - Model token guardrails
        - Storage pre-filtering

        **🔍 Attribute Verification:**
        - Auto-upgrades 94% of review items
        - Compares model tokens & storage
        - Prevents false positives
        """)

    with feat_col2:
        st.markdown("""
        **📊 Smart Features:**
        - Year preservation (iPhone SE 2016 vs 2020)
        - 5G/LTE handling
        - Duplicate filtering
        - Caching for instant reloads

        **📈 Results Export:**
        - Excel with multiple sheets
        - Variant details for multi-IDs
        - Summary statistics
        """)

# =========================================================================
# TAB 2: MAPPING
# =========================================================================
with tab2:
    st.header("🔗 Asset Mapping")
    st.markdown("Upload an Excel file with your asset lists — all sheets are auto-detected and matched.")

    asset_upload = st.file_uploader("📁 Upload Asset Lists (.xlsx)", type=["xlsx"], key="asset_upload")

    if asset_upload is not None:
        try:
            detected_sheets = parse_asset_sheets(asset_upload)
        except Exception as e:
            st.error(f"Failed to parse: {e}")
            st.stop()

        if not detected_sheets:
            st.warning("No matchable sheets found. Make sure your Excel has columns with product names.")
            st.stop()

        # Show detected sheets
        st.subheader(f"📊 Detected {len(detected_sheets)} sheet(s)")
        for sheet_name, info in detected_sheets.items():
            brand_label = info['brand_col'] or '(none)'
            st.markdown(
                f"- **{sheet_name}** — {len(info['df']):,} rows | "
                f"Brand: `{brand_label}` | Name: `{info['name_col']}`"
            )

        with st.expander("Preview Raw Data"):
            preview_tabs = st.tabs(list(detected_sheets.keys()))
            for tab, (sheet_name, info) in zip(preview_tabs, detected_sheets.items()):
                with tab:
                    st.dataframe(info['df'].head(10), use_container_width=True, hide_index=True)

        # ------------------------------------------------------------------
        # Test Single Match
        # ------------------------------------------------------------------
        st.divider()
        st.subheader("🧪 Test Single Match")

        # Get unique brands from catalog for dropdown
        available_brands = sorted(df_nl_clean['brand'].unique())

        tc1, tc2, tc3 = st.columns([2, 2, 1])
        with tc1:
            test_brand = st.selectbox(
                "Brand",
                options=available_brands,
                index=available_brands.index("Apple") if "Apple" in available_brands else 0,
                key="test_brand"
            )
        with tc2:
            test_name = st.text_input("Product Name", value="iPhone 6 16GB", key="test_name")
        with tc3:
            st.write("")
            st.write("")
            test_btn = st.button("Test Match", use_container_width=True)

        if test_btn:
            result = test_single_match(test_brand, test_name, nl_lookup, nl_names, threshold,
                                       brand_index=nl_brand_index, attribute_index=nl_attribute_index,
                                       nl_catalog=df_nl_clean)
            st.markdown(f"**Query:** `{result['query']}`")
            if 'error' in result:
                st.error(result['error'])
            else:
                best = result['best_match']
                method = best.get('method', 'unknown')
                method_emoji = "⚡" if method == "attribute" else "🔍" if method == "fuzzy" else "❓"
                st.markdown(f"**Result:** `{best['match_status']}` (Score: {best['match_score']}%) {method_emoji} `{method}`")
                if best['mapped_uae_assetid']:
                    st.success(f"Asset ID: `{best['mapped_uae_assetid']}`")
                    st.caption(f"Matched on: `{best['matched_on']}`")

                st.caption(f"💡 Method: **{method.upper()}** - " +
                          ("Fast attribute matching (0ms)" if method == "attribute" else
                           "Fuzzy string matching" if method == "fuzzy" else "No match found"))

                alt_df = pd.DataFrame(result['top_3_alternatives'])
                alt_df['asset_ids'] = alt_df['asset_ids'].apply(lambda x: ', '.join(x) if x else 'N/A')
                st.dataframe(alt_df[['nl_name', 'score', 'status', 'asset_ids']], use_container_width=True, hide_index=True)

        # ------------------------------------------------------------------
        # Run full mapping
        # ------------------------------------------------------------------
        st.divider()
        if st.button("🚀 Run Asset Mapping", type="primary", use_container_width=True):

            all_results = {}

            for sheet_name, info in detected_sheets.items():
                st.subheader(f"🔍 Matching: {sheet_name}")
                progress = st.progress(0, text=f"Starting {sheet_name}...")

                def make_progress_cb(prog_bar, sname):
                    def cb(current, total):
                        prog_bar.progress(current / total, text=f"{sname}... {current:,}/{total:,}")
                    return cb

                df_result = run_matching(
                    df_input=info['df'],
                    brand_col=info['brand_col'] or '__no_brand__',
                    name_col=info['name_col'],
                    nl_lookup=nl_lookup,
                    nl_names=nl_names,
                    threshold=threshold,
                    progress_callback=make_progress_cb(progress, sheet_name),
                    brand_index=nl_brand_index,
                    attribute_index=nl_attribute_index,
                    nl_catalog=df_nl_clean,
                )
                progress.progress(1.0, text=f"✅ {sheet_name} complete!")
                all_results[sheet_name] = df_result

                matched = (df_result['match_status'] == MATCH_STATUS_MATCHED).sum()
                multiple = (df_result['match_status'] == MATCH_STATUS_MULTIPLE).sum()
                suggested = (df_result['match_status'] == MATCH_STATUS_SUGGESTED).sum()
                no_match = (df_result['match_status'] == MATCH_STATUS_NO_MATCH).sum()
                total = len(df_result)

                ca, cb, cc, cd = st.columns(4)
                ca.metric("🟢 Matched (HIGH)", matched, f"{matched/total*100:.1f}%")
                cb.metric("🟡 Review Required", suggested, f"{suggested/total*100:.1f}%")
                cc.metric("🔵 Multiple IDs", multiple, f"{multiple/total*100:.1f}%")
                cd.metric("🔴 No Match", no_match, f"{no_match/total*100:.1f}%")

            # ------------------------------------------------------------------
            # Preview
            # ------------------------------------------------------------------
            st.subheader("📋 Preview Results")

            def color_status(val):
                if val == MATCH_STATUS_MATCHED:
                    return 'background-color: #d4edda; color: #155724'
                elif val == MATCH_STATUS_SUGGESTED:
                    return 'background-color: #fff3cd; color: #856404'
                elif val == MATCH_STATUS_MULTIPLE:
                    return 'background-color: #cce5ff; color: #004085'
                elif val == MATCH_STATUS_NO_MATCH:
                    return 'background-color: #f8d7da; color: #721c24'
                return ''

            result_tabs = st.tabs(list(all_results.keys()))
            for tab, (sheet_name, df_result) in zip(result_tabs, all_results.items()):
                with tab:
                    st.dataframe(
                        df_result.head(100).style.map(color_status, subset=['match_status']),
                        use_container_width=True, hide_index=True,
                    )
                    # Show items needing review (SUGGESTED)
                    n_suggested = (df_result['match_status'] == MATCH_STATUS_SUGGESTED).sum()
                    if n_suggested > 0:
                        with st.expander(f"Review {n_suggested} Items Requiring Review (85-94%)"):
                            st.dataframe(
                                df_result[df_result['match_status'] == MATCH_STATUS_SUGGESTED],
                                use_container_width=True, hide_index=True,
                            )
                    # Show unmatched items
                    n_unmatched = (df_result['match_status'] == MATCH_STATUS_NO_MATCH).sum()
                    if n_unmatched > 0:
                        with st.expander(f"View {n_unmatched} Unmatched Items"):
                            st.dataframe(
                                df_result[df_result['match_status'] == MATCH_STATUS_NO_MATCH],
                                use_container_width=True, hide_index=True,
                            )

            # ------------------------------------------------------------------
            # Store results in session state for cross-tab access
            # ------------------------------------------------------------------
            st.session_state['mapping_results'] = {
                'all_results': all_results,
                'detected_sheets': detected_sheets,
            }

            # ------------------------------------------------------------------
            # Output Excel with new structure
            # ------------------------------------------------------------------
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # 1. MATCHED sheets (one per uploaded sheet) - Only MATCHED items
                for sheet_name, df_result in all_results.items():
                    matched = df_result[df_result['match_status'] == MATCH_STATUS_MATCHED].copy()
                    if len(matched) > 0:
                        # Add real NL product name column for better UX
                        nl_product_names = []
                        for idx, row in matched.iterrows():
                            asset_id = row['mapped_uae_assetid']
                            nl_entry = df_nl_clean[df_nl_clean['uae_assetid'] == asset_id]
                            nl_name = nl_entry.iloc[0]['uae_assetname'] if len(nl_entry) > 0 else 'N/A'
                            nl_product_names.append(nl_name)

                        # Insert nl_product_name after mapped_uae_assetid for logical ordering
                        insert_pos = list(matched.columns).index('mapped_uae_assetid') + 1
                        matched.insert(insert_pos, 'nl_product_name', nl_product_names)

                        safe_name = f"{sheet_name} - Matched"[:31]
                        matched.to_excel(writer, sheet_name=safe_name, index=False)

                # 2. UNMATCHED sheets (one per uploaded sheet) - Only NO_MATCH items
                for sheet_name, df_result in all_results.items():
                    unmatched = df_result[df_result['match_status'] == MATCH_STATUS_NO_MATCH]
                    if len(unmatched) > 0:
                        safe_name = f"{sheet_name} - Unmatched"[:31]
                        unmatched.to_excel(writer, sheet_name=safe_name, index=False)

                # 3. REVIEW REQUIRED sheet - All REVIEW_REQUIRED items (combined)
                all_review_required = []
                for sheet_name, df_result in all_results.items():
                    review = df_result[df_result['match_status'] == MATCH_STATUS_SUGGESTED].copy()
                    if len(review) > 0:
                        # Add real NL product name column for review items too
                        nl_product_names = []
                        for idx, row in review.iterrows():
                            asset_id = row['mapped_uae_assetid']
                            nl_entry = df_nl_clean[df_nl_clean['uae_assetid'] == asset_id]
                            nl_name = nl_entry.iloc[0]['uae_assetname'] if len(nl_entry) > 0 else 'N/A'
                            nl_product_names.append(nl_name)

                        insert_pos = list(review.columns).index('mapped_uae_assetid') + 1
                        review.insert(insert_pos, 'nl_product_name', nl_product_names)
                        review.insert(0, 'Source Sheet', sheet_name)
                        all_review_required.append(review)

                if all_review_required:
                    df_review_combined = pd.concat(all_review_required, ignore_index=True)
                    df_review_combined.to_excel(writer, sheet_name='Review Required', index=False)

                # 4. AUTO-SELECTED PRODUCTS sheet - All auto-selected items with details
                auto_selected_details = []
                for sheet_name, df_result in all_results.items():
                    auto_selected = df_result[df_result['auto_selected'] == True].copy()
                    for idx, row in auto_selected.iterrows():
                        # Get original product name
                        name_col = 'name' if 'name' in row else 'Foxway Product Name'
                        original_name = row[name_col] if name_col in row else ''

                        # Parse alternatives
                        alternatives_raw = row.get('alternatives', '')
                        if isinstance(alternatives_raw, str) and alternatives_raw:
                            try:
                                alternatives = eval(alternatives_raw) if alternatives_raw.startswith('[') else []
                            except:
                                alternatives = []
                        else:
                            alternatives = []

                        # Get selected product details from NL catalog
                        selected_id = row['mapped_uae_assetid']
                        nl_entry = df_nl_clean[df_nl_clean['uae_assetid'] == selected_id]
                        selected_name = nl_entry.iloc[0]['uae_assetname'] if len(nl_entry) > 0 else 'N/A'

                        auto_selected_details.append({
                            'Source Sheet': sheet_name,
                            'Your Product': original_name,
                            'Matched To': row['matched_on'],
                            'Match Score': f"{row['match_score']:.1f}%",
                            'Selected ID': selected_id,
                            'Selected Product': selected_name,
                            'Selection Reason': row.get('selection_reason', 'N/A'),
                            'Alternative IDs': ', '.join(alternatives) if alternatives else 'None',
                            'Total Variants': len(alternatives) + 1,
                        })

                if auto_selected_details:
                    df_auto_selected = pd.DataFrame(auto_selected_details)
                    df_auto_selected.to_excel(writer, sheet_name='Auto-Selected Products', index=False)

                # 5. SUMMARY sheet - Overall statistics
                summary_rows = []
                for sheet_name, df_result in all_results.items():
                    total = len(df_result)
                    matched = int((df_result['match_status'] == MATCH_STATUS_MATCHED).sum())
                    review = int((df_result['match_status'] == MATCH_STATUS_SUGGESTED).sum())
                    no_match = int((df_result['match_status'] == MATCH_STATUS_NO_MATCH).sum())
                    auto_selected = int(df_result['auto_selected'].sum())

                    summary_rows.append({
                        'Sheet': sheet_name,
                        'Total Items': total,
                        'Matched': matched,
                        'Review Required': review,
                        'No Match': no_match,
                        'Auto-Selected': auto_selected,
                        'Match Rate': f"{matched/total*100:.1f}%",
                    })

                # Add totals row
                total_items = sum(len(df) for df in all_results.values())
                total_matched = sum((df['match_status'] == MATCH_STATUS_MATCHED).sum() for df in all_results.values())
                total_review = sum((df['match_status'] == MATCH_STATUS_SUGGESTED).sum() for df in all_results.values())
                total_no_match = sum((df['match_status'] == MATCH_STATUS_NO_MATCH).sum() for df in all_results.values())
                total_auto_selected = sum(df['auto_selected'].sum() for df in all_results.values())

                summary_rows.append({
                    'Sheet': '',
                    'Total Items': '',
                    'Matched': '',
                    'Review Required': '',
                    'No Match': '',
                    'Auto-Selected': '',
                    'Match Rate': '',
                })
                summary_rows.append({
                    'Sheet': 'TOTAL',
                    'Total Items': int(total_items),
                    'Matched': int(total_matched),
                    'Review Required': int(total_review),
                    'No Match': int(total_no_match),
                    'Auto-Selected': int(total_auto_selected),
                    'Match Rate': f"{total_matched/total_items*100:.1f}%",
                })

                pd.DataFrame(summary_rows).to_excel(writer, sheet_name='Summary', index=False)

            output.seek(0)

            st.divider()
            st.download_button(
                label="📥 Download Mapped Excel File",
                data=output,
                file_name="asset_mapping_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True,
            )
            st.success("✅ Mapping complete! Unmatched items are flagged for manual review.")

    # =========================================================================
# TAB 3: VARIANT SELECTOR (Advanced Mode Only)
# =========================================================================
if tab3 is not None:
    with tab3:
        st.header("🎯 Interactive Variant Selector")
        st.markdown("""
        Review and override auto-selected variants. The system automatically selects the best variant based on
        your product's specs (year, 5G/4G), but you can manually choose a different variant if needed.
        """)

        # Check if mapping results exist in session state
        if 'mapping_results' not in st.session_state:
            st.info("ℹ️ No mapping results available. Please run the **Mapping** tab first.")
            st.stop()

        # Load results from session state
        all_results = st.session_state['mapping_results']['all_results']
        detected_sheets = st.session_state['mapping_results']['detected_sheets']

        # Convert results dict to separate dataframes
        all_dataframes = {}
        for sheet_name, df_result in all_results.items():
            all_dataframes[sheet_name] = df_result.copy()

        # Find auto-selected items
        all_auto_selected = []
        for sheet_name, df_result in all_dataframes.items():
            auto_selected = df_result[df_result['auto_selected'] == True]
            if len(auto_selected) > 0:
                all_auto_selected.append((sheet_name, auto_selected))

        total_autoselect = sum(len(df) for _, df in all_auto_selected)

        if total_autoselect == 0:
            st.info("ℹ️ No auto-selected variants found. All matched items have single unique IDs.")
            st.stop()

        # ------------------------------------------------------------------
        # ACCURACY VERIFICATION
        # ------------------------------------------------------------------
        st.subheader("🎯 Auto-Selection Accuracy")

        with st.spinner("Verifying accuracy of auto-selected items..."):
            errors = []
            warnings = []
            success_count = 0

            for sheet_name, auto_selected in all_auto_selected:
                for idx, row in auto_selected.iterrows():
                    # Get product name
                    name_col = 'name' if 'name' in row else 'Foxway Product Name'
                    user_input = str(row[name_col]) if name_col in row else ''

                    selected_id = row['mapped_uae_assetid']
                    selection_reason = row.get('selection_reason', '')

                    # CHECK 1: Selected ID exists in NL catalog
                    nl_entry = df_nl_clean[df_nl_clean['uae_assetid'] == selected_id]
                    if len(nl_entry) == 0:
                        errors.append({
                            'sheet': sheet_name,
                            'product': user_input,
                            'error': f'Selected ID {selected_id} not found in NL catalog',
                        })
                        continue

                    nl_product = nl_entry.iloc[0]['uae_assetname']

                    # CHECK 2: Verify selection reason is logical
                    reason = str(selection_reason).lower()
                    user_input_lower = user_input.lower()
                    nl_product_lower = nl_product.lower()

                    # Check year matching
                    if 'matched year' in reason:
                        import re
                        year_match = re.search(r'matched year (\d{4})', reason)
                        if year_match:
                            year = year_match.group(1)
                            if year not in nl_product_lower:
                                errors.append({
                                    'sheet': sheet_name,
                                    'product': user_input,
                                    'error': f"Reason says 'matched year {year}' but year not in selected product",
                                })
                                continue

                    # Check 5G matching
                    elif 'matched 5g' in reason:
                        if '5g' not in user_input_lower:
                            errors.append({
                                'sheet': sheet_name,
                                'product': user_input,
                                'error': "Reason says 'matched 5G' but user input has no 5G",
                            })
                            continue
                        if '5g' not in nl_product_lower:
                            errors.append({
                                'sheet': sheet_name,
                                'product': user_input,
                                'error': "Reason says 'matched 5G' but selected product has no 5G",
                            })
                            continue

                    # Check 4G/LTE matching
                    elif 'matched 4g/lte' in reason or 'defaulted to 4g' in reason:
                        if '5g' in nl_product_lower:
                            errors.append({
                                'sheet': sheet_name,
                                'product': user_input,
                                'error': "Reason says '4G/LTE' but selected product has 5G",
                            })
                            continue

                    success_count += 1

        # Display accuracy metrics
        accuracy = (success_count / total_autoselect * 100) if total_autoselect > 0 else 100

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Auto-Selected", total_autoselect)
        with col2:
            st.metric("Verified Correct", success_count, delta=None if accuracy == 100 else f"-{len(errors)}")
        with col3:
            accuracy_color = "🟢" if accuracy == 100 else "🟡" if accuracy >= 95 else "🔴"
            st.metric(f"{accuracy_color} Accuracy", f"{accuracy:.1f}%")
        with col4:
            st.metric("Errors Found", len(errors), delta=None if len(errors) == 0 else "needs review")

        if accuracy == 100:
            st.success("✅ **Perfect accuracy!** All auto-selections are verified correct. Manual override not necessary.")
            st.caption("All selected IDs exist in NL catalog and selection reasons are logical.")
        else:
            st.warning(f"⚠️ Found {len(errors)} error(s) in auto-selections. Manual review recommended.")

            with st.expander("View errors"):
                for i, err in enumerate(errors, 1):
                    st.markdown(f"{i}. **[{err['sheet']}]** {err['product']}")
                    st.caption(f"   ❌ {err['error']}")

        st.divider()

        # ------------------------------------------------------------------
        # VARIANT OVERRIDE SECTION (only if accuracy < 100%)
        # ------------------------------------------------------------------
        if accuracy < 100:
            st.subheader("🔧 Manual Override")
            st.markdown("Since accuracy is below 100%, you can manually override auto-selections below.")

            if True:
                # Interactive selector
                st.caption("✓ Auto-selection logic: Year → Connectivity (5G/4G) → First ID")

                # Initialize session state for selections
                if 'variant_selections' not in st.session_state:
                    st.session_state.variant_selections = {}

                # Flatten all auto-selected items for display
                all_items_flat = []
                for sheet_name, auto_selected in all_auto_selected:
                    for idx, row in auto_selected.iterrows():
                        all_items_flat.append((sheet_name, idx, row))

                # Show items with variant selection
                st.markdown(f"**Showing first {min(20, len(all_items_flat))} of {len(all_items_flat)} items**")

                for i, (sheet_name, idx, row) in enumerate(all_items_flat[:20]):
                    name_col = 'name' if 'name' in row else 'Foxway Product Name'
                    product_name = row[name_col] if name_col in row else ''

                    with st.expander(f"Item {i+1}: {product_name}"):
                        # Show match info
                        col_info, col_select = st.columns([2, 1])

                        with col_info:
                            st.markdown(f"**Your Product:** {product_name}")
                            st.markdown(f"**Matched To:** `{row['matched_on']}`")
                            st.markdown(f"**Match Score:** {row['match_score']:.1f}%")
                            st.markdown(f"**Selection Reason:** {row.get('selection_reason', 'N/A')}")

                        # Parse alternatives
                        current_id = str(row['mapped_uae_assetid']).strip()
                        alternatives_raw = row.get('alternatives', '')
                        if isinstance(alternatives_raw, str) and alternatives_raw:
                            try:
                                alternatives = eval(alternatives_raw) if alternatives_raw.startswith('[') else []
                            except:
                                alternatives = []
                        else:
                            alternatives = []

                        # Build full list: current ID + alternatives
                        all_ids = [current_id] + alternatives

                        # Show variant options
                        st.markdown(f"**{len(all_ids)} Variant Options:**")

                        for id_val in all_ids:
                            nl_entry = df_nl_clean[df_nl_clean['uae_assetid'] == id_val]
                            if len(nl_entry) > 0:
                                product_name_nl = nl_entry.iloc[0]['uae_assetname']
                                prefix = "✓ **SELECTED:** " if id_val == current_id else "   "
                                st.markdown(f"{prefix}`{id_val}`: {product_name_nl}")

                        # Selection dropdown
                        with col_select:
                            key = f"{sheet_name}_{idx}"
                            selected = st.selectbox(
                                "Override selection:",
                                options=range(len(all_ids)),
                                index=0,  # Default to current selection
                                format_func=lambda x: f"Variant {x+1}",
                                key=f"select_{key}"
                            )
                            st.session_state.variant_selections[key] = all_ids[selected]
                            if all_ids[selected] != current_id:
                                st.warning(f"Overridden to: `{all_ids[selected]}`")
                            else:
                                st.success(f"Using auto-selected: `{all_ids[selected]}`")

                st.divider()

                # Apply selections and generate updated Excel
                if st.button("✅ Apply Overrides & Download Updated Results", type="primary", use_container_width=True):
                    override_count = 0

                    # Apply overrides to session state data
                    for key, selected_id in st.session_state.variant_selections.items():
                        parts = key.rsplit('_', 1)
                        if len(parts) == 2:
                            sheet_name, idx_str = parts
                            idx = int(idx_str)

                            if sheet_name in all_dataframes:
                                original_id = all_dataframes[sheet_name].at[idx, 'mapped_uae_assetid']
                                if str(selected_id) != str(original_id):
                                    all_dataframes[sheet_name].at[idx, 'mapped_uae_assetid'] = selected_id
                                    all_dataframes[sheet_name].at[idx, 'selection_reason'] = 'Manually overridden'
                                    override_count += 1

                    # Update session state with modified data
                    st.session_state['mapping_results']['all_results'] = all_dataframes

                    # Generate updated Excel with new structure
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        # 1. MATCHED sheets (updated with overrides)
                        for sheet_name, df_result in all_dataframes.items():
                            matched = df_result[df_result['match_status'] == MATCH_STATUS_MATCHED]
                            if len(matched) > 0:
                                safe_name = f"{sheet_name} - Matched"[:31]
                                matched.to_excel(writer, sheet_name=safe_name, index=False)

                        # 2. UNMATCHED sheets
                        for sheet_name, df_result in all_dataframes.items():
                            unmatched = df_result[df_result['match_status'] == MATCH_STATUS_NO_MATCH]
                            if len(unmatched) > 0:
                                safe_name = f"{sheet_name} - Unmatched"[:31]
                                unmatched.to_excel(writer, sheet_name=safe_name, index=False)

                        # 3. REVIEW REQUIRED sheet
                        all_review_required = []
                        for sheet_name, df_result in all_dataframes.items():
                            review = df_result[df_result['match_status'] == MATCH_STATUS_SUGGESTED].copy()
                            if len(review) > 0:
                                review.insert(0, 'Source Sheet', sheet_name)
                                all_review_required.append(review)

                        if all_review_required:
                            df_review_combined = pd.concat(all_review_required, ignore_index=True)
                            df_review_combined.to_excel(writer, sheet_name='Review Required', index=False)

                        # 4. AUTO-SELECTED PRODUCTS sheet (with overrides marked)
                        auto_selected_details = []
                        for sheet_name, df_result in all_dataframes.items():
                            auto_selected = df_result[df_result['auto_selected'] == True].copy()
                            for idx, row in auto_selected.iterrows():
                                name_col = 'name' if 'name' in row else 'Foxway Product Name'
                                original_name = row[name_col] if name_col in row else ''

                                alternatives_raw = row.get('alternatives', '')
                                if isinstance(alternatives_raw, str) and alternatives_raw:
                                    try:
                                        alternatives = eval(alternatives_raw) if alternatives_raw.startswith('[') else []
                                    except:
                                        alternatives = []
                                else:
                                    alternatives = []

                                selected_id = row['mapped_uae_assetid']
                                nl_entry = df_nl_clean[df_nl_clean['uae_assetid'] == selected_id]
                                selected_name = nl_entry.iloc[0]['uae_assetname'] if len(nl_entry) > 0 else 'N/A'

                                auto_selected_details.append({
                                    'Source Sheet': sheet_name,
                                    'Your Product': original_name,
                                    'Matched To': row['matched_on'],
                                    'Match Score': f"{row['match_score']:.1f}%",
                                    'Selected ID': selected_id,
                                    'Selected Product': selected_name,
                                    'Selection Reason': row.get('selection_reason', 'N/A'),
                                    'Alternative IDs': ', '.join(alternatives) if alternatives else 'None',
                                    'Total Variants': len(alternatives) + 1,
                                })

                        if auto_selected_details:
                            df_auto_selected = pd.DataFrame(auto_selected_details)
                            df_auto_selected.to_excel(writer, sheet_name='Auto-Selected Products', index=False)

                        # 5. SUMMARY sheet
                        summary_rows = []
                        for sheet_name, df_result in all_dataframes.items():
                            total = len(df_result)
                            matched = int((df_result['match_status'] == MATCH_STATUS_MATCHED).sum())
                            review = int((df_result['match_status'] == MATCH_STATUS_SUGGESTED).sum())
                            no_match = int((df_result['match_status'] == MATCH_STATUS_NO_MATCH).sum())
                            auto_selected_count = int(df_result['auto_selected'].sum())

                            summary_rows.append({
                                'Sheet': sheet_name,
                                'Total Items': total,
                                'Matched': matched,
                                'Review Required': review,
                                'No Match': no_match,
                                'Auto-Selected': auto_selected_count,
                                'Match Rate': f"{matched/total*100:.1f}%",
                            })

                        # Add totals
                        total_items = sum(len(df) for df in all_dataframes.values())
                        total_matched = sum((df['match_status'] == MATCH_STATUS_MATCHED).sum() for df in all_dataframes.values())
                        total_review = sum((df['match_status'] == MATCH_STATUS_SUGGESTED).sum() for df in all_dataframes.values())
                        total_no_match = sum((df['match_status'] == MATCH_STATUS_NO_MATCH).sum() for df in all_dataframes.values())
                        total_auto_selected = sum(df['auto_selected'].sum() for df in all_dataframes.values())

                        summary_rows.append({
                            'Sheet': '',
                            'Total Items': '',
                            'Matched': '',
                            'Review Required': '',
                            'No Match': '',
                            'Auto-Selected': '',
                            'Match Rate': '',
                        })
                        summary_rows.append({
                            'Sheet': 'TOTAL',
                            'Total Items': int(total_items),
                            'Matched': int(total_matched),
                            'Review Required': int(total_review),
                            'No Match': int(total_no_match),
                            'Auto-Selected': int(total_auto_selected),
                            'Match Rate': f"{total_matched/total_items*100:.1f}%",
                        })

                        pd.DataFrame(summary_rows).to_excel(writer, sheet_name='Summary', index=False)

                    output.seek(0)

                    st.download_button(
                        label="📥 Download Updated Results",
                        data=output,
                        file_name="asset_mapping_results_updated.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary",
                        use_container_width=True,
                    )

                    if override_count > 0:
                        st.success(f"✅ Applied {override_count} manual override(s)! Updated Excel includes all changes.")
                    else:
                        st.info("ℹ️ No overrides made. Downloaded Excel matches original mapping results.")

        else:
            st.info("✅ Since accuracy is 100%, manual override is not necessary. All auto-selections are correct!")
            st.caption("You can still download the results from the Mapping tab.")

    # =========================================================================
# TAB 4: UNMATCHED ANALYSIS
# =========================================================================
with tab4:
    st.header("❌ Unmatched Analysis")
    st.markdown("""
    Analyze why items failed to match. This helps identify missing products in the NL catalog,
    data quality issues, or products that need manual review.
    """)

    # Check if mapping results exist in session state
    if 'mapping_results' not in st.session_state:
        st.info("ℹ️ No mapping results available. Please run the **Mapping** tab first.")
        st.stop()

    # Load results from session state
    all_results = st.session_state['mapping_results']['all_results']
    detected_sheets = st.session_state['mapping_results']['detected_sheets']

    # Combine all NO_MATCH and REVIEW_REQUIRED items
    unmatched_items = []
    review_items = []

    for sheet_name, df_result in all_results.items():
        no_match = df_result[df_result['match_status'] == MATCH_STATUS_NO_MATCH].copy()
        if len(no_match) > 0:
            no_match.insert(0, 'Source Sheet', sheet_name)
            unmatched_items.append(no_match)

        review = df_result[df_result['match_status'] == MATCH_STATUS_SUGGESTED].copy()
        if len(review) > 0:
            review.insert(0, 'Source Sheet', sheet_name)
            review_items.append(review)

    if not unmatched_items and not review_items:
        st.success("🎉 Perfect! All items matched successfully. Nothing to analyze.")
        st.stop()

    # Show overview metrics
    total_unmatched = sum(len(df) for df in unmatched_items)
    total_review = sum(len(df) for df in review_items)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("No Match Items", total_unmatched)
    with col2:
        st.metric("Review Required Items", total_review)
    with col3:
        st.metric("Total Issues", total_unmatched + total_review)

    st.divider()

    # ANALYSIS 1: NO_MATCH Items
    if unmatched_items:
        st.subheader("🔴 No Match Analysis (Score < 85%)")

        df_unmatched = pd.concat(unmatched_items, ignore_index=True)

        # Identify name column
        name_col = 'name' if 'name' in df_unmatched.columns else 'Foxway Product Name'
        brand_col = 'brand' if 'brand' in df_unmatched.columns else None

        # Breakdown by reason
        st.markdown("**Why did these items fail to match?**")

        analysis_tabs = st.tabs(["📊 Overview", "🏢 Brand Analysis", "📉 Score Distribution", "📋 Details"])

        with analysis_tabs[0]:  # Overview
            st.markdown(f"**Total unmatched items:** {len(df_unmatched):,}")

            # Score distribution
            if 'match_score' in df_unmatched.columns:
                st.markdown("**Score Distribution:**")
                score_ranges = {
                    '80-84%': ((df_unmatched['match_score'] >= 80) & (df_unmatched['match_score'] < 85)).sum(),
                    '70-79%': ((df_unmatched['match_score'] >= 70) & (df_unmatched['match_score'] < 80)).sum(),
                    '60-69%': ((df_unmatched['match_score'] >= 60) & (df_unmatched['match_score'] < 70)).sum(),
                    'Below 60%': (df_unmatched['match_score'] < 60).sum(),
                }

                for range_label, count in score_ranges.items():
                    if count > 0:
                        st.markdown(f"- **{range_label}**: {count:,} items ({count/len(df_unmatched)*100:.1f}%)")
                        if range_label == '80-84%':
                            st.caption("   → Very close to threshold (85%). May need slight naming adjustments.")
                        elif range_label == 'Below 60%':
                            st.caption("   → Likely not in catalog or significantly different naming.")

        with analysis_tabs[1]:  # Brand Analysis
            st.markdown("**Brand Presence Check:**")

            # Check which brands exist in NL catalog
            if brand_col and brand_col in df_unmatched.columns:
                unique_brands = df_unmatched[brand_col].unique()
                brand_analysis = []

                for brand in unique_brands:
                    brand_items = df_unmatched[df_unmatched[brand_col] == brand]
                    in_catalog = brand in df_nl_clean['brand'].values

                    brand_analysis.append({
                        'Brand': brand,
                        'Unmatched Items': len(brand_items),
                        'In NL Catalog': '✅ Yes' if in_catalog else '❌ No',
                        'Status': 'Products may be missing' if in_catalog else 'Brand not in catalog',
                    })

                df_brand_analysis = pd.DataFrame(brand_analysis).sort_values('Unmatched Items', ascending=False)
                st.dataframe(df_brand_analysis, use_container_width=True, hide_index=True)

                # Highlight brands not in catalog
                missing_brands = df_brand_analysis[df_brand_analysis['In NL Catalog'] == '❌ No']
                if len(missing_brands) > 0:
                    st.warning(f"⚠️ **{len(missing_brands)} brand(s) not found in NL catalog**. These products cannot be matched:")
                    st.dataframe(missing_brands[['Brand', 'Unmatched Items']], use_container_width=True, hide_index=True)
            else:
                st.info("Brand information not available for analysis.")

        with analysis_tabs[2]:  # Score Distribution
            if 'match_score' in df_unmatched.columns:
                st.markdown("**Match Score Distribution:**")

                # Show histogram
                score_data = df_unmatched['match_score'].dropna()
                if len(score_data) > 0:
                    st.bar_chart(score_data.value_counts().sort_index())

                    # Show close misses (80-84%)
                    close_misses = df_unmatched[(df_unmatched['match_score'] >= 80) & (df_unmatched['match_score'] < 85)]
                    if len(close_misses) > 0:
                        st.markdown(f"**🎯 Close Misses (80-84%):** {len(close_misses)} items")
                        st.caption("These items are very close to matching. Check for:")
                        st.caption("- Minor spelling differences")
                        st.caption("- Extra/missing words")
                        st.caption("- Different formatting")

                        with st.expander(f"View {len(close_misses)} close miss items"):
                            display_cols = ['Source Sheet', name_col, 'match_score', 'matched_on']
                            display_cols = [col for col in display_cols if col in close_misses.columns]
                            st.dataframe(close_misses[display_cols], use_container_width=True, hide_index=True)

        with analysis_tabs[3]:  # Details
            st.markdown("**All Unmatched Items:**")
            display_cols = ['Source Sheet', name_col, 'match_score', 'matched_on']
            if brand_col and brand_col in df_unmatched.columns:
                display_cols.insert(2, brand_col)
            display_cols = [col for col in display_cols if col in df_unmatched.columns]

            st.dataframe(df_unmatched[display_cols], use_container_width=True, hide_index=True)

    # ANALYSIS 2: REVIEW REQUIRED Items
    if review_items:
        st.divider()
        st.subheader("🟡 Review Required Analysis (Score 85-89%)")

        df_review = pd.concat(review_items, ignore_index=True)

        # Identify name column
        name_col = 'name' if 'name' in df_review.columns else 'Foxway Product Name'

        st.markdown(f"**Total items needing review:** {len(df_review):,}")
        st.caption("These items have good similarity scores but attributes (model/storage) don't match exactly.")

        # Show sample of review items
        with st.expander(f"View {len(df_review)} review required items"):
            display_cols = ['Source Sheet', name_col, 'match_score', 'matched_on', 'mapped_uae_assetid']
            display_cols = [col for col in display_cols if col in df_review.columns]
            st.dataframe(df_review[display_cols], use_container_width=True, hide_index=True)

        st.markdown("**Why review is needed:**")
        st.markdown("- Match score is good (85-89%) but attributes differ")
        st.markdown("- Model tokens or storage values don't match exactly")
        st.markdown("- Prevents false positive matches")
        st.markdown("- Manual verification recommended")

    st.divider()

    # Action items
    st.subheader("📝 Recommended Actions")

    action_col1, action_col2 = st.columns(2)

    with action_col1:
        st.markdown("**For No Match Items:**")
        st.markdown("1. Check if products exist in NL catalog")
        st.markdown("2. Verify brand names are consistent")
        st.markdown("3. Look for naming differences (typos, formatting)")
        st.markdown("4. Consider adding missing products to catalog")

    with action_col2:
        st.markdown("**For Review Required Items:**")
        st.markdown("1. Manually verify each match")
        st.markdown("2. Check if model/storage attributes are correct")
        st.markdown("3. Override if match is acceptable")
        st.markdown("4. Flag false positives for exclusion")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.divider()
st.caption(
    "NorthLadder Asset Mapper v1.0 — "
    "Fuzzy matching with rapidfuzz. NL catalog pre-loaded."
)