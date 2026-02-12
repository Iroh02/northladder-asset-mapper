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

st.sidebar.markdown("**Match Status:**")
st.sidebar.markdown("🟢 **MATCHED** — Confident match with single ID (auto-selected if needed)")
st.sidebar.markdown("🟡 **REVIEW REQUIRED** — Needs manual review (score 85-94%, attributes differ)")
st.sidebar.markdown("🔴 **NO_MATCH** — No confident match found (score < 85%)")

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
tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "🔗 Mapping", "🎯 Variant Selector"])

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
        # Output Excel
        # ------------------------------------------------------------------
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for sheet_name, df_result in all_results.items():
                safe_name = f"{sheet_name} - Mapped"[:31]
                df_result.to_excel(writer, sheet_name=safe_name, index=False)

            summary_rows = []
            for sheet_name, df_result in all_results.items():
                total = len(df_result)
                matched = int((df_result['match_status'] == MATCH_STATUS_MATCHED).sum())
                suggested = int((df_result['match_status'] == MATCH_STATUS_SUGGESTED).sum())
                multiple = int((df_result['match_status'] == MATCH_STATUS_MULTIPLE).sum())
                no_match = int((df_result['match_status'] == MATCH_STATUS_NO_MATCH).sum())
                summary_rows.append({
                    'Sheet': sheet_name, 'Total': total,
                    'Matched (HIGH)': matched, 'Review Required': suggested,
                    'Multiple IDs': multiple, 'No Match': no_match,
                    'Auto-Apply Rate': f"{matched/total*100:.2f}%",
                })
            summary_rows.append({'Sheet': '', 'Total': '', 'Matched (HIGH)': '', 'Review Required': '', 'Multiple IDs': '', 'No Match': '', 'Auto-Apply Rate': ''})
            summary_rows.append({
                'Sheet': 'NL Reference', 'Total': nl_stats.get('final', len(df_nl_clean)),
                'Matched (HIGH)': '', 'Review Required': '', 'Multiple IDs': '', 'No Match': '',
                'Auto-Apply Rate': f"Auto-accept >= {HIGH_CONFIDENCE_THRESHOLD}%",
            })
            pd.DataFrame(summary_rows).to_excel(writer, sheet_name='Summary', index=False)

            for sheet_name, df_result in all_results.items():
                suggested = df_result[df_result['match_status'] == MATCH_STATUS_SUGGESTED]
                if len(suggested) > 0:
                    safe_name = f"{sheet_name} - Review"[:31]
                    suggested.to_excel(writer, sheet_name=safe_name, index=False)
                unmatched = df_result[df_result['match_status'] == MATCH_STATUS_NO_MATCH]
                if len(unmatched) > 0:
                    safe_name = f"{sheet_name} - Unmatched"[:31]
                    unmatched.to_excel(writer, sheet_name=safe_name, index=False)

            # Add variant details for MULTIPLE_MATCHES items
            variant_details = []
            for sheet_name, df_result in all_results.items():
                multiple = df_result[df_result['match_status'] == MATCH_STATUS_MULTIPLE]
                for idx, row in multiple.iterrows():
                    # Get original product name
                    name_col = 'name' if 'name' in row else 'Foxway Product Name'
                    original_name = row[name_col]

                    # Split comma-separated IDs
                    ids = str(row['mapped_uae_assetid']).split(',')
                    ids = [id.strip() for id in ids]

                    # Look up each ID in NL catalog to get full product name
                    for id_val in ids:
                        nl_entry = df_nl_clean[df_nl_clean['uae_assetid'] == id_val]
                        if len(nl_entry) > 0:
                            variant_details.append({
                                'Sheet': sheet_name,
                                'Your Product': original_name,
                                'Matched To': row['matched_on'],
                                'Match Score': f"{row['match_score']:.1f}%",
                                'Variant ID': id_val,
                                'NL Product Name': nl_entry.iloc[0]['uae_assetname'],
                                'Category': nl_entry.iloc[0]['category'],
                                'Brand': nl_entry.iloc[0]['brand'],
                            })

            if variant_details:
                df_variants = pd.DataFrame(variant_details)
                df_variants.to_excel(writer, sheet_name='Multiple ID Variants', index=False)

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
# TAB 3: VARIANT SELECTOR
# =========================================================================
with tab3:
    st.header("🎯 Interactive Variant Selector")
    st.markdown("""
    Review and override auto-selected variants. The system automatically selects the best variant based on
    your product's specs (year, 5G/4G), but you can manually choose a different variant if needed.

    Upload your latest mapping results from the **Mapping** tab to review auto-selections.
    """)

    # Upload results file
    results_upload = st.file_uploader("📁 Upload Mapping Results (.xlsx)", type=["xlsx"], key="results_upload")

    if results_upload is not None:
        try:
            # Load all sheets
            df_l1_mapped = pd.read_excel(results_upload, sheet_name='List 1 - Mapped')
            df_l2_mapped = pd.read_excel(results_upload, sheet_name=' List 2 - Mapped')

            # Find auto-selected items (these have alternatives to choose from)
            l1_autoselect = df_l1_mapped[df_l1_mapped['auto_selected'] == True]
            l2_autoselect = df_l2_mapped[df_l2_mapped['auto_selected'] == True]

            total_autoselect = len(l1_autoselect) + len(l2_autoselect)

            if total_autoselect == 0:
                st.info("ℹ️ No auto-selected variants found. All items have single unique IDs.")
                st.stop()

            # Show stats
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Auto-Selected Items", total_autoselect)
            with col2:
                st.metric("List 1", len(l1_autoselect))
            with col3:
                st.metric("List 2", len(l2_autoselect))

            st.divider()

            # Interactive selector
            st.subheader("Review Auto-Selected Variants")
            st.caption("✓ Auto-selection logic: Year → Connectivity (5G/4G) → First ID")

            # Initialize session state for selections
            if 'variant_selections' not in st.session_state:
                st.session_state.variant_selections = {}

            # Combine all auto-selected items
            all_autoselect = []
            for idx, row in l1_autoselect.iterrows():
                all_autoselect.append(('List 1', idx, row))
            for idx, row in l2_autoselect.iterrows():
                all_autoselect.append(('List 2', idx, row))

            # Show items with variant selection
            st.markdown(f"**Showing {min(20, len(all_autoselect))} of {len(all_autoselect)} items** (first 20 for demo)")

            for i, (sheet, idx, row) in enumerate(all_autoselect[:20]):
                with st.expander(f"Item {i+1}: {row.get('name', row.get('Foxway Product Name', ''))}"):
                    # Show match info
                    col_info, col_select = st.columns([2, 1])

                    with col_info:
                        st.markdown(f"**Your Product:** {row.get('name', row.get('Foxway Product Name', ''))}")
                        st.markdown(f"**Matched To:** `{row['matched_on']}`")
                        st.markdown(f"**Match Score:** {row['match_score']:.1f}%")
                        st.markdown(f"**Selection Reason:** {row.get('selection_reason', 'N/A')}")

                    # Parse alternatives (stored as list in Excel)
                    current_id = str(row['mapped_uae_assetid']).strip()

                    # Get alternatives from the alternatives column
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

                    variant_options = []
                    for id_val in all_ids:
                        nl_entry = df_nl_clean[df_nl_clean['uae_assetid'] == id_val]
                        if len(nl_entry) > 0:
                            product_name = nl_entry.iloc[0]['uae_assetname']
                            prefix = "✓ **SELECTED:** " if id_val == current_id else "   "
                            variant_options.append(f"{id_val}: {product_name}")
                            st.markdown(f"{prefix}`{id_val}`: {product_name}")

                    # Selection dropdown
                    with col_select:
                        key = f"{sheet}_{idx}"
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

            # Apply selections button
            if st.button("✅ Apply Overrides & Download", type="primary", use_container_width=True):
                # Count overrides
                override_count = 0

                # Apply selections to dataframes
                for key, selected_id in st.session_state.variant_selections.items():
                    sheet, idx_str = key.split('_', 1)
                    idx = int(idx_str)

                    if sheet == 'List 1':
                        original_id = df_l1_mapped.at[idx, 'mapped_uae_assetid']
                        if str(selected_id) != str(original_id):
                            df_l1_mapped.at[idx, 'mapped_uae_assetid'] = selected_id
                            df_l1_mapped.at[idx, 'selection_reason'] = 'Manually overridden'
                            override_count += 1
                    else:
                        original_id = df_l2_mapped.at[idx, 'mapped_uae_assetid']
                        if str(selected_id) != str(original_id):
                            df_l2_mapped.at[idx, 'mapped_uae_assetid'] = selected_id
                            df_l2_mapped.at[idx, 'selection_reason'] = 'Manually overridden'
                            override_count += 1

                # Generate output Excel
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_l1_mapped.to_excel(writer, sheet_name='List 1 - Mapped', index=False)
                    df_l2_mapped.to_excel(writer, sheet_name=' List 2 - Mapped', index=False)

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
                    st.success(f"✅ Applied {override_count} manual override(s)!")
                else:
                    st.info("ℹ️ No overrides made. All auto-selections kept.")

        except Exception as e:
            st.error(f"Error loading results: {e}")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.divider()
st.caption(
    "NorthLadder Asset Mapper v1.0 — "
    "Fuzzy matching with rapidfuzz. NL catalog pre-loaded."
)