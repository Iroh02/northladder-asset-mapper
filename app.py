"""
NorthLadder Asset Mapping Tool — Streamlit UI

Architecture:
    Phase 1 (one-time): Upload NL master list -> cleaned & saved to disk
    Phase 2 (daily use): Upload any Excel with asset sheets -> all matched against saved NL

Run with:
    streamlit run app.py
"""

import io
import streamlit as st
import pandas as pd

from matcher import (
    load_and_clean_nl_list,
    build_nl_lookup,
    run_matching,
    test_single_match,
    parse_nl_sheet,
    parse_asset_sheets,
    save_nl_reference,
    load_nl_reference,
    nl_reference_exists,
    delete_nl_reference,
    SIMILARITY_THRESHOLD,
    MATCH_STATUS_MATCHED,
    MATCH_STATUS_MULTIPLE,
    MATCH_STATUS_NO_MATCH,
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

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.header("⚙️ Settings")
threshold = st.sidebar.slider(
    "Similarity Threshold (%)",
    min_value=50, max_value=100, value=SIMILARITY_THRESHOLD, step=1,
    help="Minimum fuzzy match score. Default 85%.",
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Status Legend:**")
st.sidebar.markdown("🟢 **MATCHED** — Single confident match")
st.sidebar.markdown("🟡 **MULTIPLE_MATCHES** — Multiple IDs, needs review")
st.sidebar.markdown("🔴 **NO_MATCH** — Below threshold, manual mapping needed")

# =========================================================================
# STEP 1 — NL Reference (one-time)
# =========================================================================
st.header("Step 1: NorthLadder Reference Catalog")

has_reference = nl_reference_exists()

if has_reference:
    nl_data = load_nl_reference()
    if nl_data is not None:
        df_nl_clean, nl_stats = nl_data
        st.success(
            f"NL Reference loaded — **{nl_stats['final']:,}** usable records "
            f"(from {nl_stats['original']:,} original, "
            f"{nl_stats['null_dropped']:,} null + {nl_stats['test_dropped']:,} test dropped)"
        )
        with st.expander("Preview NL Reference (first 10 rows)"):
            st.dataframe(df_nl_clean.head(10), use_container_width=True, hide_index=True)

        if st.button("Refresh NL Reference", help="Re-upload the NL catalog if it has been updated"):
            delete_nl_reference()
            st.rerun()
    else:
        has_reference = False

if not has_reference:
    st.info(
        "No NL reference saved yet. Upload your NorthLadder master Excel "
        "(must have a **NorthLadder List** sheet)."
    )
    nl_upload = st.file_uploader("Upload NL Master Excel (.xlsx)", type=["xlsx"], key="nl_upload")

    if nl_upload is not None:
        try:
            df_nl_raw = parse_nl_sheet(nl_upload)
        except Exception as e:
            st.error(f"Failed to parse NL sheet: {e}")
            st.stop()

        st.write(f"Parsed **{len(df_nl_raw):,}** raw NL rows.")

        if st.button("Save as NL Reference", type="primary"):
            with st.spinner("Cleaning and saving..."):
                df_nl_clean, nl_stats = load_and_clean_nl_list(df_nl_raw)
                save_nl_reference(df_nl_clean, nl_stats)
            st.success(f"Saved {nl_stats['final']:,} usable records.")
            st.rerun()

    st.stop()

# Build lookup once from saved reference
nl_lookup = build_nl_lookup(df_nl_clean)
nl_names = list(nl_lookup.keys())

# =========================================================================
# STEP 2 — Upload asset lists & run matching
# =========================================================================
st.divider()
st.header("Step 2: Upload Asset Lists & Run Mapping")
st.markdown("Upload any Excel file — all sheets will be auto-detected and matched against the NL reference.")

asset_upload = st.file_uploader("Upload Asset Lists Excel (.xlsx)", type=["xlsx"], key="asset_upload")

if asset_upload is not None:
    try:
        detected_sheets = parse_asset_sheets(asset_upload)
    except Exception as e:
        st.error(f"Failed to parse: {e}")
        st.stop()

    if not detected_sheets:
        st.warning("No matchable sheets found. Make sure the Excel has sheets with product name columns.")
        st.stop()

    # Show what was detected
    st.subheader(f"📊 Detected {len(detected_sheets)} sheet(s)")
    for sheet_name, info in detected_sheets.items():
        brand_label = info['brand_col'] or '(none — will match on name only)'
        st.markdown(
            f"- **{sheet_name}** — {len(info['df']):,} rows | "
            f"Brand col: `{brand_label}` | Name col: `{info['name_col']}`"
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
    tc1, tc2, tc3 = st.columns([2, 2, 1])
    with tc1:
        test_brand = st.text_input("Brand", value="Apple", key="test_brand")
    with tc2:
        test_name = st.text_input("Product Name", value="iPhone 6 16GB", key="test_name")
    with tc3:
        st.write("")
        st.write("")
        test_btn = st.button("Test Match", use_container_width=True)

    if test_btn:
        result = test_single_match(test_brand, test_name, nl_lookup, nl_names, threshold)
        st.markdown(f"**Query:** `{result['query']}`")
        if 'error' in result:
            st.error(result['error'])
        else:
            best = result['best_match']
            st.markdown(f"**Result:** `{best['match_status']}` (Score: {best['match_score']}%)")
            if best['mapped_uae_assetid']:
                st.success(f"Asset ID: `{best['mapped_uae_assetid']}`")
                st.caption(f"Matched on: `{best['matched_on']}`")
            alt_df = pd.DataFrame(result['top_3_alternatives'])
            alt_df['asset_ids'] = alt_df['asset_ids'].apply(lambda x: ', '.join(x) if x else 'N/A')
            st.dataframe(alt_df[['nl_name', 'score', 'status', 'asset_ids']], use_container_width=True, hide_index=True)

    # ------------------------------------------------------------------
    # Run full mapping on ALL sheets
    # ------------------------------------------------------------------
    st.divider()
    if st.button("🚀 Run Asset Mapping", type="primary", use_container_width=True):

        all_results = {}  # sheet_name -> df_result

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
            )
            progress.progress(1.0, text=f"✅ {sheet_name} complete!")
            all_results[sheet_name] = df_result

            # Stats
            matched = (df_result['match_status'] == MATCH_STATUS_MATCHED).sum()
            multiple = (df_result['match_status'] == MATCH_STATUS_MULTIPLE).sum()
            no_match = (df_result['match_status'] == MATCH_STATUS_NO_MATCH).sum()
            total = len(df_result)

            ca, cb, cc = st.columns(3)
            ca.metric("✅ Matched", matched, f"{matched/total*100:.1f}%")
            cb.metric("⚠️ Multiple", multiple, f"{multiple/total*100:.1f}%")
            cc.metric("❌ No Match", no_match, f"{no_match/total*100:.1f}%")

        # ------------------------------------------------------------------
        # Preview results
        # ------------------------------------------------------------------
        st.subheader("📋 Preview Results")

        def color_status(val):
            if val == MATCH_STATUS_MATCHED:
                return 'background-color: #d4edda; color: #155724'
            elif val == MATCH_STATUS_MULTIPLE:
                return 'background-color: #fff3cd; color: #856404'
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
                n_unmatched = (df_result['match_status'] == MATCH_STATUS_NO_MATCH).sum()
                if n_unmatched > 0:
                    with st.expander(f"View {n_unmatched} Unmatched Items"):
                        unmatched = df_result[df_result['match_status'] == MATCH_STATUS_NO_MATCH]
                        st.dataframe(unmatched, use_container_width=True, hide_index=True)

        # ------------------------------------------------------------------
        # Build output Excel
        # ------------------------------------------------------------------
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Each sheet's mapped results
            for sheet_name, df_result in all_results.items():
                safe_name = f"{sheet_name} - Mapped"[:31]  # Excel 31-char sheet name limit
                df_result.to_excel(writer, sheet_name=safe_name, index=False)

            # Summary
            summary_rows = []
            for sheet_name, df_result in all_results.items():
                total = len(df_result)
                matched = int((df_result['match_status'] == MATCH_STATUS_MATCHED).sum())
                multiple = int((df_result['match_status'] == MATCH_STATUS_MULTIPLE).sum())
                no_match = int((df_result['match_status'] == MATCH_STATUS_NO_MATCH).sum())
                summary_rows.append({
                    'Sheet': sheet_name,
                    'Total': total,
                    'Matched': matched,
                    'Multiple Matches': multiple,
                    'No Match': no_match,
                    'Match Rate': f"{matched/total*100:.2f}%",
                })
            summary_rows.append({
                'Sheet': '', 'Total': '', 'Matched': '',
                'Multiple Matches': '', 'No Match': '',
                'Match Rate': '',
            })
            summary_rows.append({
                'Sheet': 'NL Reference Records', 'Total': nl_stats['final'],
                'Matched': '', 'Multiple Matches': '',
                'No Match': '', 'Match Rate': f"Threshold: {threshold}%",
            })
            pd.DataFrame(summary_rows).to_excel(writer, sheet_name='Summary', index=False)

            # Unmatched sheets for manual review
            for sheet_name, df_result in all_results.items():
                unmatched = df_result[df_result['match_status'] == MATCH_STATUS_NO_MATCH]
                if len(unmatched) > 0:
                    safe_name = f"{sheet_name} - Unmatched"[:31]
                    unmatched.to_excel(writer, sheet_name=safe_name, index=False)

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

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.divider()
st.caption(
    "NorthLadder Asset Mapper v1.0 — "
    "Auto-detects all sheets, fuzzy matches with rapidfuzz. "
    "NL reference is saved locally and reused across sessions."
)