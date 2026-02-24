"""
NL AssetMapper — Streamlit UI

The NL master catalog is bundled with the app (nl_reference/).
Users only need to upload their asset list Excel files.

Run with:
    streamlit run app.py

Version: FULLY FIXED + NL catalog rebuilt with years (Feb 2026)
"""

import io
import json
import os
import streamlit as st
import pandas as pd

from matcher import (
    load_and_clean_nl_list,
    build_nl_lookup,
    build_brand_index,
    build_attribute_index,
    build_signature_index,
    run_matching,
    parse_nl_sheet,
    parse_asset_sheets,
    save_nl_reference,
    load_nl_reference,
    nl_reference_exists,
    delete_nl_reference,
    compute_coverage_metrics,
    detect_catalog_gaps,
    SIMILARITY_THRESHOLD,
    HIGH_CONFIDENCE_THRESHOLD,
    MATCH_STATUS_MATCHED,
    MATCH_STATUS_MULTIPLE,
    MATCH_STATUS_SUGGESTED,
    MATCH_STATUS_NO_MATCH,
    CONFIDENCE_HIGH,
    CONFIDENCE_MEDIUM,
    CONFIDENCE_LOW,
    generate_catalog_add_requests_v2,
    generate_diagnostics_sheet_v2,
    generate_safety_audit_v2,
    generate_schema_audit_v2,
    extract_model_family_key,
    normalize_text,
    normalize_brand,
)

def _parse_alternatives(raw):
    """Safely parse alternatives from JSON string, Python str repr, or raw list."""
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return []
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return []


# ---------------------------------------------------------------------------
# MMS enrichment: UAE → MMS mapping
# ---------------------------------------------------------------------------

_MMS_FILE_NAME = 'EU X MMS X UAE Asset list Mapping.xlsx'

@st.cache_data(show_spinner="Loading MMS mapping...")
def load_mms_mapping():
    """
    Load the UAE → MMS mapping from the catalogue mapping file.

    Returns a dict:  uae_id (str, stripped) → {
        'mms_asset_id': str or '',
        'mms_asset_label': str or '',
        'status': 'FOUND' | 'AMBIGUOUS',
        'candidates_json': str (JSON array) — only for AMBIGUOUS
    }
    If file not found, returns empty dict.
    """
    # Search in common locations relative to app.py
    search_dirs = [
        os.path.dirname(__file__),                       # src/
        os.path.join(os.path.dirname(__file__), '..'),   # project root
        os.path.join(os.path.dirname(__file__), '..', 'data'),
    ]
    fpath = None
    for d in search_dirs:
        candidate = os.path.join(d, _MMS_FILE_NAME)
        if os.path.isfile(candidate):
            fpath = candidate
            break
    if fpath is None:
        return {}

    df = pd.read_excel(fpath, sheet_name='Catalogue Mapping',
                        usecols=['UAE Asset Id', 'MMS AssetId', 'MMS Asset Label'])
    # Normalize join keys
    df['UAE Asset Id'] = df['UAE Asset Id'].astype(str).str.strip()
    df['MMS AssetId'] = df['MMS AssetId'].astype(str).str.strip()
    df['MMS Asset Label'] = df['MMS Asset Label'].astype(str).str.strip()

    # Drop exact full-row duplicates (safe), keep first
    df = df.drop_duplicates()

    # Build lookup, detecting ambiguity (1 UAE → many distinct MMS IDs)
    mapping = {}
    for uae_id, grp in df.groupby('UAE Asset Id'):
        distinct_mms = grp['MMS AssetId'].unique()
        if len(distinct_mms) == 1:
            row = grp.iloc[0]
            mapping[uae_id] = {
                'mms_asset_id': row['MMS AssetId'],
                'mms_asset_label': row['MMS Asset Label'],
                'status': 'FOUND',
                'candidates_json': '',
            }
        else:
            # Ambiguous: multiple distinct MMS IDs for same UAE ID
            cands = [{'mms_asset_id': r['MMS AssetId'], 'mms_asset_label': r['MMS Asset Label']}
                     for _, r in grp.iterrows()]
            mapping[uae_id] = {
                'mms_asset_id': '',
                'mms_asset_label': '',
                'status': 'AMBIGUOUS',
                'candidates_json': json.dumps(cands),
            }
    return mapping


def _mms_lookup_single(uae_id, mms_map):
    """Look up a single UAE Asset ID in the MMS map.
    Returns (mms_asset_id, mms_asset_label, status)."""
    uid = str(uae_id).strip() if pd.notna(uae_id) else ''
    if not uid:
        return '', '', ''
    entry = mms_map.get(uid)
    if entry is None:
        return '', '', 'NOT_FOUND'
    return entry['mms_asset_id'], entry['mms_asset_label'], entry['status']


def _enrich_df_with_mms(df, mms_map, primary_output='UAE'):
    """
    Enrich a DataFrame that has mapped_uae_assetid with MMS columns.

    Adds: mms_asset_id, mms_asset_label, mms_lookup_status,
          primary_output_id, primary_output_catalog.
    Also enriches alt_N_id and blk_N_id columns if present.
    Returns a new DataFrame (does not modify in place).
    """
    if 'mapped_uae_assetid' not in df.columns or not mms_map:
        return df
    df = df.copy()

    # Core enrichment on mapped_uae_assetid
    mms_ids, mms_labels, mms_statuses = [], [], []
    for uid in df['mapped_uae_assetid']:
        mid, mlbl, mst = _mms_lookup_single(uid, mms_map)
        mms_ids.append(mid)
        mms_labels.append(mlbl)
        mms_statuses.append(mst)

    # Insert MMS columns right after mapped_uae_assetid
    insert_at = list(df.columns).index('mapped_uae_assetid') + 1
    # Insert in reverse order so positions stay correct
    df.insert(insert_at, 'mms_lookup_status', mms_statuses)
    df.insert(insert_at, 'mms_asset_label', mms_labels)
    df.insert(insert_at, 'mms_asset_id', mms_ids)

    # Primary output columns
    if primary_output == 'MMS':
        df['primary_output_id'] = df['mms_asset_id']
        df['primary_output_catalog'] = 'MMS'
    else:
        df['primary_output_id'] = df['mapped_uae_assetid']
        df['primary_output_catalog'] = 'UAE'

    # Enrich alt_N_id and blk_N_id columns if present
    for prefix in ('alt', 'blk'):
        for i in range(1, 4):
            id_col = f'{prefix}_{i}_id'
            if id_col in df.columns:
                mms_col_id = f'{prefix}_{i}_mms_id'
                mms_col_lbl = f'{prefix}_{i}_mms_label'
                c_ids, c_lbls = [], []
                for uid in df[id_col]:
                    mid, mlbl, _ = _mms_lookup_single(uid, mms_map)
                    c_ids.append(mid)
                    c_lbls.append(mlbl)
                df[mms_col_id] = c_ids
                df[mms_col_lbl] = c_lbls

    return df


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="NL AssetMapper",
    page_icon="🔗",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🔗 NL AssetMapper")
st.markdown("**Intelligent fuzzy matching with attribute verification and hybrid indexing**")

# ---------------------------------------------------------------------------
# Data Hygiene: Device Type Normalization
# ---------------------------------------------------------------------------
def normalize_device_type(device_type_str):
    """
    Normalize inconsistent device type names to canonical categories.

    Mappings:
    - ipads, ipad → tablet
    - tablet's, tablets → tablet
    - mobile phone, phone, mobiles → mobile
    - smartwatch, smart watch → smartwatch
    - laptop, laptops → laptop
    """
    if not isinstance(device_type_str, str):
        return str(device_type_str).lower().strip()

    normalized = device_type_str.lower().strip()

    # Tablet variants
    if normalized in ('ipads', 'ipad', 'tablet\'s', 'tablets'):
        return 'tablet'

    # Mobile variants
    if normalized in ('mobile phone', 'phone', 'mobiles', 'cell phone'):
        return 'mobile'

    # Smartwatch variants
    if normalized in ('smart watch', 'smartwatches', 'watch'):
        return 'smartwatch'

    # Laptop variants
    if normalized in ('laptops', 'notebook', 'notebooks'):
        return 'laptop'

    return normalized

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

# Engine: default to v2, no user toggle
selected_engine = "v2"

st.sidebar.divider()

# Primary Output ID toggle (UAE vs MMS)
mms_map = load_mms_mapping()
if mms_map:
    primary_output_choice = st.sidebar.radio(
        "Primary Output ID",
        options=["UAE", "MMS"],
        index=0,
        help="UAE: primary_output_id = mapped_uae_assetid. MMS: primary_output_id = MMS AssetId (when found).",
    )
    _ambiguous_count = sum(1 for v in mms_map.values() if v['status'] == 'AMBIGUOUS')
    st.sidebar.caption(f"MMS mapping loaded: {len(mms_map):,} UAE IDs")
    if _ambiguous_count > 0:
        st.sidebar.warning(f"{_ambiguous_count} UAE IDs map to multiple MMS IDs (marked AMBIGUOUS)")
else:
    primary_output_choice = "UAE"
    st.sidebar.caption(f"MMS mapping file not found ({_MMS_FILE_NAME})")

st.sidebar.divider()

# Export View toggle (Analyst View vs Debug View)
export_view = st.sidebar.radio(
    "Export View",
    options=["Analyst View", "Debug View"],
    index=0,
    help="Analyst View: clean columns for analysts. Debug View: all columns for debugging.",
)
_analyst_view = (export_view == "Analyst View")

# MMS Mode Display toggle (only visible when MMS is selected)
if primary_output_choice == 'MMS':
    _mms_display = st.sidebar.radio(
        "MMS Mode Display",
        options=["MMS-first (recommended)", "NL-first (legacy)"],
        index=0,
        help="MMS-first: MMS fields shown first for analysts. NL-first: NL product name shown first.",
    )
    _mms_first = (_mms_display == "MMS-first (recommended)")
else:
    _mms_first = False

# Analyst View: keep ALL original input cols + clean output cols only.
# These are columns ADDED by the matcher / enrichment (not from user's Excel).
_MATCHER_ADDED_COLS = {
    'original_input', 'mapped_uae_assetid', 'match_score', 'match_status',
    'confidence', 'matched_on', 'method', 'auto_selected', 'selection_reason',
    'alternatives', 'category', 'verification_pass', 'verification_reasons',
    'review_reason', 'no_match_reason', 'review_priority', 'review_summary',
    'blocked_candidates', 'nl_product_name',
    'mms_asset_id', 'mms_asset_label', 'mms_lookup_status',
    'mms_resolution_hint',
    'primary_output_id', 'primary_output_catalog',
    'query_category', 'matched_category', 'query_storage', 'matched_storage',
    'query_model_tokens', 'matched_model_tokens',
    'top1_name', 'top1_score', 'top2_name', 'top2_score', 'top3_name', 'top3_score',
}

# --- Analyst View output column specs ---
# UAE mode (no MMS columns)
_ANALYST_MATCHED_OUTPUT = [
    'original_input', 'nl_product_name',
    'match_status', 'match_score', 'confidence',
    'mapped_uae_assetid',
    'alternatives',
]
# MMS-first mode: MMS fields shown before NL
_ANALYST_MATCHED_MMS_FIRST = [
    'mms_asset_id', 'mms_asset_label', 'mms_lookup_status',
    'mms_resolution_hint',
    'primary_output_catalog', 'primary_output_id',
    'mapped_uae_assetid', 'nl_product_name',
    'match_status', 'match_score', 'confidence',
    'alternatives',
]
# MMS NL-first (legacy): NL shown before MMS
_ANALYST_MATCHED_MMS_LEGACY = [
    'original_input', 'nl_product_name',
    'match_status', 'match_score', 'confidence',
    'mapped_uae_assetid',
    'mms_asset_id', 'mms_asset_label', 'mms_lookup_status',
    'mms_resolution_hint',
    'primary_output_catalog', 'primary_output_id',
    'alternatives',
]

# Unmatched: UAE mode
_ANALYST_UNMATCHED_OUTPUT = [
    'original_input',
    'match_status', 'no_match_reason', 'review_summary',
    'mapped_uae_assetid',
    'primary_output_id', 'primary_output_catalog',
]
# Unmatched: MMS mode
_ANALYST_UNMATCHED_MMS = [
    'match_status', 'no_match_reason', 'review_summary',
    'mapped_uae_assetid',
    'mms_lookup_status', 'mms_resolution_hint',
    'primary_output_catalog', 'primary_output_id',
]

# Resolution hints for non-FOUND MMS statuses
_MMS_HINT_MAP = {
    'NOT_FOUND': 'UAE id not present in MMS mapping file (request mapping add)',
    'AMBIGUOUS': 'Multiple MMS ids for this UAE id (needs resolution)',
}


def _add_mms_resolution_hint(df):
    """Add mms_resolution_hint column based on mms_lookup_status."""
    if 'mms_lookup_status' not in df.columns:
        return df
    df = df.copy()
    df['mms_resolution_hint'] = df['mms_lookup_status'].map(
        lambda s: _MMS_HINT_MAP.get(str(s).strip(), '') if pd.notna(s) else ''
    )
    return df


def _apply_analyst_cols(df, output_cols):
    """Keep all original input columns + specified output columns.

    - Original columns = everything NOT added by the matcher/enrichment.
    - Deduplicates Category/category (drops lowercase matcher one).
    """
    # Detect original input columns (preserve order from user's Excel)
    original_cols = [c for c in df.columns if c not in _MATCHER_ADDED_COLS]
    # Dedup: if 'Category' (original) exists, skip 'category' (matcher)
    if 'Category' in original_cols:
        output_cols = [c for c in output_cols if c != 'category']
    # Build final: originals + output cols (only those present)
    append_cols = [c for c in output_cols if c in df.columns]
    final = original_cols + append_cols
    return df[final]


def _get_matched_analyst_cols():
    """Return the right Matched column spec based on current mode."""
    if primary_output_choice == 'MMS':
        return _ANALYST_MATCHED_MMS_FIRST if _mms_first else _ANALYST_MATCHED_MMS_LEGACY
    return _ANALYST_MATCHED_OUTPUT


def _get_unmatched_analyst_cols():
    """Return the right Unmatched column spec based on current mode."""
    if primary_output_choice == 'MMS':
        return _ANALYST_UNMATCHED_MMS
    return _ANALYST_UNMATCHED_OUTPUT


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
    nl_signature_index = build_signature_index(df_nl_clean)

    return {
        'df': df_nl_clean,
        'stats': nl_stats,
        'lookup': nl_lookup,
        'names': nl_names,
        'brand_index': nl_brand_index,
        'attribute_index': nl_attribute_index,
        'signature_index': nl_signature_index,
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
nl_signature_index = catalog['signature_index']

st.success(
    f"NL Reference: **{nl_stats.get('final', len(df_nl_clean)):,}** asset records loaded "
    f"({len(nl_brand_index)} brands, hybrid matching enabled)"
)

# =========================================================================
# Dashboard helper functions
# =========================================================================

@st.cache_data(show_spinner="Loading diagnostic report...")
def load_diagnostic_report(file) -> pd.DataFrame:
    """Load the diagnostic report Excel and return the combined DataFrame."""
    try:
        df = pd.read_excel(file, sheet_name='All Combined')
        return df
    except Exception:
        # Fallback: try first sheet
        df = pd.read_excel(file, sheet_name=0)
        return df


def _safe_col(df, candidates):
    """Return the first column name from candidates that exists in df, or None."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def render_dashboard(df: pd.DataFrame):
    """Render the full mapping performance dashboard from a diagnostic DataFrame."""
    total = len(df)
    if total == 0:
        st.warning("No data in diagnostic report.")
        return

    status_col = _safe_col(df, ['match_status'])
    score_col = _safe_col(df, ['match_score'])
    method_col = _safe_col(df, ['method'])
    source_col = _safe_col(df, ['source_sheet', 'Source Sheet'])
    name_col = _safe_col(df, ['name', 'Foxway Product Name', 'product_name'])
    brand_col = _safe_col(df, ['Brand', 'brand', 'manufacturer'])
    matched_on_col = _safe_col(df, ['matched_on'])
    vpass_col = _safe_col(df, ['verification_pass'])
    vreasons_col = _safe_col(df, ['verification_reasons'])
    qcat_col = _safe_col(df, ['query_category'])
    top1_name = _safe_col(df, ['top1_name', 'top1_candidate_name'])
    top1_score = _safe_col(df, ['top1_score', 'top1_candidate_score'])

    matched = df[df[status_col] == 'MATCHED'] if status_col else pd.DataFrame()
    review = df[df[status_col] == 'REVIEW_REQUIRED'] if status_col else pd.DataFrame()
    no_match = df[df[status_col] == 'NO_MATCH'] if status_col else pd.DataFrame()

    # ---- SECTION 1: Top Summary Metrics ----
    st.subheader("1. Summary Metrics")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Assets", f"{total:,}")
    c2.metric("MATCHED", f"{len(matched):,}", f"{len(matched)/total*100:.1f}%")
    c3.metric("REVIEW REQUIRED", f"{len(review):,}", f"{len(review)/total*100:.1f}%")
    c4.metric("NO MATCH", f"{len(no_match):,}", f"{len(no_match)/total*100:.1f}%")

    c5, c6, c7 = st.columns(3)
    c5.metric("Match Rate", f"{len(matched)/total*100:.1f}%")
    if method_col:
        attr_count = df[method_col].str.contains('attribute', case=False, na=False).sum()
        fuzzy_count = df[method_col].str.contains('fuzzy', case=False, na=False).sum()
        c6.metric("Attribute Match Rate", f"{attr_count/total*100:.1f}%" if total else "0%")
        c7.metric("Fuzzy Match Rate", f"{fuzzy_count/total*100:.1f}%" if total else "0%")

    st.divider()

    # ---- SECTION 2: Match Rate by Sheet ----
    if source_col:
        st.subheader("2. Match Rate by Sheet")
        sheet_stats = []
        for sheet, grp in df.groupby(source_col):
            n = len(grp)
            m = len(grp[grp[status_col] == 'MATCHED']) if status_col else 0
            sheet_stats.append({'Sheet': sheet, 'Total': n, 'Matched': m, 'Match Rate (%)': round(m / n * 100, 1) if n else 0})
        df_sheets = pd.DataFrame(sheet_stats)
        st.bar_chart(df_sheets.set_index('Sheet')['Match Rate (%)'])
        st.dataframe(df_sheets, use_container_width=True, hide_index=True)
        st.divider()

    # ---- SECTION 3: Match Status Breakdown ----
    if status_col:
        st.subheader("3. Match Status Breakdown")
        status_counts = df[status_col].value_counts()
        col_left, col_right = st.columns(2)
        with col_left:
            st.bar_chart(status_counts)
        with col_right:
            for status, count in status_counts.items():
                pct = count / total * 100
                emoji = {"MATCHED": "🟢", "REVIEW_REQUIRED": "🟡", "NO_MATCH": "🔴", "MULTIPLE_MATCHES": "🔵"}.get(status, "⚪")
                st.markdown(f"{emoji} **{status}**: {count:,} ({pct:.1f}%)")
        st.divider()

    # ---- SECTION 4: Match Method Breakdown ----
    if method_col:
        st.subheader("4. Match Method Breakdown")
        method_counts = df[method_col].value_counts()

        # Group into attribute vs fuzzy vs none
        attr_total = method_counts[method_counts.index.str.contains('attribute', case=False, na=False)].sum()
        fuzzy_total = method_counts[method_counts.index.str.contains('fuzzy', case=False, na=False)].sum()
        none_total = method_counts.get('none', 0)
        other_total = total - attr_total - fuzzy_total - none_total

        summary_methods = pd.Series({
            'Attribute (fast path)': int(attr_total),
            'Fuzzy (fallback)': int(fuzzy_total),
            'No match': int(none_total),
        })
        col_left, col_right = st.columns(2)
        with col_left:
            st.bar_chart(summary_methods)
        with col_right:
            st.markdown("**Detailed methods:**")
            for m, c in method_counts.head(10).items():
                st.markdown(f"- `{m}`: {c:,} ({c/total*100:.1f}%)")

        st.divider()

    # ---- SECTION 5: Brand Coverage Analysis ----
    if brand_col and status_col:
        st.subheader("5. Brand Coverage Analysis")
        brand_stats = []
        for brand, grp in df.groupby(df[brand_col].astype(str).str.strip()):
            if brand.lower() in ('nan', 'none', ''):
                continue
            n = len(grp)
            m = len(grp[grp[status_col] == 'MATCHED'])
            brand_stats.append({
                'Brand': brand,
                'Total': n,
                'Matched': m,
                'Match Rate (%)': round(m / n * 100, 1) if n else 0,
            })
        df_brands = pd.DataFrame(brand_stats).sort_values('Match Rate (%)')

        col_left, col_right = st.columns(2)
        with col_left:
            st.markdown("**Lowest 15 brands by match rate:**")
            bottom15 = df_brands.head(15).set_index('Brand')
            st.bar_chart(bottom15['Match Rate (%)'])
        with col_right:
            st.dataframe(df_brands, use_container_width=True, hide_index=True, height=400)

        st.divider()

    # ---- SECTION 6: Near-Miss Analysis ----
    st.subheader("6. Near-Miss Analysis")
    if status_col and top1_score:
        near_miss = df[
            (df[status_col] == 'NO_MATCH') &
            (pd.to_numeric(df[top1_score], errors='coerce') >= 80)
        ]
        st.metric("Near-Miss Items (score 80-84)", len(near_miss))
        if len(near_miss) > 0:
            display_cols = [c for c in [name_col, brand_col, top1_name, top1_score] if c]
            st.dataframe(
                near_miss[display_cols].sort_values(top1_score, ascending=False).head(50),
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("No near-miss items found.")
    elif status_col and score_col:
        near_miss = df[
            (df[status_col] == 'NO_MATCH') &
            (pd.to_numeric(df[score_col], errors='coerce') >= 80)
        ]
        st.metric("Near-Miss Items (score >= 80)", len(near_miss))
        if len(near_miss) > 0:
            display_cols = [c for c in [name_col, brand_col, matched_on_col, score_col] if c]
            st.dataframe(
                near_miss[display_cols].sort_values(score_col, ascending=False).head(50),
                use_container_width=True, hide_index=True,
            )
    st.divider()

    # ---- SECTION 7: Risk Monitoring ----
    st.subheader("7. Risk Monitoring — Potential False Positives")
    if vpass_col and status_col:
        # Items that are MATCHED but verification gate failed
        risk_items = df[
            (df[status_col] == 'MATCHED') &
            (df[vpass_col] == False)
        ]
        st.metric("False Positive Risk Items", len(risk_items))
        if len(risk_items) > 0:
            st.warning(f"Found {len(risk_items)} MATCHED items where verification gate failed. Audit recommended.")
            display_cols = [c for c in [name_col, matched_on_col, score_col, vreasons_col] if c]
            st.dataframe(risk_items[display_cols].head(50), use_container_width=True, hide_index=True)
        else:
            st.success("No false positive risks detected. All MATCHED items pass verification gate.")
    else:
        st.info("Verification gate columns not found in report. Run matching with `diagnostic=True` to enable.")
    st.divider()

    # ---- SECTION 8: Category Coverage ----
    if qcat_col and status_col:
        st.subheader("8. Category Coverage")
        cat_stats = []
        for cat, grp in df.groupby(df[qcat_col].astype(str).str.strip()):
            if cat.lower() in ('nan', 'none', ''):
                continue
            n = len(grp)
            m = len(grp[grp[status_col] == 'MATCHED'])
            cat_stats.append({
                'Category': cat,
                'Total': n,
                'Matched': m,
                'Match Rate (%)': round(m / n * 100, 1) if n else 0,
            })
        df_cats = pd.DataFrame(cat_stats).sort_values('Match Rate (%)', ascending=False)

        col_left, col_right = st.columns(2)
        with col_left:
            st.bar_chart(df_cats.set_index('Category')['Match Rate (%)'])
        with col_right:
            st.dataframe(df_cats, use_container_width=True, hide_index=True)
    elif brand_col and status_col:
        st.subheader("8. Category Coverage")
        st.info("Category column not found in report. Run matching with `diagnostic=True` for category breakdown.")


# =========================================================================
# Tab Navigation
# =========================================================================
# Conditionally show Variant Selector tab based on advanced mode toggle
if show_advanced:
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Dashboard", "🔗 Mapping", "🎯 Variant Selector", "❌ Unmatched Analysis", "📊 Mapping Performance"])
else:
    tab1, tab2, tab4, tab5 = st.tabs(["📊 Dashboard", "🔗 Mapping", "❌ Unmatched Analysis", "📊 Mapping Performance"])
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

    # Hardened Matching Explanation
    st.subheader("🔄 How the Matching Engine Works")
    st.markdown("""
    NL AssetMapper uses a **hardened multi-stage matching pipeline** to ensure accurate results
    while preventing false positives:
    """)

    st.markdown("""
    **Matching Pipeline:**

    1️⃣ **Attribute Matching** (Fast Path)
       - Extracts product attributes: brand, model, storage, category
       - Matches against pre-built attribute index
       - ~70-80% of queries match here instantly
       - Example: "Apple iPhone 13 128GB" → exact attribute match

    2️⃣ **Signature Matching** (Model Code Path)
       - Uses hardware signatures (model codes, serial patterns)
       - Matches products with specific identifiers
       - Example: "Samsung SM-G960F" → signature match

    3️⃣ **Fuzzy Matching** (Fallback)
       - String similarity with brand partitioning
       - Storage pre-filtering for efficiency
       - Token-based comparison
       - Only fires if attribute/signature matching fails

    4️⃣ **Verification Gate** (Quality Control)
       - **Mobile gate**: Exact model, variant (Pro/Max/Ultra), storage
       - **Tablet gate**: Exact family (iPad Pro/Mini), screen size (±0.15"), generation, year
       - **Laptop gate**: Exact processor, generation (no tolerance), RAM, storage
       - Fuzzy matches **always downgraded** to REVIEW_REQUIRED
       - Prevents false positives by requiring exact attribute alignment

    5️⃣ **Results Classification:**
       - ✅ **MATCHED** (≥90% score + passed verification gate)
       - 🟡 **REVIEW_REQUIRED** (85-89% score OR failed gate)
       - 🔴 **NO_MATCH** (<85% score)
    """)

    st.divider()

    # Usage Guide
    st.subheader("📖 How to Use This Tool")
    st.markdown("""
    **Step 1: Upload Your Asset Lists**
    - Go to the **Mapping** tab
    - Upload your Excel file with asset lists
    - The tool auto-detects sheets and columns (Brand, Product Name)

    **Step 2: Run Matching**
    - Click "Run Asset Mapping"
    - The engine processes each sheet automatically
    - Progress bars show real-time status

    **Step 3: Review Results**
    - Download the Excel file with multiple sheets:
      - **Matched**: Successfully mapped assets (ready to use)
      - **Unmatched**: Items with no confident match (needs catalog expansion)
      - **Review Required**: Good matches but attributes differ (manual verification)
      - **Auto-Selected Products**: Items with multiple variants (shows selection logic)
      - **Summary**: Overall statistics

    **Step 4: Analyze Issues (Optional)**
    - Use **Unmatched Analysis** tab to understand why items didn't match
    - Check brand presence, score distribution, and close misses
    - Identify missing products or data quality issues

    **Advanced Features:**
    - Enable "Show Advanced Options" in sidebar for manual variant override
    - Use **Variant Selector** tab to review auto-selections
    - Upload diagnostic reports to **Mapping Performance** tab for deep analysis
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
    st.markdown("Upload an Excel or CSV file with your asset lists — all sheets are auto-detected and matched.")

    # Sample template download
    st.subheader("📥 Download Sample Template")
    st.markdown("""
    **New to the tool?** Download a sample template to see the required format.
    The template shows the correct column names and data structure.
    """)

    # Create sample template DataFrame
    sample_data = {
        'Brand': ['Apple', 'Samsung', 'Dell', 'HP', 'Apple'],
        'Product Name': [
            'iPhone 14 Pro Max 256GB',
            'Galaxy S23 Ultra 512GB',
            'Latitude 5420 Intel Core i7 11th Gen 16GB 512GB',
            'Pavilion Ryzen 5 8GB 256GB',
            'iPad Pro 11 5th Gen WiFi 256GB'
        ],
        'Category': ['Mobile', 'Mobile', 'Laptop', 'Laptop', 'Tablet']
    }
    sample_df = pd.DataFrame(sample_data)

    # Convert to Excel bytes
    sample_excel = io.BytesIO()
    with pd.ExcelWriter(sample_excel, engine='openpyxl') as writer:
        sample_df.to_excel(writer, sheet_name='Asset List', index=False)
    sample_excel.seek(0)

    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label="📥 Download Excel Template",
            data=sample_excel,
            file_name="asset_list_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    with col2:
        # CSV template
        sample_csv = sample_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV Template",
            data=sample_csv,
            file_name="asset_list_template.csv",
            mime="text/csv",
            use_container_width=True
        )

    st.info("💡 **Tip:** Your file must have at least a **Product Name** column. Brand and Category columns are optional but recommended.")

    st.divider()

    # File uploader with CSV support
    st.subheader("📤 Upload Your Asset List")
    asset_upload = st.file_uploader(
        "📁 Upload Asset Lists (.xlsx or .csv)",
        type=["xlsx", "csv"],
        key="asset_upload",
        help="Upload an Excel file with multiple sheets or a CSV file with your asset list"
    )

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
        # Run full mapping
        # ------------------------------------------------------------------
        st.divider()
        if st.button("🚀 Run Asset Mapping", type="primary", use_container_width=True):

            all_results = {}
            all_results_v2 = {}  # Only populated in compare mode

            engines_to_run = ["v1", "v2"] if selected_engine == "compare" else [selected_engine]

            for run_engine in engines_to_run:
              engine_label = f"[{run_engine.upper()}] " if selected_engine == "compare" else ""
              for sheet_name, info in detected_sheets.items():
                st.subheader(f"🔍 {engine_label}Matching: {sheet_name}")
                progress = st.progress(0, text=f"Starting {sheet_name}...")

                def make_progress_cb(prog_bar, sname):
                    def cb(current, total):
                        prog_bar.progress(current / total, text=f"{sname}... {current:,}/{total:,}")
                    return cb

                # Task B: conservative widening for List 1 sheets (avoid review spam)
                _is_list1 = sheet_name.lower().startswith('list 1')
                _widen = 'conservative' if (_is_list1 and run_engine == 'v2') else 'aggressive'

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
                    signature_index=nl_signature_index,
                    engine=run_engine,
                    widen_mode=_widen,
                )
                progress.progress(1.0, text=f"✅ {engine_label}{sheet_name} complete!")

                # Data hygiene: Normalize device types to canonical categories
                if 'category' in df_result.columns:
                    df_result['category'] = df_result['category'].apply(normalize_device_type)

                # Flatten mixed-type columns (lists/dicts) to strings for PyArrow compatibility
                for col in ('alternatives', 'selection_reason'):
                    if col in df_result.columns:
                        df_result[col] = df_result[col].astype(str)

                if run_engine == "v2" and selected_engine == "compare":
                    all_results_v2[sheet_name] = df_result
                    continue  # Don't overwrite v1 results

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
            # Compare mode: show v1 vs v2 diff summary
            # ------------------------------------------------------------------
            if selected_engine == "compare" and all_results_v2:
                st.subheader("V1 vs V2 Comparison")
                for sheet_name in all_results:
                    if sheet_name not in all_results_v2:
                        continue
                    v1_df = all_results[sheet_name]
                    v2_df = all_results_v2[sheet_name]
                    col1, col2 = st.columns(2)
                    for label, df, col in [("Stable (v1)", v1_df, col1), ("Experimental (v2)", v2_df, col2)]:
                        with col:
                            st.markdown(f"**{label} — {sheet_name}**")
                            _t = len(df)
                            _m = int((df['match_status'] == MATCH_STATUS_MATCHED).sum())
                            _r = int((df['match_status'] == MATCH_STATUS_SUGGESTED).sum())
                            _n = int((df['match_status'] == MATCH_STATUS_NO_MATCH).sum())
                            st.metric("Matched", _m, f"{_m/_t*100:.1f}%")
                            st.metric("Review", _r, f"{_r/_t*100:.1f}%")
                            st.metric("No Match", _n, f"{_n/_t*100:.1f}%")

                    # Show rows that changed status between v1 and v2
                    if len(v1_df) == len(v2_df):
                        changed = v1_df['match_status'] != v2_df['match_status']
                        n_changed = changed.sum()
                        if n_changed > 0:
                            st.info(f"{n_changed} rows changed status between v1 and v2 in {sheet_name}")
                            with st.expander(f"View {n_changed} changed rows"):
                                diff_df = v1_df[changed][['original_input', 'match_status', 'matched_on']].copy()
                                diff_df.columns = ['Product', 'v1_status', 'v1_matched_on']
                                diff_df['v2_status'] = v2_df[changed]['match_status'].values
                                diff_df['v2_matched_on'] = v2_df[changed]['matched_on'].values
                                st.dataframe(diff_df, use_container_width=True, hide_index=True)

            # ------------------------------------------------------------------
            # MMS enrichment: add MMS columns to all result DataFrames
            # ------------------------------------------------------------------
            if mms_map:
                for sn in list(all_results.keys()):
                    all_results[sn] = _enrich_df_with_mms(
                        all_results[sn], mms_map, primary_output_choice)
                if selected_engine == "compare" and all_results_v2:
                    for sn in list(all_results_v2.keys()):
                        all_results_v2[sn] = _enrich_df_with_mms(
                            all_results_v2[sn], mms_map, primary_output_choice)

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

                        suffix = ' - Matched'
                        safe_name = sheet_name[:31 - len(suffix)] + suffix
                        out_matched = _apply_analyst_cols(_add_mms_resolution_hint(matched), _get_matched_analyst_cols()) if _analyst_view else matched
                        out_matched.to_excel(writer, sheet_name=safe_name, index=False)

                # 2. UNMATCHED sheets (one per uploaded sheet) - Only NO_MATCH items
                for sheet_name, df_result in all_results.items():
                    unmatched = df_result[df_result['match_status'] == MATCH_STATUS_NO_MATCH]
                    if len(unmatched) > 0:
                        suffix = ' - Unmatched'
                        safe_name = sheet_name[:31 - len(suffix)] + suffix
                        out_unmatched = _apply_analyst_cols(_add_mms_resolution_hint(unmatched), _get_unmatched_analyst_cols()) if _analyst_view else unmatched
                        out_unmatched.to_excel(writer, sheet_name=safe_name, index=False)

                # 3. REVIEW REQUIRED sheet - All REVIEW_REQUIRED items (combined)
                # Uses curated columns to avoid NaN when sheets have different input column names
                # (e.g., List 1 has "manufacturer"/"name"/"type", List 2 has "Brand"/"Foxway Product Name"/"Category")
                all_review_required = []
                for sheet_name, df_result in all_results.items():
                    review = df_result[df_result['match_status'] == MATCH_STATUS_SUGGESTED].copy()
                    if len(review) > 0:
                        # Add real NL product name column for review items
                        nl_product_names = []
                        for idx, row in review.iterrows():
                            asset_id = row['mapped_uae_assetid']
                            nl_entry = df_nl_clean[df_nl_clean['uae_assetid'] == asset_id]
                            nl_name = nl_entry.iloc[0]['uae_assetname'] if len(nl_entry) > 0 else 'N/A'
                            nl_product_names.append(nl_name)

                        review['nl_product_name'] = nl_product_names
                        review.insert(0, 'Source Sheet', sheet_name)
                        all_review_required.append(review)

                if all_review_required:
                    df_review_combined = pd.concat(all_review_required, ignore_index=True)

                    # Parse standardized alternatives into alt_1/2/3 columns
                    for i in range(1, 4):
                        df_review_combined[f'alt_{i}_id'] = ''
                        df_review_combined[f'alt_{i}_name'] = ''
                        df_review_combined[f'alt_{i}_score'] = ''
                        df_review_combined[f'alt_{i}_reason'] = ''
                    for idx, row in df_review_combined.iterrows():
                        alts = _parse_alternatives(row.get('alternatives', ''))
                        for j, alt in enumerate(alts[:3], 1):
                            if isinstance(alt, dict):
                                df_review_combined.at[idx, f'alt_{j}_id'] = alt.get('uae_assetid', '')
                                df_review_combined.at[idx, f'alt_{j}_name'] = alt.get('uae_assetname', '')
                                df_review_combined.at[idx, f'alt_{j}_score'] = alt.get('score', '')
                                df_review_combined.at[idx, f'alt_{j}_reason'] = alt.get('reason', '')

                    # Parse blocked_candidates into blk_1/2/3 columns (Task A)
                    for i in range(1, 4):
                        df_review_combined[f'blk_{i}_id'] = ''
                        df_review_combined[f'blk_{i}_name'] = ''
                        df_review_combined[f'blk_{i}_score'] = ''
                        df_review_combined[f'blk_{i}_reason'] = ''
                    for idx, row in df_review_combined.iterrows():
                        blk = _parse_alternatives(row.get('blocked_candidates', ''))
                        for j, b in enumerate(blk[:3], 1):
                            if isinstance(b, dict):
                                df_review_combined.at[idx, f'blk_{j}_id'] = b.get('uae_assetid', '')
                                df_review_combined.at[idx, f'blk_{j}_name'] = b.get('uae_assetname', '')
                                df_review_combined.at[idx, f'blk_{j}_score'] = b.get('score', '')
                                df_review_combined.at[idx, f'blk_{j}_reason'] = b.get('reason', '')

                    # Sort by review_priority (highest first) if available
                    if 'review_priority' in df_review_combined.columns:
                        df_review_combined = df_review_combined.sort_values('review_priority', ascending=False)

                    # Build curated column set: canonical fields present in ALL sheets
                    review_cols = [
                        'Source Sheet', 'original_input', 'category',
                        'review_priority', 'review_summary',
                        'mapped_uae_assetid', 'nl_product_name',
                        'match_score', 'match_status', 'confidence',
                        'matched_on', 'method',
                        'auto_selected', 'selection_reason',
                        'review_reason', 'no_match_reason',
                        'alt_1_id', 'alt_1_name', 'alt_1_score', 'alt_1_reason',
                        'alt_2_id', 'alt_2_name', 'alt_2_score', 'alt_2_reason',
                        'alt_3_id', 'alt_3_name', 'alt_3_score', 'alt_3_reason',
                        'blk_1_id', 'blk_1_name', 'blk_1_score', 'blk_1_reason',
                        'blk_2_id', 'blk_2_name', 'blk_2_score', 'blk_2_reason',
                        'blk_3_id', 'blk_3_name', 'blk_3_score', 'blk_3_reason',
                        'verification_pass', 'verification_reasons',
                    ]
                    # Add MMS columns only when MMS mode is active
                    if primary_output_choice == 'MMS':
                        # Insert MMS cols after mapped_uae_assetid
                        _mms_insert = review_cols.index('mapped_uae_assetid') + 1
                        for _mc in reversed(['mms_asset_id', 'mms_asset_label', 'mms_lookup_status',
                                             'primary_output_id', 'primary_output_catalog']):
                            review_cols.insert(_mms_insert, _mc)
                        # Add MMS cols for alt/blk
                        for _p in ('alt', 'blk'):
                            for _i in range(1, 4):
                                _after = f'{_p}_{_i}_name'
                                if _after in review_cols:
                                    _pos = review_cols.index(_after) + 1
                                    for _mc in reversed([f'{_p}_{_i}_mms_id', f'{_p}_{_i}_mms_label']):
                                        review_cols.insert(_pos, _mc)
                    # Only include columns that actually exist
                    review_cols = [c for c in review_cols if c in df_review_combined.columns]
                    df_review_combined[review_cols].to_excel(writer, sheet_name='Review Required', index=False)

                # 4. AUTO-SELECTED PRODUCTS sheet - All auto-selected items with details
                auto_selected_details = []
                for sheet_name, df_result in all_results.items():
                    auto_selected = df_result[df_result['auto_selected'] == True].copy()
                    for idx, row in auto_selected.iterrows():
                        # Get original product name from the canonical field
                        original_name = str(row.get('original_input', ''))

                        # Parse alternatives (JSON-safe)
                        alternatives = _parse_alternatives(row.get('alternatives', ''))
                        # Extract IDs from dicts (v2) or use raw strings (v1)
                        alt_ids = []
                        for a in alternatives:
                            if isinstance(a, dict):
                                aid = a.get('uae_assetid', '')
                                if aid:
                                    alt_ids.append(aid)
                            elif isinstance(a, str):
                                alt_ids.append(a)

                        # Get selected product details from NL catalog
                        selected_id = row['mapped_uae_assetid']
                        nl_entry = df_nl_clean[df_nl_clean['uae_assetid'] == selected_id]
                        selected_name = nl_entry.iloc[0]['uae_assetname'] if len(nl_entry) > 0 else 'N/A'

                        # MMS enrichment for auto-selected
                        _mid, _mlbl, _mst = _mms_lookup_single(selected_id, mms_map)

                        detail = {
                            'Source Sheet': sheet_name,
                            'Your Product': original_name,
                            'Matched To': row['matched_on'],
                            'Match Score': f"{row['match_score']:.1f}%",
                            'Selected ID': selected_id,
                            'Selected Product': selected_name,
                            'Selection Reason': row.get('selection_reason', 'N/A'),
                            'Alternative IDs': ', '.join(alt_ids) if alt_ids else 'None',
                            'Total Variants': len(alt_ids) + 1,
                        }
                        if mms_map:
                            detail['mms_asset_id'] = _mid
                            detail['mms_asset_label'] = _mlbl
                            detail['mms_lookup_status'] = _mst
                            detail['primary_output_id'] = _mid if primary_output_choice == 'MMS' else selected_id
                        auto_selected_details.append(detail)

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

                df_summary = pd.DataFrame(summary_rows)
                # Add MMS note row
                _mms_note = (
                    "Note: mms_lookup_status NOT_FOUND means UAE asset id not present "
                    "in MMS mapping reference file (not a matcher failure)."
                )
                df_summary = pd.concat([
                    df_summary,
                    pd.DataFrame([{'Sheet': '', **{c: '' for c in df_summary.columns if c != 'Sheet'}}]),
                    pd.DataFrame([{'Sheet': _mms_note, **{c: '' for c in df_summary.columns if c != 'Sheet'}}]),
                ], ignore_index=True)
                df_summary.to_excel(writer, sheet_name='Summary', index=False)

                # ---- V2-only sheets: Catalog Add Requests + Diagnostics ----
                if selected_engine in ("v2", "compare"):
                    _v2_src = all_results_v2 if selected_engine == "compare" else all_results
                    if generate_catalog_add_requests_v2 is not None:
                        try:
                            df_cat_reqs = generate_catalog_add_requests_v2(_v2_src)
                            if df_cat_reqs is not None and len(df_cat_reqs) > 0:
                                df_cat_reqs.to_excel(writer, sheet_name='Catalog Add Requests', index=False)
                        except Exception:
                            pass  # Don't break output on v2 helper failure
                    if generate_diagnostics_sheet_v2 is not None:
                        try:
                            df_diag = generate_diagnostics_sheet_v2(_v2_src)
                            if df_diag is not None and len(df_diag) > 0:
                                df_diag.to_excel(writer, sheet_name='Diagnostics', index=False)
                        except Exception:
                            pass
                    # Task E: Safety + Schema audit sheets (Debug View only)
                    if not _analyst_view:
                        if generate_safety_audit_v2 is not None:
                            try:
                                df_safety = generate_safety_audit_v2(_v2_src)
                                if df_safety is not None and len(df_safety) > 0:
                                    df_safety.to_excel(writer, sheet_name='V2 Safety Audit', index=False)
                            except Exception:
                                pass
                        if generate_schema_audit_v2 is not None:
                            try:
                                df_schema = generate_schema_audit_v2(_v2_src)
                                if df_schema is not None and len(df_schema) > 0:
                                    df_schema.to_excel(writer, sheet_name='V2 Schema Audit', index=False)
                            except Exception:
                                pass

                    # ---- Analyst-facing action sheets (v2 only, Debug View) ----
                    if not _analyst_view:
                        try:
                            _v2_combined = pd.concat(_v2_src.values(), ignore_index=True)
                            _v2_review = _v2_combined[_v2_combined['match_status'] == MATCH_STATUS_SUGGESTED].copy()

                            # Parse alt/blk scores for priority sorting
                            _v2_review['_alt1_score'] = 0.0
                            _v2_review['_blk1_score'] = 0.0
                            _v2_review['_alt1_id'] = ''
                            _v2_review['_alt1_name'] = ''
                            _v2_review['_blk1_id'] = ''
                            _v2_review['_blk1_name'] = ''
                            for idx, row in _v2_review.iterrows():
                                alts = _parse_alternatives(row.get('alternatives', ''))
                                if alts and isinstance(alts[0], dict):
                                    _v2_review.at[idx, '_alt1_score'] = float(alts[0].get('score', 0) or 0)
                                    _v2_review.at[idx, '_alt1_id'] = alts[0].get('uae_assetid', '')
                                    _v2_review.at[idx, '_alt1_name'] = alts[0].get('uae_assetname', '')
                                blk = _parse_alternatives(row.get('blocked_candidates', ''))
                                if blk and isinstance(blk[0], dict):
                                    _v2_review.at[idx, '_blk1_score'] = float(blk[0].get('score', 0) or 0)
                                    _v2_review.at[idx, '_blk1_id'] = blk[0].get('uae_assetid', '')
                                    _v2_review.at[idx, '_blk1_name'] = blk[0].get('uae_assetname', '')

                            _v2_review['_best_score'] = _v2_review[['_alt1_score', '_blk1_score']].max(axis=1)

                            # Best candidate id/name for quick display
                            _v2_review['suggested_id'] = ''
                            _v2_review['suggested_name'] = ''
                            for idx, row in _v2_review.iterrows():
                                if row['_alt1_score'] >= row['_blk1_score'] and row['_alt1_id']:
                                    _v2_review.at[idx, 'suggested_id'] = row['_alt1_id']
                                    _v2_review.at[idx, 'suggested_name'] = row['_alt1_name']
                                elif row['_blk1_id']:
                                    _v2_review.at[idx, 'suggested_id'] = row['_blk1_id']
                                    _v2_review.at[idx, 'suggested_name'] = row['_blk1_name']

                            # A) Quick Review: high-score candidates (>=90)
                            quick_mask = _v2_review['_best_score'] >= 90
                            df_quick = _v2_review[quick_mask].sort_values('review_priority', ascending=False) if 'review_priority' in _v2_review.columns else _v2_review[quick_mask].sort_values('_best_score', ascending=False)
                            if len(df_quick) > 0:
                                quick_cols = ['original_input', 'category', 'suggested_id', 'suggested_name',
                                              '_best_score', 'review_summary', 'review_reason', 'method']
                                quick_cols = [c for c in quick_cols if c in df_quick.columns]
                                df_quick_out = df_quick[quick_cols].copy()
                                df_quick_out = df_quick_out.rename(columns={'_best_score': 'best_candidate_score'})
                                df_quick_out['action'] = 'Verify ID and approve, or reject'
                                df_quick_out.to_excel(writer, sheet_name='Quick Review', index=False)

                            # B) Deep Review: remaining review rows (score < 90)
                            deep_mask = ~quick_mask
                            df_deep = _v2_review[deep_mask].sort_values('review_priority', ascending=False) if 'review_priority' in _v2_review.columns else _v2_review[deep_mask].sort_values('_best_score', ascending=False)
                            if len(df_deep) > 0:
                                # Parse top 3 candidate names+scores for analyst
                                for i in range(1, 4):
                                    df_deep[f'cand_{i}_name'] = ''
                                    df_deep[f'cand_{i}_score'] = ''
                                for idx, row in df_deep.iterrows():
                                    # Merge alt + blk, sort by score, take top 3
                                    all_cands = []
                                    for src in ('alternatives', 'blocked_candidates'):
                                        parsed = _parse_alternatives(row.get(src, ''))
                                        for c in parsed:
                                            if isinstance(c, dict) and c.get('uae_assetname'):
                                                all_cands.append(c)
                                    all_cands.sort(key=lambda x: float(x.get('score', 0) or 0), reverse=True)
                                    for j, c in enumerate(all_cands[:3], 1):
                                        df_deep.at[idx, f'cand_{j}_name'] = c.get('uae_assetname', '')
                                        df_deep.at[idx, f'cand_{j}_score'] = c.get('score', '')
                                deep_cols = ['original_input', 'category',
                                             'cand_1_name', 'cand_1_score',
                                             'cand_2_name', 'cand_2_score',
                                             'cand_3_name', 'cand_3_score',
                                             'review_summary', 'review_reason', 'method']
                                deep_cols = [c for c in deep_cols if c in df_deep.columns]
                                df_deep[deep_cols].to_excel(writer, sheet_name='Deep Review', index=False)

                            # C) Catalog Missing Likely
                            _v2_nomatch = _v2_combined[_v2_combined['no_match_reason'] == 'CATALOG_MISSING_LIKELY'].copy()
                            if len(_v2_nomatch) > 0:
                                _v2_nomatch['_brand'] = ''
                                _v2_nomatch['_group_key'] = ''
                                for idx, row in _v2_nomatch.iterrows():
                                    name = str(row.get('original_input', ''))
                                    brand = ''
                                    for bcol in ('Brand', 'brand', 'manufacturer'):
                                        if bcol in row.index and pd.notna(row.get(bcol)):
                                            brand = str(row[bcol]).strip()
                                            break
                                    _v2_nomatch.at[idx, '_brand'] = brand
                                    cat = str(row.get('category', ''))
                                    try:
                                        mfk = extract_model_family_key(name, cat, brand_hint=brand)
                                    except Exception:
                                        mfk = ''
                                    if not mfk:
                                        nb = normalize_brand(brand) or brand.lower()
                                        ct = normalize_text(name)
                                        ft = ' '.join(ct.split()[:3]) if ct else ''
                                        mfk = f'{nb}:{ft}' if ft else nb
                                    _v2_nomatch.at[idx, '_group_key'] = mfk

                                # Aggregate by group_key
                                cat_rows = []
                                for gk, grp in _v2_nomatch.groupby('_group_key'):
                                    examples = grp['original_input'].unique()[:3].tolist()
                                    cat_rows.append({
                                        'group_key': gk,
                                        'brand': grp['_brand'].mode().iloc[0] if len(grp['_brand'].mode()) > 0 else '',
                                        'category': grp['category'].mode().iloc[0] if len(grp['category'].mode()) > 0 else '',
                                        'count': len(grp),
                                        'example_1': examples[0] if len(examples) > 0 else '',
                                        'example_2': examples[1] if len(examples) > 1 else '',
                                        'example_3': examples[2] if len(examples) > 2 else '',
                                        'action': 'Add to NL catalog',
                                    })
                                if cat_rows:
                                    df_cat_missing = pd.DataFrame(cat_rows).sort_values('count', ascending=False)
                                    df_cat_missing.to_excel(writer, sheet_name='Catalog Missing Likely', index=False)
                        except Exception:
                            pass  # Don't break export on analyst sheet failure

                # ---- Compare mode: add V2 Summary sheet ----
                if selected_engine == "compare" and all_results_v2:
                    v2_summary = []
                    for sn, df_r in all_results_v2.items():
                        _t = len(df_r)
                        v2_summary.append({
                            'Sheet': sn,
                            'Total Items': _t,
                            'Matched': int((df_r['match_status'] == MATCH_STATUS_MATCHED).sum()),
                            'Review Required': int((df_r['match_status'] == MATCH_STATUS_SUGGESTED).sum()),
                            'No Match': int((df_r['match_status'] == MATCH_STATUS_NO_MATCH).sum()),
                            'Match Rate': f"{(df_r['match_status'] == MATCH_STATUS_MATCHED).sum()/_t*100:.1f}%",
                        })
                    df_v2_summ = pd.DataFrame(v2_summary)
                    df_v2_summ = pd.concat([
                        df_v2_summ,
                        pd.DataFrame([{'Sheet': '', **{c: '' for c in df_v2_summ.columns if c != 'Sheet'}}]),
                        pd.DataFrame([{'Sheet': _mms_note, **{c: '' for c in df_v2_summ.columns if c != 'Sheet'}}]),
                    ], ignore_index=True)
                    df_v2_summ.to_excel(writer, sheet_name='Summary (V2)', index=False)

                    # ---- FIX 3: Compare mode — full V2 Matched/Unmatched/Review/Auto-Selected ----
                    # Matched (V2) — per sheet
                    for sheet_name, df_v2 in all_results_v2.items():
                        matched_v2 = df_v2[df_v2['match_status'] == MATCH_STATUS_MATCHED].copy()
                        if len(matched_v2) > 0:
                            nl_names_v2 = []
                            for _, r in matched_v2.iterrows():
                                aid = r['mapped_uae_assetid']
                                nle = df_nl_clean[df_nl_clean['uae_assetid'] == aid]
                                nl_names_v2.append(nle.iloc[0]['uae_assetname'] if len(nle) > 0 else 'N/A')
                            insert_pos = list(matched_v2.columns).index('mapped_uae_assetid') + 1
                            matched_v2.insert(insert_pos, 'nl_product_name', nl_names_v2)
                            suffix = ' - Matched (V2)'
                            safe_name = sheet_name[:31 - len(suffix)] + suffix
                            out_mv2 = _apply_analyst_cols(_add_mms_resolution_hint(matched_v2), _get_matched_analyst_cols()) if _analyst_view else matched_v2
                            out_mv2.to_excel(writer, sheet_name=safe_name, index=False)

                    # Unmatched (V2) — per sheet
                    for sheet_name, df_v2 in all_results_v2.items():
                        unmatched_v2 = df_v2[df_v2['match_status'] == MATCH_STATUS_NO_MATCH]
                        if len(unmatched_v2) > 0:
                            suffix = ' - Unmatched (V2)'
                            safe_name = sheet_name[:31 - len(suffix)] + suffix
                            out_uv2 = _apply_analyst_cols(_add_mms_resolution_hint(unmatched_v2), _get_unmatched_analyst_cols()) if _analyst_view else unmatched_v2
                            out_uv2.to_excel(writer, sheet_name=safe_name, index=False)

                    # Review Required (V2) — combined, with alt columns
                    all_review_v2 = []
                    for sheet_name, df_v2 in all_results_v2.items():
                        rev = df_v2[df_v2['match_status'] == MATCH_STATUS_SUGGESTED].copy()
                        if len(rev) > 0:
                            nl_names_v2 = []
                            for _, r in rev.iterrows():
                                aid = r['mapped_uae_assetid']
                                nle = df_nl_clean[df_nl_clean['uae_assetid'] == aid]
                                nl_names_v2.append(nle.iloc[0]['uae_assetname'] if len(nle) > 0 else 'N/A')
                            rev['nl_product_name'] = nl_names_v2
                            rev.insert(0, 'Source Sheet', sheet_name)
                            all_review_v2.append(rev)
                    if all_review_v2:
                        df_rev_v2 = pd.concat(all_review_v2, ignore_index=True)
                        for i in range(1, 4):
                            df_rev_v2[f'alt_{i}_id'] = ''
                            df_rev_v2[f'alt_{i}_name'] = ''
                            df_rev_v2[f'alt_{i}_score'] = ''
                            df_rev_v2[f'alt_{i}_reason'] = ''
                        for idx, row in df_rev_v2.iterrows():
                            alts = _parse_alternatives(row.get('alternatives', ''))
                            for j, alt in enumerate(alts[:3], 1):
                                if isinstance(alt, dict):
                                    df_rev_v2.at[idx, f'alt_{j}_id'] = alt.get('uae_assetid', '')
                                    df_rev_v2.at[idx, f'alt_{j}_name'] = alt.get('uae_assetname', '')
                                    df_rev_v2.at[idx, f'alt_{j}_score'] = alt.get('score', '')
                                    df_rev_v2.at[idx, f'alt_{j}_reason'] = alt.get('reason', '')
                        # Blocked candidates (Task A)
                        for i in range(1, 4):
                            df_rev_v2[f'blk_{i}_id'] = ''
                            df_rev_v2[f'blk_{i}_name'] = ''
                            df_rev_v2[f'blk_{i}_score'] = ''
                            df_rev_v2[f'blk_{i}_reason'] = ''
                        for idx, row in df_rev_v2.iterrows():
                            blk = _parse_alternatives(row.get('blocked_candidates', ''))
                            for j, b in enumerate(blk[:3], 1):
                                if isinstance(b, dict):
                                    df_rev_v2.at[idx, f'blk_{j}_id'] = b.get('uae_assetid', '')
                                    df_rev_v2.at[idx, f'blk_{j}_name'] = b.get('uae_assetname', '')
                                    df_rev_v2.at[idx, f'blk_{j}_score'] = b.get('score', '')
                                    df_rev_v2.at[idx, f'blk_{j}_reason'] = b.get('reason', '')
                        rev_cols_v2 = [
                            'Source Sheet', 'original_input', 'category',
                            'mapped_uae_assetid', 'nl_product_name',
                            'match_score', 'match_status', 'confidence',
                            'matched_on', 'method',
                            'auto_selected', 'selection_reason',
                            'review_reason', 'no_match_reason',
                            'alt_1_id', 'alt_1_name', 'alt_1_score', 'alt_1_reason',
                            'alt_2_id', 'alt_2_name', 'alt_2_score', 'alt_2_reason',
                            'alt_3_id', 'alt_3_name', 'alt_3_score', 'alt_3_reason',
                            'blk_1_id', 'blk_1_name', 'blk_1_score', 'blk_1_reason',
                            'blk_2_id', 'blk_2_name', 'blk_2_score', 'blk_2_reason',
                            'blk_3_id', 'blk_3_name', 'blk_3_score', 'blk_3_reason',
                            'verification_pass', 'verification_reasons',
                        ]
                        if primary_output_choice == 'MMS':
                            _ins = rev_cols_v2.index('mapped_uae_assetid') + 1
                            for _mc in reversed(['mms_asset_id', 'mms_asset_label', 'mms_lookup_status',
                                                 'primary_output_id', 'primary_output_catalog']):
                                rev_cols_v2.insert(_ins, _mc)
                            for _p in ('alt', 'blk'):
                                for _i in range(1, 4):
                                    _af = f'{_p}_{_i}_name'
                                    if _af in rev_cols_v2:
                                        _ps = rev_cols_v2.index(_af) + 1
                                        for _mc in reversed([f'{_p}_{_i}_mms_id', f'{_p}_{_i}_mms_label']):
                                            rev_cols_v2.insert(_ps, _mc)
                        rev_cols_v2 = [c for c in rev_cols_v2 if c in df_rev_v2.columns]
                        df_rev_v2[rev_cols_v2].to_excel(writer, sheet_name='Review Required (V2)', index=False)

                    # Auto-Selected Products (V2)
                    auto_v2_details = []
                    for sheet_name, df_v2 in all_results_v2.items():
                        auto_v2 = df_v2[df_v2['auto_selected'] == True].copy()
                        for _, r in auto_v2.iterrows():
                            alternatives = _parse_alternatives(r.get('alternatives', ''))
                            a_ids = [a.get('uae_assetid', '') for a in alternatives if isinstance(a, dict) and a.get('uae_assetid')]
                            sel_id = r['mapped_uae_assetid']
                            nle = df_nl_clean[df_nl_clean['uae_assetid'] == sel_id]
                            sel_name = nle.iloc[0]['uae_assetname'] if len(nle) > 0 else 'N/A'
                            _mid, _mlbl, _mst = _mms_lookup_single(sel_id, mms_map)
                            detail_v2 = {
                                'Source Sheet': sheet_name,
                                'Your Product': str(r.get('original_input', '')),
                                'Matched To': r['matched_on'],
                                'Match Score': f"{r['match_score']:.1f}%",
                                'Selected ID': sel_id,
                                'Selected Product': sel_name,
                                'Selection Reason': r.get('selection_reason', 'N/A'),
                                'Alternative IDs': ', '.join(a_ids) if a_ids else 'None',
                                'Total Variants': len(a_ids) + 1,
                            }
                            if mms_map:
                                detail_v2['mms_asset_id'] = _mid
                                detail_v2['mms_asset_label'] = _mlbl
                                detail_v2['mms_lookup_status'] = _mst
                                detail_v2['primary_output_id'] = _mid if primary_output_choice == 'MMS' else sel_id
                            auto_v2_details.append(detail_v2)
                    if auto_v2_details:
                        pd.DataFrame(auto_v2_details).to_excel(writer, sheet_name='Auto-Selected (V2)', index=False)

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
                    # Get product name from canonical field
                    user_input = str(row.get('original_input', ''))

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
                    product_name = str(row.get('original_input', ''))

                    with st.expander(f"Item {i+1}: {product_name}"):
                        # Show match info
                        col_info, col_select = st.columns([2, 1])

                        with col_info:
                            st.markdown(f"**Your Product:** {product_name}")
                            st.markdown(f"**Matched To:** `{row['matched_on']}`")
                            st.markdown(f"**Match Score:** {row['match_score']:.1f}%")
                            st.markdown(f"**Selection Reason:** {row.get('selection_reason', 'N/A')}")

                        # Parse alternatives (JSON-safe)
                        current_id = str(row['mapped_uae_assetid']).strip()
                        alternatives = _parse_alternatives(row.get('alternatives', ''))
                        # Extract IDs from dicts (v2) or use raw strings (v1)
                        alt_ids = []
                        for a in alternatives:
                            if isinstance(a, dict):
                                aid = a.get('uae_assetid', '')
                                if aid:
                                    alt_ids.append(aid)
                            elif isinstance(a, str):
                                alt_ids.append(a)

                        # Build full list: current ID + alternatives
                        all_ids = [current_id] + alt_ids

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

                    # Re-enrich MMS columns for overridden rows
                    if mms_map and override_count > 0:
                        for key, sel_id in st.session_state.variant_selections.items():
                            parts = key.rsplit('_', 1)
                            if len(parts) != 2:
                                continue
                            sn, idx_s = parts
                            ridx = int(idx_s)
                            if sn not in all_dataframes:
                                continue
                            uid = str(sel_id).strip()
                            mid, mlbl, mst = _mms_lookup_single(uid, mms_map)
                            all_dataframes[sn].at[ridx, 'mms_asset_id'] = mid
                            all_dataframes[sn].at[ridx, 'mms_asset_label'] = mlbl
                            all_dataframes[sn].at[ridx, 'mms_lookup_status'] = mst
                            if primary_output_choice == 'MMS':
                                all_dataframes[sn].at[ridx, 'primary_output_id'] = mid
                                all_dataframes[sn].at[ridx, 'primary_output_catalog'] = mlbl
                            else:
                                all_dataframes[sn].at[ridx, 'primary_output_id'] = uid
                                nl_e = df_nl_clean[df_nl_clean['uae_assetid'] == uid]
                                all_dataframes[sn].at[ridx, 'primary_output_catalog'] = nl_e.iloc[0]['uae_assetname'] if len(nl_e) > 0 else ''

                    # Update session state with modified data
                    st.session_state['mapping_results']['all_results'] = all_dataframes

                    # Generate updated Excel with new structure
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        # 1. MATCHED sheets (updated with overrides)
                        for sheet_name, df_result in all_dataframes.items():
                            matched = df_result[df_result['match_status'] == MATCH_STATUS_MATCHED]
                            if len(matched) > 0:
                                suffix = ' - Matched'
                                safe_name = sheet_name[:31 - len(suffix)] + suffix
                                out_m = _apply_analyst_cols(_add_mms_resolution_hint(matched), _get_matched_analyst_cols()) if _analyst_view else matched
                                out_m.to_excel(writer, sheet_name=safe_name, index=False)

                        # 2. UNMATCHED sheets
                        for sheet_name, df_result in all_dataframes.items():
                            unmatched = df_result[df_result['match_status'] == MATCH_STATUS_NO_MATCH]
                            if len(unmatched) > 0:
                                suffix = ' - Unmatched'
                                safe_name = sheet_name[:31 - len(suffix)] + suffix
                                out_u = _apply_analyst_cols(_add_mms_resolution_hint(unmatched), _get_unmatched_analyst_cols()) if _analyst_view else unmatched
                                out_u.to_excel(writer, sheet_name=safe_name, index=False)

                        # 3. REVIEW REQUIRED sheet (curated columns to avoid NaN across sheets)
                        all_review_required = []
                        for sheet_name, df_result in all_dataframes.items():
                            review = df_result[df_result['match_status'] == MATCH_STATUS_SUGGESTED].copy()
                            if len(review) > 0:
                                review.insert(0, 'Source Sheet', sheet_name)
                                all_review_required.append(review)

                        if all_review_required:
                            df_review_combined = pd.concat(all_review_required, ignore_index=True)
                            review_cols = [
                                'Source Sheet', 'original_input', 'category',
                                'mapped_uae_assetid',
                                'match_score', 'match_status',
                                'confidence', 'matched_on', 'method',
                                'auto_selected', 'selection_reason', 'alternatives',
                                'verification_pass', 'verification_reasons',
                            ]
                            if primary_output_choice == 'MMS':
                                _ins = review_cols.index('mapped_uae_assetid') + 1
                                for _mc in reversed(['mms_asset_id', 'mms_asset_label', 'mms_lookup_status',
                                                     'primary_output_id', 'primary_output_catalog']):
                                    review_cols.insert(_ins, _mc)
                            review_cols = [c for c in review_cols if c in df_review_combined.columns]
                            df_review_combined[review_cols].to_excel(writer, sheet_name='Review Required', index=False)

                        # 4. AUTO-SELECTED PRODUCTS sheet (with overrides marked)
                        auto_selected_details = []
                        for sheet_name, df_result in all_dataframes.items():
                            auto_selected = df_result[df_result['auto_selected'] == True].copy()
                            for idx, row in auto_selected.iterrows():
                                original_name = str(row.get('original_input', ''))

                                alternatives = _parse_alternatives(row.get('alternatives', ''))
                                alt_ids = []
                                for a in alternatives:
                                    if isinstance(a, dict):
                                        aid = a.get('uae_assetid', '')
                                        if aid:
                                            alt_ids.append(aid)
                                    elif isinstance(a, str):
                                        alt_ids.append(a)

                                selected_id = row['mapped_uae_assetid']
                                nl_entry = df_nl_clean[df_nl_clean['uae_assetid'] == selected_id]
                                selected_name = nl_entry.iloc[0]['uae_assetname'] if len(nl_entry) > 0 else 'N/A'

                                _mid, _mlbl, _mst = _mms_lookup_single(selected_id, mms_map)
                                detail_ovr = {
                                    'Source Sheet': sheet_name,
                                    'Your Product': original_name,
                                    'Matched To': row['matched_on'],
                                    'Match Score': f"{row['match_score']:.1f}%",
                                    'Selected ID': selected_id,
                                    'Selected Product': selected_name,
                                    'Selection Reason': row.get('selection_reason', 'N/A'),
                                    'Alternative IDs': ', '.join(alt_ids) if alt_ids else 'None',
                                    'Total Variants': len(alt_ids) + 1,
                                }
                                if mms_map:
                                    detail_ovr['mms_asset_id'] = _mid
                                    detail_ovr['mms_asset_label'] = _mlbl
                                    detail_ovr['mms_lookup_status'] = _mst
                                    detail_ovr['primary_output_id'] = _mid if primary_output_choice == 'MMS' else selected_id
                                auto_selected_details.append(detail_ovr)

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

                        df_ovr_summ = pd.DataFrame(summary_rows)
                        df_ovr_summ = pd.concat([
                            df_ovr_summ,
                            pd.DataFrame([{'Sheet': '', **{c: '' for c in df_ovr_summ.columns if c != 'Sheet'}}]),
                            pd.DataFrame([{'Sheet': _mms_note, **{c: '' for c in df_ovr_summ.columns if c != 'Sheet'}}]),
                        ], ignore_index=True)
                        df_ovr_summ.to_excel(writer, sheet_name='Summary', index=False)

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

        # Identify name column — prefer canonical 'original_input', fall back to legacy names
        name_col = 'original_input' if 'original_input' in df_unmatched.columns else (
            'name' if 'name' in df_unmatched.columns else 'Foxway Product Name')
        brand_col = 'brand' if 'brand' in df_unmatched.columns else None

        # Breakdown by reason
        st.markdown("**Why did these items fail to match?**")

        analysis_tabs = st.tabs(["📊 Overview", "🏢 Brand Analysis", "📋 Details"])

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

        with analysis_tabs[2]:  # Details
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

        # Identify name column — prefer canonical 'original_input', fall back to legacy names
        name_col = 'original_input' if 'original_input' in df_review.columns else (
            'name' if 'name' in df_review.columns else 'Foxway Product Name')

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

# =========================================================================
# TAB 5: MAPPING PERFORMANCE DASHBOARD
# =========================================================================
with tab5:
    # COMMENTED OUT FOR MANAGER DEMO - Performance dashboard hidden
    st.info("📊 Performance dashboard is currently disabled for this demo version.")

    # st.header("📊 Mapping Performance Dashboard")
    # st.markdown("Upload a diagnostic report Excel file to visualize mapping performance, coverage, and risks.")

    # diag_upload = st.file_uploader(
    #     "📁 Upload Diagnostic Report (.xlsx)",
    #     type=["xlsx"],
    #     key="diag_upload",
    #     help="Upload match_diagnostic_report_batch3.xlsx or any diagnostic report generated by the matching engine.",
    # )

    # if diag_upload is not None:
    #     df_diag = load_diagnostic_report(diag_upload)
    #     if df_diag is not None and len(df_diag) > 0:
    #         st.success(f"Loaded {len(df_diag):,} rows from diagnostic report. Columns: {len(df_diag.columns)}")
    #         render_dashboard(df_diag)
    #     else:
    #         st.error("Failed to load diagnostic report or file is empty.")
    # else:
    #     st.info("Upload a diagnostic report to view the performance dashboard.")
    #     st.markdown("""
    #     **How to generate a diagnostic report:**
    #     1. Run the mapping in the **Mapping** tab
    #     2. Or use `run_matching(..., diagnostic=True)` in Python
    #     3. The report should contain columns like `match_status`, `match_score`, `method`, etc.
    #     """)

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.divider()
st.caption(
    "NL AssetMapper v1.0 — "
    "Hybrid matching with rapidfuzz. NL catalog pre-loaded."
)