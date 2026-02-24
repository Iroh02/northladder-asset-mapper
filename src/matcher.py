"""
Thin routing wrapper — delegates to matcher_v1 (Stable) or matcher_v2 (Experimental).

Default engine is "v1" (Stable).  Every public symbol is re-exported from v1 so
that existing callers (app.py, tests, scripts) keep working with zero changes.

Only `run_matching` gains an optional `engine` parameter.
"""

# Re-export the ENTIRE v1 namespace so every existing import still resolves.
from matcher_v1 import *  # noqa: F401,F403

# Explicitly import the v2 run_matching under an alias
from matcher_v1 import run_matching as _run_matching_v1
from matcher_v2 import run_matching as _run_matching_v2

# Also expose v2 helpers that only exist in v2 (added in Phase 1)
try:
    from matcher_v2 import (
        generate_catalog_add_requests as generate_catalog_add_requests_v2,
        generate_diagnostics_sheet as generate_diagnostics_sheet_v2,
    )
except ImportError:
    generate_catalog_add_requests_v2 = None
    generate_diagnostics_sheet_v2 = None

try:
    from matcher_v2 import (
        generate_safety_audit_v2 as generate_safety_audit_v2,
        generate_schema_audit_v2 as generate_schema_audit_v2,
    )
except ImportError:
    generate_safety_audit_v2 = None
    generate_schema_audit_v2 = None

# V2-only helpers used by app.py analyst sheets
try:
    from matcher_v2 import (
        extract_model_family_key as extract_model_family_key,
    )
except ImportError:
    pass  # Falls back to v1's version if available


def run_matching(
    df_input,
    brand_col,
    name_col,
    nl_lookup,
    nl_names,
    threshold=85,
    progress_callback=None,
    brand_index=None,
    attribute_index=None,
    nl_catalog=None,
    diagnostic=False,
    signature_index=None,
    engine="v1",
    widen_mode="aggressive",
):
    """Route to v1 (Stable) or v2 (Experimental) matching engine."""
    if engine == "v2":
        return _run_matching_v2(
            df_input, brand_col, name_col, nl_lookup, nl_names,
            threshold=threshold, progress_callback=progress_callback,
            brand_index=brand_index, attribute_index=attribute_index,
            nl_catalog=nl_catalog, diagnostic=diagnostic,
            signature_index=signature_index, widen_mode=widen_mode,
        )
    # Default: v1 (Stable)
    return _run_matching_v1(
        df_input, brand_col, name_col, nl_lookup, nl_names,
        threshold=threshold, progress_callback=progress_callback,
        brand_index=brand_index, attribute_index=attribute_index,
        nl_catalog=nl_catalog, diagnostic=diagnostic,
        signature_index=signature_index,
    )
