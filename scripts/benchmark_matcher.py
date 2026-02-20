"""
Micro-benchmark for matcher.py performance analysis.

Tests:
1. build_attribute_index() on synthetic 10k catalog
2. run_matching() end-to-end on synthetic 1k input sheet
3. Individual function hot spots (normalize_text, extract_product_attributes)

Usage:
    python benchmark_matcher.py
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import time
import pandas as pd
import numpy as np
from matcher import (
    build_attribute_index, run_matching, normalize_text,
    extract_product_attributes, load_and_clean_nl_list,
    build_nl_lookup, build_brand_index
)


def generate_synthetic_nl_catalog(n_rows: int = 10000) -> pd.DataFrame:
    """Generate synthetic NL catalog for benchmarking."""
    brands = ['Apple', 'Samsung', 'Google', 'Xiaomi', 'Huawei', 'OnePlus', 'Oppo', 'Vivo']
    models = ['11', '12', '13', '14', '15']
    variants = ['', ' Pro', ' Pro Max', ' Plus', ' Ultra', ' Lite']
    storage = ['64GB', '128GB', '256GB', '512GB', '1TB']

    data = []
    for i in range(n_rows):
        brand = np.random.choice(brands)
        model = np.random.choice(models)
        variant = np.random.choice(variants)
        stor = np.random.choice(storage)

        if brand == 'Apple':
            name = f"iPhone {model}{variant} {stor}"
        elif brand == 'Samsung':
            name = f"Galaxy S{model}{variant} {stor}"
        elif brand == 'Google':
            name = f"Pixel {model}{variant} {stor}"
        else:
            name = f"{brand} {model}{variant} {stor}"

        data.append({
            'category': 'Mobile',
            'brand': brand,
            'uae_assetid': f'UAE-{i:05d}',
            'uae_assetname': name
        })

    return pd.DataFrame(data)


def generate_synthetic_input(n_rows: int = 1000) -> pd.DataFrame:
    """Generate synthetic input sheet for matching."""
    brands = ['Apple', 'Samsung', 'Google', 'Xiaomi', 'Huawei']
    models = ['11', '12', '13', '14', '15']
    variants = ['', ' Pro', ' Pro Max', ' Plus', ' Ultra']
    storage = ['64GB', '128GB', '256GB', '512GB']

    data = []
    for i in range(n_rows):
        brand = np.random.choice(brands)
        model = np.random.choice(models)
        variant = np.random.choice(variants)
        stor = np.random.choice(storage)

        if brand == 'Apple':
            name = f"iPhone {model}{variant} {stor}"
        else:
            name = f"{brand} {model}{variant} {stor}"

        data.append({
            'Manufacturer': brand,
            'Product Name': name
        })

    return pd.DataFrame(data)


def benchmark_function(func, *args, **kwargs):
    """Benchmark a function and return (result, elapsed_ms)."""
    start = time.perf_counter()
    result = func(*args, **kwargs)
    end = time.perf_counter()
    elapsed_ms = (end - start) * 1000
    return result, elapsed_ms


def benchmark_normalize_text(n_iterations: int = 10000):
    """Benchmark normalize_text() on hot path."""
    test_strings = [
        "Apple iPhone 14 Pro Max 256GB (2022)",
        "Samsung Galaxy S23 Ultra 512GB 5G Dual SIM",
        "Google Pixel 9 Pro XL 128GB",
        "Huawei Mate 40 Pro 256GB LTE",
        "Dell XPS 15 9520 - Core i7-12700H / 16GB / 512GB SSD",
    ]

    print("\n" + "="*70)
    print("BENCHMARK: normalize_text() - Hot Path (called 20k+ times)")
    print("="*70)

    for test_str in test_strings:
        start = time.perf_counter()
        for _ in range(n_iterations):
            _ = normalize_text(test_str)
        end = time.perf_counter()

        elapsed_ms = (end - start) * 1000
        per_call_us = elapsed_ms * 1000 / n_iterations

        print(f"\nInput: {test_str}")
        print(f"  Total: {elapsed_ms:.2f}ms ({n_iterations} calls)")
        print(f"  Per call: {per_call_us:.2f}μs")


def benchmark_build_attribute_index():
    """Benchmark build_attribute_index() on 10k catalog."""
    print("\n" + "="*70)
    print("BENCHMARK: build_attribute_index() - 10k NL Catalog")
    print("="*70)

    # Generate 10k synthetic catalog
    print("\nGenerating 10k synthetic NL catalog...")
    df_nl = generate_synthetic_nl_catalog(10000)

    # Clean catalog
    print("Cleaning catalog...")
    df_nl_clean, cleanup_time = benchmark_function(load_and_clean_nl_list, df_nl)
    print(f"  Cleanup: {cleanup_time:.2f}ms")

    # Build attribute index
    print("Building attribute index...")
    attr_index, elapsed = benchmark_function(build_attribute_index, df_nl_clean)
    print(f"  Attribute index: {elapsed:.2f}ms")

    # Count entries
    total_entries = 0
    for brand in attr_index:
        for line in attr_index[brand]:
            for model in attr_index[brand][line]:
                total_entries += len(attr_index[brand][line][model])

    print(f"\nIndex Stats:")
    print(f"  Brands: {len(attr_index)}")
    print(f"  Total entries: {total_entries}")
    print(f"  Indexing rate: {len(df_nl_clean) / (elapsed / 1000):.0f} rows/sec")


def benchmark_run_matching():
    """Benchmark run_matching() end-to-end on 1k input."""
    print("\n" + "="*70)
    print("BENCHMARK: run_matching() - 1k Input Sheet")
    print("="*70)

    # Generate 10k NL catalog
    print("\nGenerating 10k NL catalog...")
    df_nl = generate_synthetic_nl_catalog(10000)
    df_nl_clean, _ = load_and_clean_nl_list(df_nl)

    # Build indexes
    print("Building indexes...")
    nl_lookup_start = time.perf_counter()
    nl_lookup = build_nl_lookup(df_nl_clean)
    nl_names = list(nl_lookup.keys())
    nl_lookup_time = (time.perf_counter() - nl_lookup_start) * 1000

    brand_index_start = time.perf_counter()
    brand_index = build_brand_index(df_nl_clean)
    brand_index_time = (time.perf_counter() - brand_index_start) * 1000

    attr_index_start = time.perf_counter()
    attr_index = build_attribute_index(df_nl_clean)
    attr_index_time = (time.perf_counter() - attr_index_start) * 1000

    print(f"  NL lookup: {nl_lookup_time:.2f}ms")
    print(f"  Brand index: {brand_index_time:.2f}ms")
    print(f"  Attribute index: {attr_index_time:.2f}ms")
    print(f"  Total index time: {nl_lookup_time + brand_index_time + attr_index_time:.2f}ms")

    # Generate 1k input sheet
    print("\nGenerating 1k input sheet...")
    df_input = generate_synthetic_input(1000)

    # Run matching
    print("Running matching (1k items)...")
    match_start = time.perf_counter()
    df_result = run_matching(
        df_input,
        brand_col='Manufacturer',
        name_col='Product Name',
        nl_lookup=nl_lookup,
        nl_names=nl_names,
        brand_index=brand_index,
        attribute_index=attr_index,
        nl_catalog=df_nl_clean,
    )
    match_time = (time.perf_counter() - match_start) * 1000

    print(f"  Matching time: {match_time:.2f}ms")
    print(f"  Per-item time: {match_time / len(df_input):.2f}ms")
    print(f"  Throughput: {len(df_input) / (match_time / 1000):.0f} items/sec")

    # Show match stats
    match_counts = df_result['match_status'].value_counts()
    print(f"\nMatch Results:")
    for status, count in match_counts.items():
        print(f"  {status}: {count} ({count/len(df_result)*100:.1f}%)")


def benchmark_extract_attributes():
    """Benchmark extract_product_attributes() on various inputs."""
    print("\n" + "="*70)
    print("BENCHMARK: extract_product_attributes() - Attribute Extraction")
    print("="*70)

    test_cases = [
        ("Apple", "iPhone 14 Pro Max 256GB"),
        ("Samsung", "Galaxy S23 Ultra 512GB"),
        ("Google", "Pixel 9 Pro XL 128GB"),
        ("Dell", "XPS 15 9520 Core i7-12700H 16GB 512GB"),
        ("Apple", "Watch Series 9 GPS 45mm"),
    ]

    n_iterations = 1000

    for brand, name in test_cases:
        normalized = normalize_text(f"{brand} {name}")

        start = time.perf_counter()
        for _ in range(n_iterations):
            _ = extract_product_attributes(normalized, brand)
        end = time.perf_counter()

        elapsed_ms = (end - start) * 1000
        per_call_us = elapsed_ms * 1000 / n_iterations

        print(f"\nInput: {brand} {name}")
        print(f"  Total: {elapsed_ms:.2f}ms ({n_iterations} calls)")
        print(f"  Per call: {per_call_us:.2f}μs")


def main():
    """Run all benchmarks."""
    print("="*70)
    print("MATCHER.PY PERFORMANCE BENCHMARK")
    print("="*70)
    print("\nThis benchmark measures:")
    print("  1. normalize_text() - Hot path (20k+ calls)")
    print("  2. build_attribute_index() - Index building (10k catalog)")
    print("  3. run_matching() - End-to-end (1k input)")
    print("  4. extract_product_attributes() - Attribute extraction")

    # Run benchmarks
    benchmark_normalize_text(10000)
    benchmark_extract_attributes()
    benchmark_build_attribute_index()
    benchmark_run_matching()

    print("\n" + "="*70)
    print("BENCHMARK COMPLETE")
    print("="*70)
    print("\nNext steps:")
    print("  1. Apply regex pre-compilation (expected 10-20x speedup)")
    print("  2. Replace iterrows() with vectorized ops (expected 5-10x speedup)")
    print("  3. Use sets for membership checks (expected 2-5x speedup)")
    print("  4. Re-run benchmark to measure improvements")


if __name__ == '__main__':
    main()
