from pathlib import Path

import pandas as pd
import pytest

from portus.caches.disk_cache import DiskCache, DiskCacheConfig


@pytest.fixture
def cache(tmp_path: Path) -> DiskCache:
    config = DiskCacheConfig(db_dir=tmp_path)
    return DiskCache(config)


def test_set_and_get(cache: DiskCache) -> None:
    sql_text = "SELECT * FROM dummy_table"
    source = "dummy_source"
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    cache.set_sql(sql_text, df, source_name=source)
    cached_df = cache.get_sql(sql_text, source_name=source)
    assert cached_df is not None
    pd.testing.assert_frame_equal(df, cached_df)


def test_set_and_get_empty(cache: DiskCache) -> None:
    sql_text = "SELECT * FROM dummy_table"
    source = "dummy_source"
    df = pd.DataFrame({"": [1, 2, 3]})

    cache.set_sql(sql_text, df, source_name=source)
    cached_df = cache.get_sql(sql_text, source_name=source)
    assert cached_df is not None
    pd.testing.assert_frame_equal(df, cached_df)


def test_set_and_get_empty_duplicates(cache: DiskCache) -> None:
    sql_text = "SELECT * FROM dummy_table"
    source = "dummy_source"
    df = pd.DataFrame.from_records([(1, 2, 3), (4, 5, 6)], columns=["", "", "a"])

    cache.set_sql(sql_text, df, source_name=source)
    cached_df = cache.get_sql(sql_text, source_name=source)
    assert cached_df is not None
    pd.testing.assert_frame_equal(df, cached_df)


def test_get_with_no_match(cache: DiskCache) -> None:
    sql_text = "SELECT * FROM nonexistent_table"
    source = "nonexistent_source"
    cached_df = cache.get_sql(sql_text, source_name=source)
    assert cached_df is None


def test_invalidate_source_if_stale(cache: DiskCache) -> None:
    is_stale_query = "SELECT * FROM validation_table"
    source = "stale_source"
    cached_df = pd.DataFrame({"col1": [1, 2], "col2": ["x", "y"]})
    fresh_df = pd.DataFrame({"col1": [3, 4], "col2": ["z", "w"]})

    # Set the initial cached dataframe
    cache.set_sql(is_stale_query, cached_df, source_name=source)

    # Call invalidate_source_if_stale with a different dataframe
    cache.invalidate_source_if_stale_query(
        is_stale_query=is_stale_query, source_name=source, is_stale_query_df=fresh_df
    )

    # Verify the source has been invalidated and updated with the fresh dataframe
    updated_df = cache.get_sql(is_stale_query, source_name=source)
    assert updated_df is not None
    pd.testing.assert_frame_equal(fresh_df, updated_df)

    # Call invalidate_source_if_stale with the same dataframe
    cache.invalidate_source_if_stale_query(
        is_stale_query=is_stale_query, source_name=source, is_stale_query_df=fresh_df
    )

    # Verify that the source remains with the same data
    final_df = cache.get_sql(is_stale_query, source_name=source)
    assert final_df is not None
    pd.testing.assert_frame_equal(fresh_df, final_df)


def test_invalidate_tag_no_match(cache: DiskCache) -> None:
    source = "nonexistent_source"
    invalidated_rows = cache.invalidate_tag(source)
    assert invalidated_rows == 0
