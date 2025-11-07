from pathlib import Path

import pandas as pd
import pytest

from databao.caches.disk_cache import DiskCache, DiskCacheConfig


@pytest.fixture
def cache(tmp_path: Path) -> DiskCache:
    config = DiskCacheConfig(db_dir=tmp_path)
    return DiskCache(config)


def test_set_and_get(cache: DiskCache) -> None:
    sql_text = "SELECT * FROM dummy_table"
    source = "dummy_source"
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    key = DiskCache.make_json_key({"sql": sql_text, "source": source})
    cache.set_object(key, df, tag=source)
    cached_df = cache.get_object(key, default=None)
    assert cached_df is not None
    pd.testing.assert_frame_equal(df, cached_df)


def test_set_and_get_empty(cache: DiskCache) -> None:
    sql_text = "SELECT * FROM dummy_table"
    source = "dummy_source"
    df = pd.DataFrame({"": [1, 2, 3]})

    key = DiskCache.make_json_key({"sql": sql_text, "source": source})
    cache.set_object(key, df, tag=source)
    cached_df = cache.get_object(key, default=None)
    assert cached_df is not None
    pd.testing.assert_frame_equal(df, cached_df)


def test_set_and_get_empty_duplicates(cache: DiskCache) -> None:
    sql_text = "SELECT * FROM dummy_table"
    source = "dummy_source"
    df = pd.DataFrame.from_records([(1, 2, 3), (4, 5, 6)], columns=["", "", "a"])

    key = DiskCache.make_json_key({"sql": sql_text, "source": source})
    cache.set_object(key, df, tag=source)
    cached_df = cache.get_object(key, default=None)
    assert cached_df is not None
    pd.testing.assert_frame_equal(df, cached_df)


def test_get_with_no_match(cache: DiskCache) -> None:
    sql_text = "SELECT * FROM nonexistent_table"
    source = "nonexistent_source"
    key = DiskCache.make_json_key({"sql": sql_text, "source": source})
    cached_df = cache.get_object(key, default=None)
    assert cached_df is None


def test_invalidate_tag_no_match(cache: DiskCache) -> None:
    source = "nonexistent_source"
    invalidated_rows = cache.invalidate_tag(source)
    assert invalidated_rows == 0
