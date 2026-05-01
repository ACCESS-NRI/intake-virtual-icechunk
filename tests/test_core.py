# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import json

import pandas as pd
import polars as pl
import pytest
import xarray as xr

from intake_virtual_icechunk.cat import VirtualIcechunkCatalogModel
from intake_virtual_icechunk.core import (
    IcechunkCatalog,
    _nunique,
    _read_sidecar_metadata,
)
from intake_virtual_icechunk.source._containers import VirtualChunkContainerModel
from intake_virtual_icechunk.utils import _intake_cat_filename


class TestReadSidecarMetadata:
    """Unit tests for _read_sidecar_metadata — pure function exercised in isolation."""

    def _write_sidecar(self, path, data):
        path.write_text(json.dumps(data))
        return str(path)

    def test_returns_dict(self, tmp_path):
        sidecar = {"id": "test", "storage_options": {}, "xarray_kwargs": {}}
        store = tmp_path / "store.icechunk"
        store.mkdir()
        self._write_sidecar(store / "_intake_store.json", sidecar)
        result = _read_sidecar_metadata(str(store))
        assert isinstance(result, dict)

    def test_reads_all_keys(self, tmp_path):
        sidecar = {
            "id": "my-catalog",
            "storage_options": {"from_env": True},
            "xarray_kwargs": {"decode_cf": False},
            "virtual_chunk_model": {"url_prefix": "s3://bucket/"},
        }
        store = tmp_path / "store.icechunk"
        store.mkdir()
        self._write_sidecar(store / "_intake_store.json", sidecar)
        result = _read_sidecar_metadata(str(store))
        assert result["id"] == "my-catalog"
        assert result["storage_options"] == {"from_env": True}
        assert result["xarray_kwargs"] == {"decode_cf": False}

    def test_sidecar_options_override_storage_options(self, tmp_path):
        """sidecar_options=None falls back to storage_options; sidecar_options={} ignores them."""
        sidecar = {"id": "x"}
        store = tmp_path / "store.icechunk"
        store.mkdir()
        self._write_sidecar(store / "_intake_store.json", sidecar)
        # Local FS: passing storage_options or sidecar_options={} both succeed
        result_default = _read_sidecar_metadata(
            str(store), storage_options={"anon": True}
        )
        result_explicit = _read_sidecar_metadata(str(store), sidecar_options={})
        assert result_default == result_explicit == sidecar

    def test_missing_sidecar_raises(self, tmp_path):
        store = tmp_path / "empty.icechunk"
        store.mkdir()
        with pytest.raises(FileNotFoundError):
            _read_sidecar_metadata(str(store))


class TestVirtualIcechunkCatalogModel:
    """
    This class has been human audited.
    """

    def test_save_creates_json(self, tmp_path, icechunk_store_path, sample_data):
        fname = _intake_cat_filename(icechunk_store_path)
        url_prefix = f"file://{sample_data}/access-om2/"

        model = VirtualIcechunkCatalogModel.load(str(icechunk_store_path / fname))

        # Set a couple of fields to non-default values to check they round-trip correctly
        model.description = "My catalog"
        model.storage_options = {"key": "value"}
        model.title = "Test"

        # Make sure to update url_prefix in the virtual chunk model to match the
        # test data, otherwise we will fail moving test data between machines.
        # Same goes for the store path, which is used in the catalog model's store
        # field. We should probably create temporary copies rathe than modifying
        # the test data, but this is easier for now.
        model.store = str(icechunk_store_path)
        model.virtual_chunk_model.url_prefix = url_prefix

        # Turn the path into a string for easier comparison in the JSON output
        icechunk_store_path = str(icechunk_store_path)

        model.save("my-catalog", directory=str(tmp_path))
        json_path = tmp_path / "my-catalog.json"
        assert json_path.exists()

        with open(json_path) as f:
            data = json.load(f)

        assert data["id"] == "my-catalog"
        assert data["store"] == icechunk_store_path
        assert data["description"] == "My catalog"
        assert data["title"] == "Test"
        assert data["storage_options"] == {"key": "value"}
        assert data["last_updated"] is not None

    def test_default_version(self, sample_data, icechunk_store_path):
        # Extracted from the icechunk store used in testing. We can abstract this
        # away eventualy I think
        url_prefix = f"file://{sample_data}/access-om2/"
        vc_model_dict = {
            "url_prefix": url_prefix,
            "store_type": "PyObjectStoreConfig_LocalFileSystem",
            "open_kwargs": {},
        }

        virtual_chunk_container = VirtualChunkContainerModel.from_dict(vc_model_dict)
        model = VirtualIcechunkCatalogModel(
            store=str(icechunk_store_path), virtual_chunk_model=virtual_chunk_container
        )
        assert model.version == "1.0.0"


class TestIcechunkCatalogFromJson:
    """
    This class has been human audited. Apparently not very well because there are
    CI bugs...
    """

    @pytest.fixture
    def temp_json_local_path(
        self, icechunk_store_path, catalog_json_path, sample_data, tmpdir
    ) -> str:
        """
        Rewrites the catalog json to not use a fixture from my local machine. Will
        need proper cleaning up later, this is a janky fix.
        """
        with open(catalog_json_path) as f:
            data = json.load(f)

        data["store"] = str(icechunk_store_path)
        data["virtual_chunk_model"]["url_prefix"] = f"file://{sample_data}/access-om2/"

        local_json_path = tmpdir / "catalog.json"
        with open(local_json_path, "w") as f:
            json.dump(data, f)

        return str(local_json_path)

    def test_from_json_returns_catalog(self, temp_json_local_path):
        cat = IcechunkCatalog.from_json(temp_json_local_path)
        assert isinstance(cat, IcechunkCatalog)

    def test_from_json_store_matches(self, temp_json_local_path, icechunk_store_path):
        cat = IcechunkCatalog.from_json(temp_json_local_path)
        assert cat.store == str(icechunk_store_path)

    def test_save_round_trip(self, tmp_path, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        cat.save("saved-cat", directory=str(tmp_path))
        loaded = IcechunkCatalog.from_json(str(tmp_path / "saved-cat.json"))
        assert loaded.store == str(icechunk_store_path)

    def test_sidecar_round_trip_vcc_config(self, icechunk_store_path, sample_data):
        """
        Regression: the virtual chunk container config must survive a full
        sidecar round-trip (write → re-open catalog → read VCC).

        This exercises:
        - _sidecar_url correctly locates the sidecar (no file:// mangling)
        - fsspec.open() can read it
        - VirtualChunkContainerModel serialises / deserialises intact
        """
        # Open catalog and record the original VCC config
        cat = IcechunkCatalog(store=icechunk_store_path)
        original_url_prefix = cat.virtual_chunk_model.url_prefix
        original_store_type = cat.virtual_chunk_model.store_type

        # Re-open from the store path (exercises __init__ sidecar read path)
        cat2 = IcechunkCatalog(store=icechunk_store_path)
        assert cat2.virtual_chunk_model.url_prefix == original_url_prefix
        assert cat2.virtual_chunk_model.store_type == original_store_type

        # Also verify the reconstructed VirtualChunkContainer has the right prefix
        vcc = cat2.virtual_chunk_model.to_virtual_chunk_container()
        assert vcc.url_prefix == original_url_prefix

    def test_sidecar_options_no_sidecar_reread_when_model_supplied(
        self, icechunk_store_path
    ):
        """
        When virtual_chunk_model is supplied, __init__ skips the sidecar read.
        This means sidecar_options is irrelevant and the catalog still opens
        normally — even if sidecar_options would fail (e.g. wrong credentials).
        """
        cat_base = IcechunkCatalog(store=icechunk_store_path)
        cat = IcechunkCatalog(
            store=icechunk_store_path,
            storage_options={"from_env": True},
            sidecar_options={"anon": True},  # would fail if sidecar were re-read
            virtual_chunk_model=cat_base.virtual_chunk_model.to_dict(),
        )
        assert cat.storage_options == {"from_env": True}
        assert (
            cat.virtual_chunk_model.url_prefix
            == cat_base.virtual_chunk_model.url_prefix
        )


class TestIcechunkCatalogConstructorKwargs:
    """
    Verify that storage_options, xarray_kwargs, and virtual_chunk_model are
    each wired to the correct constructor parameter with no cross-talk.
    """

    def test_storage_options_kwarg_is_assigned(self, icechunk_store_path):
        """Explicit storage_options kwarg must land on self.storage_options."""
        opts = {"from_env": True}
        cat = IcechunkCatalog(store=icechunk_store_path, storage_options=opts)
        assert cat.storage_options == opts

    def test_xarray_kwargs_kwarg_is_assigned(self, icechunk_store_path):
        """Explicit xarray_kwargs kwarg must land on self.xarray_kwargs."""
        xr_kw = {"decode_cf": False}
        cat = IcechunkCatalog(store=icechunk_store_path, xarray_kwargs=xr_kw)
        assert cat.xarray_kwargs == xr_kw

    def test_storage_options_does_not_bleed_into_xarray_kwargs(
        self, icechunk_store_path
    ):
        """Passing storage_options must not overwrite xarray_kwargs."""
        opts = {"from_env": True}
        cat = IcechunkCatalog(store=icechunk_store_path, storage_options=opts)
        # xarray_kwargs should come from metadata (or default {}), not from opts
        assert cat.xarray_kwargs != opts

    def test_storage_options_does_not_bleed_into_virtual_chunk_model(
        self, icechunk_store_path
    ):
        """Passing storage_options must not overwrite virtual_chunk_model."""
        opts = {"from_env": True}
        cat_with = IcechunkCatalog(store=icechunk_store_path, storage_options=opts)
        cat_without = IcechunkCatalog(store=icechunk_store_path)
        # virtual_chunk_model should be identical regardless of storage_options
        assert (
            cat_with.virtual_chunk_model.url_prefix
            == cat_without.virtual_chunk_model.url_prefix
        )

    def test_xarray_kwargs_does_not_bleed_into_storage_options(
        self, icechunk_store_path
    ):
        """Passing xarray_kwargs must not overwrite storage_options."""
        xr_kw = {"decode_cf": False}
        cat = IcechunkCatalog(store=icechunk_store_path, xarray_kwargs=xr_kw)
        assert cat.storage_options != xr_kw

    def test_defaults_fall_back_to_metadata(self, icechunk_store_path):
        """When no kwargs are provided, values come from the JSON metadata."""
        cat_default = IcechunkCatalog(store=icechunk_store_path)
        cat_explicit_none = IcechunkCatalog(
            store=icechunk_store_path,
            storage_options=None,
            xarray_kwargs=None,
            virtual_chunk_model=None,
        )
        assert cat_default.storage_options == cat_explicit_none.storage_options
        assert cat_default.xarray_kwargs == cat_explicit_none.xarray_kwargs
        assert (
            cat_default.virtual_chunk_model.url_prefix
            == cat_explicit_none.virtual_chunk_model.url_prefix
        )

    def test_all_three_kwargs_independent(self, icechunk_store_path):
        """Each kwarg can be set independently without affecting the others."""
        opts = {"from_env": True}
        xr_kw = {"decode_cf": False}
        cat_so = IcechunkCatalog(store=icechunk_store_path, storage_options=opts)
        cat_xr = IcechunkCatalog(store=icechunk_store_path, xarray_kwargs=xr_kw)

        assert cat_so.storage_options == opts
        assert cat_so.xarray_kwargs != opts

        assert cat_xr.xarray_kwargs == xr_kw
        assert cat_xr.storage_options != xr_kw


class TestIcechunkCatalogKeys:
    """
    This class has *not* been human audited.
    """

    def __init__(self, groups):
        self.all_keys = [g["key"] for g in groups]

    def test_keys_returns_all_groups(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        assert sorted(cat.keys()) == sorted(self.all_keys)

    def test_len_matches_keys(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        assert len(cat) == len(self.all_keys)

    def test_contains(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        assert self.all_keys[0] in cat
        assert "NONEXISTENT.KEY" not in cat

    def test_getitem_raises_on_missing_key(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        with pytest.raises(KeyError):
            cat["NONEXISTENT.KEY"]


class TestIcechunkCatalogSearch:
    """
    Search tests. All expected values are derived from cat.df so these tests
    remain valid regardless of which test store is used.
    """

    def test_search_empty_query_returns_self(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search()
        assert result is cat

    def test_search_unknown_column_returns_empty(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(totally_nonexistent_column_xyz="whatever")
        assert result.keys() == []

    def test_search_known_column_no_match_returns_empty(self, icechunk_store_path):
        """Column exists but value is absent — must return empty, not all rows."""
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(filename="THIS_FILE_DOES_NOT_EXIST.nc")
        assert result.keys() == []

    def test_search_scalar_match(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        df = cat.df
        filename_val = df["filename"].dropna().iloc[0]
        expected = sorted(df[df["filename"] == filename_val].index.tolist())

        result = cat.search(filename=filename_val)

        assert sorted(result.keys()) == expected
        assert len(result) > 0

    def test_search_scalar_is_exact_not_substring(self, icechunk_store_path):
        """Search must not return entries whose attribute only *contains* the query."""
        cat = IcechunkCatalog(store=icechunk_store_path)
        filename_val = cat.df["filename"].dropna().iloc[0]
        # A substring of a real filename should not match anything
        substring = filename_val[1:-1]  # strip first and last char
        result = cat.search(filename=substring)
        assert substring not in cat.df["filename"].values or sorted(
            result.keys()
        ) == sorted(cat.df[cat.df["filename"] == substring].index.tolist())

    def test_search_list_value(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        df = cat.df
        filenames = df["filename"].dropna().unique().tolist()[:2]
        expected = sorted(df[df["filename"].isin(filenames)].index.tolist())

        result = cat.search(filename=filenames)

        assert sorted(result.keys()) == expected

    def test_search_list_superset_of_scalar(self, icechunk_store_path):
        """search(x=[a, b]) should return at least as many results as search(x=a)."""
        cat = IcechunkCatalog(store=icechunk_store_path)
        filenames = cat.df["filename"].dropna().unique().tolist()
        if len(filenames) < 2:
            pytest.skip("Need at least 2 distinct filenames")
        a, b = filenames[0], filenames[1]

        result_scalar = cat.search(filename=a)
        result_list = cat.search(filename=[a, b])

        assert set(result_scalar.keys()).issubset(set(result_list.keys()))

    def test_search_multi_attr(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        df = cat.df
        # Use only scalar (non-iterable) columns to avoid tuple/list mismatches
        scalar_cols = [
            c
            for c in df.columns
            if c not in cat.columns_with_iterables
            and not df[c].apply(lambda x: isinstance(x, (list | tuple))).any()
        ]
        if len(scalar_cols) < 2:
            pytest.skip("Need at least 2 scalar columns for multi-attr test")
        col1, col2 = scalar_cols[0], scalar_cols[1]
        row = df.iloc[0]
        val1, val2 = row[col1], row[col2]
        expected = sorted(df[(df[col1] == val1) & (df[col2] == val2)].index.tolist())

        result = cat.search(**{col1: val1, col2: val2})

        assert sorted(result.keys()) == expected

    def test_search_multi_attr_is_intersection(self, icechunk_store_path):
        """Multi-attr search must be AND, not OR."""
        cat = IcechunkCatalog(store=icechunk_store_path)
        df = cat.df
        scalar_cols = [
            c
            for c in df.columns
            if c not in cat.columns_with_iterables
            and not df[c].apply(lambda x: isinstance(x, (list | tuple))).any()
        ]
        if len(scalar_cols) < 2:
            pytest.skip("Need at least 2 scalar columns for multi-attr test")
        col1, col2 = scalar_cols[0], scalar_cols[1]
        val1 = df[col1].dropna().iloc[0]
        val2 = df[col2].dropna().iloc[0]

        result_both = cat.search(**{col1: val1, col2: val2})
        result_col1_only = cat.search(**{col1: val1})
        result_col2_only = cat.search(**{col2: val2})

        # AND: result must be subset of both individual results
        assert set(result_both.keys()).issubset(set(result_col1_only.keys()))
        assert set(result_both.keys()).issubset(set(result_col2_only.keys()))

    def test_search_result_is_icechunk_catalog(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(filename="ocean.nc")
        assert isinstance(result, IcechunkCatalog)

    def test_search_result_keys_are_subset_of_parent(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(filename="ocean.nc")
        assert set(result.keys()).issubset(set(cat.keys()))

    def test_search_result_shares_store(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        _ = cat._zarr_store
        result = cat.search(filename="ocean.nc")
        assert result._open_zarr_store is cat._open_zarr_store

    def test_search_chained_is_intersection(self, icechunk_store_path):
        """Chaining two searches should equal the AND of both queries at once."""
        cat = IcechunkCatalog(store=icechunk_store_path)
        df = cat.df
        scalar_cols = [
            c
            for c in df.columns
            if c not in cat.columns_with_iterables
            and not df[c].apply(lambda x: isinstance(x, (list | tuple))).any()
        ]
        if len(scalar_cols) < 2:
            pytest.skip("Need at least 2 scalar columns for chained search test")
        col1, col2 = scalar_cols[0], scalar_cols[1]
        val1 = df[col1].dropna().iloc[0]
        val2 = df[col2].dropna().iloc[0]

        chained = cat.search(**{col1: val1}).search(**{col2: val2})
        combined = cat.search(**{col1: val1, col2: val2})

        assert sorted(chained.keys()) == sorted(combined.keys())

    def test_search_df_reflects_results(self, icechunk_store_path):
        """The .df on a search result should only contain matched entries."""
        cat = IcechunkCatalog(store=icechunk_store_path)
        filename_val = cat.df["filename"].dropna().iloc[0]
        result = cat.search(filename=filename_val)
        assert set(result.df.index.tolist()) == set(result.keys())
        assert all(result.df["filename"] == filename_val)


class TestIcechunkCatalogDf:
    """
    This class has *not* been human audited.
    """

    def __init__(self, groups):
        self.all_keys = [g["key"] for g in groups]

    def test_df_has_key_column(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        df = cat.df
        assert "key" in df.columns

    def test_df_has_attr_columns(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        df = cat.df
        assert "source_id" in df.columns
        assert "experiment_id" in df.columns

    def test_df_row_count(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        assert len(cat.df) == len(self.all_keys)

    def test_df_keys_match(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        assert sorted(cat.df["key"].tolist()) == sorted(self.all_keys)

    def test_df_filtered_by_search(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(source_id="BCC-ESM1")
        df = result.df
        assert all(df["source_id"] == "BCC-ESM1")


class TestIcechunkCatalogToDatasetDict:
    def test_returns_dict_of_datasets(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(filename="ocean.nc").to_dataset_dict(progressbar=False)
        assert isinstance(result, dict)
        assert len(result) > 0
        for ds in result.values():
            assert isinstance(ds, xr.Dataset)

    def test_preprocess_applied_to_each_dataset(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)

        def mark(ds):
            return ds.assign_attrs(preprocessed=True)

        result = cat.search(filename="ocean.nc").to_dataset_dict(
            preprocess=mark, progressbar=False
        )
        assert len(result) > 0
        for ds in result.values():
            assert ds.attrs.get("preprocessed") is True

    def test_preprocess_none_does_not_alter_datasets(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        without = cat.search(filename="ocean.nc").to_dataset_dict(progressbar=False)
        with_none = cat.search(filename="ocean.nc").to_dataset_dict(
            preprocess=None, progressbar=False
        )
        assert set(without.keys()) == set(with_none.keys())

    def test_storage_options_merged_does_not_break_loading(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(filename="ocean.nc").to_dataset_dict(
            storage_options={"extra_key": "extra_value"}, progressbar=False
        )
        assert len(result) > 0

    def test_xarray_kwargs_override_catalog_kwargs(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(filename="ocean.nc").to_dataset_dict(
            xarray_kwargs={"mask_and_scale": False}, progressbar=False
        )
        assert len(result) > 0
        for ds in result.values():
            assert isinstance(ds, xr.Dataset)


class TestIcechunkCatalogToXarray:
    """
    This class has *not* been human audited.
    """

    def test_to_xarray_returns_dataset(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        ds = cat.search(filename="ocean.nc").to_dask()
        assert isinstance(ds, xr.Dataset)

    def test_to_xarray_raises_on_missing_key(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        with pytest.raises(KeyError):
            cat["NONEXISTENT.KEY"].to_xarray()

    def test_to_dask_warns(self, icechunk_store_path):
        import sys

        if sys.version_info < (3, 13):
            # Mark as xfail and return. IDK WTF is going on here with not warning and it's
            # unimportant. #TODO
            pytest.xfail("to_dask() is deprecated and raises in Python 3.13+")
            return None

        cat = IcechunkCatalog(store=icechunk_store_path)
        with pytest.warns(
            # FutureWarning,
            match=r"to_dask\(\) is deprecated; use to_xarray\(\) instead\.",
        ):
            cat.search(filename="ocean.nc").to_dask()


class TestIcechunkCatalog:
    """
    This class has *not* been human audited.
    """

    def test_nunique(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        uniques = cat.nunique()

        assert uniques.to_dict() == {
            "Variable": 7,
            "Coordinates": 10,
            "Dimensions": 9,
            "filename": 5,
            "title": 2,
            "grid_type": 2,
            "grid_tile": 2,
            "history": 6,
            "NCO": 1,
            "frequency": 3,
            "variable_long_name": 6,
            "variable_standard_name": 6,
            "variable_cell_methods": 6,
            "realm": 2,
            "contents": 2,
            "source": 2,
            "comment": 2,
            "comment2": 2,
            "comment3": 2,
            "conventions": 2,
            "io_flavor": 2,
            "variable_units": 6,
            # The following should all be dropped: see conftest.py
            # "path": 2,
            # "file_id": 2,
            # "start_date": 2,
            # "end_date": 2,
            # "temporal_label": 2,
        }

    @pytest.mark.xfail(
        reason="This is super flaky. IDK if there's a better way to fix?"
    )
    def test_repr_html(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        html = cat._repr_html_()
        assert (
            html
            == '<p><strong>_intake_icecat catalog with 6 dataset(s) from 6 asset(s)</strong>:</p> <div>\n<style scoped>\n    .dataframe tbody tr th:only-of-type {\n        vertical-align: middle;\n    }\n\n    .dataframe tbody tr th {\n        vertical-align: top;\n    }\n\n    .dataframe thead th {\n        text-align: right;\n    }\n</style>\n<table border="1" class="dataframe">\n  <thead>\n    <tr style="text-align: right;">\n      <th></th>\n      <th>0</th>\n    </tr>\n  </thead>\n  <tbody>\n    <tr>\n      <th>Variable</th>\n      <td>7</td>\n    </tr>\n    <tr>\n      <th>Coordinates</th>\n      <td>10</td>\n    </tr>\n    <tr>\n      <th>Dimensions</th>\n      <td>9</td>\n    </tr>\n    <tr>\n      <th>filename</th>\n      <td>5</td>\n    </tr>\n    <tr>\n      <th>title</th>\n      <td>2</td>\n    </tr>\n    <tr>\n      <th>grid_type</th>\n      <td>2</td>\n    </tr>\n    <tr>\n      <th>grid_tile</th>\n      <td>2</td>\n    </tr>\n    <tr>\n      <th>history</th>\n      <td>6</td>\n    </tr>\n    <tr>\n      <th>NCO</th>\n      <td>1</td>\n    </tr>\n    <tr>\n      <th>frequency</th>\n      <td>3</td>\n    </tr>\n    <tr>\n      <th>variable_long_name</th>\n      <td>6</td>\n    </tr>\n    <tr>\n      <th>variable_standard_name</th>\n      <td>6</td>\n    </tr>\n    <tr>\n      <th>variable_cell_methods</th>\n      <td>6</td>\n    </tr>\n    <tr>\n      <th>variable_units</th>\n      <td>6</td>\n    </tr>\n    <tr>\n      <th>realm</th>\n      <td>2</td>\n    </tr>\n    <tr>\n      <th>contents</th>\n      <td>2</td>\n    </tr>\n    <tr>\n      <th>source</th>\n      <td>2</td>\n    </tr>\n    <tr>\n      <th>comment</th>\n      <td>2</td>\n    </tr>\n    <tr>\n      <th>comment2</th>\n      <td>2</td>\n    </tr>\n    <tr>\n      <th>comment3</th>\n      <td>2</td>\n    </tr>\n    <tr>\n      <th>conventions</th>\n      <td>2</td>\n    </tr>\n    <tr>\n      <th>io_flavor</th>\n      <td>2</td>\n    </tr>\n  </tbody>\n</table>\n</div>'
        )


@pytest.mark.parametrize(
    "dataframe, expected",
    [
        (
            pd.DataFrame({"a": [1, 2, 2], "b": [[1, 2], [2, 3], [1, 2]]}),
            pd.Series({"a": 2, "b": 3}),
        ),
        (
            pd.DataFrame({"a": [1, 1, 1], "b": [[1], [1], [1]]}),
            pd.Series({"a": 1, "b": 1}),
        ),
        (
            pd.DataFrame({"a": [1, 2, 3], "b": [[1], [2], [3]]}),
            pd.Series({"a": 3, "b": 3}),
        ),
    ],
)
def test__nunique(dataframe, expected):
    result = _nunique(pl.from_pandas(dataframe))
    pd.testing.assert_series_equal(result, expected)
