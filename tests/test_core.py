# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import json

import pytest

from intake_virtual_icechunk.cat import VirtualIcechunkCatalogModel
from intake_virtual_icechunk.core import IcechunkCatalog

# ---------------------------------------------------------------------------
# VirtualIcechunkCatalogModel — save / load round-trip
# ---------------------------------------------------------------------------


class TestVirtualIcechunkCatalogModel:
    def test_save_creates_json(self, tmp_path, icechunk_store_path):
        model = VirtualIcechunkCatalogModel(
            store=icechunk_store_path,
            description="My catalog",
            title="Test",
            storage_options={"key": "value"},
        )
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

    def test_load_round_trip(self, tmp_path, icechunk_store_path):
        model = VirtualIcechunkCatalogModel(
            store=icechunk_store_path,
            description="Round-trip test",
            storage_options={},
        )
        model.save("round-trip", directory=str(tmp_path))

        loaded = VirtualIcechunkCatalogModel.load(str(tmp_path / "round-trip.json"))

        assert loaded.store == icechunk_store_path
        assert loaded.description == "Round-trip test"
        assert loaded.id == "round-trip"

    def test_default_version(self, icechunk_store_path):
        model = VirtualIcechunkCatalogModel(store=icechunk_store_path)
        assert model.version == "1.0.0"


# ---------------------------------------------------------------------------
# IcechunkCatalog — from_json
# ---------------------------------------------------------------------------


class TestIcechunkCatalogFromJson:
    def test_from_json_returns_catalog(self, catalog_json_path):
        cat = IcechunkCatalog.from_json(catalog_json_path)
        assert isinstance(cat, IcechunkCatalog)

    def test_from_json_store_matches(self, catalog_json_path, icechunk_store_path):
        cat = IcechunkCatalog.from_json(catalog_json_path)
        assert cat.store == icechunk_store_path

    def test_save_round_trip(self, tmp_path, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        cat.save("saved-cat", directory=str(tmp_path))
        loaded = IcechunkCatalog.from_json(str(tmp_path / "saved-cat.json"))
        assert loaded.store == icechunk_store_path


# ---------------------------------------------------------------------------
# IcechunkCatalog — keys()
# ---------------------------------------------------------------------------


class TestIcechunkCatalogKeys:
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


# ---------------------------------------------------------------------------
# IcechunkCatalog — search()
# ---------------------------------------------------------------------------


class TestIcechunkCatalogSearch:
    def test_search_scalar_match(self, icechunk_store_path, groups):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(source_id="BCC-ESM1")
        assert sorted(result.keys()) == sorted(
            [g["key"] for g in groups if g["attrs"]["source_id"] == "BCC-ESM1"]
        )

    def test_search_multi_attr(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(source_id="BCC-ESM1", experiment_id="historical")
        assert result.keys() == ["CMIP.BCC.BCC-ESM1.historical"]

    def test_search_list_value(self, icechunk_store_path, groups):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(experiment_id=["historical", "ssp585"])
        historical_ssp = [
            g["key"]
            for g in groups
            if g["attrs"]["experiment_id"] in ("historical", "ssp585")
        ]
        assert sorted(result.keys()) == sorted(historical_ssp)

    def test_search_no_match(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(source_id="NONEXISTENT")
        assert result.keys() == []

    def test_search_empty_query_returns_self(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search()
        assert result is cat

    def test_search_result_shares_store(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        _ = cat._zarr_store  # open the store
        result = cat.search(source_id="BCC-ESM1")
        # The filtered catalog shares the open store objects
        assert result._open_zarr_store is cat._open_zarr_store


# ---------------------------------------------------------------------------
# IcechunkCatalog — df property
# ---------------------------------------------------------------------------


class TestIcechunkCatalogDf:
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
