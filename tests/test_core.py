# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import json

import pytest

from intake_virtual_icechunk.cat import VirtualIcechunkCatalogModel
from intake_virtual_icechunk.core import IcechunkCatalog
from intake_virtual_icechunk.source._containers import VirtualChunkContainerModel
from intake_virtual_icechunk.utils import _intake_cat_filename


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
    This class has *not* been human audited.
    """

    @pytest.mark.xfail(reason="Search functionality not yet implemented correctly")
    def test_search_scalar_match(self, icechunk_store_path, groups):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(source_id="BCC-ESM1")
        assert sorted(result.keys()) == sorted(
            [g["key"] for g in groups if g["attrs"]["source_id"] == "BCC-ESM1"]
        )

    @pytest.mark.xfail(reason="Search functionality not yet implemented correctly")
    def test_search_multi_attr(self, icechunk_store_path):
        cat = IcechunkCatalog(store=icechunk_store_path)
        result = cat.search(source_id="BCC-ESM1", experiment_id="historical")
        assert result.keys() == ["CMIP.BCC.BCC-ESM1.historical"]

    @pytest.mark.xfail(reason="Search functionality not yet implemented correctly")
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
