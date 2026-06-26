import os
import uuid
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import icechunk
import intake
import numpy as np
import pandas as pd
import pytest
import tlz
import virtualizarr
import xarray as xr
import zarr
from access_nri_intake.source.builders import AccessOm2Builder
from dotenv import load_dotenv
from icechunk.xarray import to_icechunk
from intake_esm.core import esm_datastore
from obstore.store import ObjectStore, from_url
from pandas.testing import assert_frame_equal

from intake_virtual_icechunk.source import (
    AbstractIcechunkStoreBuilder,
    IcechunkStoreBuilder,
    VirtualIcechunkStoreBuilder,
)
from intake_virtual_icechunk.source._build import (
    _inner_chunk_shape,
    _normalize_size_arg,
    _rechunk_dataset,
    _RechunkSpec,
    _resolve_shard_shape,
)
from intake_virtual_icechunk.source.utils import GroupEntry, GroupEntryError
from intake_virtual_icechunk.utils import (
    _intake_cat_filename,
    _representative_source_size,
)

__all__ = ["VirtualIcechunkStoreBuilder", "pytest"]


@pytest.fixture(scope="session")
def local_om2_datastore_path(sample_data, tmp_path_factory) -> Path:
    data_root = sample_data / "access-om2"
    tmp_root = tmp_path_factory.mktemp("access-om2")
    catalog_dir = tmp_root / "esmcat"

    catalog_dir.mkdir(parents=True, exist_ok=True)

    builder = AccessOm2Builder(str(data_root))
    builder.build()
    builder.save(
        name="access-om2",
        description="Test catalog for ACCESS-OM2",
        directory=str(catalog_dir),
    )

    catalog_path = catalog_dir / "access-om2.json"

    return catalog_path


class BuilderTests:
    """
    Tests for the IcechunkStoreBuilder class, which is responsible for building
    an IcechunkStore from a given intake-esm datastore.
    """

    def test_init_infer_parser(self, *args, **kwargs):
        """
        Initialisation without a parser should trigger parser inference, which
        in turn should open the esm datastore
        """
        raise NotImplementedError("Base test, to be implemented by child classes")

    def test_init_with_parser(self, *args, **kwargs):
        """
        Initialisation with a parser should use the provided parser not instantiate
        the esm datastore until it's asked for
        """
        raise NotImplementedError("Base test, to be implemented by child classes")

    @pytest.mark.parametrize(
        "format_val, parser",
        [
            ("netcdf", virtualizarr.parsers.HDFParser),
            ("zarr", virtualizarr.parsers.ZarrParser),
            ("zarr2", virtualizarr.parsers.ZarrParser),
            ("zarr3", virtualizarr.parsers.ZarrParser),
            ("reference", virtualizarr.parsers.KerchunkJSONParser),
        ],
    )
    def test_infer_parser(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir, format_val, parser
    ):
        """
        Mostly a regression test for now.
        """
        raise NotImplementedError("Base test, to be implemented by child classes")

    def test_clean_build(self, *args, **kwargs):
        """
        Test that the build method creates an IcechunkStore with the expected
        store type and storage options.
        """
        raise NotImplementedError("Base test, to be implemented by child classes")

    def test_build_all_failures(self, *args, **kwargs):
        """
        Test that the build method creates an IcechunkStore with the expected
        store type and storage options. To ensure we have some failures, we're
        going to change the parser
        """
        raise NotImplementedError("Base test, to be implemented by child classes")

    def test_build_not_concat_dim_issue(self, *args, **kwargs):
        """
        Test that the build method creates an IcechunkStore with the expected
        store type and storage options. To ensure we have some failures, we're
        going to change the parser to one that doesn't support concatenation along a dimension.
        This should trigger a specific failure mode that we want to check is handled correctly.
        """
        raise NotImplementedError("Base test, to be implemented by child classes")

    def test_build_concat_dim_issue(self, *args, **kwargs):
        """
        Test that the build method creates an IcechunkStore with the expected
        store type and storage options. To ensure we have some failures, we're
        going to change the parser to one that doesn't support concatenation along a dimension.
        This should trigger a specific failure mode that we want to check is handled correctly.
        """
        raise NotImplementedError("Base test, to be implemented by child classes")

    def test_build_deiters_cols_existing(self, *args, **kwargs):
        """
        Test that the build method correctly de-iterates columns specified in the cols_to_deiter argument.
        This is a regression test for a specific issue we had where if the column to de-iterate had some null values, the de-iteration would fail.
        """
        raise NotImplementedError("Base test, to be implemented by child classes")

    def test_repr_defaults(self, *args, **kwargs):
        """
        __repr__ should include all key fields with their default values when no
        optional arguments are provided.
        """
        raise NotImplementedError("Base test, to be implemented by child classes")

    def test_repr_with_custom_args(self, *args, **kwargs):
        """
        __repr__ should reflect non-default values for all optional arguments.
        """
        raise NotImplementedError("Base test, to be implemented by child classes")

    def test_repr_parser_name_matches_instance(self, *args, **kwargs):
        """
        The parser name in __repr__ should match the class name of the instantiated parser.
        """
        raise NotImplementedError("Base test, to be implemented by child classes")

    def test_build_deiters_cols_exceptionlogic(self, *args, **kwargs):
        """
        Test that the build method correctly de-iterates columns specified in the cols_to_deiter argument.
        This is a regression test for a specific issue we had where if the column to de-iterate had some null values, the de-iteration would fail.
        """
        raise NotImplementedError("Base test, to be implemented by child classes")


class TestVirtualIcechunkStoreBuilder(BuilderTests):
    """
    Tests for VirtualIcechunkStoreBuilder (the virtual-reference builder).
    """

    @pytest.fixture
    def intake_esm_kwargs(self) -> dict[str, list[str]]:
        return {
            "columns_with_iterables": [
                "variable",
                "variable_long_name",
                "variable_standard_name",
                "variable_cell_methods",
                "variable_units",
            ]
        }

    @pytest.fixture
    def om2_datastore(
        self, local_om2_datastore_path, intake_esm_kwargs
    ) -> esm_datastore:
        """
        Fixture that provides an intake-esm datastore for the OM2 model.
        """

        return intake.open_esm_datastore(
            str(local_om2_datastore_path),
            **intake_esm_kwargs,
        )

    def test_init_infer_parser(
        self, local_om2_datastore_path, om2_datastore, intake_esm_kwargs, tmpdir
    ):
        """
        Initialisation without a parser should trigger parser inference, which
        in turn should open the esm datastore
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
        )

        assert isinstance(builder.parser, virtualizarr.parsers.hdf.hdf.HDFParser)
        assert_frame_equal(builder.esm_ds.df, om2_datastore.df)

    def test_init_with_parser(
        self, local_om2_datastore_path, om2_datastore, intake_esm_kwargs, tmpdir
    ):
        """
        Initialisation with a parser should use the provided parser not instantiate
        the esm åtastore until it's asked for
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        parser = virtualizarr.parsers.hdf.hdf.HDFParser
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            icechunk_store_path=dummy_store_path,
            parser=parser,
        )

        assert builder._esm_ds is None
        assert isinstance(builder.parser, virtualizarr.parsers.hdf.hdf.HDFParser)

        assert_frame_equal(builder.esm_ds.df, om2_datastore.df)

    @pytest.mark.parametrize(
        "format_val, parser",
        [
            ("netcdf", virtualizarr.parsers.HDFParser),
            ("zarr", virtualizarr.parsers.ZarrParser),
            ("zarr2", virtualizarr.parsers.ZarrParser),
            ("zarr3", virtualizarr.parsers.ZarrParser),
            ("reference", virtualizarr.parsers.KerchunkJSONParser),
        ],
    )
    def test_infer_parser(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir, format_val, parser
    ):
        """
        Mostly a regression test for now.
        """

        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            icechunk_store_path=dummy_store_path,
        )
        from intake_esm.cat import DataFormat

        builder.esm_ds.esmcat.assets.format = DataFormat(format_val)

        inferred_parser = builder._infer_parser()

        assert inferred_parser == parser

    def test_group_entry_from_esm_group(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """GroupEntry.from_esm_group should encapsulate ESM-specific derivation."""
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            icechunk_store_path=dummy_store_path,
        )

        structure = builder._prepare_group_iteration()
        esmcat = builder.esm_ds.esmcat
        public_key, internal_key = next(iter(esmcat._construct_group_keys().items()))
        group_df = esmcat.grouped.get_group(internal_key)

        entry = GroupEntry.from_esm_group(
            public_key=public_key,
            group_df=group_df,
            groupby_attrs=structure.groupby_attrs,
            assets_col=structure.assets_col,
        )

        assert entry.public_key == public_key
        assert entry.has_metadata_df is True
        assert entry.group_df is group_df
        assert entry.file_paths == group_df[structure.assets_col].tolist()
        assert set(entry.group_attrs).issubset(set(group_df.columns))

    def test_group_entry_requirements(self):
        """Entry helpers should fail clearly when a path lacks payloads."""
        missing_paths = GroupEntry(
            public_key="foo", group_attrs={}, metadata_df=pd.DataFrame()
        )
        missing_metadata = GroupEntry(
            public_key="bar", group_attrs={}, source_file_paths=["a"]
        )

        assert missing_paths.has_metadata_df is True
        assert missing_metadata.has_metadata_df is False

        with pytest.raises(GroupEntryError, match="does not include source file paths"):
            _ = missing_paths.file_paths

        with pytest.raises(
            GroupEntryError, match="does not include a metadata dataframe"
        ):
            _ = missing_metadata.group_df

    def test_attach_entry_metadata_without_group_df_uses_group_attrs(self, tmpdir):
        """Reduced-metadata entries should still attach searchable group attrs."""
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = IcechunkStoreBuilder(
            esm_datastore_path="dummy.json",
            icechunk_store_path=dummy_store_path,
            drop_cols=["path"],
        )
        entry = GroupEntry(
            public_key="foo",
            group_attrs={
                "source_id": "demo",
                "experiment_id": "hist",
                "path": "drop-me",
            },
            source_file_paths=["a"],
        )

        zarr_group = MagicMock()
        zarr_group.attrs = {}

        builder._attach_entry_metadata(zarr_group, entry)

        assert zarr_group.attrs == {
            "source_id": "demo",
            "experiment_id": "hist",
        }

    def test_attach_catalog_metadata_keeps_falsy_values(self, tmpdir):
        """Legitimate falsy metadata (0, 0.0, False) must survive deduplication.

        Regression test: the dedup step previously filtered with ``if val``,
        which silently dropped 0/0.0/False alongside nulls. Empty strings remain
        treated as an 'absent' marker (collapse to None when deiterated).
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = IcechunkStoreBuilder(
            esm_datastore_path="dummy.json",
            icechunk_store_path=dummy_store_path,
            drop_cols=["path"],
            cols_to_deiter=["level", "flag", "ratio", "missing"],
        )
        group_df = pd.DataFrame(
            {
                "level": [0, 0],
                "flag": [False, False],
                "ratio": [0.0, 0.0],
                "missing": ["", ""],
                "path": ["a", "b"],
            }
        )

        zarr_group = MagicMock()
        zarr_group.attrs = {}

        builder._attach_catalog_metadata(zarr_group, group_df, group_attrs={})

        assert zarr_group.attrs["level"] == 0
        assert zarr_group.attrs["flag"] is False
        assert zarr_group.attrs["ratio"] == 0.0
        # Empty strings stay an 'absent' marker, collapsing to None.
        assert zarr_group.attrs["missing"] is None

    def test_iter_esm_groups(self, local_om2_datastore_path, intake_esm_kwargs, tmpdir):
        """The shared ESM iterator should yield one structured entry per catalog key."""
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            icechunk_store_path=dummy_store_path,
        )

        entries = list(builder._iter_esm_groups())

        assert entries
        assert len(entries) == len(builder.esm_ds.keys())
        assert all(isinstance(entry, GroupEntry) for entry in entries)
        assert {entry.public_key for entry in entries} == set(builder.esm_ds.keys())
        assert builder.esm_ds.esmcat.assets.column_name in builder.drop_cols
        assert all(not entry.group_df.empty for entry in entries)
        assert all(entry.file_paths for entry in entries)
        assert all(
            set(entry.group_attrs).issubset(set(entry.group_df.columns))
            for entry in entries
        )

    def test_build_from_entries_tracks_failures_and_continues(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """The shared entry-writing seam should keep iterating after one entry fails."""
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            icechunk_store_path=dummy_store_path,
        )

        entries = [
            GroupEntry(public_key="one", group_attrs={}, source_file_paths=["a"]),
            GroupEntry(public_key="two", group_attrs={}, source_file_paths=["b"]),
            GroupEntry(public_key="three", group_attrs={}, source_file_paths=["c"]),
        ]

        class FakeTransaction:
            def __enter__(self):
                return object()

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeRepo:
            def __init__(self):
                self.calls = []

            def transaction(self, branch, *, message):
                self.calls.append((branch, message))
                return FakeTransaction()

        repo = FakeRepo()

        with patch.object(
            builder,
            "_write_entry",
            side_effect=[None, RuntimeError("boom"), None],
        ) as write_entry:
            builder._build_from_entries(repo, entries, message="demo build")

        assert write_entry.call_count == len(entries)
        assert repo.calls == [("main", "demo build")]
        assert len(builder.failed_list) == 1
        assert builder.failed_list[0][0] == "two"
        with pytest.raises(RuntimeError, match="boom"):
            raise builder.failed_list[0][1]

    def test_clean_build(self, local_om2_datastore_path, intake_esm_kwargs, tmpdir):
        """
        Test that the build method creates an IcechunkStore with the expected
        store type and storage options.
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            icechunk_store_path=dummy_store_path,
        )

        builder.build()

        assert Path(builder.store_path).exists()
        assert Path(builder.store_path).is_dir()

        fname = _intake_cat_filename(builder.store_path)

        assert builder.failed_list == []
        assert (Path(builder.store_path) / fname).exists()

    def test_build_all_failures(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """
        Test that the build method creates an IcechunkStore with the expected
        store type and storage options. To ensure we have some failures, we're
        going to change the parser
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            parser=virtualizarr.parsers.ZarrParser,
        )
        with pytest.raises(
            icechunk.IcechunkError,
            match="cannot commit, no changes made to the session",
        ):
            builder.build()

        # If the build failed, we should have a list of all the datasets that failed and why
        assert len(builder.failed_list) == len(builder.esm_ds.keys())
        assert set(fl[0] for fl in builder.failed_list) == set(builder.esm_ds.keys())

    def test_build_not_concat_dim_issue(
        self,
        local_om2_datastore_path,
        intake_esm_kwargs,
        tmpdir,
    ):
        """
        Test that the build method creates an IcechunkStore with the expected
        store type and storage options. To ensure we have some failures, we're
        going to change the parser to one that doesn't support concatenation along a dimension.
        This should trigger a specific failure mode that we want to check is handled correctly.
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
        )

        with pytest.raises(
            icechunk.IcechunkError,
            match="cannot commit, no changes made to the session",
        ):
            with patch(
                "intake_virtual_icechunk.source._build.open_virtual_mfdataset",
                side_effect=RuntimeError("Something stupid"),
            ):
                builder.build()

        assert len(builder.failed_list) == len(builder.esm_ds.keys())
        assert set(fl[0] for fl in builder.failed_list) == set(builder.esm_ds.keys())

        with pytest.raises(RuntimeError, match="Something stupid"):
            raise builder.failed_list[0][1]

    def test_build_concat_dim_issue(
        self,
        local_om2_datastore_path,
        intake_esm_kwargs,
        tmpdir,
    ):
        """
        Test that the build method creates an IcechunkStore with the expected
        store type and storage options. To ensure we have some failures, we're
        going to change the parser to one that doesn't support concatenation along a dimension.
        This should trigger a specific failure mode that we want to check is handled correctly.
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
        )

        with pytest.raises(
            icechunk.IcechunkError,
            match="cannot commit, no changes made to the session",
        ):
            with patch(
                "intake_virtual_icechunk.source._build.open_virtual_mfdataset",
                side_effect=ValueError("Something stupid"),
            ):
                builder.build()

        assert len(builder.failed_list) == len(builder.esm_ds.keys())
        assert set(fl[0] for fl in builder.failed_list) == set(builder.esm_ds.keys())

        with pytest.raises(ValueError, match="Something stupid"):
            raise builder.failed_list[0][1]

        dummy_store_path_2 = tmpdir / "dummy_store2.icechunk"
        builder_2 = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            icechunk_store_path=dummy_store_path_2,
        )

        with patch(
            "intake_virtual_icechunk.source._build.open_virtual_mfdataset",
            side_effect=ValueError(
                "Could not find any dimension coordinates to use to order the Dataset objects for concatenation"
            ),
        ):
            builder_2.build()

        assert builder_2.failed_list == []

    def test_build_deiters_cols_existing(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """
        Test that the build method correctly de-iterates columns specified in the cols_to_deiter argument.
        This is a regression test for a specific issue we had where if the column to de-iterate had some null values, the de-iteration would fail.
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            cols_to_deiter=["variable_cell_methods"],
        )

        builder.build()

        # Open the built store and check that variable_cell_methods was de-iterated.
        cat = intake.open_virtual_icechunk(str(dummy_store_path))

        assert "variable_cell_methods" in cat.df.columns
        assert cat.df.loc["ocean.fx.xt_ocean:1.yt_ocean:1.point"].variable is None

    def test_repr_defaults(self, local_om2_datastore_path, intake_esm_kwargs, tmpdir):
        """
        __repr__ should include all key fields with their default values when no
        optional arguments are provided.
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
        )

        result = repr(builder)

        assert f"esm_datastore_path='{builder.esm_datastore_path}'" in result
        assert f"icechunk_store_path='{builder.store_path}'" in result
        assert f"parser={builder.parser.__class__.__name__}" in result
        assert "storage_options={}" in result
        assert "store_options={}" in result
        assert "drop_cols=[]" in result
        assert "cols_to_deiter=[]" in result
        assert result.startswith("VirtualIcechunkStoreBuilder(")
        assert result.endswith(")")

    def test_repr_with_custom_args(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """
        __repr__ should reflect non-default values for all optional arguments.
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            parser=virtualizarr.parsers.HDFParser,
            icechunk_storage_options={"key": "value"},
            icechunk_store_options={"opt": 1},
            drop_cols=["path"],
            cols_to_deiter=["variable"],
        )

        result = repr(builder)

        assert "storage_options={'key': 'value'}" in result
        assert "store_options={'opt': 1}" in result
        assert "drop_cols=['path']" in result
        assert "cols_to_deiter=['variable']" in result
        assert "parser=HDFParser" in result

    def test_repr_parser_name_matches_instance(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """
        The parser name in __repr__ should match the class name of the instantiated parser.
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            parser=virtualizarr.parsers.HDFParser,
        )

        assert f"parser={builder.parser.__class__.__name__}" in repr(builder)
        assert "parser=HDFParser" in repr(builder)

    def test_build_deiters_cols_exceptionlogic(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """
        Test that the build method correctly de-iterates columns specified in the cols_to_deiter argument.
        This is a regression test for a specific issue we had where if the column to de-iterate had some null values, the de-iteration would fail.
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            icechunk_store_path=dummy_store_path,
            cols_to_deiter=["start_date", "variable_standard_name"],
        )

        builder.build()

        # Open the built store and check that configured columns were de-iterated.
        cat = intake.open_virtual_icechunk(str(dummy_store_path))

        assert "start_date" in cat.df.columns

        # The fixture represents missing scalar dates with the sentinel string "none".
        assert cat.df.loc["ocean.fx.xt_ocean:1.yt_ocean:1.point"].start_date == "none"
        # Nothing in here for this dataset
        assert (
            cat.df.loc["ocean.fx.xt_ocean:1.yt_ocean:1.point"].variable_standard_name
            is None
        )

    def test_infer_parser_missing_format_attribute(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """_infer_parser raises ParserInferenceError when assets.format has no .value attribute."""
        from intake_virtual_icechunk.source._build import ParserInferenceError

        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            icechunk_store_path=dummy_store_path,
        )
        # MagicMock(spec=[]) exposes no attributes — accessing .value raises AttributeError
        mock_esm_ds = MagicMock()
        mock_esm_ds.esmcat.assets.format = MagicMock(spec=[])
        builder._esm_ds = mock_esm_ds

        with pytest.raises(ParserInferenceError, match="Cannot infer parser"):
            builder._infer_parser()

    def test_infer_parser_format_none(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """_infer_parser raises ParserInferenceError when format.value is None."""
        from intake_virtual_icechunk.source._build import ParserInferenceError

        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            icechunk_store_path=dummy_store_path,
        )
        mock_esm_ds = MagicMock()
        mock_esm_ds.esmcat.assets.format.value = None
        builder._esm_ds = mock_esm_ds

        with pytest.raises(ParserInferenceError, match="Cannot infer parser"):
            builder._infer_parser()

    def test_infer_parser_unknown_format(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """_infer_parser raises ParserInferenceError when format.value is not in PARSER_MAP."""
        from intake_virtual_icechunk.source._build import ParserInferenceError

        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            icechunk_store_path=dummy_store_path,
        )
        mock_esm_ds = MagicMock()
        mock_esm_ds.esmcat.assets.format.value = "csv"
        builder._esm_ds = mock_esm_ds

        with pytest.raises(
            ParserInferenceError, match="Unsupported parser format 'csv'"
        ):
            builder._infer_parser()

    def test_build_virtual_concat_dim_fallback_failure(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """When the concat-dim fallback's open_virtual_dataset also fails, each group
        lands in failed_list and the build raises IcechunkError."""
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            icechunk_store_path=dummy_store_path,
        )
        concat_dim_msg = (
            "Could not find any dimension coordinates to use to order "
            "the Dataset objects for concatenation"
        )
        with pytest.raises(
            icechunk.IcechunkError,
            match="cannot commit, no changes made to the session",
        ):
            with patch(
                "intake_virtual_icechunk.source._build.open_virtual_mfdataset",
                side_effect=ValueError(concat_dim_msg),
            ):
                with patch(
                    "intake_virtual_icechunk.source._build.open_virtual_dataset",
                    side_effect=RuntimeError("single file virtualisation also failed"),
                ):
                    builder.build()

        assert len(builder.failed_list) == len(builder.esm_ds.keys())
        assert set(fl[0] for fl in builder.failed_list) == set(builder.esm_ds.keys())

    @pytest.mark.parametrize(
        "xr_kwargs",
        [None, {"decode_cf": True}, [{"decode_cf": True}, {"decode_times": True}]],
    )
    def test_init_xarray_kwargs(
        self,
        local_om2_datastore_path,
        intake_esm_kwargs,
        tmpdir,
        xr_kwargs,
    ):
        """
        Test that we can initialise and pass through the xarray kwargs corerctly.
        This behaviour is defined on the ABC, so we only need to test it on one
        of the child classes
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            xarray_kwargs=xr_kwargs,
        )
        if xr_kwargs is None:
            assert builder.xarray_kwargs == [{} for _ in builder.esm_ds]
        elif isinstance(xr_kwargs, dict):
            assert builder.xarray_kwargs == [xr_kwargs for _ in builder.esm_ds]
        elif isinstance(xr_kwargs, list):
            assert builder.xarray_kwargs == xr_kwargs

    @patch("intake_virtual_icechunk.source._build.open_virtual_mfdataset")
    def test_build_nested(
        self,
        mock_open_virtual_mfdataset,
        local_om2_datastore_path,
        intake_esm_kwargs,
        tmpdir,
    ):
        """
        Test that we can initialise and pass through the xarray kwargs corerctly.
        This behaviour is defined on the ABC, so we only need to test it on one
        of the child calssed
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            xarray_kwargs={"combine": "nested"},
        )
        builder.build()

        assert mock_open_virtual_mfdataset.call_count == len(builder.esm_ds)
        for call in mock_open_virtual_mfdataset.call_args_list:
            _, kwargs = call
            assert kwargs["combine"] == "nested"


class TestIcechunkStoreBuilderIsAbstract:
    """Verify that IcechunkStoreBuilder cannot be instantiated directly."""

    @pytest.fixture
    def intake_esm_kwargs(self) -> dict[str, list[str]]:
        return {
            "columns_with_iterables": [
                "variable",
                "variable_long_name",
                "variable_standard_name",
                "variable_cell_methods",
                "variable_units",
            ]
        }

    def test_cannot_instantiate_abstract_base(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        import pytest

        dummy_store_path = tmpdir / "dummy_store.icechunk"
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            AbstractIcechunkStoreBuilder(
                esm_datastore_path=local_om2_datastore_path,
                icechunk_store_path=dummy_store_path,
                esm_datastore_kwargs=intake_esm_kwargs,
            )


class TestZarrIcechunkStoreBuilder:
    """Tests for ZarrIcechunkStoreBuilder (the real-data builder)."""

    @pytest.fixture
    def intake_esm_kwargs(self) -> dict[str, list[str]]:
        return {
            "columns_with_iterables": [
                "variable",
                "variable_long_name",
                "variable_standard_name",
                "variable_cell_methods",
                "variable_units",
            ]
        }

    def test_init(self, local_om2_datastore_path, intake_esm_kwargs, tmpdir):
        """Initialisation should store all parameters and not open the datastore."""
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
        )

        assert builder._esm_ds is None

        n_datasets = len(builder.esm_ds)
        assert builder.xarray_kwargs == [{} for _ in range(n_datasets)]
        assert builder.storage_options == {}
        assert builder.drop_cols == []
        assert builder.cols_to_deiter == []

    def test_init_with_xarray_kwargs(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """xarray_kwargs should be forwarded and stored."""
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            xarray_kwargs={"decode_times": False},
        )

        n_datasets = len(builder.esm_ds)
        assert builder.xarray_kwargs == [
            {"decode_times": False} for _ in range(n_datasets)
        ]

    def test_repr_defaults(self, local_om2_datastore_path, intake_esm_kwargs, tmpdir):
        """__repr__ should show all fields and start with the class name."""
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
        )

        result = repr(builder)

        assert result.startswith("ZarrIcechunkStoreBuilder(")
        assert result.endswith(")")
        assert f"esm_datastore_path='{builder.esm_datastore_path}'" in result
        assert f"icechunk_store_path='{builder.store_path}'" in result
        assert "xarray_kwargs={}" in result
        assert "storage_options={}" in result
        assert "drop_cols=[]" in result
        assert "cols_to_deiter=[]" in result

    def test_repr_with_custom_args(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """__repr__ should reflect non-default values."""
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            xarray_kwargs={"decode_times": False},
            icechunk_storage_options={"key": "value"},
            drop_cols=["path"],
            cols_to_deiter=["variable"],
        )

        result = repr(builder)

        assert "xarray_kwargs={'decode_times': False}" in result
        assert "storage_options={'key': 'value'}" in result
        assert "drop_cols=['path']" in result
        assert "cols_to_deiter=['variable']" in result

    def test_clean_build(self, local_om2_datastore_path, intake_esm_kwargs, tmpdir):
        """
        Build should create an Icechunk store with one group per catalog entry,
        a JSON sidecar with no virtual_chunk_model, and zero failures.
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
        )

        builder.build()

        assert Path(builder.store_path).exists()
        assert Path(builder.store_path).is_dir()

        fname = _intake_cat_filename(builder.store_path)
        assert (Path(builder.store_path) / fname).exists()
        assert builder.failed_list == []

    def test_build_sidecar_has_no_virtual_chunk_model(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """
        The JSON sidecar written by ZarrIcechunkStoreBuilder must have
        virtual_chunk_model set to null (None).
        """
        import json

        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
        )
        builder.build()

        fname = _intake_cat_filename(builder.store_path)
        sidecar_path = Path(builder.store_path) / fname
        with open(sidecar_path) as f:
            sidecar = json.load(f)

        assert sidecar["virtual_chunk_model"] is None

    def test_catalog_round_trip(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """
        A store built by ZarrIcechunkStoreBuilder should be openable via
        IcechunkCatalog without needing virtual-chunk credentials.
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
        )
        builder.build()

        cat = intake.open_virtual_icechunk(str(dummy_store_path))

        assert cat.virtual_chunk_model is None
        assert cat.virtual_chunk_container is None
        assert len(cat) == len(builder.esm_ds.keys())
        assert set(cat.keys()) == set(builder.esm_ds.keys())

    def test_build_all_failures(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """When xr.open_mfdataset fails with a non-concat-dim error, all groups land
        in failed_list and the build raises IcechunkError."""
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
        )
        with pytest.raises(
            icechunk.IcechunkError,
            match="cannot commit, no changes made to the session",
        ):
            with patch(
                "xarray.open_mfdataset",
                side_effect=RuntimeError("simulated generic failure"),
            ):
                builder.build()

        assert len(builder.failed_list) == len(builder.esm_ds.keys())
        assert set(fl[0] for fl in builder.failed_list) == set(builder.esm_ds.keys())

    def test_build_concat_dim_fallback_failure(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """When the concat-dim fallback's xr.open_dataset also fails, all groups land
        in failed_list and the build raises IcechunkError."""
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            icechunk_store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
        )
        concat_dim_msg = (
            "Could not find any dimension coordinates to use to order "
            "the Dataset objects for concatenation"
        )
        with pytest.raises(
            icechunk.IcechunkError,
            match="cannot commit, no changes made to the session",
        ):
            with patch(
                "xarray.open_mfdataset",
                side_effect=ValueError(concat_dim_msg),
            ):
                with patch(
                    "xarray.open_dataset",
                    side_effect=RuntimeError("single file open also failed"),
                ):
                    builder.build()

        assert len(builder.failed_list) == len(builder.esm_ds.keys())
        assert set(fl[0] for fl in builder.failed_list) == set(builder.esm_ds.keys())

    def _builder(self, datastore_path, esm_kwargs, store_path):
        return IcechunkStoreBuilder(
            esm_datastore_path=datastore_path,
            icechunk_store_path=store_path,
            esm_datastore_kwargs=esm_kwargs,
        )

    @staticmethod
    def _multidim_data_arrays(store_path) -> dict:
        """Return {path: zarr.Array} for every >=2-D array in the built store."""
        repo = icechunk.Repository.open(
            icechunk.local_filesystem_storage(str(store_path))
        )
        root = zarr.open_group(repo.readonly_session("main").store, mode="r")
        arrays = {}
        for gname, group in root.groups():
            for aname, arr in group.arrays():
                if arr.ndim >= 2:
                    arrays[f"{gname}/{aname}"] = arr
        return arrays

    def test_rechunk_returns_self(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        builder = self._builder(
            local_om2_datastore_path, intake_esm_kwargs, tmpdir / "s.icechunk"
        )
        assert builder._rechunk is None
        assert builder.rechunk(chunks="1MiB") is builder
        assert builder._rechunk is not None

    def test_normalize_size_arg(self):
        assert _normalize_size_arg(None, "chunks", allow_auto=True) is None
        assert _normalize_size_arg("auto", "chunks", allow_auto=True) == "auto"
        assert _normalize_size_arg("128", "chunks", allow_auto=True) == 128
        assert _normalize_size_arg("128MiB", "chunks", allow_auto=True) == 128 * 1024**2
        assert _normalize_size_arg({"time": 3}, "chunks", allow_auto=True) == {
            "time": 3
        }

    def test_normalize_size_arg_invalid(self):
        with pytest.raises(ValueError):
            _normalize_size_arg("not-a-size", "chunks", allow_auto=True)
        with pytest.raises(ValueError):
            _normalize_size_arg({"time": 0}, "chunks", allow_auto=True)
        with pytest.raises(ValueError):
            _normalize_size_arg(-5, "chunks", allow_auto=True)
        with pytest.raises(TypeError):
            _normalize_size_arg([1, 2], "chunks", allow_auto=True)
        with pytest.raises(ValueError):
            _normalize_size_arg("auto", "shards", allow_auto=False)

    def test_inner_chunk_shape(self):
        # numpy-backed (not dask) -> whole dimension is one chunk
        numpy_var = xr.DataArray(np.zeros((3, 4)), dims=("a", "b"))
        assert _inner_chunk_shape(numpy_var) == {"a": 3, "b": 4}
        # dask-backed -> first block length per dimension
        dask_var = numpy_var.chunk({"a": 2, "b": 4})
        assert _inner_chunk_shape(dask_var) == {"a": 2, "b": 4}

    def test_resolve_shard_shape_dict(self):
        var = xr.DataArray(np.zeros((240, 200), dtype="f4"), dims=("t", "x"))
        chunk_by_dim = {"t": 12, "x": 50}
        # unlisted dims default to one chunk; listed dims kept (multiples of chunk)
        shard = _resolve_shard_shape("v", var, chunk_by_dim, {"t": 24})
        assert shard == {"t": 24, "x": 50}

    def test_resolve_shard_shape_dict_smaller_than_chunk_disables(self):
        var = xr.DataArray(np.zeros((240, 200), dtype="f4"), dims=("t", "x"))
        chunk_by_dim = {"t": 12, "x": 50}
        with pytest.warns(UserWarning, match="unsharded"):
            assert _resolve_shard_shape("v", var, chunk_by_dim, {"t": 6}) is None

    def test_rechunk_dataset_sharded_roundtrip(self, tmpdir):
        """Inner chunk strictly smaller than shard must survive a to_icechunk write.

        Regression guard: this only succeeds if the dask graph is realigned to the
        shard shape (otherwise to_icechunk raises "would overlap multiple Dask
        chunks") and encoding['chunks'] pins the inner chunk.
        """
        ds = xr.Dataset({"v": (("t", "x"), np.zeros((240, 200), dtype="f4"))}).chunk(
            {"t": 24, "x": 100}
        )
        out = _rechunk_dataset(
            ds, _RechunkSpec(chunks={"t": 12, "x": 50}, shards={"t": 24, "x": 100})
        )

        repo = icechunk.Repository.create(
            icechunk.local_filesystem_storage(str(tmpdir / "sharded.icechunk"))
        )
        with repo.transaction("main", message="shard test") as store:
            to_icechunk(out, store.session, group="g", mode="a")

        arr = zarr.open_group(repo.readonly_session("main").store, mode="r")["g"]["v"]
        assert arr.chunks == (12, 50)
        assert arr.shards == (24, 100)
        assert arr.chunks != arr.shards  # genuinely sub-shard chunked

    def test_rechunk_dataset_chunks_auto(self):
        # "auto" hands the shape to dask; a numpy-backed dataset becomes chunked.
        ds = xr.Dataset({"v": (("t", "x"), np.zeros((100, 100), dtype="f4"))})
        assert ds["v"].chunks is None
        out = _rechunk_dataset(ds, _RechunkSpec(chunks="auto", shards=None))
        assert out["v"].chunks is not None

    def test_rechunk_dataset_skips_scalar_data_var(self):
        # A dimensionless data variable can't be sharded and must be skipped.
        ds = xr.Dataset(
            {
                "s": ((), np.float32(1.0)),
                "v": (("t", "x"), np.zeros((48, 48), dtype="f4")),
            }
        ).chunk({"t": 12, "x": 12})
        out = _rechunk_dataset(ds, _RechunkSpec(chunks=None, shards={"t": 24, "x": 24}))
        assert "shards" not in out["s"].encoding
        assert out["v"].encoding["shards"] == (24, 24)

    def test_rechunk_dataset_shard_multiple_of_chunk(self):
        ds = xr.Dataset({"v": (("t", "x"), np.zeros((240, 200), dtype="f4"))}).chunk(
            {"t": 24, "x": 100}
        )
        out = _rechunk_dataset(
            ds, _RechunkSpec(chunks={"t": 12, "x": 50}, shards=4 * 1024 * 1024)
        )
        chunks = out["v"].encoding["chunks"]
        shards = out["v"].encoding["shards"]
        assert chunks == (12, 50)
        assert all(s % c == 0 for s, c in zip(shards, chunks))

    def test_rechunk_dataset_shard_smaller_than_chunk_disables(self):
        ds = xr.Dataset({"v": (("t", "x"), np.zeros((240, 200), dtype="f4"))}).chunk(
            {"t": 240, "x": 200}
        )
        with pytest.warns(UserWarning, match="unsharded"):
            out = _rechunk_dataset(ds, _RechunkSpec(chunks=None, shards=8))
        assert "shards" not in out["v"].encoding

    def test_representative_source_size(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        builder = self._builder(
            local_om2_datastore_path, intake_esm_kwargs, tmpdir / "s.icechunk"
        )
        size = _representative_source_size(builder._source_file_paths(), n=5)
        assert isinstance(size, int) and size > 0

    def test_build_with_chunk_size(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        store_path = tmpdir / "chunksize.icechunk"
        builder = self._builder(local_om2_datastore_path, intake_esm_kwargs, store_path)
        builder.rechunk(chunks="4KiB").build()

        arrays = self._multidim_data_arrays(store_path)
        assert arrays  # the store has multi-dim data variables
        for arr in arrays.values():
            chunk_bytes = int(np.prod(arr.chunks)) * arr.dtype.itemsize
            full_bytes = int(np.prod(arr.shape)) * arr.dtype.itemsize
            assert arr.shards is None
            # dask keeps each chunk at/under the target; only arrays bigger than
            # the target are actually split.
            if full_bytes > 4 * 1024:
                assert chunk_bytes <= 4 * 1024
                assert arr.chunks != arr.shape

    def test_build_with_chunk_dict(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        store_path = tmpdir / "chunkdict.icechunk"
        builder = self._builder(local_om2_datastore_path, intake_esm_kwargs, store_path)
        builder.rechunk(chunks={"time": 1}).build()

        seen_time = False
        for arr in self._multidim_data_arrays(store_path).values():
            dims = arr.metadata.dimension_names
            if dims and "time" in dims:
                seen_time = True
                assert arr.chunks[dims.index("time")] == 1
        assert seen_time  # at least one data var has a time dimension

    def test_build_with_shards_auto(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        store_path = tmpdir / "shardsauto.icechunk"
        builder = self._builder(local_om2_datastore_path, intake_esm_kwargs, store_path)
        builder.rechunk(chunks="2KiB", shards="auto").build()

        sharded = [
            a for a in self._multidim_data_arrays(store_path).values() if a.shards
        ]
        assert sharded  # at least one variable ended up sharded
        for arr in sharded:
            assert all(s % c == 0 for s, c in zip(arr.shards, arr.chunks))


class TestIcechunkCephStoreBuilder(BuilderTests):
    """
    Tests for the IcechunkStoreBuilder class, which is responsible for building
    an IcechunkStore from a given intake-esm datastore.
    """

    @pytest.fixture(scope="class")
    def bucket_base_url(self) -> str:
        return "s3://intake-virtual-icechunk-store"

    @pytest.fixture
    def icecat_store_tmp_url(self, bucket_base_url) -> Generator[str, None, None]:
        """
        Should not be used for anything that intends to write to the store.
        as this is not a yield fixture so doesn't perform cleanup
        """
        hash_suffix = uuid.uuid4().hex
        yield f"{bucket_base_url}/icecat-{hash_suffix}"

        # Cleanup - delete all objects with the prefix we used for the test

        # This might fail if we didnjt actually create the store? Only one way to
        # find out I guess.

        try:
            load_dotenv()
            access_key = os.getenv("CEPH_ACCESS_KEY_ID")
            secret_key = os.getenv("CEPH_SECRET_ACCESS_KEY")

            if not access_key or not secret_key:
                print(
                    "Skipping Ceph cleanup because CEPH_ACCESS_KEY_ID/CEPH_SECRET_ACCESS_KEY are not configured"
                )
                return

            s3_store: ObjectStore = from_url(
                bucket_base_url,
                config={
                    "endpoint_url": "https://projects.pawsey.org.au",
                    "access_key_id": access_key,
                    "secret_access_key": secret_key,
                },
            )

            s3_store.delete(f"icecat-{hash_suffix}")
        except Exception as e:
            print(f"Error during teardown of Ceph store: {e}")
            print(
                f"Please manually delete the objects with prefix icecat-{hash_suffix} from the intake-virtual-icechunk-store bucket"
            )

    @pytest.fixture
    def esm_datastore_kwargs(self) -> dict[str, Any]:
        return {
            "storage_options": {
                "endpoint_url": "https://projects.pawsey.org.au",
                "anon": True,
            },
            "columns_with_iterables": [
                "variable",
                "variable_long_name",
                "variable_standard_name",
                "variable_cell_methods",
                "variable_units",
            ],
        }

    @pytest.fixture
    def icechunk_store_opts(self) -> dict[str, str | bool]:
        return {
            "endpoint_url": "https://projects.pawsey.org.au",
            "s3_compatible": True,
            "force_path_style": True,
            "anonymous": True,
        }

    @pytest.fixture
    def icechunk_storage_opts(self) -> dict[str, str | bool]:
        return {
            "endpoint_url": "https://projects.pawsey.org.au",
            "force_path_style": True,
            "anonymous": True,
        }

    @pytest.fixture(scope="class")
    def esm_datastore_path(self) -> str:
        return "s3://intake-virtual-icechunk-om2-esm-ds-container/access-om2.json"

    def test_init_infer_parser(
        self,
        esm_datastore_kwargs,
        esm_datastore_path,
        icechunk_cephstore_info,
        icechunk_storage_opts,
    ):
        """
        Initialisation without a parser should trigger parser inference, which
        in turn should open the esm datastore
        """
        store_url = f"{icechunk_cephstore_info.icecat_bucket_url}{icechunk_cephstore_info.icecat_prefix}"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=esm_datastore_path,
            icechunk_store_path=store_url,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_storage_options=icechunk_storage_opts,
        )

        assert isinstance(builder.parser, virtualizarr.parsers.hdf.hdf.HDFParser)

    def test_clean_build(
        self,
        esm_datastore_path,
        esm_datastore_kwargs,
        icecat_store_tmp_url,
        icechunk_store_opts,
        icechunk_storage_opts,
    ):
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=esm_datastore_path,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_store_path=icecat_store_tmp_url,
            icechunk_store_options=icechunk_store_opts,
            icechunk_storage_options=icechunk_storage_opts,
        )

        builder.build()

        s3_store: ObjectStore = from_url(  # type: ignore[annotation-unchecked]
            icecat_store_tmp_url,
            config={
                "endpoint_url": "https://projects.pawsey.org.au",
                "skip_signature": True,
            },
        )

        obj_list = list(tlz.concat(s3_store.list()))

        fname = _intake_cat_filename(builder.store_path)

        assert [i for i in obj_list if i["path"] == fname]  # Wrote the json file
        assert (
            len([i for i in obj_list if i["path"] == fname]) == 1
        )  # Only wrote one json file
        assert len(obj_list) > 1  # Wrote some chunks
        assert builder.failed_list == []  # No failures

    def test_build_all_failures(
        self,
        esm_datastore_path,
        esm_datastore_kwargs,
        icecat_store_tmp_url,
        icechunk_store_opts,
        icechunk_storage_opts,
    ):
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=esm_datastore_path,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_store_path=icecat_store_tmp_url,
            icechunk_store_options=icechunk_store_opts,
            icechunk_storage_options=icechunk_storage_opts,
            parser=virtualizarr.parsers.ZarrParser,
        )
        with pytest.raises(
            icechunk.IcechunkError,
            match="cannot commit, no changes made to the session",
        ):
            builder.build()

        # If the build failed, we should have a list of all the datasets that failed and why
        assert len(builder.failed_list) == len(builder.esm_ds.keys())
        assert set(fl[0] for fl in builder.failed_list) == set(builder.esm_ds.keys())

    def test_build_not_concat_dim_issue(
        self,
        esm_datastore_path,
        esm_datastore_kwargs,
        icecat_store_tmp_url,
        icechunk_store_opts,
        icechunk_storage_opts,
    ):
        """
        Test that the build method creates an IcechunkStore with the expected
        store type and storage options. To ensure we have some failures, we're
        going to change the parser to one that doesn't support concatenation along a dimension.
        This should trigger a specific failure mode that we want to check is handled correctly.
        """
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=esm_datastore_path,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_store_path=icecat_store_tmp_url,
            icechunk_store_options=icechunk_store_opts,
            icechunk_storage_options=icechunk_storage_opts,
        )

        with pytest.raises(
            icechunk.IcechunkError,
            match="cannot commit, no changes made to the session",
        ):
            with patch(
                "intake_virtual_icechunk.source._build.open_virtual_mfdataset",
                side_effect=RuntimeError("Something stupid"),
            ):
                builder.build()

        assert len(builder.failed_list) == len(builder.esm_ds.keys())
        assert set(fl[0] for fl in builder.failed_list) == set(builder.esm_ds.keys())

        with pytest.raises(RuntimeError, match="Something stupid"):
            raise builder.failed_list[0][1]

    def test_init_with_parser(
        self,
        esm_datastore_path,
        esm_datastore_kwargs,
        icechunk_cephstore_info,
        icechunk_storage_opts,
    ):
        """
        Initialisation with a parser should use the provided parser not instantiate
        the esm datastore until it's asked for
        """
        store_url = f"{icechunk_cephstore_info.icecat_bucket_url}{icechunk_cephstore_info.icecat_prefix}"
        parser = virtualizarr.parsers.hdf.hdf.HDFParser
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=esm_datastore_path,
            icechunk_store_path=store_url,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_storage_options=icechunk_storage_opts,
            parser=parser,
        )

        assert builder._esm_ds is None
        assert isinstance(builder.parser, virtualizarr.parsers.hdf.hdf.HDFParser)

        # Accessing esm_ds should trigger lazy loading
        _ = builder.esm_ds
        assert builder._esm_ds is not None

    @pytest.mark.parametrize(
        "format_val, parser",
        [
            ("netcdf", virtualizarr.parsers.HDFParser),
            ("zarr", virtualizarr.parsers.ZarrParser),
            ("zarr2", virtualizarr.parsers.ZarrParser),
            ("zarr3", virtualizarr.parsers.ZarrParser),
            ("reference", virtualizarr.parsers.KerchunkJSONParser),
        ],
    )
    def test_infer_parser(
        self,
        esm_datastore_path,
        esm_datastore_kwargs,
        icechunk_cephstore_info,
        icechunk_storage_opts,
        format_val,
        parser,
    ):
        """
        Mostly a regression test for now.
        """
        store_url = f"{icechunk_cephstore_info.icecat_bucket_url}{icechunk_cephstore_info.icecat_prefix}"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=esm_datastore_path,
            icechunk_store_path=store_url,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_storage_options=icechunk_storage_opts,
        )
        from intake_esm.cat import DataFormat

        builder.esm_ds.esmcat.assets.format = DataFormat(format_val)

        inferred_parser = builder._infer_parser()

        assert inferred_parser == parser

    def test_build_concat_dim_issue(
        self,
        esm_datastore_path,
        esm_datastore_kwargs,
        icecat_store_tmp_url,
        bucket_base_url,
        icechunk_store_opts,
        icechunk_storage_opts,
    ):
        """
        Test that the build method creates an IcechunkStore with the expected
        store type and storage options. To ensure we have some failures, we're
        going to change the parser to one that doesn't support concatenation along a dimension.
        This should trigger a specific failure mode that we want to check is handled correctly.
        """
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=esm_datastore_path,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_store_path=icecat_store_tmp_url,
            icechunk_store_options=icechunk_store_opts,
            icechunk_storage_options=icechunk_storage_opts,
        )

        with pytest.raises(
            icechunk.IcechunkError,
            match="cannot commit, no changes made to the session",
        ):
            with patch(
                "intake_virtual_icechunk.source._build.open_virtual_mfdataset",
                side_effect=ValueError("Something stupid"),
            ):
                builder.build()

        assert len(builder.failed_list) == len(builder.esm_ds.keys())
        assert set(fl[0] for fl in builder.failed_list) == set(builder.esm_ds.keys())

        with pytest.raises(ValueError, match="Something stupid"):
            raise builder.failed_list[0][1]

        second_hash = uuid.uuid4().hex
        second_store_url = f"{bucket_base_url}/icecat-{second_hash}"

        try:
            builder_2 = VirtualIcechunkStoreBuilder(
                esm_datastore_path=esm_datastore_path,
                esm_datastore_kwargs=esm_datastore_kwargs,
                icechunk_store_path=second_store_url,
                icechunk_store_options=icechunk_store_opts,
                icechunk_storage_options=icechunk_storage_opts,
            )

            with patch(
                "intake_virtual_icechunk.source._build.open_virtual_mfdataset",
                side_effect=ValueError(
                    "Could not find any dimension coordinates to use to order the Dataset objects for concatenation"
                ),
            ):
                builder_2.build()

            assert builder_2.failed_list == []
        finally:
            load_dotenv()
            access_key = os.getenv("CEPH_ACCESS_KEY_ID")
            secret_key = os.getenv("CEPH_SECRET_ACCESS_KEY")

            if access_key and secret_key:
                cleanup_store: ObjectStore = from_url(
                    bucket_base_url,
                    config={
                        "endpoint_url": "https://projects.pawsey.org.au",
                        "access_key_id": access_key,
                        "secret_access_key": secret_key,
                    },
                )
                cleanup_store.delete(f"icecat-{second_hash}")

    def test_build_deiters_cols_existing(
        self,
        esm_datastore_path,
        esm_datastore_kwargs,
        icecat_store_tmp_url,
        icechunk_store_opts,
        icechunk_storage_opts,
    ):
        """
        Test that the build method correctly de-iterates columns specified in the cols_to_deiter argument.
        This is a regression test for a specific issue we had where if the column to de-iterate had some null values, the de-iteration would fail.
        """
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=esm_datastore_path,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_store_path=icecat_store_tmp_url,
            icechunk_store_options=icechunk_store_opts,
            icechunk_storage_options=icechunk_storage_opts,
            cols_to_deiter=["variable_cell_methods"],
        )

        builder.build()

        # Open the built store and check that variable_cell_methods was de-iterated.
        cat = intake.open_virtual_icechunk(
            icecat_store_tmp_url, storage_options=icechunk_storage_opts
        )

        assert "variable_cell_methods" in cat.df.columns
        assert cat.df.loc["ocean.fx.xt_ocean:1.yt_ocean:1.point"].variable is None

    def test_build_roundtrip_reads_dataset(
        self,
        esm_datastore_path,
        esm_datastore_kwargs,
        icecat_store_tmp_url,
        icechunk_store_opts,
        icechunk_storage_opts,
    ):
        """Build a Ceph-backed catalog, reopen it, and read real data back."""
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=esm_datastore_path,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_store_path=icecat_store_tmp_url,
            icechunk_store_options=icechunk_store_opts,
            icechunk_storage_options=icechunk_storage_opts,
        )

        builder.build()

        cat = intake.open_virtual_icechunk(
            icecat_store_tmp_url, storage_options=icechunk_storage_opts
        )

        assert len(cat.df) > 0
        assert len(cat.keys()) > 0

        datasets_with_data_vars = 0

        for key in cat.keys():
            ds = cat[key].to_xarray()
            if not ds.data_vars:
                continue

            datasets_with_data_vars += 1
            var_name = next(iter(ds.data_vars))
            sample = ds[var_name].isel({dim: 0 for dim in ds[var_name].dims}).load()

            assert isinstance(sample.values, np.ndarray)
            assert sample.size == 1

        assert datasets_with_data_vars > 0

    def test_repr_defaults(
        self,
        esm_datastore_path,
        esm_datastore_kwargs,
        icechunk_cephstore_info,
        icechunk_storage_opts,
    ):
        """
        __repr__ should include all key fields with their default values when no
        optional arguments are provided.
        """
        store_url = f"{icechunk_cephstore_info.icecat_bucket_url}{icechunk_cephstore_info.icecat_prefix}"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=esm_datastore_path,
            icechunk_store_path=store_url,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_storage_options=icechunk_storage_opts,
        )

        result = repr(builder)

        assert f"esm_datastore_path='{builder.esm_datastore_path}'" in result
        assert f"icechunk_store_path='{builder.store_path}'" in result
        assert f"parser={builder.parser.__class__.__name__}" in result
        assert "drop_cols=[]" in result
        assert "cols_to_deiter=[]" in result
        assert result.startswith("VirtualIcechunkStoreBuilder(")
        assert result.endswith(")")

    def test_repr_with_custom_args(
        self,
        esm_datastore_path,
        esm_datastore_kwargs,
        icechunk_cephstore_info,
        icechunk_store_opts,
        icechunk_storage_opts,
    ):
        """
        __repr__ should reflect non-default values for all optional arguments.
        """
        store_url = f"{icechunk_cephstore_info.icecat_bucket_url}{icechunk_cephstore_info.icecat_prefix}"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=esm_datastore_path,
            icechunk_store_path=store_url,
            esm_datastore_kwargs=esm_datastore_kwargs,
            parser=virtualizarr.parsers.HDFParser,
            icechunk_storage_options=icechunk_storage_opts,
            icechunk_store_options=icechunk_store_opts,
            drop_cols=["path"],
            cols_to_deiter=["variable"],
        )

        result = repr(builder)

        assert f"storage_options={icechunk_storage_opts}" in result
        assert f"store_options={icechunk_store_opts}" in result
        assert "drop_cols=['path']" in result
        assert "cols_to_deiter=['variable']" in result
        assert "parser=HDFParser" in result

    def test_repr_parser_name_matches_instance(
        self,
        esm_datastore_path,
        esm_datastore_kwargs,
        icechunk_cephstore_info,
        icechunk_storage_opts,
    ):
        """
        The parser name in __repr__ should match the class name of the instantiated parser.
        """
        store_url = f"{icechunk_cephstore_info.icecat_bucket_url}{icechunk_cephstore_info.icecat_prefix}"
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=esm_datastore_path,
            icechunk_store_path=store_url,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_storage_options=icechunk_storage_opts,
            parser=virtualizarr.parsers.HDFParser,
        )

        assert f"parser={builder.parser.__class__.__name__}" in repr(builder)
        assert "parser=HDFParser" in repr(builder)

    def test_build_deiters_cols_exceptionlogic(
        self,
        esm_datastore_path,
        esm_datastore_kwargs,
        icecat_store_tmp_url,
        icechunk_store_opts,
        icechunk_storage_opts,
    ):
        """
        Test that the build method correctly de-iterates columns specified in the cols_to_deiter argument.
        This is a regression test for a specific issue we had where if the column to de-iterate had some null values, the de-iteration would fail.
        """
        builder = VirtualIcechunkStoreBuilder(
            esm_datastore_path=esm_datastore_path,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_store_path=icecat_store_tmp_url,
            icechunk_store_options=icechunk_store_opts,
            icechunk_storage_options=icechunk_storage_opts,
            cols_to_deiter=["start_date", "variable_standard_name"],
        )

        builder.build()

        # Open the built store and check that configured columns were de-iterated.
        cat = intake.open_virtual_icechunk(
            icecat_store_tmp_url, storage_options=icechunk_storage_opts
        )

        assert "start_date" in cat.df.columns

        # The fixture represents missing scalar dates with the sentinel string "none".
        assert cat.df.loc["ocean.fx.xt_ocean:1.yt_ocean:1.point"].start_date == "none"
        # Nothing in here for this dataset
        assert (
            cat.df.loc["ocean.fx.xt_ocean:1.yt_ocean:1.point"].variable_standard_name
            is None
        )
