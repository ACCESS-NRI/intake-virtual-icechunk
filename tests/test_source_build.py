from pathlib import Path
from unittest.mock import patch

import icechunk
import intake
import pytest
import virtualizarr
from access_nri_intake.source.builders import AccessOm2Builder
from intake_esm import esm_datastore
from pandas.testing import assert_frame_equal

from intake_virtual_icechunk.source import IcechunkStoreBuilder
from intake_virtual_icechunk.utils import _intake_cat_filename

__all__ = ["IcechunkStoreBuilder", "pytest"]


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


class TestIcechunkStoreBuilder:
    """
    Tests for the IcechunkStoreBuilder class, which is responsible for building
    an IcechunkStore from a given intake-esm datastore.
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
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            store_path=dummy_store_path,
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
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            store_path=dummy_store_path,
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
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            store_path=dummy_store_path,
        )
        from intake_esm.cat import DataFormat

        builder.esm_ds.esmcat.assets.format = DataFormat(format_val)

        inferred_parser = builder._infer_parser()

        assert inferred_parser == parser

    def test_clean_build(self, local_om2_datastore_path, intake_esm_kwargs, tmpdir):
        """
        Test that the build method creates an IcechunkStore with the expected
        store type and storage options.
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            store_path=dummy_store_path,
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
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            store_path=dummy_store_path,
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
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            store_path=dummy_store_path,
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
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            store_path=dummy_store_path,
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
        builder_2 = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            store_path=dummy_store_path_2,
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
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            store_path=dummy_store_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            cols_to_deiter=["variable_cell_methods"],
        )

        builder.build()

        # Now open the built stoer and check that the variable_cell_methods column has been de-iterated correctly
        cat = intake.open_virtual_icechunk(str(dummy_store_path))

        assert "variable_cell_methods" in cat.df.columns
        assert cat.df.loc["ocean.fx.xt_ocean:1.yt_ocean:1.point"].Variable is None

    def test_build_deiters_cols_exceptionlogic(
        self, local_om2_datastore_path, intake_esm_kwargs, tmpdir
    ):
        """
        Test that the build method correctly de-iterates columns specified in the cols_to_deiter argument.
        This is a regression test for a specific issue we had where if the column to de-iterate had some null values, the de-iteration would fail.
        """
        dummy_store_path = tmpdir / "dummy_store.icechunk"
        builder = IcechunkStoreBuilder(
            esm_datastore_path=local_om2_datastore_path,
            esm_datastore_kwargs=intake_esm_kwargs,
            store_path=dummy_store_path,
            cols_to_deiter=["start_date", "variable_standard_name"],
        )

        builder.build()

        # Now open the built stoer and check that the variable_cell_methods column has been de-iterated correctly
        cat = intake.open_virtual_icechunk(str(dummy_store_path))

        assert "start_date" in cat.df.columns

        # Stupid sentinel value quirk
        assert cat.df.loc["ocean.fx.xt_ocean:1.yt_ocean:1.point"].start_date == "none"
        # Nothing in here for this dataset
        assert (
            cat.df.loc["ocean.fx.xt_ocean:1.yt_ocean:1.point"].variable_standard_name
            is None
        )
