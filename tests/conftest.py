# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from dask.distributed import Client

client = Client(threads_per_worker=1)

import pytest

from intake_virtual_icechunk.source import IcechunkStoreBuilder


@pytest.fixture(scope="session")
def sample_data() -> Path:
    return Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def groups():
    return [
        {
            "key": "CMIP.BCC.BCC-ESM1.historical",
            "attrs": {
                "source_id": "BCC-ESM1",
                "experiment_id": "historical",
                "member_id": "r1i1p1f1",
            },
        },
        {
            "key": "CMIP.BCC.BCC-ESM1.ssp585",
            "attrs": {
                "source_id": "BCC-ESM1",
                "experiment_id": "ssp585",
                "member_id": "r1i1p1f1",
            },
        },
        {
            "key": "CMIP.MRI.MRI-ESM2-0.historical",
            "attrs": {
                "source_id": "MRI-ESM2-0",
                "experiment_id": "historical",
                "member_id": "r1i1p1f1",
            },
        },
    ]


@pytest.fixture(scope="session")
def esm_datastore_path(sample_data, tmp_path_factory) -> Path:
    """
    Before building an icechunk store, we need an esm_datastore to build from!
    This is a minimal fixture to do that.
    """

    from access_nri_intake.experiment import use_datastore
    from access_nri_intake.source.builders import AccessOm2Builder

    data_root = sample_data / "access-om2"

    catalog_dir = tmp_path_factory.mktemp("access-om2") / "esmcat"
    catalog_dir.mkdir(parents=True, exist_ok=True)

    use_datastore(
        experiment_dir=data_root,
        builder=AccessOm2Builder,
        catalog_dir=catalog_dir,
        open_ds=True,
        datastore_name="access-om2",
    )
    return catalog_dir / "access-om2.json"


@pytest.fixture(scope="session")
def icechunk_store_path(esm_datastore_path, tmp_path_factory) -> Path:
    """
    Use a minimal icechunk store for testing. This needs to be rebuilt at the
    start of each test session, or virtualizarr will complain about manifests not
    being up to date.
    """

    cat_path = tmp_path_factory.mktemp("access-om2") / "icecat.icechunk"

    iscb = IcechunkStoreBuilder(
        esm_datastore_path=esm_datastore_path,
        store_path=cat_path,
        drop_cols=[
            "filename",
            # "path", # This should be droppped automatically...
            "start_date",
            "end_date",
            "file_id",
            "temporal_label",
        ],
    )

    iscb.build()

    return cat_path


@pytest.fixture
def catalog_json_path(icechunk_store_path) -> Path:
    from intake_virtual_icechunk.utils import _intake_cat_filename

    fname = _intake_cat_filename(icechunk_store_path)

    return icechunk_store_path / fname
