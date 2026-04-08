# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest


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
def icechunk_store_path(tmp_path_factory, groups):
    """
    Create a minimal Icechunk store for testing.

    The store has one top-level Zarr group per entry in ``GROUPS``.  Each
    group contains a single 1-D float32 array (``temperature``) and has its
    metadata written into ``.zattrs``.
    """
    import icechunk
    import numpy as np
    import zarr

    store_path = str(tmp_path_factory.mktemp("stores") / "test.icechunk")
    storage = icechunk.local_filesystem_storage(store_path)
    repo = icechunk.Repository.create(storage)

    with repo.transaction("main", message="Initialise test catalog") as store:
        for entry in groups:
            grp = zarr.open_group(store, path=entry["key"], mode="w")
            grp.attrs.update(entry["attrs"])
            grp.create_array("temperature", data=np.zeros(10))

    return store_path


@pytest.fixture(scope="session")
def catalog_json_path(tmp_path_factory, icechunk_store_path):
    """Write a JSON sidecar for the test Icechunk store and return its path."""
    from intake_virtual_icechunk.cat import VirtualIcechunkCatalogModel

    out_dir = str(tmp_path_factory.mktemp("catalogs"))
    model = VirtualIcechunkCatalogModel(
        store=icechunk_store_path,
        description="Test catalog",
        title="Test",
    )
    model.save("test-catalog", directory=out_dir)
    return f"{out_dir}/test-catalog.json"
