# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest


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


@pytest.fixture
def icechunk_store_path(sample_data) -> Path:
    """
    Use a minimal icechunk store for testing. This is
    """
    return sample_data / "access-om2" / "icecat.icechunk"


@pytest.fixture
def catalog_json_path(icechunk_store_path) -> Path:
    from intake_virtual_icechunk.utils import _intake_cat_filename

    fname = _intake_cat_filename(icechunk_store_path)

    return icechunk_store_path / fname
