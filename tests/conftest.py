# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import uuid
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

import pytest
from dask.distributed import Client

from intake_virtual_icechunk.source import VirtualIcechunkStoreBuilder

client = Client(threads_per_worker=1)


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
def icechunk_localstore_path(esm_datastore_path, tmp_path_factory) -> Path:
    """
    Use a minimal icechunk store for testing. This needs to be rebuilt at the
    start of each test session, or virtualizarr will complain about manifests not
    being up to date.
    """

    cat_path = tmp_path_factory.mktemp("access-om2") / "icecat.icechunk"

    iscb = VirtualIcechunkStoreBuilder(
        esm_datastore_path=esm_datastore_path,
        icechunk_store_path=cat_path,
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


@dataclass
class CephStoreInfo:
    icecat_bucket_url: str
    icecat_prefix: str
    vcc_bucket_url: str
    vcc_prefix: str


@pytest.fixture(scope="session")
def icechunk_cephstore_info() -> Generator[CephStoreInfo, None, None]:
    hash_suffix = uuid.uuid4().hex

    ESM_DATASTORE_OPTS = {
        "storage_options": {
            "endpoint_url": "https://projects.pawsey.org.au",
            "anon": True,
        },
    }

    ICECHUNK_STORE_OPTS = {
        "endpoint_url": "https://projects.pawsey.org.au",
        "s3_compatible": True,
        "force_path_style": True,
        "anonymous": True,
    }

    ICECHUNK_STORAGE_OPTS = {
        "endpoint_url": "https://projects.pawsey.org.au",
        "force_path_style": True,
        "anonymous": True,
    }

    icsb = VirtualIcechunkStoreBuilder(
        esm_datastore_path="s3://intake-virtual-icechunk-om2-esm-ds-container/access-om2.json",
        esm_datastore_kwargs=ESM_DATASTORE_OPTS,
        icechunk_store_path=f"s3://intake-virtual-icechunk-store/icecat-{hash_suffix}",
        icechunk_store_options=ICECHUNK_STORE_OPTS,
        icechunk_storage_options=ICECHUNK_STORAGE_OPTS,
    )

    icsb.build()

    yield CephStoreInfo(
        icecat_bucket_url="s3://intake-virtual-icechunk-store/",
        icecat_prefix=f"icecat-{hash_suffix}",
        vcc_bucket_url=f"{icsb.source_url_prefix.split('/icecat-')[0]}",
        vcc_prefix=icsb.source_url_prefix.split("/")[-1],
    )

    # Teardown - we need to delete the store afterwards.
    # This store is publicly readable/writable, so we can just delete the objects
    # and leave the bucket there for next time.
    #
    # We'll do this via obstore for consistency with everything else, but we'll need
    # creds to do anything useful here. These will also be in the repo, and we'll have
    # a periodic cleanup job running on the bucket to make sure it doesn't get too
    # cluttered with old test stores.
    try:
        import os

        from dotenv import load_dotenv
        from obstore.store import ObjectStore, from_url

        load_dotenv()

        access_key = os.getenv("CEPH_ACCESS_KEY_ID")
        secret_key = os.getenv("CEPH_SECRET_ACCESS_KEY")

        s3_store: ObjectStore = from_url(
            "s3://intake-virtual-icechunk-store",
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
def catalog_json_path(icechunk_localstore_path) -> Path:
    from intake_virtual_icechunk.utils import _intake_cat_filename

    fname = _intake_cat_filename(icechunk_localstore_path)

    return icechunk_localstore_path / fname
