"""
Tests for roundtripping a VirtualChunkContainer through the VirtualChunkContainerModel.
This ensures that we can serialise the necessary information about the VirtualChunkContainer
in the catalog JSON, and then reconstitute the same container when we open the store later on.
"""

import icechunk
import pytest

from intake_virtual_icechunk.source import VirtualChunkContainerModel


class StoreTests:
    """
    Base class / mixin that we can use to ensure that each store type that we are
    aiming to support is properly tested.
    """

    def test_roundtrip(self, sample_data):
        raise NotImplementedError(f"Not implemented yet for {type(self).__name__}")

    def test_from_virtual_chunk_container(self, *args, **kwargs):
        raise NotImplementedError(f"Not implemented yet for {type(self).__name__}")

    def test_to_virtual_chunk_container(self, *args, **kwargs):
        raise NotImplementedError(f"Not implemented yet for {type(self).__name__}")

    def test__build_object_store_config(self, *args, **kwargs):
        raise NotImplementedError(f"Not implemented yet for {type(self).__name__}")

    def test_to_dict(self, *args, **kwargs):
        raise NotImplementedError(f"Not implemented yet for {type(self).__name__}")

    def test_from_dict(self, *args, **kwargs):
        raise NotImplementedError(f"Not implemented yet for {type(self).__name__}")


class TestLocalFilesystemStore(StoreTests):
    # STORE = icechunk.local_filesystem_store
    # ^ Doing this causes some weird argument number error, that I'll figure out later.

    @pytest.fixture(scope="class")
    def canonical_vc_container(self, sample_data) -> icechunk.VirtualChunkContainer:
        sample_prefix = f"file://{sample_data}/"
        return icechunk.VirtualChunkContainer(
            url_prefix=sample_prefix,
            store=icechunk.local_filesystem_store(sample_prefix),
        )

    @pytest.fixture(scope="class")
    def sample_prefix(self, sample_data) -> str:
        return f"file://{sample_data}/"

    def test_roundtrip(self, sample_prefix):
        """
        First things first, create a VirtualChunkContainerModel from a real
        VirtualChunkContainer with a local filesystem store, befoer we go back to
        dealing with test data
        """

        # sample_prefix = "file:///Volumes/T7/netcdfs/1deg_jra55_iaf_omip2spunup_cycle31/"
        config = icechunk.RepositoryConfig.default()

        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(
                url_prefix=sample_prefix,
                store=icechunk.local_filesystem_store(sample_prefix),
            )
        )

        vc_container = config.get_virtual_chunk_container(sample_prefix)

        config_model = VirtualChunkContainerModel.from_virtual_chunk_container(
            vc_container
        )

        reconst_vc_container = config_model.to_virtual_chunk_container()

        # Probably redundant to do both assertions here, but just to be on the safe side
        for attr in ["url_prefix", "name", "store"]:
            assert getattr(vc_container, attr) == getattr(reconst_vc_container, attr)

        assert reconst_vc_container == vc_container

    def test_to_dict(self, sample_prefix, canonical_vc_container):
        """
        First things first, create a VirtualChunkContainerModel from a real
        VirtualChunkContainer with a local filesystem store, befoer we go back to
        dealing with test data
        """

        vc_container = canonical_vc_container

        config_model = VirtualChunkContainerModel.from_virtual_chunk_container(
            vc_container
        )

        dict_repr = config_model.to_dict()

        assert dict_repr == {
            "url_prefix": sample_prefix,
            "store_type": "PyObjectStoreConfig_LocalFileSystem",
            "open_kwargs": {},
        }

    def test_from_dict(self, sample_prefix, canonical_vc_container):
        dict_repr = {
            "url_prefix": sample_prefix,
            "store_type": "PyObjectStoreConfig_LocalFileSystem",
            "open_kwargs": {},
        }
        fromdict_config_model = VirtualChunkContainerModel.from_dict(dict_repr)

        config_model = VirtualChunkContainerModel.from_virtual_chunk_container(
            canonical_vc_container
        )

        assert fromdict_config_model == config_model

    def test_from_virtual_chunk_container(self, canonical_vc_container):
        config_model = VirtualChunkContainerModel.from_virtual_chunk_container(
            canonical_vc_container
        )

        assert config_model.store_type == "PyObjectStoreConfig_LocalFileSystem"
        assert config_model.open_kwargs == {}

    def test_to_virtual_chunk_container(self, sample_prefix, canonical_vc_container):
        config_model = VirtualChunkContainerModel(
            url_prefix=sample_prefix,
            store_type="LocalStore",
            open_kwargs={},
        )
        vc_container = config_model.to_virtual_chunk_container()

        assert vc_container == canonical_vc_container

    def test__build_object_store_config(self, sample_prefix):
        config_model = VirtualChunkContainerModel(
            url_prefix=sample_prefix,
            store_type="LocalStore",
            open_kwargs={},
        )

        object_store_config = config_model._build_object_store_config()

        expected_object_store_config = icechunk.local_filesystem_store(sample_prefix)

        assert object_store_config == expected_object_store_config
