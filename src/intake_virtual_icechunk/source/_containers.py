from collections.abc import Callable
from dataclasses import asdict, dataclass, field

import icechunk
from icechunk import ObjectStoreConfig, VirtualChunkContainer

STORE_TYPE_MAP: dict[str, Callable] = {
    "LocalStore": icechunk.local_filesystem_store,
    "PyObjectStoreConfig_LocalFileSystem": icechunk.local_filesystem_store,
    "S3Store": icechunk.s3_store,
    "GCSStore": icechunk.gcs_store,
    # "AzureBlobStore": icechunk.azure_store,
    # ^ Doesn't appear to be an icechunk.azure_store storage config builder yet
}

STORE_TYPE_INVERSE_MAP = {
    icechunk.ObjectStoreConfig.LocalFileSystem: "LocalStore",
    icechunk.ObjectStoreConfig.S3: "S3Store",  # Untested...
    icechunk.ObjectStoreConfig.Gcs: "GCSStore",  # Untested...
    # icechunk.ObjectStoreConfig.Azure: "AzureBlobStore",  # Untested...
    icechunk.ObjectStoreConfig.S3Compatible: "S3CompatibleStore",  # Untested...
    icechunk.ObjectStoreConfig.Http: "HttpStore",  # Untested...
}

_store_types = [
    icechunk.ObjectStoreConfig.LocalFileSystem,
    icechunk.ObjectStoreConfig.S3,
    icechunk.ObjectStoreConfig.Gcs,
    icechunk.ObjectStoreConfig.Azure,
    icechunk.ObjectStoreConfig.S3Compatible,
    icechunk.ObjectStoreConfig.Http,
]


# TODO: Probably replace this with a pydantic model down the line...
@dataclass
class VirtualChunkContainerModel:
    """
    A dataclass representing the configuration of an IceChunk VirtualChunkContainer,
    for the purpose of serialisation in the catalog JSON. This allows us to come
    back and reinstantiate the same VirtualChunkContainer when we open the store later,
    without having to set the virtual chunk configuration again - which the safe
    by default behaviour of IceChunk would otherwise require.

    This *is* a hack, in some sense. It is unsafe by default, but we are writing
    Python here, not Rust. The aim is to make it easy for a user to use a virtualised
    icechunk store without having to understand or even know about virtualisation
    or whatnot.
    """

    url_prefix: str
    store_type: str
    open_kwargs: dict = field(default_factory=dict)

    @staticmethod
    def from_virtual_chunk_container(
        vc_container: VirtualChunkContainer,
    ) -> "VirtualChunkContainerModel":
        return VirtualChunkContainerModel(
            url_prefix=vc_container.url_prefix,
            store_type=type(vc_container.store).__name__,
            # open_kwargs=vc_container.store.open_kwargs, # Not right, doesn't matter until we start handling remoe stores
        )

    def to_virtual_chunk_container(self) -> VirtualChunkContainer:
        """
        Create an IceChunk VirtualChunkContainer from this model. Note - the
        mypy type: ignore below is a typing issue in icechunk, really. Maybe push
        a fix?
        """
        return VirtualChunkContainer(
            url_prefix=self.url_prefix,
            store=self._build_object_store_config(),  # type: ignore
        )

    def _build_object_store_config(self) -> ObjectStoreConfig:
        """
        Recreate the icechunk ObjectStoreConfig from the store_type and open_kwargs.
        """
        store_type = STORE_TYPE_MAP.get(self.store_type, None)

        if store_type is None:
            raise ValueError(f"Unsupported store type: {store_type!r}")
        return store_type(self.url_prefix, **self.open_kwargs)

    def to_dict(self) -> dict:
        """
        Dump the model to a dictionary for serialisation as JSON.
        """
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "VirtualChunkContainerModel":
        """
        Create a VirtualChunkContainerModel from a dictionary (e.g. from JSON).
        """
        return cls(**d)
