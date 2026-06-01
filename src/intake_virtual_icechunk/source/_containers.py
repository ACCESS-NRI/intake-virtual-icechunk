from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass, field

import icechunk
from icechunk import ObjectStoreConfig, VirtualChunkContainer

from intake_virtual_icechunk.utils import _VCC_SAFE_KWARGS

STORE_TYPE_MAP: dict[str, Callable] = {
    "LocalStore": icechunk.local_filesystem_store,
    "PyObjectStoreConfig_LocalFileSystem": icechunk.local_filesystem_store,
    "S3Store": icechunk.s3_store,
    "S3CompatibleStore": icechunk.s3_store,  # S3-compatible (e.g. Ceph/Pawsey Acacia)
    "PyObjectStoreConfig_S3": icechunk.s3_store,
    "PyObjectStoreConfig_S3Compatible": icechunk.s3_store,
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
    """Serializable VirtualChunkContainer configuration for catalog sidecars.

    Icechunk requires virtual chunk access to be configured explicitly when a
    repository is reopened. This model stores the non-secret parts of that
    configuration in the catalog JSON sidecar so read-side consumers can
    reconstruct an equivalent ``VirtualChunkContainer`` later.

    Only explicitly safe kwargs are preserved in ``open_kwargs``; credential-like
    values are intentionally omitted from the serialised form.
    """

    url_prefix: str
    store_type: str
    open_kwargs: dict = field(default_factory=dict)

    @staticmethod
    def from_virtual_chunk_container(
        vc_container: VirtualChunkContainer,
        store_options: dict | None = None,
    ) -> VirtualChunkContainerModel:
        """Build a serialisable model from a live Icechunk container.

        Parameters
        ----------
        vc_container
            The configured Icechunk virtual chunk container.
        store_options
            Source-store options from the builder. Only keys listed in
            ``_VCC_SAFE_KWARGS`` are retained in the serialised output.
        """
        # Filter to only non-credential, serialisable keys so that config such
        # as a custom endpoint URL survives a round-trip through the JSON sidecar
        # without storing secrets.
        safe_kwargs = {
            k: v for k, v in (store_options or {}).items() if k in _VCC_SAFE_KWARGS
        }
        return VirtualChunkContainerModel(
            url_prefix=vc_container.url_prefix,
            store_type=type(vc_container.store).__name__,
            open_kwargs=safe_kwargs,
        )

    def to_virtual_chunk_container(self) -> VirtualChunkContainer:
        """Recreate an Icechunk ``VirtualChunkContainer`` from this model."""
        return VirtualChunkContainer(
            url_prefix=self.url_prefix,
            store=self._build_object_store_config(),  # type: ignore
        )

    def _build_object_store_config(self) -> ObjectStoreConfig:
        """Recreate the Icechunk object-store config for this container model."""
        store_type = STORE_TYPE_MAP.get(self.store_type, None)

        if store_type is None:
            raise ValueError(f"Unsupported store type: {self.store_type!r}")
        if store_type is icechunk.local_filesystem_store:
            return store_type(self.url_prefix, **self.open_kwargs)
        return store_type(**self.open_kwargs)

    def to_dict(self) -> dict:
        """Return a plain dictionary suitable for JSON serialisation."""
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict | None) -> VirtualChunkContainerModel | None:
        """Construct the model from a dictionary, typically decoded from JSON.

        Returns ``None`` if *d* is ``None``.
        """
        if d is None:
            return None
        return cls(**d)
