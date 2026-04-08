# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from urllib.parse import urlparse


def _resolve_storage(store: str, storage_options: dict):
    """
    Resolve a store path/URL to an ``icechunk.Storage`` object.

    Parameters
    ----------
    store : str
        The store URI.  Supported schemes:

        * Local path (including Windows paths such as ``C:/path/...``)
          or ``file://...`` → local filesystem storage.
        * ``s3://bucket/prefix`` → AWS S3 (or compatible) storage.
        * ``gs://bucket/prefix`` or ``gcs://bucket/prefix`` → Google Cloud Storage.
        * ``az://container/prefix`` → Azure Blob Storage.  The ``account``
          name must be supplied via *storage_options*.

    storage_options : dict
        Credential/config keyword arguments forwarded to the appropriate
        icechunk storage factory.  For example ``{'from_env': True}`` for S3
        or ``{'account': 'myaccount', 'from_env': True}`` for Azure.

    Returns
    -------
    icechunk.Storage
    """
    import icechunk

    parsed = urlparse(store)
    scheme = parsed.scheme

    # On Windows, urlparse("C:/path/...") sets scheme="c" (the drive letter).
    # Treat single-character schemes as local paths.
    if scheme in ("", "file") or (len(scheme) == 1 and scheme.isalpha()):
        path = parsed.path if scheme == "file" else store
        return icechunk.local_filesystem_storage(path)

    elif scheme == "s3":
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        return icechunk.s3_storage(bucket=bucket, prefix=prefix, **storage_options)

    elif scheme in ("gs", "gcs"):
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        return icechunk.gcs_storage(bucket=bucket, prefix=prefix, **storage_options)

    elif scheme in ("az", "abfs"):
        container = parsed.netloc
        prefix = parsed.path.lstrip("/")
        return icechunk.azure_storage(
            container=container, prefix=prefix, **storage_options
        )

    else:
        raise ValueError(
            f"Unsupported store scheme: {scheme!r}. "
            "Expected a local path or one of s3://, gs://, gcs://, az://."
        )
