# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import datetime
import json
import typing
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import obstore
import pydantic
from obstore.store import from_url as _obs_from_url
from pydantic import ConfigDict

from intake_virtual_icechunk.source._containers import VirtualChunkContainerModel
from intake_virtual_icechunk.utils import _filter_config_args, _path_to_url

if TYPE_CHECKING:
    from obstore.store import ObjectStore


class VirtualIcechunkCatalogModel(pydantic.BaseModel):
    """
    Pydantic model for a Virtual Icechunk catalog sidecar file.

    The sidecar JSON is a lightweight pointer to an Icechunk store together
    with catalog-level metadata.  All per-entry (dataset) metadata is stored
    in each Zarr group's ``.zattrs``, written by
    :class:`~intake_virtual_icechunk._build.IcechunkStoreBuilder`.

    Examples
    --------
    Save a catalog pointer:

    >>> model = VirtualIcechunkCatalogModel(
    ...     store='s3://my-bucket/my-catalog.icechunk',
    ...     virtual_chunk_model=virtual_chunk_model,
    ...     storage_options={'from_env': True},
    ...     description='My climate catalog',
    ... )
    >>> from obstore.store import from_url
    >>> model.save('my-catalog', store=from_url('file:///path/to/output'))

    Load it back:

    >>> model = VirtualIcechunkCatalogModel.load('/path/to/output/my-catalog.json')
    """

    id: str = ""
    version: pydantic.StrictStr = "1.0.0"
    store: pydantic.StrictStr
    virtual_chunk_model: VirtualChunkContainerModel | None = None
    description: pydantic.StrictStr | None = None
    title: pydantic.StrictStr | None = None
    last_updated: datetime.datetime | datetime.date | None = None
    storage_options: dict[str, typing.Any] = {}

    model_config = ConfigDict(validate_assignment=True)

    @classmethod
    def load(
        cls,
        json_file: str,
        storage_options: dict[str, typing.Any] | None = None,
    ) -> VirtualIcechunkCatalogModel:
        """
        Load a catalog model from a JSON sidecar file.

        Parameters
        ----------
        json_file : str
            Path or URL to the JSON sidecar file.
        storage_options : dict, optional
            obstore config kwargs for reading the JSON file itself (e.g. S3
            credentials).  These are independent of the catalog's own
            ``storage_options``, which are stored inside the JSON and used to
            open the Icechunk store.
        """
        storage_options = storage_options or {}
        parsed = urlparse(json_file)
        scheme = parsed.scheme
        if scheme in ("", "file") or (len(scheme) == 1 and scheme.isalpha()):
            p = Path(parsed.path if scheme == "file" else json_file)
            directory_url = _path_to_url(str(p.parent))
            filename = p.name
        else:
            directory_url, filename = json_file.rsplit("/", 1)
        obs_store = _obs_from_url(
            directory_url, config=_filter_config_args(storage_options)
        )
        content = obstore.get(obs_store, filename).bytes()
        return cls.model_validate(json.loads(bytes(content)))

    def save(
        self,
        name: str,
        *,
        store: ObjectStore,
        json_dump_kwargs: dict | None = None,
    ) -> None:
        """
        Save the catalog model to a JSON sidecar file.

        Parameters
        ----------
        name : str
            Stem of the output file. If it ends with '.json' it will be stripped
            and re-added to ensure we get a single `.json` ext, no matter what.
        store : ObjectStore
            An obstore store (e.g. ``S3Store``, ``LocalStore``) pointing at the
            directory into which the sidecar should be written.
        json_dump_kwargs : dict, optional
            Additional keyword arguments forwarded to :func:`json.dump`.
        """
        name = name.removesuffix(".json")

        data = self.model_dump().copy()
        data["id"] = name
        data["last_updated"] = datetime.datetime.now().isoformat()

        json_kwargs: dict[str, typing.Any] = {"indent": 2, "default": str}
        json_kwargs |= json_dump_kwargs or {}
        content = json.dumps(data, **json_kwargs).encode()

        path = f"{name}.json"
        obstore.put(store, path, content)

        print(f"Successfully wrote Virtual Icechunk catalog json file to: {path}")
