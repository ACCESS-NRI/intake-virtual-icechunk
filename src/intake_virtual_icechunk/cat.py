# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import datetime
import json
import os
import typing

import fsspec
import pydantic
from pydantic import ConfigDict


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
    ...     storage_options={'from_env': True},
    ...     description='My climate catalog',
    ... )
    >>> model.save('my-catalog', directory='/path/to/output')

    Load it back:

    >>> model = VirtualIcechunkCatalogModel.load('/path/to/output/my-catalog.json')
    """

    id: str = ""
    version: pydantic.StrictStr = "1.0.0"
    store: pydantic.StrictStr
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
            fsspec parameters for reading the JSON file itself (e.g. S3
            credentials).  These are independent of the catalog's own
            ``storage_options``, which are stored inside the JSON and used to
            open the Icechunk store.
        """
        storage_options = storage_options or {}
        with fsspec.open(json_file, **storage_options) as fobj:
            data = json.loads(fobj.read())
        return cls.model_validate(data)

    def save(
        self,
        name: str,
        *,
        directory: str | None = None,
        json_dump_kwargs: dict | None = None,
        storage_options: dict[str, typing.Any] | None = None,
    ) -> None:
        """
        Save the catalog model to a JSON sidecar file.

        Parameters
        ----------
        name : str
            Stem of the output file (without the ``.json`` extension).
        directory : str, optional
            Directory or cloud storage bucket to write the file to.
            Defaults to the current working directory.
        json_dump_kwargs : dict, optional
            Additional keyword arguments forwarded to :func:`json.dump`.
        storage_options : dict, optional
            fsspec parameters for *writing* the JSON file (e.g. S3 credentials
            for the sidecar file itself, independent of the catalog's own
            ``storage_options``).
        """
        if directory is None:
            directory = os.getcwd()

        storage_options = storage_options or {}
        mapper = fsspec.get_mapper(directory, **storage_options)
        fs = mapper.fs
        json_file_name = fs.unstrip_protocol(f"{mapper.root}/{name}.json")

        data = self.model_dump().copy()
        data["id"] = name
        data["last_updated"] = datetime.datetime.now().isoformat()

        with fs.open(json_file_name, "w") as outfile:
            json_kwargs: dict[str, typing.Any] = {"indent": 2, "default": str}
            json_kwargs |= json_dump_kwargs or {}
            json.dump(data, outfile, **json_kwargs)

        print(
            f"Successfully wrote Virtual Icechunk catalog json file to: {json_file_name}"
        )
