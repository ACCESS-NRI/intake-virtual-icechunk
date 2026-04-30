from __future__ import annotations

from collections.abc import Mapping

# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0
from dataclasses import astuple, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import icechunk
import intake
import pandas as pd
import polars as pl
import zarr
from intake_esm.core import esm_datastore
from intake_esm.utils import MinimalExploder
from virtualizarr import open_virtual_dataset, open_virtual_mfdataset

from intake_virtual_icechunk.source._containers import VirtualChunkContainerModel
from intake_virtual_icechunk.utils import (
    _intake_cat_filename,
    _resolve_storage,
    _resolve_store,
)

if TYPE_CHECKING:
    from obspec_utils.registry import ObjectStoreRegistry
    from virtualizarr.parsers import (
        DMRPPParser,
        FITSParser,
        HDFParser,
        KerchunkJSONParser,
        KerchunkParquetParser,
        NetCDF3Parser,
        ZarrParser,
    )

    VirtualizarrParser = (
        type[DMRPPParser]
        | type[FITSParser]
        | type[HDFParser]
        | type[KerchunkJSONParser]
        | type[KerchunkParquetParser]
        | type[NetCDF3Parser]
        | type[ZarrParser]
    )


class IcechunkBuildError(Exception):
    """Custom exception for errors during the Icechunk store build process."""

    pass


class ParserInferenceError(IcechunkBuildError):
    """Raised when the parser cannot be inferred from the intake-esm catalog."""

    pass


@dataclass
class DataStoreStructure:
    groupby_attrs: list[str]
    assets_col: str


class IcechunkStoreBuilder:
    """Build a virtual Icechunk store from an existing intake-esm datastore.

    Given a pre-built intake-esm catalog, this builder iterates over every
    dataset group in the catalog, opens the constituent files with VirtualiZarr to create virtual references, and writes each dataset as a named Zarr
    *group* inside a single Icechunk store.  The result is one Icechunk store
    with one group per dataset, mirroring the logical structure of the
    intake-esm catalog.

    Each group's ``.zattrs`` is populated with the ``groupby_attrs`` values for
    that dataset, so that :class:`~intake_virtual_icechunk.core.IcechunkCatalog`
    can discover, list, and search entries without reading any data arrays.

    After the store is built, a lightweight JSON sidecar is written alongside
    the store so the catalog can be re-opened with
    :meth:`~intake_virtual_icechunk.core.IcechunkCatalog.from_json`.

    Icechunk session, store, and branch management is handled internally so
    callers need only supply paths.

    Warning: store_options are for the source data store, not the target Icechunk store.
    The target Icechunk store is always created with default options.
    If you need to customize the target store (e.g. for a non-file-based storage backend),
    please open an issue or submit a PR.

    Parameters
    ----------
    catalog_path : Path : str
        Path to an existing intake-esm catalog JSON file. Stored internally as a string.
    store_path : str
        Path or URI at which to create (or open) the Icechunk store.
        Supported schemes: local path, ``s3://``, ``gs://`` / ``gcs://``,
        ``az://``.
    parser : VirtualizarrParser, optional
        Optionally specify the VirtualiZarr parser to use when opening source. I
        not provided, the builder will attempt to infer the parser from the intake-esm
        catalog's ``assets.format`` field. See https://intake-esm.readthedocs.io/en/stable/reference/esm-catalog-spec
    storage_options : dict, optional
        Keyword arguments forwarded to the Icechunk storage backend. See _resolve_storage() for details.
    store_options: dict, optional

    drop_cols: list[str], optional
        List of column names in the intake-esm catalog's assets dataframe to ignore when attaching metadata.
        the `path` column is always ignored - drop cols allows you to remove other metadata columns that
        might no longer make sense, post virtualisation.
    cols_to_deiter: list[str], optional
        Intake-ESM supports columns with iterable values, which it represents as lists. Some of these will no
        longer make sense to keep as lists post virtualisation. This argument allows you to specify columns to
        transform from iterabls to scalars by taking the first value in each row, post deduplication.
    """

    def __init__(
        self,
        *,
        esm_datastore_path: Path | str,
        store_path: Path | str,
        esm_datastore_kwargs: dict | None = None,
        parser: VirtualizarrParser | None = None,
        storage_options: dict | None = None,
        store_options: dict | None = None,
        drop_cols: list[str] | None = None,
        cols_to_deiter: list[str] | None = None,
    ):
        self.esm_datastore_path = str(esm_datastore_path)
        self.esm_datastore_kwargs = esm_datastore_kwargs or {}

        self.store_path = str(store_path)
        self._esm_ds: esm_datastore | None = None

        self.storage_options = storage_options or {}
        self.store_options = store_options or {}
        self.drop_cols = drop_cols or []
        self.cols_to_deiter = cols_to_deiter or []

        parser = parser or self._infer_parser()
        self.parser = parser()

    @property
    def esm_ds(self) -> esm_datastore:
        """
        Use a property to lazily load the intake-esm datastore only when needed,
        and cache it on the builder instance. mostly so we can put all our optional
        arguments in `__init__` together and not have to worry about ordering.
        """

        if self._esm_ds is None:
            self._esm_ds = intake.open_esm_datastore(
                self.esm_datastore_path, **self.esm_datastore_kwargs
            )
        return self._esm_ds

    def _infer_parser(self) -> VirtualizarrParser:
        """
        We can infer the parser from the esm datastore, since it's specification
        contains it. We use this to determinte the parser.

        Warning: Calling this function *does not* set the builder's parser attribute.
        It also returns a type, not an instance.
        """
        from virtualizarr import parsers

        try:
            format = self.esm_ds.esmcat.assets.format.value  # type: ignore[union-attr]
        except AttributeError:
            raise ParserInferenceError(
                "Cannot infer parser from intake-esm catalog: "
                "the 'format' field in the catalog's 'assets' specification is missing. "
                "Only asserts with a specified 'format' can be built into an Icechunk store. "
                "See https://intake-esm.readthedocs.io/en/stable/reference/esm-catalog-spec.html#assets-object"
            )

        if format is None:
            raise ParserInferenceError(
                "Cannot infer parser from intake-esm catalog: "
                "the 'format' field in the catalog's 'assets' specification is missing."
                "Only asserts with a specified 'format' can be built into an Icechunk store."
                "See https://intake-esm.readthedocs.io/en/stable/reference/esm-catalog-spec.html#assets-object"
            )

        PARSER_MAP: dict[str, VirtualizarrParser] = {
            "netcdf": parsers.HDFParser,
            "zarr": parsers.ZarrParser,
            "zarr2": parsers.ZarrParser,
            "zarr3": parsers.ZarrParser,
            "reference": parsers.KerchunkJSONParser,
            # Don't know the best way to handle things that overlap in the format but have different parsers yet.
            # "todo_0": parsers.DMRPPParser,
            # "todo_1": parsers.FITSParser,
            # "todo_2": parsers.NetCDF3Parser,
            # "todo_4": parsers.KerchunkParquetParser,
        }

        try:
            return PARSER_MAP[format]
        except KeyError:
            raise ParserInferenceError(
                f"Unsupported parser format '{format}' specified in intake-esm catalog. "
                f"Supported formats are: {list(PARSER_MAP.keys())}."
            )

    def _create_registry(self) -> ObjectStoreRegistry:
        """
        Create an ObjectStoreRegistry to keep our source files in during the virtuualization
        process. We should be able to infer this from the esm_datastore's assets and our
        `_resolve_storage()` functionlity, determining a root & registry type.

        We also cache the registry on the builder instance.
        """

        path_column = self.esm_ds.esmcat.assets.column_name
        paths = self.esm_ds.esmcat.df[path_column].tolist()
        self.obsstore_registry, self.source_url_prefix = _resolve_store(
            paths, self.store_options
        )

        return self.obsstore_registry

    def _extract_datastore_structure(self) -> DataStoreStructure:
        """
        Grab the groupby_attrs and assets column name from the intake-esm catalog,
        which we use to build the icechunk store and populate .zattrs.
        We need these for both the build process and to populate the catalog metadata,
        """
        esmcat = self.esm_ds.esmcat
        agg_control = esmcat.aggregation_control
        groupby_attrs = (
            agg_control.groupby_attrs if agg_control else list(esmcat.df.columns)
        )
        assets_col = esmcat.assets.column_name
        return DataStoreStructure(groupby_attrs, assets_col)

    def build(self) -> None:
        """Build the Icechunk store.

        For each dataset group in the intake-esm catalog:

        1. Collects the asset file paths that belong to the group.
        2. Opens each file with VirtualiZarr to create virtual references.
        3. Combines and writes the virtual dataset as a named Zarr group in
           the Icechunk store.
        4. Writes the group's ``groupby_attrs`` values into ``.zattrs``.

        After all groups are written, a JSON sidecar is saved alongside the
        store for use with
        :meth:`~intake_virtual_icechunk.core.IcechunkCatalog.from_json`.

        The group name for each dataset is derived from the catalog's
        ``groupby_attrs``, so the Icechunk store structure mirrors the
        intake-esm grouping.
        """
        from intake_virtual_icechunk.cat import VirtualIcechunkCatalogModel

        esmcat = self.esm_ds.esmcat
        groupby_attrs, assets_col = astuple(self._extract_datastore_structure())

        _drop_cols = list(set(self.drop_cols + [assets_col]))
        self.drop_cols = _drop_cols
        # Resolve registry first so self.source_url_prefix is available for the
        # VirtualChunkContainer config below.
        self._create_registry()

        storage = _resolve_storage(self.store_path, self.storage_options)

        self.vc_container = icechunk.VirtualChunkContainer(
            url_prefix=self.source_url_prefix,
            store=icechunk.local_filesystem_store(self.source_url_prefix),
        )

        config = icechunk.RepositoryConfig.default()
        config.set_virtual_chunk_container(self.vc_container)

        credentials = icechunk.containers_credentials({self.source_url_prefix: None})
        repo = icechunk.Repository.create(storage, config, credentials)

        # Persist the configuration so we don't need to figure it out when we come
        # back to open the store
        repo.save_config()

        # Now, we need to persist the virtual chunk containers so that we can
        # reinstantiate them when we open the store later, to avoid the 'safe by
        # default' behaviour.
        # self.vc_containers

        # ------------------------------------------------------------------
        # 3. Build each group inside a single transaction
        # ------------------------------------------------------------------
        group_key_map = esmcat._construct_group_keys()
        self.failed_list: list[tuple[str, Exception]] = []

        with repo.transaction(
            "main", message=f"Build Virtual Icechunk catalog for {self.esm_ds.name}"
        ) as store:
            for public_key, internal_key in group_key_map.items():
                grouped = esmcat.grouped
                group_df: pd.DataFrame = grouped.get_group(internal_key)

                # Collect group-level metadata for .zattrs
                group_attrs: dict = {}
                for attr in groupby_attrs:
                    if attr in group_df.columns:
                        group_attrs[attr] = group_df[attr].iloc[0]
                # ^ Still necessary?

                # Collect asset file paths for this group
                file_paths: list[str] = group_df[assets_col].tolist()
                try:
                    with open_virtual_mfdataset(
                        urls=file_paths,
                        parser=self.parser,
                        registry=self.obsstore_registry,
                        parallel="dask",
                        decode_times=False,
                        coords="minimal",
                        compat="override",
                    ) as vds:
                        vds.vz.to_icechunk(store, group=public_key)

                    # Write group metadata into .zattrs so the catalog can search
                    # these groups without opening the arrays.
                    # We also want to attach metadata from the intake-esm catalog
                    # at this point. We don't need to attach things like variable
                    # names, because we can get those direct from the groups, but
                    # things like 'frequency', etc. will need to be included.

                    # To keep life simple, we'll just attach everything, and then
                    # compute variables, coordinates, dimensions etc. on the fly later
                    zarr_group = zarr.open_group(store, path=public_key, mode="a")
                    # Would make more sense to merge group_attrs and esm_ds_metadata
                    # first in a sensible way
                    self._attach_catalog_metadata(zarr_group, group_df, group_attrs)

                    print(f"Virtualised group {public_key} successfully!")
                except Exception as e:
                    if (
                        "Could not find any dimension coordinates to use to order the Dataset objects for concatenation"
                        not in str(e)
                    ):
                        self.failed_list.append((public_key, e))
                        print(f"Failed to virtualise group {public_key}: {e}")
                    else:
                        with open_virtual_dataset(
                            url=file_paths[0],
                            parser=self.parser,
                            registry=self.obsstore_registry,
                            decode_times=False,
                        ) as vds:
                            vds.vz.to_icechunk(store, group=public_key)
                        # Write group metadata into .zattrs so the catalog can search
                        # these groups without opening the arrays.
                        zarr_group = zarr.open_group(store, path=public_key, mode="a")
                        self._attach_catalog_metadata(zarr_group, group_df, group_attrs)

                        print(f"Virtualised group {public_key} successfully!")

                except Exception as e:
                    self.failed_list.append((public_key, e))
                    print(f"Failed to virtualise group {public_key}: {e}")

        # Write the JSON sidecar inside the store directory

        # Might not be safe on object stores - deal with that later.
        storepath = Path(self.store_path).expanduser()
        sidecar_fname = _intake_cat_filename(self.store_path)

        sidecar_dir = str(storepath)

        model = VirtualIcechunkCatalogModel(
            store=self.store_path,
            storage_options=self.storage_options,
            virtual_chunk_model=VirtualChunkContainerModel.from_virtual_chunk_container(
                self.vc_container
            ),
        )
        model.save(sidecar_fname, directory=sidecar_dir or None)

    def _attach_catalog_metadata(
        self,
        zarr_group: zarr.Group,
        group_df: pd.DataFrame,
        group_attrs: dict,
    ) -> None:
        """
        Attach relevant metadata from the intake-esm catalog to the zarr group as attributes.
        This is important for the catalog to be able to search and filter groups without
        having to open the arrays.

        For now, we'll just attach the groupby_attrs, but we could also consider attaching
        other metadata from the catalog if needed.
        """

        group_df = group_df.drop(columns=self.drop_cols, errors="ignore")

        exploded_metadata: Mapping[str, list[Any] | None] = (
            MinimalExploder(pl.from_pandas(group_df))()
            .unique()
            .to_dict(as_series=False)
        )

        # No None type until we deiter columns
        exploded_metadata = {
            k: [val for val in set(v) if val]  # type: ignore[arg-type]
            for k, v in exploded_metadata.items()
        }

        for col in self.cols_to_deiter:
            try:
                exploded_metadata[col] = exploded_metadata.get(col, [None])[0]  # type: ignore[index]
            except IndexError:
                exploded_metadata[col] = None

        # And remove any cols we want to drop
        exploded_metadata = {
            k: v for k, v in exploded_metadata.items() if k not in self.drop_cols
        }

        # Do exploded metadata first so it takes precedence over the groupby attrs,
        # which are more likely to have unimportant comments from NCO, etc.
        # TODO: Figure out if we can keep the order stable?

        group_attrs = {
            k: v
            for k, v in group_attrs.items()
            if k not in exploded_metadata and k not in self.drop_cols
        }

        zarr_group.attrs.update(exploded_metadata)
        zarr_group.attrs.update(group_attrs)

        for column in group_df.columns:
            if column not in zarr_group.attrs:
                zarr_group.attrs[column] = group_df[column].iloc[0]
