from __future__ import annotations

# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pandas as pd
import xarray as xr

if TYPE_CHECKING:
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


class GroupEntryError(IcechunkBuildError):
    """Raised when a builder entry is missing data required by a build path."""

    pass


@dataclass
class DataStoreStructure:
    """Minimal structural metadata extracted from an intake-esm catalog.

    Attributes
    ----------
    groupby_attrs
        Columns that define each logical dataset grouping.
    assets_col
        Column containing the asset URLs or paths for each grouped dataset.
    """

    groupby_attrs: list[str]
    assets_col: str


@dataclass
class GroupEntry:
    """One logical dataset-group entry consumed by a builder.

    The current intake-esm path can populate all fields, but later sources may
    only be able to supply a subset. Builder paths should therefore request the
    specific payload they need via the helper methods below rather than reaching
    directly into the raw attributes.
    """

    public_key: str
    group_attrs: dict[str, Any]
    xarray_kwargs: dict = field(default_factory=dict)
    metadata_df: pd.DataFrame | None = None
    source_file_paths: list[str] | None = None

    @classmethod
    def from_esm_group(
        cls,
        *,
        public_key: str,
        group_df: pd.DataFrame,
        groupby_attrs: list[str],
        assets_col: str,
        xarray_kwargs: dict = {},
    ) -> GroupEntry:
        """Construct a builder entry from one grouped intake-esm dataframe slice."""

        group_attrs = {
            attr: group_df[attr].iloc[0]
            for attr in groupby_attrs
            if attr in group_df.columns
        }
        file_paths: list[str] = group_df[assets_col].tolist()
        return cls(
            public_key=public_key,
            group_attrs=group_attrs,
            metadata_df=group_df,
            source_file_paths=file_paths,
            xarray_kwargs=xarray_kwargs,
        )

    @property
    def has_metadata_df(self) -> bool:
        """Return whether this entry includes rich per-asset metadata rows."""

        return self.metadata_df is not None

    @property
    def group_df(self) -> pd.DataFrame:
        """Return the metadata dataframe required by catalog-shaped builder paths."""

        if self.metadata_df is None:
            raise GroupEntryError(
                "Group entry "
                f"'{self.public_key}' does not include a metadata dataframe."
            )
        return self.metadata_df

    @property
    def file_paths(self) -> list[str]:
        """Return the source paths required by source-asset builder paths."""

        if not self.source_file_paths:
            raise GroupEntryError(
                f"Group entry '{self.public_key}' does not include source file paths."
            )
        return self.source_file_paths


def filter_kwargs(kwargs: dict) -> dict:
    """Filter out mfdataset specific kwargs that would cause the single-dataset open to fail"""
    return {
        k: v
        for k, v in kwargs.items()
        if k
        not in [
            "parallel",
            "coords",
            "compat",
            "combine_attrs",
            "join",
            "concat_dim",
        ]
    }


def infer_loadable_vars(entry: GroupEntry) -> list[str]:
    """Infer variables to load based on the group dataframe.

    Virtualizarr typically falls over if 1D variables are not opened, as xarray
    will generate an error message something like:
    ```python
    Every dimension requires a corresponding 1D coordinate and index for inferring
    concatenation order but the coordinate 'model_theta_level_number' has no
    corresponding index
    ````

    To get around this, we'll load the first file in the dataset, and grab any
    one dimensional coordinates. This should hopefully be enough...
    """

    dummy_file = entry.file_paths[0]

    # Don't pass any kwargs for now... might have to change that though.
    with xr.open_dataset(dummy_file) as ds:
        loadable_vars = [d for d in ds.coords if ds[d].ndim == 1]

    return loadable_vars  # type: ignore[return-value]
