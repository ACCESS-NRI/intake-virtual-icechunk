# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from importlib.metadata import version

from .cat import VirtualIcechunkCatalogModel
from .core import IcechunkCatalog

__version__ = version("intake_virtual_icechunk")

__all__ = [
    "IcechunkCatalog",
    "VirtualIcechunkCatalogModel",
]
