"""pgfacets - Python client for the pg_facets PostgreSQL extension."""

from .client import (
    Config,
    FacetDefinition,
    FacetResult,
    FacetValue,
    FacetingZigSearch,
    SearchResult,
    SearchWithFacetsRequest,
    SearchWithFacetsResponse,
)

__all__ = [
    "Config",
    "FacetDefinition",
    "FacetResult",
    "FacetValue",
    "FacetingZigSearch",
    "SearchResult",
    "SearchWithFacetsRequest",
    "SearchWithFacetsResponse",
]
