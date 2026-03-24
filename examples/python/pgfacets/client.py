"""
FacetingZigSearch - Python client for the pg_facets PostgreSQL extension.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class Config:
    schema_name: str
    document_table: str


@dataclass
class SearchWithFacetsRequest:
    query: str
    facets: Optional[dict[str, str]] = None
    content_column: str = "content"
    limit: int = 10
    offset: int = 0
    min_score: float = 0.0
    facet_limit: int = 5


@dataclass
class SearchResult:
    id: int
    content: str
    bm25_score: float
    vector_score: float
    combined_score: float
    metadata: Optional[dict[str, Any]] = None


@dataclass
class FacetValue:
    value: str
    count: int


@dataclass
class FacetResult:
    facet_name: str
    facet_id: int
    values: list[FacetValue]


@dataclass
class SearchWithFacetsResponse:
    results: list[SearchResult]
    facets: list[FacetResult]
    total_found: int
    search_time: int


@dataclass
class FacetDefinition:
    column: str
    facet_type: str  # plain, array, bucket, boolean
    custom_name: Optional[str] = None
    buckets: Optional[list[float]] = None


class FacetingZigSearch:
    """Python client for pg_facets extension."""

    def __init__(self, conn, config: Config) -> None:
        self._conn = conn
        self.config = config

    def _cursor(self):
        return self._conn.cursor()

    def search_with_facets(
        self,
        req: SearchWithFacetsRequest,
    ) -> SearchWithFacetsResponse:
        """Full-text + facet search."""
        limit = req.limit or 10
        content_col = req.content_column or "content"
        facet_limit = req.facet_limit or 5

        with self._cursor() as cur:
            cur.execute(
                r"""SELECT * FROM facets.search_documents_with_facets(
                    %s, %s, %s, %s, NULL, %s, 'metadata', 'created_at', 'updated_at',
                    %s, %s, %s, 0.5, %s, NULL
                )""",
                (
                    self.config.schema_name,
                    self.config.document_table,
                    req.query,
                    json.dumps(req.facets) if req.facets else None,
                    content_col,
                    limit,
                    req.offset,
                    req.min_score,
                    facet_limit,
                ),
            )
            row = cur.fetchone()
            results_raw = row[0]
            facets_raw = row[1]
            total_found = row[2]
            search_time = row[3]

        results_data = results_raw if isinstance(results_raw, list) else json.loads(results_raw or "[]")
        facets_data = facets_raw if isinstance(facets_raw, list) else json.loads(facets_raw or "[]")

        results = [
            SearchResult(
                id=r.get("id", 0),
                content=r.get("content", ""),
                bm25_score=r.get("bm25_score", 0),
                vector_score=r.get("vector_score", 0),
                combined_score=r.get("combined_score", 0),
                metadata=r.get("metadata"),
            )
            for r in results_data
        ]

        facets = []
        for f in facets_data:
            values = [
                FacetValue(value=v.get("value", ""), count=v.get("count", 0))
                for v in f.get("values", [])
            ]
            facets.append(
                FacetResult(
                    facet_name=f.get("facet_name", ""),
                    facet_id=f.get("facet_id", 0),
                    values=values,
                )
            )

        return SearchWithFacetsResponse(
            results=results,
            facets=facets,
            total_found=total_found or 0,
            search_time=search_time or 0,
        )

    def filter_documents_by_facets(
        self,
        facets: dict[str, str],
    ) -> list[int]:
        """Get document IDs matching facet filters."""
        if not facets:
            return []
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM facets.filter_documents_by_facets(%s, %s::jsonb, %s)",
                (self.config.schema_name, json.dumps(facets), self.config.document_table),
            )
            return [r[0] for r in cur.fetchall()]

    def filter_documents_by_facets_bitmap(
        self,
        facets: dict[str, str],
    ) -> Optional[bytes]:
        """Get bitmap of matching document IDs."""
        if not facets:
            return None
        with self._cursor() as cur:
            cur.execute(
                "SELECT facets.filter_documents_by_facets_bitmap(%s, %s::jsonb, %s)::bytea",
                (self.config.schema_name, json.dumps(facets), self.config.document_table),
            )
            return cur.fetchone()[0]

    def get_bitmap_cardinality(self, bitmap: Optional[bytes]) -> int:
        """Get number of documents in a bitmap."""
        if not bitmap:
            return 0
        with self._cursor() as cur:
            cur.execute("SELECT rb_cardinality(%s::roaringbitmap)", (bitmap,))
            return cur.fetchone()[0] or 0

    def get_top_facet_values(
        self,
        facet_names: Optional[list[str]] = None,
        limit: int = 10,
    ) -> list[FacetResult]:
        """Get top N values for specified facets."""
        limit = limit or 10
        with self._cursor() as cur:
            cur.execute(
                "SELECT facet_name, facet_value, cardinality, facet_id FROM facets.top_values(%s::regclass, %s, %s)",
                (
                    f"{self.config.schema_name}.{self.config.document_table}",
                    limit,
                    facet_names,
                ),
            )
            rows = cur.fetchall()

        facet_map: dict[str, FacetResult] = {}
        for r in rows:
            name, value, card, fid = r
            if name not in facet_map:
                facet_map[name] = FacetResult(facet_name=name, facet_id=fid, values=[])
            facet_map[name].values.append(FacetValue(value=value, count=card))
        return list(facet_map.values())

    def merge_deltas(self) -> None:
        """Apply pending delta updates."""
        table = f"{self.config.schema_name}.{self.config.document_table}"
        with self._cursor() as cur:
            cur.execute("SELECT facets.merge_deltas(%s::regclass)", (table,))
        self._conn.commit()

    def add_facet(self, defn: FacetDefinition) -> None:
        """Add facet definition."""
        table = f"{self.config.schema_name}.{self.config.document_table}"
        if defn.facet_type == "plain":
            facet_sql = f"facets.plain_facet('{defn.column}')" if not defn.custom_name else f"facets.plain_facet('{defn.column}', '{defn.custom_name}')"
        elif defn.facet_type == "array":
            facet_sql = f"facets.array_facet('{defn.column}')"
        elif defn.facet_type == "bucket" and defn.buckets:
            buckets = ",".join(str(b) for b in defn.buckets)
            facet_sql = f"facets.bucket_facet('{defn.column}', ARRAY[{buckets}])"
        elif defn.facet_type == "boolean":
            facet_sql = f"facets.boolean_facet('{defn.column}')"
        else:
            raise ValueError(f"Unsupported facet type: {defn.facet_type}")
        with self._cursor() as cur:
            cur.execute(f"SELECT facets.add_facets('{table}', ARRAY[{facet_sql}])")
        self._conn.commit()

    def drop_facet(self, facet_name: str) -> None:
        """Remove facet."""
        table = f"{self.config.schema_name}.{self.config.document_table}"
        with self._cursor() as cur:
            cur.execute("SELECT facets.drop_facets(%s::regclass, %s)", (table, [facet_name]))
        self._conn.commit()

    def index_document(self, doc_id: int, content: str, language: str = "english") -> None:
        """Index document for BM25 search."""
        table = f"{self.config.schema_name}.{self.config.document_table}"
        with self._cursor() as cur:
            cur.execute(
                "SELECT facets.bm25_index_document(%s::regclass, %s, %s, 'content', %s)",
                (table, doc_id, content, language),
            )
        self._conn.commit()

    def delete_document(self, doc_id: int) -> None:
        """Remove document from BM25 index."""
        table = f"{self.config.schema_name}.{self.config.document_table}"
        with self._cursor() as cur:
            cur.execute("SELECT facets.bm25_delete_document(%s::regclass, %s)", (table, doc_id))
        self._conn.commit()

    def bm25_search(
        self,
        query: str,
        language: str = "english",
        limit: int = 10,
    ) -> list[tuple[int, float]]:
        """BM25 search. Returns list of (doc_id, score)."""
        limit = limit or 10
        table = f"{self.config.schema_name}.{self.config.document_table}"
        with self._cursor() as cur:
            cur.execute(
                r"""SELECT doc_id, score FROM facets.bm25_search(
                    %s::regclass, %s, %s, false, false, 0.3, 1.2, 0.75, %s
                ) ORDER BY score DESC""",
                (table, query, language, limit),
            )
            return [(r[0], r[1]) for r in cur.fetchall()]

    def recalculate_statistics(self) -> None:
        """Recalculate BM25 statistics."""
        table = f"{self.config.schema_name}.{self.config.document_table}"
        with self._cursor() as cur:
            cur.execute("SELECT facets.bm25_recalculate_statistics(%s::regclass)", (table,))
        self._conn.commit()
