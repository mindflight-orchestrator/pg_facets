//! FacetingZigSearch - Rust client for the pg_facets extension.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_postgres::Client;

// ============================================================================
// Data types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub schema_name: String,
    pub document_table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchWithFacetsRequest {
    pub query: String,
    pub facets: Option<HashMap<String, String>>,
    pub content_column: Option<String>,
    pub limit: Option<i32>,
    pub offset: Option<i32>,
    pub min_score: Option<f64>,
    pub facet_limit: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub id: i64,
    pub content: String,
    pub bm25_score: f64,
    pub vector_score: f64,
    pub combined_score: f64,
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FacetValue {
    pub value: String,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FacetResult {
    pub facet_name: String,
    pub facet_id: i32,
    pub values: Vec<FacetValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchWithFacetsResponse {
    pub results: Vec<SearchResult>,
    pub facets: Vec<FacetResult>,
    pub total_found: i64,
    pub search_time: i32,
}

#[derive(Debug, Clone)]
pub struct FacetDefinition {
    pub column: String,
    pub facet_type: String,
    pub custom_name: Option<String>,
    pub buckets: Option<Vec<f64>>,
}

// ============================================================================
// Client
// ============================================================================

pub struct FacetingZigSearch {
    client: Client,
    config: Config,
}

impl FacetingZigSearch {
    pub fn new(client: Client, config: Config) -> Self {
        FacetingZigSearch { client, config }
    }

    pub async fn search_with_facets(
        &self,
        req: &SearchWithFacetsRequest,
    ) -> Result<SearchWithFacetsResponse, Box<dyn std::error::Error + Send + Sync>> {
        let limit = req.limit.unwrap_or(10);
        let content_col = req.content_column.as_deref().unwrap_or("content");
        let facet_limit = req.facet_limit.unwrap_or(5);
        let offset = req.offset.unwrap_or(0);
        let min_score = req.min_score.unwrap_or(0.0);

        let facets_json: Option<serde_json::Value> = req.facets.as_ref().map(|m| {
            serde_json::to_value(m).unwrap_or(serde_json::Value::Object(serde_json::Map::new()))
        });

        let row = self
            .client
            .query_one(
                r#"SELECT * FROM facets.search_documents_with_facets(
                    $1, $2, $3, $4, NULL, $5, 'metadata', 'created_at', 'updated_at',
                    $6, $7, $8, 0.5, $9, NULL
                )"#,
                &[
                    &self.config.schema_name,
                    &self.config.document_table,
                    &req.query,
                    &facets_json,
                    &content_col,
                    &limit,
                    &offset,
                    &min_score,
                    &facet_limit,
                ],
            )
            .await?;

        let results_raw: serde_json::Value = row.get(0);
        let facets_raw: serde_json::Value = row.get(1);
        let total_found: i64 = row.get(2);
        let search_time: i32 = row.get(3);

        let results: Vec<SearchResult> = serde_json::from_value(results_raw).unwrap_or_default();
        let facets: Vec<FacetResult> = serde_json::from_value(facets_raw).unwrap_or_default();

        Ok(SearchWithFacetsResponse {
            results,
            facets,
            total_found,
            search_time,
        })
    }

    pub async fn filter_documents_by_facets(
        &self,
        facets: &HashMap<String, String>,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
        if facets.is_empty() {
            return Ok(vec![]);
        }
        let rows = self
            .client
            .query(
                "SELECT * FROM facets.filter_documents_by_facets($1, $2, $3)",
                &[
                    &self.config.schema_name,
                    &serde_json::to_value(facets)?,
                    &self.config.document_table,
                ],
            )
            .await?;
        let ids: Vec<i64> = rows.iter().map(|r| r.get::<_, i64>(0)).collect();
        Ok(ids)
    }

    pub async fn filter_documents_by_facets_bitmap(
        &self,
        facets: &HashMap<String, String>,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        if facets.is_empty() {
            return Ok(None);
        }
        let row = self
            .client
            .query_one(
                "SELECT facets.filter_documents_by_facets_bitmap($1, $2, $3)::bytea",
                &[
                    &self.config.schema_name,
                    &serde_json::to_value(facets)?,
                    &self.config.document_table,
                ],
            )
            .await?;
        let bitmap: Option<Vec<u8>> = row.get(0);
        Ok(bitmap)
    }

    pub async fn get_bitmap_cardinality(
        &self,
        bitmap: Option<&[u8]>,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        if bitmap.is_none() || bitmap.unwrap().is_empty() {
            return Ok(0);
        }
        let row = self
            .client
            .query_one("SELECT rb_cardinality($1::roaringbitmap)", &[&bitmap])
            .await?;
        let card: i64 = row.get(0);
        Ok(card)
    }

    pub async fn hierarchical_facets_bitmap(
        &self,
        filter_bitmap: Option<&[u8]>,
        limit: i32,
    ) -> Result<Vec<FacetResult>, Box<dyn std::error::Error + Send + Sync>> {
        let limit = if limit <= 0 { 10 } else { limit };
        let table_name = format!("{}.{}", self.config.schema_name, self.config.document_table);
        let table_oid: i32 = self
            .client
            .query_one("SELECT $1::regclass::oid", &[&table_name])
            .await?
            .get(0);

        let row = self
            .client
            .query_one(
                "SELECT facets.hierarchical_facets_bitmap($1::oid, $2, $3::roaringbitmap)",
                &[&table_oid, &limit, &filter_bitmap],
            )
            .await?;
        let raw: serde_json::Value = row.get(0);
        let facets: Vec<FacetResult> = serde_json::from_value(raw).unwrap_or_default();
        Ok(facets)
    }

    pub async fn get_top_facet_values(
        &self,
        facet_names: Option<&[String]>,
        limit: i32,
    ) -> Result<Vec<FacetResult>, Box<dyn std::error::Error + Send + Sync>> {
        let limit = if limit <= 0 { 10 } else { limit };
        let table_name = format!("{}.{}", self.config.schema_name, self.config.document_table);

        let rows = self
            .client
            .query(
                "SELECT facet_name, facet_value, cardinality, facet_id FROM facets.top_values($1::regclass, $2, $3)",
                &[&table_name, &limit, &facet_names],
            )
            .await?;

        let mut facet_map: HashMap<String, FacetResult> = HashMap::new();
        for row in rows {
            let facet_name: String = row.get(0);
            let facet_value: String = row.get(1);
            let cardinality: i64 = row.get(2);
            let facet_id: i32 = row.get(3);
            facet_map
                .entry(facet_name.clone())
                .or_insert_with(|| FacetResult {
                    facet_name: facet_name.clone(),
                    facet_id,
                    values: vec![],
                })
                .values
                .push(FacetValue {
                    value: facet_value,
                    count: cardinality,
                });
        }
        Ok(facet_map.into_values().collect())
    }

    pub async fn merge_deltas(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let table_name = format!("{}.{}", self.config.schema_name, self.config.document_table);
        self.client
            .execute("SELECT facets.merge_deltas($1::regclass)", &[&table_name])
            .await?;
        Ok(())
    }

    pub async fn add_facet(
        &self,
        def: &FacetDefinition,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let table_name = format!("{}.{}", self.config.schema_name, self.config.document_table);
        let facet_sql = match def.facet_type.as_str() {
            "plain" => {
                if let Some(ref name) = def.custom_name {
                    format!("facets.plain_facet('{}', '{}')", def.column, name)
                } else {
                    format!("facets.plain_facet('{}')", def.column)
                }
            }
            "array" => format!("facets.array_facet('{}')", def.column),
            "bucket" => {
                let buckets = def
                    .buckets
                    .as_ref()
                    .map(|b| b.iter().map(|x| format!("{}", x)).collect::<Vec<_>>().join(", "))
                    .unwrap_or_else(|| "0".to_string());
                format!("facets.bucket_facet('{}', ARRAY[{}])", def.column, buckets)
            }
            "boolean" => format!("facets.boolean_facet('{}')", def.column),
            _ => return Err(format!("unsupported facet type: {}", def.facet_type).into()),
        };
        let query = format!(
            "SELECT facets.add_facets('{}', ARRAY[{}])",
            table_name, facet_sql
        );
        self.client.execute(&query, &[]).await?;
        Ok(())
    }

    pub async fn drop_facet(
        &self,
        facet_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let table_name = format!("{}.{}", self.config.schema_name, self.config.document_table);
        self.client
            .execute(
                "SELECT facets.drop_facets($1::regclass, ARRAY[$2])",
                &[&table_name, &facet_name],
            )
            .await?;
        Ok(())
    }

    pub async fn index_document(
        &self,
        doc_id: i64,
        content: &str,
        language: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let table_name = format!("{}.{}", self.config.schema_name, self.config.document_table);
        let lang = if language.is_empty() { "english" } else { language };
        self.client
            .execute(
                "SELECT facets.bm25_index_document($1::regclass, $2, $3, 'content', $4)",
                &[&table_name, &doc_id, &content, &lang],
            )
            .await?;
        Ok(())
    }

    pub async fn delete_document(&self, doc_id: i64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let table_name = format!("{}.{}", self.config.schema_name, self.config.document_table);
        self.client
            .execute("SELECT facets.bm25_delete_document($1::regclass, $2)", &[&table_name, &doc_id])
            .await?;
        Ok(())
    }

    pub async fn bm25_search(
        &self,
        query: &str,
        language: &str,
        limit: i32,
    ) -> Result<Vec<(i64, f64)>, Box<dyn std::error::Error + Send + Sync>> {
        let table_name = format!("{}.{}", self.config.schema_name, self.config.document_table);
        let lang = if language.is_empty() { "english" } else { language };
        let limit = if limit <= 0 { 10 } else { limit };

        let rows = self
            .client
            .query(
                r#"SELECT doc_id, score FROM facets.bm25_search(
                    $1::regclass, $2, $3, false, false, 0.3, 1.2, 0.75, $4
                ) ORDER BY score DESC"#,
                &[&table_name, &query, &lang, &limit],
            )
            .await?;

        let results: Vec<(i64, f64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        Ok(results)
    }

    pub async fn recalculate_statistics(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let table_name = format!("{}.{}", self.config.schema_name, self.config.document_table);
        self.client
            .execute("SELECT facets.bm25_recalculate_statistics($1::regclass)", &[&table_name])
            .await?;
        Ok(())
    }
}
