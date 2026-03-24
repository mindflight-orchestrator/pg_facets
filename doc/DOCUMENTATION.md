# pg_facets Documentation

**Current Version: 0.4.3** | **Last Updated: December 2024**

## Table of Contents

- [Vue d'ensemble](#vue-densemble)
- [JSON API Reference](#json-api-reference)
  - [Facet Response Schemas](#facet-response-schemas)
  - [Core Function Signatures](#core-function-signatures)
- [Quick Start Example](#quick-start-example)
- [API Reference](API_REFERENCE.md) - Complete function reference
- [Architecture](#architecture)
  - [Structure des fichiers](#structure-des-fichiers)
  - [Modules principaux](#modules-principaux)
- [API SQL](#api-sql)
- [Tests](#tests)
- [Client Go](#client-go)
- [Dependencies externes](#dependencies-externes)
- [Notes de performance](#notes-de-performance)
- [Limitations](#limitations)
- [Arguments en Faveur de la Recherche par Facettes](#arguments-en-faveur-de-la-recherche-par-facettes)
- [ACID Compliance and Transaction Safety](#acid-compliance-and-transaction-safety)
- [UNLOGGED Table Support](#unlogged-table-support)
- [pg_cron Integration](#pg_cron-integration)
- [Historique des Versions](#historique-des-versions)
- [Contribuer](#contribuer)

## Vue d'ensemble

`pg_facets` is a port of the PostgreSQL `pgfaceting` extension to the Zig language. Cette extension fournit des fonctionnalités de faceting (filtrage et comptage par facettes) pour PostgreSQL en utilisant des bitmaps Roaring pour des performances optimales.

### Objectifs

- **Performance** : Réimplémentation en Zig des parties critiques pour de meilleures performances
- **Compatibilité** : Maintien de l'API SQL existante pour une migration transparente
- **Maintenabilité** : Code plus sûr et plus lisible grâce aux garanties de sécurité de Zig

## JSON API Reference

### Facet Response Schemas

All facet-related functions return structured JSON data. Here are the complete schemas for frontend/Golang integration:

#### 1. Individual Facet Counts (`facet_counts` type)
```json
{
  "facet_name": "category",
  "facet_value": "electronics",
  "cardinality": 1250,
  "facet_id": 1
}
```

#### 2. Hierarchical Facets Response
```json
{
  "regular_facets": {
    "category": [
      {"facet_name": "category", "facet_value": "electronics", "cardinality": 1250, "facet_id": 1},
      {"facet_name": "category", "facet_value": "books", "cardinality": 890, "facet_id": 1}
    ],
    "brand": [
      {"facet_name": "brand", "facet_value": "samsung", "cardinality": 450, "facet_id": 2},
      {"facet_name": "brand", "facet_value": "apple", "cardinality": 380, "facet_id": 2}
    ]
  },
  "hierarchical_facets": {
    "category": {
      "electronics": {
        "count": 1250,
        "children": {
          "smartphones": {"count": 650},
          "laptops": {"count": 420},
          "tablets": {"count": 180}
        }
      },
      "books": {
        "count": 890,
        "children": {
          "fiction": {"count": 520},
          "non-fiction": {"count": 370}
        }
      }
    }
  }
}
```

#### 3. Search Results with Facets
```json
{
  "results": [
    {
      "document_id": 12345,
      "score": 0.85,
      "content": "High-performance laptop...",
      "metadata": {
        "title": "Gaming Laptop Pro",
        "category": "electronics",
        "price": 1299.99
      },
      "created_at": "2024-01-15T10:30:00Z",
      "updated_at": "2024-01-15T10:30:00Z"
    }
  ],
  "facets": {
    "regular_facets": {
      "category": [
        {"facet_name": "category", "facet_value": "electronics", "cardinality": 1250, "facet_id": 1},
        {"facet_name": "category", "facet_value": "books", "cardinality": 890, "facet_id": 1}
      ]
    },
    "hierarchical_facets": {
      "category": {
        "electronics": {
          "count": 1250,
          "children": {
            "smartphones": {"count": 650},
            "laptops": {"count": 420}
          }
        }
      }
    }
  },
  "total_found": 2500,
  "search_time": 45
}
```

#### 4. BM25 Search Results
```json
{
  "results": [
    {
      "doc_id": 12345,
      "score": 1.234
    },
    {
      "doc_id": 67890,
      "score": 0.987
    }
  ],
  "total_found": 150,
  "search_time": 23
}
```

### Core Function Signatures

#### Facet Operations
```sql
-- Get counts for a specific facet
SELECT * FROM facets.get_facet_counts(table_oid, 'facet_name', filter_bitmap, limit);

-- Get all facet counts (with optional filter)
SELECT * FROM facets.hierarchical_facets(table_oid, limit, filter_bitmap);
SELECT * FROM facets.hierarchical_facets_bitmap(table_oid, limit, filter_bitmap);

-- Filter documents by facets
SELECT facets.filter_documents_by_facets_bitmap(schema_name, facets_jsonb, table_name);
```

#### Search Operations
```sql
-- Combined search with facets and BM25/vector
SELECT * FROM facets.search_documents_with_facets(
    schema_name, table_name, query, facets_filter,
    vector_column, content_column, metadata_column,
    created_at_column, updated_at_column,
    limit, offset, min_score, vector_weight, facet_limit, language
);
```

#### BM25 Operations
```sql
-- Index operations
SELECT facets.bm25_index_document(table_regclass, doc_id, content, content_column, language);
SELECT facets.bm25_index_documents_batch(table_regclass, documents_jsonb, content_column, language);
SELECT facets.bm25_rebuild_index(table_regclass, id_column, content_column, language, workers);

-- Search operations
SELECT * FROM facets.bm25_search(table_regclass, query, language, prefix_match, fuzzy_match, fuzzy_threshold, k1, b, limit);
SELECT facets.bm25_score(table_regclass, query, doc_id, language, k1, b);

-- Analysis operations
SELECT * FROM facets.bm25_term_stats(table_oid, limit);
SELECT * FROM facets.bm25_doc_stats(table_oid, limit);
SELECT * FROM facets.bm25_collection_stats(table_oid);
```

## Quick Start Example

Here's a complete example of setting up pg_facets with BM25 search for an e-commerce product catalog:

### 1. Create Product Table
```sql
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    category TEXT,
    brand TEXT,
    price DECIMAL(10,2),
    in_stock BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Insert sample data
INSERT INTO products (title, description, category, brand, price) VALUES
    ('iPhone 15 Pro', 'Latest iPhone with advanced camera', 'smartphones', 'Apple', 999.99),
    ('Samsung Galaxy S24', 'Android flagship smartphone', 'smartphones', 'Samsung', 899.99),
    ('MacBook Pro 16"', 'Professional laptop for developers', 'laptops', 'Apple', 2499.99),
    ('Dell XPS 13', 'Ultrabook for business users', 'laptops', 'Dell', 1299.99);
```

### 2. One-Stop Setup (Recommended)
```sql
SELECT facets.setup_table_with_bm25(
    'public.products'::regclass,
    'id',                    -- id_column
    'title',                 -- content_column (or use concatenated content)
    ARRAY[
        facets.plain_facet('category'),
        facets.plain_facet('brand'),
        facets.boolean_facet('in_stock')
    ],                       -- facets
    'english',               -- language
    true,                    -- create_trigger
    NULL,                    -- chunk_bits (auto)
    true,                    -- populate_facets
    true,                    -- build_bm25_index
    0                        -- bm25_workers (auto)
);
```

### 3. Search with Facets
```sql
-- Search for "phone" in smartphones category
SELECT * FROM facets.search_documents_with_facets(
    'public',               -- schema
    'products',             -- table
    'phone',                -- BM25 query
    '{"category": "smartphones"}'::jsonb,  -- facet filters
    NULL,                   -- vector_column
    'title',                -- content_column
    NULL,                   -- metadata_column
    'created_at',           -- created_at_column
    'updated_at',           -- updated_at_column
    10,                     -- limit
    0,                      -- offset
    0.0,                    -- min_score
    NULL,                   -- vector_weight
    5                       -- facet_limit
);
```

**Response:**
```json
{
  "results": [
    {
      "document_id": 1,
      "score": 0.95,
      "content": "iPhone 15 Pro",
      "metadata": null,
      "created_at": "2024-01-15T10:00:00Z",
      "updated_at": "2024-01-15T10:00:00Z"
    },
    {
      "document_id": 2,
      "score": 0.87,
      "content": "Samsung Galaxy S24",
      "metadata": null,
      "created_at": "2024-01-15T10:05:00Z",
      "updated_at": "2024-01-15T10:05:00Z"
    }
  ],
  "facets": {
    "regular_facets": {
      "category": [
        {"facet_name": "category", "facet_value": "smartphones", "cardinality": 2, "facet_id": 1},
        {"facet_name": "category", "facet_value": "laptops", "cardinality": 2, "facet_id": 1}
      ],
      "brand": [
        {"facet_name": "brand", "facet_value": "Apple", "cardinality": 1, "facet_id": 2},
        {"facet_name": "brand", "facet_value": "Samsung", "cardinality": 1, "facet_id": 2}
      ]
    }
  },
  "total_found": 2,
  "search_time": 5
}
```

### 4. Get Facets Only (Browse Mode)
```sql
-- Get all facets without search query
SELECT * FROM facets.hierarchical_facets_bitmap(
    'public.products'::regclass::oid,
    10,     -- limit per facet
    NULL    -- no filter (all products)
);
```

**Response:**
```json
{
  "regular_facets": {
    "category": [
      {"facet_name": "category", "facet_value": "smartphones", "cardinality": 2, "facet_id": 1},
      {"facet_name": "category", "facet_value": "laptops", "cardinality": 2, "facet_id": 1}
    ],
    "brand": [
      {"facet_name": "brand", "facet_value": "Apple", "cardinality": 2, "facet_id": 2},
      {"facet_name": "brand", "facet_value": "Samsung", "cardinality": 1, "facet_id": 2},
      {"facet_name": "brand", "facet_value": "Dell", "cardinality": 1, "facet_id": 2}
    ]
  }
}
```

## Architecture

### Structure des fichiers

```
pgfaceting_zig/
├── src/
│   ├── main.zig          # Point d'entrée, exports PostgreSQL
│   ├── utils.zig         # Utilitaires et helpers C-interop
│   ├── deltas.zig        # Gestion des deltas (ajouts/suppressions)
│   ├── filters.zig       # Construction de filtres bitmap
│   ├── facets.zig        # Comptage des facettes
│   ├── search.zig        # Recherche de documents
│   ├── filter_helper.c   # Helpers C pour macros PostgreSQL
│   └── test_utils.zig    # Utilitaires de test
├── build.zig            # Script de build Zig
├── sql/                  # Scripts SQL d'installation
└── test/                 # Tests SQL
```

## Modules principaux

### 1. `main.zig` - Point d'entrée

**Rôle** : Exporte les fonctions natives pour PostgreSQL et définit le module magic.

**Fonctions exportées** :
- `merge_deltas_native` : Fusion des deltas de facettes
- `build_filter_bitmap_native` : Construction de bitmap de filtres
- `get_facet_counts_native` : Comptage des facettes
- `search_documents_native` : Recherche de documents
- `bm25_index_worker_native` : Worker d'indexation BM25 parallèle haute performance
- `test_tokenize_only` : Test de tokenization (debug)
- `bm25_term_stats` : Statistiques des termes (équivalent `ts_stat`)
- `bm25_doc_stats` : Statistiques des documents
- `bm25_collection_stats` : Statistiques de collection
- `bm25_explain_doc` : Analyse des poids BM25 par terme dans un document

**Points clés** :
- `Pg_magic_func` : Structure de compatibilité PostgreSQL (version 13+)
- `pg_finfo_*` : Fonctions d'information pour chaque fonction native
- Utilisation de `callconv(.C)` pour la compatibilité C

### 2. `utils.zig` - Utilitaires et C-interop

**Rôle** : Fournit des helpers pour l'interopérabilité C-Zig et la gestion mémoire.

#### Allocateur PostgreSQL

```zig
pub const PgAllocator = struct {
    // Utilise palloc/pfree de PostgreSQL pour la gestion mémoire
    // Compatible avec l'allocateur standard de Zig
}
```

#### Helpers pour FunctionCallInfo

PostgreSQL utilise un tableau flexible (`args`) dans `FunctionCallInfo` que Zig ne traduit pas toujours correctement. Au lieu d'utiliser un offset hardcodé (fragile entre versions/compilateurs), on expose deux helpers C compilés contre les headers PostgreSQL, puis on les wrappe côté Zig :

```zig
pub fn get_arg_datum(fcinfo: c.FunctionCallInfo, n: usize) c.Datum
pub fn is_arg_null(fcinfo: c.FunctionCallInfo, n: usize) bool
```

Ces wrappers appellent des helpers C (`fcinfo_get_arg_value_helper`, `fcinfo_get_arg_isnull_helper`) qui lisent `fcinfo->args[n]` de façon sûre.

#### Wrappers pour macros PostgreSQL

Les macros C complexes sont encapsulées dans des helpers C :

- `detoast_datum` : Détostage des données PostgreSQL
- `varsize`, `vardata`, `varhdrsz` : Accès aux données varlena
- `set_varsize` : Définition de la taille varlena
- `isA`, `tReturnSetInfo` : Vérification de types de nœuds
- `elog` : Journalisation (macro variadic)
- `workMem` : Accès à la variable globale `work_mem`

### 3. `deltas.zig` - Gestion des deltas

**Rôle** : Traite les modifications (ajouts/suppressions) de facettes.

**Fonction principale** : `merge_deltas_native`

**Algorithme** :
1. Récupération des informations de table via SPI
2. Lecture des deltas depuis la base de données
3. Pour chaque delta :
   - Désérialisation du bitmap Roaring
   - Application des modifications (ajout/suppression)
   - Mise à jour de la table `facets_table`
4. Retour du nombre de deltas traités

**Structures de données** :
- `DeltaMap` : HashMap pour regrouper les deltas par (facet_id, facet_value)

### 4. `filters.zig` - Construction de filtres

**Rôle** : Construit un bitmap combiné à partir d'un tableau de filtres de facettes.

**Fonction principale** : `build_filter_bitmap_native`

**Algorithme** :
1. Parsing du tableau `facet_filter[]` (via helper C)
2. Résolution des IDs de facettes depuis `facet_definition`
3. Pour chaque facette :
   - Récupération des bitmaps de posting lists
   - Union (OR) des bitmaps pour les valeurs multiples
4. Intersection (AND) entre les différentes facettes
5. Sérialisation et retour du bitmap final

**Helper C** : `extract_facet_filter_fields`
- Parse les types composites PostgreSQL `facet_filter`
- Évite les problèmes de macros C complexes dans Zig

**Structure** :
```zig
pub const FilterEntry = struct {
    facet_name: []const u8,
    facet_value: []const u8,
};
```

### 5. `facets.zig` - Comptage des facettes

**Rôle** : Calcule les cardinalités des facettes, optionnellement filtrées.

**Fonction principale** : `get_facet_counts_native`

**Algorithme** :
1. Récupération du bitmap de filtre (optionnel)
2. Récupération des facettes cibles (ou toutes si non spécifiées)
3. Pour chaque facette :
   - Récupération de la posting list
   - Application du filtre (intersection)
   - Comptage des éléments
4. Retour via `tuplestore` (set-returning function)

**Points clés** :
- Utilise `ReturnSetInfo` pour les fonctions retournant des sets
- `tuplestore` pour le retour de résultats
- Support de la pagination via `limit`

### 6. `search.zig` - Recherche de documents

**Rôle** : Recherche des documents correspondant aux filtres de facettes.

**Fonction principale** : `search_documents_native`

**Algorithme** :
1. Construction du bitmap de filtre (similaire à `filters.zig`)
2. Itération sur les éléments du bitmap
3. Retour des IDs de documents avec pagination (offset, limit)

**Points clés** :
- Utilise `roaring_uint32_iterator` pour itérer efficacement
- Support de la pagination
- Retour via `tuplestore`

## Helpers C (`filter_helper.c`)

### Pourquoi des helpers C ?

Zig ne peut pas traduire certaines macros C complexes de PostgreSQL :
- Macros variadic (`elog`, `ereport`)
- Macros avec opérateurs de concaténation (`IsA`)
- Accès à des structures avec tableaux flexibles
- Variables globales non exportées correctement

### Helpers disponibles

1. **`extract_facet_filter_fields`** : Parse les types composites `facet_filter`
2. **`detoast_datum_helper`** : Détostage des données
3. **`varsize_helper`, `vardata_helper`** : Accès aux données varlena
4. **`varhdrsz_helper`** : Constante VARHDRSZ
5. **`set_varsize_helper`** : Définition de la taille varlena
6. **`isa_helper`** : Vérification de type de nœud (évite la macro `IsA`)
7. **`t_returnsetinfo_helper`** : Constante `T_ReturnSetInfo`
8. **`elog_helper`** : Journalisation (évite la macro variadic)
9. **`work_mem_helper`** : Accès à la variable globale `work_mem`

## Patterns et conventions

### Gestion mémoire

- **Allocateur PostgreSQL** : Utilise `PgAllocator` qui wrappe `palloc`/`pfree`
- **Pas de gestion manuelle** : Zig gère automatiquement avec `defer`
- **Pas de fuites** : Tous les `alloc` ont un `defer free` correspondant

### Gestion d'erreurs

- **`unreachable`** : Pour les cas où l'erreur ne devrait jamais arriver
- **`elog(ERROR, ...)`** : Pour les erreurs fatales (arrêt de la transaction)
- **Retour de `null`** : Pour les cas d'erreur non-fatals

### Accès aux arguments

```zig
// ✅ Correct
const table_id = @intCast(utils.get_arg_datum(fcinfo, 0));
if (utils.is_arg_null(fcinfo, 1)) {
    return c.PointerGetDatum(null);
}

// ❌ Incorrect (ne fonctionne pas avec les tableaux flexibles)
const table_id = fcinfo.*.args[0].value;
```

### Casts et alignements

```zig
// Cast avec alignement explicite
const array = @as(*c.ArrayType, @alignCast(@ptrCast(c.DatumGetPointer(datum))));

// Cast de types numériques
const len = @intCast(text_len);  // c_int -> usize
const size = @as(usize, @intCast(utils.varhdrsz()));  // c_int -> usize
```

### Utilisation de SPI (Server Programming Interface)

```zig
if (c.SPI_connect() != c.SPI_OK_CONNECT) {
    utils.elog(c.ERROR, "SPI_connect failed");
}
defer _ = c.SPI_finish();

// Exécution de requête
if (c.SPI_execute(query.ptr, true, 1) != c.SPI_OK_SELECT) {
    utils.elog(c.ERROR, "Query failed");
}

// Accès aux résultats
const tuple = c.SPI_tuptable.*.vals[0];
const tupdesc = c.SPI_tuptable.*.tupdesc;
const value = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);
```

## Compilation

### Prérequis

- Zig 0.15.2+
- PostgreSQL 17+ (headers de développement)
- `pg_roaringbitmap` extension


### Build local

```bash
cd pgfaceting_zig
zig build -Doptimize=ReleaseFast
```

### Build dans Docker

```bash
docker build --target builder -f Dockerfile.zig -t pgfaceting-builder .
```

Le Dockerfile inclut :
- Installation de Zig
- Build de `pg_roaringbitmap`
- Build de `pgfacets_zig`

## API SQL

Les fonctions Zig sont exposées via des wrappers SQL dans `sql/pg_facets--0.4.0.sql` (et plus généralement `sql/pg_facets--*.sql`) :

### Fonctions Core

- `facets.merge_deltas(table_id, deltas)` → `merge_deltas_native`
- `facets.build_filter_bitmap(table_id, filters)` → `build_filter_bitmap_native`
- `facets.get_facet_counts(table_id, filter_bitmap, target_facets, limit)` → `get_facet_counts_native`
- `facets.search_documents_with_facets(...)` → Recherche avec facettes (optimisée bitmap en 0.3.6)

### Fonctions Bitmap (NEW in 0.3.6)

Ces fonctions évitent les "array explosions" avec des millions de documents :

| Fonction | Description | Avantage |
|----------|-------------|----------|
| `filter_documents_by_facets_bitmap(schema, facets_jsonb, table)` | Retourne un roaringbitmap au lieu d'un tableau | Évite les tableaux de 8M éléments |
| `hierarchical_facets_bitmap(table_oid, limit, filter_bitmap)` | Calcule les facettes directement depuis un bitmap | Pas de conversion bitmap→array→bitmap |
| `_get_regular_facets(table_oid, tdef, facet_names, bitmap, n)` | Helper interne pour les facettes non-hiérarchiques | Utilisé par hierarchical_facets_bitmap |

### Exemple d'Utilisation des Fonctions Bitmap

```sql
-- AVANT (problématique avec 8M documents):
SELECT * FROM facets.filter_documents_by_facets(
    'my_schema', '{"category": "Electronics"}'::jsonb, 'products'
);
-- Retourne: SETOF integer (peut être 8M lignes!)

-- APRÈS (efficace):
SELECT facets.filter_documents_by_facets_bitmap(
    'my_schema', '{"category": "Electronics"}'::jsonb, 'products'
);
-- Retourne: roaringbitmap (quelques MB compressés)

-- Obtenir le count sans matérialiser le tableau:
SELECT rb_cardinality(
    facets.filter_documents_by_facets_bitmap(
        'my_schema', '{"category": "Electronics"}'::jsonb, 'products'
    )
);
-- Retourne: 8000000 (instantané)

-- Obtenir les facettes directement depuis le bitmap:
SELECT facets.hierarchical_facets_bitmap(
    'my_schema.products'::regclass::oid,
    10,  -- limite par facette
    facets.filter_documents_by_facets_bitmap(
        'my_schema', '{"category": "Electronics"}'::jsonb, 'products'
    )
);
-- Retourne: JSONB avec les comptages de facettes
```

### Optimisation de `search_documents_with_facets` (0.3.6)

La fonction principale a été optimisée pour utiliser les bitmaps en interne :

```sql
-- Cette requête est maintenant optimisée automatiquement:
SELECT * FROM facets.search_documents_with_facets(
    'my_schema',           -- schema
    'products',            -- table
    '',                    -- query (vide = browse mode)
    '{"category": "Electronics"}'::jsonb,  -- facets filter
    NULL,                  -- vector_column
    'content',             -- content_column
    'metadata',            -- metadata_column
    'created_at',          -- created_at_column
    'updated_at',          -- updated_at_column
    20,                    -- limit
    0,                     -- offset
    NULL,                  -- min_score
    NULL,                  -- vector_weight
    1000                   -- facet_limit
);
```

**Optimisations internes (0.3.6):**

1. **Filtrage par bitmap**: Utilise `filter_documents_by_facets_bitmap` au lieu de `filter_documents_by_facets`
2. **Pas d'array explosion**: Le filtre reste sous forme de bitmap jusqu'au dernier moment
3. **rb_contains au lieu de ANY()**: Pour les requêtes texte, utilise `rb_contains(bitmap, id)` au lieu de `id = ANY(array)`
4. **Facettes depuis bitmap**: Appelle `hierarchical_facets_bitmap` directement avec le bitmap

### Fonctions BM25 (NEW in 0.4.0)

Les fonctions BM25 permettent la recherche full-text avec scoring de pertinence basé sur l'algorithme BM25 :

| Fonction | Description | Paramètres |
|----------|-------------|------------|
| `bm25_index_document(table_id, doc_id, content, content_column, language)` | Indexe un document pour la recherche BM25 | `table_id` (regclass), `doc_id` (bigint), `content` (text), `content_column` (text, défaut: 'content'), `language` (text, défaut: 'english') |
| `bm25_delete_document(table_id, doc_id)` | Supprime un document de l'index BM25 | `table_id` (regclass), `doc_id` (bigint) |
| `bm25_search(table_id, query, language, prefix_match, fuzzy_match, fuzzy_threshold, k1, b, limit)` | Recherche avec scoring BM25 | Retourne `TABLE(doc_id bigint, score float)` |
| `bm25_score(table_id, query, doc_id, language, k1, b)` | Calcule le score BM25 pour un document spécifique | Retourne `float` |
| `bm25_recalculate_statistics(table_id)` | Recalcule les statistiques de collection | `table_id` (regclass) |
| `bm25_get_statistics(table_id)` | Récupère les statistiques de collection | Retourne `TABLE(total_docs bigint, avg_length float)` |
| `bm25_term_stats(table_id, limit)` | Top N termes par fréquence (équivalent `ts_stat`) | Retourne `TABLE(term_text, ndoc, nentry)` |
| `bm25_doc_stats(table_id, limit)` | Top N documents par longueur | Retourne `TABLE(doc_id, doc_length, unique_terms)` |
| `bm25_collection_stats(table_id)` | Statistiques globales de collection | Retourne `TABLE(total_documents, avg_document_length, ...)` |
| `bm25_explain_doc(table_id, doc_id, k1, b)` | Analyse BM25 d'un document | Retourne `TABLE(term_text, tf, df, idf, bm25_weight)` |

**Pré-requis prefix/fuzzy :**

- **`prefix_match`** et **`fuzzy_match`** nécessitent l'extension `pg_trgm`.
- Si `pg_trgm` est installé **après** `pg_facets`, l'index optionnel peut ne pas avoir été créé automatiquement. Dans ce cas, créez-le manuellement :

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS bm25_index_term_prefix
    ON facets.bm25_index USING gin (term_text gin_trgm_ops);
```

**Exemple d'utilisation SQL :**

```sql
-- Indexer un document
SELECT facets.bm25_index_document(
    'my_schema.products'::regclass,
    123,
    'High-performance laptop with SSD storage',
    'content',
    'english'
);

-- Recherche BM25
SELECT * FROM facets.bm25_search(
    'my_schema.products'::regclass,
    'laptop performance',
    'english',        -- language
    false,            -- prefix_match
    false,            -- fuzzy_match
    0.3,              -- fuzzy_threshold
    1.2,              -- k1
    0.75,             -- b
    10                -- limit
)
ORDER BY score DESC;

-- Calculer le score pour un document spécifique
SELECT facets.bm25_score(
    'my_schema.products'::regclass,
    'laptop performance',
    123,
    'english',
    1.2,
    0.75
);

-- Recalculer les statistiques après indexation batch
SELECT facets.bm25_recalculate_statistics('my_schema.products'::regclass);

-- Obtenir les statistiques
SELECT * FROM facets.bm25_get_statistics('my_schema.products'::regclass);
```

**Tables BM25 internes :**

- `facets.bm25_index` : Index inversé (term_hash → doc_ids bitmap, term_freqs)
- `facets.bm25_documents` : Métadonnées des documents (doc_id, doc_length)
- `facets.bm25_statistics` : Statistiques de collection (total_documents, avg_document_length)

**Indexation batch :**

Pour indexer plusieurs documents efficacement :

```sql
-- Indexation batch avec recalcul automatique des statistiques
SELECT facets.bm25_index_documents_batch(
    'my_schema.products'::regclass,
    '[
        {"doc_id": 1, "content": "Laptop description"},
        {"doc_id": 2, "content": "Phone description"}
    ]'::jsonb,
    'content',
    'english'
);
```

### Fonctions BM25 Debug/Analysis (NEW in 0.4.2)

Ces fonctions permettent d'analyser et déboguer les index BM25, similaires à `ts_stat()` pour le full-text search :

| Fonction | Description |
|----------|-------------|
| `bm25_term_stats(table_id, limit)` | Retourne les termes les plus fréquents avec `ndoc` et `nentry` |
| `bm25_doc_stats(table_id, limit)` | Retourne les documents triés par longueur |
| `bm25_collection_stats(table_id)` | Statistiques globales de la collection |
| `bm25_explain_doc(table_id, doc_id, k1, b)` | Analyse les poids BM25 de chaque terme dans un document |

**Exemple d'utilisation :**

```sql
-- Équivalent de ts_stat() pour BM25 : top 10 termes par fréquence
SELECT * FROM facets.bm25_term_stats('my_schema.products'::regclass::oid, 10);
-- Résultat:
--  term_text  | ndoc | nentry
-- ------------+------+--------
--  product    |  500 |   1234
--  price      |  450 |    890
--  descr      |  400 |    756

-- Top 10 documents par longueur
SELECT * FROM facets.bm25_doc_stats('my_schema.products'::regclass::oid, 10);
-- Résultat:
--  doc_id | doc_length | unique_terms
-- --------+------------+--------------
--    1234 |        156 |            0
--    5678 |        142 |            0

-- Statistiques globales de la collection
SELECT * FROM facets.bm25_collection_stats('my_schema.products'::regclass::oid);
-- Résultat:
--  total_documents | avg_document_length | total_terms | unique_terms
-- -----------------+---------------------+-------------+--------------
--             5000 |                45.2 |           0 |         1234

-- Expliquer pourquoi un document a un certain score
-- Montre la contribution BM25 de chaque terme
SELECT * FROM facets.bm25_explain_doc('my_schema.products'::regclass::oid, 123);
-- Résultat:
--  term_text  | tf | df |   idf    | bm25_weight
-- ------------+----+----+----------+-------------
--  laptop     |  3 |  5 |    1.39  |    2.16      -- terme rare, TF élevé = poids élevé
--  product    |  1 | 500|    0.10  |    0.09      -- terme commun = poids faible
```

**Cas d'usage :**

- **Debug de ranking** : Comprendre pourquoi un document est classé haut/bas
- **Analyse de vocabulaire** : Identifier les termes les plus importants dans le corpus
- **Optimisation** : Identifier les candidats stop-words spécifiques au domaine
- **Validation** : Vérifier que l'indexation fonctionne correctement

### Fonctions BM25 Helper (NEW in 0.4.2)

Ces fonctions simplifient la configuration et la maintenance des index BM25 :

| Fonction | Description |
|----------|-------------|
| `bm25_create_sync_trigger(table, id_col, content_col, lang)` | Crée un trigger de synchronisation automatique |
| `bm25_drop_sync_trigger(table)` | Supprime le trigger de synchronisation |
| `bm25_rebuild_index(table, id_col, content_col, lang, workers, conn_str, progress_step)` | Reconstruit l'index BM25 |
| `bm25_status()` | Retourne le statut de tous les index BM25 |
| `bm25_progress(table)` | Retourne la progression de l'indexation |
| `bm25_active_processes()` | Liste les processus BM25 en cours |
| `bm25_cleanup_dblinks()` | Déconnecte les dblinks orphelins |
| `bm25_cleanup_staging()` | Supprime les tables staging orphelines |
| `bm25_kill_stuck(min_duration)` | Termine les processus bloqués |
| `bm25_full_cleanup(kill_threshold)` | Nettoyage complet |
| `setup_table_with_bm25(...)` | Configuration one-stop (facettes + BM25) |
| `bm25_term_stats(table_id, limit)` | Top N termes par fréquence (comme `ts_stat`) |
| `bm25_doc_stats(table_id, limit)` | Top N documents par longueur |
| `bm25_collection_stats(table_id)` | Statistiques globales de collection |
| `bm25_explain_doc(table_id, doc_id, k1, b)` | Analyse des poids BM25 par terme dans un document |

**Exemple de configuration simplifiée (0.4.2) :**

```sql
-- Configuration one-stop pour une nouvelle table
SELECT facets.setup_table_with_bm25(
    'my_schema.products'::regclass,
    'id',                                    -- id_column
    'content',                               -- content_column
    ARRAY[facets.plain_facet('category')],   -- facets
    'english',                               -- language
    true,                                    -- create_trigger
    NULL,                                    -- chunk_bits (auto)
    true,                                    -- populate_facets
    true,                                    -- build_bm25_index
    0                                        -- bm25_workers (auto: 4 si dblink, 1 sinon)
);

-- Monitoring
SELECT * FROM facets.bm25_status();
SELECT * FROM facets.bm25_progress('my_schema.products');

-- Reconstruction si nécessaire
SELECT facets.bm25_rebuild_index('my_schema.products', 'id', 'content', 'english', 4);

-- En cas de problème
SELECT * FROM facets.bm25_full_cleanup();
```

### Fonctions d'Introspection (NEW in 0.4.3)

Ces fonctions permettent aux frontends de découvrir dynamiquement la configuration des facettes et d'adapter leurs filtres en conséquence. Quand le backend ajoute ou modifie des facettes, le frontend peut interroger ces fonctions pour adapter son UI automatiquement.

---

#### 1. `facets.list_table_facets(p_table)` — Full Details

Returns all facet metadata as a table. This is the most complete view of facet definitions.

```sql
SELECT * FROM facets.list_table_facets('client_abc123.documents'::regclass);
```

**Returns:**

| Column | Type | Description |
|--------|------|-------------|
| `facet_id` | int | Unique identifier for the facet |
| `facet_name` | text | Name used in filter queries |
| `facet_type` | text | Type of facet (plain, bucket, boolean, function, etc.) |
| `base_column` | name | Source column name (NULL for function facets) |
| `params` | jsonb | Type-specific parameters (buckets, precision, function name, etc.) |
| `is_multi` | boolean | True for array/multi-value facets |
| `supports_delta` | boolean | True if facet supports incremental updates |

**Example output:**

```
 facet_id |        facet_name         | facet_type | base_column  |                    params                     | is_multi | supports_delta
----------+---------------------------+------------+--------------+-----------------------------------------------+----------+----------------
        1 | project_id                | plain      | project_id   | {}                                            | f        | t
        2 | source_type               | plain      | source_type  | {}                                            | f        | t
        3 | call_duration_bucket      | bucket     | call_duration| {"buckets": "{0,30,120,300}"}                 | f        | t
        4 | has_recording             | boolean    | has_recording| {}                                            | f        | t
        5 | tags                      | array      | tags         | {}                                            | t        | t
        6 | ns_xxx_dim_business_stage | function   |              | {"function": "get_business_stage", ...}       | f        | t
```

---

#### 2. `facets.list_table_facet_names(p_table)` — Simple Array

Returns just the facet names as a text array. Useful for quick checks or iteration.

```sql
SELECT facets.list_table_facet_names('client_abc123.documents'::regclass);
```

**Returns:**

```
{project_id,source_type,content_type,language_code,call_duration_bucket,has_recording,tags,ns_xxx_dim_business_stage}
```

---

#### 3. `facets.list_table_facets_with_types(p_table)` — JSONB Object Format

Returns a JSONB object with facet names as keys and metadata as values. **Best for API responses** where you need to look up facet metadata by name.

```sql
SELECT facets.list_table_facets_with_types('client_abc123.documents'::regclass);
```

**Returns JSONB like:**

```json
{
  "project_id": {
    "facet_id": 1,
    "facet_type": "plain",
    "base_column": "project_id",
    "is_multi": false,
    "params": null
  },
  "source_type": {
    "facet_id": 2,
    "facet_type": "plain",
    "base_column": "source_type",
    "is_multi": false,
    "params": null
  },
  "call_duration_bucket": {
    "facet_id": 3,
    "facet_type": "bucket",
    "base_column": "call_duration",
    "is_multi": false,
    "params": {"buckets": "{0,30,120,300}"}
  },
  "has_recording": {
    "facet_id": 4,
    "facet_type": "boolean",
    "base_column": "has_recording",
    "is_multi": false,
    "params": null
  },
  "tags": {
    "facet_id": 5,
    "facet_type": "array",
    "base_column": "tags",
    "is_multi": true,
    "params": null
  },
  "ns_xxx_dim_business_stage": {
    "facet_id": 6,
    "facet_type": "function",
    "base_column": null,
    "is_multi": false,
    "params": {"function": "client_abc123.get_business_stage", "base_column": "fid"}
  }
}
```

**Note:** JSON object key order is not guaranteed in most programming languages. Use `list_table_facets_for_ui()` if you need ordered results.

---

#### 4. `facets.list_table_facets_simple(p_table)` — Name and Type Only

Returns a simple table with just facet name and type. Perfect for quick queries or generating filter dropdowns.

```sql
SELECT * FROM facets.list_table_facets_simple('client_abc123.documents'::regclass);
```

**Returns:**

```
        facet_name         | facet_type
---------------------------+------------
 project_id                | plain
 source_type               | plain
 content_type              | plain
 language_code             | plain
 call_provider             | plain
 call_duration_bucket      | bucket
 speaker_count_bucket      | bucket
 has_recording             | boolean
 email_from                | plain
 email_priority            | bucket
 tags                      | array
 ns_xxx_dim_business_stage | function
```

---

#### 5. `facets.describe_table(p_table)` — Table-Level Metadata

Returns configuration information about the faceted table itself.

```sql
SELECT * FROM facets.describe_table('client_abc123.documents'::regclass);
```

**Returns:**

| Column | Type | Description |
|--------|------|-------------|
| `table_id` | oid | PostgreSQL OID of the table |
| `schemaname` | text | Schema name |
| `tablename` | text | Table name |
| `key_column` | name | Primary key column used for document IDs |
| `key_type` | text | Data type of the key column |
| `chunk_bits` | int | Chunk size configuration (2^n documents per chunk) |
| `bm25_language` | text | Language for BM25 text search |
| `has_bm25_index` | boolean | True if BM25 index contains documents |
| `has_delta_table` | boolean | True if delta table exists for incremental updates |
| `facet_count` | int | Number of facets registered |

**Example output:**

```
 table_id |  schemaname   |  tablename  | key_column | key_type | chunk_bits | bm25_language | has_bm25_index | has_delta_table | facet_count
----------+---------------+-------------+------------+----------+------------+---------------+----------------+-----------------+-------------
    16528 | client_abc123 | documents   | id         | bigint   |         20 | english       | t              | t               |          12
```

---

#### 6. `facets.list_tables()` — List All Registered Tables

Returns all tables that have faceting enabled. Useful for multi-tenant systems or admin dashboards.

```sql
SELECT * FROM facets.list_tables();
```

**Returns:**

| Column | Type | Description |
|--------|------|-------------|
| `table_id` | oid | PostgreSQL OID |
| `qualified_name` | text | Schema-qualified table name |
| `schemaname` | text | Schema name |
| `tablename` | text | Table name |
| `facet_count` | int | Number of facets |
| `has_bm25` | boolean | True if BM25 index exists |
| `has_delta` | boolean | True if delta table exists |
| `bm25_language` | text | BM25 language setting |

**Example output:**

```
 table_id |       qualified_name        |  schemaname   |  tablename  | facet_count | has_bm25 | has_delta | bm25_language
----------+-----------------------------+---------------+-------------+-------------+----------+-----------+---------------
    16528 | client_abc123.documents     | client_abc123 | documents   |          12 | t        | t         | english
    16892 | client_xyz789.products      | client_xyz789 | products    |           8 | t        | t         | english
    17156 | public.articles             | public        | articles    |           5 | f        | t         | french
```

---

#### 7. `facets.get_facet_hierarchy(p_table)` — Hierarchical Relationships

Returns parent-child relationships between facets. Use this for building tree/cascading filter UIs.

```sql
SELECT * FROM facets.get_facet_hierarchy('client_abc123.documents'::regclass);
```

**Returns:**

| Column | Type | Description |
|--------|------|-------------|
| `facet_name` | text | Facet name |
| `facet_type` | text | Facet type |
| `parent_facet` | text | Parent facet name (NULL if not hierarchical) |
| `is_root` | boolean | True if this facet is a hierarchy root |
| `is_hierarchical` | boolean | True if part of any hierarchy |
| `depth` | int | Depth in hierarchy (0 = root or non-hierarchical) |

**Example output:**

```
        facet_name         | facet_type | parent_facet | is_root | is_hierarchical | depth
---------------------------+------------+--------------+---------+-----------------+-------
 project_id                | plain      |              | f       | f               |     0
 category                  | plain      |              | t       | t               |     0
 subcategory               | function   | category     | f       | t               |     1
 product_line              | function   | subcategory  | f       | t               |     2
 has_recording             | boolean    |              | f       | f               |     0
```

---

#### 8. `facets.list_table_facets_for_ui(p_table)` — Facets with UI Hints

Returns a JSON array with UI component suggestions. **Best for frontend filter generation** — array format preserves ordering.

```sql
SELECT facets.list_table_facets_for_ui('client_abc123.documents'::regclass);
```

**Returns JSON array like:**

```json
[
  {
    "facet_id": 1,
    "name": "project_id",
    "type": "plain",
    "base_column": "project_id",
    "is_multi": false,
    "params": null,
    "parent_facet": null,
    "is_hierarchical": false,
    "ui_component": "dropdown"
  },
  {
    "facet_id": 2,
    "name": "category",
    "type": "plain",
    "base_column": "category",
    "is_multi": false,
    "params": null,
    "parent_facet": null,
    "is_hierarchical": true,
    "ui_component": "tree"
  },
  {
    "facet_id": 3,
    "name": "has_recording",
    "type": "boolean",
    "base_column": "has_recording",
    "is_multi": false,
    "params": null,
    "parent_facet": null,
    "is_hierarchical": false,
    "ui_component": "checkbox"
  },
  {
    "facet_id": 4,
    "name": "call_duration_bucket",
    "type": "bucket",
    "base_column": "call_duration",
    "is_multi": false,
    "params": {"buckets": "{0,30,120,300}"},
    "parent_facet": null,
    "is_hierarchical": false,
    "ui_component": "range"
  },
  {
    "facet_id": 5,
    "name": "tags",
    "type": "array",
    "base_column": "tags",
    "is_multi": true,
    "params": null,
    "parent_facet": null,
    "is_hierarchical": false,
    "ui_component": "multiselect"
  },
  {
    "facet_id": 6,
    "name": "created_month",
    "type": "datetrunc",
    "base_column": "created_at",
    "is_multi": false,
    "params": {"precision": "month"},
    "parent_facet": null,
    "is_hierarchical": false,
    "ui_component": "datepicker"
  }
]
```

---

#### 9. `facets.introspect(p_table)` — Complete Introspection (Single API Endpoint)

Returns everything in a single call: table metadata, facets with UI hints, and hierarchy information. **Best for initial frontend configuration** — one query gets everything needed.

```sql
SELECT facets.introspect('client_abc123.documents'::regclass);
```

**Returns complete JSONB structure:**

```json
{
  "table": {
    "table_id": 16528,
    "schema": "client_abc123",
    "name": "documents",
    "qualified_name": "client_abc123.documents",
    "key_column": "id",
    "key_type": "bigint",
    "chunk_bits": 20,
    "bm25_language": "english",
    "has_bm25_index": true,
    "has_delta_table": true
  },
  "facets": [
    {
      "facet_id": 1,
      "name": "project_id",
      "type": "plain",
      "base_column": "project_id",
      "is_multi": false,
      "params": null,
      "parent_facet": null,
      "is_hierarchical": false,
      "ui_component": "dropdown"
    },
    {
      "facet_id": 2,
      "name": "category",
      "type": "plain",
      "base_column": "category",
      "is_multi": false,
      "params": null,
      "parent_facet": null,
      "is_hierarchical": true,
      "ui_component": "tree"
    }
  ],
  "hierarchy": {
    "roots": ["category"],
    "regular_facets": ["project_id", "has_recording", "tags"],
    "relationships": {
      "subcategory": {
        "parent": "category",
        "facet_type": "function"
      },
      "product_line": {
        "parent": "subcategory",
        "facet_type": "function"
      }
    }
  },
  "facet_count": 12
}
```

---

#### Facet Types Reference

| Type | Description | Example | UI Component |
|------|-------------|---------|--------------|
| `plain` | Direct column mapping | `project_id`, `source_type`, `category` | `dropdown` |
| `bucket` | Numeric/string bucketing | `call_duration_bucket`, `price_range` | `range` |
| `boolean` | Boolean values | `has_recording`, `is_active` | `checkbox` |
| `function` | Function-based facets | Business facets from `facet_assignments` | `dropdown` or `tree` |
| `array` | Array column values | `tags`, `keywords` | `multiselect` |
| `datetrunc` | Date truncation | `created_month`, `updated_week` | `datepicker` |
| `rating` | Rating/score values | `stars`, `quality_score` | `rating` |
| `joined_plain` | Values from joined tables | Lookup tables | `dropdown` |
| `function_array` | Function returning arrays | Complex computed arrays | `multiselect` |

---

#### UI Component Mapping

The `ui_component` field in `list_table_facets_for_ui()` and `introspect()` suggests which UI control to use:

| UI Component | When Used | Frontend Implementation |
|--------------|-----------|------------------------|
| `dropdown` | Plain facets, function facets (non-hierarchical) | `<select>` or autocomplete input |
| `multiselect` | Array facets, multi-value facets | Multi-select dropdown or tag input |
| `checkbox` | Boolean facets | Checkbox or toggle switch |
| `range` | Bucket facets (numeric ranges) | Range slider or segmented control |
| `datepicker` | Date truncation facets | Date picker with appropriate precision |
| `rating` | Rating facets | Star rating or number selector |
| `tree` | Hierarchical facets (any with parent_facet) | Tree selector or cascading dropdowns |

---

#### Choosing the Right Function

| Use Case | Function |
|----------|----------|
| Quick check of available facets | `list_table_facet_names()` |
| Simple facet list for debugging | `list_table_facets_simple()` |
| Full facet metadata for backend logic | `list_table_facets()` |
| API response with facet lookup by name | `list_table_facets_with_types()` |
| Building filter UI with component hints | `list_table_facets_for_ui()` |
| Table configuration info | `describe_table()` |
| Admin dashboard listing all tables | `list_tables()` |
| Tree/cascading filter UI | `get_facet_hierarchy()` |
| **Initial frontend setup (one call)** | `introspect()` |

## ACID Compliance and Transaction Safety

### Overview

pg_facets 0.4.3 implements full ACID compliance for all critical operations. All Zig functions use savepoints for atomicity, row-level locking for isolation, and proper error handling with rollback.

### ACID Compliance Features

#### 1. Atomicity (A)

All multi-step operations are wrapped in savepoints:

- **`indexDocument()`**: Creates savepoint `bm25_index_doc` before indexing
- **`deleteDocument()`**: Creates savepoint `bm25_delete_doc` before deletion
- **`merge_deltas_native()`**: Creates savepoint `merge_deltas_atomic` before merging

If any step fails, the operation automatically rolls back to the savepoint, ensuring all-or-nothing semantics.

#### 2. Consistency (C)

- Foreign key constraints enforced
- Unique constraints on facet combinations
- Statistics maintained incrementally with validation
- Delta tables tracked and merged atomically

#### 3. Isolation (I)

Row-level locking prevents race conditions:

```zig
// In merge_deltas_native() - locks rows during read-modify-write
SELECT postinglist FROM ... WHERE ... FOR UPDATE
```

This ensures concurrent transactions don't interfere with bitmap modifications.

#### 4. Durability (D)

- All operations participate in PostgreSQL transactions
- WAL (Write-Ahead Logging) ensures durability for LOGGED tables
- UNLOGGED tables available for bulk operations (see below)

### Safe Wrapper Functions

SQL wrapper functions provide additional safety:

- **`facets.bm25_index_document_safe()`**: Wraps indexing with savepoint
- **`facets.bm25_delete_document_safe()`**: Wraps deletion with savepoint
- **`facets.merge_deltas_safe()`**: Wraps delta merging with savepoint

These functions automatically handle savepoint creation, error handling, and rollback.

### Usage Example

```sql
-- Safe indexing with automatic rollback on error
BEGIN;
SELECT facets.bm25_index_document_safe(
    'public.products'::regclass,
    12345,
    'Product description text...',
    'content',
    'english'
);
COMMIT;
```

---

## UNLOGGED Table Support

### Overview

pg_facets 0.4.3 supports UNLOGGED tables for bulk loading operations. UNLOGGED tables provide 2-3x faster bulk loads by bypassing WAL writes, then can be converted to LOGGED for durability.

### Performance Benefits

| Operation | UNLOGGED | LOGGED | Improvement |
|-----------|----------|--------|-------------|
| Bulk load (10M rows) | 10 min | 30 min | **3x faster** |
| WAL size during load | 0 GB | 50 GB | **100% reduction** |
| Conversion time | N/A | 5-15 min | One-time cost |

### Creating UNLOGGED Tables

```sql
-- Create table as UNLOGGED for bulk load
SELECT facets.add_faceting_to_table(
    'public.products'::regclass,
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category'),
        facets.plain_facet('brand')
    ],
    chunk_bits => 20,
    populate => false,
    unlogged => true  -- Create as UNLOGGED
);

-- Bulk populate (FAST - no WAL)
SELECT facets.populate_facets('public.products'::regclass::oid);

-- Convert to LOGGED for durability
SELECT * FROM facets.set_table_logged('public.products'::regclass);
```

### Conversion Functions

- **`facets.set_table_unlogged(p_table)`**: Convert to UNLOGGED (for bulk operations)
- **`facets.set_table_logged(p_table)`**: Convert to LOGGED (for durability)
- **`facets.check_table_logging_status(p_table)`**: Check current logging status
- **`facets.verify_before_logged_conversion(p_table)`**: Safety checks before conversion
- **`facets.bulk_load_with_unlogged(...)`**: Complete workflow in one call

### Complete Workflow

```sql
-- All-in-one: Create UNLOGGED, load, convert to LOGGED
SELECT * FROM facets.bulk_load_with_unlogged(
    'public.products'::regclass,
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category'),
        facets.plain_facet('brand')
    ],
    chunk_bits => 20
);
```

### ACID Compliance with UNLOGGED Tables

**Important**: UNLOGGED tables maintain ACID properties A, C, I but **lose durability (D)**:

- ✅ **Atomicity**: Transactions still atomic
- ✅ **Consistency**: Constraints still enforced
- ✅ **Isolation**: Concurrent transactions still isolated
- ❌ **Durability**: Data lost on PostgreSQL crash

**Recommendation**: Use UNLOGGED for bulk loads, convert to LOGGED for production.

---

## pg_cron Integration for Delta Merging

### Overview

pg_facets 0.4.3 includes helper functions for automatic delta merging via pg_cron. This ensures deltas are regularly applied without manual intervention.

### Setup

```sql
-- Install pg_cron extension (as superuser)
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Schedule delta merging every 5 minutes
SELECT cron.schedule(
    'merge-facets-deltas',
    '*/5 * * * *',  -- Every 5 minutes
    $$SELECT facets.merge_deltas_all()$$
);
```

### Helper Functions

#### Merge All Tables

```sql
-- Merge deltas for all registered tables
SELECT * FROM facets.merge_deltas_all();
```

Returns:
- `table_name`: Name of the table
- `rows_merged`: Number of deltas merged
- `elapsed_ms`: Time taken in milliseconds
- `status`: 'success', 'no_deltas', or 'error: ...'

#### Smart Merging

```sql
-- Only merge if deltas exceed threshold
SELECT * FROM facets.merge_deltas_smart(
    'public.products'::regclass,
    p_min_delta_count => 5000,      -- Only merge if >= 5000 deltas
    p_max_delta_age => '10 minutes' -- Or if deltas are 10+ minutes old
);
```

#### Monitor Delta Status

```sql
-- Check delta status across all tables
SELECT * FROM facets.delta_status();
```

Returns:
- `table_name`: Name of the table
- `delta_count`: Number of pending deltas
- `delta_size_mb`: Size of delta table in MB
- `recommendation`: Action recommendation

#### Health Check

```sql
-- Check delta health for alerting
SELECT * FROM facets.check_delta_health();
```

Returns alert levels: 'ok', 'info', 'warning', 'critical'

### Recommended Cron Schedules

**High-Volume Systems** (1000+ updates/minute):
```sql
-- Merge every minute for critical tables
SELECT cron.schedule(
    'merge-deltas-critical',
    '* * * * *',
    $$SELECT facets.merge_deltas('public.critical_table'::regclass)$$
);
```

**Medium-Volume Systems** (100-1000 updates/minute):
```sql
-- Merge every 5 minutes
SELECT cron.schedule(
    'merge-deltas-all',
    '*/5 * * * *',
    $$SELECT facets.merge_deltas_all()$$
);
```

**Smart Scheduling** (Recommended):
```sql
-- Only merge when needed
SELECT cron.schedule(
    'merge-deltas-smart',
    '*/2 * * * *',  -- Check every 2 minutes
    $$
    SELECT facets.merge_deltas_smart(
        table_id::regclass,
        p_min_delta_count => 5000,
        p_max_delta_age => '10 minutes'
    )
    FROM facets.faceted_table
    WHERE delta_table IS NOT NULL
    $$
);
```

### Delta Merge History

The extension tracks merge history in `facets.delta_merge_history`:

```sql
-- View merge history
SELECT 
    table_name,
    delta_count,
    rows_merged,
    elapsed_ms,
    merged_at,
    status
FROM facets.delta_merge_history
ORDER BY merged_at DESC
LIMIT 10;
```

### Merge with History Tracking

```sql
-- Merge with automatic history tracking
SELECT facets.merge_deltas_with_history('public.products'::regclass);
```

---

## Tests

### Tests SQL

Les tests SQL sont dans `test/sql/` :

- `complete_test.sql` : Suite complète de tests
- `bm25_search_test.sql` : Tests de recherche BM25
- `bitmap_optimization_test.sql` : Tests des optimisations bitmap (0.3.6)
- `bm25_text_pk_test.sql` : Tests avec clés primaires texte
- `bm25_helpers_test.sql` : Tests des fonctions helper BM25 (0.4.2)
- `parallel_indexing_test.sql` : Tests d'indexation parallèle optimisée (0.4.2)

```bash
# Exécuter avec le script
cd test && ./run_all_tests.sh
```

### Tests Go

Les tests Go vérifient le client et l'API contre une vraie instance PostgreSQL :

```bash
cd examples/golang

# Méthode recommandée: build Docker + tests
make test

# Ou manuellement:
make start-db    # Démarre PostgreSQL avec pg_facets
make test-fast   # Lance les tests Go
make stop-db     # Arrête le container
```

**Variables d'environnement:**

| Variable | Description |
|----------|-------------|
| `TEST_DATABASE_URL` | URL de connexion PostgreSQL |
| `PGFACETS_TEST_FAIL_ON_NO_DB=true` | FAIL au lieu de SKIP si pas de DB |

**Tests bitmap (0.3.6):**

```go
// TestBitmapOptimization vérifie:
// - FilterDocumentsByFacetsBitmap() fonctionne
// - GetBitmapCardinality() retourne le bon count
// - HierarchicalFacetsBitmap() retourne les facettes
// - SearchWithFacetsBitmap() avec query vide
// - SearchWithFacetsBitmap() avec query texte
// - CompareBitmapVsArray() vérifie la cohérence
```

## Client Go

Le client Go se trouve dans `examples/golang/` :

### Installation

```go
import pgfaceting "path/to/examples/golang"
```

### API Principale

```go
// Créer un client
search, err := pgfaceting.NewFacetingZigSearch(pool, config, debug)

// Recherche standard (utilise bitmap en interne depuis 0.3.6)
resp, err := search.SearchWithFacets(ctx, pgfaceting.SearchWithFacetsRequest{
    Query:      "laptop",
    Facets:     map[string]string{"category": "Electronics"},
    Limit:      20,
    FacetLimit: 10,
})

// Recherche explicitement optimisée bitmap
resp, err := search.SearchWithFacetsBitmap(ctx, req)
```

### API Bitmap (0.3.6)

```go
// Filtrage par bitmap (efficace pour millions de docs)
bitmap, err := search.FilterDocumentsByFacetsBitmap(ctx, filters)

// Obtenir le count sans array
count, err := search.GetBitmapCardinality(ctx, bitmap)

// Facettes depuis bitmap
facets, err := search.HierarchicalFacetsBitmap(ctx, bitmap, 10)
```

### Quand utiliser les fonctions bitmap directement?

| Cas d'usage | Fonction recommandée |
|-------------|---------------------|
| Recherche normale (<100K résultats) | `SearchWithFacets()` |
| Browse avec filtre large (>100K résultats) | `SearchWithFacetsBitmap()` |
| Juste besoin du count | `FilterDocumentsByFacetsBitmap()` + `GetBitmapCardinality()` |
| Opérations bitmap custom | `NativeFaceting.AndBitmaps()`, `OrBitmaps()`, etc. |

### API BM25 (0.4.0+)

Le client Go supporte maintenant les fonctions BM25 pour la recherche full-text avec scoring de pertinence :

```go
// Indexer un document pour la recherche BM25
err := search.IndexDocument(ctx, docID, content, "english")

// Supprimer un document de l'index BM25
err := search.DeleteDocument(ctx, docID)

// Recherche BM25 avec options
results, err := search.BM25Search(ctx, "laptop", pgfaceting.BM25SearchOptions{
    Language:       "english",
    PrefixMatch:    false,
    FuzzyMatch:     false,
    FuzzyThreshold: 0.3,
    K1:             1.2,
    B:              0.75,
    Limit:          10,
})

// Calculer le score BM25 pour un document spécifique
score, err := search.BM25Score(ctx, "laptop", docID, "english", 1.2, 0.75)

// Recalculer les statistiques de collection (après indexation batch)
err := search.RecalculateStatistics(ctx)

// Obtenir les statistiques de collection
stats, err := search.GetStatistics(ctx)
// stats.TotalDocs, stats.AvgLength
```

**Fonctions BM25 disponibles :**

| Fonction | Description | Paramètres |
|----------|-------------|------------|
| `IndexDocument()` | Indexe un document pour la recherche BM25 | `docID`, `content`, `language` |
| `DeleteDocument()` | Supprime un document de l'index BM25 | `docID` |
| `BM25Search()` | Recherche avec scoring BM25 | `query`, `options` |
| `BM25Score()` | Calcule le score BM25 pour un document | `query`, `docID`, `language`, `k1`, `b` |
| `RecalculateStatistics()` | Recalcule les statistiques de collection | - |
| `GetStatistics()` | Récupère les statistiques de collection | - |
| `BM25GetMatchesBitmap()` | Retourne un roaring bitmap des documents correspondants (0.4.1) | `query`, `options` |
| `IndexDocumentsBatch()` | Indexation batch de plusieurs documents (0.4.1) | `documents`, `language`, `batchSize` |
| `IndexDocumentsParallel()` | Indexation parallèle avec dblink (0.4.1) | `sourceQuery`, `language`, `numWorkers`, `connectionString` |

**Paramètres BM25 :**

- **`k1`** (défaut: 1.2) : Contrôle la saturation de la fréquence de terme
- **`b`** (défaut: 0.75) : Contrôle la normalisation par longueur de document
- **`language`** (défaut: "english") : Configuration de recherche textuelle PostgreSQL
- **`prefix_match`** : Active la correspondance par préfixe
- **`fuzzy_match`** : Active la correspondance floue (fuzzy matching)
- **`fuzzy_threshold`** : Seuil pour la correspondance floue (0.0-1.0)

**Exemple d'utilisation complète :**

```go
// 1. Indexer des documents
for _, doc := range documents {
    err := search.IndexDocument(ctx, doc.ID, doc.Content, "english")
    if err != nil {
        log.Printf("Failed to index document %d: %v", doc.ID, err)
    }
}

// 2. Recalculer les statistiques après indexation batch
err := search.RecalculateStatistics(ctx)
if err != nil {
    log.Printf("Failed to recalculate statistics: %v", err)
}

// 3. Effectuer une recherche
results, err := search.BM25Search(ctx, "PostgreSQL performance", pgfaceting.BM25SearchOptions{
    Language: "english",
    K1:       1.2,
    B:        0.75,
    Limit:    20,
})

// 4. Les résultats sont triés par score décroissant
for _, result := range results {
    fmt.Printf("Doc ID: %d, Score: %.4f\n", result.DocID, result.Score)
}
```

**Indexation batch efficace (0.4.1+) :**

```go
// IndexDocumentsBatch est plus efficace que d'appeler IndexDocument en boucle
docs := []pgfaceting.BM25Document{
    {DocID: 1, Content: "Premier document sur PostgreSQL"},
    {DocID: 2, Content: "Deuxième document sur les bases de données"},
    {DocID: 3, Content: "Troisième document sur l'optimisation"},
}

count, elapsedMs, err := search.IndexDocumentsBatch(ctx, docs, "french", 1000)
if err != nil {
    log.Printf("Batch indexing failed: %v", err)
}
log.Printf("Indexed %d documents in %.2fms", count, elapsedMs)
```

**Obtenir un bitmap BM25 pour combinaison avec filtres facettes (0.4.1+) :**

```go
// Obtenir un bitmap des documents correspondant à la requête BM25
bitmap, err := search.BM25GetMatchesBitmap(ctx, "PostgreSQL", pgfaceting.BM25SearchOptions{
    Language: "english",
})
if err != nil {
    log.Printf("BM25GetMatchesBitmap failed: %v", err)
}

// Combiner avec un bitmap de filtre facettes
facetBitmap, _ := search.FilterDocumentsByFacetsBitmap(ctx, map[string]string{
    "category": "Technology",
})

// Le résultat peut être utilisé avec NativeFaceting.AndBitmaps()
```

## Dependencies externes

### Roaring Bitmaps

Utilise l'API C de `pg_roaringbitmap` :
- `roaring_bitmap_portable_deserialize_safe`
- `roaring_bitmap_or_inplace`
- `roaring_bitmap_and_inplace`
- `roaring_bitmap_portable_serialize`
- `roaring_uint32_iterator`

### PostgreSQL SPI

- `SPI_connect` / `SPI_finish`
- `SPI_execute` / `SPI_execute_with_args`
- `SPI_getbinval` / `SPI_getvalue`

## Notes de performance

1. **Bitmaps Roaring** : Structure de données optimisée pour les opérations sur ensembles
2. **Opérations in-place** : `*_inplace` pour éviter les allocations
3. **Itérateurs** : Accès séquentiel efficace aux bitmaps
4. **SPI** : Exécution de requêtes depuis le code C/Zig

### Optimisation Bitmap (0.3.6)

L'optimisation majeure de la version 0.3.6 concerne la gestion des grands ensembles de résultats.

#### Le Problème (avant 0.3.6)

```sql
-- Si un filtre correspond à 8M documents:
SELECT * FROM facets.filter_documents_by_facets(...);
-- → Crée un tableau de 8M integers (~32 MB)
-- → Passé à d'autres fonctions via ANY(array)
-- → Risque d'OOM, performances dégradées
```

#### La Solution (0.3.6)

```sql
-- Même filtre, mais retourne un bitmap:
SELECT facets.filter_documents_by_facets_bitmap(...);
-- → Retourne un roaringbitmap (~4 MB compressé)
-- → Utilisé directement pour le filtrage via rb_contains()
-- → Pas de conversion en tableau
```

#### Comparaison des Performances

| Scénario | Avant 0.3.6 | Après 0.3.6 |
|----------|-------------|-------------|
| 100K docs filtrés | 50ms | 30ms |
| 1M docs filtrés | 200ms | 50ms |
| 8M docs filtrés | 1900ms ou OOM | 200-500ms |

#### Fonctionnement Interne

```
┌─────────────────────────────────────────────────────────────┐
│  AVANT 0.3.6:                                               │
│                                                             │
│  filter_documents_by_facets() → int[] (8M éléments)        │
│           ↓                                                 │
│  rb_build(array) → bitmap                                   │
│           ↓                                                 │
│  hierarchical_facets(bitmap)                                │
│                                                             │
│  Problème: Le tableau intermédiaire de 8M éléments!        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  APRÈS 0.3.6:                                               │
│                                                             │
│  filter_documents_by_facets_bitmap() → bitmap              │
│           ↓                                                 │
│  hierarchical_facets_bitmap(bitmap)                         │
│                                                             │
│  Solution: Le bitmap passe directement, pas de conversion! │
└─────────────────────────────────────────────────────────────┘
```

#### Recommandations

1. **Utilisez `search_documents_with_facets`** : Elle utilise automatiquement les optimisations bitmap
2. **Pour les opérations custom** : Utilisez `filter_documents_by_facets_bitmap` + opérations rb_*
3. **Évitez `filter_documents_by_facets`** : Sauf si vous avez vraiment besoin d'un tableau

## Limitations connues

1. **Macros C** : Certaines macros nécessitent des helpers C
2. **Tableaux flexibles** : Accès via helpers C (évite l'arithmétique de pointeurs / offsets hardcodés)
3. **Types variadic** : Wrappers nécessaires pour `elog`, etc.

## Évolutions futures

- [ ] Support de plus de types de données
- [ ] Optimisations supplémentaires
- [ ] Tests de performance comparatifs
- [ ] Documentation API complète

## Références

- [PostgreSQL Extension Development](https://www.postgresql.org/docs/current/extend.html)
- [Zig Language Reference](https://ziglang.org/documentation/)
- [Roaring Bitmaps](https://roaringbitmap.org/)
- [pgfaceting original](https://github.com/cybertec-postgresql/pgfaceting)


# Limites
Analysons les limites de `pg_roaringbitmap` et de `pgfaceting_zig` :

## Limites de pg_roaringbitmap

[1 tool called]

D'après la documentation, voici les **limites de taille** :

## Limites de pg_roaringbitmap / pgfaceting_zig

### 1. **roaringbitmap (32-bit)** - Type par défaut utilisé par pgfaceting

| Paramètre | Limite |
|-----------|--------|
| **Plage d'IDs** | `0` à `4,294,967,295` (2³² - 1) |
| **Nombre max de documents** | **~4.3 milliards** |
| **Type d'ID requis** | `uint32` (non signé) |

> *"Logically, you could think of the roaringbitmap data type as `bit(4294967296)`"*

### 2. **roaringbitmap64 (64-bit)** - Pour des besoins plus importants

| Paramètre | Limite |
|-----------|--------|
| **Plage d'IDs** | `0` à `18,446,744,073,709,551,615` (2⁶⁴ - 1) |
| **Nombre max de documents** | **~18 quintillions** |
| **Type d'ID requis** | `uint64` (non signé) |

### 3. **Limites pratiques de pgfaceting_zig**

Le code utilise `roaringbitmap` (32-bit), donc :

```
┌─────────────────────────────────────────────────────────────┐
│  Maximum documents par facette : ~4.3 milliards             │
│  Maximum valeurs de facettes : Illimité (1 bitmap/valeur)   │
│  Maximum facettes par table : Illimité                      │
└─────────────────────────────────────────────────────────────┘
```

### 4. **Chunking dans pgfaceting**

pgfaceting utilise un système de **chunks** (configuré par `chunk_bits`, défaut = 20) :

```
chunk_bits = 20  →  2²⁰ = 1,048,576 documents par chunk
chunk_bits = 24  →  2²⁴ = 16,777,216 documents par chunk
```

Cela permet de :
- Paralléliser les requêtes
- Réduire la taille des bitmaps individuels
- Améliorer les performances de mise à jour

### 5. **Résumé pratique**

| Cas d'usage | Limite | Recommandation |
|-------------|--------|----------------|
| **Petite app** (< 1M docs) | ✅ Aucun problème | `chunk_bits = 20` |
| **Moyenne app** (1-100M docs) | ✅ OK | `chunk_bits = 20-22` |
| **Grande app** (100M-1B docs) | ✅ OK | `chunk_bits = 22-24` |
| **Très grande app** (> 4B docs) | ⚠️ Utiliser `roaringbitmap64` | Modification requise |

### 6. **Si vous dépassez 4 milliards de documents**

Vous devrez modifier pgfaceting_zig pour utiliser `roaringbitmap64` au lieu de `roaringbitmap`. Les fonctions sont similaires mais avec le suffixe `64` :
- `rb_build` → `rb64_build`
- `rb_cardinality` → `rb64_cardinality`
- etc.

---

## Limites des Paramètres de Chaînes (Strings) pour BM25

Les fonctions BM25 ont des limites de taille pour les paramètres de type texte afin d'éviter les problèmes de sécurité mémoire et les débordements de tampon :

### 1. **Paramètre `language` (nom de la langue)**

| Paramètre | Limite | Description |
|-----------|--------|-------------|
| **Taille maximale** | **63 bytes** | Rejette les chaînes ≥ 64 bytes |
| **Fonctions concernées** | Toutes les fonctions BM25 | `bm25_index_document_native`, `bm25_search_native`, `bm25_score_native`, etc. |
| **Raison** | Les noms de langues PostgreSQL sont courts (ex: 'english' = 7 bytes, 'spanish' = 7 bytes) |

**Exemple d'erreur** :
```
ERROR: bm25_index_document_native: Language text too long or not null-terminated 
       (length >= 64 bytes, max 64 bytes). Language names should be short 
       (e.g., 'english', 'spanish').
```

### 2. **Paramètre `content` (contenu du document)**

| Paramètre | Limite | Description |
|-----------|--------|-------------|
| **Taille maximale** | **10 MB** (10,485,760 bytes) | Rejette les documents ≥ 10 MB |
| **Fonctions concernées** | `bm25_index_document_native`, `bm25_index_worker_native` | Indexation de documents |
| **Recommandation** | Pour les documents très volumineux, considérez de les diviser en plusieurs documents plus petits |

**⚠️ Important : Les documents > 10 MB sont automatiquement tronqués à 10 MB**

Le comportement est identique pour toutes les fonctions :

#### `bm25_index_document_native` (indexation unitaire)
- **Comportement** : **Troncature automatique + avertissement**
- **Action** : Le document est tronqué aux 10 premiers MB, un WARNING est loggé, l'indexation continue
- **Message** :
```
WARNING: bm25_index_document_native: Document content exceeds maximum size (10485760 bytes). 
         Truncating to 10485760 bytes. Consider splitting large documents.
```

#### `bm25_index_worker_native` (indexation parallèle)
- **Comportement** : **Troncature automatique + avertissement**
- **Action** : Le document est tronqué aux 10 premiers MB, un WARNING est loggé avec le `doc_id`, l'indexation continue
- **Message** :
```
WARNING: bm25_index_worker_native: Document content exceeds maximum size for doc_id=12345. 
         Truncating to 10485760 bytes.
```

**Note** : Seuls les 10 premiers MB du document sont indexés. La fin du document est perdue. Pour préserver tout le contenu, divisez les documents > 10 MB en plusieurs documents plus petits avant l'indexation.

### 3. **Paramètre `query` (requête de recherche)**

| Paramètre | Limite | Description |
|-----------|--------|-------------|
| **Taille maximale** | **1 MB** (1,048,576 bytes) | Rejette les requêtes ≥ 1 MB |
| **Fonctions concernées** | `bm25_search_native`, `bm25_score_native`, `bm25_get_matches_bitmap_native` | Recherche BM25 |
| **Recommandation** | Pour les requêtes très longues, divisez-les en plusieurs requêtes plus courtes |

**Exemple d'erreur** :
```
ERROR: bm25_search_native: Query text too long or not null-terminated 
       (length >= 1048576 bytes, max 1048576 bytes). Please reduce query size 
       or split into multiple queries.
```

### 4. **Résumé des Limites**

| Type de Paramètre | Limite | Fonctions | Cas d'Usage Typique |
|-------------------|--------|-----------|---------------------|
| **`language`** | 63 bytes | Toutes les fonctions BM25 | 'english' (7 bytes), 'spanish' (7 bytes) |
| **`content`** | 10 MB | Indexation | Documents texte complets |
| **`query`** | 1 MB | Recherche | Requêtes de recherche |

### 5. **Validation et Sécurité**

Ces limites sont appliquées avec des vérifications de sécurité mémoire :
- **Vérification de null-termination** : Détecte les chaînes non null-terminées (corruption mémoire)
- **Vérification de longueur bornée** : Évite les débordements de tampon lors de `strlen()`
- **Messages d'erreur détaillés** : Incluent le nom de la fonction, la longueur détectée et la limite

**Exemple de validation dans le code** :
```zig
// Vérification avec longueur maximale pour éviter les débordements
var lang_len: usize = 0;
const max_lang_len: usize = 64;
while (lang_len < max_lang_len and lang_cstr[lang_len] != 0) {
    lang_len += 1;
}
if (lang_len >= max_lang_len) {
    // Rejette les chaînes trop longues ou non null-terminées
    utils.elogFmt(c.ERROR, "Language text too long...", .{lang_len, max_lang_len});
}
```

### 6. **Recommandations Pratiques**

- **Language** : Utilisez toujours des noms de langues PostgreSQL valides ('english', 'french', 'spanish', etc.)
- **Content** : Pour les documents > 10 MB, divisez-les en sections logiques (chapitres, paragraphes, etc.)
- **Query** : Les requêtes de recherche > 1 MB sont rares ; si nécessaire, utilisez plusieurs appels avec des filtres combinés

---

## Performances et Complexité Algorithmique

Les Roaring Bitmaps sont une structure de données hautement optimisée pour les opérations ensemblistes. Voici l'analyse de complexité basée sur la bibliothèque CRoaring (v4.x).

### Architecture des Roaring Bitmaps

Les roaring bitmaps divisent l'espace des entiers en **conteneurs** de 2¹⁶ (65,536) éléments chacun :

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Roaring Bitmap = [Container₀, Container₁, ..., Containerₙ]             │
│                                                                         │
│  Chaque container couvre une plage de 65,536 valeurs (16 bits hauts)    │
│  3 types de containers : Array, Bitmap, Run-Length Encoded (RLE)        │
└─────────────────────────────────────────────────────────────────────────┘
```

**Variables de complexité** :
- `n` = nombre de containers dans le bitmap
- `m` = nombre total d'éléments (cardinalité)
- `c` = taille moyenne d'un container (éléments)

### Complexité des Opérations de Base

| Opération | Complexité | Notes |
|-----------|------------|-------|
| **Ajout d'un élément** (`rb_add`) | O(log n) | Recherche du container + insertion |
| **Suppression d'un élément** (`rb_remove`) | O(log n) | Recherche du container + suppression |
| **Test d'appartenance** (`@>` élément) | O(log n) | Recherche binaire dans les containers |
| **Cardinalité** (`rb_cardinality`) | O(n) | Somme des cardinalités des containers |
| **Min/Max** (`rb_min`, `rb_max`) | O(1) | Accès au premier/dernier container |

### Complexité des Opérations Binaires (entre 2 bitmaps)

Soit deux bitmaps A et B avec respectivement `n₁` et `n₂` containers :

| Opération | Complexité | Formule détaillée |
|-----------|------------|-------------------|
| **Union** (`\|`, `rb_or`) | O(n₁ + n₂) | Fusion linéaire des containers triés |
| **Intersection** (`&`, `rb_and`) | O(min(n₁, n₂)) à O(n₁ + n₂) | Dépend de la distribution |
| **Différence** (`-`, `rb_andnot`) | O(n₁ + n₂) | Parcours linéaire |
| **XOR** (`#`, `rb_xor`) | O(n₁ + n₂) | Parcours linéaire |
| **Test de chevauchement** (`&&`) | O(min(n₁, n₂)) | Arrêt au premier élément commun |
| **Test d'inclusion** (`@>`, `<@`) | O(min(n₁, n₂)) | Arrêt à la première différence |

### Complexité des Opérations de Cardinalité

| Opération | Complexité | Avantage |
|-----------|------------|----------|
| `rb_and_cardinality` | O(n₁ + n₂) | Sans allocation mémoire |
| `rb_or_cardinality` | O(n₁ + n₂) | Sans allocation mémoire |
| `rb_xor_cardinality` | O(n₁ + n₂) | Sans allocation mémoire |
| `rb_andnot_cardinality` | O(n₁ + n₂) | Sans allocation mémoire |

> **Note** : Les fonctions `*_cardinality` sont plus rapides que de calculer d'abord l'opération puis la cardinalité car elles évitent l'allocation du bitmap résultant.

### Complexité des Agrégations

Pour l'agrégation de `k` bitmaps :

| Opération | Complexité | Notes |
|-----------|------------|-------|
| `rb_or_agg` | O(k × n × log k) | Utilise une heap pour la fusion optimale |
| `rb_and_agg` | O(k × n) | Intersection progressive |
| `rb_xor_agg` | O(k × n) | Application successive |
| `rb_build_agg` | O(m log m) | Tri et compression des éléments |

### Complexité des Opérations d'Itération

| Opération | Complexité | Notes |
|-----------|------------|-------|
| `rb_to_array` | O(m) | Conversion en tableau |
| `rb_iterate` | O(m) | Itérateur streaming |
| `rb_select` | O(m) | Pagination avec offset |
| `rb_range` | O(n + r) | r = éléments dans la plage |

### Performances dans le Contexte de pgfaceting

#### Calcul de Filtre (Intersection de facettes)

Pour `f` facettes avec `v` valeurs chacune et `n` containers par bitmap :

```
Complexité totale du filtre :
  - Union par facette : O(v × n) par facette
  - Intersection des facettes : O(f × n)
  
  Total : O(f × v × n)
```

#### Comptage des Facettes

Pour compter `F` facettes avec le bitmap de filtre :

```
Complexité : O(F × n)
  où n = nombre de containers dans le filtre
```

#### Recherche de Documents

```
Complexité : O(n + limit)
  - Parcours des containers : O(n)
  - Extraction des IDs : O(limit)
```

### Tableau Récapitulatif des Performances

| Scénario | Documents | Facettes | Valeurs/Facette | Temps estimé |
|----------|-----------|----------|-----------------|--------------|
| **Petit** | 100K | 10 | 100 | < 1 ms |
| **Moyen** | 1M | 50 | 500 | 1-10 ms |
| **Grand** | 10M | 100 | 1000 | 10-50 ms |
| **Très grand** | 100M | 200 | 2000 | 50-200 ms |

> Ces estimations supposent des données bien distribuées et une exécution à chaud (données en cache).

### Optimisations Internes des Roaring Bitmaps

1. **Containers adaptatifs** :
   - **Array** : Pour les containers peu denses (< 4096 éléments)
   - **Bitmap** : Pour les containers denses (≥ 4096 éléments)
   - **RLE** : Pour les séquences consécutives

2. **Opérations SIMD** : Utilisation d'instructions vectorielles (AVX2/AVX-512) pour :
   - Opérations AND/OR/XOR sur les containers bitmap
   - Population count (comptage de bits)

3. **Copy-on-write** : Les containers sont partagés quand c'est possible, copiés uniquement lors de modifications.

### Comparaison avec d'autres Approches

| Approche | Complexité Intersection | Complexité Comptage | Mémoire |
|----------|------------------------|---------------------|---------|
| **Roaring Bitmap** | O(n) | O(n) | Compressée |
| **Bitmap classique** | O(N) | O(N) | N/8 bytes |
| **Set (HashSet)** | O(min(m₁, m₂)) | O(1) | ~40×m bytes |
| **B-tree Index** | O(m × log N) | O(m) | ~2×m bytes |

Où `N` = taille max de l'univers, `n` = containers, `m` = cardinalité.

### Recommandations de Performance

1. **Utilisez `*_cardinality`** au lieu de `rb_cardinality(rb_and(...))` pour éviter les allocations
2. **Limitez le nombre de valeurs par facette** : < 10,000 valeurs pour des performances optimales
3. **Utilisez le chunking** pour paralléliser sur de très grands datasets
4. **Surveillez la densité** : Les roaring bitmaps sont plus efficaces quand les données sont clusterisées


# Arguments en Faveur de la Recherche par Facettes vs Recherche Paramétrique/Boolean

## 1. Avantages Fondamentaux de la Recherche par Facettes

### 🚀 Performance sur les Comptages Simultanés

**Faceted Search (pg_facets)**:
```
Une seule requête → Comptages de TOUTES les facettes
Complexité: O(F × n) où F = nombre de facettes, n = containers bitmap
```

**Boolean/Parametric Search (Qdrant, Elasticsearch)**:
```
N requêtes séparées ou agrégations coûteuses
Complexité: O(N × m × log N) par facette avec B-tree
```

| Scénario | Faceted (pg_facets) | Boolean (Qdrant) |
|----------|----------------------|------------------|
| 50 facettes × 100 valeurs | **1-10 ms** | 50-500 ms |
| 100 facettes × 1000 valeurs | **10-50 ms** | 500ms - 5s |

### 📊 UX "Drill-Down" Native

La recherche par facettes est conçue pour le **refinement progressif**:

```
┌─────────────────────────────────────────────────────────────┐
│  User: Recherche "appartement Paris"                         │
│                                                             │
│  → Facettes affichées instantanément:                       │
│    Prix: <500€ (234) | 500-1000€ (567) | >1000€ (123)       │
│    Surface: <30m² (145) | 30-50m² (456) | >50m² (323)       │
│    Type: Studio (234) | T2 (345) | T3 (345)                 │
│                                                             │
│  User clique sur "500-1000€"                                │
│  → TOUTES les facettes se mettent à jour instantanément     │
└─────────────────────────────────────────────────────────────┘
```

Avec Boolean/Qdrant, il faudrait **relancer N agrégations** à chaque clic.

---

## 2. Cas d'Usage Où les Facettes Dominent

### ✅ **E-commerce / Marketplaces**

| Caractéristique | Pourquoi Facettes? |
|-----------------|-------------------|
| **Données structurées** | Catégories, marques, prix, tailles = cardinalité finie |
| **Comptages critiques** | "234 résultats" rassure l'utilisateur |
| **Navigation exploratoire** | L'utilisateur ne sait pas exactement ce qu'il cherche |
| **Volume de filtres** | 10-50 filtres simultanés = cas nominal |

```
Exemple: Recherche immobilière
- 15 facettes (prix, surface, type, étage, parking, etc.)
- 10M annonces
- pg_facets: 10-50ms pour tous les comptages
- Boolean search: 500ms+ avec agrégations
```

### ✅ **Catalogues Techniques / PIM**

| Caractéristique | Pourquoi Facettes? |
|-----------------|-------------------|
| **Attributs multivalués** | Un produit a N caractéristiques |
| **Recherche par spécifications** | "Résistance > 50MPa AND Température < 200°C" |
| **Cross-filtering** | Filtrer sur une facette met à jour les autres |

### ✅ **Gestion Documentaire / Archives**

| Caractéristique | Pourquoi Facettes? |
|-----------------|-------------------|
| **Taxonomies hiérarchiques** | Catégories/sous-catégories |
| **Metadata standardisées** | Dates, auteurs, types de documents |
| **Volumes importants** | Millions de documents |

### ✅ **BI / Analytics Interactives**

| Caractéristique | Pourquoi Facettes? |
|-----------------|-------------------|
| **Dimensions multiples** | Équivalent aux cubes OLAP |
| **Agrégations rapides** | Comptages par dimension |
| **Exploration ad-hoc** | Drill-down/drill-up |

---

## 3. Cas d'Usage Où Boolean/Qdrant Est Meilleur

### ❌ **Quand NE PAS utiliser les facettes**

| Cas | Pourquoi Boolean/Qdrant? |
|-----|-------------------------|
| **Recherche sémantique** | Embeddings + similarité vectorielle |
| **Cardinalité très élevée** | >100K valeurs par facette |
| **Full-text search** | Ranking par pertinence |
| **Données non structurées** | Texte libre, descriptions |
| **Recherche "one-shot"** | Un seul filtre, pas de comptages |

---

## 4. Analyse Comparative Détaillée

### Par Type de Données

| Type de Données | Facettes | Boolean/Parametric |
|-----------------|----------|-------------------|
| **Catégories** (10-1000 valeurs) | ⭐⭐⭐ | ⭐ |
| **Ranges numériques** (prix, dates) | ⭐⭐⭐ | ⭐⭐ |
| **Tags multivalués** | ⭐⭐⭐ | ⭐ |
| **Texte libre** | ❌ | ⭐⭐⭐ |
| **Embeddings/Vecteurs** | ❌ | ⭐⭐⭐ |
| **Géolocalisation** | ⭐ | ⭐⭐⭐ |

### Par Pattern de Requête

| Pattern | Facettes | Boolean/Parametric |
|---------|----------|-------------------|
| **"Montre-moi tout + affine"** | ⭐⭐⭐ | ⭐ |
| **"Je sais exactement ce que je veux"** | ⭐⭐ | ⭐⭐⭐ |
| **Comptages pour chaque option** | ⭐⭐⭐ | ⭐ |
| **Recherche par similarité** | ❌ | ⭐⭐⭐ |
| **Filtrage + Ranking** | ⭐⭐ | ⭐⭐⭐ |

### Par Volume de Données

| Volume | Facettes (pg_facets) | Boolean (Qdrant) |
|--------|----------------------|------------------|
| **< 100K docs** | Overkill mais fonctionne | OK |
| **100K - 10M docs** | ⭐⭐⭐ Optimal | ⭐⭐ OK |
| **10M - 1B docs** | ⭐⭐⭐ Excellent avec chunking | ⭐ Limité |
| **> 1B docs** | ⭐⭐ Nécessite rb64 | ❌ Problématique |

---

## 5. Arguments Techniques Clés

### Complexité Mémoire

```
┌─────────────────────────────────────────────────────────────┐
│  Roaring Bitmap: Compression adaptive                       │
│  - Dense: bitmap classique                                  │
│  - Sparse: array trié                                       │
│  - Runs: RLE                                                │
│                                                             │
│  → 10M documents × 50 facettes × 100 valeurs                │
│     Mémoire: ~500 MB (vs ~4 GB pour HashSets)               │
└─────────────────────────────────────────────────────────────┘
```

### Opérations Sans Allocation

```zig
// pg_facets utilise rb_and_cardinality
// → Calcule la cardinalité SANS créer le bitmap résultat
// → Économie de mémoire et CPU

// vs Boolean search:
// → Doit matérialiser le résultat pour compter
```

### Parallélisation Native

```
Chunking (chunk_bits=20) → 1M docs/chunk
- 10 chunks peuvent être traités en parallèle
- Résultats mergés efficacement
- Scalabilité horizontale native
```

---

## 6. Tableau Décisionnel Final

| Votre Cas | Recommandation |
|-----------|----------------|
| **Catalogue produits avec filtres** | ✅ **Facettes** |
| **Recherche sémantique/RAG** | ❌ Boolean/Qdrant |
| **Dashboard BI interactif** | ✅ **Facettes** |
| **Moteur de recherche texte** | ❌ Elasticsearch/Qdrant |
| **Marketplace/Annonces** | ✅ **Facettes** |
| **Recherche hybride (texte + filtres)** | 🔶 **Combiner les deux** |
| **Système de recommandation** | ❌ BM25+embeddings |
| **Gestion documentaire structurée** | ✅ **Facettes** |

---

## 7. Architecture Hybride Recommandée

Pour le meilleur des deux mondes:

```
┌─────────────────────────────────────────────────────────────┐
│                    Architecture Hybride                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   [Postgres/BM25+Embeddings]  [pg_facets]                 │
│         ↓                      ↓                            │
│   Recherche sémantique    Filtrage rapide                   │
│   → Top-1000 IDs          → Bitmap de filtre                │
│         ↓                      ↓                            │
│         └──────── AND ────────┘                             │
│                   ↓                                         │
│            Résultats filtrés                                │
│            + Comptages facettes                             │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Conclusion**: La recherche par facettes avec Roaring Bitmaps excelle dans les **interfaces de navigation exploratoire** où les utilisateurs veulent voir **combien de résultats correspondent à chaque option**, puis **affiner progressivement**. C'est l'outil idéal pour l'e-commerce, les marketplaces, et les catalogues structurés.

---

## Version Information

**Current Stable Version: 0.4.3 (December 2024)**

This version includes full ACID compliance, UNLOGGED table support for bulk loading, pg_cron integration, and comprehensive BM25 search capabilities.

## Historique des Versions

### Version 0.4.3 (Décembre 2024) - CURRENT

#### ACID Compliance

- ✅ **Savepoints in Zig functions**: All critical operations (`indexDocument`, `deleteDocument`, `merge_deltas_native`) use savepoints for atomicity
- ✅ **Row-level locking**: `SELECT ... FOR UPDATE` in `merge_deltas_native()` prevents race conditions
- ✅ **Error handling**: Automatic rollback to savepoint on any error
- ✅ **Safe wrapper functions**: SQL wrappers with savepoints (`bm25_index_document_safe`, `bm25_delete_document_safe`, `merge_deltas_safe`)

#### UNLOGGED Table Support

- ✅ **`unlogged` parameter**: Added to `add_faceting_to_table()` for bulk load performance
- ✅ **Conversion functions**: `set_table_unlogged()`, `set_table_logged()`, `check_table_logging_status()`
- ✅ **Bulk load workflow**: `bulk_load_with_unlogged()` for complete UNLOGGED → LOGGED workflow
- ✅ **Performance**: 2-3x faster bulk loads, 100% WAL reduction during load

#### pg_cron Integration

- ✅ **`merge_deltas_all()`**: Merge deltas for all registered tables
- ✅ **`merge_deltas_smart()`**: Smart merging based on thresholds
- ✅ **`delta_status()`**: Monitor delta status across tables
- ✅ **`check_delta_health()`**: Health check with alert levels
- ✅ **`merge_deltas_with_history()`**: Merge with automatic history tracking
- ✅ **`delta_merge_history` table**: Tracks all merge operations

#### Nouvelles Fonctions SQL

- `facets.set_table_unlogged(p_table, p_include_deltas, p_include_bm25)`
- `facets.set_table_logged(p_table, p_include_deltas, p_include_bm25)`
- `facets.check_table_logging_status(p_table)`
- `facets.verify_before_logged_conversion(p_table)`
- `facets.bulk_load_with_unlogged(p_table, key, facets, chunk_bits, p_source_query)`
- `facets.merge_deltas_all()`
- `facets.merge_deltas_smart(p_table, p_min_delta_count, p_max_delta_age)`
- `facets.delta_status()`
- `facets.merge_deltas_with_history(p_table)`
- `facets.check_delta_health()`
- `facets.bm25_index_document_safe(...)`
- `facets.bm25_delete_document_safe(p_table, p_doc_id)`
- `facets.merge_deltas_safe(p_table)`

#### Introspection Functions (Frontend Support)

- ✅ **`list_table_facets()`**: Full facet definitions for a table
- ✅ **`list_table_facet_names()`**: Simple array of facet names
- ✅ **`list_table_facets_with_types()`**: JSONB object with facet metadata
- ✅ **`list_table_facets_simple()`**: Simple name/type pairs
- ✅ **`describe_table()`**: Table-level metadata (key column, BM25 language, etc.)
- ✅ **`list_tables()`**: List all registered faceted tables
- ✅ **`get_facet_hierarchy()`**: Parent-child relationships for tree UIs
- ✅ **`list_table_facets_for_ui()`**: Facets with UI component hints (dropdown, checkbox, tree, etc.)
- ✅ **`introspect()`**: Single API endpoint returning everything needed for frontend configuration

### Version 0.4.2 (Décembre 2024)

**Fonctions Helper BM25 et Configuration Simplifiée**

#### Nouvelles Fonctions SQL

| Fonction | Description |
|----------|-------------|
| `bm25_create_sync_trigger()` | Crée un trigger pour synchroniser automatiquement l'index BM25 lors des INSERT/UPDATE/DELETE |
| `bm25_drop_sync_trigger()` | Supprime le trigger de synchronisation BM25 |
| `bm25_rebuild_index()` | Reconstruit l'index BM25 avec support parallèle automatique |
| `bm25_status()` | Affiche le statut de tous les index BM25 |
| `bm25_progress()` | Affiche la progression de l'indexation pour une table |
| `bm25_active_processes()` | Liste les processus BM25 en cours d'exécution |
| `bm25_cleanup_dblinks()` | Déconnecte les connexions dblink orphelines |
| `bm25_cleanup_staging()` | Supprime les tables staging orphelines |
| `bm25_kill_stuck()` | Termine les processus BM25 bloqués |
| `bm25_full_cleanup()` | Nettoyage complet (dblinks + staging + processus bloqués) |
| `setup_table_with_bm25()` | Configuration one-stop pour facettes + BM25 |

#### Fonctions Debug/Analysis Natives (Zig)

Nouvelles fonctions implémentées en Zig pour l'analyse et le debug des index BM25 :

- **`bm25_term_stats(table_id, limit)`** : Équivalent de `ts_stat()` pour BM25 - retourne les termes les plus fréquents avec `ndoc` et `nentry`
- **`bm25_doc_stats(table_id, limit)`** : Top N documents par longueur
- **`bm25_collection_stats(table_id)`** : Statistiques globales (total_documents, avg_document_length, unique_terms)
- **`bm25_explain_doc(table_id, doc_id, k1, b)`** : Analyse les poids BM25 de chaque terme dans un document - utile pour comprendre pourquoi un document est classé haut/bas

Ces fonctions sont implémentées en Zig natif (`src/bm25/stats_native.zig`) pour des performances optimales.

#### Indexation Parallèle Optimisée

La fonction `bm25_index_documents_parallel()` a été **entièrement réécrite** avec une approche lock-free :

- **Avant (0.4.1)** : Utilisation d'OFFSET pour partitionner le travail (lent avec contention)
- **Après (0.4.2)** : Tables staging privées par worker avec ROW_NUMBER (90-95% plus rapide)

**Architecture de l'indexation parallèle 0.4.2 :**

```
Phase 1: Création table staging avec ROW_NUMBER()
    ↓
Phase 2: Workers parallèles écrivent dans leurs tables privées
    ↓
Phase 3: Merge des résultats dans bm25_index et bm25_documents
    ↓
Phase 4: Cleanup et recalcul des statistiques
```

#### Nouvelles Fonctions Go

| Fonction | Description |
|----------|-------------|
| `BM25CreateSyncTrigger()` | Crée un trigger de synchronisation BM25 |
| `BM25DropSyncTrigger()` | Supprime le trigger de synchronisation |
| `BM25RebuildIndex()` | Reconstruit l'index BM25 |
| `BM25Status()` | Récupère le statut de tous les index |
| `BM25Progress()` | Récupère la progression de l'indexation |
| `BM25ActiveProcesses()` | Liste les processus actifs |
| `BM25CleanupDblinks()` | Nettoie les connexions dblink |
| `BM25CleanupStaging()` | Nettoie les tables staging |
| `BM25KillStuck()` | Termine les processus bloqués |
| `BM25FullCleanup()` | Nettoyage complet |
| `SetupTableWithBM25()` | Configuration one-stop |
| `BM25TermStats()` | Statistiques des termes (top N par fréquence) |
| `BM25DocStats()` | Statistiques des documents (top N par longueur) |
| `BM25CollectionStats()` | Statistiques globales de la collection |
| `BM25ExplainDoc()` | Analyse des poids BM25 par terme pour un document |

#### Nouveaux Types Go

- `BM25RebuildOptions` : Options pour la reconstruction d'index
- `BM25StatusResult` : Résultat du statut d'un index
- `BM25ProgressResult` : Progression de l'indexation
- `BM25ActiveProcessResult` : Processus actif
- `BM25CleanupResult` : Résultat de nettoyage
- `BM25KillStuckResult` : Résultat de terminaison de processus
- `BM25FullCleanupResult` : Résultat de nettoyage complet
- `SetupTableWithBM25Options` : Options de configuration
- `BM25TermStatsResult` : Statistiques d'un terme (term_text, ndoc, nentry)
- `BM25DocStatsResult` : Statistiques d'un document (doc_id, doc_length, unique_terms)
- `BM25CollectionStatsResult` : Statistiques de collection (total_documents, avg_document_length, etc.)
- `BM25ExplainDocResult` : Explication d'un terme dans un document (term_text, tf, df, idf, bm25_weight)

#### Exemple d'Utilisation Simplifiée

**Avant (0.4.1) - Configuration manuelle :**

```sql
-- 1. Enregistrer la table
SELECT facets.add_faceting_to_table('my_schema.products', 'id', ARRAY[...]);

-- 2. Configurer la langue BM25
SELECT facets.bm25_set_language('my_schema.products', 'english');

-- 3. Créer une fonction de trigger personnalisée
CREATE OR REPLACE FUNCTION my_schema.products_bm25_trigger() ...

-- 4. Créer le trigger
CREATE TRIGGER products_bm25_sync ...

-- 5. Indexer les documents (boucle manuelle ou fonction personnalisée)
FOR doc IN SELECT * FROM my_schema.products LOOP ...

-- 6. Recalculer les statistiques
SELECT facets.bm25_recalculate_statistics('my_schema.products');
```

**Après (0.4.2) - Configuration one-stop :**

```sql
SELECT facets.setup_table_with_bm25(
    'my_schema.products'::regclass,
    'id',           -- id_column
    'content',      -- content_column
    ARRAY[facets.plain_facet('category')],
    'english',      -- language
    true,           -- create_trigger
    NULL,           -- chunk_bits (auto)
    true,           -- populate_facets
    true,           -- build_bm25_index
    0               -- bm25_workers (auto)
);
```

**Client Go :**

```go
err := search.SetupTableWithBM25(ctx, pgfaceting.SetupTableWithBM25Options{
    IDColumn:      "id",
    ContentColumn: "content",
    FacetDefinitions: []string{
        "facets.plain_facet('category')",
    },
    Language:       "english",
    CreateTrigger:  true,
    PopulateFacets: true,
    BuildBM25Index: true,
    BM25Workers:    0, // auto
})
```

#### Monitoring et Maintenance

```sql
-- Vérifier le statut de tous les index BM25
SELECT * FROM facets.bm25_status();

-- Progression de l'indexation
SELECT * FROM facets.bm25_progress('my_schema.products');

-- Processus actifs
SELECT * FROM facets.bm25_active_processes();

-- Nettoyage complet en cas de problème
SELECT * FROM facets.bm25_full_cleanup();
```

---

### Version 0.4.1 (Décembre 2024)

**Améliorations du Client Go**

#### Nouvelles Fonctions Go

| Fonction | Description |
|----------|-------------|
| `BM25GetMatchesBitmap()` | Retourne un roaring bitmap des documents correspondant à une requête BM25 |
| `IndexDocumentsBatch()` | Indexation batch efficace de plusieurs documents |
| `IndexDocumentsParallel()` | Indexation parallèle utilisant dblink pour les grands volumes |

#### Nouveaux Types Go

- `BM25Document` : Structure pour représenter un document à indexer en batch
- `ParallelIndexResult` : Résultat d'indexation par worker parallèle

#### Corrections et Améliorations

- Mise à jour des tests pour inclure l'indexation BM25 dans le setup
- Tests dédiés pour les fonctions BM25 (`TestBM25Functions`)
- Documentation mise à jour avec exemples d'indexation batch

### Version 0.4.0 (2024)

**Support BM25 Complet**

#### Nouvelles Fonctions BM25

| Fonction | Description |
|----------|-------------|
| `bm25_index_document()` | Indexe un document pour la recherche BM25 |
| `bm25_delete_document()` | Supprime un document de l'index BM25 |
| `bm25_search()` | Recherche avec scoring BM25 |
| `bm25_score()` | Calcule le score BM25 pour un document |
| `bm25_recalculate_statistics()` | Recalcule les statistiques de collection |
| `bm25_get_statistics()` | Récupère les statistiques de collection |
| `bm25_index_documents_batch()` | Indexation batch de plusieurs documents |
| `bm25_index_documents_parallel()` | Indexation parallèle pour grandes collections |

#### Implémentation Zig Native

- **Tokenisation** : Utilise le tokenizer Zig natif (`tokenizer_native.tokenizeNative()`) qui appelle directement l'API C de PostgreSQL `to_tsvector`, évitant l'overhead de `ts_debug()` ou `ts_stat()`
- **Index inversé** : Stockage efficace avec Roaring Bitmaps pour les posting lists
- **Scoring BM25+** : Implémentation optimisée avec calcul d'IDF
- **Gestion mémoire** : Évite les allocations dynamiques pendant les opérations SPI
- **Recherche optimisée** : Phases séparées pour éviter les connexions SPI imbriquées
- **Worker SQL** : `bm25_index_worker_lockfree` utilise maintenant `facets.test_tokenize_only()` (tokenizer Zig natif) au lieu de `ts_debug()` pour de meilleures performances

#### Client Go

Nouvelles méthodes BM25 :
- `IndexDocument()` : Indexe un document
- `DeleteDocument()` : Supprime un document
- `BM25Search()` : Recherche avec options configurables
- `BM25Score()` : Calcule le score pour un document
- `RecalculateStatistics()` : Recalcule les statistiques
- `GetStatistics()` : Récupère les statistiques

#### Tables Internes

- `facets.bm25_index` : Index inversé (term_hash → doc_ids, term_freqs)
- `facets.bm25_documents` : Métadonnées des documents (doc_id, doc_length)
- `facets.bm25_statistics` : Statistiques de collection (total_documents, avg_document_length)

#### Paramètres BM25

- **k1** (défaut: 1.2) : Contrôle la saturation de la fréquence de terme
- **b** (défaut: 0.75) : Contrôle la normalisation par longueur de document
- **language** : Configuration de recherche textuelle PostgreSQL (défaut: "english")
- **prefix_match** : Correspondance par préfixe
- **fuzzy_match** : Correspondance floue avec seuil configurable

### Version 0.3.6 (Décembre 2024)

**Optimisation Bitmap pour Grands Ensembles de Données**

#### Nouvelles Fonctions SQL

| Fonction | Description |
|----------|-------------|
| `filter_documents_by_facets_bitmap()` | Retourne un bitmap au lieu d'un tableau |
| `hierarchical_facets_bitmap()` | Calcule les facettes depuis un bitmap |
| `_get_regular_facets()` | Helper pour facettes non-hiérarchiques |

#### Optimisations Internes

- `search_documents_with_facets` utilise maintenant les bitmaps en interne
- Remplacement de `ANY(array)` par `rb_contains(bitmap, id)`
- Évite les conversions array↔bitmap inutiles

#### Client Go

Nouvelles méthodes:
- `FilterDocumentsByFacetsBitmap()`
- `GetBitmapCardinality()`
- `HierarchicalFacetsBitmap()`
- `SearchWithFacetsBitmap()`

#### Infrastructure de Tests

- Tests Go avec Docker intégré (`make test`)
- Mode CI avec `PGFACETS_TEST_FAIL_ON_NO_DB=true`
- Tests spécifiques pour les optimisations bitmap

#### Corrections de Bugs

- Fix: Parsing des timestamps PostgreSQL sans timezone dans le client Go
- Fix: Cast incorrect `::jsonb::bytea` dans `HierarchicalFacetsBitmap` Go

### Version 0.3.5

- Version initiale avec support BM25 et recherche vectorielle
- API JSONB pour les filtres de facettes
- Intégration avec pgvector

---

## Contribuer

### Structure du Projet

```
pg_facets/
├── src/                    # Code Zig
├── sql/                    # Scripts SQL d'extension
├── test/                   # Tests SQL
├── examples/golang/        # Client Go + tests
├── docker/                 # Configuration Docker
├── build.zig              # Build Zig
└── DOCUMENTATION.md       # Cette documentation
```

### Lancer les Tests

```bash
# Tests SQL
cd test && ./run_all_tests.sh

# Tests Go
cd examples/golang && make test
```

### Conventions de Code

- **Zig** : Style standard Zig, pas de `unsafe` sauf nécessité absolue
- **SQL** : Fonctions dans le schéma `facets.`
- **Go** : Style standard Go avec `gofmt`