# pg_facets - Usage Guide

## Table des matières

1. [Installation](#installation)
2. [Concepts de base](#concepts-de-base)
3. [Configuration initiale](#configuration-initiale)
4. [Types de facettes](#types-de-facettes)
5. [Fonctions principales](#fonctions-principales)
6. [Scénarios de test](#scénarios-de-test)
7. [Intégration avec la recherche](#intégration-avec-la-recherche)
8. [Maintenance](#maintenance)

---

## Installation

```sql
-- Prérequis : roaringbitmap doit être installé
CREATE EXTENSION IF NOT EXISTS roaringbitmap;

-- Install pg_facets
CREATE EXTENSION IF NOT EXISTS pg_facets;

-- Optionnel : extension vector pour la recherche vectorielle
CREATE EXTENSION IF NOT EXISTS vector;
```

---

## Concepts de base

### Qu'est-ce qu'une facette ?

Une **facette** est un attribut filtrable d'un document. Par exemple :
- Catégorie de produit
- Plage de prix
- Date de création
- Tags

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Table principale                          │
│  (ex: documents)                                             │
│  ┌────┬─────────┬──────────┬──────────┬─────────┐          │
│  │ id │ title   │ category │ price    │ tags    │          │
│  └────┴─────────┴──────────┴──────────┴─────────┘          │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              Table des facettes (auto-générée)               │
│  (ex: documents_facets)                                      │
│  ┌──────────┬─────────────┬──────────────────────┐          │
│  │ facet_id │ facet_value │ postinglist (bitmap) │          │
│  └──────────┴─────────────┴──────────────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

Les **postinglists** sont des bitmaps roaring qui contiennent les IDs des documents correspondant à chaque valeur de facette.

---

## Configuration initiale

### Étape 1 : Créer votre table de données

```sql
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT,
    subcategory TEXT,
    price DECIMAL(10,2),
    tags TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    in_stock BOOLEAN DEFAULT true
);
```

### Étape 2 : Ajouter le faceting

```sql
SELECT facets.add_faceting_to_table(
    'products',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category'),
        facets.plain_facet('subcategory'),
        facets.bucket_facet('price', buckets => ARRAY[0, 50, 100, 200, 500, 1000]),
        facets.array_facet('tags'),
        facets.datetrunc_facet('created_at', 'month'),
        facets.boolean_facet('in_stock')
    ],
    populate => true  -- Remplir immédiatement les facettes
);
```

---

## Types de facettes

### 1. Plain Facet (Valeur simple)

Pour les colonnes avec des valeurs discrètes (catégories, statuts, etc.)

```sql
facets.plain_facet('category')

-- Avec un nom personnalisé
facets.plain_facet('category', 'product_category')
```

### 2. Array Facet (Tableau)

Pour les colonnes de type `TEXT[]` (tags, labels, etc.)

```sql
facets.array_facet('tags')
```

### 3. Bucket Facet (Plages numériques)

Pour créer des plages de valeurs numériques

```sql
-- Crée les buckets: 0-50, 50-100, 100-200, 200-500, 500-1000, 1000+
facets.bucket_facet('price', buckets => ARRAY[0, 50, 100, 200, 500, 1000])
```

### 4. DateTrunc Facet (Dates tronquées)

Pour grouper les dates par période

```sql
-- Par mois
facets.datetrunc_facet('created_at', 'month')

-- Par année
facets.datetrunc_facet('created_at', 'year')

-- Par jour
facets.datetrunc_facet('created_at', 'day')
```

### 5. Boolean Facet

Pour les colonnes booléennes

```sql
facets.boolean_facet('in_stock')
```

### 6. Joined Plain Facet (Jointure)

Pour les facettes basées sur des tables liées

```sql
facets.joined_plain_facet(
    'e.department',
    from_clause => 'employees e JOIN categories c ON c.owner_id = e.id',
    correlation => 'c.id = {TABLE}.category_id'
)
```

---

## Fonctions principales

### Obtenir les valeurs les plus fréquentes

```sql
-- Top 5 valeurs pour toutes les facettes
SELECT * FROM facets.top_values('products'::regclass);

-- Top 10 valeurs pour des facettes spécifiques
SELECT * FROM facets.top_values('products'::regclass, 10, ARRAY['category', 'tags']);
```

**Résultat :**
| facet_name | facet_value | cardinality | facet_id |
|------------|-------------|-------------|----------|
| category   | Electronics | 150         | 1        |
| category   | Books       | 120         | 1        |
| tags       | premium     | 200         | 4        |

### Compter les résultats avec filtres

```sql
-- Compter les documents avec category = 'Electronics'
SELECT * FROM facets.count_results(
    'products'::regclass::oid,
    filters => ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[]
);
```

### Filtrer les documents par facettes

```sql
-- Obtenir les IDs des documents filtrés
SELECT * FROM facets.filter_documents_by_facets(
    'public',
    '{"category": "Electronics", "in_stock": "true"}'::jsonb,
    'products'
);
```

### Recherche avec facettes (API 0.3.4)

```sql
-- Recherche textuelle avec facettes
SELECT * FROM facets.search_documents_with_facets(
    p_schema_name => 'public',
    p_table_name => 'products',
    p_query => 'laptop gaming',
    p_facets => '{"category": "Electronics"}'::jsonb,
    p_vector_column => NULL,  -- ou 'embedding' si vous avez des vecteurs
    p_content_column => 'name',
    p_limit => 10,
    p_offset => 0,
    p_min_score => 0.0,
    p_facet_limit => 5
);
```

**Résultat :**
```json
{
  "results": [...],
  "facets": [
    {"facet_name": "subcategory", "facet_id": 2, "values": [...]},
    {"facet_name": "price", "facet_id": 3, "values": [...]}
  ],
  "total_found": 42,
  "search_time": 15
}
```

---

## Scénarios de test

### Scénario 1 : Configuration de base

```sql
-- Créer le schéma de test
DROP SCHEMA IF EXISTS test_faceting CASCADE;
CREATE SCHEMA test_faceting;

-- Créer la table de test
CREATE TABLE test_facets.documents (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    category TEXT,
    tags TEXT[],
    price DECIMAL(10,2),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insérer des données de test
INSERT INTO test_facets.documents (title, category, tags, price) VALUES
    ('Laptop Pro', 'Electronics', ARRAY['premium', 'laptop'], 1299.99),
    ('Laptop Basic', 'Electronics', ARRAY['budget', 'laptop'], 499.99),
    ('Headphones', 'Electronics', ARRAY['premium', 'audio'], 299.99),
    ('Novel A', 'Books', ARRAY['fiction', 'bestseller'], 19.99),
    ('Novel B', 'Books', ARRAY['fiction'], 14.99),
    ('Cookbook', 'Books', ARRAY['cooking', 'bestseller'], 29.99),
    ('Chair', 'Furniture', ARRAY['office', 'ergonomic'], 399.99),
    ('Desk', 'Furniture', ARRAY['office', 'premium'], 599.99);

-- Ajouter le faceting
SELECT facets.add_faceting_to_table(
    'test_facets.documents',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category'),
        facets.array_facet('tags'),
        facets.bucket_facet('price', buckets => ARRAY[0, 100, 500, 1000])
    ],
    populate => true
);

-- Vérifier les facettes
SELECT * FROM facets.top_values('test_facets.documents'::regclass);
```

**Résultat attendu :**
```
 facet_name |  facet_value  | cardinality | facet_id 
------------+---------------+-------------+----------
 category   | Electronics   |           3 |        1
 category   | Books         |           3 |        1
 category   | Furniture     |           2 |        1
 tags       | premium       |           3 |        2
 tags       | laptop        |           2 |        2
 tags       | fiction       |           2 |        2
 price      | 2             |           3 |        3
 price      | 3             |           3 |        3
 price      | 4             |           2 |        3
```

### Scénario 2 : Mise à jour avec deltas

```sql
-- Insérer de nouveaux documents
INSERT INTO test_facets.documents (title, category, tags, price) VALUES
    ('Tablet', 'Electronics', ARRAY['premium', 'tablet'], 799.99),
    ('Magazine', 'Books', ARRAY['magazine'], 9.99);

-- Appliquer les deltas (utilise la fonction native Zig)
SELECT merge_deltas_native('test_facets.documents'::regclass);

-- Vérifier que les nouvelles valeurs sont présentes
SELECT * FROM facets.top_values('test_facets.documents'::regclass, 10, ARRAY['category']);
```

**Résultat attendu :**
```
 facet_name |  facet_value  | cardinality | facet_id 
------------+---------------+-------------+----------
 category   | Electronics   |           4 |        1
 category   | Books         |           4 |        1
 category   | Furniture     |           2 |        1
```

### Scénario 3 : Filtrage avec plusieurs facettes

```sql
-- Compter les documents Electronics avec tag 'premium'
SELECT * FROM facets.count_results(
    'test_facets.documents'::regclass::oid,
    filters => ARRAY[
        ROW('category', 'Electronics'),
        ROW('tags', 'premium')
    ]::facets.facet_filter[]
);
```

**Résultat attendu :** Les comptages pour chaque facette, filtrés par Electronics ET premium.

### Scénario 4 : Suppression et mise à jour

```sql
-- Supprimer un document
DELETE FROM test_facets.documents WHERE title = 'Magazine';

-- Mettre à jour un document
UPDATE test_facets.documents SET category = 'Home' WHERE title = 'Chair';

-- Appliquer les deltas
SELECT merge_deltas_native('test_facets.documents'::regclass);

-- Vérifier les changements
SELECT * FROM facets.top_values('test_facets.documents'::regclass, 10, ARRAY['category']);
```

### Scénario 5 : Facettes hiérarchiques

```sql
-- Créer une table avec hiérarchie
CREATE TABLE test_facets.products (
    id SERIAL PRIMARY KEY,
    name TEXT,
    main_category TEXT,
    category TEXT,
    sub_category TEXT,
    price DECIMAL(10,2)
);

INSERT INTO test_facets.products (name, main_category, category, sub_category, price) VALUES
    ('iPhone', 'Electronics', 'Phones', 'Smartphones', 999),
    ('Galaxy', 'Electronics', 'Phones', 'Smartphones', 899),
    ('MacBook', 'Electronics', 'Computers', 'Laptops', 1999),
    ('ThinkPad', 'Electronics', 'Computers', 'Laptops', 1499),
    ('iPad', 'Electronics', 'Tablets', 'Consumer', 799),
    ('Sofa', 'Furniture', 'Living Room', 'Seating', 1200),
    ('Table', 'Furniture', 'Living Room', 'Tables', 500);

-- Ajouter le faceting avec hiérarchie
SELECT facets.add_faceting_to_table(
    'test_facets.products',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('main_category'),
        facets.plain_facet('category'),
        facets.plain_facet('sub_category'),
        facets.bucket_facet('price', buckets => ARRAY[0, 500, 1000, 2000])
    ],
    populate => true
);

-- Obtenir les facettes hiérarchiques
SELECT facets.hierarchical_facets(
    'test_facets.products'::regclass::oid,
    n => 5
);
```

### Scénario 6 : Test des fonctions natives Zig

```sql
-- Test direct de build_filter_bitmap_native
SELECT rb_cardinality(
    build_filter_bitmap_native(
        'test_facets.documents'::regclass::oid,
        ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[]
    )
) AS electronics_count;

-- Test direct de get_facet_counts_native
SELECT * FROM get_facet_counts_native(
    'test_facets.documents'::regclass::oid,
    NULL,  -- pas de filtre bitmap
    ARRAY['category', 'tags'],
    5
);

-- Test direct de search_documents_native
SELECT * FROM search_documents_native(
    'test_facets.documents'::regclass::oid,
    ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[],
    10,  -- limit
    0    -- offset
);
```

---

## Intégration avec la recherche

### Recherche textuelle simple

```sql
SELECT * FROM facets.search_documents(
    p_schema_name => 'test_faceting',
    p_table_name => 'documents',
    p_query => 'laptop',
    p_content_column => 'title',
    p_limit => 10
);
```

### Recherche avec facettes et filtres

```sql
SELECT * FROM facets.search_documents_with_facets(
    p_schema_name => 'test_faceting',
    p_table_name => 'documents',
    p_query => 'laptop',
    p_facets => '{"category": "Electronics"}'::jsonb,
    p_content_column => 'title',
    p_limit => 10,
    p_facet_limit => 5
);
```

### Recherche vectorielle (si pgvector est installé)

```sql
-- Avec une table d'embeddings séparée
SELECT * FROM facets.search_documents_with_facets(
    p_schema_name => 'my_schema',
    p_table_name => 'documents',
    p_query => 'machine learning applications',
    p_facets => NULL,
    p_vector_column => 'embedding',
    p_content_column => 'content',
    p_vector_weight => 0.7,  -- 70% vector, 30% BM25
    p_min_score => 0.3,
    p_limit => 20
);
```

---

## Maintenance

### Appliquer les deltas manuellement

```sql
-- Pour une table spécifique
SELECT facets.merge_deltas('my_table'::regclass);

-- Pour toutes les tables
CALL facets.run_maintenance();
```

### Ajouter de nouvelles facettes

```sql
SELECT facets.add_facets(
    'my_table',
    facets => ARRAY[
        facets.plain_facet('new_column')
    ]
);
```

### Supprimer des facettes

```sql
SELECT facets.drop_facets('my_table', ARRAY['old_facet']);
```

### Supprimer complètement le faceting

```sql
SELECT facets.drop_faceting('my_table');
```

### Repeupler les facettes

```sql
-- Vider et repeupler
SELECT facets.populate_facets('my_table'::regclass);
```

---

## Bonnes pratiques

1. **Appliquer les deltas régulièrement** : Configurez un job cron pour appeler `facets.run_maintenance()` périodiquement.

2. **Choisir les bons types de facettes** : Utilisez `bucket_facet` pour les valeurs numériques continues, `array_facet` pour les tags.

3. **Limiter le nombre de facettes** : Trop de facettes peuvent ralentir les requêtes. Concentrez-vous sur les filtres les plus utilisés.

4. **Indexer les colonnes de facettes** : Créez des index sur les colonnes utilisées pour les facettes pour accélérer le peuplement.

5. **Monitorer les performances** : Utilisez `EXPLAIN ANALYZE` pour identifier les goulots d'étranglement.

---

## Dépannage

### Les facettes ne se mettent pas à jour

```sql
-- Vérifier s'il y a des deltas en attente
SELECT COUNT(*) FROM my_schema.my_table_facets_deltas;

-- Appliquer les deltas
SELECT merge_deltas_native('my_schema.my_table'::regclass);
```

### Erreur "Table not found"

```sql
-- Vérifier que la table est bien enregistrée
SELECT * FROM facets.faceted_table WHERE tablename = 'my_table';
```

### Performances lentes

```sql
-- Vérifier la cardinalité des bitmaps
SELECT facet_id, facet_value, rb_cardinality(postinglist) 
FROM my_schema.my_table_facets 
ORDER BY rb_cardinality(postinglist) DESC 
LIMIT 10;
```

