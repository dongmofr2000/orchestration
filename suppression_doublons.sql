--------------------------------------------------------------------------------
-- ÉTAPE 1: Chargement des Données Brutes et Nettoyage Primaire (Type/Format/Filtre)
-- On utilise l'encodage 'CP1252' et on filtre les IDs non numériques.
--------------------------------------------------------------------------------

-- Table 1: Fichier_erp.csv
-- On corrige le prix (virgule décimale en point)
CREATE OR REPLACE TABLE raw_erp AS
SELECT
    product_id,
    onsale_web,
    -- Remplacement de la virgule par un point pour convertir en NUMERIC
    CAST(REPLACE(price, ',', '.') AS NUMERIC) AS price, 
    stock_quantity,
    stock_status
FROM READ_CSV_AUTO('Fichier_erp.csv', SEP = ';', HEADER = true, ENCODING = 'CP1252');

-- Table 2: fichier_liaison.csv
-- Standardisation: Conversion de id_web en INTEGER et exclusion des IDs non numériques (ex: '13127-1').
CREATE OR REPLACE TABLE raw_liaison AS
SELECT
    product_id,
    -- On caste id_web en INTEGER pour assurer la cohérence avec la table web
    CAST(id_web AS INTEGER) AS id_web 
FROM READ_CSV_AUTO('fichier_liaison.csv', SEP = ';', HEADER = true, ENCODING = 'CP1252')
-- FILTRE : Exclure les IDs web qui ne sont pas strictement numériques, en gardant les NULL
WHERE id_web IS NULL OR REGEXP_MATCHES(id_web, '^\d+$');


-- Table 3: Fichier_web.csv
-- Standardisation: Conversion de sku/total_sales en INTEGER et exclusion des SKUs non numériques.
CREATE OR REPLACE TABLE raw_web AS
SELECT
    CAST(sku AS INTEGER) AS id_web,
    CAST(total_sales AS INTEGER) AS total_sales,
    post_title
FROM READ_CSV_AUTO('Fichier_web.csv', SEP = ';', HEADER = true, ENCODING = 'CP1252')
-- FILTRE : Assure que le SKU est purement numérique avant la conversion en INTEGER
WHERE REGEXP_MATCHES(sku, '^\d+$');


--------------------------------------------------------------------------------
-- ÉTAPE 2: Dédoublonnage des Clés (Conserver une seule ligne par clé)
--------------------------------------------------------------------------------

-- 2.1 Dédoublonnage pour ERP (Clé: product_id)
-- On conserve le premier enregistrement pour chaque product_id
CREATE OR REPLACE TABLE deduped_erp AS
WITH ranked_erp AS (
    SELECT
        *,
        -- Attribution d'un rang pour chaque product_id
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) as rn
    FROM raw_erp
)
SELECT
    product_id,
    onsale_web,
    price,
    stock_quantity,
    stock_status
FROM ranked_erp
WHERE rn = 1; -- On garde uniquement le premier rang (le non-doublon)

-- 2.2 Dédoublonnage pour Liaison (Clé: product_id)
-- On conserve un seul id_web par product_id.
CREATE OR REPLACE TABLE deduped_liaison AS
WITH ranked_liaison AS (
    SELECT
        *,
        -- On partitionne par product_id et on ordonne pour s'assurer que si un ID WEB existe,
        -- on le conserve en priorité (id_web IS NULL ASC met les NULL en dernier).
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY id_web IS NULL ASC, product_id
        ) as rn
    FROM raw_liaison
)
SELECT
    product_id,
    id_web
FROM ranked_liaison
WHERE rn = 1; -- On garde le premier rang unique

-- 2.3 Dédoublonnage pour Web (Clé: id_web)
-- On conserve le premier enregistrement pour chaque id_web
CREATE OR REPLACE TABLE deduped_web AS
WITH ranked_web AS (
    SELECT
        *,
        -- Attribution d'un rang pour chaque id_web
        ROW_NUMBER() OVER (PARTITION BY id_web ORDER BY id_web) as rn
    FROM raw_web
)
SELECT
    id_web,
    total_sales,
    post_title
FROM ranked_web
WHERE rn = 1; -- On garde le premier rang unique


--------------------------------------------------------------------------------
-- ÉTAPE 3: Validation des résultats (Comptage des doublons supprimés)
-- Cette partie est utile pour vérifier l'efficacité des étapes 1 et 2.
--------------------------------------------------------------------------------
SELECT 
    'Fichier_erp.csv' AS Source,
    (SELECT COUNT(*) FROM raw_erp) AS Avant,
    (SELECT COUNT(*) FROM deduped_erp) AS Après,
    (SELECT COUNT(*) FROM raw_erp) - (SELECT COUNT(*) FROM deduped_erp) AS Doublons
UNION ALL
SELECT 
    'fichier_liaison.csv',
    (SELECT COUNT(*) FROM raw_liaison),
    (SELECT COUNT(*) FROM deduped_liaison),
    (SELECT COUNT(*) FROM raw_liaison) - (SELECT COUNT(*) FROM deduped_liaison) AS Doublons
UNION ALL
SELECT 
    'Fichier_web.csv',
    (SELECT COUNT(*) FROM raw_web),
    (SELECT COUNT(*) FROM deduped_web),
    (SELECT COUNT(*) FROM raw_web) - (SELECT COUNT(*) FROM deduped_web) AS Doublons;
