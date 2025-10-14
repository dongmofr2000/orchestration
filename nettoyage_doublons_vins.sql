--------------------------------------------------------------------------------
-- ÉTAPE 1: Chargement et Préparation des Données Brutes (avec Nettoyage Primaire)
-- Utilisation de READ_CSV_AUTO avec ENCODING = 'CP1252' et standardisation des ID WEB en INTEGER.
--------------------------------------------------------------------------------

-- Table 1: Fichier_erp.csv
-- Correction des prix (virgule décimale en point)
CREATE OR REPLACE TABLE raw_erp AS
SELECT
    product_id,
    onsale_web,
    -- Remplacement de la virgule par un point pour convertir en NUMERIC
    CAST(REPLACE(price, ',', '.') AS NUMERIC) AS price, 
    stock_quantity,
    stock_status
-- Correction: Utilisation de l'encodage CP1252
FROM READ_CSV_AUTO('Fichier_erp.csv', SEP = ';', HEADER = true, ENCODING = 'CP1252');

-- Table 2: fichier_liaison.csv
-- Standardisation: Conversion de id_web en INTEGER et exclusion des IDs non numériques (ex: '13127-1').
CREATE OR REPLACE TABLE raw_liaison AS
SELECT
    product_id,
    -- On caste id_web en INTEGER pour assurer la cohérence avec la table web
    CAST(id_web AS INTEGER) AS id_web 
-- Correction: Utilisation de l'encodage CP1252
FROM READ_CSV_AUTO('fichier_liaison.csv', SEP = ';', HEADER = true, ENCODING = 'CP1252')
-- NOUVEAU FILTRE : Exclure les IDs web qui ne sont pas strictement numériques (ex: '13127-1')
WHERE id_web IS NULL OR REGEXP_MATCHES(id_web, '^\d+$');


-- Table 3: Fichier_web.csv
-- Ajout d'un filtre pour exclure les SKUs non numériques (comme 'bon-cadeau-25-euros')
CREATE OR REPLACE TABLE raw_web AS
SELECT
    CAST(sku AS INTEGER) AS id_web,
    CAST(total_sales AS INTEGER) AS total_sales,
    post_title
FROM READ_CSV_AUTO('Fichier_web.csv', SEP = ';', HEADER = true, ENCODING = 'CP1252')
-- FILTRE : Assure que le SKU est purement numérique avant la conversion en INTEGER
WHERE REGEXP_MATCHES(sku, '^\d+$');


--------------------------------------------------------------------------------
-- ÉTAPE 2: Dédoublonnage (Application de la logique de conservation)
--------------------------------------------------------------------------------

-- 2.1 Dédoublonnage pour raw_erp
-- Conserver le premier enregistrement pour un product_id donné
CREATE OR REPLACE TABLE deduped_erp AS
WITH ranked_erp AS (
    SELECT
        *,
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
WHERE rn = 1;

-- 2.2 Dédoublonnage pour raw_liaison
-- Conserver l'enregistrement ayant un id_web non-NULL en priorité
CREATE OR REPLACE TABLE deduped_liaison AS
WITH ranked_liaison AS (
    SELECT
        *,
        -- Priorité: 1 si id_web est NOT NULL, 2 sinon. Puis on prend le premier product_id
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY id_web IS NULL ASC, product_id
        ) as rn
    FROM raw_liaison
)
SELECT
    product_id,
    id_web -- id_web est maintenant INTEGER ou NULL
FROM ranked_liaison
WHERE rn = 1;


-- 2.3 Dédoublonnage pour raw_web
-- Conserver le premier enregistrement pour un id_web donné
CREATE OR REPLACE TABLE deduped_web AS
WITH ranked_web AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id_web ORDER BY id_web) as rn
    FROM raw_web
)
SELECT
    id_web,
    total_sales,
    post_title
FROM ranked_web
WHERE rn = 1;


--------------------------------------------------------------------------------
-- ÉTAPE 3: Validation des résultats
-- Cette requête est la dernière du script et son résultat est affiché par Python.
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

