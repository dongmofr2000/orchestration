--------------------------------------------------------------------------------
-- SCRIPT DE FUSION DES DONNÉES NETTOYÉES
-- Crée la table finale 'produits_fusionnes'
--------------------------------------------------------------------------------

CREATE OR REPLACE TABLE produits_fusionnes AS
SELECT
    -- Colonnes de l'ERP (Base de vérité pour le stock et le prix)
    erp.product_id,
    erp.onsale_web,
    erp.price,
    erp.stock_quantity,
    erp.stock_status,
    
    -- Colonnes de la Liaison (Clé de jointure entre ERP et WEB)
    liaison.id_web,
    
    -- Colonnes du Web (Ventes et informations sur le produit)
    web.total_sales,
    web.post_title
    
FROM 
    -- 1. La table ERP est la base (LEFT)
    deduped_erp AS erp
    
-- 2. Jointure avec la table de liaison sur product_id (ERP -> LIAISON)
-- LEFT JOIN pour conserver tous les produits ERP, même ceux sans id_web
LEFT JOIN 
    deduped_liaison AS liaison 
ON 
    erp.product_id = liaison.product_id
    
-- 3. Jointure avec la table Web sur id_web (LIAISON -> WEB)
-- LEFT JOIN car certains produits ERP/Liaison n'auront pas de correspondance web
LEFT JOIN 
    deduped_web AS web 
ON 
    liaison.id_web = web.id_web
    
ORDER BY 
    erp.product_id;

--------------------------------------------------------------------------------
-- VALIDATION : Afficher le nombre de lignes (doit être égal au nombre de lignes ERP)
--------------------------------------------------------------------------------
SELECT 
    'Produits Fusionnés' AS table_name, 
    COUNT(*) AS row_count 
FROM 
    produits_fusionnes;
