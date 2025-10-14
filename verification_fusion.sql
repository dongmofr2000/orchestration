--------------------------------------------------------------------------------
-- VÉRIFICATION DU CONTENU DE LA TABLE FINALE
-- Cette requête permet de s'assurer que les jointures ont réussi 
-- et que les données des trois sources sont correctement agrégées.
--------------------------------------------------------------------------------

SELECT 
    -- ERP
    product_id, 
    onsale_web,
    price, 
    stock_quantity, 
    
    -- Liaison
    id_web, 
    
    -- WEB
    total_sales, 
    post_title
FROM 
    produits_fusionnes 
LIMIT 10;
