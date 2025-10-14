-- Fichier : calcul_ca.sql
-- Calcule le Chiffre d'Affaires (CA) par produit et le CA global à partir de la table fusionnée.
-- Se concentre uniquement sur les produits en vente sur le web (onsale_web = 1).

-- ÉTAPE 1: Calcul du CA par produit (Affichage détaillé)
-- La colonne 'CA_produit' est ajoutée pour l'analyse
SELECT
    product_id,
    post_title,
    price,
    total_sales,
    -- Calcul du CA pour chaque ligne
    (price * total_sales) AS CA_produit,
    onsale_web
FROM
    produits_fusionnes
WHERE
    -- Filtre sur les produits destinés à la vente en ligne et qui ont un prix
    onsale_web = 1 AND price IS NOT NULL
ORDER BY
    CA_produit DESC;

-- ÉTAPE 2: Calcul du Chiffre d'Affaires Global (Résultat final)
-- Somme de tous les 'CA_produit' pour obtenir la métrique globale
SELECT
    SUM(price * total_sales) AS CA_Global_Total_Web
FROM
    produits_fusionnes
WHERE
    onsale_web = 1 AND price IS NOT NULL;

-- Note : Le script Python lira le résultat de la DERNIÈRE requête (CA_Global_Total_Web)
-- pour l'afficher dans le terminal.
