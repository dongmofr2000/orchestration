-- models/qualified_rh.sql

WITH rh_clean AS (
    -- Sélection et nettoyage de base (utiliser les noms de colonnes réels de votre BDD)
    SELECT
        "ID salarié" AS id_salarie,
        "Salaire brut" AS salaire_brut,
        "Adresse du domicile" AS adresse_domicile,
        "Moyen de déplacement" AS moyen_de_deplacement,
        -- Rendre le salaire numérique pour les calculs
        CAST("Salaire brut" AS NUMERIC) AS salaire_brut_num 
    FROM rh_brut
),

-- Simulation de la validation de distance (remplacer par une API Google Maps en production)
distance_validation AS (
    SELECT
        id_salarie,
        -- Simulation: Distance Domicile-Travail < 15/25 km
        CASE
            WHEN moyen_de_deplacement IN ('Marche/running', 'Marche') THEN 'Valide' 
            WHEN moyen_de_deplacement IN ('Vélo/Trottinette/Autres', 'Vélo') THEN 'Valide'
            -- Pour le POC, on suppose qu'un certain pourcentage est valide.
            ELSE 'Non-Valide'
        END AS distance_validee_statut
    FROM rh_clean
)

SELECT
    r.*,
    d.distance_validee_statut,
    -- Calcul de l'éligibilité à la Prime Sportive
    CASE 
        WHEN r.moyen_de_deplacement IN ('Marche/running', 'Vélo/Trottinette/Autres') 
             AND d.distance_validee_statut = 'Valide' 
        THEN TRUE
        ELSE FALSE
    END AS is_eligible_prime_sportive
    
FROM rh_clean r
LEFT JOIN distance_validation d ON r.id_salarie = d.id_salarie