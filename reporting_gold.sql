-- models/reporting_gold.sql

WITH 
-- 1. Agrégation des activités (Comptage des 12 derniers mois)
activites_count AS (
    SELECT 
        "ID salarié" AS id_salarie,
        COUNT(*) AS nb_activites_12_mois
    FROM activites_brutes
    -- Condition glissante : seulement les activités des 12 derniers mois (simulation)
    WHERE "Date" >= date(now() - interval '12 months')
    GROUP BY 1
),

-- 2. Jointure avec les données RH qualifiées (Prime)
eligible_data AS (
    SELECT 
        r.*,
        COALESCE(a.nb_activites_12_mois, 0) AS nb_activites_12_mois,
        
        -- Calcul de l'éligibilité aux 5 jours Bien-être (utilisation du paramètre Kestra)
        CASE 
            -- Nécessite 15 activités (ou {{ var('min_activites') }})
            WHEN COALESCE(a.nb_activites_12_mois, 0) >= {{ var('min_activites') }}
            THEN TRUE
            ELSE FALSE
        END AS is_eligible_jours_bien_etre
        
    FROM {{ ref('qualified_rh') }} r
    LEFT JOIN activites_count a ON r.id_salarie = a.id_salarie
)

-- 3. CALCUL DE L'IMPACT FINANCIER
SELECT
    id_salarie,
    salaire_brut_num,
    is_eligible_prime_sportive,
    is_eligible_jours_bien_etre,
    nb_activites_12_mois,

    -- Impact financier individuel de la prime (5% du salaire)
    CASE 
        WHEN is_eligible_prime_sportive THEN salaire_brut_num * {{ var('prime_rate') }}
        ELSE 0
    END AS cout_prime_sportive,

    -- Impact financier des 5 jours (basé sur le coût journalier moyen)
    CASE 
        WHEN is_eligible_jours_bien_etre THEN 5 * {{ var('cout_journalier_moyen') }}
        ELSE 0
    END AS cout_jours_bien_etre,

    -- KPI principal: Coût total par employé
    (CASE WHEN is_eligible_prime_sportive THEN salaire_brut_num * {{ var('prime_rate') }} ELSE 0 END) +
    (CASE WHEN is_eligible_jours_bien_etre THEN 5 * {{ var('cout_journalier_moyen') }} ELSE 0 END)
    AS cout_total_employe
    
FROM eligible_data