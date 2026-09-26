SELECT DISTINCT
    annee::INT AS annee,
    TRIM(axe)::STRING AS axe,
    TRIM(code_gare)::STRING AS code_gare,
    TRIM(date)::DATE AS date,
    TRIM(ligne)::STRING AS ligne,
    TRIM(nom_gare)::STRING AS nom_gare,
    TRIM(somme_de_montants)::INT AS somme_de_montants,
    TRIM(tranche_horaire)::STRING AS tranche_horaire,
    TRIM(type_jour)::STRING AS type_jour
FROM {{ source('bronze', 'comptage') }}
