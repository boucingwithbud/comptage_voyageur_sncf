WITH cte AS (
    SELECT DISTINCT
        tranche_horaire
    FROM {{ref("stg_comptage")}}
)
SELECT ROW_NUMBER() OVER (ORDER BY tranche_horaire) AS tranche_horaire_sk,
tranche_horaire
FROM cte;