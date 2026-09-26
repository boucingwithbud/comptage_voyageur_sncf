WITH cte AS (
    SELECT DISTINCT
        axe,
        ligne
    FROM {{ref("stg_comptage")}}
)
SELECT ROW_NUMBER() OVER (ORDER BY ligne, axe) AS ligne_sk,
ligne,
axe
FROM cte;