WITH cte AS (
    SELECT DISTINCT
        code_gare,
        nom_gare
    FROM {{ref("stg_comptage")}}
)
SELECT ROW_NUMBER() OVER (ORDER BY code_gare) AS gare_sk,
*
FROM cte;