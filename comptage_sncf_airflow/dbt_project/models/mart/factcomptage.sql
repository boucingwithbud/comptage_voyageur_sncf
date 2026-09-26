SELECT
d.date_key,
g.gare_sk,
l.ligne_sk,
th.tranche_horaire_sk,
c.somme_de_montants

FROM {{ref("stg_comptage")}} c
JOIN {{ref("dimdate")}} d ON c.date = d.date_day
JOIN {{ref("dimgare")}} g ON c.code_gare = g.code_gare
JOIN {{ref("dimligne")}} l ON c.ligne = l.ligne
JOIN {{ref("dimtranchehoraire")}} th ON c.tranche_horaire = th.tranche_horaire;