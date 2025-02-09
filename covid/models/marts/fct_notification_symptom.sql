WITH notifications AS (
    SELECT * FROM {{ ref('fct_notification') }}
),

symptoms_exploded AS (
    SELECT
        n.SK_NOTIFICATION,  -- Chave estrangeira para a fato_notificacao
        TRIM(s.value) AS symptom  -- Pegamos cada sintoma separadamente
    FROM notifications n,
    LATERAL FLATTEN(input => SPLIT(n.SYMPTOMS, ',')) s  -- Explode os sintomas em linhas
),

symptoms AS (
    SELECT * FROM {{ ref('dim_symptom') }}
)

SELECT
    --ROW_NUMBER() OVER (ORDER BY SK_NOTIFICATION) AS fato_covid_sintoma_id,  -- Chave primária sequencial
    t1.SK_NOTIFICATION,  -- Ligação com a fato de notificações
    t2.SK_SYMPTOM
FROM symptoms_exploded t1
INNER JOIN symptoms t2
ON t1.symptom = t2.symptom