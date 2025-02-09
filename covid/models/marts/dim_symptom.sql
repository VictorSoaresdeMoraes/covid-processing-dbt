WITH notifications AS (
    SELECT * FROM {{ ref('stg_notifications') }}
)

SELECT ROW_NUMBER() OVER (ORDER BY SYMPTOM) AS SK_SYMPTOM, * 
FROM
    (
        SELECT DISTINCT 
            --MD5(TRIM(REPLACE(value, '"', ''))) AS SK_SYMPTOM, 
            TRIM(REPLACE(value, '"', '')) AS SYMPTOM
        FROM (
            SELECT SPLIT(SYMPTOMS, ',') AS SYMPTOMS_ARRAY
            FROM notifications
        ) T,
        LATERAL FLATTEN(INPUT => SYMPTOMS_ARRAY) AS value
    ) S