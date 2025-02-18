WITH notifications AS (
    SELECT * FROM {{ ref('stg_notifications') }}
)

SELECT DISTINCT 
    MD5(TRIM(REPLACE(value, '"', ''))) AS SK_SYMPTOM, 
    TRIM(REPLACE(value, '"', '')) AS SYMPTOM,
    GETDATE() AS UPDATE_DATE
FROM (
    SELECT SPLIT(SYMPTOMS, ',') AS SYMPTOMS_ARRAY
    FROM notifications
) T,
LATERAL FLATTEN(INPUT => SYMPTOMS_ARRAY) AS value