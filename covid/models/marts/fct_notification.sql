WITH notifications AS (
    SELECT * FROM {{ ref('stg_notifications') }}
),

locations AS (
    SELECT * FROM {{ ref('dim_location') }}
),

test_type AS (
    SELECT * FROM {{ ref('dim_test_type') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY NOTIFICATION_DATE) AS SK_NOTIFICATION,
    CAST(TO_VARCHAR(t1.NOTIFICATION_DATE, 'YYYYMMDD') AS INT) AS SK_TIME,
    t2.SK_LOCATION,
    t3.SK_TEST_TYPE,
    t1.PATIENT_GENDER, 
    t1.PATIENT_IS_HEALTH_PROFESSIONAL, 
    t1.PATIENT_AGE, 
    t1.PATIENT_RACE, 
    t1.PATIENT_IS_VACCINATED, 
    t1.QUANTITY_VACCINE_DOSES AS PATIENT_QUANTITY_VACCINE_DOSES, 
    t1.SYMPTOMS_START_DATE,
    t1.TEST_DATE,
    CASE --Criar regra para verificar caso chegue um campo novo
        WHEN T1.TEST_RESULT IN ('Não Reagente', 'Não Detectável') THEN 0
        WHEN T1.TEST_RESULT IN ('Reagente', 'Detectável') THEN 1
    END AS PATIENT_COVID_POSITIVE,
    t1.SYMPTOMS,
    GETDATE() AS UPDATE_DATE
FROM notifications AS t1
INNER JOIN locations AS t2
ON t1.NOTIFICATION_IBGE_CITY_CODE = t2.IBGE_CITY_CODE
INNER JOIN test_type AS t3
ON t1.TEST_TYPE = T3.TEST_TYPE
WHERE TEST_STATUS = 'Concluído'