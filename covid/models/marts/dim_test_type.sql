WITH notifications AS (
    SELECT * FROM {{ ref('stg_notifications') }}
)

SELECT DISTINCT 
    MD5(TEST_TYPE) AS SK_TEST_TYPE,
    TEST_TYPE, 
    '' AS TEST_TYPE_DESCRIPTION,
    GETDATE() AS UPDATE_DATE
FROM notifications
WHERE TEST_TYPE IS NOT NULL
