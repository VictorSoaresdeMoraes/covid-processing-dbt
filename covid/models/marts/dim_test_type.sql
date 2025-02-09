WITH notifications AS (
    SELECT * FROM {{ ref('stg_notifications') }}
)

SELECT ROW_NUMBER() OVER (ORDER BY TEST_TYPE) AS SK_TEST_TYPE, *
FROM
    (
        SELECT DISTINCT 
            --MD5(TEST_TYPE) AS SK_TEST_TYPE,
            TEST_TYPE, 
            '' AS TEST_TYPE_DESCRIPTION
        FROM notifications
        WHERE TEST_TYPE IS NOT NULL
    ) t