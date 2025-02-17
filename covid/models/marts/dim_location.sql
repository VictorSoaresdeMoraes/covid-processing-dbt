WITH notifications AS (
    SELECT * FROM {{ ref('stg_notifications') }}
)

SELECT ROW_NUMBER() OVER (ORDER BY IBGE_CITY_CODE) AS SK_LOCATION, *
FROM
    (
        SELECT DISTINCT
            --MD5(NOTIFICATION_IBGE_CITY_CODE) AS SK_LOCATION,
            NOTIFICATION_IBGE_CITY_CODE AS IBGE_CITY_CODE,
            NOTIFICATION_CITY_NAME AS CITY_NAME,
            NOTIFICATION_IBGE_STATE_CODE AS IBGE_STATE_CODE,
            NOTIFICATION_STATE_NAME AS STATE_NAME,
            GETDATE() AS UPDATE_DATE
        FROM notifications
    ) t