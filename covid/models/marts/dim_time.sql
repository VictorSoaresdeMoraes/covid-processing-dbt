WITH date_range AS (
    SELECT 
        DATEADD(DAY, seq4(), '2020-01-01') AS date
    FROM 
        TABLE(GENERATOR(ROWCOUNT => 365 * 10))  -- Gera 10 anos de dados
)

SELECT 
    CAST(TO_VARCHAR(DATE, 'YYYYMMDD') AS INT) AS SK_TIME,
    DATE,
    EXTRACT(YEAR FROM date) AS YEAR,
    EXTRACT(MONTH FROM date) AS MONTH,
    EXTRACT(DAY FROM date) AS DAY,
    EXTRACT(QUARTER FROM date) AS QUARTER,
    DAYNAME(date) AS DAY_OF_WEEK
FROM 
    date_range
ORDER BY 
    date