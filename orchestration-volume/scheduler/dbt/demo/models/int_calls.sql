{{ config(
    materialized = 'table'
) }}

WITH calls AS (
    -- Aquí lees la tabla cruda en BigQuery
    -- ajusta el source según tu proyecto dbt
    SELECT
        id,
        user_id,
        DATE(call_date) AS call_date,
        duration
    FROM {{ source('megaline', 'calls') }}
),

calls_with_month AS (
    SELECT
        user_id,
        -- mes truncado (primer día del mes)
        DATE_TRUNC(call_date, MONTH) AS month,
        id,
        duration
    FROM calls
),

calls_grouped AS (
    SELECT
        user_id,
        month,
        COUNT(id) AS calls_count,
        SUM(duration) AS total_minutes
    FROM calls_with_month
    GROUP BY
        user_id,
        month
)

SELECT
    user_id,
    month,
    calls_count,
    total_minutes
FROM calls_grouped
ORDER BY
    user_id,
    month
