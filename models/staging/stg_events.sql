{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by={'field': 'event_date', 'data_type': 'date',
    'granularity': 'day'},
    cluster_by=['country', 'platform']
) }}
SELECT
user_id,
event_date,
install_date,
UPPER(platform) AS platform,
IFNULL(UPPER(country), 'XX') AS country,
total_session_count,
total_session_duration,
match_start_count,
match_end_count,
victory_count,
defeat_count,
server_connection_error,
iap_revenue,
ad_revenue
FROM  {{ source('vertigo_raw', 'raw_daily_metrics') }}
WHERE user_id IS NOT NULL
AND event_date IS NOT NULL
AND install_date IS NOT NULL
AND victory_count + defeat_count = match_end_count
 {% if is_incremental() %}
AND event_date >= (SELECT MAX(event_date) FROM {{ this }})
{% endif %}
