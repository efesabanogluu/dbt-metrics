{{ config(
materialized='incremental',
unique_key=['event_date','country','platform'],
incremental_strategy='merge',
partition_by={'field': 'event_date', 'data_type': 'date',
'granularity': 'day'},
cluster_by=['country', 'platform']
) }}
SELECT
event_date,
country,
platform,
COUNT(DISTINCT user_id) AS dau,
SUM(iap_revenue) AS total_iap_revenue,
SUM(ad_revenue) AS total_ad_revenue,
(SUM(iap_revenue)+SUM(ad_revenue)) / NULLIF(COUNT(DISTINCT user_id), 0) AS
arpdau,
SUM(match_start_count) AS matches_started,
SUM(match_start_count) /  NULLIF(COUNT(DISTINCT user_id), 0) AS match_per_dau,
SAFE_DIVIDE(SUM(victory_count), SUM(match_end_count)) AS win_ratio,
SAFE_DIVIDE(SUM(defeat_count), SUM(match_end_count)) AS defeat_ratio,
SUM(server_connection_error) /  NULLIF(COUNT(DISTINCT user_id), 0) AS
server_error_per_dau
FROM {{ ref('stg_events') }}
-- Only process new or updated dates when the model is run incrementally
{% if is_incremental() %}
WHERE event_date >= (SELECT MAX(event_date) FROM {{ this }})
{% endif %}
GROUP BY 1,2,3