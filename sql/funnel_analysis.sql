-- Ecommerce Funnel Analysis (GA4)
-- Author: Halyna Lanovska
-- Description: Session-level funnel analysis using GA4 BigQuery dataset

-- Project 1: Ecommerce funnel (GA4 public dataset)
-- Output: 1 row = 1 session (session-level), ready for Tableau funnel + filters

WITH base AS (
  SELECT
    user_pseudo_id,
    (SELECT value.int_value
     FROM UNNEST(event_params)
     WHERE key = 'ga_session_id') AS ga_session_id,

    TIMESTAMP_MICROS(event_timestamp) AS event_ts,
    event_name,
    event_value_in_usd,

    -- traffic source (session-related in this sample)
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name AS campaign,

    -- device dimensions
    device.category AS device_category,
    device.language AS device_language,
    device.operating_system AS operating_system,

    -- page url
    (SELECT value.string_value
     FROM UNNEST(event_params)
     WHERE key = 'page_location') AS page_location,

    -- transaction id (for orders)
    (SELECT value.string_value
     FROM UNNEST(event_params)
     WHERE key = 'transaction_id') AS transaction_id

  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'
),

session_level AS (
  SELECT
    CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING)) AS session_id,
    user_pseudo_id,
    ga_session_id,

    -- session start time (required filter)
    MIN(event_ts) AS session_start_ts,
    DATE(MIN(event_ts)) AS session_start_date,

    -- session dimensions for slicing
    ANY_VALUE(source) AS source,
    ANY_VALUE(medium) AS medium,
    ANY_VALUE(campaign) AS campaign,
    ANY_VALUE(device_category) AS device_category,
    ANY_VALUE(device_language) AS device_language,
    ANY_VALUE(operating_system) AS operating_system,

    -- landing page = first page_location in session
    ARRAY_AGG(page_location IGNORE NULLS ORDER BY event_ts LIMIT 1)[OFFSET(0)] AS landing_page,

    -- funnel steps flags (1 if occurred within session)
    MAX(IF(event_name = 'session_start', 1, 0)) AS step_session_start,
    MAX(IF(event_name = 'view_item', 1, 0)) AS step_view_item,
    MAX(IF(event_name = 'add_to_cart', 1, 0)) AS step_add_to_cart,
    MAX(IF(event_name = 'begin_checkout', 1, 0)) AS step_begin_checkout,
    MAX(IF(event_name = 'add_shipping_info', 1, 0)) AS step_add_shipping_info,
    MAX(IF(event_name = 'add_payment_info', 1, 0)) AS step_add_payment_info,
    MAX(IF(event_name = 'purchase', 1, 0)) AS step_purchase,

    -- orders & revenue
    COUNT(DISTINCT IF(event_name = 'purchase', transaction_id, NULL)) AS orders,
    SUM(IF(event_name = 'purchase', event_value_in_usd, 0)) AS revenue_usd

  FROM base
  WHERE ga_session_id IS NOT NULL
  GROUP BY 1,2,3
)

SELECT *
FROM session_level;

