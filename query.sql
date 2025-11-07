-- ПРОЄКТ 1: Ecommerce Funnel
--Отримуємо дані про сесії користувачів 
WITH sessions_info AS (
  SELECT 
    CONCAT(user_pseudo_id, '-', CAST((SELECT value.int_value 
                                        FROM UNNEST(event_params) 
                                        WHERE key = 'ga_session_id') AS STRING)) AS user_session_id,
    TIMESTAMP_MICROS(event_timestamp) AS session_start_at,
    IFNULL(REGEXP_EXTRACT((SELECT ep.value.string_value 
                       FROM UNNEST(event_params) ep 
                       WHERE ep.key = 'page_location'),r'^https?:\/\/[^\/]+\/([^\/\?]+)'),'Unknown') AS landing_page_location, 
    geo.country AS country,
    device.category AS device_category,
    device.language AS device_language,
    device.operating_system AS os,
    traffic_source.source,
    traffic_source.medium,
    traffic_source.name AS campaign
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE event_name = 'session_start'),

--Отримуємо всі події воронки продажів
events AS (
  SELECT 
    CONCAT(user_pseudo_id, '-', CAST((SELECT value.int_value 
                                      FROM UNNEST(event_params) 
                                      WHERE key = 'ga_session_id') AS STRING)) AS user_session_id,
    event_name,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE event_name IN (
    'session_start',
    'view_item',
    'add_to_cart',
    'begin_checkout',
    'add_shipping_info',
    'add_payment_info',
    'purchase'))

-- Об’єднуємо сесії та події за user_session_id
SELECT 
  e.event_name,
  e.event_timestamp,
  s.user_session_id,
  s.session_start_at,         
  s.landing_page_location,
  s.country,
  s.device_category,
  s.device_language,
  s.os,
  s.source,
  s.medium,
  s.campaign
FROM sessions_info s
LEFT JOIN events e
USING (user_session_id)
ORDER BY e.event_timestamp;
