/* 
 User Journey - All Steps

  Aggreggates all steps in the user journey deduplicating consecutive steps with same content group
  label (i.e. "Homepage > Homepage > Services > Service > Homepage" becomes "Homepage > Services > Homepage")

  Query includes ar "user_filter" CTE that (1) flags conversion time for converting users and (2) optionally
  eliminates user who should not be included in the analysis.

  Final query includes several steps:
    - Get user_id, pagepaths of page_views, hit timestamps
    - Inner join user_ids that should be included in query and elimiate post-conversion hits if the user converts.
    - Apply custom content_group to tag all pageview
    - Lag over content_group to get the prior value, elimiate rows where the prior groups equals the current group
    - Sting agg all steps in the journey ordered by hit timestamp
    - Group by Jounrey path and count the number of users in the hourney

*/

-- CTE: Use filter as inner join on fact table.
WITH user_filter AS (
  SELECT
    user_pseudo_id,
    -- Get the earliest converter timestamp to retain the converter and exclude cost conversion hits
    MIN((SELECT IF(REGEXP_CONTAINS(value.string_value, 'Order: Checkout'), event_timestamp, NULL) FROM UNNEST(event_params) WHERE event_name = 'purchase' and key ='page_title')) AS conversion_time
    --(select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location') as page
    
 FROM `applebees-olo.analytics_245284004.events_*`
    WHERE 
     _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      AND device.category = 'desktop' --'mobile'
  GROUP BY 1
  HAVING conversion_time IS NOT NULL
),

-- CTE: Pull events as a CTE to ensure proper date suffixes on inner join.
event_facts as (
SELECT *
FROM (  
  (SELECT
    user_pseudo_id,
    (SELECT MAX(value.string_value) FROM UNNEST(event_params) WHERE event_name = 'purchase' AND value.string_value = 'Order: Checkout') AS order_conversion,
    (SELECT value.string_value from UNNEST(event_params) WHERE event_name = 'page_view' AND key ='page_location') AS page_location,
    (SELECT value.string_value from UNNEST(event_params) WHERE event_name = 'add_to_cart' AND key ='page_location') AS page_location_when_added_to_cart,
    (SELECT value.string_value from UNNEST(event_params) WHERE event_name = 'user_engagement' AND key ='logged_in') AS logged_in_status,
    event_name,
    event_timestamp,
    FROM `applebees-olo.analytics_245284004.events_*`
    WHERE 
             _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        AND device.category = 'desktop' --'mobile'
            ))
  WHERE event_name in ('page_view','add_to_cart','user_engagement') OR order_conversion IS NOT NULL
  order by user_pseudo_id
)

-- Final Query: Aggregrate complete user journey
SELECT
  journey, conversion_flg,
  COUNT(DISTINCT user_pseudo_id) as counts
FROM (
  SELECT 
    user_pseudo_id, max(conversion_flg) as conversion_flg,
    -- Aggregate all steps into a single ordered string
    STRING_AGG(content_group, ' > ' ORDER BY event_timestamp ASC) as journey
  FROM (
    SELECT
      *,
      -- Find the prior content group so we can filter consequtive rows with the same content group
      LAG(content_group) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) as content_group_lag,
    FROM (
      SELECT
        *,
        -- Parse all relevent content groups 
          CASE  
          --Page Views
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/appetizers') AND event_name  = 'page_view' THEN 'Viewed Appetizers'
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/chicken') AND event_name = 'page_view' THEN 'Viewed Chicken'
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/pasta') AND event_name = 'page_view' THEN 'Viewed Pasta'
          WHEN REGEXP_CONTAINS(page_location, r'/en/menu$') AND event_name = 'page_view' THEN 'Viewed Menu'
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/salads') AND event_name = 'page_view' THEN 'Viewed Salads'
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/from-the-grill') AND event_name = 'page_view' THEN 'Viewed From the Grill'
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/burgers') AND event_name = 'page_view' THEN 'Viewed Burgers'
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/sandwiches-and-more') AND event_name = 'page_view' THEN 'Viewed Sandwiches and More'
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/ala-carte') AND event_name = 'page_view' THEN 'Viewed ala-carte'
          WHEN REGEXP_CONTAINS(page_location, '/en/order/cart') AND event_name = 'page_view' THEN 'Viewed Cart'
          WHEN REGEXP_CONTAINS(page_location, '/en/accounts/cart-sign-in?returnUrl=/en/order/check-out') AND event_name = 'page_view' THEN 'Viewed Sign In'
          WHEN REGEXP_CONTAINS(page_location, 'en/menu/2-for-37') AND event_name = 'page_view' THEN 'Viewed 2-for-37' --check for more offers
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/non-alcoholic-beverages') AND event_name = 'page_view' THEN 'Viewed non-alcoholic-beverages'
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/dessert') AND event_name = 'page_view' THEN 'Viewed dessert'
         --Add to cart
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/appetizers') AND event_name  = 'add_to_cart' THEN 'Appetizers_add_to_cart'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/chicken') AND event_name = 'add_to_cart' THEN 'Chicken_add_to_cart'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/pasta') AND event_name = 'add_to_cart' THEN 'Pasta_add_to_cart'
          WHEN REGEXP_CONTAINS(page_location, r'/en/menu$') AND event_name = 'page_view' THEN 'Viewed Menu'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/salads') AND event_name = 'add_to_cart' THEN 'Salads_add_to_cart'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/from-the-grill') AND event_name = 'add_to_cart' THEN 'From the Grill_add_to_cart'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/burgers') AND event_name = 'add_to_cart' THEN 'Burgers_add_to_cart'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/ala-carte') AND event_name = 'add_to_cart' THEN 'ala-carte_add_to_cart'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/sandwiches-and-more') AND event_name = 'add_to_cart' THEN 'Sandwiches and More_add_to_cart'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, 'en/menu/2-for-37') AND event_name = 'add_to_cart' THEN '2-for-37_add_to_cart'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/dessert') AND event_name = 'add_to_cart' THEN 'add_to_cart dessert'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/order/cross-sell-pre-checkout') AND event_name = 'add_to_cart' THEN 'add_to_cart cross_sell_pre_checkout'
         
          WHEN REGEXP_CONTAINS(logged_in_status,'Logged in') AND event_name = 'user_engagement' THEN 'logged_in'
          ELSE NULL
        END AS content_group,
        case WHEN order_conversion IS NOT NULL THEN TRUE else FALSE end as conversion_flg
      FROM (
        -- Join both base tables
        SELECT
          *
        FROM event_facts
        LEFT JOIN (
          SELECT
            user_pseudo_id,
            conversion_time
          FROM user_filter
        )
        USING(user_pseudo_id)
        -- Get all remaining non-converters and all hits from converters leading up to conversion
        WHERE conversion_time IS NULL OR conversion_time >= event_timestamp
      )    
    )
    -- Limit to only pageviews where you have a valid content group to reduce noise
    WHERE content_group IS NOT NULL
  )
  -- Eliminate consecutive instances of the same content groups
  WHERE content_group_lag != content_group OR content_group_lag IS NULL
  GROUP BY user_pseudo_id
)
-- --optional journey content filtering for specific card pages in journey
--WHERE REGEXP_CONTAINS(LOWER(journey), '>') --|^(apply now click)') 
--WHERE conversion_flg = TRUE
GROUP BY 1,2
ORDER BY 2 DESC,3 DESC
