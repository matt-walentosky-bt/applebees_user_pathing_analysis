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


--Query for 

-- CTE: Use filter as inner join on fact table.
WITH user_filter AS (
  SELECT
    user_pseudo_id, platform,
    -- Get the earliest converter timestamp to retain the converter and exclude cost conversion hits
    MIN((SELECT IF(REGEXP_CONTAINS(value.string_value, 'Order: Checkout'), event_timestamp, NULL) FROM UNNEST(event_params) WHERE event_name = 'purchase' and key ='page_title')) AS conversion_time
    --(select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location') as page
    
 FROM `applebees-olo.analytics_245284004.events_*`
    WHERE 
     _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      --AND device.category = 'desktop' --'mobile'
    and stream_id = '2124971281'

  GROUP BY 1, 2
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
    (SELECT value.string_value from UNNEST(event_params) WHERE event_name = 'Dynamic Click' AND key ='logged_in') AS dynamic_clicklogged_in_status,
    event_name,
    event_timestamp,
    platform
    FROM `applebees-olo.analytics_245284004.events_*`
    WHERE 
             _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
            ))
  WHERE event_name in ('page_view','add_to_cart','purchase','user_engagement') OR order_conversion IS NOT NULL
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
        CASE
        WHEN REGEXP_CONTAINS(page_location, '/en/menu/appetizers') AND event_name  = 'page_view' THEN 'PV: Appetizers'
        WHEN REGEXP_CONTAINS(page_location, '/en/menu/chicken|/en/menu/pasta|/en/menu/salads|/en/menu/from-the-grill|/en/menu/burgers|/en/menu/sandwiches-and-more|menu/seafood|irresist-a-bowls|steaks-and-ribs|fire-grilled-and-chef-selections|sandwiches|all-you-can-eat|handcrafted-burgers|tex-mex-lime-grilled-shrimp-bowl') AND     event_name = 'page_view' THEN 'PV: Entrees'
          WHEN REGEXP_CONTAINS(page_location, r'/en/menu$') AND event_name = 'page_view' THEN 'PV: Menu'
         -- WHEN REGEXP_CONTAINS(page_location, '/en/menu/ala-carte') AND event_name = 'page_view' THEN 'PV: Ala-Carte'
          WHEN REGEXP_CONTAINS(page_location, '/en/order/cart') AND event_name = 'page_view' THEN 'PV: Cart'
          WHEN REGEXP_CONTAINS(page_location, '/en/accounts/cart-sign-in?returnUrl=/en/order/check-out') AND event_name = 'page_view' THEN 'PV: Sign In'
          WHEN REGEXP_CONTAINS(page_location, 'en/menu/2-for') AND event_name = 'page_view' THEN 'PV: Offers' 
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/non-alcoholic-beverages|menu/beer-and-wine') AND event_name = 'page_view' THEN 'PV: Drinks'
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/dessert') AND event_name = 'page_view' THEN 'PV: Dessert'
          WHEN REGEXP_CONTAINS(page_location, 'en/menu/kids-menu') AND event_name = 'page_view' THEN 'PV: Kids Menu'
          WHEN REGEXP_CONTAINS(page_location, 'en/order/check-out') AND event_name = 'page_view' THEN 'PV: Check Out'
          WHEN REGEXP_CONTAINS(page_location, 'order/cross-sell-pre-checkout') AND event_name = 'page_view' THEN 'PV: Cross Sell Page'
         -- WHEN REGEXP_CONTAINS(page_location, 'en/menu/catering') AND event_name = 'page_view' THEN 'PV: Catering'
          WHEN REGEXP_CONTAINS(page_location, 'menu/family-value-bundles') AND event_name = 'page_view' THEN 'PV: Viewed Family Value Bundles'
          WHEN REGEXP_CONTAINS(page_location, 'en/order/confirm-restaurant') AND event_name = 'page_view' THEN 'PV: Confirm Restaurant'
          WHEN REGEXP_CONTAINS(page_location, 'en/menu/extras') AND event_name = 'page_view' THEN 'PV: Menu Extras'
          WHEN REGEXP_CONTAINS(page_location, 'accounts/sign-in') AND event_name = 'page_view' THEN 'PV: Account Sign In'
          WHEN REGEXP_CONTAINS(page_location, 'en/order/ordermethod') AND event_name = 'page_view' THEN 'PV: Order Method'
          WHEN REGEXP_CONTAINS(page_location, 'nutrition') AND event_name = 'page_view' THEN 'PV: Nutrition'
          --WHEN REGEXP_CONTAINS(page_location, 'terms-of-use') AND event_name = 'page_view' THEN 'PV: Terms of Use'
       --WHEN REGEXP_CONTAINS(page_location, 'order/confirmation?') AND event_name = 'page_view' THEN 'PV: Order Confirmation'
          WHEN REGEXP_CONTAINS(page_location, 'accounts/my-account') AND event_name = 'page_view' THEN 'PV: My Account'
       --   WHEN REGEXP_CONTAINS(page_location, 'contact-us') AND event_name = 'page_view' THEN 'PV: Contact Us'
          WHEN REGEXP_CONTAINS(page_location, 'gift-cards') AND event_name = 'page_view' THEN 'PV: Gift Cards'
          WHEN REGEXP_CONTAINS(page_location, '/en/sign-up') AND event_name = 'page_view' THEN 'PV: Sign Up'
          WHEN page_location = 'https://www.applebees.com/en' or page_location = 'https://restaurants.applebees.com/en-us/' AND event_name = 'page_view' THEN 'PV: Home Page'
          WHEN REGEXP_CONTAINS(page_location, '/en/order/cross-sell-pre-checkout') AND event_name = 'page_view' THEN 'PV: Cross-Sell'
          --WHEN event_name = 'add_to_cart' then 'A2C'
         --Add to cart
         /*
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/appetizers') AND event_name  = 'add_to_cart' THEN 'A2C: Appetizers'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/chicken|/en/menu/pasta|/en/menu/salads|/en/menu/from-the-grill|/en/menu/burgers|/en/menu/sandwiches-and-more') AND event_name = 'add_to_cart' THEN 'A2C: Entree'
          WHEN REGEXP_CONTAINS(page_location, r'/en/menu$') AND event_name = 'page_view' THEN 'PV: Menu'
       --   WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/ala-carte') AND event_name = 'add_to_cart' THEN 'A2C: Al-A-Carte'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, 'en/menu/2-for') AND event_name = 'add_to_cart' THEN 'A2C: 2-For ...'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/dessert') AND event_name = 'add_to_cart' THEN 'A2C: Dessert'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/order/cross-sell-pre-checkout') AND event_name = 'add_to_cart' THEN 'A2C: Cross-Sell'
          WHEN REGEXP_CONTAINS(logged_in_status,'Logged in')  THEN 'logged_in'
          */
          WHEN event_name = 'purchase' then 'Purchase'
          else NULL
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
        WHERE conversion_time IS NULL OR conversion_time >= event_timestamp --and (event_name != 'user_engagement' and page_location is null)
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
WHERE conversion_flg = TRUE
GROUP BY 1,2
ORDER BY 2 DESC,3 DESC;









-----------------------------------------------
-----------------------------------------------
--QC Query

with
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
        --AND device.category = 'desktop' --'mobile'
        and stream_id = '2124971281'
            ))
  WHERE event_name in ('page_view','add_to_cart','user_engagement') OR order_conversion IS NOT NULL 
  order by user_pseudo_id
),



all_records as
(SELECT
        page_location,
        event_name,
        -- Parse all relevent content groups 
          
       CASE
        WHEN REGEXP_CONTAINS(page_location, '/en/menu/appetizers') AND event_name  = 'page_view' THEN 'PV: Appetizers'
        WHEN REGEXP_CONTAINS(page_location, '/en/menu/chicken|/en/menu/pasta|/en/menu/salads|/en/menu/from-the-grill|/en/menu/burgers|/en/menu/sandwiches-and-more|menu/seafood|irresist-a-bowls|steaks-and-ribs|fire-grilled-and-chef-selections|sandwiches|all-you-can-eat|handcrafted-burgers|tex-mex-lime-grilled-shrimp-bowl') AND     event_name = 'page_view' THEN 'PV: Entrees'
          WHEN REGEXP_CONTAINS(page_location, r'/en/menu$') AND event_name = 'page_view' THEN 'PV:  Menu'
         -- WHEN REGEXP_CONTAINS(page_location, '/en/menu/ala-carte') AND event_name = 'page_view' THEN 'PV: Ala-Carte'
          WHEN REGEXP_CONTAINS(page_location, '/en/order/cart') AND event_name = 'page_view' THEN 'PV: Cart'
          WHEN REGEXP_CONTAINS(page_location, '/en/accounts/cart-sign-in?returnUrl=/en/order/check-out') AND event_name = 'page_view' THEN 'PV: Sign In'
          WHEN REGEXP_CONTAINS(page_location, 'en/menu/2-for') AND event_name = 'page_view' THEN 'Page View: Offers' 
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/non-alcoholic-beverages|menu/beer-and-wine') AND event_name = 'page_view' THEN 'PV: Drinks'
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/dessert') AND event_name = 'page_view' THEN 'PV: Dessert'
          WHEN REGEXP_CONTAINS(page_location, 'en/menu/kids-menu') AND event_name = 'page_view' THEN 'PV: Kids Menu'
          WHEN REGEXP_CONTAINS(page_location, 'en/order/check-out') AND event_name = 'page_view' THEN 'PV: Check Out'
          WHEN REGEXP_CONTAINS(page_location, 'order/cross-sell-pre-checkout') AND event_name = 'page_view' THEN 'PV: Cross Sell Page'
         -- WHEN REGEXP_CONTAINS(page_location, 'en/menu/catering') AND event_name = 'page_view' THEN 'PV: Catering'
          WHEN REGEXP_CONTAINS(page_location, 'menu/family-value-bundles') AND event_name = 'page_view' THEN 'PV: Viewed Family Value Bundles'
          WHEN REGEXP_CONTAINS(page_location, 'en/order/confirm-restaurant') AND event_name = 'page_view' THEN 'PV: Confirm Restaurant'
          WHEN REGEXP_CONTAINS(page_location, 'en/menu/extras') AND event_name = 'page_view' THEN 'PV: Menu Extras'
          WHEN REGEXP_CONTAINS(page_location, 'accounts/sign-in') AND event_name = 'page_view' THEN 'PV: Account Sign In'
          WHEN REGEXP_CONTAINS(page_location, 'en/order/ordermethod') AND event_name = 'page_view' THEN 'PV: Order Method'
          WHEN REGEXP_CONTAINS(page_location, 'nutrition') AND event_name = 'page_view' THEN 'PV: Nutrition'
          --WHEN REGEXP_CONTAINS(page_location, 'terms-of-use') AND event_name = 'page_view' THEN 'PV: Terms of Use'
       --   WHEN REGEXP_CONTAINS(page_location, 'order/confirmation?') AND event_name = 'page_view' THEN 'PV: Order Confirmation'
          WHEN REGEXP_CONTAINS(page_location, 'accounts/my-account') AND event_name = 'page_view' THEN 'PV: My Account'
       --   WHEN REGEXP_CONTAINS(page_location, 'contact-us') AND event_name = 'page_view' THEN 'PV: Contact Us'
          WHEN REGEXP_CONTAINS(page_location, 'gift-cards') AND event_name = 'page_view' THEN 'PV: Gift Cards'
          WHEN REGEXP_CONTAINS(page_location, '/en/sign-up') AND event_name = 'page_view' THEN 'PV: Sign Up'
          WHEN page_location = 'https://www.applebees.com/en' or page_location = 'https://restaurants.applebees.com/en-us/' AND event_name = 'page_view' THEN 'PV: Home Page'
          WHEN REGEXP_CONTAINS(page_location, '/en/order/cross-sell-pre-checkout') AND event_name = 'page_view' THEN 'PV: Cross-Sell'
          --WHEN event_name = 'add_to_cart' then 'A2C'
         --Add to cart
         /*
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/appetizers') AND event_name  = 'add_to_cart' THEN 'A2C: Appetizers'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/chicken|/en/menu/pasta|/en/menu/salads|/en/menu/from-the-grill|/en/menu/burgers|/en/menu/sandwiches-and-more') AND event_name = 'add_to_cart' THEN 'A2C: Entree'
          WHEN REGEXP_CONTAINS(page_location, r'/en/menu$') AND event_name = 'page_view' THEN 'PV: Menu'
       --   WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/ala-carte') AND event_name = 'add_to_cart' THEN 'A2C: Al-A-Carte'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, 'en/menu/2-for') AND event_name = 'add_to_cart' THEN 'A2C: 2-For ...'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/menu/dessert') AND event_name = 'add_to_cart' THEN 'A2C: Dessert'
          WHEN REGEXP_CONTAINS(page_location_when_added_to_cart, '/en/order/cross-sell-pre-checkout') AND event_name = 'add_to_cart' THEN 'A2C: Cross-Sell'
          WHEN REGEXP_CONTAINS(logged_in_status,'Logged in')  THEN 'logged_in'
          */
         -- WHEN event_name = 'purchase' then 'Purchase'
          WHEN event_name = 'purchase' then 'Purchase'
          else 'Other'
          END AS content_group

        FROM 
        event_facts)
        
        --select content_group, count(*) as record_count 
        --from all_records
        --group by content_group order by record_count desc
        
        select
        page_location,
        event_name,
        content_group,
        count(*) as p_count
        from
        all_records
        --where content_group ='Other'
        group by content_group, page_location, event_name
        order by p_count desc
        ;

