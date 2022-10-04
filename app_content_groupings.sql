with item_lkup_create as (
select TRIM(item_name) as item_name ,product_grouping ,num_events, rank() OVER (partition by TRIM(item_name) ORDER BY num_events desc) as item_rank
from (
select item_name,product_grouping , count(event_name) as num_events
from (
select  item_name,event_name,
CASE
        WHEN REGEXP_CONTAINS(page_location, '/en/menu/appetizers') THEN 'PV: Appetizers'
        WHEN REGEXP_CONTAINS(page_location, '/en/menu/chicken|/en/menu/pasta|/en/menu/salads|/en/menu/from-the-grill|/en/menu/burgers|/en/menu/sandwiches-and-more|menu/seafood|irresist-a-bowls|steaks-and-ribs|fire-grilled-and-chef-selections|sandwiches|all-you-can-eat|handcrafted-burgers|tex-mex-lime-grilled-shrimp-bowl') THEN 'PV: Entrees'
          WHEN REGEXP_CONTAINS(page_location, r'/en/menu$')  THEN 'PV:  Menu'
          WHEN REGEXP_CONTAINS(page_location, '/en/order/cart') THEN 'PV: Cart'
          WHEN REGEXP_CONTAINS(page_location, '/en/accounts/cart-sign-in?returnUrl=/en/order/check-out') THEN 'PV: Sign In'
          WHEN REGEXP_CONTAINS(page_location, 'en/menu/2-for') THEN 'Page View: Offers' 
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/non-alcoholic-beverages|menu/beer-and-wine') THEN 'PV: Drinks'
          WHEN REGEXP_CONTAINS(page_location, '/en/menu/dessert') THEN 'PV: Dessert'
          WHEN REGEXP_CONTAINS(page_location, 'en/menu/kids-menu') THEN 'PV: Kids Menu'
          WHEN REGEXP_CONTAINS(page_location, 'en/order/check-out') THEN 'PV: Check Out'
          WHEN REGEXP_CONTAINS(page_location, 'order/cross-sell-pre-checkout')  THEN 'PV: Cross Sell Page'
          WHEN REGEXP_CONTAINS(page_location, 'menu/family-value-bundles')  THEN 'PV: Viewed Family Value Bundles'
          WHEN REGEXP_CONTAINS(page_location, 'en/order/confirm-restaurant') THEN 'PV: Confirm Restaurant'
          WHEN REGEXP_CONTAINS(page_location, 'en/menu/extras')  THEN 'PV: Menu Extras'
          WHEN REGEXP_CONTAINS(page_location, 'accounts/sign-in') THEN 'PV: Account Sign In'
          WHEN REGEXP_CONTAINS(page_location, 'en/order/ordermethod') THEN 'PV: Order Method'
          WHEN REGEXP_CONTAINS(page_location, 'nutrition') THEN 'PV: Nutrition'
          WHEN REGEXP_CONTAINS(page_location, 'accounts/my-account') THEN 'PV: My Account'
          WHEN page_location = 'https://www.applebees.com/en' or page_location = 'https://restaurants.applebees.com/en-us/' THEN 'PV: Home Page'
          WHEN REGEXP_CONTAINS(page_location, '/en/order/cross-sell-pre-checkout') AND event_name = 'page_view' THEN 'PV: Cross-Sell'
          --WHEN event_name = 'add_to_cart' then 'A2C'
          else null
          end as product_grouping 
from (
select event_name,	items.item_category,items.item_name, 
(SELECT value.string_value from UNNEST(event_params) where key = "firebase_screen_class") as screen_class,
(SELECT value.string_value, from UNNEST(event_params) where key = "page_location") as page_location,
event_timestamp,
--(SELECT value.string_value from UNNEST(event_params) where key = "page_title") as page_title
FROM `applebees-olo.analytics_245284004.events_*`, UNNEST(event_params), UNNEST(items) AS items
WHERE  _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
)
where screen_class is null
and event_name in ("add_to_cart")
)
where product_grouping is not null
and item_name != ''
group by item_name,product_grouping
order by item_name, num_events desc
)
order by item_name, product_grouping
)
,item_lkup AS (
  SELECT item_name, product_grouping
  from item_lkup_create
  where item_rank = 1
)

,user_filter AS (
  SELECT
    user_pseudo_id, platform,
    -- Get the earliest converter timestamp to retain the converter and exclude cost conversion hits
    MIN((SELECT IF(REGEXP_CONTAINS(value.string_value, 'CheckoutViewController|CheckoutActivity'), event_timestamp, NULL) 
    FROM UNNEST(event_params) WHERE event_name = 'ecommerce_purchase' and key ='firebase_screen_class')) AS conversion_time
    --(select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location') as page
    
 FROM `applebees-olo.analytics_245284004.events_*`
    WHERE 
     _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND stream_id in  ('2034588539','2034592889') 


  GROUP BY 1, 2
  HAVING conversion_time IS NOT NULL
)

,event_facts as (
SELECT *
FROM (  
  (SELECT
    user_pseudo_id
  --view products
  ,(select value.string_value from UNNEST(event_params) WHERE event_name  = 'view_item_list' and key = 'firebase_screen_class') as app_view_itemlist
  --start a new order
  --, (select value.string_value from UNNEST(event_params) WHERE event_name = 'start_new_order' and key = 'firebase_screen_class') as order_time_controler
  --add_to_cart
  , (select value.string_value from UNNEST(event_params) WHERE event_name = 'add_to_cart' and key = 'firebase_screen_class') as product_added_to_cart
  --begin checkout
  , (select value.string_value from UNNEST(event_params) WHERE event_name = 'begin_checkout' and key = 'firebase_screen_class') as begin_checkout
  --view_favorite_store
  , (select value.string_value from UNNEST(event_params) WHERE event_name = 'view_favorite_store' and key = 'firebase_screen_class') as view_favorite_store
  --view_nearby_store
  --  , (select value.string_value from UNNEST(event_params) WHERE event_name = 'view_nearby_store' and key = 'firebase_screen_class') as view_nearby_store
  --ecommerse purchase (conversion)
  , (SELECT MAX(value.string_value) FROM UNNEST(event_params) WHERE event_name = 'ecommerce_purchase' AND REGEXP_CONTAINS(value.string_value, r'CheckoutViewController|CheckoutActivity')) AS order_conversion,
  (SELECT value.string_value from UNNEST(event_params) WHERE event_name = 'view_item' AND key ='item_name') as viewed_item_name,
    event_name,
    event_timestamp,
    platform
    FROM `applebees-olo.analytics_245284004.events_*`
    WHERE _TABLE_SUFFIX 
  BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)) 
      AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      AND stream_id in ('2034588539','2034592889')
            ))
  WHERE event_name in ('view_item'
                      ,'view_item_list'
                      ,'ecommerce_purchase'
                      ,'start_new_order'
                      ,'add_to_cart'
                      ,'begin_checkout'
                      ,'view_favorite_store'
                      ,'view_nearby_store') 
                    OR order_conversion IS NOT NULL
  order by user_pseudo_id
)
,event_facts_2 as (
  select a.*
,b.product_grouping
from event_facts a
left join item_lkup b
on a.viewed_item_name=b.item_name
order by viewed_item_name
)
-- select count(distinct user_pseudo_id ) from event_facts_2 -- 74235
--Final Query
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
          when event_name ='view_item_list' then app_view_itemlist
          when event_name ='view_item' then product_grouping
         -- when event_name = 'start_new_order' then order_time_controler
          when event_name = 'add_to_cart' then product_added_to_cart
          when event_name = 'begin_checkout' then begin_checkout
          when event_name = 'view_favorite_store' then view_favorite_store
        --  when event_name = 'view_nearby_store' then view_nearby_store
          when event_name = 'ecommerce_purchase' then order_conversion

          WHEN event_name = 'add_to_cart' then 'A2C'
          else NULL
          END AS content_group,
        case WHEN order_conversion IS NOT NULL THEN TRUE else FALSE end as conversion_flg
      FROM (
        -- Join both base tables
        SELECT
          *
        FROM event_facts_2
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
--WHERE conversion_flg = TRUE
GROUP BY 1,2
ORDER BY 3 DESC
;
