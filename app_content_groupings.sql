with user_filter AS (
  SELECT
    concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) as sessions_session_and_user_id
    , platform,
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
  ORDER by 1
)

,event_facts as (
SELECT *
FROM (  
  (SELECT
    user_pseudo_id,
    concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) as sessions_session_and_user_id
  --view products
  ,(select value.string_value from UNNEST(event_params) WHERE event_name  = 'view_item_list' and key = 'item_category') as app_view_itemlist
  --start a new order
  --, (select value.string_value from UNNEST(event_params) WHERE event_name = 'start_new_order' and key = 'firebase_screen_class') as order_time_controler
  --add_to_cart
  , (select IF(value.string_value is null, null,'added_item')  from UNNEST(event_params) WHERE event_name = 'add_to_cart' and key = 'item_name') as product_added_to_cart
  --quick reorder
  , (select IF(REGEXP_CONTAINS(value.string_value, r'QuickReorderViewController'),'Quick_Reorder',null) 
  from UNNEST(event_params) WHERE event_name = 'screen_view' and key = 'firebase_screen_class') as  quick_reorder
  --view_favorite_store
  , (select IF(REGEXP_CONTAINS(value.string_value, r'StoreTableViewController'),'viewed_favourite_store',null) from UNNEST(event_params) WHERE event_name = 'view_favorite_store' and key = 'firebase_screen_class') as view_favorite_store
  --view_nearby_store
  --  , (select value.string_value from UNNEST(event_params) WHERE event_name = 'view_nearby_store' and key = 'firebase_screen_class') as view_nearby_store
  --ecommerse purchase (conversion)
  , (SELECT MAX(value.string_value) FROM UNNEST(event_params) WHERE event_name = 'ecommerce_purchase' AND REGEXP_CONTAINS(value.string_value, r'CheckoutViewController|CheckoutActivity')) AS order_conversion,
  --(SELECT value.string_value from UNNEST(event_params) WHERE event_name = 'view_item' AND key ='item_name') as viewed_item_name,
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
                      ,'view_nearby_store'
                      ,'screen_view') 
                    OR order_conversion IS NOT NULL
  order by user_pseudo_id,2
)
--select * from event_facts
-- select app_view_itemlist
-- , product_grouping
-- ,product_added_to_cart
-- ,view_favorite_store
-- ,order_conversion
-- , count (distinct sessions_session_and_user_id )  from event_facts_2
-- group by  app_view_itemlist
-- , product_grouping
-- ,product_added_to_cart
-- ,view_favorite_store
-- ,order_conversion
-- order by 3 desc


--select count(distinct sessions_session_and_user_id ) from event_facts_2 -- 97090
--Final Query
SELECT
   journey, conversion_flg,
  COUNT(DISTINCT sessions_session_and_user_id) as counts
FROM (
  SELECT 
    sessions_session_and_user_id, max(conversion_flg) as conversion_flg,
    -- Aggregate all steps into a single ordered string
    STRING_AGG(content_group, ' > ' ORDER BY event_timestamp ASC) as journey
  FROM (
    SELECT
      *,
      -- Find the prior content group so we can filter consequtive rows with the same content group
      LAG(content_group) OVER (PARTITION BY sessions_session_and_user_id ORDER BY event_timestamp) as content_group_lag,
    FROM (
      SELECT
        *,  
        CASE
          --when event_name ='view_item_list' then app_view_itemlist
          -----------------------------------------------------------
          --start item categories
          -----------------------------------------------------------
          WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Appetizers$') and event_name ='view_item_list' then 'Appetizers'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Pasta$') and event_name ='view_item_list' then 'Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Chicken$') and event_name ='view_item_list' then 'Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Steaks & Ribs$') and event_name ='view_item_list' then 'Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Sandwiches & More$') and event_name ='view_item_list' then 'Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Handcrafted Burgers$') and event_name ='view_item_list' then 'Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Salads$') and event_name ='view_item_list' then 'Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Sauces & Sides$') and event_name ='view_item_list' then 'Sauces & Sides'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Kids Menu$') and event_name ='view_item_list' then 'Kids Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Seafood$') and event_name ='view_item_list' then 'Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Desserts$') and event_name ='view_item_list' then 'Dessert'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^2 for $25$') and event_name ='view_item_list' then 'Offers'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Non-Alcoholic Beverages$') and event_name ='view_item_list' then 'Drinks'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Irresist-A-Bowls¬Æ$') and event_name ='view_item_list' then 'Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Irresist-A-Bowls$') and event_name ='view_item_list' then 'Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^2 for $24$') and event_name ='view_item_list' then 'Offers'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Cheetos¬Æ Exclusive  Flavors$') and event_name ='view_item_list' then 'CHEETOS  EXCLUSIVE FLAVORS'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Extras$') and event_name ='view_item_list' then 'Menu Extras'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Exclusive Cheetos¬Æ Flavors$') and event_name ='view_item_list' then 'CHEETOS  EXCLUSIVE FLAVORS'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Catering$') and event_name ='view_item_list' then ''
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Signature Cocktails$') and event_name ='view_item_list' then 'Drinks'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Signature Cocktails To Go$') and event_name ='view_item_list' then 'Drinks'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Family Bundle Meals$') and event_name ='view_item_list' then 'Family Value Bundles'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^2 for $22$') and event_name ='view_item_list' then 'Offers'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^2 for $22/-/27$') and event_name ='view_item_list' then 'Offers'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Beer and Wine$') and event_name ='view_item_list' then 'Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^2 for $25 (Price may vary by location or selection.)$') and event_name ='view_item_list' then 'Offers'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^2 for $23$') and event_name ='view_item_list' then 'Offers'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Beer and Wine To Go$') and event_name ='view_item_list' then 'Drinks'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^2 for $2X$') and event_name ='view_item_list' then 'Offers'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^$9.99 Lunch Meal Deals, $1 Drinks$') and event_name ='view_item_list' then 'Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Mucho Cocktails$') and event_name ='view_item_list' then 'Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^2 for $24 - 29$') and event_name ='view_item_list' then 'Offers'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^$13.99 Burger Bundle$') and event_name ='view_item_list' then 'Menu'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Lunch Specials (Mon-Fri Until 4pm)$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Kid\'s Menu$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Steaks$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^$14.99 Burger Bundle$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^$9.99 Burger Bundle$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Ribs$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^2 for $26$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Packaging$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^2 for $37$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^2 for $30$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Beer & Wine To Go$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^2 for $28$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Party Platters$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Wings & Tenders$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^National Cheeseburger Day$') and event_name ='view_item_list' then 'Other'
WHEN REGEXP_CONTAINS(app_view_itemlist, r'^Support Alex‚Äôs Lemonade Stand Foundation -- Crush Childhood Cancer$') and event_name ='view_item_list' then 'Other'


          -----------------------------------------------------------
          --end
          -----------------------------------------------------------

         -- when event_name ='view_item' then product_grouping
         -- when event_name = 'start_new_order' then order_time_controler
          when event_name = 'add_to_cart' then product_added_to_cart
         -- when event_name = 'begin_checkout' then begin_checkout
          when event_name = 'view_favorite_store' then view_favorite_store
          when event_name = 'screen_view' then quick_reorder
          when event_name = 'ecommerce_purchase' then order_conversion

          --WHEN event_name = 'add_to_cart' then 'A2C'
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
            sessions_session_and_user_id,
            conversion_time
          FROM user_filter
        )
        USING(sessions_session_and_user_id)
        -- Get all remaining non-converters and all hits from converters leading up to conversion
        WHERE conversion_time IS NULL OR conversion_time >= event_timestamp --and (event_name != 'user_engagement' and page_location is null)
      )    
    )
    -- Limit to only pageviews where you have a valid content group to reduce noise
    WHERE content_group IS NOT NULL
  )
  -- Eliminate consecutive instances of the same content groups
  WHERE content_group_lag != content_group OR content_group_lag IS NULL
  GROUP BY sessions_session_and_user_id
)
-- --optional journey content filtering for specific card pages in journey
--WHERE REGEXP_CONTAINS(LOWER(journey), '>') --|^(apply now click)') 
WHERE conversion_flg = TRUE
and journey !=''
GROUP BY 1,2
ORDER BY 3 DESC
;
