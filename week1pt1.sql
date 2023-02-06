--only runs on Snowflake, since it uses qualify
--this code is QUITE untidy

with cities as (
select 
    upper(trim(city_name))as city_name,
    upper(state_abbr) as state,
    GEO_LOCATION,
    concat(city_name,state) as city_state
from vk_data.resources.us_cities
qualify row_number() over (partition by city_name, state order by 1) = 1
),
customers as (
select
    c2.first_name,
    c2.last_name,
    c2.email,
    upper(trim(CUSTOMER_CITY))as city_name,
    upper(CUSTOMER_STATE) as state,
    c1.CUSTOMER_ID,
    concat(city_name,state) as city_state
FROM vk_data.customers.customer_address as c1
JOIN vk_data.customers.customer_data as c2
    on c2.customer_id = c1.customer_id
),
valid_customers as(
    SELECT * 
    FROM customers
    INNER JOIN cities
    on cities.city_state = customers.city_state
),
suppliers as(
    select
        supplier_id,
        supplier_name,
        upper(supplier_city) as supply_city,
        supplier_state as supply_state,
        concat(supply_city,supply_state) as city_state,
        c1.geo_location as geo
    from vk_data.suppliers.supplier_info as s1
    LEFT JOIN cities as c1 ON
        upper(s1.supplier_city) = c1.city_name
        AND s1.supplier_state = c1.state
),
final_result as(
select 
    customer_id,
    first_name as customer_first_name,
    last_name as customer_last_name,
    email as customer_email,
    supplier_id,
    supplier_name,
    st_distance(geo, geo_location) /1000 as distance_in_km
from valid_customers 
CROSS JOIN suppliers 
    qualify row_number() over (partition by customer_id order by distance_in_km) = 1
        order by customer_last_name, customer_first_name
)
select * from final_result