-- Create the read-only analysis view after loading the normalized sales schema.

CREATE OR REPLACE VIEW vw_sales_analysis AS
SELECT
    o.order_id,
    oi.row_id,
    o.customer_id,
    cu.customer_name,
    oi.product_id,
    p.product_name,
    o.order_date,
    o.ship_date,
    oi.sales,
    oi.quantity,
    oi.discount,
    oi.profit,
    oi.shipping_cost,
    (ret.order_id IS NOT NULL) AS is_returned,
    cu.segment,
    cat.category_name,
    sc.sub_category_name,
    mk.market_name,
    co.country_name,
    st.state_name,
    ci.city_name,
    rg.region_name,
    mg.manager_name,
    loc.location_id,
    loc.postal_code,
    o.ship_mode,
    o.order_priority
FROM orders_normalized AS o
INNER JOIN order_items AS oi
    ON oi.order_id = o.order_id
INNER JOIN customers AS cu
    ON cu.customer_id = o.customer_id
INNER JOIN products AS p
    ON p.product_id = oi.product_id
INNER JOIN sub_categories AS sc
    ON sc.sub_category_id = p.sub_category_id
INNER JOIN categories AS cat
    ON cat.category_id = sc.category_id
INNER JOIN locations AS loc
    ON loc.location_id = o.location_id
INNER JOIN cities AS ci
    ON ci.city_id = loc.city_id
INNER JOIN states AS st
    ON st.state_id = ci.state_id
INNER JOIN countries AS co
    ON co.country_id = st.country_id
INNER JOIN markets AS mk
    ON mk.market_id = co.market_id
INNER JOIN regions AS rg
    ON rg.region_id = ci.region_id
LEFT JOIN managers AS mg
    ON mg.manager_id = rg.manager_id
LEFT JOIN order_returns AS ret
    ON ret.order_id = o.order_id;
