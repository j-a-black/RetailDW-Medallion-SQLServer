/*
-------------------------------------------------------------------------------------
Script Name   : quality_checks_silver.sql
Purpose       : Performs quality checks for data consistency, accuracy, and 
				standardization across the 'silver' layer. QC includes:
					- Null or duplicate primary keys.
					- Unwanted spaces in string fields.
					- Data standardization and consistency.
					- Invalid date ranges and orders.
					- Data consistency between related fields.
Run Notes     : Run this script after loading 'silver' layer.
-------------------------------------------------------------------------------------
*/


------------------------------------------------------------------
-- Checking 'silver.cus.cust_data'
------------------------------------------------------------------
-- Checking for white spaces
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN customer_id       != TRIM(customer_id)       THEN 1 ELSE 0 END) AS customer_id_dirty,
    SUM(CASE WHEN customer_name     != TRIM(customer_name)     THEN 1 ELSE 0 END) AS customer_name_dirty,
    SUM(CASE WHEN segment           != TRIM(segment)           THEN 1 ELSE 0 END) AS segment_dirty,
    SUM(CASE WHEN city              != TRIM(city)              THEN 1 ELSE 0 END) AS city_dirty,
    SUM(CASE WHEN state             != TRIM(state)             THEN 1 ELSE 0 END) AS state_dirty,
    SUM(CASE WHEN country           != TRIM(country)           THEN 1 ELSE 0 END) AS country_dirty,
    SUM(CASE WHEN postal_code       != TRIM(postal_code)       THEN 1 ELSE 0 END) AS postal_code_dirty,
    SUM(CASE WHEN market            != TRIM(market)            THEN 1 ELSE 0 END) AS market_dirty,
    SUM(CASE WHEN region            != TRIM(region)            THEN 1 ELSE 0 END) AS region_dirty
FROM silver.cus_cust_data;

-- Data standardization and consistency
SELECT DISTINCT
	trim([state]) as [state], -- will cycle thu columns as appropriate
	COUNT(*) AS count
FROM silver.cus_cust_data
GROUP BY state
ORDER BY count DESC, state

SELECT postal_code
FROM silver.cus_cust_data
WHERE LEN(TRIM(postal_code)) < 5 AND postal_code IS NOT NULL;

SELECT city
FROM silver.cus_cust_data
WHERE city IS NULL;

-- Check for duplicates
SELECT
	customer_id,
	COUNT(*) AS count
FROM silver.cus_cust_data
GROUP BY customer_id
HAVING COUNT(*) > 1

-- Checking for data consistency between related fields
SELECT DISTINCT 
	TRIM(sales.customer_id) AS customer_id
FROM silver.cus_sales_tx AS sales
LEFT JOIN silver.cus_cust_data AS cust
    ON TRIM(sales.customer_id) = TRIM(cust.customer_id)
WHERE TRIM(cust.customer_id) IS NULL

SELECT DISTINCT
	TRIM(cust.customer_id) AS customer_id
FROM silver.cus_cust_data AS cust
LEFT JOIN silver.cus_sales_tx AS sales
	ON TRIM(cust.customer_id) = TRIM(sales.customer_id)
WHERE TRIM(sales.customer_id) IS NULL

SELECT DISTINCT 
	TRIM(sales.product_id) AS product_id
FROM silver.cus_sales_tx AS sales
LEFT JOIN silver.ops_prod_master AS prod
    ON TRIM(sales.product_id) = TRIM(prod.product_id)
WHERE TRIM(prod.product_id) IS NULL

SELECT DISTINCT 
	TRIM(prod.product_id) AS product_id
FROM silver.ops_prod_master AS prod
LEFT JOIN silver.cus_sales_tx AS sales 
    ON TRIM(prod.product_id) = TRIM(sales.product_id)
WHERE TRIM(sales.product_id) IS NULL


------------------------------------------------------------------
-- Checking 'silver.cus_sales_tx'
------------------------------------------------------------------
-- Check for white spaces
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN customer_id       != TRIM(customer_id)        THEN 1 ELSE 0 END) AS customer_id_dirty,
    SUM(CASE WHEN order_id		    != TRIM(order_id)			THEN 1 ELSE 0 END) AS order_id_dirty,
    SUM(CASE WHEN ship_mode         != TRIM(ship_mode)          THEN 1 ELSE 0 END) AS ship_mode_dirty,
    SUM(CASE WHEN product_id        != TRIM(product_id )        THEN 1 ELSE 0 END) AS product_id_dirty,
	SUM(CASE WHEN order_priority    != TRIM(order_priority)     THEN 1 ELSE 0 END) AS order_priority_dirty
from silver.cus_sales_tx

-- Data standardization and consistency
SELECT DISTINCT
	order_priority,
	COUNT(*) AS count
FROM silver.cus_sales_tx
GROUP BY order_priority
ORDER BY count DESC, order_priority

-- Check for NULLs
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN row_id				IS NULL							THEN 1 ELSE 0 END) AS null_row_id,
    SUM(CASE WHEN TRIM(customer_id)		= '' OR customer_id IS NULL		THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN TRIM(order_id)		= '' OR order_id IS NULL		THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN order_date			IS NULL							THEN 1 ELSE 0 END) AS null_order_date,
    SUM(CASE WHEN ship_date				IS NULL							THEN 1 ELSE 0 END) AS null_ship_date,
    SUM(CASE WHEN TRIM(ship_mode)		= '' OR ship_mode IS NULL		THEN 1 ELSE 0 END) AS null_ship_mode,
    SUM(CASE WHEN TRIM(product_id)		= '' OR product_id IS NULL		THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN unit_price			IS NULL							THEN 1 ELSE 0 END) AS null_unit_price,
    SUM(CASE WHEN quantity				IS NULL							THEN 1 ELSE 0 END) AS null_quantity,
    SUM(CASE WHEN discount				IS NULL							THEN 1 ELSE 0 END) AS null_discount,
    SUM(CASE WHEN shipping_cost			IS NULL							THEN 1 ELSE 0 END) AS null_shipping_cost,
    SUM(CASE WHEN TRIM(order_priority)  = '' OR order_priority IS NULL  THEN 1 ELSE 0 END) AS null_order_priority
FROM silver.cus_sales_tx

-- Check for duplicates
SELECT
	customer_id,
	order_id,
	COUNT(*) AS dup_count
FROM silver.cus_sales_tx
GROUP BY customer_id, order_id
HAVING COUNT(*) > 1

 -- Check for negative values in unit_price, discount, shipping_cost
SELECT
	unit_price,
	discount,
	shipping_cost
FROM silver.cus_sales_tx
WHERE unit_price < 0 OR unit_price IS NULL
	  OR discount < 0 OR discount IS NULL
	  OR shipping_cost < 0 OR shipping_cost IS NULL

-- Checking for data consistency between related fields
SELECT DISTINCT
	TRIM(cust.customer_id) AS customer_id
FROM silver.cus_cust_data AS cust
LEFT JOIN silver.ops_ship_data AS ship
	ON TRIM(cust.customer_id) = TRIM(ship.customer_id)
WHERE ship.customer_id IS NULL

SELECT DISTINCT
	TRIM(ship.customer_id) AS customer_id
FROM silver.ops_ship_data AS ship
LEFT JOIN silver.cus_cust_data AS cust
	ON TRIM(ship.customer_id) = TRIM(cust.customer_id)
WHERE cust.customer_id IS NULL

-- Check for invalid data ranges
SELECT
	order_date,
	ship_date
FROM bronze.cus_sales_tx
WHERE order_date > ship_date


------------------------------------------------------------------
-- Checking 'silver.ops_product_master'
------------------------------------------------------------------
-- Check for NULLs
SELECT
	SUM(CASE WHEN TRIM(product_id)		= '' OR product_id IS NULL		THEN 1 ELSE 0 END) AS null_product_id,
	SUM(CASE WHEN TRIM(category)		= '' OR category IS NULL		THEN 1 ELSE 0 END) AS null_category,
	SUM(CASE WHEN TRIM(sub_category)	= '' OR sub_category IS NULL	THEN 1 ELSE 0 END) AS null_sub_category,
	SUM(CASE WHEN TRIM(product_name)	= '' OR product_name IS NULL	THEN 1 ELSE 0 END) AS null_product_name,
	SUM(CASE WHEN unit_price			<= 0 OR unit_price IS NULL		THEN 1 ELSE 0 END) AS null_product_id
FROM silver.ops_prod_master

-- Data standardization and consistency
SELECT DISTINCT
	product_name
FROM silver.ops_prod_master

-- Check for unwanted spaces
SELECT
	SUM(CASE WHEN product_id	!= TRIM(product_id)		THEN 1 ELSE 0 END) AS product_id_dirty,
	SUM(CASE WHEN category		!= TRIM(category)		THEN 1 ELSE 0 END) AS category_dirty,
	SUM(CASE WHEN sub_category  != TRIM(sub_category)	THEN 1 ELSE 0 END) AS sub_category_dirty,
	SUM(CASE WHEN product_name  != TRIM(product_name)	THEN 1 ELSE 0 END) AS product_name_dirty
FROM silver.ops_prod_master

