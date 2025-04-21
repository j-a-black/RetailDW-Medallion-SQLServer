/*
-------------------------------------------------------------------------------------
Script Name   : ddl_gold.sql
Purpose       : Creates views in the 'gold' schema and will drop existing views.
-------------------------------------------------------------------------------------
*/


-------------------------------------------------------------
-- Create Dimension: gold.vw_dim_customers
-------------------------------------------------------------
IF OBJECT_ID('gold.vw_dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.vw_dim_customers;
GO

CREATE VIEW gold.vw_dim_customers AS
SELECT
	cust.customer_id,
	cust.customer_name,
	cust.segment,
	cust.city,
	cust.[state],
	cust.region,
	cust.customer_create_date,
	DATEDIFF(YEAR, cust.customer_create_date, GETDATE()) AS customer_tenure_years,
	SUM(sales.quantity) AS total_units,
    SUM(sales.unit_price * sales.quantity * (1 - sales.discount)) AS total_spend
FROM silver.cus_cust_data AS cust
LEFT JOIN silver.cus_sales_tx AS sales
	ON cust.customer_id = sales.customer_id
GROUP BY 
    cust.customer_id, cust.customer_name, cust.segment, 
    cust.city, cust.[state], cust.region, cust.customer_create_date;
GO


-------------------------------------------------------------
-- Create Dimension: gold.vw_fact_sales
-------------------------------------------------------------
IF OBJECT_ID('gold.vw_fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.vw_fact_sales;
GO

CREATE VIEW gold.vw_fact_sales AS
SELECT
	sales.row_id,
	sales.order_id,
	sales.customer_id,
	cust.segment,
	cust.[state],
	cust.region,
	sales.product_id,
	prod.product_name,
	prod.category,
	prod.sub_category,
	sales.order_date,
	sales.ship_date,
	sales.ship_mode,
	sales.quantity,
	sales.unit_price,
	sales.discount,
	sales.shipping_cost,
	sales.order_priority,
	(sales.unit_price * sales.quantity) AS gross_sales,
	sales.net_sales,
	sales.net_sales + sales.shipping_cost AS total_order_value
FROM silver.cus_sales_tx as sales
LEFT JOIN silver.ops_prod_master as prod
	ON sales.product_id = prod.product_id
LEFT JOIN silver.cus_cust_data AS cust
	ON sales.customer_id = cust.customer_id;
GO


-------------------------------------------------------------
-- Create Dimension: gold.vw_dim_products
-------------------------------------------------------------
IF OBJECT_ID('gold.vw_dim_products', 'V') IS NOT NULL
	DROP VIEW gold.vw_dim_products;
GO

CREATE VIEW gold.vw_dim_products AS
SELECT
	product_id,
	product_name,
	sub_category,
	category,
	unit_price
FROM silver.ops_prod_master;
GO