/*
-------------------------------------------------------------------------------------
Script Name   : quality_checks_gold.sql
Purpose       : Performs quality checks for data consistency, accuracy, and 
				integrity of the 'gold' layer. QC includes:
					- Uniqueness of PK in views
					- Connectivity between fact and dimension vews
-------------------------------------------------------------------------------------
*/


------------------------------------------------------------------
-- Checking 'gold.vw_dim_customers'
------------------------------------------------------------------
-- Checking for customer_id duplicates
SELECT
	customer_id,
	COUNT(*) AS dup_count
FROM gold.vw_dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1

------------------------------------------------------------------
-- Checking 'gold.vw_fact_sales'
------------------------------------------------------------------
-- Checking for row_id duplicates
SELECT
	row_id,
	COUNT (*) AS dup_count
FROM gold.vw_fact_sales
GROUP BY row_id
HAVING COUNT(*) > 1

-- Checking connectivity between fact and dimensions
SELECT
	*
FROM gold.vw_fact_sales AS sales
LEFT JOIN gold.vw_dim_customers AS cust
	ON sales.customer_id = cust.customer_id
LEFT JOIN gold.vw_dim_products AS prod
	ON sales.product_id = prod.product_id
WHERE prod.product_id IS NULL OR cust.customer_id IS NULL


------------------------------------------------------------------
-- Checking 'gold.vw_dim_products'
------------------------------------------------------------------
-- Checking for product_id duplicates
SELECT
	product_id,
	COUNT (*) AS dup_count
FROM gold.vw_dim_products
GROUP BY product_id
HAVING COUNT(*) > 1