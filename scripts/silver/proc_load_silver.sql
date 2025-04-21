/*
-------------------------------------------------------------------------------------
Script Name   : proc_load_silver
Purpose       : Stored procedure performs the ETL process to populate the 'silver'
				schema tables from the 'bronze' schema. Actions taken are:
					- Truncates 'silver' tables
					- Inserts cleaned and transformed data from 'bronze' to 'sivler' tables
Run Notes     : Run this script by using 'EXEC silver.load_silver;'
-------------------------------------------------------------------------------------
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '--------------------------------------------------';
		PRINT '	Starting Silver Layer Load';
		PRINT '	Loading CUS Tables...';
		PRINT '--------------------------------------------------';


		--------------------------------------
		-- Loading 'silver.cus_cust_data'
		--------------------------------------
		SET @start_time = GETDATE();
		PRINT'--> Truncating Table: silver.cus_cust_data';
		TRUNCATE TABLE silver.cus_cust_data;
		PRINT'--> Inserting data into: silver.cus_cust_data';
		
		-- Step 1: Clean raw customer data
		WITH cleaned_customers AS (
			SELECT
				TRIM(customer_id) as customer_id,
		
				-- Remove prefixes and certain suffixes
				CASE WHEN TRIM(customer_name) IS NULL THEN 'n/a' 
				ELSE TRIM(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(customer_name,  'Mr. ', ''),
												'Ms. ', ''),
												'Miss ', ''),
												'Dr. ', ''),
												'MD', ''),
												'DMV', ''),
												'DDS', ''),
												'PhD', '')
				) END AS customer_name,

				-- Standarized 'segment'
				CASE
					-- Variations of Consumer
					WHEN LOWER(TRIM(segment)) IN (
						'consumea', 'consumej', 'consumev', 'consumew', 'consumed',
						'consumem', 'consumeu', 'consumef', 'consumee', 'consumez',
						'consumec', 'consumel', 'consumeo', 'consumey', 'consumep',
						'consumei', 'consumeb', 'consumeg', 'consumeh', 'consumek',
						'consumex', 'consumeq', 'consumet'
					) THEN 'Consumer'

					-- Variations of Corporate
					WHEN LOWER(TRIM(segment)) IN (
						'corporatv', 'corporatx', 'corporatz', 'corporatb', 'corporatl',
						'corporatn', 'corporatt', 'corporatu', 'corporatc', 'corporatd',
						'corporato', 'corporatg', 'corporath', 'corporatf', 'corporati',
						'corporatk', 'corporatp', 'corporatr', 'corporata', 'corporatm',
						'corporatw', 'corporaty', 'corporatj', 'corporatq', 'corporats'
					) THEN 'Corporate'

					-- Variations of Home Office
					WHEN LOWER(TRIM(segment)) IN (
						'home officc', 'home officw', 'home officf', 'home offici', 'home officb',
						'home officv', 'home offica', 'home officm', 'home officy', 'home officd',
						'home officg', 'home officp', 'home offics', 'home offich', 'home offick',
						'home officn', 'home officx', 'home officj', 'home officq', 'home officr',
						'home offict', 'home officu', 'home offico', 'home officz'
					) THEN 'Home Office'

					-- Catch properly spelled values
					WHEN LOWER(TRIM(segment)) = 'consumer' THEN 'Consumer'
					WHEN LOWER(TRIM(segment)) = 'corporate' THEN 'Corporate'
					WHEN LOWER(TRIM(segment)) = 'home office' THEN 'Home Office'
					ELSE 'n/a'
				END AS segment,
	
				-- Clean other fields
				CASE WHEN TRIM(city) IS NULL THEN 'n/a' ELSE TRIM(city)	END AS city,
				CASE WHEN TRIM(state) IS NULL THEN 'n/a'ELSE TRIM(state) END AS state,
				'United States' as country, -- verified all customers are domestic
				CASE
					WHEN postal_code IS NULL OR TRIM(postal_code) = '' THEN '00000'
					WHEN LEN(TRIM(postal_code)) < 5 THEN RIGHT('00000' + TRIM(postal_code), 5)
					ELSE TRIM(postal_code)
				END AS	postal_code,
				'US' AS market, -- verified all customers are domestic
				TRIM(region) AS region,
				customer_create_date
			FROM bronze.cus_cust_data
		), 
		-- Step 2: Deduplicate by customer_id, keeping latest and most complete
		deduplicated_customers AS (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_create_date DESC,
				CASE
					WHEN customer_name = 'n/a' OR segment = 'n/a' OR city = 'n/a' OR state = 'n/a' THEN 1
					ELSE 0
				END ASC
				) AS rn
			FROM cleaned_customers
		)
		INSERT INTO silver.cus_cust_data (
			customer_id, 
			customer_name, 
			segment, city, 
			state, 
			country, 
			postal_code, 
			market, 
			region, 
			customer_create_date
		)
		SELECT
			customer_id,
			customer_name,
			segment,
			city,
			state,
			country,
			postal_code,
			market,
			region,
			customer_create_date
		FROM deduplicated_customers
		WHERE rn = 1;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------' + CHAR(13);


		--------------------------------------
		-- Loading 'silver.cus.sales_tx'
		--------------------------------------
		SET @start_time = GETDATE();
		PRINT'--> Truncating Table: silver.cus_sales_tx';
		TRUNCATE TABLE silver.cus_sales_tx;
		PRINT'--> Inserting data into: silver.cus_sales_txx';
		
		-- Step 1: Clean raw customer data
		WITH cleaned_sales_tx AS (
		SELECT
			row_id,
			customer_id,
			order_id,
			order_date,
			ship_date,
			-- Standarized 'ship_mode'
			CASE
				-- Variations of 'Same Day'
				WHEN LOWER(TRIM(ship_mode)) IN (
				'same dab', 'same dao', 'same dax', 'same daa', 'same daf', 'same dag',
				'same dak', 'same dam', 'same dar', 'same dau', 'same dai', 'same daj',
				'same dan', 'same dap', 'same dac', 'same dae', 'same dav', 'same daz',
				'same dal', 'same dat', 'same daw', 'same das', 'same dad', 'same daq'
				) THEN 'Same Day'

				-- Variations of 'First Class'
				WHEN LOWER(TRIM(ship_mode)) IN (
					'first clasb', 'first clasa', 'first clasl', 'first clash', 'first clask',
					'first clasw', 'first clasi', 'first clasn', 'first clasr', 'first clasv',
					'first clasy', 'first clasf', 'first clasp', 'first clast', 'first clasc',
					'first clase', 'first clasq', 'first clasu', 'first clasx', 'first clasg',
					'first claso', 'first clasm', 'first clasj', 'first clasz', 'first clasd'
				) THEN 'First Class'

				-- Variations of 'Second Class'
				WHEN LOWER(TRIM(ship_mode)) IN (
					'second clasw', 'second clasd', 'second clast', 'second clasv', 'second clasj',
					'second clasl', 'second clasm', 'second clasn', 'second clasr', 'second clasi',
					'second clasa', 'second clasb', 'second clasc', 'second clash', 'second clasz',
					'second clase', 'second clasu', 'second clasp', 'second clasx', 'second classy',
					'second clasf', 'second clasg', 'second claso'
				) THEN 'Second Class'

				-- Variations of 'Standard Class'
				WHEN LOWER(TRIM(ship_mode)) IN (
					'standard clask', 'standard clasf', 'standard clast', 'standard clasd',
					'standard clasq', 'standard clasl', 'standard clasa', 'standard clash',
					'standard clasp', 'standard clasx', 'standard clasj', 'standard clasw',
					'standard clase', 'standard clasm', 'standard clasu', 'standard clasv',
					'standard classy', 'standard clasn', 'standard clasr', 'standard clasz',
					'standard clasb', 'standard clasi', 'standard claso', 'standard clasc',
					'standard clasg'
				) THEN 'Standard Class'

				-- Catch properly spelled values
				WHEN LOWER(TRIM(ship_mode)) = 'same day' THEN 'Same Day'
				WHEN LOWER(TRIM(ship_mode)) = 'first class' THEN 'First Class'
				WHEN LOWER(TRIM(ship_mode)) = 'second class' THEN 'Second Class'
				WHEN LOWER(TRIM(ship_mode)) = 'standard class' THEN 'Standard Class'

				ELSE 'n/a'
			END AS ship_mode,
			TRIM(product_id) AS product_id,
			unit_price,
			quantity,
			discount,
			shipping_cost,
			TRIM(order_priority) as order_priority
		FROM bronze.cus_sales_tx
		), 
		-- Step 2: Deduplicate by customer_id, keeping latest and most complete
		deduplicated_sales_tx AS (
			SELECT
				*,
				ROW_NUMBER() OVER (
					PARTITION BY customer_id, order_id, order_date, ship_date, ship_mode, product_id, unit_price, quantity, discount, shipping_cost, order_priority 
					ORDER BY row_id) AS rn
			FROM cleaned_sales_tx
		)
		INSERT INTO silver.cus_sales_tx (
			row_id, 
			customer_id, 
			order_id, 
			order_date, 
			ship_date, 
			ship_mode, 
			product_id, 
			unit_price, 
			quantity, 
			discount, 
			shipping_cost, 
			order_priority,
			net_sales
		)
		SELECT
			row_id, 
			customer_id, 
			order_id, 
			order_date, 
			ship_date, 
			ship_mode, 
			product_id, 
			unit_price, 
			quantity, 
			discount, 
			shipping_cost, 
			order_priority,
			unit_price * quantity * (CASE WHEN discount > 0 THEN (1 - discount) ELSE 1 END) AS net_sales
		FROM deduplicated_sales_tx
		WHERE rn = 1


		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------' + CHAR(13);
		
		
		PRINT '--------------------------------------------------';
		PRINT '	Loading OPS Tables...';
		PRINT '--------------------------------------------------';
		

		--------------------------------------
		-- Loading 'silver.ops_prod_master'
		--------------------------------------
		SET @start_time = GETDATE();
		PRINT'--> Truncating Table: silver.ops_prod_master';
		TRUNCATE TABLE silver.ops_prod_master;
		PRINT'--> Inserting data into: silver.ops_prod_master';
		
		INSERT INTO silver.ops_prod_master (
			product_id, 
			category, 
			sub_category, 
			product_name, 
			unit_price
		)
		SELECT
			TRIM(product_id) AS product_id, 
			TRIM(category) AS category, 
			TRIM(sub_category) AS sub_category, 
			TRIM(product_name) AS product_name, 
			unit_price
		FROM bronze.ops_prod_master

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------' + CHAR(13);


		SET @batch_end_time = GETDATE();
		PRINT '--------------------------------------------------';
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------------------'
	END TRY
	BEGIN CATCH
		PRINT '--------------------------------------------------';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '--------------------------------------------------';
	END CATCH
END