/*
-------------------------------------------------------------------------------------------
Script Name   : proc_load_bronze.sql
Purpose       : This stored procedure load data into the 'bronze' schema from .csv files.
				The procedure truncates the table before loading data using the
				"BULK INSERT" command
Notes		  : Use by running "EXEC bronze.load_bronze;"
-------------------------------------------------------------------------------------------
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '--------------------------------------------------';
		PRINT '	Starting Bronze Layer Load';
		PRINT '	Loading CUS Tables...';
		PRINT '--------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'--> Truncating Table: bronze.cus_cust_data';
		TRUNCATE TABLE bronze.cus_cust_data;
		PRINT'--> Inserting data into: bronze.cus_cust_data';
		BULK INSERT bronze.cus_cust_data
		FROM 'C:\Users\jebus\Documents\da_portfolio\online_retail_store\dw\datasets\source_cus\cust_data.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------' + CHAR(13);


		SET @start_time = GETDATE();
		PRINT'--> Truncating Table: bronze.cus_sales_tx';
		TRUNCATE TABLE bronze.cus_sales_tx;
		PRINT'--> Inserting data into: bronze.cus_sales_tx';
		BULK INSERT bronze.cus_sales_tx
		FROM 'C:\Users\jebus\Documents\da_portfolio\online_retail_store\dw\datasets\source_cus\sales_tx.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------' + CHAR(13);

		PRINT '--------------------------------------------------';
		PRINT '	Loading OPS Tables...';
		PRINT '--------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'--> Truncating Table: bronze.ops_prod_master';
		TRUNCATE TABLE bronze.ops_prod_master;
		PRINT'--> Inserting data into: bronze.ops_prod_master';
		BULK INSERT bronze.ops_prod_master
		FROM 'C:\Users\jebus\Documents\da_portfolio\online_retail_store\dw\datasets\source_ops\prod_master.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------' + CHAR(13);


		SET @batch_end_time = GETDATE();
		PRINT '--------------------------------------------------';
		PRINT 'Loading Bronze Layer is Completed';
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