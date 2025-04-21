/*
-------------------------------------------------------------------------------------
Script Name   : ddl_bronze.sql
Purpose       : Creates tables in the 'bronze' schema and will drop existing tables.
Run Notes     : Run this script to re-define the DDL structure of 'bronze' tables.
-------------------------------------------------------------------------------------
*/


USE OnlineRetailDW;
GO

-- Source: cus

IF OBJECT_ID('bronze.cus_cust_data', 'U') IS NOT NULL
	DROP TABLE bronze.cus_cust_data;
GO

CREATE TABLE bronze.cus_cust_data (
	customer_id NVARCHAR(50),
	customer_name NVARCHAR(50),
	segment NVARCHAR(50),
	city NVARCHAR(50),
	state NVARCHAR(50),
	country NVARCHAR(50),
	postal_code NVARCHAR(10),
	market NVARCHAR(3),
	region NVARCHAR(10),
	customer_create_date DATE
);
GO

IF OBJECT_ID('bronze.cus_sales_tx', 'U') IS NOT NULL
	DROP TABLE bronze.cus_sales_tx;
GO

CREATE TABLE bronze.cus_sales_tx (
	row_id INT,
	customer_id NVARCHAR(50),
	order_id NVARCHAR(20),
	order_date DATE,
	ship_date DATE,
	ship_mode NVARCHAR(20),
	product_id NVARCHAR(50),
	unit_price DECIMAL(10,2),
	quantity INT,
	discount DECIMAL(4,2),
	shipping_cost DECIMAL(5,2),
	order_priority NVARCHAR(10)
);
GO

-- Source: ops

IF OBJECT_ID('bronze.ops_prod_master', 'U') IS NOT NULL
	DROP TABLE bronze.ops_prod_master;
GO

CREATE TABLE bronze.ops_prod_master (
	product_id NVARCHAR(50),
	category NVARCHAR(50),
	sub_category NVARCHAR(50),
	product_name NVARCHAR(50),
	unit_price DECIMAL(10,2)
);
GO
