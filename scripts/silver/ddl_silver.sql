/*
-------------------------------------------------------------------------------------
Script Name   : ddl_silver.sql
Purpose       : Creates tables in the 'silver' schema and will drop existing tables.
Run Notes     : Run this script to re-define the DDL structure of 'silver' tables.
-------------------------------------------------------------------------------------
*/

USE OnlineRetailDW;
GO

-- Source: cus

IF OBJECT_ID('silver.cus_cust_data', 'U') IS NOT NULL
	DROP TABLE silver.cus_cust_data;
GO

CREATE TABLE silver.cus_cust_data (
	customer_id NVARCHAR(50),
	customer_name NVARCHAR(50),
	segment NVARCHAR(50),
	city NVARCHAR(50),
	state NVARCHAR(50),
	country NVARCHAR(50),
	postal_code NVARCHAR(10),
	market NVARCHAR(3),
	region NVARCHAR(10),
	customer_create_date DATE,
	dw_create_date DATETIME2 DEFAULT GETDATE(),
	dw_last_updated DATETIME2 DEFAULT GETDATE(),
	dw_source NVARCHAR(100) DEFAULT 'CUS_System'
);
GO

IF OBJECT_ID('silver.cus_sales_tx', 'U') IS NOT NULL
	DROP TABLE silver.cus_sales_tx;
GO

CREATE TABLE silver.cus_sales_tx (
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
	order_priority NVARCHAR(10),
	net_sales DECIMAL(10,2),
	dw_create_date DATETIME2 DEFAULT GETDATE(),
	dw_last_updated DATETIME2 DEFAULT GETDATE(),
	dw_source NVARCHAR(100) DEFAULT 'CUS_System'
);
GO

-- Source: ops

IF OBJECT_ID('silver.ops_prod_master', 'U') IS NOT NULL
	DROP TABLE silver.ops_prod_master;
GO

CREATE TABLE silver.ops_prod_master (
	product_id NVARCHAR(50),
	category NVARCHAR(50),
	sub_category NVARCHAR(50),
	product_name NVARCHAR(50),
	unit_price DECIMAL(10,2),
	dw_create_date DATETIME2 DEFAULT GETDATE(),
	dw_last_updated DATETIME2 DEFAULT GETDATE(),
	dw_source NVARCHAR(100) DEFAULT 'OPS_System'
);
GO

IF OBJECT_ID('silver.ops_ship_data', 'U') IS NOT NULL
	DROP TABLE silver.ops_ship_data;
GO

