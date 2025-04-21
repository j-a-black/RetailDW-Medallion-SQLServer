/*
--------------------------------------------------------------------------------
-- Script Name   : init_database.sql
-- Purpose       : Creates a new database name 'GlobalRetailDW'. If the database exits,
				   it will be dropped and recreated. The script also establishes three
				   schemas: 'bronze', 'silver', and 'gold.
-- Run Notes     : Executing this script will first permanently delete all existing data
				   and then create a new database.
--------------------------------------------------------------------------------
*/

USE master;
GO

-- Drop and recreate 'GlobalRetailDW' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'OnlineRetailDW')
BEGIN
	ALTER DATABASE OnlineRetailDW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE OnlineRetailDW;
END;
GO

-- Create 'GlobalRetailDW' database
CREATE DATABASE OnlineRetailDW;
GO

USE OnlineRetailDW;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO