
/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/



USE MASTER;
GO

-- DROP AND RECREATE THE 'DATAWAREHOUSE' DATABASE IF IT EXISTS
IF EXISTS (SELECT 1 FROM SYS.DATABASES WHERE NAME = 'DATAWAREHOUSE')
BEGIN
	ALTER DATABASE DATAWAREHOUSE SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DATAWAREHOUSE;
END
GO

-- Create the DataWarehouse database
CREATE DATABASE DataWarehouse;

USE DATAWAREHOUSE;

-- Create Schemas
CREATE SCHEMA BRONZE;
GO
CREATE SCHEMA SILVER;
GO
CREATE SCHEMA GOLD;
GO


