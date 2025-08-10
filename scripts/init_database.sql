
/*
========================================
Create Database and schemas
========================================
script Purpose:
	This script creates a new database named 'Datawarehouse' after checking if it already exists.
	if the database exists, it is dropped amd recreated. Additionally, the script sets up three schemas
	within the database: 'bronze','silver',and 'gold'.

WARNING:
	Runing this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanetly deleted proceed with caution and 
	backuo before running this script 
	*/


USE MASTER;
GO
If EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK_TIMEDATE;
	DROP DATABASE Datawarehouse;
END;
GO
CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
