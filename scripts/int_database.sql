/*
==========================================================
Create Database and Schemas
==========================================================
Script Purpose:
    This script initializes a new database called 'DataWarehouse'. It first checks whether the database already exists. 
    If so, it removes and rebuilds it from scratch. Three schemas are then established within the database: 'bronze', 'silver',
    and 'gold'.

WARNING:
    Executing this script will permanently destroy the existing 'DataWarehouse' database if one is found. All stored data
    will be irretrievably lost. Make sure you have a valid backup in place before proceeding.
*/

USE MASTER;
GO

--- Drop and recreate the 'DataWarehouse'database
  
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO 

--- Create Database 'Datawarehouse' 
  
CREATE DATABASE Datawarehouse;

USE Datawarehouse;

CREATE SCHEMA bronze;
GO 
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO



  
