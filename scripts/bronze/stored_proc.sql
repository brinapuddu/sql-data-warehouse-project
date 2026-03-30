/*
================================================================================
Stored Procedure: bronze.load_bronze
================================================================================
Purpose:
    Loads raw data into the Bronze layer of the data warehouse by bulk inserting
    CSV files from local disk into staging tables. Each table is truncated prior
    to loading to ensure a clean, full refresh.

Sources:
    - CRM : cust_info, prd_info, sales_details
    - ERP : CUST_AZ12, LOC_A101, PX_CAT_G1V2

Features:
    - Tracks load duration per table and total batch runtime
    - Error handling via TRY/CATCH block

Usage:
    EXEC bronze.load_bronze;

Notes:
    - File paths are hardcoded and must be updated to match the local environment
    - Intended for development/learning purposes only
================================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	    SET @batch_start_time = GETDATE()
		PRINT '=============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=============================================';

		PRINT '---------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info; 

		PRINT '>> Inserting Data into Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\UNG\OneDrive - QlikTech Inc\Documents\Certifications\sql-ultimate-course\sql-ultimate-course\docs\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------';
        
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info; 

		PRINT '>> Inserting Data into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\UNG\OneDrive - QlikTech Inc\Documents\Certifications\sql-ultimate-course\sql-ultimate-course\docs\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH ( 
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details; 

		PRINT '>> Inserting Data into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\UNG\OneDrive - QlikTech Inc\Documents\Certifications\sql-ultimate-course\sql-ultimate-course\docs\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH ( 
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------';


		PRINT '---------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12; 

		PRINT '>> Inserting Data into Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\UNG\OneDrive - QlikTech Inc\Documents\Certifications\sql-ultimate-course\sql-ultimate-course\docs\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH ( 
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101; 

		PRINT '>> Inserting Data into Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\UNG\OneDrive - QlikTech Inc\Documents\Certifications\sql-ultimate-course\sql-ultimate-course\docs\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH ( 
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2; 

		PRINT '>> Inserting Data into Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\UNG\OneDrive - QlikTech Inc\Documents\Certifications\sql-ultimate-course\sql-ultimate-course\docs\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH ( 
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
	    SET @batch_end_time = GETDATE()
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------';

	END TRY
	BEGIN CATCH
	PRINT '---------------------------------------------';
	PRINT 'ERROR OCCURRED DURING BRONZE LAYER'
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS VARCHAR);
	PRINT '---------------------------------------------';
	END CATCH


END


