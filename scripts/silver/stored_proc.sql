/*
================================================================================
Stored Procedure: silver.load_silver
================================================================================
Purpose:
    Transforms and loads data from the Bronze layer into the Silver layer of the 
    data warehouse. This process includes data cleansing, normalization, 
    deduplication, and basic business rule enforcement to prepare structured 
    datasets for downstream consumption.

Sources:
    - Bronze CRM Tables: crm_cust_info, crm_prd_info, crm_sales_details
    - Bronze ERP Tables: erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2

Features:
    - Truncates Silver tables before loading to ensure a full refresh
    - Applies data cleansing (TRIM, NULL handling, format corrections)
    - Standardizes categorical values (e.g., gender, marital status, country)
    - Deduplicates records using window functions (ROW_NUMBER)
    - Derives and recalculates fields (e.g., sales, price, product end dates)
    - Validates and converts date fields
    - Tracks load duration per table and total batch runtime
    - Error handling via TRY/CATCH block

Usage:
    EXEC silver.load_silver;

Notes:
    - Designed as part of a layered data architecture (Bronze → Silver → Gold)
    - Focuses on data quality and consistency rather than raw ingestion
    - Intended for transformation and standardization of source system data
================================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN 
	DECLARE @start_time DATETIME,@end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	    SET @batch_start_time = GETDATE()
		PRINT '=============================================';
		PRINT 'Loading Silver Layer';
		PRINT '=============================================';

		PRINT '---------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------';

		--Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info; 
		PRINT '>> Inserting data into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)

		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' --Normalise marital status values 
			ELSE 
			'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' -- Normalise Gender Values
			ELSE 
			'n/a'
		END cst_gndr,
		cst_create_date
		FROM 
			(SELECT
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM
			bronze.crm_cust_info)t
		where flag_last = 1;

		---Empty Row deletion 
		DELETE FROM silver.crm_cust_info
		WHERE cst_id IS NULL;

		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> --------------------';
        

		--Loading silver.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info; 
		PRINT '>> Inserting data into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost ,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)

		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, --Extract category ID
		SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key, -- Extract product ID
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost, 
		CASE UPPER(TRIM(prd_line)) 
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
		END as prd_line, --Map product line to descriptive values
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt --Calculate end date before the next start date
		FROM bronze.crm_prd_info 

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------------';
        

		--Loading silver.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details; 
		PRINT '>> Inserting data into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales, 
			sls_quantity,
			sls_price)

		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE)
		END sls_order_dt,
		CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS varchar) AS DATE)
		END sls_ship_dt,
		CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS varchar) AS DATE)
		END sls_due_dt,
		CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity* ABS(sls_price) 
			THEN sls_quantity* ABS(sls_price) 
		 ELSE sls_sales
		END AS sls_sales, -- Recalculate sales if value is missing or invalid
		sls_quantity,
		CASE WHEN sls_price <= 0 OR sls_price IS NULL
			THEN sls_sales/NULLIF(sls_quantity, 0)
		 ELSE sls_price -- Derive price if original value is invalid
		END AS sls_price
		FROM bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------------';

		PRINT '---------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------------';

		--Loading silver.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12; 
		PRINT '>> Inserting data into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END cid,
		CASE WHEN  bdate > GETDATE() THEN NULL
			ELSE bdate
		END bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------------';


		--Loading silver.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101; 
		PRINT '>> Inserting data into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 
		(cid, cntry)

		SELECT 
		REPLACE(cid, '-', '') cid, --Handling invalid values 
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry) --Normalize and handle missing or blank country codes 
		END cntry
		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------------';

		--Loading silver.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2; 
		PRINT '>> Inserting data into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
		id, 
		cat,
		subcat,
		maintenance
		)

		SELECT
		id, 
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;
	
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
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
