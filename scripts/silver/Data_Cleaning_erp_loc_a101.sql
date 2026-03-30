/*
================================================================================
Data Cleaning
================================================================================
Purpose:
    Process and clean data from the Bronze layer of the data warehouse into the Silver layer. 
Focus:
    - ERP : erp_loc_a101
================================================================================
*/


--Data Standardization & Consistency 
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101;


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

--Final check
SELECT*
FROM silver.erp_loc_a101;
