/*
================================================================================
Data Cleaning
================================================================================
Purpose:
    Process and clean data from the Bronze layer of the data warehouse into the Silver layer. 
Focus:
    - ERP : erp_px_cat_g1v2
================================================================================
*/



--Check for unwanted spaces
SELECT* 
FROM
bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat);

--Data Standardization & Consistency
SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2;

--Insertion of data into silver layer
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

--Final Check
SELECT*
FROM silver.erp_px_cat_g1v2;
