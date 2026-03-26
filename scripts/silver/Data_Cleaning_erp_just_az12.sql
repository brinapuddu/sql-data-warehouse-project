/*
================================================================================
Data Cleaning
================================================================================
Purpose:
    Process and clean data from the Bronze layer of the data warehouse into the Silver layer. 
Focus:
    - ERP : erp_just_az12
================================================================================
*/

--Identify out of range birth dates
SELECT DISTINCT
bdate
FROM bronze.erp_just_az12
WHERE bdate < '1926-01-01' OR bdate > GETDATE();

--Data Standardization & Consistency
SELECT
gen
FROM bronze.erp_just_az12;


INSERT INTO silver.erp_just_az12 (
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
FROM bronze.erp_just_az12

--Final Check
SELECT*
FROM
silver.erp_just_az12
