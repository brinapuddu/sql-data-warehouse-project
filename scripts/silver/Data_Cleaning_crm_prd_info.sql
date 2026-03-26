
/*
================================================================================
Data Cleaning
================================================================================
Purpose:
    Process and clean data from the Bronze layer of the data warehouse into the Silver layer. 
Focus:
    - CRM : prd_info
================================================================================
*/





--Check for Nulls or Duplicates in Primary Key
SELECT
prd_id,
COUNT(*)
FROM 
bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--Check for unwanted spaces
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--Check for NULL or negative values
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL;

--Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

--Check for Invalid Date Orders
SELECT*
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

--Update Silver table
IF OBJECT_ID ('silver.crm_prd_info') IS NOT NULL
	DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE, 
	dwh_create_date DATETIME2 DEFAULT GETDATE()
	);


-- Insert Clean Data Into Silver table
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
