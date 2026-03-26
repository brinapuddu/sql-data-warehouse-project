/*
================================================================================
Data Cleaning
================================================================================
Purpose:
    Process and clean data from the Bronze layer of the data warehouse into the Silver layer. 
Focus:
    - CRM : crm_sales_details
================================================================================
*/

--Check for invalid date
SELECT
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM
bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt < 190001
OR sls_order_dt > 20500101;

--Check for invalid date orders
SELECT*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_due_dt OR sls_order_dt > sls_ship_dt;

--Check Data Consistency between sales, price, and quantity
--Sales = Quantity*Price
-- Values must be NULL, zero, or negative
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales;

--If sales are negative, zero, or null we will derive them using quantity and price
--If sales are negative, convert them to a positive value
--If price is zero, calculate it using quantity and sales


IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
 DROP TABLE silver.crm_sales_details; 

CREATE TABLE silver.crm_sales_details (

	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT, 
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

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

--Final Check
SELECT*
FROM bronze.crm_sales_details;
