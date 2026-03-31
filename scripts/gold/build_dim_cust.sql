/*
-- ============================================================
-- Data Integration & Gold Layer View Creation
-- ============================================================
*/

-- ------------------------------------------------------------
-- Part 1: Gender Reconciliation Check
-- ------------------------------------------------------------
-- Validates gender data across CRM and ERP source systems.
-- CRM is the authoritative (master) source for gender.
-- Falls back to ERP value if CRM is 'n/a', defaults to 'n/a'
-- if both sources are missing.
-- ------------------------------------------------------------

--Data Integration 
SELECT 
ci.cst_gndr,
ca.gen,
CASE WHEN ci.cst_gndr != 'n/a' THEN  ci.cst_gndr --CRM is the Master for gender info
	ELSE COALESCE(ca.gen, 'n/a')
END AS new_gender
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
ORDER BY 1,2

CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry as country,
ci.cst_marital_status AS marital_status,
CASE WHEN ci.cst_gndr != 'n/a' THEN  ci.cst_gndr --CRM is the Master for gender info
	ELSE COALESCE(ca.gen, 'n/a')
END AS gender,
ca.bdate as birthdate,
ci.cst_create_date AS create_date, 
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid
