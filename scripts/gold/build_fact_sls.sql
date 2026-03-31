/*
-- ============================================================
-- Gold Layer View Creation & Validation - fact_sales
-- ============================================================
*/

-- ------------------------------------------------------------
-- Gold Fact View - fact_sales
-- ------------------------------------------------------------
-- Creates the central sales fact table in the Gold layer by
-- integrating one Silver layer source with two Gold dimensions:
--   - crm_sales_details  : Core transactional sales data
--   - gold.dim_products  : Product surrogate key lookup
--   - gold.dim_customers : Customer surrogate key lookup
--
-- Key design decisions:
--   - Surrogate keys from both dimension tables replace the
--     raw source IDs, ensuring referential integrity across
--     the Gold layer
--   - LEFT JOINs retain all sales records even if a matching
--     dimension entry is not found
--   - Columns aliased to business-friendly names
-- ------------------------------------------------------------

CREATE VIEW gold.fact_sales AS


CREATE VIEW gold.fact_sales AS 

SELECT
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date, 
sd.sls_due_dt AS due_date, 
sd.sls_sales AS sales,
sd.sls_quantity As sales_quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd 
LEFT JOIN gold.dim_products pr 
ON sd.sls_prd_key = pr.product_number 
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;

--Foreign Key Integrity( Dimensions)
SELECT*
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL 
