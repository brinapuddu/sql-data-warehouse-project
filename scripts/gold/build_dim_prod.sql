*
-- ============================================================
-- Gold Layer View Creation - dim_products
-- ============================================================
*/

-- ------------------------------------------------------------
-- Gold Dimension View - dim_products
-- ------------------------------------------------------------
-- Creates a unified product dimension in the Gold layer by
-- integrating two Silver layer sources:
--   - crm_prd_info      : Core product attributes
--   - erp_px_cat_g1v2   : Category enrichment data
--                         (category, subcategory, maintenance)
--
-- Key design decisions:
--   - Surrogate key generated via ROW_NUMBER() ordered by
--     start date and product key for deterministic ordering
--   - Only active products are included (prd_end_dt IS NULL)
--     filtering out all historical/expired records
--   - LEFT JOIN retains all products even if category
--     data is missing in the ERP system
--   - Columns aliased to business-friendly names
-- ------------------------------------------------------------

CREATE VIEW gold.dim_products AS 
SELECT
ROW_NUMBER() OVER(ORDER BY prd_start_dt,prd_key) AS product_key, 
	pn.prd_id AS product_id,
	pn.prd_nm AS product_name,
	pn.prd_key AS product_number,
	pn.prd_cost AS product_cost, 
	pn.prd_line AS product_line, 
	pn.prd_start_dt AS start_date, 
	pn.cat_id AS category_id,
	pc.cat AS category, 
	pc.subcat AS subcategory,
	pc.maintenance AS maintenance 
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id 
WHERE prd_end_dt IS NULL -- Filter our all the historica data
