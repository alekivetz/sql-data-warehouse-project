/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================

-- Check for nulls or duplicates in primary key
SELECT
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for whitespaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for nulls or negative numbers
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL;

-- Data standardization & consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for invalid date orders
SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- TEST
SELECT 
    prd_id, 
    prd_key, 
    prd_nm, 
    prd_start_dt, 
    prd_end_dt,
DATEADD(day, -1,
	LEAD(prd_start_dt) 
	OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
) AS prd_end_dt_test
FROM silver.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE--U509');

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================

-- Check for invalid dates
SELECT 
    NULLIF(sls_order_dt,0) sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) < 8
OR sls_order_dt < 19900101
OR sls_order_dt > 20300101;

-- Check for invalid date orders
SELECT 
    *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check data consistency between sales/quantity/price
SELECT 
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,

    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		    THEN sls_quantity * ABS(sls_price)
	     ELSE sls_sales
    END AS sls_sales,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
		    THEN sls_sales / NULLIF(sls_quantity, 0)
	     ELSE sls_price
    END AS sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_sales IS NULL
OR sls_quantity <= 0 OR sls_quantity IS NULL
OR sls_price <= 0 OR sls_price IS NULL;
 
-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================

-- Identify out of range dates
SELECT DISTINCT
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Data standardization & consistency
SELECT DISTINCT
    gen
FROM silver.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================

-- Data standardization & consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101;

SELECT * FROM silver.erp_loc_a101;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================

-- Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data standardization & consistency
SELECT DISTINCT 
cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT 
subcat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT 
maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.erp_px_cat_g1v2;
