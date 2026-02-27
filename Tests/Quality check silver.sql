/*
=============================================================================
Quality Checks
=============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
=============================================================================
*/


----------------------<<Silver.crm_cust_info>>--------------------
SELECT cst_id,
       COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- 2- check unwanted spaces 

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status); -- no

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr); -- no

-- >> checking the data standardization & consistency

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- we consider that we must have the full word not as an abbreviation

---------------------------<<Silver.crm_prd_info>>---------------------------------------
SELECT prd_id,
       COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT prd_cost
FROM Silver.crm_prd_info
WHERE prd_cost<0 OR prd_cost IS NULL 

SELECT DISTINCT(prd_line),
       COUNT(*)
FROM Silver.crm_prd_info
GROUP BY(prd_line)

--all are good

---------------------------<<silver.crm_sales_details>>---------------------------------------
SELECT * FROM silver.crm_sales_details

------------------------------------------<<silver.erp_cust_az12>>--------------------------------
SELECT *
FROM silver.erp_cust_az12
----------------------------------------<<silver.erp_loc_a101>>--------------------
SELECT *
FROM silver.erp_loc_a101
---------------------------------------<<silver.erp_px_cat_g1v2>>-------------------------
SELECT * 
FROM silver.erp_px_cat_g1v2
