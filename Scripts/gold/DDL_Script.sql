/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customer','v') IS NOT NULL
    DROP gold.dim_customer;
GO
CREATE VIEW gold.dim_customers AS
SELECT 
       ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- surrogate key
       ci.cst_id AS customer_ID,
       ci.cst_key AS customer_number,
       ci.cst_firstname AS first_name,
       ci.cst_lastname AS last_name,
       cl.cntry AS country,
       ci.cst_marital_status AS marital_status,
       CASE 
           WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr 
           ELSE COALESCE(ca.gen, 'N/A')
       END AS gender,
       ca.bdate AS birthdate,
       ci.cst_create_date AS create_date
FROM Silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
       ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 cl
       ON ci.cst_key = cl.cid;

SELECT * FROM gold.dim_customers;


-- =========================
-- Second Dimension: Products
-- =========================
IF OBJECT_ID('gold.dim_products','v') IS NOT NULL
    DROP gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT 
      ROW_NUMBER() OVER (ORDER BY cp.prd_start_dt, cp.prd_id) AS product_key,
      cp.prd_id          AS product_id,
      cp.prd_key         AS product_number,
      cp.prd_nm          AS product_name,
      cp.cat_id          AS category_id,
      pp.cat             AS product_category,
      pp.subcat          AS sub_category,
      pp.maintenance     AS maintenance,
      cp.prd_cost        AS product_cost, 
      cp.prd_line        AS line,  
      cp.prd_start_dt    AS start_date,
      cp.dwh_create_date AS creation_date
FROM silver.crm_prd_info cp
JOIN silver.erp_px_cat_g1v2 pp
     ON cp.cat_id = pp.id
WHERE cp.prd_end_dt IS NULL; -- exclude historical data


-- =========================
-- Third  Dimension: Sales Fact
-- =========================
IF OBJECT_ID('gold.fact_sales','v') IS NOT NULL
    DROP gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT 
      s.sls_ord_num      AS order_number, -- PK
      p.product_key,     -- surrogate key from dim_products
      c.customer_ID,     -- business key from dim_customers
      s.sls_order_dt     AS order_date,
      s.sls_ship_dt      AS shipping_date,
      s.sls_due_dt       AS due_date,
      s.sls_sales        AS sales_amount,
      s.sls_quantity     AS quantity,
      s.sls_price        AS price,
      s.dwh_create_date  AS creation_time
FROM silver.crm_sales_details s
JOIN gold.dim_products p
     ON s.sls_prd_key = p.product_number
JOIN gold.dim_customers c
     ON s.sls_cust_id = c.customer_ID;
