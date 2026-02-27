/*
******************************************************************************
DDL Script: Create Silver Tables
******************************************************************************
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables
******************************************************************************
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, 
            @End_time DATETIME, 
            @B_start_time DATETIME,
            @B_end_time DATETIME

    BEGIN TRY
        PRINT '============================='
        PRINT 'Loading Silver Layer'
        PRINT '============================='
        SET @B_start_time = GETDATE()

        PRINT 'Loading CRM Tables'

        SET @start_time = GETDATE();

        PRINT '>> truncating table silver.crm_cust_info '
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> inserting table silver.crm_cust_info '
        INSERT INTO silver.crm_cust_info(
                cst_id,
                cst_key,
                cst_firstname,
                cst_lastname,
                cst_marital_status,
                cst_gndr,
                cst_create_date)
        SELECT 
                cst_id,
                cst_key,
                TRIM(cst_firstname),
                TRIM(cst_lastname),
                CASE 
                    WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
                    WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
                    ELSE 'N/A'
                END,
                CASE 
                    WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
                    WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
                    ELSE 'N/A'
                END,
                cst_create_date
        FROM (
                SELECT  cst_id,
                        cst_key,
                        cst_firstname,
                        cst_lastname,
                        cst_marital_status,
                        cst_gndr,
                        cst_create_date,
                        ROW_NUMBER() OVER(
                            PARTITION BY cst_id 
                            ORDER BY cst_create_date DESC
                        ) AS flag_last
                FROM bronze.crm_cust_info
                WHERE cst_id IS NOT NULL
             ) t
        WHERE flag_last = 1

        SET @End_time = GETDATE();
        PRINT 'Duration of loading: ' + CAST(DATEDIFF(SECOND,@start_time,@End_time) AS NVARCHAR) + ' Seconds'
        PRINT '-----------------------------------------'


        SET @start_time = GETDATE();

        PRINT '>> truncating table silver.crm_prd_info '
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> inserting table silver.crm_prd_info '

        INSERT INTO silver.crm_prd_info(
                prd_id,
                cat_id,
                prd_key,
                prd_nm,
                prd_cost,
                prd_line,
                prd_start_dt,
                prd_end_dt)
        SELECT 
                prd_id,
                REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
                SUBSTRING(prd_key,7,LEN(prd_key)),
                prd_nm,
                ISNULL(prd_cost,0),
                ISNULL(prd_line,'N/A'),
                CAST(prd_start_dt AS DATE),
                DATEADD(
                    DAY,-1,
                    CAST(
                        LEAD(prd_start_dt) OVER(
                            PARTITION BY prd_key 
                            ORDER BY prd_start_dt
                        ) AS DATE
                    )
                )
        FROM bronze.crm_prd_info

        SET @End_time = GETDATE();
        PRINT 'Duration of loading: ' + CAST(DATEDIFF(SECOND,@start_time,@End_time) AS NVARCHAR) + ' Seconds'
        PRINT '-----------------------------------------'


        SET @start_time = GETDATE();

        PRINT '>> truncating table silver.crm_sales_details '
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> inserting table silver.crm_sales_details '

        INSERT INTO silver.crm_sales_details(
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
                CASE 
                    WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
                END,
                CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE),
                CAST(CAST(sls_due_dt AS VARCHAR) AS DATE),
                CASE 
                    WHEN sls_sales IS NULL 
                         OR sls_sales <= 0 
                         OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                    ELSE sls_sales
                END,
                sls_quantity,
                CASE 
                    WHEN sls_price <= 0 OR sls_price IS NULL 
                    THEN sls_sales / NULLIF(sls_quantity,0)
                    ELSE sls_price
                END
        FROM bronze.crm_sales_details

        SET @End_time = GETDATE();
        PRINT 'Duration of loading: ' + CAST(DATEDIFF(SECOND,@start_time,@End_time) AS NVARCHAR) + ' Seconds'
        PRINT '-----------------------------------------'


        SET @start_time = GETDATE();

        PRINT '>> truncating table silver.erp_cust_az12 '
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> inserting table silver.erp_cust_az12 '

        INSERT INTO silver.erp_cust_az12(
                cid,
                bdate,
                gen)
        SELECT 
                CASE 
                    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
                    ELSE cid
                END,
                CASE 
                    WHEN bdate > GETDATE() THEN NULL
                    ELSE bdate
                END,
                CASE 
                    WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                    WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                    ELSE 'N/A'
                END
        FROM bronze.erp_cust_az12

        SET @End_time = GETDATE();
        PRINT 'Duration of loading: ' + CAST(DATEDIFF(SECOND,@start_time,@End_time) AS NVARCHAR) + ' Seconds'
        PRINT '-----------------------------------------'


        SET @start_time = GETDATE();

        PRINT '>> truncating table silver.erp_loc_a101 '
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> inserting table silver.erp_loc_a101 '

        INSERT INTO silver.erp_loc_a101(
                cid,
                cntry)
        SELECT 
                REPLACE(cid,'-',''),
                CASE 
                    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                    WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                    WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'N/A'
                    ELSE TRIM(cntry)
                END
        FROM bronze.erp_loc_a101

        SET @End_time = GETDATE();
        PRINT 'Duration of loading: ' + CAST(DATEDIFF(SECOND,@start_time,@End_time) AS NVARCHAR) + ' Seconds'
        PRINT '-----------------------------------------'


        SET @start_time = GETDATE();

        PRINT '>> truncating table silver.erp_px_cat_g1v2 '
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> inserting table silver.erp_px_cat_g1v2 '

        INSERT INTO silver.erp_px_cat_g1v2(
                id,
                cat,
                subcat,
                maintenance)
        SELECT 
                id,
                TRIM(cat),
                TRIM(subcat),
                maintenance
        FROM bronze.erp_px_cat_g1v2

        SET @End_time = GETDATE();
        PRINT 'Duration of loading: ' + CAST(DATEDIFF(SECOND,@start_time,@End_time) AS NVARCHAR) + ' Seconds'
        PRINT '-----------------------------------------'


        SET @B_end_time = GETDATE()

        PRINT 'Load End Successfully'
        PRINT 'Duration of loading Silver layer: ' 
              + CAST(DATEDIFF(SECOND,@B_start_time,@B_end_time) AS NVARCHAR) 
              + ' Seconds'

    END TRY
    BEGIN CATCH
        PRINT '============================================'
        PRINT 'AN ERROR OCCURED DURING LOADING Silver LAYER'
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================================'
    END CATCH
END
