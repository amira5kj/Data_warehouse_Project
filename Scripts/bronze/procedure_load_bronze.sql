/*
======================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
======================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

======================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DEClARE @start_time DATETIME, @End_time DATETIME ,@B_start_time DATETIME,@B_end_time DATETIME;
    BEGIN TRY
        PRINT '============================='
        PRINT 'Loading Bronze Layer'
        PRINT '============================='
        SET @B_start_time=GETDATE();

        PRINT 'Laoding CRM Tables'

        SET @start_time=GETDATE();
        PRINT 'Truncatnig Table: bronze.crm_cust_info'
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT 'InsERting Table: bronze.crm_cust_info'
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\1-data analysis\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
        WITH (
                FIRSTROW=2,
                FIELDTERMINATOR=',',
                TABLOCK
        );
        SET @End_time=GETDATE();
        PRINT'Duration of laoding:'+ CAST(DATEDIFF(second,@Start_time,@End_time) AS NVARCHAR)+' Seconds'
        PRINT'-----------------------------------------'
        
        SET @start_time=GETDATE();
        PRINT 'Truncatnig Table: bronze.crm_prd_info'
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT 'InsERting Table: bronze.crm_prd_info'
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\1-data analysis\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
        WITH (
                FIRSTROW=2,
                FIELDTERMINATOR=',',
                TABLOCK
        );
        SET @End_time=GETDATE();
        PRINT'Duration of laoding:'+ CAST(DATEDIFF(second,@Start_time,@End_time) AS NVARCHAR)+' Seconds'
        PRINT'-----------------------------------------'
       

        SET @start_time=GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'D:\1-data analysis\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
        WITH (
                FIRSTROW=2,
                FIELDTERMINATOR=',',
                TABLOCK
        );
        SET @End_time=GETDATE();
        PRINT'Duration of laoding:'+ CAST(DATEDIFF(second,@Start_time,@End_time) AS NVARCHAR)+' Seconds'
        PRINT'-----------------------------------------'
        SELECT * FROM bronze.crm_sales_details;

        PRINT '-------------------------------'
        PRINT 'Laoding ERP Tables'
        PRINT '-------------------------------'

        SET @start_time=GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\1-data analysis\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
        WITH (
                FIRSTROW=2,
                FIELDTERMINATOR=',',
                TABLOCK
        );
        SET @End_time=GETDATE();
        PRINT'Duration of laoding:'+ CAST(DATEDIFF(second,@Start_time,@End_time) AS NVARCHAR)+' Seconds'
        PRINT'-----------------------------------------'

        SET @start_time=GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12 ;

        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\1-data analysis\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
        WITH (
                FIRSTROW=2,
                FIELDTERMINATOR=',',
                TABLOCK
        );
        SET @End_time=GETDATE();
        PRINT'Duration of laoding:'+ CAST(DATEDIFF(second,@Start_time,@End_time) AS NVARCHAR)+' Seconds'
        PRINT'-----------------------------------------'

        SET @start_time=GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\1-data analysis\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
                FIRSTROW=2,
                FIELDTERMINATOR=',',
                TABLOCK
        );
        SET @End_time=GETDATE();
        PRINT'Duration of laoding:'+ CAST(DATEDIFF(second,@Start_time,@End_time) AS NVARCHAR)+' Seconds'
        PRINT'-----------------------------------------'

         SET @B_end_time=GETDATE()
        PRINT 'Load End Successfully'
        PRINT 'Duration of loading bronze layer:' +CAST(DATEDIFF(second,@B_start_time,@B_end_time) AS NVARCHAR )+' Seconds' --measuring the speed of loading the data
    END TRY
    BEGIN CATCH
        PRINT '============================================' 
        PRINT 'AN ERROR OCCUREd DURING LOADING BRoNZE LAYER'
        PRINT 'Error Message'+ERROR_MESSAGE();
        PRINT 'Error NUmBER'+CAST(ERROR_NUMBER()AS NVARCHAR );
        PRINT 'Error NUmBER'+CAST(ERROR_STATE()AS NVARCHAR );
        PRINT '============================================' 
    END CATCH 
END
