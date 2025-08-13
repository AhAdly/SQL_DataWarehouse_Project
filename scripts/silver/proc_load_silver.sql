/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @sp_start_time DATETIME, @sp_end_time DATETIME;
    SET @sp_start_time = GETDATE();

    BEGIN TRY

        PRINT '=====================================================================================';
        PRINT 'Loading Silver Layer';
        PRINT '=====================================================================================';

        PRINT '-------------------------------------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-------------------------------------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Insering Data Into: silver.crm_cust_info';
        INSERT into silver.crm_cust_info (
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
            TRIM(cst_firstname) cst_firstname,
            TRIM(cst_lastname) cst_lastname,
            case UPPER(TRIM(cst_marital_status))
                when 'S' then 'Single'
                when 'M' then 'Married'
                else 'n/a'
            end as cst_marital_status,
            case UPPER(TRIM(cst_gndr))
                when 'F' then 'Female'
                when 'M' then 'Male'
                else 'n/a'
            end as cst_gndr,
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date Desc) duplicate_flag
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        )t WHERE duplicate_flag = 1

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Insering Data Into: silver.crm_prd_info';
        INSERT into silver.crm_prd_info (
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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_' ) AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) as prd_cost,
            CASE UPPER(TRIM(prd_line))
                when 'M' then 'Mountain'
                when 'R' then 'Road'
                when 'T' then 'Touring'
                when 'S' then 'Other Sales'
                else 'n/a'
            END AS prd_line,
            CAST(prd_start_dt as date) as prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER (partition by prd_key order by prd_start_dt) -1 as date) AS prd_end_dt
        FROM bronze.crm_prd_info

        PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Insering Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            
            CASE WHEN  sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt as varchar) as date)
            END as sls_order_dt,

            CASE WHEN  sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt as varchar) as date)
            END as sls_ship_dt,

            CASE WHEN  sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt as varchar) as date)
            END as sls_due_dt,

            CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END as sls_sales,

            sls_quantity,

            CASE  WHEN sls_price <= 0 OR sls_price IS NULL
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details

        PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '-------------------------------------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-------------------------------------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Insering Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,

            CASE WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,

            CASE 
                WHEN UPPER(TRIM(TRANSLATE(gen, CHAR(13)+CHAR(10)+CHAR(9)+CHAR(160), '    '))) IN ('M', 'MALE') THEN 'Male'
                WHEN UPPER(TRIM(TRANSLATE(gen, CHAR(13)+CHAR(10)+CHAR(9)+CHAR(160), '    '))) IN ('F', 'FEMALE') THEN 'Female'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12

        PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Insering Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid,'-', '') AS cid,
            CASE 
                WHEN UPPER(TRIM(TRANSLATE(cntry, CHAR(13)+CHAR(10)+CHAR(9)+CHAR(160), '    '))) IN ('USA', 'US') THEN 'United States'
                WHEN UPPER(TRIM(TRANSLATE(cntry, CHAR(13)+CHAR(10)+CHAR(9)+CHAR(160), '    '))) = 'DE' THEN 'Germany'
                WHEN UPPER(TRIM(TRANSLATE(cntry, CHAR(13)+CHAR(10)+CHAR(9)+CHAR(160), '    '))) = '' THEN 'n/a'
                ELSE TRIM(TRANSLATE(cntry, CHAR(13)+CHAR(10)+CHAR(9)+CHAR(160), '    '))
            END AS cntry
        FROM bronze.erp_loc_a101

        PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Insering Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            CASE 
                WHEN UPPER(TRIM(TRANSLATE(maintenance, CHAR(13)+CHAR(10)+CHAR(9)+CHAR(160), '    '))) = 'YES' THEN 'Yes'
                WHEN UPPER(TRIM(TRANSLATE(maintenance, CHAR(13)+CHAR(10)+CHAR(9)+CHAR(160), '    '))) = 'NO' THEN 'No'
                ELSE 'n/a'
            END AS maintenance
        FROM bronze.erp_px_cat_g1v2

        PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------';

    END TRY
    BEGIN CATCH
        PRINT '=====================================================================================';
        PRINT 'ERROR OCCURED DURING Loading Bronze Layer';
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=====================================================================================';
    END CATCH

    SET @sp_end_time = GETDATE();
    PRINT '>> Stored Procedure Load Duration: ' + CAST (DATEDIFF(SECOND, @sp_start_time, @sp_end_time) AS NVARCHAR) + ' seconds';
    PRINT '--------------------------------------------------';

END
