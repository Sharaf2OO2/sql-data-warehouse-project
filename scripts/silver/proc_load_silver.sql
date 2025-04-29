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
    CALL silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    -- Start logging
    RAISE NOTICE 'Starting Silver Layer Loading at %', CURRENT_TIMESTAMP;

    -- Load silver.crm_cust_info
    RAISE NOTICE '>> Truncating and loading silver.crm_cust_info';
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info
    SELECT cst_id,
           cst_key,
           TRIM(cst_firstname),
           TRIM(cst_lastname),
           CASE
               WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
               WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
               ELSE 'N/A'
           END AS cst_marital_status,
           CASE
               WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
               WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
               ELSE 'N/A'
           END AS cst_gndr,
           cst_create_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) AS filtered
    WHERE rn = 1;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration for silver.crm_cust_info: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- Load silver.crm_prd_info
    RAISE NOTICE '>> Truncating and loading silver.crm_prd_info';
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info
    SELECT prd_id,
           SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
           REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id,
           prd_nm,
           COALESCE(prd_cost, 0) AS prd_cost,
           CASE UPPER(TRIM(prd_line))
               WHEN 'M' THEN 'Mountain'
               WHEN 'S' THEN 'Other Sales'
               WHEN 'R' THEN 'Road'
               WHEN 'T' THEN 'Touring'
               ELSE 'N/A'
           END AS prd_line,
           prd_start_dt,
           LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt
    FROM bronze.crm_prd_info;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration for silver.crm_prd_info: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- Load silver.crm_sales_details
    RAISE NOTICE '>> Truncating and loading silver.crm_sales_details';
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details
    SELECT sls_ord_num,
           sls_prd_key,
           sls_cust_id,
           CASE
               WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::VARCHAR) != 8 THEN NULL
               ELSE (sls_order_dt::VARCHAR)::DATE
           END AS sls_order_dt,
           CASE
               WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::VARCHAR) != 8 THEN NULL
               ELSE (sls_ship_dt::VARCHAR)::DATE
           END AS sls_ship_dt,
           CASE
               WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::VARCHAR) != 8 THEN NULL
               ELSE (sls_due_dt::VARCHAR)::DATE
           END AS sls_due_dt,
           CASE
               WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * sls_price
               ELSE sls_sales
           END AS sls_sales,
           sls_quantity,
           CASE
               WHEN sls_price = 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
               WHEN sls_price < 0 THEN ABS(sls_price)
               ELSE sls_price
           END AS sls_price
    FROM bronze.crm_sales_details;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration for silver.crm_sales_details: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

       -- Load silver.erp_cust_az12
    RAISE NOTICE '>> Truncating and loading silver.erp_cust_az12';
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid, 4, LENGTH(cid))
            ELSE cid
        END AS cid,
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'N/A'
        END AS gen
    FROM bronze.erp_cust_az12;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration for silver.erp_cust_az12: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- Load silver.erp_loc_a101
    RAISE NOTICE '>> Truncating and loading silver.erp_loc_a101';
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101
    SELECT 
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'N/A'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration for silver.erp_loc_a101: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- Load silver.erp_px_cat_g1v2
    RAISE NOTICE '>> Truncating and loading silver.erp_px_cat_g1v2';
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2
    SELECT *
    FROM bronze.erp_px_cat_g1v2;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration for silver.erp_px_cat_g1v2: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- Completion Log
    RAISE NOTICE 'Silver Layer Loading Completed at %', CURRENT_TIMESTAMP;

EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'An error occurred: %', SQLERRM;
END;
$$;
