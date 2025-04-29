/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    -- Start logging
    RAISE NOTICE 'Starting Bronze Layer Loading at %', CURRENT_TIMESTAMP;

    -- Load bronze.crm_cust_info
    RAISE NOTICE '>> Truncating and loading bronze.crm_cust_info';
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM '/datasets/source_crm/cust_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration for bronze.crm_cust_info: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- Load bronze.crm_prd_info
    RAISE NOTICE '>> Truncating and loading bronze.crm_prd_info';
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/datasets/source_crm/prd_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration for bronze.crm_prd_info: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- Load bronze.crm_sales_details
    RAISE NOTICE '>> Truncating and loading bronze.crm_sales_details';
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/datasets/source_crm/sales_details.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration for bronze.crm_sales_details: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- Load bronze.erp_cust_az12
    RAISE NOTICE '>> Truncating and loading bronze.erp_cust_az12';
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/datasets/source_erp/CUST_AZ12.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration for bronze.erp_cust_az12: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- Load bronze.erp_loc_a101
    RAISE NOTICE '>> Truncating and loading bronze.erp_loc_a101';
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/datasets/source_erp/LOC_A101.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration for bronze.erp_loc_a101: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- Load bronze.erp_px_cat_g1v2
    RAISE NOTICE '>> Truncating and loading bronze.erp_px_cat_g1v2';
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration for bronze.erp_px_cat_g1v2: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- Completion Log
    RAISE NOTICE 'Bronze Layer Loading Completed at %', CURRENT_TIMESTAMP;

EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'An error occurred: %', SQLERRM;
END;
$$;
