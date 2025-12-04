/*
==================================================================
DDL Script: Load Bronze Layer
==================================================================
Script Purpose:
        This stored procedure performs the ETL (Extract Transform Load) process to populate
        the silver schema tables from the bronze schema.
        It performs following actions:
                    - Truncate the silver tables before loading data
                    - Inserts transformed and cleansed data from Bronze into Silver tables 
Parameters:
        None. This procedure does not accept any parameters ir return any values

Usage Example:
        EXEC silver.load_silver;
==================================================================
USE DataWarehouse;
*/



CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @start_batch_time = GETDATE()
		PRINT '=========================';
		PRINT 'Loading the Silver layer'
		PRINT '=========================';
		PRINT '';

		PRINT '--------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Inserting Data into:crm_cust_info';
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
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE UPPER(TRIM(cst_marital_status))
				 WHEN 'S' THEN 'Single'
				 WHEN 'M' THEN 'Married'
				 ELSE 'n/a'
			END cst_marital_status,

			CASE UPPER(TRIM(cst_gndr))
				 WHEN 'F' THEN 'Female'
				 WHEN 'M' THEN 'Male'
				 ELSE 'n/a'
			END cst_gendr,
			cst_create_date
				FROM (SELECT 
					*,
					ROW_NUMBER() OVER (PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC) AS FLAG_LAST
					FROM bronze.crm_cust_info
					WHERE cst_id IS NOT NULL)T
					WHERE FLAG_LAST = 1
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>> Inserting Data into:crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY PRD_KEY ORDER BY PRD_START_DT)-1 AS DATE) AS prd_end_dt
		FROM BRONZE.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data into:crm_sales_details'
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
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(SLS_ORDER_DT AS VARCHAR) AS DATE)
				END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity + ABS(sls_price)
				 THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
				END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
				 THEN sls_quantity * NULLIF(sls_price,0)
				 ELSE sls_price
				END AS sls_price
		FROM BRONZE.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data into:erp_cust_az12'
		INSERT INTO SILVER.erp_cust_az12(cid,bdate,gen)
		SELECT
			CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
				 ELSE CID
			END AS cid,
			CASE WHEN BDATE > GETDATE() THEN NULL
				 ELSE BDATE
			END AS bdate,
			CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
				 ELSE 'n/a'
			END AS gen
		FROM BRONZE.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data into:erp_loc_a101'
		INSERT INTO SILVER.erp_loc_a101
		(cid,cntry)
		SELECT
			REPLACE(CID,'-','') AS cid,
			CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				 WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
				 WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
				 ELSE TRIM(CNTRY)
				END AS cntry
		FROM BRONZE.erp_loc_a101

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data into:erp_px_cat_g1v2'
		INSERT INTO SILVER.erp_px_cat_g1v2
		(id,cat,subcat,maintenance)
		SELECT
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		FROM BRONZE.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------------';

		PRINT '=================================================================================';
		PRINT 'SILVER LAYER LOADING COMPLETED';
		SET @batch_end_time = GETDATE();
		PRINT '>> Total Load Duration for Silver Layer:' + CAST(DATEDIFF(second, @start_batch_time,@batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '=================================================================================';

	END TRY
	BEGIN CATCH
		PRINT '======================================'
		PRINT 'Error Occured During Loading Bronze Layer'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '======================================'
	END CATCH
END;
