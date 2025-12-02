/*
==================================================================
DDL Script: Load Bronze Layer
==================================================================
Script Purpose:
        This stored procedure loads data into the 'bronze' schema from external csv files.
        It performs following actions:
                    - Truncate the bronze tables before loading data
                    - Uses the 'BULK INSERT' command to load data from CSV files to bronze tables
Parameters:
        None. This procedure does not accept any parameters ir return any values

Usage Example:
        EXEC bronze.load_bronze;
==================================================================
USE DataWarehouse;
*/


CREATE OR ALTER PROCEDURE BRONZE.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @start_batch_time = GETDATE()
		PRINT '=========================';
		PRINT 'Loading the bronze layer'
		PRINT '=========================';
		PRINT '';
		PRINT '--------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\SQL\SQL_YT\Projects\My_Project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------------';

		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\SQL\SQL_YT\Projects\My_Project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\SQL\SQL_YT\Projects\My_Project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------------';

		PRINT '------------------------';
		PRINT 'Loading the ERP Tables';
		PRINT '------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_prd_info';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		PRINT '>> Inserting Data Into: bronze.CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'D:\SQL\SQL_YT\Projects\My_Project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;
		PRINT '>> Inserting Data Into: bronze.LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'D:\SQL\SQL_YT\Projects\My_Project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		PRINT '>> Inserting Data Into: bronze.PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'D:\SQL\SQL_YT\Projects\My_Project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------------';
		PRINT '================================================================================='
		PRINT 'BRONZE LAYER LOADING COMPLETED'
		SET @batch_end_time = GETDATE();
		PRINT '>> Total Load Duration for Bronze Layer:' + CAST(DATEDIFF(second, @start_batch_time,@batch_end_time) AS NVARCHAR) + 'seconds'
		PRINT '================================================================================='
		END TRY
	BEGIN CATCH
		PRINT '======================================'
		PRINT 'Error Occured During Loading Bronze Layer'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
	END CATCH
END
