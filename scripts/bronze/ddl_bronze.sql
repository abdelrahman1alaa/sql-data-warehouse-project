/*
DDL Script: Create Bronze Tables
Script Purpose:
This script creates tables in the 'bronze' schema, dropping existing tables if they already exist.
Run this script to re-define the DDL structure of 'bronze' Tables
*/

---


IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;



CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR (50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);

IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
sls_ord_num NVARCHAR (50),
sls_prd_key NVARCHAR (50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;


CREATE TABLE bronze.erp_loc_a101 (
cid NVARCHAR(50),
cntry NVARCHAR(50)
);


IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;


CREATE TABLE bronze.erp_cust_az12 ( 
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)

);


IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;


CREATE TABLE bronze.erp_px_cat_g1v2 (
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);




PRINT'****************************************************************************'




EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time AS DATETIME , @end_time AS DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT'***************************************';
		PRINT 'LOADING BRONZE LAYER';
		PRINT'***************************************';

		PRINT'----------------------------------';
		PRINT'LOADING CRM TABLES';
		PRINT'----------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING TABLE : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT'>> INSERTING DATA INTO : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\Data Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
		FIRSTROW = 2 ,
		FIELDTERMINATOR =',',
		TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' +	CAST(DATEDIFF(second , @start_time , @end_time) AS NVARCHAR)+ ' Seconds';
		PRINT'--------------';

		PRINT'>> TRUNCATING TABLE : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT'>> INSERTING DATA INTO : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\Data Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
		FIRSTROW = 2 ,
		FIELDTERMINATOR =',',
		TABLOCK 
		);

		PRINT'>> TRUNCATING TABLE : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT'>> INSERTING DATA INTO : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\Data Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
		FIRSTROW = 2 ,
		FIELDTERMINATOR =',',
		TABLOCK 
		);


		PRINT'----------------------------------';
		PRINT'LOADING ERP TABLES';
		PRINT'----------------------------------';


		PRINT'>> TRUNCATING TABLE : bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;

		PRINT'>> INSERTING DATA INTO : bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'E:\Data Warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
		FIRSTROW = 2 ,
		FIELDTERMINATOR =',',
		TABLOCK 
		);


		PRINT'>> TRUNCATING TABLE : bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;

		PRINT'>> INSERTING DATA INTO : bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'E:\Data Warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
		FIRSTROW = 2 ,
		FIELDTERMINATOR =',',
		TABLOCK 
		);

		PRINT'>> TRUNCATING TABLE : bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

		PRINT'>> INSERTING DATA INTO : bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'E:\Data Warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		FIRSTROW = 2 ,
		FIELDTERMINATOR =',',
		TABLOCK 
		);

		SET @end_time =	GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second,@start_time , @end_time) AS NVARCHAR) + 'seconds';
		PRINT'>> -------------------';

		SET @batch_end_time = GETDATE();
		PRINT'====================================';
		PRINT' Loading Bronze Layer is Completed';
		PRINT'  - Total Load Duration : '+ CAST(DATEDIFF(SECOND , @batch_start_time , @batch_end_time ) AS NVARCHAR) + 'seconds';
		PRINT'====================================';



	END TRY
	BEGIN CATCH
		PRINT'*******************************************';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'*******************************************';

	END CATCH
END
