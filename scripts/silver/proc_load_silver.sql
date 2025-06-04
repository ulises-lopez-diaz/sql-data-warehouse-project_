/*

============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
============================================================================
Script Purpose:
	This stored procedure performs the ETL (Extract, Transform, Load) process to
	populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
	- Truncates Silver tables.
	- Inserts transformed and cleansed data from Bronze into Silver tables.

Paramaters:
	None.
	This sotred procedure does not accept any parameters of return any values.

Usage Example:
	EXEC silver.load_silver;
============================================================================



*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '======================================================';
		PRINT 'Loading Silver Layer';
		PRINT '======================================================';

		PRINT '-------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------';

		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
		t.cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastame,
		CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END cst_material_status, -- Normalize material status values to readable format
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr, -- Normalize gender values to readable format
		cst_create_date
		FROM 
		(
			SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t 
		WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------';


		---- Check For Nulls or Duplicates in Primary Key
		---- Expectation: No Result


		--SELECT
		--cst_id,
		--COUNT(*)
		--FROM silver.crm_cust_info
		--GROUP BY cst_id
		--HAVING COUNT(*) > 1 OR cst_id IS NULL;

		---- Check for unwanted Spaces
		---- Expectation: No Results
		--SELECT cst_key
		--FROM silver.crm_cust_info
		--WHERE cst_key != TRIM(cst_key);

		--SELECT cst_lastname
		--FROM silver.crm_cust_info
		--WHERE cst_lastname != TRIM(cst_lastname);


		---- Data Standardization & Consistency
		--SELECT DISTINCT cst_gndr
		--FROM silver.crm_cust_info;

		--SELECT DISTINCT prd_line
		--FROM bronze.crm_prd_info;


		---- Check for NULLS or Negative Numbers
		---- Expectation: No results
		--SELECT prd_cost
		--FROM bronze.crm_prd_info
		--WHERE prd_cost < 0 OR prd_cost IS NULL;

		---- Check for Invalid Date Orders
		--SELECT *
		--FROM bronze.crm_prd_info
		--WHERE prd_end_dt < prd_start_dt;

		-----------------------------------------------------------------------------


		-- Loading silver.crm_prd_info
		--SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
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
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,		   -- Extract product key
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line, -- Map product line codes to descriptive values
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
				AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;


		---- Quality Checks
		---- Check for Nulls or Duplicates in Primary Key
		---- Expectation: No Result
		--SELECT
		--prd_id,
		--COUNT(*)
		--FROM silver.crm_prd_info
		--GROUP BY prd_id
		--HAVING COUNT(*) > 1 OR prd_id IS NULL;

		---- Check for unwanted spaces
		---- Expectation: No Results
		--SELECT prd_nm
		--FROM silver.crm_prd_info
		--WHERE prd_nm != TRIM(prd_nm);

		---- Check for NULLS or Negative Numbers
		---- Expectation: No Results
		--SELECT prd_cost
		--FROM silver.crm_prd_info
		--WHERE prd_cost < 0 OR prd_cost IS NULL;

		---- Data Standardization & Consistency
		--SELECT DISTINCT prd_line
		--FROM silver.crm_prd_info;

		---- Check for Invalid Date Orders
		--SELECT *
		--FROM silver.crm_prd_info
		--WHERE prd_end_dt < prd_start_dt;

		--SELECT *
		--FROM silver.crm_prd_info;



		------------------------------------------------

		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';

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
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END as sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END as sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END as sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price -- Derive price if original value is invalid
		END AS sls_price
		FROM bronze.crm_sales_details;


		---- Check for Invalid Dates
		--SELECT
		--NULLIF(sls_order_dt,0) sls_order_dt
		--FROM bronze.crm_sales_details
		--WHERE sls_order_dt <= 0
		--OR LEN(sls_order_dt) != 8
		--OR sls_order_dt > 20500101
		--OR sls_order_dt < 19000101;

		---- Check for Invalid Date Orders
		--SELECT
		--*
		--FROM silver.crm_sales_details
		--WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

		---- Check Data Consistency: Between Sales, Quantity, and Price
		---- >> Sales = Quantity * Price
		---- >> Values must not be NULL, zero, or negative

		--SELECT DISTINCT
		--sls_sales AS old_sls_sales,
		--sls_quantity,
		--sls_price AS old_sls_price
		--FROM silver.crm_sales_details
		--WHERE sls_sales != sls_quantity * sls_price
		--OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
		--OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
		--ORDER BY sls_sales, sls_quantity, sls_price;

		--SELECT * FROM silver.crm_sales_details;

		---------------------------------------------------------------------------

		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';

		INSERT INTO silver.erp_cust_az12(
			cid, 
			bdate, 
			gen
		)
		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
			ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate, -- Set future birthdates to NULL
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen -- Normalize gender values and handl unknown cases
		FROM bronze.erp_cust_az12;



		--SELECT * FROM silver.crm_cust_info;

		---- Identify Out-of-Range Dates

		--SELECT DISTINCT 
		--bdate
		--FROM silver.erp_cust_az12
		--WHERE bdate < '1924-01-01' OR bdate > GETDATE();


		---- Data Standardization & Consistency
		--SELECT DISTINCT 
		--gen
		--FROM silver.erp_cust_az12;

		--SELECT * FROM silver.erp_cust_az12;


		-------------------------------------------------------------
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';

		INSERT INTO silver.erp_loc_a101
		(cid,cntry)
		SELECT
		REPLACE(cid, '-', '') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;






		---- Data Standardization & Consistency
		--SELECT DISTINCT 
		--cntry
		--FROM silver.erp_loc_a101
		--ORDER BY cntry;

		--SELECT * FROM silver.erp_loc_a101;






		--------------------------------------------------------------------------


		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

		INSERT INTO silver.erp_px_cat_g1v2(
		id,cat,subcat,maintenance
		)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;






		---- Check for unwanted Spaces
		--SELECT * FROM silver.erp_px_cat_g1v2
		--WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance) 

		---- Data Standardization & Consistency
		--SELECT DISTINCT
		--maintenance
		--FROM silver.erp_px_cat_g1v2

		--SELECT * FROM silver.erp_px_cat_g1v2

		SET @batch_end_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT '	- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==============================================';

		END TRY
		BEGIN CATCH
			PRINT '==============================================';
			PRINT 'ERROR OCURRED DURING LOADING BRONZE LAYER';
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '==============================================';
		END CATCH


END
