/*
==============================================================
Quality Checks
==============================================================
Script purpose:
	This script performs various quality checks for data consistency, accuracy,
	and standardization accross the 'silver' schema. It includes checks for:
	-	Null or duplicate primary key.
	-	Unwanted spaces in string fields.
	-	Data standardization in string fields.
	-	Invalid date ranges and orders.
	-	Data consistency between related fields

Usage Notes:
	-	Run these checks after data loading silver layer.
	-	Investigate and resolve any discrepancies found during the checks
==============================================================

*/

		-- Check For Nulls or Duplicates in Primary Key
		-- Expectation: No Result


		SELECT
		cst_id,
		COUNT(*)
		FROM silver.crm_cust_info
		GROUP BY cst_id
		HAVING COUNT(*) > 1 OR cst_id IS NULL;

		-- Check for unwanted Spaces
		-- Expectation: No Results
		SELECT cst_key
		FROM silver.crm_cust_info
		WHERE cst_key != TRIM(cst_key);

		SELECT cst_lastname
		FROM silver.crm_cust_info
		WHERE cst_lastname != TRIM(cst_lastname);


		-- Data Standardization & Consistency
		SELECT DISTINCT cst_gndr
		FROM silver.crm_cust_info;

		SELECT DISTINCT prd_line
		FROM bronze.crm_prd_info;


		-- Check for NULLS or Negative Numbers
		-- Expectation: No results
		SELECT prd_cost
		FROM bronze.crm_prd_info
		WHERE prd_cost < 0 OR prd_cost IS NULL;

		-- Check for Invalid Date Orders
		SELECT *
		FROM bronze.crm_prd_info
		WHERE prd_end_dt < prd_start_dt;



------------------------------------------------------------------------------------------------------

	-- Quality Checks
		-- Check for Nulls or Duplicates in Primary Key
		-- Expectation: No Result
		SELECT
		prd_id,
		COUNT(*)
		FROM silver.crm_prd_info
		GROUP BY prd_id
		HAVING COUNT(*) > 1 OR prd_id IS NULL;

		-- Check for unwanted spaces
		-- Expectation: No Results
		SELECT prd_nm
		FROM silver.crm_prd_info
		WHERE prd_nm != TRIM(prd_nm);

		-- Check for NULLS or Negative Numbers
		-- Expectation: No Results
		SELECT prd_cost
		FROM silver.crm_prd_info
		WHERE prd_cost < 0 OR prd_cost IS NULL;

		-- Data Standardization & Consistency
		SELECT DISTINCT prd_line
		FROM silver.crm_prd_info;

		-- Check for Invalid Date Orders
		SELECT *
		FROM silver.crm_prd_info
		WHERE prd_end_dt < prd_start_dt;

		SELECT *
		FROM silver.crm_prd_info;


------------------------------------------------------------------------------------------------------

	-- Check for Invalid Dates
		SELECT
		NULLIF(sls_order_dt,0) sls_order_dt
		FROM bronze.crm_sales_details
		WHERE sls_order_dt <= 0
		OR LEN(sls_order_dt) != 8
		OR sls_order_dt > 20500101
		OR sls_order_dt < 19000101;

		-- Check for Invalid Date Orders
		SELECT
		*
		FROM silver.crm_sales_details
		WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

		-- Check Data Consistency: Between Sales, Quantity, and Price
		-- >> Sales = Quantity * Price
		-- >> Values must not be NULL, zero, or negative

		SELECT DISTINCT
		sls_sales AS old_sls_sales,
		sls_quantity,
		sls_price AS old_sls_price
		FROM silver.crm_sales_details
		WHERE sls_sales != sls_quantity * sls_price
		OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
		OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
		ORDER BY sls_sales, sls_quantity, sls_price;

		SELECT * FROM silver.crm_sales_details;

------------------------------------------------------------------------------------------------------

-- Identify Out-of-Range Dates

		SELECT DISTINCT 
		bdate
		FROM silver.erp_cust_az12
		WHERE bdate < '1924-01-01' OR bdate > GETDATE();


		-- Data Standardization & Consistency
		SELECT DISTINCT 
		gen
		FROM silver.erp_cust_az12;

		SELECT * FROM silver.erp_cust_az12;

------------------------------------------------------------------------------------------------------

	-- Data Standardization & Consistency
		SELECT DISTINCT 
		cntry
		FROM silver.erp_loc_a101
		ORDER BY cntry;

		SELECT * FROM silver.erp_loc_a101;

------------------------------------------------------------------------------------------------------

-- Check for unwanted Spaces
		SELECT * FROM silver.erp_px_cat_g1v2
		WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance) 

		-- Data Standardization & Consistency
		SELECT DISTINCT
		maintenance
		FROM silver.erp_px_cat_g1v2

		SELECT * FROM silver.erp_px_cat_g1v2
