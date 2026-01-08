-- ************************* 1st Table in CRM *****************************
-- crm_cust_info




-- Check for NULLs Or Duplicates In Primary Key
-- Expectation : No Result
SELECT
cst_id,
COUNT (*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for Unwanted Spaces
-- Expectation : No Results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- Data Standarzation & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info





-- ************************* 2nd Table in CRM *****************************
-- crm_prd_info Table





SELECT * FROM bronze.crm_prd_info

-- Check for NULLs Or Duplicates In Primary Key
-- Expectation : No Result
SELECT
prd_id,
COUNT (*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for Unwanted Spaces
-- Expectation : No Result
select prd_nm 
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- Check for NULLs or Nigative Numbers 
-- Expectation : No Result
select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost IS NULL

-- Data Standarzation & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date Orders
select * from silver.crm_prd_info
where prd_end_dt < prd_start_dt



SELECT 
*
FROM silver.crm_prd_info



-- ************************* 3rd Table in CRM *****************************
-- crm_sales_details


-- Check for Invalid Dates
select * from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt




-- ///////////////////////////////////////////////////////////////////////////////////////



-- ************************* 1st Table in ERP *****************************
-- erp_cust_az12



-- Identify Out-of-Range Dates
select distinct 
bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()



-- Date Standarzatio & Consistency
select distinct 
gen
from silver.erp_cust_az12



select * from silver.erp_cust_az12 




-- ************************* 2nd Table in ERP *****************************
-- erp_loc_a101




-- Data Standarzation & Consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
order by cntry

select * from silver.erp_loc_a101





-- ************************* 3rd Table in ERP *****************************
-- erp_px_cat_g1v2


select 
id , 
cat, 
subcat, 
maintenance
from bronze.erp_px_cat_g1v2




-- Checkink for Unwanted Spaces 
select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance) 



-- Data Standarzation & Consistency
SELECT DISTINCT cat
from bronze.erp_px_cat_g1v2
order by cat


-- Data Standarzation & Consistency
SELECT DISTINCT subcat
from bronze.erp_px_cat_g1v2
order by subcat

-- Data Standarzation & Consistency
SELECT DISTINCT maintenance
from bronze.erp_px_cat_g1v2
order by maintenance
