/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
create or replace view gold.dim_customers as
select
	row_number() over(order by cst_id) customer_key,
	cci.cst_id customer_id,
	cci.cst_key customer_number,
	cci.cst_firstname first_name,
	cci.cst_lastname last_name,
	ela.cntry country,
	case
		when cci.cst_gndr <> 'N/A' then cci.cst_gndr
		else eca.gen
	end gender,
	cci.cst_marital_status marital_status,
	eca.bdate birth_date,
	cci.cst_create_date create_date
from silver.crm_cust_info cci 
left join silver.erp_loc_a101 ela on cci.cst_key = ela.cid
left join silver.erp_cust_az12 eca on cci.cst_key = eca.cid;

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
create or replace view gold.dim_products as
select 
	row_number() over(order by cpi.prd_start_dt, cpi.prd_key) product_key,
	cpi.prd_id product_id,
	cpi.prd_key product_number,
	cpi.prd_nm product_name,
	cpi.cat_id category_id,
	epcgv.cat category,
	epcgv.subcat subcategory,	
	epcgv.maintenance,
	cpi.prd_cost cost,
	cpi.prd_line procduct_line,
	cpi.prd_start_dt start_date
from silver.crm_prd_info cpi
left join silver.erp_px_cat_g1v2 epcgv on cpi.cat_id = epcgv.id
where cpi.prd_end_dt is null; -- filter out all historical data

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
create or replace view gold.fact_sales as
select 
	csd.sls_ord_num order_number,
	dp.product_key,
	dc.customer_key,
	csd.sls_order_dt order_date,
	csd.sls_ship_dt shipping_date,
	csd.sls_due_dt due_date,
	csd.sls_sales sales_amount,
	csd.sls_quantity quantity,
	csd.sls_price price
from silver.crm_sales_details csd 
left join gold.dim_customers dc on csd.sls_cust_id = dc.customer_id
left join gold.dim_products dp on csd.sls_prd_key = dp.product_number;
