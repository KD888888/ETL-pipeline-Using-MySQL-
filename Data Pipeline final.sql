-- ==================================================
-- STEP 0: Safety / environment
-- Run as a user with privileges to CREATE DATABASE/PROCEDURES/TABLES.
-- ==================================================

-- ==================================================
-- STEP 1: Create Databases (preserve original naming)
-- ==================================================
CREATE DATABASE IF NOT EXISTS Source_DB;
CREATE DATABASE IF NOT EXISTS Staging_DB;
CREATE DATABASE IF NOT EXISTS ODS_DB;
CREATE DATABASE IF NOT EXISTS DWH_DB;
CREATE DATABASE IF NOT EXISTS DM_DB;          -- Data Marts DB (Finance, Marketing, HR)
CREATE DATABASE IF NOT EXISTS CONTROL_DB;     -- Optional: housekeeping (etl_control_log lives here)

-- ==================================================
-- STEP 2: SOURCE (raw) tables in Source_DB
-- (these match your original layout; values intentionally VARCHAR for raw)
-- ==================================================
USE Source_DB;

CREATE TABLE IF NOT EXISTS raw_customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age VARCHAR(20),
    gender VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    registration_date VARCHAR(50),
    annual_income VARCHAR(50),
    total_orders VARCHAR(50),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    category VARCHAR(100),
    brand VARCHAR(100),
    price VARCHAR(50),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_transactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity VARCHAR(50),
    unit_price VARCHAR(50),
    transaction_date VARCHAR(100),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_suppliers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    supplier_id VARCHAR(50),
    supplier_name VARCHAR(200),
    email VARCHAR(200),
    city VARCHAR(100),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==================================================
-- STEP 3: STAGING tables in Staging_DB
-- (use "staging_" prefix, as you requested)
-- ==================================================
USE Staging_DB;

CREATE TABLE IF NOT EXISTS staging_customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age VARCHAR(20),
    gender VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    registration_date VARCHAR(50),
    annual_income VARCHAR(50),
    total_orders VARCHAR(50),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    load_timestamp DATETIME DEFAULT NOW(),
    data_source VARCHAR(50) DEFAULT 'SOURCE_SYSTEM'
);

CREATE TABLE IF NOT EXISTS staging_products LIKE Source_DB.raw_products;
ALTER TABLE staging_products ADD COLUMN load_timestamp DATETIME DEFAULT NOW();
ALTER TABLE staging_products ADD COLUMN data_source VARCHAR(50) DEFAULT 'SOURCE_SYSTEM';

CREATE TABLE IF NOT EXISTS staging_transactions LIKE Source_DB.raw_transactions;
ALTER TABLE staging_transactions ADD COLUMN load_timestamp DATETIME DEFAULT NOW();
ALTER TABLE staging_transactions ADD COLUMN data_source VARCHAR(50) DEFAULT 'SOURCE_SYSTEM';

CREATE TABLE IF NOT EXISTS staging_suppliers LIKE Source_DB.raw_suppliers;
ALTER TABLE staging_suppliers ADD COLUMN load_timestamp DATETIME DEFAULT NOW();
ALTER TABLE staging_suppliers ADD COLUMN data_source VARCHAR(50) DEFAULT 'SOURCE_SYSTEM';

-- ==================================================
-- STEP 4: ODS tables in ODS_DB (cleaned / typed)
-- ==================================================
USE ODS_DB;

DROP TABLE IF EXISTS ods_customers;
CREATE TABLE ods_customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id TEXT,
    first_name TEXT,
    last_name TEXT,
    age TEXT,
    gender TEXT,
    city TEXT,
    state TEXT,
    registration_date TEXT,
    annual_income TEXT,
    total_orders TEXT,
    data_quality_score TEXT,
    cleaning_status VARCHAR(50) DEFAULT 'PENDING',
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS ods_products;
CREATE TABLE ods_products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id TEXT,
    product_name TEXT,
    category TEXT,
    brand TEXT,
    price DECIMAL(10,2),
    data_quality_score TEXT,
    cleaning_status VARCHAR(50) DEFAULT 'PENDING',
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS ods_transactions;
CREATE TABLE ods_transactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id TEXT,
    customer_id TEXT,
    product_id TEXT,
    quantity INT,
    unit_price DECIMAL(10,2),
    transaction_date TEXT,
    data_quality_score TEXT,
    cleaning_status VARCHAR(50) DEFAULT 'PENDING',
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS ods_suppliers;
CREATE TABLE ods_suppliers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    supplier_id TEXT,
    supplier_name TEXT,
    email TEXT,
    city TEXT,
    data_quality_score TEXT,
    cleaning_status VARCHAR(50) DEFAULT 'PENDING',
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==================================================
-- STEP 5: DWH layer (simple fact/dim placeholders)
-- (keeps it simple: you can expand to star schemas as needed)
-- ==================================================
USE DWH_DB;

-- dims
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INT,
    gender VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    category VARCHAR(100),
    brand VARCHAR(100)
);

-- fact sales (from transactions)
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_key INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(50),
    customer_key INT,
    product_key INT,
    quantity INT,
    unit_price DECIMAL(12,2),
    transaction_date DATE,
    amount DECIMAL(14,2)
);

-- ==================================================
-- STEP 6: Data Marts in DM_DB (Finance, Marketing, HR)
-- ==================================================
USE DM_DB;

-- Finance mart (sales + revenue)
CREATE TABLE IF NOT EXISTS dm_finance_sales AS
SELECT * FROM DWH_DB.fact_sales WHERE 1=0; -- empty structure

-- Marketing mart (customer segmentation + product popularity)
CREATE TABLE IF NOT EXISTS dm_marketing_customers AS
SELECT * FROM DWH_DB.dim_customer WHERE 1=0;

-- HR mart (supplier/vendor view)
CREATE TABLE IF NOT EXISTS dm_hr_suppliers AS
SELECT * FROM ODS_DB.ods_suppliers WHERE 1=0;

-- ==================================================
-- STEP 7: CONTROL tables (in CONTROL_DB or Staging_DB)
-- We'll create them in Staging_DB for simplicity (you can move them)
-- ==================================================
USE Staging_DB;

CREATE TABLE IF NOT EXISTS etl_control_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    process_name VARCHAR(100),
    status VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP NULL,
    records_processed INT DEFAULT 0,
    error_message TEXT NULL,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS data_quality_audit;
CREATE TABLE data_quality_audit (
    id INT AUTO_INCREMENT PRIMARY KEY,
    audit_run_id VARCHAR(50),
    table_name VARCHAR(100),
    record_id VARCHAR(100),
    field_name VARCHAR(100),
    issue_type VARCHAR(100),
    original_value TEXT,
    cleaned_value TEXT,
    severity_level ENUM('LOW','MEDIUM','HIGH','CRITICAL'),
    audit_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    process_name VARCHAR(100),
    INDEX idx_audit_run (audit_run_id),
    INDEX idx_table_field (table_name, field_name),
    INDEX idx_issue_type (issue_type)
);

DROP TABLE IF EXISTS data_quality_summary;
CREATE TABLE data_quality_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    audit_run_id VARCHAR(50),
    table_name VARCHAR(100),
    total_records INT,
    clean_records INT,
    issues_found INT,
    critical_issues INT,
    high_issues INT,
    medium_issues INT,
    low_issues INT,
    data_quality_percentage DECIMAL(5,2),
    audit_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_audit_run (audit_run_id),
    INDEX idx_table (table_name)
);

-- ==================================================
-- STEP 8: ETL procedures (Source -> Staging -> ODS)
--   - SourceToStaging
--   - StagingToODS
--   - PopulateDataQualityAudit (all tables)
--   - BuildAuditSummary
--   - DWH load and DM load procedures (simple)
-- ==================================================

-- 1) Source -> Staging
USE Staging_DB;
DROP PROCEDURE IF EXISTS SourceToStaging;
DELIMITER $$
CREATE PROCEDURE SourceToStaging()
BEGIN
  DECLARE v_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
  INSERT INTO etl_control_log(process_name,status,start_time) VALUES ('SOURCE_TO_STAGING','RUNNING',v_start);

  START TRANSACTION;
    TRUNCATE TABLE staging_customers;
    INSERT INTO staging_customers (customer_id, first_name, last_name, age, gender, city, state, registration_date, annual_income, total_orders, created_timestamp, load_timestamp, data_source)
    SELECT TRIM(customer_id), TRIM(first_name), TRIM(last_name), age, gender, city, state, registration_date, annual_income, total_orders, created_timestamp, NOW(), 'SOURCE_SYSTEM'
    FROM Source_DB.raw_customers
    WHERE customer_id IS NOT NULL AND customer_id != '';

    TRUNCATE TABLE staging_products;
    INSERT INTO staging_products (product_id, product_name, category, brand, price, created_timestamp, load_timestamp, data_source)
    SELECT TRIM(product_id), TRIM(product_name), category, brand, price, created_timestamp, NOW(), 'SOURCE_SYSTEM'
    FROM Source_DB.raw_products
    WHERE product_id IS NOT NULL AND product_id != '';

    TRUNCATE TABLE staging_transactions;
    INSERT INTO staging_transactions (transaction_id, customer_id, product_id, quantity, unit_price, transaction_date, created_timestamp, load_timestamp, data_source)
    SELECT TRIM(transaction_id), TRIM(customer_id), TRIM(product_id), quantity, unit_price, transaction_date, created_timestamp, NOW(), 'SOURCE_SYSTEM'
    FROM Source_DB.raw_transactions
    WHERE transaction_id IS NOT NULL AND transaction_id != '';

    TRUNCATE TABLE staging_suppliers;
    INSERT INTO staging_suppliers (supplier_id, supplier_name, email, city, created_timestamp, load_timestamp, data_source)
    SELECT TRIM(supplier_id), TRIM(supplier_name), email, city, created_timestamp, NOW(), 'SOURCE_SYSTEM'
    FROM Source_DB.raw_suppliers
    WHERE supplier_id IS NOT NULL AND supplier_id != '';
  COMMIT;

  UPDATE etl_control_log SET status='COMPLETED', end_time=NOW() WHERE process_name='SOURCE_TO_STAGING' AND start_time=v_start;
END $$
DELIMITER ;

-- 2) Staging -> ODS (cleaning logic)
USE Staging_DB;
DROP PROCEDURE IF EXISTS StagingToODS;
DELIMITER $$
CREATE PROCEDURE StagingToODS()
BEGIN
    DECLARE v_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    INSERT INTO etl_control_log(process_name,status,start_time) VALUES ('STAGING_TO_ODS','RUNNING',v_start);

    START TRANSACTION;
      TRUNCATE TABLE ODS_DB.ods_customers;
      INSERT INTO ODS_DB.ods_customers (customer_id, first_name, last_name, age, gender, city, state, registration_date, annual_income, total_orders, data_quality_score, cleaning_status)
      SELECT
        COALESCE(NULLIF(TRIM(customer_id),''),'UNKNOWN'),
        COALESCE(NULLIF(TRIM(first_name),''),'UNKNOWN'),
        COALESCE(NULLIF(TRIM(last_name),''),'UNKNOWN'),
        CASE
          WHEN age IS NULL OR age = '' OR NOT age REGEXP '^[0-9]+$' OR CAST(age AS SIGNED) <=0 THEN '25'
          WHEN CAST(age AS SIGNED) > 100 THEN '65'
          ELSE age
        END,
        CASE WHEN UPPER(TRIM(gender)) IN ('M','MALE') THEN 'MALE'
             WHEN UPPER(TRIM(gender)) IN ('F','FEMALE') THEN 'FEMALE'
             ELSE 'OTHER' END,
        UPPER(COALESCE(NULLIF(TRIM(city),''),'UNKNOWN')),
        UPPER(COALESCE(NULLIF(TRIM(state),''),'UNKNOWN')),
        COALESCE(NULLIF(TRIM(registration_date),''),'2023-01-01'),
        COALESCE(NULLIF(TRIM(annual_income),''),'30000'),
        COALESCE(NULLIF(TRIM(total_orders),''),'1'),
        (
            CASE WHEN customer_id IS NOT NULL AND customer_id != '' THEN 2 ELSE 0 END +
            CASE WHEN first_name IS NOT NULL AND first_name != '' THEN 2 ELSE 0 END +
            CASE WHEN last_name IS NOT NULL AND last_name != '' THEN 2 ELSE 0 END +
            CASE WHEN age IS NOT NULL AND age REGEXP '^[0-9]+$' AND CAST(age AS SIGNED) > 0 THEN 2 ELSE 0 END +
            CASE WHEN gender IS NOT NULL AND gender != '' THEN 2 ELSE 0 END
        ),
        'CLEANED'
      FROM staging_customers
      WHERE customer_id IS NOT NULL AND customer_id != '';

      TRUNCATE TABLE ODS_DB.ods_products;
      INSERT INTO ODS_DB.ods_products (product_id, product_name, category, brand, price, data_quality_score, cleaning_status)
      SELECT
        COALESCE(NULLIF(TRIM(product_id),''),'UNKNOWN'),
        COALESCE(NULLIF(TRIM(product_name),''),'UNKNOWN'),
        UPPER(COALESCE(NULLIF(TRIM(category),''),'GENERAL')),
        UPPER(COALESCE(NULLIF(TRIM(brand),''),'GENERIC')),
        CASE WHEN price IS NULL OR price = '' OR NOT price REGEXP '^[0-9]+\\.?[0-9]*$' OR CAST(price AS DECIMAL(10,2)) <= 0 THEN 10.00
             ELSE CAST(price AS DECIMAL(10,2)) END,
        (
            CASE WHEN product_id IS NOT NULL AND product_id != '' THEN 2 ELSE 0 END +
            CASE WHEN product_name IS NOT NULL AND product_name != '' THEN 2 ELSE 0 END +
            CASE WHEN category IS NOT NULL AND category != '' THEN 2 ELSE 0 END +
            CASE WHEN price IS NOT NULL AND price REGEXP '^[0-9]+\\.?[0-9]*$' AND CAST(price AS DECIMAL(10,2)) > 0 THEN 2 ELSE 0 END
        ),
        'CLEANED'
      FROM staging_products
      WHERE product_id IS NOT NULL AND product_id != '';

      TRUNCATE TABLE ODS_DB.ods_transactions;
      INSERT INTO ODS_DB.ods_transactions (transaction_id, customer_id, product_id, quantity, unit_price, transaction_date, data_quality_score, cleaning_status)
      SELECT
        COALESCE(NULLIF(TRIM(transaction_id),''),'UNKNOWN'),
        COALESCE(NULLIF(TRIM(customer_id),''),'UNKNOWN'),
        COALESCE(NULLIF(TRIM(product_id),''),'UNKNOWN'),
        CASE WHEN quantity IS NULL OR quantity = '' OR NOT quantity REGEXP '^[0-9]+$' OR CAST(quantity AS SIGNED) <= 0 THEN 1 ELSE CAST(quantity AS SIGNED) END,
        CASE WHEN unit_price IS NULL OR unit_price = '' OR NOT unit_price REGEXP '^[0-9]+\\.?[0-9]*$' OR CAST(unit_price AS DECIMAL(10,2)) <= 0 THEN 10.00 ELSE CAST(unit_price AS DECIMAL(10,2)) END,
        COALESCE(NULLIF(TRIM(transaction_date),''),'2023-01-01'),
        (
            CASE WHEN transaction_id IS NOT NULL AND transaction_id != '' THEN 2 ELSE 0 END +
            CASE WHEN customer_id IS NOT NULL AND customer_id != '' THEN 2 ELSE 0 END +
            CASE WHEN product_id IS NOT NULL AND product_id != '' THEN 2 ELSE 0 END +
            CASE WHEN quantity IS NOT NULL AND quantity REGEXP '^[0-9]+$' AND CAST(quantity AS SIGNED) > 0 THEN 2 ELSE 0 END
        ),
        'CLEANED'
      FROM staging_transactions
      WHERE transaction_id IS NOT NULL AND transaction_id != '';

      TRUNCATE TABLE ODS_DB.ods_suppliers;
      INSERT INTO ODS_DB.ods_suppliers (supplier_id, supplier_name, email, city, data_quality_score, cleaning_status)
      SELECT
        COALESCE(NULLIF(TRIM(supplier_id),''),'UNKNOWN'),
        COALESCE(NULLIF(TRIM(supplier_name),''),'UNKNOWN'),
        COALESCE(NULLIF(LOWER(TRIM(email)),'') , 'unknown@company.com'),
        UPPER(COALESCE(NULLIF(TRIM(city),''),'UNKNOWN')),
        (
            CASE WHEN supplier_id IS NOT NULL AND supplier_id != '' THEN 2 ELSE 0 END +
            CASE WHEN supplier_name IS NOT NULL AND supplier_name != '' THEN 2 ELSE 0 END +
            CASE WHEN email IS NOT NULL AND email LIKE '%@%' THEN 2 ELSE 0 END
        ),
        'CLEANED'
      FROM staging_suppliers
      WHERE supplier_id IS NOT NULL AND supplier_id != '';
    COMMIT;

    UPDATE etl_control_log SET status='COMPLETED', end_time=NOW() WHERE process_name='STAGING_TO_ODS' AND start_time=v_start;
END $$
DELIMITER ;

-- 3) PopulateDataQualityAudit (compares staging -> ods and writes detail rows)
USE Staging_DB;
DROP PROCEDURE IF EXISTS PopulateDataQualityAudit;
DELIMITER $$
CREATE PROCEDURE PopulateDataQualityAudit(IN p_audit_run_id VARCHAR(50))
BEGIN
    DECLARE v_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    -- Log start
    INSERT INTO etl_control_log(process_name, status, start_time)
      VALUES ('POPULATE_DQ_AUDIT', 'RUNNING', v_start_time);

    -- Clear previous rows for same audit id (idempotent)
    DELETE FROM data_quality_audit WHERE audit_run_id = p_audit_run_id;

    -- ========== CUSTOMERS ==========
    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id, 'customers', COALESCE(TRIM(s.customer_id), TRIM(o.customer_id)), 'age',
      CASE
        WHEN s.age IS NULL OR s.age = '' THEN 'MissingValue'
        WHEN s.age NOT REGEXP '^[0-9]+$' THEN 'InvalidFormat'
        WHEN CAST(s.age AS SIGNED) <= 0 THEN 'OutOfRangeLow'
        WHEN CAST(s.age AS SIGNED) > 100 THEN 'OutOfRangeHigh'
        ELSE 'Standardized'
      END,
      s.age, o.age,
      CASE
        WHEN s.age IS NULL OR s.age = '' THEN 'HIGH'
        WHEN s.age NOT REGEXP '^[0-9]+$' THEN 'HIGH'
        WHEN CAST(s.age AS SIGNED)<=0 OR CAST(s.age AS SIGNED)>100 THEN 'HIGH'
        ELSE 'LOW'
      END,
      'STAGING_TO_ODS'
    FROM staging_customers s
    JOIN ODS_DB.ods_customers o ON TRIM(s.customer_id)=TRIM(o.customer_id)
    WHERE (s.age IS NULL OR s.age = '' OR s.age NOT REGEXP '^[0-9]+$' OR CAST(s.age AS SIGNED) <=0 OR CAST(s.age AS SIGNED)>100 OR s.age <> o.age);

    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id, 'customers', COALESCE(TRIM(s.customer_id), TRIM(o.customer_id)), 'gender',
      'StandardizedValue', s.gender, o.gender, 'LOW', 'STAGING_TO_ODS'
    FROM staging_customers s
    JOIN ODS_DB.ods_customers o ON TRIM(s.customer_id)=TRIM(o.customer_id)
    WHERE UPPER(TRIM(COALESCE(s.gender,''))) NOT IN ('MALE','FEMALE','OTHER') OR UPPER(TRIM(s.gender)) <> UPPER(TRIM(o.gender));

    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id,'customers',TRIM(s.customer_id),'city',
           CASE WHEN s.city IS NULL OR s.city='' THEN 'DefaultedValue' ELSE 'StandardizedCase' END,
           s.city,o.city,
           CASE WHEN s.city IS NULL OR s.city='' THEN 'MEDIUM' ELSE 'LOW' END,
           'STAGING_TO_ODS'
    FROM staging_customers s
    JOIN ODS_DB.ods_customers o ON TRIM(s.customer_id)=TRIM(o.customer_id)
    WHERE COALESCE(UPPER(TRIM(s.city)),'UNKNOWN') <> COALESCE(TRIM(o.city),'UNKNOWN');

    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id,'customers',TRIM(s.customer_id),'state',
           CASE WHEN s.state IS NULL OR s.state='' THEN 'DefaultedValue' ELSE 'StandardizedCase' END,
           s.state,o.state,
           CASE WHEN s.state IS NULL OR s.state='' THEN 'MEDIUM' ELSE 'LOW' END,
           'STAGING_TO_ODS'
    FROM staging_customers s
    JOIN ODS_DB.ods_customers o ON TRIM(s.customer_id)=TRIM(o.customer_id)
    WHERE COALESCE(UPPER(TRIM(s.state)),'UNKNOWN') <> COALESCE(TRIM(o.state),'UNKNOWN');

    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id,'customers',TRIM(s.customer_id),'registration_date','DefaultedValue', s.registration_date,o.registration_date,'MEDIUM','STAGING_TO_ODS'
    FROM staging_customers s
    JOIN ODS_DB.ods_customers o ON TRIM(s.customer_id)=TRIM(o.customer_id)
    WHERE (s.registration_date IS NULL OR s.registration_date = '') OR (s.registration_date <> o.registration_date);

    -- ========== PRODUCTS ==========
    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id,'products',TRIM(s.product_id),'price',
           CASE WHEN s.price IS NULL OR s.price = '' THEN 'DefaultedValue' ELSE 'InvalidNumber' END,
           s.price, CAST(o.price AS CHAR), 'HIGH','STAGING_TO_ODS'
    FROM staging_products s
    JOIN ODS_DB.ods_products o ON TRIM(s.product_id)=TRIM(o.product_id)
    WHERE s.price IS NULL OR s.price = '' OR s.price NOT REGEXP '^[0-9]+\\.?[0-9]*$' OR CAST(s.price AS DECIMAL(10,2)) <= 0 OR s.price <> CAST(o.price AS CHAR);

    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id,'products',TRIM(s.product_id),'category','StandardizedCase', s.category, o.category, 'LOW','STAGING_TO_ODS'
    FROM staging_products s
    JOIN ODS_DB.ods_products o ON TRIM(s.product_id)=TRIM(o.product_id)
    WHERE COALESCE(UPPER(TRIM(s.category)),'GENERAL') <> COALESCE(TRIM(o.category),'GENERAL');

    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id,'products',TRIM(s.product_id),'brand','StandardizedCase', s.brand, o.brand, 'LOW','STAGING_TO_ODS'
    FROM staging_products s
    JOIN ODS_DB.ods_products o ON TRIM(s.product_id)=TRIM(o.product_id)
    WHERE COALESCE(UPPER(TRIM(s.brand)),'GENERIC') <> COALESCE(TRIM(o.brand),'GENERIC');

    -- ========== TRANSACTIONS ==========
    -- quantity
    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id,'transactions',TRIM(t.transaction_id),'quantity',
           CASE WHEN t.quantity IS NULL OR t.quantity = '' THEN 'MissingValue' WHEN t.quantity NOT REGEXP '^[0-9]+$' THEN 'InvalidNumber' ELSE 'Standardized' END,
           t.quantity, CAST(o.quantity AS CHAR),
           CASE WHEN t.quantity IS NULL OR t.quantity = '' THEN 'HIGH' WHEN t.quantity NOT REGEXP '^[0-9]+$' THEN 'HIGH' ELSE 'LOW' END,
           'STAGING_TO_ODS'
    FROM staging_transactions t
    JOIN ODS_DB.ods_transactions o ON TRIM(t.transaction_id)=TRIM(o.transaction_id)
    WHERE (t.quantity IS NULL OR t.quantity = '' OR t.quantity NOT REGEXP '^[0-9]+$' OR CAST(t.quantity AS SIGNED) <= 0 OR t.quantity <> CAST(o.quantity AS CHAR));

    -- unit_price
    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id,'transactions',TRIM(t.transaction_id),'unit_price',
           CASE WHEN t.unit_price IS NULL OR t.unit_price = '' THEN 'MissingValue' WHEN t.unit_price NOT REGEXP '^[0-9]+\\.?[0-9]*$' THEN 'InvalidNumber' ELSE 'Standardized' END,
           t.unit_price, CAST(o.unit_price AS CHAR),
           CASE WHEN t.unit_price IS NULL OR t.unit_price = '' THEN 'HIGH' WHEN t.unit_price NOT REGEXP '^[0-9]+\\.?[0-9]*$' THEN 'HIGH' ELSE 'LOW' END,
           'STAGING_TO_ODS'
    FROM staging_transactions t
    JOIN ODS_DB.ods_transactions o ON TRIM(t.transaction_id)=TRIM(o.transaction_id)
    WHERE (t.unit_price IS NULL OR t.unit_price = '' OR t.unit_price NOT REGEXP '^[0-9]+\\.?[0-9]*$' OR CAST(t.unit_price AS DECIMAL(10,2)) <= 0 OR t.unit_price <> CAST(o.unit_price AS CHAR));

    -- transaction_date defaulted/changed
    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id,'transactions',TRIM(t.transaction_id),'transaction_date','DefaultedValue', t.transaction_date, o.transaction_date, 'MEDIUM','STAGING_TO_ODS'
    FROM staging_transactions t
    JOIN ODS_DB.ods_transactions o ON TRIM(t.transaction_id)=TRIM(o.transaction_id)
    WHERE (t.transaction_date IS NULL OR t.transaction_date = '') OR (t.transaction_date <> o.transaction_date);

    -- referential checks (customer/product)
    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id,'transactions',TRIM(t.transaction_id),'customer_id','REFERENTIAL_MISMATCH', t.customer_id, 'NOT_FOUND_IN_ODS_CUSTOMERS','CRITICAL','STAGING_TO_ODS'
    FROM staging_transactions t
    LEFT JOIN ODS_DB.ods_customers c ON TRIM(c.customer_id)=TRIM(t.customer_id)
    WHERE c.customer_id IS NULL;

    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id,'transactions',TRIM(t.transaction_id),'product_id','REFERENTIAL_MISMATCH', t.product_id, 'NOT_FOUND_IN_ODS_PRODUCTS','CRITICAL','STAGING_TO_ODS'
    FROM staging_transactions t
    LEFT JOIN ODS_DB.ods_products p ON TRIM(p.product_id)=TRIM(t.product_id)
    WHERE p.product_id IS NULL;

    -- ========== SUPPLIERS ==========
    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id,'suppliers',TRIM(s.supplier_id),'email','InvalidEmail', s.email, o.email, 'MEDIUM','STAGING_TO_ODS'
    FROM staging_suppliers s
    JOIN ODS_DB.ods_suppliers o ON TRIM(s.supplier_id)=TRIM(o.supplier_id)
    WHERE LOWER(TRIM(s.email)) NOT REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' OR LOWER(TRIM(s.email)) <> LOWER(TRIM(o.email));

    -- city casing / defaulted
    INSERT INTO data_quality_audit (audit_run_id, table_name, record_id, field_name, issue_type, original_value, cleaned_value, severity_level, process_name)
    SELECT p_audit_run_id,'suppliers',TRIM(s.supplier_id),'city','StandardizedCase', s.city, o.city, 'LOW','STAGING_TO_ODS'
    FROM staging_suppliers s
    JOIN ODS_DB.ods_suppliers o ON TRIM(s.supplier_id)=TRIM(o.supplier_id)
    WHERE COALESCE(UPPER(TRIM(s.city)),'UNKNOWN') <> COALESCE(TRIM(o.city),'UNKNOWN');

    -- Finalize run log
    UPDATE etl_control_log SET status='COMPLETED', end_time=NOW() WHERE process_name='POPULATE_DQ_AUDIT' AND start_time=v_start_time;
END $$
DELIMITER ;

-- 4) BuildAuditSummary (roll-up)
USE Staging_DB;
DROP PROCEDURE IF EXISTS BuildAuditSummary;
DELIMITER $$
CREATE PROCEDURE BuildAuditSummary(IN p_audit_run_id VARCHAR(50))
BEGIN
  -- delete existing summary for this run (idempotent)
  DELETE FROM data_quality_summary WHERE audit_run_id = p_audit_run_id;

  -- CUSTOMERS summary
  INSERT INTO data_quality_summary (audit_run_id, table_name, total_records, clean_records, issues_found, critical_issues, high_issues, medium_issues, low_issues, data_quality_percentage, audit_timestamp)
  SELECT
    p_audit_run_id,
    'staging_customers',
    (SELECT COUNT(*) FROM staging_customers),
    GREATEST(0, (SELECT COUNT(*) FROM staging_customers) - (SELECT COUNT(DISTINCT record_id) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='customers')),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='customers'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='customers' AND severity_level='CRITICAL'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='customers' AND severity_level='HIGH'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='customers' AND severity_level='MEDIUM'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='customers' AND severity_level='LOW'),
    ROUND(100 * (GREATEST(0,(SELECT COUNT(*) FROM staging_customers) - (SELECT COUNT(DISTINCT record_id) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='customers')) / NULLIF((SELECT COUNT(*) FROM staging_customers),0)),2),
    NOW();

  -- PRODUCTS summary
  INSERT INTO data_quality_summary (audit_run_id, table_name, total_records, clean_records, issues_found, critical_issues, high_issues, medium_issues, low_issues, data_quality_percentage, audit_timestamp)
  SELECT
    p_audit_run_id, 'staging_products',
    (SELECT COUNT(*) FROM staging_products),
    GREATEST(0,(SELECT COUNT(*) FROM staging_products) - (SELECT COUNT(DISTINCT record_id) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='products')),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='products'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='products' AND severity_level='CRITICAL'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='products' AND severity_level='HIGH'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='products' AND severity_level='MEDIUM'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='products' AND severity_level='LOW'),
    ROUND(100 * (GREATEST(0,(SELECT COUNT(*) FROM staging_products) - (SELECT COUNT(DISTINCT record_id) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='products')) / NULLIF((SELECT COUNT(*) FROM staging_products),0)),2),
    NOW();

  -- TRANSACTIONS summary
  INSERT INTO data_quality_summary (audit_run_id, table_name, total_records, clean_records, issues_found, critical_issues, high_issues, medium_issues, low_issues, data_quality_percentage, audit_timestamp)
  SELECT
    p_audit_run_id, 'staging_transactions',
    (SELECT COUNT(*) FROM staging_transactions),
    GREATEST(0,(SELECT COUNT(*) FROM staging_transactions) - (SELECT COUNT(DISTINCT record_id) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='transactions')),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='transactions'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='transactions' AND severity_level='CRITICAL'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='transactions' AND severity_level='HIGH'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='transactions' AND severity_level='MEDIUM'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='transactions' AND severity_level='LOW'),
    ROUND(100 * (GREATEST(0,(SELECT COUNT(*) FROM staging_transactions) - (SELECT COUNT(DISTINCT record_id) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='transactions')) / NULLIF((SELECT COUNT(*) FROM staging_transactions),0)),2),
    NOW();

  -- SUPPLIERS summary
  INSERT INTO data_quality_summary (audit_run_id, table_name, total_records, clean_records, issues_found, critical_issues, high_issues, medium_issues, low_issues, data_quality_percentage, audit_timestamp)
  SELECT
    p_audit_run_id, 'staging_suppliers',
    (SELECT COUNT(*) FROM staging_suppliers),
    GREATEST(0,(SELECT COUNT(*) FROM staging_suppliers) - (SELECT COUNT(DISTINCT record_id) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='suppliers')),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='suppliers'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='suppliers' AND severity_level='CRITICAL'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='suppliers' AND severity_level='HIGH'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='suppliers' AND severity_level='MEDIUM'),
    (SELECT COUNT(*) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='suppliers' AND severity_level='LOW'),
    ROUND(100 * (GREATEST(0,(SELECT COUNT(*) FROM staging_suppliers) - (SELECT COUNT(DISTINCT record_id) FROM data_quality_audit WHERE audit_run_id=p_audit_run_id AND table_name='suppliers')) / NULLIF((SELECT COUNT(*) FROM staging_suppliers),0)),2),
    NOW();
END $$
DELIMITER ;

-- 5) Load from ODS -> DWH (simple mapping). You can enrich this later.
-- 5) Load from ODS -> DWH (using JOINs for safe lookups)
USE DWH_DB;
DROP PROCEDURE IF EXISTS LoadODSToDWH;
DELIMITER $$
CREATE PROCEDURE LoadODSToDWH()
BEGIN
  DECLARE v_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
  INSERT INTO Staging_DB.etl_control_log(process_name,status,start_time) 
  VALUES ('ODS_TO_DWH','RUNNING',v_start);

  START TRANSACTION;
    -- ======================
    -- Load Dimensions
    -- ======================
    TRUNCATE TABLE dim_customer;
    INSERT INTO dim_customer (customer_id, first_name, last_name, age, gender, city, state)
    SELECT 
        TRIM(customer_id),
        first_name,
        last_name,
        CAST(age AS SIGNED),
        gender,
        city,
        state
    FROM ODS_DB.ods_customers;

    TRUNCATE TABLE dim_product;
    INSERT INTO dim_product (product_id, product_name, category, brand)
    SELECT 
        TRIM(product_id),
        product_name,
        category,
        brand
    FROM ODS_DB.ods_products;

    -- ======================
    -- Load Fact Sales
    -- ======================
    TRUNCATE TABLE fact_sales;
    INSERT INTO fact_sales (
        transaction_id, customer_key, product_key, quantity, unit_price, transaction_date, amount
    )
    SELECT 
        t.transaction_id,
        dc.customer_key,
        dp.product_key,
        t.quantity,
        t.unit_price,
        CAST(SUBSTRING_INDEX(t.transaction_date, ' ', 1) AS DATE) AS transaction_date,
        (t.quantity * t.unit_price) AS amount
    FROM ODS_DB.ods_transactions t
    LEFT JOIN dim_customer dc ON TRIM(dc.customer_id) = TRIM(t.customer_id)
    LEFT JOIN dim_product dp ON TRIM(dp.product_id) = TRIM(t.product_id);

  COMMIT;

  UPDATE Staging_DB.etl_control_log 
  SET status='COMPLETED', end_time=NOW() 
  WHERE process_name='ODS_TO_DWH' AND start_time=v_start;
END $$
DELIMITER ;


-- 6) Load DWH -> Data Marts (simple copies)
USE DM_DB;
DROP PROCEDURE IF EXISTS LoadDWHToDM;
DELIMITER $$
CREATE PROCEDURE LoadDWHToDM()
BEGIN
  DECLARE v_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
  INSERT INTO Staging_DB.etl_control_log(process_name,status,start_time) VALUES ('DWH_TO_DM','RUNNING',v_start);

  START TRANSACTION;
     TRUNCATE TABLE dm_finance_sales;
     INSERT INTO dm_finance_sales SELECT * FROM DWH_DB.fact_sales;

     TRUNCATE TABLE dm_marketing_customers;
     INSERT INTO dm_marketing_customers SELECT * FROM DWH_DB.dim_customer;

     TRUNCATE TABLE dm_hr_suppliers;
     INSERT INTO dm_hr_suppliers SELECT * FROM ODS_DB.ods_suppliers;
  COMMIT;

  UPDATE Staging_DB.etl_control_log SET status='COMPLETED', end_time=NOW() WHERE process_name='DWH_TO_DM' AND start_time=v_start;
END $$
DELIMITER ;

-- ==================================================
-- STEP 9: Helpful views for BI
-- ==================================================
USE Staging_DB;
DROP VIEW IF EXISTS vw_dq_issues_by_type;
CREATE VIEW vw_dq_issues_by_type AS
SELECT audit_run_id, table_name, issue_type, severity_level, COUNT(*) AS issues
FROM data_quality_audit
GROUP BY audit_run_id, table_name, issue_type, severity_level;

DROP VIEW IF EXISTS vw_dq_trend;
CREATE VIEW vw_dq_trend AS
SELECT audit_run_id, table_name, COUNT(*) AS issues_found, MIN(audit_timestamp) AS run_timestamp
FROM data_quality_audit
GROUP BY audit_run_id, table_name;

DROP VIEW IF EXISTS vw_dq_latest_summary;
CREATE VIEW vw_dq_latest_summary AS
SELECT s.*
FROM data_quality_summary s
JOIN (SELECT table_name, MAX(audit_timestamp) AS max_ts FROM data_quality_summary GROUP BY table_name) m
  ON s.table_name = m.table_name AND s.audit_timestamp = m.max_ts;

-- ==================================================
-- STEP 10: Usage examples (order to run for a full pipeline)
-- ==================================================
SET SQL_SAFE_UPDATES = 0;

-- 1) Load Source -> Staging (populates staging_* tables from Source_DB.raw_*)
   CALL SourceToStaging();

-- 2) Run audit run (give unique id) before/after Staging->ODS depending on your preference
   SET @audit_run_id = DATE_FORMAT(NOW(), '%Y%m%d%H%i%s');
   CALL PopulateDataQualityAudit(@audit_run_id);

-- 3) Run cleaning & load to ODS
   CALL StagingToODS();

-- 4) Build per-run DQ summary (rollup)
   CALL BuildAuditSummary(@audit_run_id);

use dwh_db;
-- 5) Load ODS -> DWH
   CALL LoadODSToDWH();
use dm_db;
-- 6) Load DWH -> Data Marts
   CALL LoadDWHToDM();


SET SQL_SAFE_UPDATES = 1;

use staging_db;
select * from data_quality_audit;