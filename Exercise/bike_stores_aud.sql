-- Run to set up bikestores
-- CREATE DATABASE BikeStores;

-- Modeling a data warehouse (DW)
-- Make a report for the total count of ordered products based on the date, product and customer
-- Create the tables, then transfer the data in the DW

CREATE SCHEMA DW;

-- DDL
CREATE TABLE DW.Dim_Product
(
    SKey          INT NOT NULL IDENTITY (1,1) PRIMARY KEY,
    product_id    INT,
    brand_id      INT,
    category_id   INT,
    model_year    INT,
    product_name  NVARCHAR(255),
    brand_name    NVARCHAR(255),
    category_name NVARCHAR(255),
    ETL_LOAD_TIME DATETIME DEFAULT GETUTCDATE()
);

CREATE TABLE DW.Dim_Customer
(
    SKey          INT NOT NULL IDENTITY (1,1) PRIMARY KEY,
    customer_id   INT,
    first_name    NVARCHAR(255),
    last_name     NVARCHAR(255),
    phone         NVARCHAR(255),
    email         NVARCHAR(255),
    street        NVARCHAR(255),
    city          NVARCHAR(255),
    state         NVARCHAR(255),
    zip_code      NVARCHAR(255),
    ETL_LOAD_TIME DATETIME DEFAULT GETUTCDATE()
);

CREATE TABLE DW.Dim_Date
(
    SKey    INT NOT NULL IDENTITY (1,1) PRIMARY KEY,
    quarter INT,
    year    INT,
);

CREATE TABLE DW.Fact_Sales
(
    dim_product_SKey  INT,
    dim_customer_SKey INT,
    dim_date_SKey     INT,
    total_quantity    INT,
    ETL_LOAD_TIME     DATETIME DEFAULT GETUTCDATE()
);

-- DML
-- Check the tables
SELECT TOP 1 *
FROM DW.Dim_Product;
SELECT TOP 1 *
FROM DW.Dim_Customer;
SELECT TOP 1 *
FROM DW.Dim_Date;

-- Get the data from the OLTP
SELECT customer_id,
       first_name,
       last_name,
       phone,
       email,
       street,
       city,
       state,
       zip_code,
       GETUTCDATE() AS ETL_LOAD_TIME
INTO #Tmp_Customers
FROM sales.customers (NOLOCK)

SELECT TOP 5 *
FROM #Tmp_Customers

-- Transfer data to DW
INSERT INTO DW.Dim_Customer
SELECT tc.*
FROM #Tmp_Customers AS tc
         LEFT JOIN DW.Dim_Customer AS dim_c ON tc.customer_id = dim_c.customer_id
WHERE dim_c.SKey IS NULL;


-- Setting up sync if the OLTP gets updated
UPDATE sales.customers
SET last_name = 'Hello'
WHERE customer_id = 1;

SELECT *
FROM sales.customers
WHERE customer_id = 1;

-- Recreate the temp table
DROP TABLE IF EXISTS #Tmp_Customers;
SELECT customer_id,
       first_name,
       last_name,
       phone,
       email,
       street,
       city,
       state,
       zip_code,
       GETUTCDATE() AS ETL_LOAD_TIME
INTO #Tmp_Customers
FROM sales.customers (NOLOCK);

UPDATE dim_c
SET first_name    = tc.first_name,
    last_name     = tc.last_name,
    phone         = tc.phone,
    email         = tc.email,
    street        = tc.street,
    city          = tc.city,
    state         = tc.state,
    zip_code      = tc.zip_code,
    ETL_LOAD_TIME = GETUTCDATE()
FROM #Tmp_Customers AS tc
         JOIN DW.Dim_Customer AS dim_c
              ON tc.customer_id = dim_c.customer_id
WHERE tc.first_name <> dim_c.first_name
   OR tc.last_name <> dim_c.last_name
   OR tc.phone <> dim_c.phone
   OR tc.email <> dim_c.email
   OR tc.street <> dim_c.street
   OR tc.city <> dim_c.city
   OR tc.state <> dim_c.state
   OR tc.zip_code <> dim_c.zip_code;

SELECT *
FROM DW.Dim_Customer
WHERE last_name LIKE '%Hello%';

-- Product
-- We need to get the required columns using joins
WITH source_products AS (SELECT p.product_id,
                                b.brand_id,
                                c.category_id,
                                p.model_year,
                                p.product_name,
                                b.brand_name,
                                c.category_name,
                                GETUTCDATE() AS ETL_LOAD_TIME
                         FROM production.products AS p
                                  LEFT JOIN production.brands AS b ON p.brand_id = b.brand_id
                                  LEFT JOIN production.categories AS c ON p.category_id = c.category_id)
INSERT
INTO DW.Dim_Product
SELECT sp.*
FROM source_products AS sp
         LEFT JOIN DW.Dim_Product AS dim_p ON sp.product_id = dim_p.product_id
WHERE dim_p.SKey IS NULL;

SELECT *
FROM DW.Dim_Product;

-- Date
-- We need to create the quarters of the year, as we do not have that information anywhere
CREATE OR ALTER FUNCTION DW.fn_get_quarter_from_date(@date DATETIME)
    RETURNS INT
AS
BEGIN
    RETURN CASE
               WHEN DATEPART(MONTH, @date) <= 3 THEN 1
               WHEN DATEPART(MONTH, @date) <= 6 THEN 2
               WHEN DATEPART(MONTH, @date) <= 9 THEN 3
               ELSE 4
        END
END

WITH quarters AS (SELECT DISTINCT DW.fn_get_quarter_from_date(order_date) AS quarter, DATEPART(YEAR, order_date) AS year
                  FROM sales.orders)
INSERT
INTO DW.Dim_Date
SELECT q.quarter, q.year
FROM quarters AS q
         LEFT JOIN DW.Dim_Date AS dim_d ON q.year = dim_d.year AND q.quarter = dim_d.quarter
WHERE dim_d.SKey IS NULL;

SELECT *
FROM DW.Dim_Date
ORDER BY year;
