-- create schema DW2;

-- Да се нацрта модел на складиште на податоци ако сакаме
-- да правиме извештаи за вкупниот број на нарачани производи
-- врз основа на датумот (по квартали), производот и потрошувачот.

CREATE TABLE DW2.Dim_Product
(
    SKey_product  INT IDENTITY (1, 1) PRIMARY KEY,
    product_id    INT,
    brand_id      INT,
    category_id   INT,
    product_name  NVARCHAR(255),
    model_year    NVARCHAR(255),
    category_name NVARCHAR(255),
    brand_name    NVARCHAR(255),
    ETL_LOAD_TIME DATETIME DEFAULT GETUTCDATE()
);

CREATE TABLE DW2.Dim_Customer
(
    SKey_customer INT IDENTITY (1, 1) PRIMARY KEY,
    customer_id   INT,
    full_name     NVARCHAR(255),
    phone         NVARCHAR(255),
    email         NVARCHAR(255),
    city          NVARCHAR(255),
    ETL_LOAD_TIME DATETIME DEFAULT GETUTCDATE()
);

CREATE TABLE DW2.Dim_Date
(
    SKey_date INT IDENTITY (1, 1) PRIMARY KEY,
    quarter   INT,
    year      INT,
);

CREATE TABLE DW2.Fact_Reports
(
    SKey_product       INT,
    SKey_customer      INT,
    quarter            INT,
    year               INT,
    total_orders       INT,
    total_sales_amount INT,
    num_customers      INT,
    num_products_sold  INT,
    average_price      DECIMAL,
    ETL_LOAD_TIME      DATETIME DEFAULT GETUTCDATE()
);

CREATE TABLE DW2.ETL_Status
(
    ID            INT IDENTITY (1, 1) PRIMARY KEY,
    TABLE_NAME    NVARCHAR(255),
    STATUS        NVARCHAR(255),
    START_TIME    DATETIME,
    END_TIME      DATETIME,
    ROWS_AFFECTED INT
);

CREATE OR ALTER PROCEDURE ETL_Dim_Product
AS
BEGIN
    DECLARE @StartTime DATETIME = GETUTCDATE();
    INSERT INTO DW2.ETL_Status (TABLE_NAME, STATUS, START_TIME)
    VALUES ('Dim_Product', 'Running', @StartTime);

    WITH Source_Products AS
        (SELECT p.product_id,
                p.brand_id,
                p.category_id,
                p.product_name,
                p.model_year,
                c.category_name,
                b.brand_name
         FROM production.products p
                  JOIN production.categories c ON p.category_id = c.category_id
                  JOIN production.brands b ON p.brand_id = b.brand_id)
        MERGE DW2.Dim_Product AS target
    USING Source_Products AS source
    ON (source.product_id = target.product_id)
    WHEN MATCHED THEN
        UPDATE
        SET target.brand_id      = source.brand_id,
            target.category_id   = source.category_id,
            target.product_name  = source.product_name,
            target.model_year    = source.model_year,
            target.category_name = source.category_name,
            target.brand_name    = source.brand_name
    WHEN NOT MATCHED THEN
        INSERT (product_id, product_name, brand_id, category_id, model_year, category_name, brand_name)
        VALUES (source.product_id,
                source.product_name,
                source.brand_id,
                source.category_id,
                source.model_year,
                source.category_name,
                source.brand_name);

    DECLARE @RowsAffected INT = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETUTCDATE();

    UPDATE DW2.ETL_Status
    SET STATUS        = 'Completed',
        END_TIME      = @EndTime,
        ROWS_AFFECTED = @RowsAffected
    WHERE TABLE_NAME = 'Dim_Product'
      AND START_TIME = @StartTime;
END;
GO

CREATE OR ALTER PROCEDURE ETL_Dim_Customer
AS
BEGIN
    DECLARE @StartTime DATETIME = GETUTCDATE();
    INSERT INTO DW2.ETL_Status (TABLE_NAME, STATUS, START_TIME)
    VALUES ('Dim_Customer', 'Running', @StartTime);

    WITH Source_Customer AS
        (SELECT customer_id,
                first_name + ' ' + last_name AS full_name,
                phone,
                email,
                city
         FROM sales.customers)
        MERGE DW2.Dim_Customer AS target
    USING Source_Customer AS source
    ON source.customer_id = target.customer_id
    WHEN MATCHED THEN
        UPDATE
        SET target.full_name = source.full_name,
            target.phone     = source.phone,
            target.email     = source.email,
            target.city      = source.city
    WHEN NOT MATCHED THEN
        INSERT (customer_id, full_name, phone, email, city)
        VALUES (source.customer_id, source.full_name, source.phone, source.email, source.city);

    DECLARE @RowsAffected INT = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETUTCDATE();

    UPDATE DW2.ETL_Status
    SET STATUS        = 'Completed',
        END_TIME      = @EndTime,
        ROWS_AFFECTED = @RowsAffected
    WHERE TABLE_NAME = 'Dim_Customer'
      AND START_TIME = @StartTime;
END;
GO

CREATE OR ALTER PROCEDURE ETL_Dim_Date
AS
BEGIN
    DECLARE @StartTime DATETIME = GETUTCDATE();
    INSERT INTO DW2.ETL_Status (TABLE_NAME, STATUS, START_TIME)
    VALUES ('Dim_Date', 'Running', @StartTime);

    WITH Source_Date AS (SELECT DISTINCT DATEPART(QUARTER, order_date) AS quarter,
                                         DATEPART(YEAR, order_date)    AS year
                         FROM sales.orders
        )
        MERGE DW2.Dim_Date AS target
    USING Source_Date AS source
    ON (source.quarter = target.quarter AND source.year = target.year)
    WHEN MATCHED THEN
        UPDATE
        SET target.quarter = source.quarter,
            target.year    = source.year
    WHEN NOT MATCHED THEN
        INSERT (quarter, year)
        VALUES (source.quarter, source.year);

    DECLARE @RowsAffected INT = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETUTCDATE();
    UPDATE DW2.ETL_Status
    SET STATUS        = 'Completed',
        END_TIME      = @EndTime,
        ROWS_AFFECTED = @RowsAffected
    WHERE TABLE_NAME = 'Dim_Date'
      AND START_TIME = @StartTime;
END;
GO

CREATE OR ALTER PROCEDURE ETL_Fact_Reports
AS
BEGIN
    DECLARE @StartTime DATETIME = GETUTCDATE();
    INSERT INTO DW2.ETL_Status (TABLE_NAME, STATUS, START_TIME)
    VALUES ('Fact_Reports', 'Running', @StartTime);

    --     total_orders
--     total_sales_amount
--     num_customers
--     num_products_sold
--     average_price

    WITH OrderData AS (SELECT o.order_id,
                              o.customer_id,
                              DATEPART(QUARTER, o.order_date) AS quarter,
                              YEAR(o.order_date)              AS year,
                              oi.product_id,
                              oi.quantity,
                              oi.list_price,
                              oi.discount
                       FROM sales.orders o
                                JOIN sales.order_items oi ON o.order_id = oi.order_id),
        AggregatedData AS (SELECT dim_p.SKey_product,
                                  dim_c.SKey_customer,
                                  dim_d.quarter,
                                  dim_d.year,
                                  COUNT(DISTINCT od.order_id)                          AS total_orders,
                                  SUM(od.quantity * od.list_price * (1 - od.discount)) AS total_sales_amount,
                                  COUNT(DISTINCT od.customer_id)                       AS num_customers,
                                  COUNT(DISTINCT od.product_id)                        AS num_products_sold,
                                  AVG(od.list_price)                                   AS average_price
                           FROM OrderData od
                                    JOIN DW2.Dim_Product dim_p
                                         ON od.product_id = dim_p.product_id
                                    JOIN DW2.Dim_Customer dim_c ON od.customer_id = dim_c.customer_id
                                    JOIN DW2.Dim_Date dim_d ON od.quarter = dim_d.quarter AND od.year = dim_d.year
                           GROUP BY dim_p.SKey_product, dim_c.SKey_customer, dim_d.quarter, dim_d.year)
        MERGE DW2.Fact_Reports AS target
    USING AggregatedData AS source
    ON (source.SKey_product = target.SKey_product
        AND source.SKey_customer = target.SKey_customer
        AND source.quarter = target.quarter
        AND source.year = target.year)
    WHEN MATCHED THEN
        UPDATE
        SET target.total_orders       = source.total_orders,
            target.total_sales_amount = source.total_sales_amount,
            target.num_customers      = source.num_customers,
            target.num_products_sold  = source.num_products_sold,
            target.average_price      = source.average_price
    WHEN NOT MATCHED THEN
        INSERT (SKey_product, SKey_customer, quarter, year, total_orders, total_sales_amount, num_customers,
                num_products_sold, average_price)
        VALUES (source.SKey_product, source.SKey_customer, source.quarter, source.year, source.total_orders,
                source.total_sales_amount, source.num_customers, source.num_products_sold, source.average_price);


    DECLARE @RowsAffected INT = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETUTCDATE();
    UPDATE DW2.ETL_Status
    SET STATUS        = 'Completed',
        END_TIME      = @EndTime,
        ROWS_AFFECTED = @RowsAffected
    WHERE TABLE_NAME = 'Fact_Reports'
      AND START_TIME = @StartTime;
END;
GO


EXEC ETL_Dim_Product;
EXEC ETL_Dim_Customer;
EXEC ETL_Dim_Date;

EXEC ETL_Fact_Reports;