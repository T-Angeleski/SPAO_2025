CREATE SCHEMA Lab1_DW
GO;

-- Part 1:  Modeling tables
CREATE TABLE Lab1_DW.Dim_Products
(
    SKey_product        INT          NOT NULL IDENTITY (1,1) PRIMARY KEY,
    product_id          INT          NOT NULL,
    name                NVARCHAR(50) NOT NULL,
    product_number      NVARCHAR(25) NOT NULL,
    color               NVARCHAR(15),
    standard_cost       MONEY        NOT NULL,
    list_price          MONEY        NOT NULL,
    size                NVARCHAR(5),
    weight              DECIMAL(8, 2),
    product_category_id INT,
    product_model_id    INT,
    ETL_LOAD_TIME       DATETIME DEFAULT GETUTCDATE()
)

CREATE TABLE Lab1_DW.Dim_SalesTerritory
(
    SKey_territory      INT NOT NULL IDENTITY (1,1) PRIMARY KEY,
    territory_id        INT NOT NULL,
    name                NVARCHAR(50),
    country_region_code NVARCHAR(3),
    [group]             NVARCHAR(50),
    sales               MONEY,
    cost                MONEY,
    ETL_LOAD_TIME       DATETIME DEFAULT GETUTCDATE()
)


CREATE TABLE Lab1_DW.Dim_Date
(
    SKey_date  INT  NOT NULL PRIMARY KEY, -- yyyyMMdd format for key
    date_value DATE NOT NULL,
    quarter    INT  NOT NULL
)

CREATE TABLE Lab1_DW.Fact_Sales
(
    SKey_territory INT   NOT NULL,
    SKey_product   INT   NOT NULL,
    SKey_date      INT   NOT NULL,
    total_quantity INT   NOT NULL,
    total_amount   MONEY NOT NULL,
    ETL_LOAD_TIME  DATETIME DEFAULT GETUTCDATE(),

    FOREIGN KEY (SKey_territory) REFERENCES Lab1_DW.Dim_SalesTerritory (SKey_territory),
    FOREIGN KEY (SKey_product) REFERENCES Lab1_DW.Dim_Products (SKey_product),
    FOREIGN KEY (SKey_date) REFERENCES Lab1_DW.Dim_Date (SKey_date)
)

-- Part 2: Expanding DW
CREATE TABLE Lab1_DW.Dim_ShipMethod
(
    SKey_ship_method INT          NOT NULL IDENTITY (1,1) PRIMARY KEY,
    ship_method_id   INT          NOT NULL,
    name             NVARCHAR(50) NOT NULL,
    ship_base        MONEY,
    ship_rate        MONEY,
    ETL_LOAD_TIME    DATETIME DEFAULT GETUTCDATE()
)

CREATE TABLE Lab1_DW.Dim_ProductModel
(
    SKey_product_model INT          NOT NULL IDENTITY (1,1) PRIMARY KEY,
    product_model_id   INT          NOT NULL,
    name               NVARCHAR(50) NOT NULL,
    ETL_LOAD_TIME      DATETIME DEFAULT GETUTCDATE()
)

CREATE TABLE Lab1_DW.Dim_ShipDate
(
    SKey_ship_date INT  NOT NULL PRIMARY KEY, -- yyyyMMdd
    date_value     DATE NOT NULL,
    quarter        INT  NOT NULL
)

CREATE TABLE Lab1_DW.Fact_Shipments
(
    SKey_ship_method INT   NOT NULL,
    SKey_product     INT   NOT NULL,
    SKey_ship_date   INT   NOT NULL,
    total_quantity   INT   NOT NULL,
    total_amount     MONEY NOT NULL,
    ETL_LOAD_TIME    DATETIME DEFAULT GETUTCDATE(),

    FOREIGN KEY (SKey_ship_method) REFERENCES Lab1_DW.Dim_ShipMethod (SKey_ship_method),
    FOREIGN KEY (SKey_product) REFERENCES Lab1_DW.Dim_Products (SKey_product),
    FOREIGN KEY (SKey_ship_date) REFERENCES Lab1_DW.Dim_ShipDate (SKey_ship_date)
)


-- Part 3 : Time dimension
DECLARE @StartDate DATE;
SELECT @StartDate = MIN(OrderDate)
FROM Sales.SalesOrderHeader;

DECLARE @NumberOfYears INT = 5;
DECLARE @EndDate DATE = DATEADD(YEAR, @NumberOfYears, @StartDate);
DECLARE @CurrentDate DATE = @StartDate;

WHILE @CurrentDate <= @EndDate
    BEGIN
        DECLARE @SKey_date INT = FORMAT(@CurrentDate, 'yyyyMMdd');
        DECLARE @Quarter INT = DATEPART(QUARTER, @CurrentDate);

        -- Check whether that date exists
        IF NOT EXISTS (SELECT 1 FROM Lab1_DW.Dim_Date WHERE SKey_date = @SKey_date)
            BEGIN
                INSERT INTO Lab1_DW.Dim_Date (SKey_date, date_value, quarter)
                VALUES (@SKey_date, @CurrentDate, @Quarter);
            END

        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END