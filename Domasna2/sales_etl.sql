-- First the dimension tables
-- We start tracking the time for the status table,
-- then load the data using merges, finally we finish the tracking

CREATE OR ALTER PROCEDURE ETL_Dim_Products
AS
BEGIN
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @RowsAffected INT;

    INSERT INTO Lab1_DW.ETL_Status (table_name, start_time, status)
    VALUES ('Dim_Products', @StartTime, 'Running');

    WITH Source_Products AS (SELECT p.ProductID,
                                    p.Name,
                                    p.ProductNumber,
                                    p.Color,
                                    p.StandardCost,
                                    p.ListPrice,
                                    p.Size,
                                    p.Weight,
                                    pc.ProductCategoryID,
                                    p.ProductModelID
                             FROM Production.Product p
                                      JOIN Production.ProductSubcategory ps
                                           ON p.ProductSubcategoryID = ps.ProductSubcategoryID
                                      JOIN Production.ProductCategory pc ON ps.ProductCategoryID = pc.ProductCategoryID)
        MERGE Lab1_DW.Dim_Products AS target
    USING (SELECT * FROM Source_Products) AS source
    ON (target.product_id = source.ProductID)
    WHEN MATCHED THEN
        UPDATE
        SET target.name                = source.Name,
            target.product_number      = source.ProductNumber,
            target.color               = source.Color,
            target.standard_cost       = source.StandardCost,
            target.list_price          = source.ListPrice,
            target.size                = source.Size,
            target.weight              = source.Weight,
            target.product_category_id = source.ProductCategoryID,
            target.product_model_id    = source.ProductModelID
    WHEN NOT MATCHED THEN
        INSERT (product_id, name, product_number, color, standard_cost, list_price, size, weight, product_category_id,
                product_model_id)
        VALUES (source.ProductID,
                source.Name,
                source.ProductNumber,
                source.Color,
                source.StandardCost,
                source.ListPrice,
                source.Size,
                source.Weight,
                source.ProductCategoryID,
                source.ProductModelID);

    SET @RowsAffected = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETDATE();

    UPDATE Lab1_DW.ETL_Status
    SET end_time      = @EndTime,
        status        = 'Completed',
        rows_affected = @RowsAffected
    WHERE table_name = 'Dim_Products'
      AND start_time = @StartTime;
END
GO



CREATE OR ALTER PROCEDURE ETL_Dim_SalesTerritory
AS
BEGIN
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @RowsAffected INT;
    INSERT INTO Lab1_DW.ETL_Status (table_name, start_time, status)
    VALUES ('Dim_SalesTerritory', @StartTime, 'Running');

    WITH Source_SalesTerritory AS
        (
        SELECT TerritoryID,
               Name,
               CountryRegionCode,
               [Group],
               SalesYTD,
               CostYTD
        FROM Sales.SalesTerritory
        )
        MERGE Lab1_DW.Dim_SalesTerritory AS target
    USING (SELECT * FROM Source_SalesTerritory) AS source
    ON (target.territory_id = source.TerritoryID)
    WHEN MATCHED THEN
        UPDATE
        SET target.name                = source.Name,
            target.country_region_code = source.CountryRegionCode,
            target.[group]             = source.[Group],
            target.sales               = source.SalesYTD,
            target.cost                = source.CostYTD
    WHEN NOT MATCHED THEN
        INSERT (territory_id, name, country_region_code, [group], sales, cost)
        VALUES (source.TerritoryID,
                source.Name,
                source.CountryRegionCode,
                source.[Group],
                source.SalesYTD,
                source.CostYTD);

    SET @RowsAffected = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETDATE();

    UPDATE Lab1_DW.ETL_Status
    SET end_time      = @EndTime,
        status        = 'Completed',
        rows_affected = @RowsAffected
    WHERE table_name = 'Dim_SalesTerritory'
      AND start_time = @StartTime;
END
GO

CREATE OR ALTER PROCEDURE ETL_Dim_Date
AS
BEGIN
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @RowsAffected INT;
    INSERT INTO Lab1_DW.ETL_Status (table_name, start_time, status)
    VALUES ('Dim_Date', @StartTime, 'Running');

    -- Get date range
    DECLARE @StartDate DATE;
    SELECT @StartDate = MIN(OrderDate)
    FROM Sales.SalesOrderHeader;

    DECLARE @EndDate DATE;
    SELECT @EndDate = MAX(OrderDate)
    FROM Sales.SalesOrderHeader

    DECLARE @CurrentDate DATE = @StartDate
    WHILE @CurrentDate <= @EndDate
        BEGIN
            DECLARE @SKey_date INT = FORMAT(@CurrentDate, 'yyyyMMdd');
            DECLARE @Quarter INT = DATEPART(QUARTER, @CurrentDate);

            MERGE Lab1_DW.Dim_Date AS target
            USING (SELECT @SKey_date AS SKey_date, @CurrentDate AS date_value, @Quarter AS quarter) AS source
            ON (target.SKey_date = source.SKey_date)
            WHEN MATCHED THEN
                UPDATE
                SET target.date_value = @CurrentDate,
                    target.quarter    = @Quarter
            WHEN NOT MATCHED THEN
                INSERT (SKey_date, date_value, quarter)
                VALUES (@SKey_date, @CurrentDate, @Quarter);

            SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
        END

    SET @RowsAffected = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETDATE();

    UPDATE Lab1_DW.ETL_Status
    SET end_time      = @EndTime,
        status        = 'Completed',
        rows_affected = @RowsAffected
    WHERE table_name = 'Dim_Date'
      AND start_time = @StartTime;
END
GO

CREATE OR ALTER PROCEDURE ETL_Fact_Sales
AS
BEGIN
    DECLARE @StartTime DATETIME = GETDATE()
    DECLARE @RowsAffected INT;
    INSERT INTO Lab1_DW.ETL_Status (table_name, start_time, status)
    VALUES ('Fact_Sales', @StartTime, 'Running')

    SELECT dim_st.SKey_territory,
           dim_p.SKey_product,
           dim_date.SKey_date,
           sod.OrderQty  AS quantity,
           sod.LineTotal AS amount
    INTO #TempSales
    FROM Sales.SalesOrderHeader soh
             JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
             JOIN Production.Product p ON sod.ProductID = p.ProductID
             JOIN Sales.SalesTerritory st ON soh.TerritoryID = st.TerritoryID

             JOIN Lab1_DW.Dim_Products dim_p ON p.ProductID = dim_p.product_id
             JOIN Lab1_DW.Dim_SalesTerritory dim_st ON st.TerritoryID = dim_st.territory_id
             JOIN Lab1_DW.Dim_Date dim_date ON FORMAT(soh.OrderDate, 'yyyyMMdd') = dim_date.SKey_date;

    WITH AggregatedSales AS (SELECT SKey_territory,
                                    SKey_product,
                                    SKey_date,
                                    SUM(quantity) AS total_quantity,
                                    SUM(amount)   AS total_amount
                             FROM #TempSales
                             GROUP BY SKey_territory,
                                      SKey_product,
                                      SKey_date)
        MERGE Lab1_DW.Fact_Sales AS target
    USING AggregatedSales AS source
    ON (target.SKey_territory = source.SKey_territory
        AND target.SKey_product = source.SKey_product
        AND target.SKey_date = source.SKey_date)
    WHEN MATCHED THEN
        UPDATE
        SET target.total_quantity = source.total_quantity,
            target.total_amount   = source.total_amount
    WHEN NOT MATCHED THEN
        INSERT (SKey_territory, SKey_product, SKey_date, total_quantity, total_amount)
        VALUES (source.SKey_territory,
                source.SKey_product,
                source.SKey_date,
                source.total_quantity,
                source.total_amount);

    SET @RowsAffected = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETDATE();

    UPDATE Lab1_DW.ETL_Status
    SET end_time      = @EndTime,
        status        = 'Completed',
        rows_affected = @RowsAffected
    WHERE table_name = 'Fact_Sales'
      AND start_time = @StartTime;

    DROP TABLE #TempSales;
END
GO;


EXEC ETL_Dim_Products;
EXEC ETL_Dim_SalesTerritory;
EXEC ETL_Dim_Date;
EXEC ETL_Fact_Sales;