CREATE OR ALTER PROCEDURE ETL_Dim_ShipMethod
AS
BEGIN
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @RowsAffected INT;
    INSERT INTO Lab1_DW.ETL_Status (table_name, start_time, status)
    VALUES ('Dim_ShipMethod', @StartTime, 'Running');

    WITH Source_ShipMethod AS (
        SELECT ShipMethodID,
               Name,
               ShipBase,
               ShipRate
        FROM Purchasing.ShipMethod
        )
        MERGE INTO Lab1_DW.Dim_ShipMethod AS target
    USING Source_ShipMethod AS source
    ON target.ship_method_id = source.ShipMethodID
    WHEN MATCHED THEN
        UPDATE
        SET target.name      = source.Name,
            target.ship_base = source.ShipBase,
            target.ship_rate = source.ShipRate
    WHEN NOT MATCHED THEN
        INSERT (ship_method_id, NAME, ship_base, ship_rate)
        VALUES (source.ShipMethodID,
                source.Name,
                source.ShipBase,
                source.ShipRate);

    SET @RowsAffected = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETDATE();

    UPDATE Lab1_DW.ETL_Status
    SET end_time      = @EndTime,
        status        = 'Completed',
        rows_affected = @RowsAffected
    WHERE table_name = 'Dim_ShipMethod'
      AND start_time = @StartTime;
END
GO

CREATE OR ALTER PROCEDURE ETL_Dim_ProductModel
AS
BEGIN
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @RowsAffected INT;
    INSERT INTO Lab1_DW.ETL_Status (table_name, start_time, status)
    VALUES ('Dim_ProductModel', @StartTime, 'Running');

    WITH Source_ProductModel AS (
        SELECT ProductModelID,
               Name
        FROM Production.ProductModel
        )
        MERGE Lab1_DW.Dim_ProductModel AS target
    USING Source_ProductModel AS source
    ON target.product_model_id = source.ProductModelID
    WHEN MATCHED THEN
        UPDATE
        SET target.name = source.Name
    WHEN NOT MATCHED THEN
        INSERT (product_model_id, name)
        VALUES (source.ProductModelID,
                source.Name);

    SET @RowsAffected = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETDATE();

    UPDATE Lab1_DW.ETL_Status
    SET end_time      = @EndTime,
        status        = 'Completed',
        rows_affected = @RowsAffected
    WHERE table_name = 'Dim_ProductModel'
      AND start_time = @StartTime;
END
GO

CREATE OR ALTER PROCEDURE ETL_Dim_ShipDate
AS
BEGIN
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @RowsAffected INT;
    INSERT INTO Lab1_DW.ETL_Status (table_name, start_time, status)
    VALUES ('Dim_ShipDate', @StartTime, 'Running');

    DECLARE @StartDate DATE;
    DECLARE @EndDate DATE;
    DECLARE @CurrentDate DATE;

    SELECT @StartDate = MIN(ShipDate),
           @EndDate = MAX(ShipDate)
    FROM Sales.SalesOrderHeader
    WHERE ShipDate IS NOT NULL;

    SET @CurrentDate = @StartDate;

    WHILE @CurrentDate <= @EndDate
        BEGIN
            DECLARE @SKey_ship_date INT = FORMAT(@CurrentDate, 'yyyyMMdd')
            DECLARE @Quarter INT = DATEPART(QUARTER, @CurrentDate);


            MERGE Lab1_DW.Dim_ShipDate AS target
            USING (SELECT @SKey_ship_date AS SKey_ship_date,
                          @CurrentDate    AS date_value,
                          @Quarter        AS quarter) AS source
            ON (target.SKey_ship_date = source.SKey_ship_date)
            WHEN MATCHED THEN
                UPDATE
                SET target.date_value = source.date_value,
                    target.quarter    = source.quarter
            WHEN NOT MATCHED THEN
                INSERT (skey_ship_date, date_value, quarter)
                VALUES (source.SKey_ship_date,
                        source.date_value,
                        source.quarter);

            SET @RowsAffected = ISNULL(@RowsAffected, 0) + @@ROWCOUNT;
            SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
        END

    DECLARE @EndTime DATETIME = GETDATE();

    UPDATE Lab1_DW.ETL_Status
    SET end_time      = @EndTime,
        status        = 'Completed',
        rows_affected = @RowsAffected
    WHERE table_name = 'Dim_ShipDate'
      AND start_time = @StartTime;
END
GO

CREATE OR ALTER PROCEDURE ETL_Fact_Shipments
AS
BEGIN
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @RowsAffected INT;

    INSERT INTO Lab1_DW.ETL_Status (table_name, start_time, status)
    VALUES ('Fact_Shipments', @StartTime, 'Running');

    SELECT dim_st.SKey_territory,
           dim_sm.SKey_ship_method,
           dim_pm.SKey_product_model,
           dim_sd.SKey_ship_date,
           sod.OrderQty  AS quantity,
           sod.LineTotal AS amount
    INTO #TempShipments
    FROM Sales.SalesOrderHeader soh
             JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
             JOIN Sales.Customer c ON soh.CustomerID = c.CustomerID
             JOIN Sales.SalesTerritory st ON c.TerritoryID = st.TerritoryID
             JOIN Purchasing.ShipMethod sm ON soh.ShipMethodID = sm.ShipMethodID
             JOIN Production.Product p ON sod.ProductID = p.ProductID
             JOIN Production.ProductModel pm ON p.ProductModelID = pm.ProductModelID

             JOIN Lab1_DW.Dim_SalesTerritory dim_st ON st.TerritoryID = dim_st.territory_id
             JOIN Lab1_DW.Dim_ShipMethod dim_sm ON sm.ShipMethodID = dim_sm.ship_method_id
             JOIN Lab1_DW.Dim_ProductModel dim_pm ON pm.ProductModelID = dim_pm.product_model_id
             JOIN Lab1_DW.Dim_ShipDate dim_sd ON FORMAT(soh.ShipDate, 'yyyyMMdd') = dim_sd.SKey_ship_date
    WHERE soh.ShipDate IS NOT NULL;

    WITH AggregatedShipments AS (SELECT SKey_territory,
                                        SKey_ship_method,
                                        SKey_product_model,
                                        SKey_ship_date,
                                        SUM(quantity) AS total_quantity,
                                        SUM(amount)   AS total_amount
                                 FROM #TempShipments
                                 GROUP BY SKey_territory,
                                          SKey_ship_method,
                                          SKey_product_model,
                                          SKey_ship_date)
        MERGE Lab1_DW.Fact_Shipments AS target
    USING AggregatedShipments AS source
    ON (target.SKey_territory = source.SKey_territory
        AND target.SKey_ship_method = source.SKey_ship_method
        AND target.SKey_product_model = source.SKey_product_model
        AND target.SKey_ship_date = source.SKey_ship_date)
    WHEN MATCHED THEN
        UPDATE
        SET target.total_quantity = source.total_quantity,
            target.total_amount   = source.total_amount
    WHEN NOT MATCHED THEN
        INSERT (SKey_territory,
                SKey_ship_method,
                SKey_product_model,
                SKey_ship_date,
                total_quantity,
                total_amount)
        VALUES (source.SKey_territory,
                source.SKey_ship_method,
                source.SKey_product_model,
                source.SKey_ship_date,
                source.total_quantity,
                source.total_amount);


    SET @RowsAffected = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETDATE();

    UPDATE Lab1_DW.ETL_Status
    SET end_time      = @EndTime,
        status        = 'Completed',
        rows_affected = @RowsAffected
    WHERE table_name = 'Fact_Shipments'
      AND start_time = @StartTime;

    DROP TABLE #TempShipments;
END
GO


EXEC ETL_Dim_ProductModel;
EXEC ETL_Dim_ShipMethod;
EXEC ETL_Dim_ShipDate;
EXEC ETL_Fact_Shipments;
