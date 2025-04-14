-- create database Zagaduvaci;
-- use Zagaduvaci;

-- dim -> zagaduvac, inspector, date
-- monthly report (vkupen broj prijavi, vkupen broj zapisnici,
--                  broj na razlicni prekrsoci, vkupen iznos od prekrsoci
--                  prosecen broj na prekrsoci po zapisnik i prosecen iznos po zapisnik)

-- PART 1 - DEFINING THE TABLES

-- create schema DW;
CREATE TABLE DW.Dim_Polluter
(
    SKey_polluter     INT IDENTITY (1,1) PRIMARY KEY,
    id                INT NOT NULL,
    UTN               NVARCHAR(50), -- unique tax number
    name              NVARCHAR(50),
    address           NVARCHAR(128),
    registration_date DATETIME,
    ETL_LOAD_TIME     DATETIME DEFAULT GETUTCDATE()
);

CREATE TABLE DW.Dim_Inspector
(
    SKey_inspector INT IDENTITY (1,1) PRIMARY KEY,
    id             INT NOT NULL,
    full_name      NVARCHAR(128),
    SSN            NVARCHAR(128),
    address        NVARCHAR(128),
    specialty      NVARCHAR(128),
    ETL_LOAD_TIME  DATETIME DEFAULT GETUTCDATE()
);

CREATE TABLE DW.Dim_Date
(
    SKey_date  INT PRIMARY KEY, -- format (yyyyMMdd)
    date_value DATE,
    year       INT,
    quarter    INT,
    month      INT
);

CREATE TABLE DW.Fact_Report
(
    SKey_polluter                 INT NOT NULL,
    SKey_inspector                INT NOT NULL,
    SKey_date                     INT NOT NULL,
    num_reports                   INT,
    num_proceedings               INT,
    num_distinct_violations       INT,
    total_sum_violations          DECIMAL,
    avg_violations_per_proceeding DECIMAL,
    avg_sum_per_proceeding        DECIMAL,
    ETL_LOAD_TIME                 DATETIME DEFAULT GETUTCDATE(),

    FOREIGN KEY (SKey_polluter) REFERENCES DW.Dim_Polluter (SKey_polluter),
    FOREIGN KEY (SKey_inspector) REFERENCES DW.Dim_Inspector (SKey_inspector),
    FOREIGN KEY (SKey_date) REFERENCES DW.Dim_Date (SKey_date)
);

CREATE TABLE DW.ETL_Status
(
    id            INT IDENTITY (1, 1) PRIMARY KEY,
    table_name    NVARCHAR(50),
    status        NVARCHAR(50) CHECK (status IN ('Running', 'Complete', 'Failed')),
    start_time    DATETIME,
    end_time      DATETIME,
    rows_affected INT,
    last_modified DATETIME
);

-- ETL
CREATE OR ALTER PROCEDURE ETL_Dim_Polluter
AS
BEGIN
    DECLARE @StartTime DATETIME = GETUTCDATE();
    DECLARE @RowsAffected INT;
    INSERT INTO dw.ETL_Status (table_name, start_time, status)
    VALUES ('Dim_Polluter', @StartTime, 'Running');


    WITH Source_Polluter AS
        (SELECT Id,
                EDB,
                Naziv,
                Adresa,
                DatumRegistracija
         FROM Zagaduvac)
        MERGE DW.Dim_Polluter AS target
    USING Source_Polluter AS source
    ON target.id = source.Id
    WHEN MATCHED THEN
        UPDATE
        SET target.UTN               = source.EDB,
            target.name              = source.Naziv,
            target.address           = source.Adresa,
            target.registration_date = source.DatumRegistracija
    WHEN NOT MATCHED THEN
        INSERT (id, UTN, name, address, registration_date)
        VALUES (source.Id, source.EDB, source.Naziv, source.Adresa, source.DatumRegistracija);

    SET @RowsAffected = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETUTCDATE();

    UPDATE dw.ETL_Status
    SET end_time      = @EndTime,
        status        = 'Complete',
        rows_affected = @RowsAffected,
        last_modified = @EndTime
    WHERE table_name = 'Dim_Polluter'
      AND start_time = @StartTime;
END;
GO

CREATE OR ALTER PROCEDURE ETL_Dim_Inspector
AS
BEGIN
    DECLARE @StartTime DATETIME = GETUTCDATE();
    DECLARE @RowsAffected INT;
    INSERT INTO dw.ETL_Status (table_name, start_time, status)
    VALUES ('Dim_Inspector', @StartTime, 'Running');

    WITH Source_Inspectors AS
        (
        SELECT i.Id,
               v.ImePrezime,
               v.EMBG,
               v.Adresa,
               i.Specijalnost
        FROM Inspektor AS i
                 JOIN dbo.Vraboten v ON i.Id = v.Id
        )
        MERGE DW.Dim_Inspector AS target
    USING Source_Inspectors AS source
    ON (target.id = source.Id)
    WHEN MATCHED THEN
        UPDATE
        SET target.full_name = source.ImePrezime,
            target.SSN       = source.EMBG,
            target.address   = source.Adresa,
            target.specialty = source.Specijalnost
    WHEN NOT MATCHED THEN
        INSERT (id, full_name, SSN, address, specialty)
        VALUES (source.Id, source.ImePrezime, source.EMBG, source.Adresa, source.Specijalnost);


    SET @RowsAffected = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETUTCDATE();

    UPDATE dw.ETL_Status
    SET end_time      = @EndTime,
        status        = 'Complete',
        rows_affected = @RowsAffected,
        last_modified = @EndTime
    WHERE table_name = 'Dim_Polluter'
      AND start_time = @StartTime;
END;
GO

CREATE OR ALTER PROCEDURE ETL_Dim_Date
AS
BEGIN
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @RowsAffected INT = 0;

    INSERT INTO DW.ETL_Status (table_name, start_time, status, last_modified)
    VALUES ('Dim_Date', @StartTime, 'Running', @StartTime);

    DECLARE @StartDate DATE;
    DECLARE @EndDate DATE;
    SELECT @StartDate = MIN(Datum), @EndDate = MAX(Datum) FROM Zapisnik;
    DECLARE @CurrentDate DATE = @StartDate;

    WHILE @CurrentDate <= @EndDate
        BEGIN
            DECLARE @SKey_date INT = FORMAT(@CurrentDate, 'yyyyMMdd');
            DECLARE @Quarter INT = DATEPART(QUARTER, @CurrentDate);

            MERGE DW.Dim_Date AS target
            USING (SELECT @SKey_date          AS SKey_date,
                          @CurrentDate        AS date_value,
                          YEAR(@CurrentDate)  AS year,
                          @Quarter            AS quarter,
                          MONTH(@CurrentDate) AS month) AS source
            ON (target.SKey_date = source.SKey_date)
            WHEN MATCHED THEN
                UPDATE
                SET target.date_value = @CurrentDate,
                    target.year       = YEAR(@CurrentDate),
                    target.quarter    = @Quarter,
                    target.month      = MONTH(@CurrentDate)
            WHEN NOT MATCHED THEN
                INSERT (SKey_date, date_value, year, quarter, month)
                VALUES (@SKey_date, @CurrentDate, YEAR(@CurrentDate), @Quarter, MONTH(@CurrentDate));

            SET @RowsAffected = ISNULL(@RowsAffected, 0) + @@ROWCOUNT;
            SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
        END;


    DECLARE @EndTime DATETIME = GETDATE();
    UPDATE DW.ETL_Status
    SET end_time      = @EndTime,
        status        = 'Complete',
        rows_affected = @RowsAffected,
        last_modified = @EndTime
    WHERE table_name = 'Dim_Date'
      AND start_time = @StartTime;
END;
GO

EXEC ETL_Dim_Polluter;
EXEC ETL_Dim_Inspector;
EXEC ETL_Dim_Date;

CREATE OR ALTER PROCEDURE ETL_Fact_Report
AS
BEGIN
    DECLARE @StartTime DATETIME = GETUTCDATE();
    DECLARE @RowsAffected INT;
    INSERT INTO DW.ETL_Status (table_name, start_time, status, last_modified)
    VALUES ('Fact_Report', @StartTime, 'Running', @StartTime);

    -- vkupno prijavi, vkupno zapisnici, br raz prekr, vkupno iznos prekr, pros broj prekr po zapisnik i pros iznos po zapis

    SELECT z.Id                        AS ZapisnikId,
           pz.PrekrsokId,
           z.PrijavaId,
           p.Iznos                     AS iznosZapisnik,
           z.InspektorId,
           z.ZagaduvacId,
           FORMAT(z.Datum, 'yyyyMMdd') AS SKey_date
    INTO #TmpData
    FROM Zapisnik z
             JOIN Prekrsok_Zapisnik pz ON z.Id = pz.ZapisnikId
             JOIN Prekrsok p ON pz.PrekrsokId = p.Id;

    WITH Averages AS (SELECT ZagaduvacId,
                             InspektorId,
                             ZapisnikId,
                             SKey_date,
                             COUNT(PrekrsokId)  AS num_violations,
                             SUM(iznosZapisnik) AS sum_amount
                      FROM #TmpData
                      GROUP BY ZagaduvacId, InspektorId, SKey_date, ZapisnikId),
        Aggregates AS (SELECT dim_p.SKey_polluter,
                              dim_i.SKey_inspector,
                              tmp.SKey_date,
                              COUNT(DISTINCT tmp.PrijavaId)  AS num_reports,
                              COUNT(DISTINCT tmp.ZapisnikId) AS num_proceedings,
                              COUNT(DISTINCT tmp.PrekrsokId) AS num_distinct_violations,
                              SUM(tmp.iznosZapisnik)         AS total_sum_violations,
                              AVG(a.num_violations)          AS avg_violations_per_proceeding,
                              AVG(a.sum_amount)              AS avg_sum_per_proceeding
                       FROM #TmpData tmp
                                JOIN Averages a
                                     ON tmp.ZagaduvacId = a.ZagaduvacId AND tmp.InspektorId = a.InspektorId AND
                                        tmp.ZapisnikId = a.ZapisnikId AND tmp.SKey_date = a.SKey_date
                                JOIN DW.Dim_Polluter dim_p ON tmp.ZagaduvacId = dim_p.id
                                JOIN DW.Dim_Inspector dim_i ON tmp.InspektorId = dim_i.id
                       GROUP BY dim_p.SKey_polluter, dim_i.SKey_inspector, tmp.SKey_date)
        MERGE DW.Fact_Report AS target
    USING Aggregates AS source
    ON (target.SKey_polluter = source.SKey_polluter
        AND target.SKey_inspector = source.SKey_inspector
        AND target.SKey_date = source.SKey_date)
    WHEN MATCHED THEN
        UPDATE
        SET target.num_reports                   = source.num_reports,
            target.num_proceedings               = source.num_proceedings,
            target.num_distinct_violations       = source.num_distinct_violations,
            target.total_sum_violations          = source.total_sum_violations,
            target.avg_violations_per_proceeding = source.avg_violations_per_proceeding,
            target.avg_sum_per_proceeding        = source.avg_sum_per_proceeding
    WHEN NOT MATCHED THEN
        INSERT (SKey_polluter,
                SKey_inspector,
                SKey_date,
                num_reports,
                num_proceedings,
                num_distinct_violations,
                total_sum_violations,
                avg_violations_per_proceeding,
                avg_sum_per_proceeding)
        VALUES (source.SKey_polluter,
                source.SKey_inspector,
                source.SKey_date,
                source.num_reports,
                source.num_proceedings,
                source.num_distinct_violations,
                source.total_sum_violations,
                source.avg_violations_per_proceeding,
                source.avg_sum_per_proceeding);


    SET @RowsAffected = @@ROWCOUNT;
    DECLARE @EndTime DATETIME = GETUTCDATE();
    UPDATE DW.ETL_Status
    SET end_time      = @EndTime,
        status        = 'Complete',
        rows_affected = @RowsAffected,
        last_modified = @EndTime
    WHERE table_name = 'Fact_Report'
      AND start_time = @StartTime;

    DROP TABLE IF EXISTS #TmpData;
END;
GO

EXEC ETL_Fact_Report;