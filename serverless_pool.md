### Leer CSV sin headers
```sql
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/csv/',
        FORMAT = 'CSV',
        PARSER_VERSION='2.0'
    )
    WITH (
        SalesOrderNumber VARCHAR(10) COLLATE Latin1_General_100_BIN2_UTF8,
        SalesOrderLineNumber INT,
        OrderDate DATE,
        CustomerName VARCHAR(25) COLLATE Latin1_General_100_BIN2_UTF8,
        EmailAddress VARCHAR(50) COLLATE Latin1_General_100_BIN2_UTF8,
        Item VARCHAR(30) COLLATE Latin1_General_100_BIN2_UTF8,
        Quantity INT,
        UnitPrice DECIMAL(18,2),
        TaxAmount DECIMAL (18,2)
    ) AS [result]
```

### Filtrar path
```sql
SELECT 
  YEAR(OrderDate) AS OrderYear,       
  COUNT(*) AS OrderedItems
FROM    
  OPENROWSET(        
    BULK 'https://datalakegiqz07b.dfs.core.windows.net/files/sales/parquet/year=*/',        
    FORMAT = 'PARQUET'    
  ) AS [result]
WHERE [result].filepath(1) IN ('2019', '2020')
GROUP BY YEAR(OrderDate)
ORDER BY OrderYear
```

### Leer datos JSON
```sql
SELECT JSON_VALUE(Doc, '$.SalesOrderNumber') AS OrderNumber,
       JSON_VALUE(Doc, '$.CustomerName') AS Customer,
       Doc
FROM
    OPENROWSET(
        BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/json/',
        FORMAT = 'CSV',
        FIELDTERMINATOR ='0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0b'
    ) WITH (Doc NVARCHAR(MAX)) as rows
```

### Crear BD con external data source
```sql
CREATE DATABASE Sales  
    COLLATE Latin1_General_100_BIN2_UTF8;
GO;

Use Sales;
GO;

CREATE EXTERNAL DATA SOURCE sales_data WITH (    
    LOCATION = 'https://datalakegiqz07b.dfs.core.windows.net/files/sales/');
GO;

SELECT *
FROM      
    OPENROWSET(        
        BULK 'parquet/year=*/*.snappy.parquet',        
        DATA_SOURCE = 'sales_data',        
        FORMAT='PARQUET'    
    ) AS orders
WHERE orders.filepath(1) = '2019';

CREATE EXTERNAL FILE FORMAT CsvFormat
    WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS(
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"'
        )
    );
GO;

CREATE EXTERNAL TABLE dbo.orders
(
    SalesOrderNumber VARCHAR(10),
    SalesOrderLineNumber INT,
    OrderDate DATE,
    CustomerName VARCHAR(25),
    EmailAddress VARCHAR(50),
    Item VARCHAR(30),
    Quantity INT,
    UnitPrice DECIMAL(18,2),
    TaxAmount DECIMAL (18,2)
)
WITH
(
    DATA_SOURCE =sales_data,
    LOCATION = 'csv/*.csv',
    FILE_FORMAT = CsvFormat
);
GO
```

### Create external destination. CETAS Transformation
```sql
-- Database for sales data
CREATE DATABASE Sales
  COLLATE Latin1_General_100_BIN2_UTF8;
GO;

Use Sales;
GO;

-- External data is in the Files container in the data lake
CREATE EXTERNAL DATA SOURCE sales_data WITH (
    LOCATION = 'https://datalakexxxxxxx.dfs.core.windows.net/files/'
);
GO;

-- Format for table files
CREATE EXTERNAL FILE FORMAT ParquetFormat
    WITH (
            FORMAT_TYPE = PARQUET,
            DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
        );
GO;

USE Sales;
GO;

SELECT Item AS Product,
       SUM(Quantity) AS ItemsSold,
       ROUND(SUM(UnitPrice) - SUM(TaxAmount), 2) AS NetRevenue
FROM
    OPENROWSET(
        BULK 'sales/csv/*.csv',
        DATA_SOURCE = 'sales_data',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS orders
GROUP BY Item;

-- Save to external file
CREATE EXTERNAL TABLE ProductSalesTotals
    WITH (
        LOCATION = 'sales/productsales/',
        DATA_SOURCE = sales_data,
        FILE_FORMAT = ParquetFormat
    )
AS
SELECT Item AS Product,
    SUM(Quantity) AS ItemsSold,
    ROUND(SUM(UnitPrice) - SUM(TaxAmount), 2) AS NetRevenue
FROM
    OPENROWSET(
        BULK 'sales/csv/*.csv',
        DATA_SOURCE = 'sales_data',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS orders
GROUP BY Item;
```

### Encapsulate data transformation in a stored procedure
```sql
USE Sales;
GO;
CREATE PROCEDURE sp_GetYearlySales
AS
BEGIN
    -- drop existing table
    IF EXISTS (
            SELECT * FROM sys.external_tables
            WHERE name = 'YearlySalesTotals'
        )
        DROP EXTERNAL TABLE YearlySalesTotals
    -- create external table
    CREATE EXTERNAL TABLE YearlySalesTotals
    WITH (
            LOCATION = 'sales/yearlysales/',
            DATA_SOURCE = sales_data,
            FILE_FORMAT = ParquetFormat
        )
    AS
    SELECT YEAR(OrderDate) AS CalendarYear,
            SUM(Quantity) AS ItemsSold,
            ROUND(SUM(UnitPrice) - SUM(TaxAmount), 2) AS NetRevenue
    FROM
        OPENROWSET(
            BULK 'sales/csv/*.csv',
            DATA_SOURCE = 'sales_data',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0',
            HEADER_ROW = TRUE
        ) AS orders
    GROUP BY YEAR(OrderDate)
END
```
