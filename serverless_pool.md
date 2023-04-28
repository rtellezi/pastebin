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
