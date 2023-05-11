DROP CREDENTIAL [cosmosyfn68r51]

CREATE CREDENTIAL [cosmosyfn68r51]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '8gJTYkwOXK8jdSuPjcePQOHZbktfYbbs8TSoWoZZQEiMC1PSyOk49guS9nEQ7QhQycQprgQqvCFTACDb9O50vw=='
GO

IF (NOT EXISTS(SELECT * FROM sys.credentials WHERE name = 'cosmosyfn68r51'))
    THROW 50000, 'As a prerequisite, create a credential with Azure Cosmos DB key in SECRET option:
    CREATE CREDENTIAL [cosmosyfn68r51]
    WITH IDENTITY = ''SHARED ACCESS SIGNATURE'', SECRET = ''<Enter your Azure Cosmos DB key here>''', 0
GO

SELECT TOP 100 *
FROM OPENROWSET(​PROVIDER = 'CosmosDB',
                CONNECTION = 'Account=cosmosyfn68r51;Database=AdventureWorks',
                OBJECT = 'Sales',
                SERVER_CREDENTIAL = 'cosmosyfn68r51'
) AS [Sales]

SELECT *
FROM OPENROWSET(​PROVIDER = 'CosmosDB',
                CONNECTION = 'Account=cosmosyfn68r51;Database=AdventureWorks',
                OBJECT = 'Sales',
                SERVER_CREDENTIAL = 'cosmosyfn68r51'
)
WITH (
    OrderID VARCHAR(10) '$.id',
    OrderDate VARCHAR(10) '$.orderdate',
    CustomerID INTEGER '$.customerid',
    CustomerName VARCHAR(40) '$.customerdetails.customername',
    CustomerEmail VARCHAR(30) '$.customerdetails.customeremail',
    Product VARCHAR(30) '$.product',
    Quantity INTEGER '$.quantity',
    Price FLOAT '$.price'
)
AS sales
ORDER BY OrderID;