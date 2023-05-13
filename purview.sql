--- SERVERLESS SQL POOL
CREATE DATABASE lakedb;
GO

USE lakedb;
GO

CREATE LOGIN purviewz9vf3dl FROM EXTERNAL PROVIDER;
GO

CREATE USER purviewz9vf3dl FOR LOGIN purviewz9vf3dl;
GO

ALTER ROLE db_datareader ADD MEMBER purviewz9vf3dl;
GO

--- DEDICATE SQL POOL
CREATE USER purviewz9vf3dl FROM EXTERNAL PROVIDER;
GO

EXEC sp_addrolemember 'db_datareader', purviewz9vf3dl;
GO