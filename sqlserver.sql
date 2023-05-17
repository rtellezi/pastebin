-- CREATE USER AND ROLE
CREATE USER user1 WITH PASSWORD = 'AzPwd31062629!'
ALTER ROLE db_datareader ADD MEMBER user1; -- EXEC sp_addrolemember 'db_datareader', 'user1';