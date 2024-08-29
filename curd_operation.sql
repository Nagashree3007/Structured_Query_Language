/*

@Author: Nagashree C R
@Date: 2024-08-11
@Last Modified by: Nagashree C R
@Last Modified: 2024-08-11
@Title : CRUD operations 

*/
use test
--Creating the table
CREATE Table dbo.CustomerMonthlySales (
   RecordID     INT IDENTITY(1,1) NOT NULL,
   CustomerID   INT NOT NULL,
   SalesMonth   DATE NOT NULL,
   SalesTotal   MONEY NOT NULL,
   SalesAverage MONEY NULL  --For later use
);

--insert values in to table

INSERT dbo.CustomerMonthlySales (CustomerID, SalesMonth, SalesTotal)
VALUES (11000, '2011-07-01', 3956.00);  
Select * from dbo.CustomerMonthlySales

--insert multiple values in to table

INSERT dbo.CustomerMonthlySales (CustomerID, SalesMonth, SalesTotal)
VALUES (11000,'2011-08-01',3350.00), 
       (11000,'2011-09-01',2350.00),
       (11000,'2011-10-01',4150.00),
       (11000,'2011-11-01',4350.00);

--Add a temporary column to hold unique IDs

ALTER TABLE dbo.CustomerMonthlySales
ADD TempCustomerID INT;

--Update the TempCustomerID with unique values starting from 13000

WITH NumberedRows AS (
    SELECT 
        RecordID,
        ROW_NUMBER() OVER (ORDER BY RecordID) + 12999 AS NewCustomerID
    FROM dbo.CustomerMonthlySales
)
UPDATE dbo.CustomerMonthlySales
SET TempCustomerID = NumberedRows.NewCustomerID
FROM dbo.CustomerMonthlySales
JOIN NumberedRows ON dbo.CustomerMonthlySales.RecordID = NumberedRows.RecordID;

--Update the CustomerID column with values from TempCustomerID

UPDATE dbo.CustomerMonthlySales
SET CustomerID = TempCustomerID;

--Drop the temporary column

ALTER TABLE dbo.CustomerMonthlySales
DROP COLUMN TempCustomerID;

--fetch all the values

use test;
select top 5 * from dbo.CustomerMonthlySales;