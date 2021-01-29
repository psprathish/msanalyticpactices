-- Databricks notebook source
-- MAGIC %run
-- MAGIC 
-- MAGIC /Users/psprathish@live.com/cs2-initialize-views

-- COMMAND ----------

REFRESH TABLE CaseStudyDB.FactSales

-- COMMAND ----------

SELECT * FROM CaseStudyDB.FactSales

-- COMMAND ----------

REFRESH TABLE ProcessedOrdersView

-- COMMAND ----------

USE CaseStudyDB;

-- COMMAND ----------

INSERT INTO CaseStudyDB.OrderHistory
  SELECT FS.SaleID, TO_DATE(T.PK_Date, 'yyyy-mm-dd') AS SaleDate, YEAR(TO_DATE(T.PK_Date, 'yyyy-mm-dd')) AS OrderYear,
    FS.Units AS TotalUnits, FS.SaleAmount AS TotalSalesAmount,
    CONCAT(C.FName, ' ', C.MName, ' ', C.LName) AS CustomerFullName,
    L.City AS CustomerCity, L.State AS CustomerState, L.Country AS CustomerCountry,
    getCustomerRegion(L.City) AS CustomerRegion, 
    getCustomerType(C.CreditLimit) AS CustomerType,
    getStatus(C.ActiveStatus) AS CustomerStatus,
    CONCAT(E.FName, ' ', E.MName, ' ', E.LName) AS EmployeeFullName,
    S.StoreName,
    P.Name AS Product, PS.ProductSubcategoryName AS Subcategory,
    PC.ProductCategoryName AS Category,
    P.Color AS ProductColor, P.Size AS ProductSize,
    SR.SalesReasonName AS Reason,
    SR.SalesReasonType AS ReasonType
  FROM CaseStudyDB.FactSales AS FS
  JOIN CaseStudyDB.Time AS T ON T.DateID = FS.SaleDate
  JOIN CaseStudyDB.Customers AS C ON C.CustomerID = FS.CustomerID
  JOIN CaseStudyDB.Locations AS L ON L.LocationID = C.LocationID
  JOIN CaseStudyDB.Stores AS S ON S.StoreID = FS.StoreID
  JOIN CaseStudyDB.SalesReasons AS SR ON SR.SalesReasonID = FS.SalesReasonID
  JOIN CaseStudyDB.Employees AS E ON E.EmployeeID = FS.EmployeeID
  JOIN CaseStudyDB.Products AS P ON P.ProductID = FS.ProductID
  JOIN CaseStudyDB.ProductSubcategories AS PS ON PS.ProductSubcategoryID = P.ProductSubcategoryID
  JOIN CaseStudyDB.ProductCategories AS PC ON PC.ProductCategoryID = PS.ProductCategoryID

-- COMMAND ----------

DESCRIBE CaseStudyDB.OrderHistory

-- COMMAND ----------

DESCRIBE EXTENDED CaseStudyDB.OrderHistory

-- COMMAND ----------

DESCRIBE EXTENDED CaseStudyDB.OrderHistory
PARTITION (OrderYear=2006)

-- COMMAND ----------

ANALYZE TABLE CaseStudyDB.OrderHistory
COMPUTE STATISTICS

-- COMMAND ----------

DESCRIBE DETAIL CaseStudyDB.OrderHistory

-- COMMAND ----------

DESCRIBE HISTORY CaseStudyDB.OrderHistory

-- COMMAND ----------

SELECT COUNT(*) FROM CaseStudyDB.OrderHistory@v1

-- COMMAND ----------

SELECT COUNT(*) FROM CaseStudyDB.OrderHistory@20210129T1317070000000

-- COMMAND ----------

SELECT COUNT(*) FROM CaseStudyDB.OrderHistory
TIMESTAMP AS OF "2021-01-29 13:01:28"

-- COMMAND ----------

OPTIMIZE CaseStudyDB.OrderHistory

-- COMMAND ----------

DESCRIBE HISTORY CaseStudyDB.OrderHistory