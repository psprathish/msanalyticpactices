-- Databricks notebook source
-- MAGIC %run
-- MAGIC 
-- MAGIC /Users/psprathish@live.com/cs2-initialize-schema-and-cache

-- COMMAND ----------

-- MAGIC %run
-- MAGIC 
-- MAGIC /Users/psprathish@live.com/cs2-initialize-udfs

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW ProcessedOrdersView
AS
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

SELECT * FROM ProcessedOrdersView