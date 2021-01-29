-- Databricks notebook source
-- MAGIC %run
-- MAGIC 
-- MAGIC /Users/psprathish@live.com/cs2-initialize-volume-mounting

-- COMMAND ----------


CREATE DATABASE IF NOT EXISTS CaseStudyDB;

USE CaseStudyDB;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS CaseStudyDB.Customers
(
  CustomerID INT,
  FName STRING,
  MName STRING,
  LName STRING,
  LocationID INT,
  CreditLimit INT,
  ActiveStatus BOOLEAN
)
USING CSV
OPTIONS
(
  "inferSchema" "false",
  "header" "true",
  "sep" ",",
  "path" "dbfs:/mnt/data/casestudy-2/Customers*.csv"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS CaseStudyDB.FactSales
(
  SaleID INT,
  SaleDate INT,
  CustomerID INT,
  EmployeeID INT,
  StoreID INT,
  ProductID INT,
  Units INT,
  SaleAmount INT,
  SalesReasonID INT,
  ProductCost INT
)
USING CSV
OPTIONS
(
  "inferSchema" "false",
  "header" "true",
  "sep" ",",
  "path" "dbfs:/mnt/data/casestudy-2/FactSales*.csv"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS CaseStudyDB.Employees
(
  EmployeeID INT,
  FName STRING,
  MName STRING,
  LName STRING,
  ManagerID INT
)
USING CSV
OPTIONS
(
  "inferSchema" "false",
  "header" "true",
  "sep" ",",
  "path" "dbfs:/mnt/data/casestudy-2/Employees*.csv"
);


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS CaseStudyDB.Locations
(
  LocationID INT,
  AddressLine1 STRING,
  AddressLine2 STRING,
  City STRING,
  State STRING,
  Country STRING,
  Zip STRING
)
USING CSV
OPTIONS
(
  "inferSchema" "false",
  "header" "true",
  "sep" ",",
  "path" "dbfs:/mnt/data/casestudy-2/Locations*.csv"
);


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS CaseStudyDB.ProductCategories
(
  ProductCategoryID INT,
  ProductCategoryName STRING
)
USING CSV
OPTIONS
(
  "inferSchema" "false",
  "header" "true",
  "sep" ",",
  "path" "dbfs:/mnt/data/casestudy-2/ProductCategories*.csv"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS CaseStudyDB.ProductSubcategories
(
  ProductSubcategoryID INT,
  ProductCategoryID INT,
  ProductSubcategoryName STRING
)
USING CSV
OPTIONS
(
  "inferSchema" "false",
  "header" "true",
  "sep" ",",
  "path" "dbfs:/mnt/data/casestudy-2/ProductSubcategories*.csv"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS CaseStudyDB.Products
(
  ProductID INT,
  Name STRING,
  ProductNumber STRING,
  ProductSubcategoryID INT,
  Color STRING,
  Size STRING,
  StockLevel INT,
  Supplier STRING
)
USING CSV
OPTIONS
(
  "inferSchema" "false",
  "header" "true",
  "sep" ",",
  "path" "dbfs:/mnt/data/casestudy-2/Products*.csv"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS CaseStudyDB.SalesReasons
(
  SalesReasonID INT,
  SalesReasonName STRING,
  SalesReasonType STRING
)
USING CSV
OPTIONS
(
  "inferSchema" "false",
  "header" "true",
  "sep" ",",
  "path" "dbfs:/mnt/data/casestudy-2/SalesReasons*.csv"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS CaseStudyDB.Stores
(
  StoreID INT,
  StoreName STRING,
  StoreManagerID INT,
  LocationID INT
)
USING CSV
OPTIONS
(
  "inferSchema" "false",
  "header" "true",
  "sep" ",",
  "path" "dbfs:/mnt/data/casestudy-2/Stores*.csv"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS CaseStudyDB.Time
(
  DateId INT,
  PK_Date STRING, 
  Date_Name STRING, 
  Year STRING, 
  Year_Name STRING, 
  Half_Year STRING, 
  Half_Year_Name STRING, 
  Quarter STRING, 
  Quarter_Name STRING, 
  Month STRING, 
  Month_Name STRING, 
  Day_Of_Year STRING, 
  Day_Of_Year_Name STRING, 
  Day_Of_Half_Year STRING, 
  Day_Of_Half_Year_Name STRING, 
  Day_Of_Quarter STRING, 
  Day_Of_Quarter_Name STRING, 
  Day_Of_Month STRING, 
  Day_Of_Month_Name STRING, 
  Month_Of_Year STRING, 
  Month_Of_Year_Name STRING, 
  Month_Of_Half_Year STRING, 
  Month_Of_Half_Year_Name STRING, 
  Month_Of_Quarter STRING, 
  Month_Of_Quarter_Name STRING, 
  Quarter_Of_Year STRING, 
  Quarter_Of_Year_Name STRING, 
  Quarter_Of_Half_Year STRING, 
  Quarter_Of_Half_Year_Name STRING, 
  Half_Year_Of_Year STRING, 
  Half_Year_Of_Year_Name STRING
)
USING CSV
OPTIONS
(
  "inferSchema" "false",
  "header" "true",
  "sep" ",",
  "path" "dbfs:/mnt/data/casestudy-2/Time*.csv"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS CaseStudyDB.OrderHistory
(
  SaleID INT,
  SaleDate DATE,
  OrderYear INT,
  TotalUnits INT,
  TotalSalesAmount INT,
  CustomerFullName STRING,
  CustomerCity STRING,
  CustomerState STRING,
  CustomerCountry STRING,
  CustomerRegion STRING,
  CustomerType STRING,
  CustomerStatus STRING,
  EmployeeName STRING,
  StoreName STRING,
  Product STRING,
  Subcategory STRING,
  Category STRING,
  ProductColor STRING,
  ProductSize STRING,
  Reason STRING,
  ReasonType STRING
)
USING DELTA
LOCATION "dbfs:/mnt/data/casestudy-2/orderhistory/"
PARTITIONED BY (OrderYear);

-- COMMAND ----------

CACHE LAZY TABLE CaseStudyDB.Customers;
CACHE LAZY TABLE CaseStudyDB.Products;
CACHE LAZY TABLE CaseStudyDB.ProductSubcategories;
CACHE LAZY TABLE CaseStudyDB.ProductCategories;
CACHE LAZY TABLE CaseStudyDB.Employees;
CACHE LAZY TABLE CaseStudyDB.Stores;
CACHE LAZY TABLE CaseStudyDB.Time;
CACHE LAZY TABLE CaseStudyDB.SalesReasons;
CACHE LAZY TABLE CaseStudyDB.Locations;