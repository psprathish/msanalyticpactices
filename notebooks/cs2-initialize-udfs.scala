// Databricks notebook source
import com.microsoft.crmsystem.udfs.CRMSystemUtils._
import com.microsoft.ordersystem.udfs.OrderSystemUtils._

// COMMAND ----------

spark.udf.register("getCustomerType", getCustomerType _)
spark.udf.register("getCustomerRegion", getCustomerRegion _)
spark.udf.register("getStatus", getStatus _)
spark.udf.register("getOrderAmount", getOrderAmount _)

// COMMAND ----------

