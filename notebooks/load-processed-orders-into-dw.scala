// Databricks notebook source

val blobStorage = "dfetldemoday3.blob.core.windows.net"
val blobContainer = "polybase"
val blobAccessKey = "HTShh4OE8ey18GeNuKHw3NwMCihHcxRUXb8+FPPoCzmuYdSSTxtAbFloEobcfm6dzGTk/+f2PdCC4xhhxvKafw=="
val tempDirectory = "wasbs://" + blobContainer + "@" + blobStorage + "/tempDirs"
val accountInfo = "fs.azure.account.key." + blobStorage

// COMMAND ----------

sc.hadoopConfiguration.set(accountInfo, blobAccessKey)

// COMMAND ----------

val dwDatabase = "prepthsynapse"
val dwServer = "preputhsynapseworkspace.sql.azuresynapse.net"
val dwUser = "sqladminuser"
val dwPassword = "xxxxx"
val dwJdbcPort = "1433"
val dwUrl = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser + ";password=" + dwPassword

// COMMAND ----------

spark.conf.set("spark.sql.parquet.writeLegacyFormat","true")

// COMMAND ----------

val statement = """
  SELECT * FROM CaseStudyDB.OrderHistory
"""
val processedOrdersHistory = spark.sql(statement)

// COMMAND ----------

processedOrdersHistory
  .write
  .format("com.databricks.spark.sqldw")
  .option("url", dwUrl)
  .option("dbtable", "OrderHistoryDW")
  .option("forward_spark_azure_storage_credentials", "true")
  .option("tempdir", tempDirectory)
  .mode("overwrite")
  .save()