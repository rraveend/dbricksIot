// Databricks notebook source
val storage_account_name = "aothistorianmigrationv2"
val storage_account_access_key = "kzpDkTrQVA0ZP6GpXxfR8PwszjCoq0V2K/NLIF8Q2q/iIWjwedtL1EAEQwO43PTf6rdVx390Sr/AaW/xg7z3nA=="
val file_location = "wasbs://config@aothistorianmigrationv2.blob.core.windows.net/mappingdata.csv"
val file_type = "csv"
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)
val df = spark.read.format(file_type).option("inferSchema", "true").option("header","true").load(file_location)
df.createOrReplaceTempView("tmpMappingConfigData")
spark.sql("CREATE DATABASE IF NOT EXISTS DataMigration")
spark.sql("USE DataMigration")


// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tmpMappingConfigData

// COMMAND ----------

