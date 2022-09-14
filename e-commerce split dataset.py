# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print('spark session created.')

# COMMAND ----------

df = spark.sql('SELECT * FROM data_1_csv')
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Invoices table

# COMMAND ----------

# just create table for unique invoices
costumer_columns = ["CustomerID","Country","InvoiceDate","InvoiceNo"]
df_invoice = df.select(costumer_columns).drop_duplicates()
display(df_invoice)

# COMMAND ----------

f"n_row={df_invoice.count()}, n_col={len(df_invoice.columns)}"

# COMMAND ----------

# Create table with path using DataFrame's schema and raise error if exist
df_invoice.write.format("delta").saveAsTable("default.invoices")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Item details

# COMMAND ----------

item_columns = ["StockCode","Description","UnitPrice","Quantity","InvoiceNo"]
df_items = df.select(item_columns).drop_duplicates()
display(df_items)

# COMMAND ----------

df_items.write.format("delta").saveAsTable("default.items")

# COMMAND ----------


