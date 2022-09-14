# Databricks notebook source
# DBTITLE 1,Start Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print('Spark session created.')

# COMMAND ----------

# DBTITLE 1,Load the customer and invoice data
invoices = spark.sql('SELECT * FROM invoices')
display(invoices)

# COMMAND ----------

# DBTITLE 1,Check number of the countries
result_df = spark.sql("SELECT COUNT(DISTINCT Country) AS Number_countries FROM default.invoices")
display(result_df)

# COMMAND ----------

# DBTITLE 1,Top 10 countries with most customers (Using SQL & Plotting by display function of Databricks)
resultDF = spark.sql("SELECT Country,\
                    COUNT(DISTINCT CustomerID) as TotalClientNumber\
                    FROM default.invoices\
                    GROUP BY Country\
                    ORDER BY TotalClientNumber DESC\
                    LIMIT 10")
display(resultDF)

# COMMAND ----------

# DBTITLE 1,Top 10 countries with the most costumers (Using pyspark & Displaying with Databricks display function)
from pyspark.sql.functions import countDistinct

data = invoices.groupBy("Country").agg(countDistinct("CustomerID"))
data = data.toPandas()
data = data.rename(columns={'count(CustomerID)':"UniqueCustomerNumber"}).sort_values(by='UniqueCustomerNumber',ascending=False)
data = data.head(10)
display(data)

# COMMAND ----------

# DBTITLE 1,Top 10 countries with the most costumers (Displaying by Matplotlib)
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure

plt.rcParams["figure.figsize"] = (20,3)
data.plot(kind='bar',x='Country',y='UniqueCustomerNumber')

# COMMAND ----------

# DBTITLE 1,Customers Ordering The Most
# CHALLENGE FOR THE STUDENT: REMOVE THE NULL CustomerID with sparkSQL
result_df = spark.sql("SELECT CustomerID,COUNT(DISTINCT InvoiceNo) as TotalOrderNumber\
                      FROM default.invoices\
                      GROUP BY Country, CustomerID\
                      ORDER BY TotalOrderNumber DESC\
                      LIMIT 10")
display(result_df)

# COMMAND ----------

items = spark.sql('SELECT * FROM items')
display(items)

# COMMAND ----------

# DBTITLE 1,Distrubution of Items Per Order
result_df = spark.sql("SELECT StockCode,COUNT(DISTINCT InvoiceNo)\
                      FROM default.items\
                      GROUP BY StockCode")
display(result_df)

# COMMAND ----------

# DBTITLE 1,Most Ordered Items
most_ordered_items_df = spark.sql("SELECT StockCode,Description,SUM(Quantity) AS TotalQuantity\
                                  FROM default.items\
                                  GROUP BY StockCode,Description\
                                  ORDER BY TotalQuantity DESC\
                                  LIMIT 10")
display(most_ordered_items_df)

# COMMAND ----------

# DBTITLE 1,Price Distrubution Per Item
# CHALLENGE FOR THE STUDENTS: WHY ARE THERE NEGATIVE PRICES?
price_df = spark.sql("SELECT UnitPrice\
                      FROM default.items\
                      GROUP BY StockCode,Description,UnitPrice")
price_df.describe().show()


# COMMAND ----------

display(price_df.select('UnitPrice'))

# COMMAND ----------

# DBTITLE 1,Price Distribution Per Item Plotting By Matplotlib
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
price_df_pd = price_df.toPandas()
plt.rcParams["figure.figsize"] = (10,8)
plt.hist(price_df_pd['UnitPrice'],bins=100);

# COMMAND ----------

# MAGIC %md ### Which customers bought a WHILE METAL LANTERN?

# COMMAND ----------

price_df = spark.sql("""
SELECT DISTINCT invoices.CustomerID
FROM default.items
JOIN invoices ON items.InvoiceNo=invoices.InvoiceNo
WHERE items.Description = 'WHITE METAL LANTERN' 
AND invoices.CustomerID IS NOT NULL
""")
price_df.show()


# COMMAND ----------

# MAGIC %md ### Which ITEMS are the most revenue generating per country outside of UK?

# COMMAND ----------

result = spark.sql("""
SELECT items.Description, avg(items.UnitPrice) * sum(items.Quantity) as total_revenue, invoices.Country
FROM default.items
JOIN invoices ON items.InvoiceNo=invoices.InvoiceNo
WHERE invoices.Country != "United Kingdom"
GROUP BY items.Description, invoices.Country
ORDER BY total_revenue desc, invoices.Country, items.Description
""")
display(result)


# COMMAND ----------

## And in UK
# CHALLENGE. Actually the GROUP BY is not needed this time, how would you simplify the query?
result = spark.sql("""
SELECT items.Description, avg(items.UnitPrice) * sum(items.Quantity) as total_revenue, invoices.Country
FROM default.items
JOIN invoices ON items.InvoiceNo=invoices.InvoiceNo
WHERE invoices.Country = "United Kingdom"
GROUP BY items.Description, invoices.Country
ORDER BY total_revenue desc, invoices.Country, items.Description
""")
display(result)

# COMMAND ----------


