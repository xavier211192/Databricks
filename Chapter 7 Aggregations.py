# Databricks notebook source
#Load data
df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("dbfs:/FileStore/shared_uploads/xavier211192@gmail.com/data/retail-data/all/online_retail_dataset.csv").coalesce(5)
df.cache()
df.createOrReplaceTempView('dfTable')

# COMMAND ----------

# MAGIC %md ## Basic aggregations ##

# COMMAND ----------

# DBTITLE 1,Count
##Basic Aggregation
#count #df.count() is an action df.select(count) is a transformation
from pyspark.sql.functions import count,countDistinct,approx_count_distinct
df.select(count("StockCode")).show()

#count distinct
df.select(countDistinct("StockCode")).show()

#count approx distinct
df.select(approx_count_distinct("StockCode",0.05)).show()

# COMMAND ----------

# DBTITLE 1,First,last,min,max
from pyspark.sql.functions import first,last,min,max
#first and last
df.select(first("StockCode"),last("StockCode")).show()
#min and max
df.select(min("Quantity"),max("Quantity")).show()

# COMMAND ----------

# DBTITLE 1,Sum 
from pyspark.sql.functions import sum,sumDistinct
#sum
df.select(sum("Quantity")).show()
#sum distinct
df.select(sumDistinct("Quantity")).show()

# COMMAND ----------

# DBTITLE 1,Avg and Mean
from pyspark.sql.functions import sum, count, avg, expr
#avg and mean are the same: syntax is different.
df.select(
    count("Quantity").alias("total"),
    sum("Quantity").alias("sum"),
    avg("Quantity").alias("avg"),
    expr("mean(Quantity)").alias("mean"))\
    .selectExpr(
    "total/sum","avg","mean").show()

# COMMAND ----------

# DBTITLE 1,Variance and Std Dev
#sample and population variance and std deviation
from pyspark.sql.functions import var_pop,stddev_pop
from pyspark.sql.functions import var_samp,stddev_samp

df.select(var_pop("Quantity"),var_samp("Quantity"),
         stddev_pop("Quantity"),stddev_samp("Quantity")).show()

# COMMAND ----------

# DBTITLE 1,Skeweness and kurtosis
from pyspark.sql.functions import skewness, kurtosis
df.select(skewness("Quantity"),kurtosis("Quantity")).show()

# COMMAND ----------

# DBTITLE 1,Covariance and Correlation
from pyspark.sql.functions import corr, covar_pop, covar_samp
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo","Quantity"),
         covar_pop("InvoiceNo", "Quantity")).show()

# COMMAND ----------

# DBTITLE 1,Aggregation to Complex data types
from pyspark.sql.functions import collect_list,collect_set
#All values
df.select(collect_list("Country")).show()
#unique values
df.select(collect_set("Country")).show()

# COMMAND ----------

# MAGIC %md ## Groupings ##

# COMMAND ----------

# DBTITLE 1,Simple grouping
#Simple grouping
df.groupBy("InvoiceNo","CustomerId").count().show()

# COMMAND ----------

# DBTITLE 1,Grouping with expressions
#Grouping with Expressions
from pyspark.sql.functions import count
df.groupBy("InvoiceNo").agg(count("Quantity").alias("cnt_quantity"), expr("count(Quantity)")).show()

# COMMAND ----------

# DBTITLE 1,Grouping with maps
df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)")).show()

# COMMAND ----------

# MAGIC %md ## Window functions ##

# COMMAND ----------

#Add a new date column
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
from pyspark.sql.functions import col, to_date, desc, max, dense_rank
from pyspark.sql.window import Window
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))

#1 define a window - use all preceding rows up and until current row
windowSpec = Window\
            .partitionBy("CustomerID","date")\
            .orderBy(desc("Quantity"))\
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
#2 define aggregation maxPurchaseQuantity
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
print(maxPurchaseQuantity)

#2.1 purchaseRank
purchaseDenseRank = dense_rank().over(windowSpec)
print(purchaseDenseRank)

# COMMAND ----------

# 3 Perform a Select 
dfWithDate.where("CustomerId IS NOT NULL AND ").orderBy("CustomerId")\
.select(
col("CustomerId"),
col("date"),
col("Quantity"),
purchaseDenseRank.alias("quantityDenseRank"),
maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()


# COMMAND ----------

# 1 define window spec
windowSpec = Window.partitionBy("CustomerId","date").orderBy("Quantity").rowsBetween(Window.unboundedPreceding, Window.currentRow)
# 2 rank and min purchase quantity
minPurchaseQuantity = min("Quantity").over(windowSpec)
reverseRank = dense_rank().over(windowSpec)

dfWithDate.where("CustomerId is NOT NULL").\
                 select(
                   col("CustomerId"),
                   col("date"),
                   col("Quantity"),
                   reverseRank.alias("reverseRank"),
                   minPurchaseQuantity.alias("minPurchase")
                 ).orderBy("CustomerId").show()

# COMMAND ----------

# MAGIC %md ### Grouping Sets ###

# COMMAND ----------

#drop nulls
dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView('dfNoNull')

# COMMAND ----------

from pyspark.sql.functions import  sum
dfNoNull.groupBy("CustomerId","StockCode").sum("Quantity").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC --#Total quantity of all stock codes and customers
# MAGIC Select CustomerId,StockCode, SUM(QUANTITY) from dfNoNull
# MAGIC group by CustomerId,StockCode order by CustomerID desc, StockCode desc

# COMMAND ----------

# MAGIC %sql
# MAGIC --Using Grouping sets achieve the same 
# MAGIC Select CustomerId,StockCode, SUM(QUANTITY) from dfNoNull
# MAGIC group by CustomerId,StockCode GROUPING SETS((customerId,stockCode))
# MAGIC order by CustomerID desc, StockCode desc

# COMMAND ----------

# DBTITLE 1,Rollups
#Rollup over date and country
rolledUpDF = dfNoNull.rollup("Date","Country").sum("Quantity").orderBy("Date")

# COMMAND ----------

display(rolledUpDF)
#where both are nulls is the grandtotal

# COMMAND ----------

rolledUpDF.where("Country is NULL").show()

# COMMAND ----------

#where one of the column is null is subtotal
rolledUpDF.where("Country is NULL").show()
rolledUpDF.where("Date is NULL").show()

# COMMAND ----------

# DBTITLE 1,Cube
#Cube takes the rollup to a level deeper.
dfNoNull.cube("Date","Country").sum("Quantity").orderBy("Date").show()

# COMMAND ----------

# DBTITLE 1,Grouping Metadata
from pyspark.sql.functions import grouping_id,sum,desc
dfNoNull.cube("CustomerId","StockCode")\
        .agg(grouping_id(), sum(col("Quantity")))\
        .orderBy(desc("grouping_id()")).show()

# COMMAND ----------

# DBTITLE 1,Pivot
pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
display(pivoted)

# COMMAND ----------

from pyspark.sql.functions import collect_set,collect_list
from pyspark.sql.window import Window

window = Window\
                   .partitionBy("date","Country")\
                   .orderBy(desc("Quantity"))\
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow)

max_q = max(col("Quantity")).over(window)

wind_df = dfWithDate.select(col("Date"),col("Country"),col("Quantity"),max_q.alias("max_q"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select Date,Country, sum(Quantity) from dfNoNull
# MAGIC group by Date,Country grouping sets(Date,Country,())
# MAGIC order by Date
