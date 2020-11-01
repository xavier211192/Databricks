# Databricks notebook source
sparkDF = spark.read.csv('/FileStore/tables/2015_summary.csv', header="true", inferSchema="true")


# COMMAND ----------

spark.read.csv('/FileStore/tables/2015_summary.csv', header="true", inferSchema="true").schema

# COMMAND ----------

# MAGIC %md
# MAGIC Out[14]: StructType(List(StructField(DEST_COUNTRY_NAME,StringType,true),StructField(ORIGIN_COUNTRY_NAME,StringType,true),StructField(count,IntegerType,true)))

# COMMAND ----------

# MAGIC %md See all column name using command
# MAGIC sparkDF.columns

# COMMAND ----------

sparkDF.columns

# COMMAND ----------

# MAGIC %md 
# MAGIC Row type object

# COMMAND ----------

from pyspark.sql import Row
myRow = Row("Hello",None,1,False,)
#access the row
myRow[0]

# COMMAND ----------

# MAGIC %md select and selectExpr

# COMMAND ----------

sparkDF.createOrReplaceTempView('dfTable')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select dest_country_name from dfTable limit 2;

# COMMAND ----------

sparkDF.select('dest_country_name').show(2)

# COMMAND ----------

from pyspark.sql.functions import expr, col, column
sparkDF.select(expr('dest_country_name'),col('dest_country_name'),column('dest_country_name')).show(2)

# COMMAND ----------

# MAGIC %md selectExpr

# COMMAND ----------

sparkDF.selectExpr('dest_country_name as newName').show(2)

# COMMAND ----------

sparkDF.selectExpr('dest_country_name','origin_country_name','count','(dest_country_name=origin_country_name) as withinCountry').show(3)

# COMMAND ----------

#literal
from pyspark.sql.functions import lit
sparkDF.select(col('*'),lit(1).alias('Flag')).show()

# COMMAND ----------

#withColumn
sparkDF.withColumn('Flag',lit(1)).show(3)

# COMMAND ----------

sparkDF.withColumn('NewName',expr('dest_country_name')).show(3)

# COMMAND ----------

#withColumnRenamed
sparkDF.withColumnRenamed('dest_country_name','newName').show(2)

# COMMAND ----------

#drop columns
newDF = sparkDF.select('*')

# COMMAND ----------

newDF.drop('dest_country_name','count')

# COMMAND ----------

#cast
sparkDF.withColumn('count2',col('count').cast('long'))

# COMMAND ----------

#filtering rows
sparkDF.filter(col('count')<2).show(2)
sparkDF.where(col('count')<2).show(2)

# COMMAND ----------

sparkDF.where(col('count')<2).where(col('dest_country_name')!='United States').show(2)

# COMMAND ----------

#distinct
sparkDF.select('origin_country_name').distinct().count()

# COMMAND ----------

#random sampling
seed = 5
withReplacement = False
fraction = 0.5
sparkDF.sample(withReplacement,fraction,seed).count()

# COMMAND ----------

#union
from pyspark.sql import Row
schema = sparkDF.schema
newRows = [Row('o1','d1',5),
          Row('o2','d2',7)]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows)

# COMMAND ----------

sparkDF.union(newDF).where(col('count')==7).show()


# COMMAND ----------

#ordering rows
from pyspark.sql.functions import desc,asc
sparkDF.sort('count').show(2)
sparkDF.orderBy(expr('count desc')).show(2)
sparkDF.orderBy(col('count').desc()).show(2)

# COMMAND ----------

#repartition
sparkDF.repartition(5,col('dest_country_name'))

# COMMAND ----------

#collect
collectDF = sparkDF.limit(10)
collectDF.collect()
