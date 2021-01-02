# Databricks notebook source
#create data

person = spark.createDataFrame([
    (0, "Bill Chambers", 0, [100]),
    (1, "Matei Zaharia", 1, [500, 250, 100]),
    (2, "Michael Armbrust", 1, [250, 100])
]).toDF("id","name","graduate_program","spark_status")

graduateProgram = spark.createDataFrame([
(0, "Masters", "School of Information", "UC Berkeley"),
(2, "Masters", "EECS", "UC Berkeley"),
(1, "Ph.D.", "EECS", "UC Berkeley")])\
.toDF("id", "degree", "department", "school")
sparkStatus = spark.createDataFrame([
(500, "Vice President"),
(250, "PMC Member"),
(100, "Contributor")])\
.toDF("id", "status")

person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

# COMMAND ----------

# DBTITLE 1,Inner Joins
joinExpression = person["graduate_program"] == graduateProgram["id"]
person.join(graduateProgram, joinExpression).show()

# COMMAND ----------

# DBTITLE 1,Outer Joins
person.join(graduateProgram, joinExpression, "outer").show()

# COMMAND ----------

# DBTITLE 1,Left Outer
graduateProgram.join(person, joinExpression, "left_outer").show()

# COMMAND ----------

# DBTITLE 1,Right outer
person.join(graduateProgram, joinExpression, "right_outer").show()

# COMMAND ----------

# DBTITLE 1,Semi join
#nothing from right data frame is included, behaves like a filter
graduateProgram.join(person, joinExpression, "left_semi").show()

# COMMAND ----------

# DBTITLE 1,Anti join
#opp of semi joins. Keeps only values that do not exist in the 2nd df.
graduateProgram.join(person,joinExpression, "left_anti").show()

# COMMAND ----------

# DBTITLE 1,Cross Cartesian Join
person.join(graduateProgram, joinExpression, "cross").show()

# COMMAND ----------

# MAGIC %md ### Challenges of Joins ###

# COMMAND ----------

# DBTITLE 1,Join on complex types
from pyspark.sql.functions import expr,col
person.withColumnRenamed("id","personId")\
  .join(sparkStatus, expr("array_contains(spark_status, id)")).show()

# COMMAND ----------

# DBTITLE 1,Handling duplicate column names
gradProgramDupe = graduateProgram.withColumnRenamed("id","graduate_program")
#now person and gradProgramDupe have column with same name
joinExp = person["graduate_program"]==gradProgramDupe["graduate_program"]

## error
#person.join(gradProgramDupe, joinExp).select("graduate_program").show()

#1 Change join Expression from boolean to string
person.join(gradProgramDupe, "graduate_program").select("graduate_program").show()


#2 Drop the column after join
person.join(gradProgramDupe, joinExp).drop(person["graduate_program"]).select("graduate_program").show()

#3 Rename column before join
gradProgram3 = graduateProgram.withColumnRenamed("id","grad_id")
jexp = person["graduate_program"] == gradProgram3["grad_id"]
person.join(gradProgram3, jexp).select("graduate_program").show()

# COMMAND ----------

# DBTITLE 1,Plan explain for Joins
person.join(graduateProgram, joinExpression).explain()

# COMMAND ----------

#hint to the optimizer to use broadcast join
from pyspark.sql.functions import broadcast
person.join(broadcast(graduateProgram), joinExpression).explain()
