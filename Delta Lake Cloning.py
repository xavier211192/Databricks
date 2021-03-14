# Databricks notebook source
# DBTITLE 1,Create table
# MAGIC %sql
# MAGIC Create database Person;
# MAGIC Drop table if exists Person.Address;
# MAGIC Create table Person.Address (
# MAGIC addressId int,
# MAGIC address string,
# MAGIC customerId int,
# MAGIC startDate string,
# MAGIC endDate string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/FileStore/Clones/Address/'

# COMMAND ----------

# DBTITLE 1,Load sample data
# MAGIC %sql
# MAGIC insert into Person.Address
# MAGIC select 1 as addressId, "1 downing" as address, cast(rand()*10 as integer) as customerId, '2020-11-05' as startDate,null as endDate
# MAGIC union
# MAGIC select 2,"2 downing",cast(rand()*10 as integer), '2020-11-05',null

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from Person.Address

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/Clones/Address/

# COMMAND ----------

# MAGIC %md ### Shallow Clones

# COMMAND ----------

# DBTITLE 1,Create a Shallow clone
# MAGIC %sql
# MAGIC Create or replace table Person.AddressShallow
# MAGIC SHALLOW CLONE Person.Address
# MAGIC LOCATION 'dbfs:/FileStore/Clones/AddressShallow/'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/Clones/AddressShallow/

# COMMAND ----------

# DBTITLE 1,Select on Shallow clone
# MAGIC %sql
# MAGIC Select *,input_file_name() from Person.AddressShallow

# COMMAND ----------

# DBTITLE 1,Update shallow clone
# MAGIC %sql
# MAGIC update Person.AddressShallow
# MAGIC set endDate = '2021-01-01'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/Clones/AddressShallow/

# COMMAND ----------

# MAGIC %sql
# MAGIC Select *,input_file_name() from Person.AddressShallow

# COMMAND ----------

# MAGIC %md ### Deep Clones

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table Person.AddressDeep
# MAGIC DEEP CLONE Person.Address
# MAGIC LOCATION 'dbfs:/FileStore/Clones/AddressDeep/'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/FileStore/Clones/AddressDeep/'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY Person.Address

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY Person.AddressDeep

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from Person.AddressDeep
