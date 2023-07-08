# Databricks notebook source
# Databricks notebook source
!pip install polars

# COMMAND ----------

#Importing polars library
import polars as po

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM titanic_train

# COMMAND ----------

#Using pyarrow to convert pyspark dataframe to arrow dataframe  
import pyarrow as pa
df = po.from_arrow(pa.Table.from_batches(_sqldf._collect_as_arrow()))

# COMMAND ----------

df

# COMMAND ----------

#Count of Passengers survived on the Gender Basis
df1=df.select(['PassengerId','Survived','Name','Sex','Age'])
df1


# COMMAND ----------

# COMMAND ----------

df2=df1.groupby('Sex',maintain_order=True).count()
df2

# COMMAND ----------

# COMMAND ----------

#Mean ages
df3=df1.groupby('Sex',maintain_order=True).mean().select(['Sex','Age'])
df3

# COMMAND ----------

# COMMAND ----------

#Median ages
df4=df1.groupby('Sex',maintain_order=True).mean().select(['Sex','Age'])
df4

