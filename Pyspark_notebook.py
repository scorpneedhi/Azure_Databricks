# Databricks notebook source
df = spark.sql("select * from winequality_red")

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

display(df.describe())

# COMMAND ----------

display(df.groupBy('alcohol').count().orderBy('alcohol'))

# COMMAND ----------

display(df.select('alcohol','fixed_acidity').filter(df.fixed_acidity > 10))

# COMMAND ----------

df.filter(df.fixed_acidity > 10).count()

# COMMAND ----------

from pyspark.sql.functions import stddev, col
display(df.select(stddev(col('density')).alias('stddev_density')))