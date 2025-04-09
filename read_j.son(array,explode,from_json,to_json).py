# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/sample.json"
file_type = "json"

# CSV options
infer_schema = "TRUE"
first_row_is_header = "TRUE"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import array,array_contains,array_position,array_remove,col,expr
df1=df.select("name",array(col("name")).alias("array_name"))
df1.show()


# COMMAND ----------

from pyspark.sql.functions import expr
df2=df.withColumn("alpha",expr("array_contains(transform(projects, x -> x.project_name), 'Alpha')"))\
      .withColumn("Beta",expr("array_contains(transform(projects, x ->x.project_name),'Beta')"))\
      .withColumn("Gamma",expr("array_contains(transform(projects,x->x.project_name),'Gamma')")) 
df2.show()       

df3=df2.select("projects","alpha","Beta","Gamma")
df3.show(truncate=False)
            
    
  

# COMMAND ----------

from pyspark.sql.functions import size
df_len=df2.withColumn("projects_count",size("projects").alias("proj_len"))
df_len.show()
df_size=df_len.select("projects","projects_count")
df_size.show()

# COMMAND ----------

from pyspark.sql.functions import array_remove,expr
df_remove=df_size.withColumn ("projects_without_alpha",
    expr("filter(projects, x -> x.project_name != 'Alpha')")
)
df1_remove=df_remove.select("projects_without_Alpha","projects").show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import array_position,expr
df_position=df_size.withColumn("Alpha_index",
                               expr("array_position(transform(projects, x -> x.project_name),'Alpha')")
)
df_index=df_position.select("projects","Alpha_index").show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import explode,posexplode,explode_outer,posexplode_outer
df_explode=df_len.withColumn("projects_",explode("projects").alias("project_explode"))
df_explode.show()
#from pyspark.sql.functions import explode,posexplode,explode_outer,posexplode_outer
#df_posex=df_len.withColumn("projects",posexplode("projects").alias("pro_ex"))
#df_posex1 = df_posex.select("projects.pos", "projects.col.hours", "projects.col.project_name")
#df_posex1.show()

                           
                





# COMMAND ----------

from pyspark.sql.functions import to_json



df_json = df.withColumn("projects_json", to_json(col("projects")))
df_json.select("projects_json").show(truncate=False)