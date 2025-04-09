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
file_location = "/FileStore/tables/pixar_films-1.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

rdd=df.rdd.distinct()
print(row)


# COMMAND ----------

rdd=df.rdd
#for row in rdd.collect():
    #print(row)

for row in rdd.take(5):
    print(row)    

# COMMAND ----------

rdd=df.rdd
rdd_map=rdd.map(lambda row:row[3])
for row in rdd_map.take(10):
    print(row)

# COMMAND ----------

rdd=df.rdd
rdd_filter=rdd.filter(lambda row:row[2].year==2020)
for row in rdd_filter.collect():
    print(row)



# COMMAND ----------

rdd=df.rdd
rdd_filter=rdd.filter(lambda row:row[2].month==6)
for row in rdd_filter.collect():
    print(row)

# COMMAND ----------

rdd=df.rdd
flat_rdd=rdd.flatMap(lambda row:row[1])
flat_rdd.take(10)
#print(flat_rdd)




# COMMAND ----------

rdd=df.rdd
reduce_rdd=rdd.map(lambda row:row[3])
result=reduce_rdd.reduce(lambda x,y:x+y)
print(result)

max_val=reduce_rdd.reduce(lambda x,y:max(x,y))
print(max_val)
min_val=reduce_rdd.reduce(lambda x,y:min(x,y))
print(min_val)

# COMMAND ----------

rdd=df.rdd
rdd1=rdd.map(lambda row:(row[1],row[2]))
reduce_rdd=rdd1.reduceByKey(lambda x,y:max(x,y))
for film,date in reduce_rdd.collect():
      print(film,date)

# COMMAND ----------

rdd=df.rdd
map_rdd=rdd.map(lambda row:(row[1],row[4]))
                
groupby_rdd=map_rdd.groupByKey()
for key,value in groupby_rdd.collect():
    print(key,list(value))               

# COMMAND ----------

rdd=df.rdd
rdd_map=rdd.map(lambda row:(row[1],(row[3],row[4])))
rdd_reduce=rdd_map.reduceByKey(lambda x,y : (max(x[0],y[0]),x[1] if x[0] >= y[0] else y[1]))
for film, (max_run_time, rating) in rdd_reduce.take(10):
    print(film, min_run_time, rating)
    

# COMMAND ----------

rdd_map = rdd.map(lambda row: (row[1], (row[3], row[4])))  


rdd_reduce = rdd_map.reduceByKey(lambda x, y: (min(x[0], y[0]), x[1] if x[0] <= y[0] else y[1]))


for film, (min_run_time, rating) in rdd_reduce.take(5):  
    print(film, min_run_time, rating)

# COMMAND ----------

from pyspark.sql import Row
data = [
    Row(number=1, film="Toy Story", release_date="1995-11-22", run_time=81, film_rating="G"),
    Row(number=2, film="A Bug's Life", release_date="1998-11-25", run_time=95, film_rating="G"),
    
]
rdd=sc.parallelize(data)
rdd1=rdd.mapPartitions(lambda partition:[(row.film,row.run_time*2,row.film_rating) for row in partition])
print(rdd1.collect())


# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import year

df_with_year = df.withColumn("year", year("release_date"))

pivot_df = df_with_year.groupBy("year") \
    .pivot("film_rating") \
    .count()

pivot_df.show()

unpivot_df = pivot_df.selectExpr("year",
    "stack(2, 'G', G, 'PG', PG) as (film_rating, count)"
).where("count is not null")

unpivot_df.show()

# COMMAND ----------

