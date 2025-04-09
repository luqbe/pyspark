# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("pyspark") \
    .getOrCreate()  
from pyspark.sql import SparkSession    
from pyspark.sql.types import StructType,StructField, IntegerType,StringType ,DateType 
schema=StructType([
        StructField("employee_id" ,IntegerType(),True),
        StructField("name", StringType(),True),
        StructField("salary",IntegerType(),True),
        StructField("email_id",StringType(),True),
        StructField("join_date",StringType(),True)
])

data=(
    [101,"John",45000,"bebe@gmail.com","2023-12-23"],
    [102,"Kevin",35000,"kev@gamil.com","2023-06-15"],
    [103,"sara",42000,"sara@gamil.com","2022-09-24"],
    [104,"Elvin",32000,"elv@outlook.com","2024-08-17"],
    [105,"simba",36000,"sim@outlook.com" ,"2025-02-15"])

df = spark.createDataFrame(data, schema=schema)
from pyspark.sql.functions import to_date,col
df1= df.withColumn("date_of_join",to_date("join_date","yyyy-mm-dd")) 

df1.show() 

df2=df1.withColumn("double_salary",df1["salary"]*2)
df2.show()
df3=df2.drop("join_date")
df3.show()
df4=df3.withColumnRenamed("name","employee_name")
df4.show()
df5=df4.filter(col("double_salary") < 73000)
df5.show()

# COMMAND ----------

from pyspark.sql.functions import avg,count,max,min
df6=df4.groupby("employee_id").agg(avg("salary").alias("avg_salary"))
df6.show()
df7=df6.filter(col("avg_salary")<40000)
df7.show()



# COMMAND ----------

from pyspark.sql.functions import col, lit, length, lower, upper, trim, ltrim, rtrim, concat, substring, instr, regexp_replace, regexp_extract,repeat,length,split
df_string=df4.select(
          col("employee_name"),
          upper(col("employee_name")).alias("upper_name"),
          lower(col("employee_name")).alias("lower_name"),
          substring(col("employee_name"),1,3).alias("substing_name"),
          regexp_extract(col("email_id"),"([a-z]+)@", 1).alias("reg_emailid"),
          instr(col("employee_id"),"e").alias("in_str"),
          ltrim(col("employee_name")).alias("ltrim_name"),
          rtrim(col("employee_name")).alias("rtrim_name"),
          repeat(col("employee_name"),2).alias("rep_name"),
          length(col("email_id")).alias("len_id")
          
)
df_string.show()
df_split=df_string.select(
    col("rep_name"),
    split(col("rep_name"),"\\|").getItem(0).alias("split_name")
)
df_split.show()


# COMMAND ----------

from pyspark.sql import SparkSession
spark= SparkSession.builder \
    .appName("null_values") \
    .getOrCreate() 

from pyspark.sql.types import StructType,StructField, IntegerType,StringType ,DateType 
schema=StructType([
       StructField("employee_id",IntegerType(),True),
       StructField("department",StringType(),True),
       StructField("city",StringType(),True),
       StructField("reporting_manger",StringType(),True)
])    
data=(
     [101,"HR","NY","Kritin"],
     [102,"IT","Kolkata","Michel"],
     [103,"logistics",None,"Ram"],
     [102,"IT","kolkata","Michel"],
     [105,"HR","chennai",None]
)
df_1 = spark.createDataFrame(data, schema=schema)

df_join=df_1.join(df1, df_1["employee_id"]==df1["employee_id"],"inner")

df_join.show()
df_left=df1.join(df_1, df1["employee_id"]==df_1["employee_id"],"left") \
            .select("email_id","name","department")
df_left.show()

df_right=df_1.join(df1,df_1["employee_id"]==df1["employee_id"],"right")\
             .select("name","city","reporting_manger")
             
df_right.show()             


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("name").orderBy(col("reporting_manger").desc())

df_with_rownum = df_right.withColumn("row_num", row_number().over(window_spec))
df_with_rownum.show()
df_latest = df_with_rownum.filter(col("row_num") == 1).drop("row_num")
df_latest.show()

# COMMAND ----------

df_drop=df_1.dropna()
df_drop.show()
df1_drop=df_1.dropna(subset=["reporting_manger","city"])
df1_drop.show()
df2_fill=df_1.fillna({"city":"unknown","reporting_manger":"not_assigned"})
df2_fill.show()

# COMMAND ----------

from pyspark.sql.functions import sum, avg, min, max, round, abs
df_agg=df1.select(max("salary").alias("max_salary"),
           avg("salary").alias ("avg_salary")
           )
df_agg.show()          

# COMMAND ----------

from pyspark.sql.functions import col,min,max
df_group=df2.groupby("name").agg(sum("double_salary").alias("sum_value"),
                                 min("salary").alias("min_values"),
                                 max("double_salary").alias("max_values")
)
df_group.show()

# COMMAND ----------

from pyspark.sql.functions import col
df_filter=df_group.filter((col("max_values")>75000)|(col("name")=="Elvin")
)
df_filter.show()

# COMMAND ----------

from pyspark.sql.functions import collect_list,collect_set,first,last,countDistinct
df_grouped=df_join.groupby("department").agg(
             countDistinct("salary").alias("distinct_salary"),
             first("join_date").alias("first_date"),
             collect_set("name").alias("set_values")
)
df_grouped.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col,desc
window_spec=Window.partitionBy("salary").orderBy("join_date")
row_num=df1.withColumn("row_num",row_number().over(window_spec))
row_num1=row_num.select(col("join_date"),
                       col("salary"),col("row_num"),col("name")
                    )
row_num1.show()

# COMMAND ----------

from pyspark.sql.functions import rank,lead,lag,col
window_spec=Window.orderBy("salary")
df_rank=df1.withColumn("rank",rank().over(window_spec))
df_rank1=df_rank.select(col("name"),col("salary"),col("rank"))
df_rank1.show()
window_spec=Window.orderBy("employee_id")
df_lead=df7.withColumn("lead",lead("avg_salary").over(window_spec))
df_lead.show()
window_spec=Window.orderBy("name")
df_lag=df_group.withColumn("lag",lag("sum_value").over(window_spec))
df_lag.show()

