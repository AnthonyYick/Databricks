# Databricks notebook source
# MAGIC %md
# MAGIC #Set up Connections - Option 1 Connect Directly to S3

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "aws_datalake", key = "aws-datalake-access-key")
secret_key = dbutils.secrets.get(scope = "aws_datalake", key = "aws-datalake-secret-key")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

# COMMAND ----------

df = spark.read.format("csv").option("header",True).load("s3://centralized-data-lake/Bronze/new_user_credentials (1).csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Set up Connections - Option 2 Connect to S3 via Mount Point

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "aws_datalake", key = "aws-datalake-access-key")
secret_key = dbutils.secrets.get(scope = "aws_datalake", key = "aws-datalake-secret-key")
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "centralized-data-lake"
mount_name = "CentralizedDataLake"


dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

df2 = spark.read.format("csv").option("header",True).load("/mnt/CentralizedDataLake/Bronze/new_user_credentials (1).csv")

# COMMAND ----------

display(df2)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

df2.schema

# COMMAND ----------

def Remove_space_in_field(df):
    df_renamed = df.toDF(*[x.replace(" ","_") for x in df.columns])
    return df_renamed

# COMMAND ----------

df2 = Remove_space_in_field(df2)

# COMMAND ----------

# df.write.format("parquet").mode("overwrite").save("C:/Users/Admin/Downloads/SampleData/Testdf_fromparquet")
df2.write.format("parquet").  \
    mode("overwrite"). \
    save("/mnt/CentralizedDataLake/Silver/new_user_credentials")

# COMMAND ----------

2+2

# COMMAND ----------


