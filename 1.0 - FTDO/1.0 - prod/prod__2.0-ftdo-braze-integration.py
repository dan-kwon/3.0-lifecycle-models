# Databricks notebook source
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import requests
import math

# rename columns and Braze attributes to be specific to the lag_days for the predictions
braze_user_track_payload_schema = StructType()\
    .add("external_id", StringType())\
    .add("uplift_score_bin", IntegerType())\
    .add("uplift_score", DoubleType())

braze_api_url = "https://rest.iad-02.braze.com/users/track"
braze_api_amplitude_prod = "46639013-e745-4bf9-8cff-f0de888469d1"
braze_api_key = braze_api_amplitude_prod

# COMMAND ----------

query = """
  select distinct signup_date
  from ds_staging.features__engagement_first_7_days
  order by 1
  """

list_of_available_dates = [str(i.date()) for i in pd.to_datetime(spark.sql(query).toPandas().values[:,0])]

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown(name="signup_date",
                         defaultValue=max(list_of_available_dates), 
                         choices=list_of_available_dates,
                         label="Signup Date")

# COMMAND ----------

SIGNUP_DATE  = dbutils.widgets.get("signup_date")

# COMMAND ----------

# 
query = f"""
  with prop_scores as (
    select p.*, 
           row_number() over(partition by hs_user_id order by signup_date desc) sub_rank
    from ds_staging.lifecycle_models__ftdo_propensity_scores p
    where signup_date = '{SIGNUP_DATE}'
  )
  select hs_user_id, 
         uplift_score_bin, 
         uplift_score
  from prop_scores
  where sub_rank = 1
"""

df_ftdo_propensity = spark.sql(query)
df_ftdo_propensity = df_ftdo_propensity.withColumn("row_number", F.row_number().over(Window.partitionBy().orderBy(df_ftdo_propensity['hs_user_id'])))

# rename table columns to be specific to the lag_days parameter in use
row_count = df_ftdo_propensity.count()
braze_request_event_limit = 50
batch_requests = math.ceil(row_count / braze_request_event_limit)

# COMMAND ----------

 # rename table columns
def to_braze_payload(hs_user_id, uplift_score_bin, uplift_score):
  return {"external_id":hs_user_id, 
          "uplift_score_bin":int(uplift_score_bin), 
          "uplift_score":uplift_score}

def send_to_braze(attributes):
  payload = {
    "api_key": braze_api_key,
    "attributes": list(map(lambda d: d.asDict(), attributes))
  }
  return requests.post(braze_api_url, json=payload).json()

# create user_track payload for each user
df_braze_payload = (df_ftdo_propensity
        .withColumn("braze_payload", F.udf(to_braze_payload, braze_user_track_payload_schema)(F.col("hs_user_id"), F.col("uplift_score_bin"), F.col("uplift_score")))
        .withColumn("batch_request_num", F.column("row_number") % batch_requests))

# batch user_track into group of 50 (braze batch request limit)
df_braze_batch_request = df_braze_payload.groupBy("batch_request_num").agg(F.collect_list(F.col("braze_payload")).alias("batch_request_payload"))

# execute the request (no throttling or retry)
df_braze_response = (df_braze_batch_request.withColumn("batch_response", F.udf(send_to_braze)(F.col("batch_request_payload")))).cache()

# COMMAND ----------

display(df_braze_response)
