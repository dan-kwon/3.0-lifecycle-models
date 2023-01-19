# Databricks notebook source
def get_user_cohort(cohort, country_code, subscription_type, begin_date, end_date):
  if cohort == "free trial drop out":
    query = f"""
      select s.*,
             s.dt signup_date,
             date_add(last_day(add_months(s.dt, -1)),1) signup_month
      from silver.fact_subscription s
      where lower(s.subscription_type) = '{subscription_type}'
        and lower(s.country_code) = '{country_code}'
        and s.free_trial_start_date between '{begin_date}' and '{end_date}'
      """
    return spark.sql(query)
  elif cohort == "winback":
    print("WIP")
    return None
  else:
    print("ERROR: COHORT NOT DEFINED")
    return None

# COMMAND ----------

# MAGIC %sql
# MAGIC select signup_date,
# MAGIC        count(*)
# MAGIC from ds_staging.lifecycle_models__ftdo_propensity_scores
# MAGIC where uplift_score_bin > 5 
# MAGIC group by 1
# MAGIC order by 1

# COMMAND ----------


