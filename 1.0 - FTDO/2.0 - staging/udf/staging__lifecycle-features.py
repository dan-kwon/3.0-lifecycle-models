# Databricks notebook source
def get_engagement_features(cohort, begin_date, end_date):
  if cohort == "free trial drop out":
    query = f"""select * 
                from ds_staging.features__engagement_first_7_days
                where signup_date between '{begin_date}' and '{end_date}'"""
    return spark.sql(query)
  elif cohort == "winback":
    print("WIP")
    return None  
  else:
    print("ERROR: COULD NOT LOAD ENGAGEMENT FEATURES")
    return None  

def get_event_features(cohort, begin_date, end_date):
  if cohort == "free trial drop out":
    query = f"""select * 
                from ds_staging.features__events_first_7_days
                where signup_date between '{begin_date}' and '{end_date}'"""
    return spark.sql(query)
  elif cohort == "winback":
    print("WIP")
    return None  
  else:
    print("ERROR: COULD NOT LOAD ENGAGEMENT FEATURES")
    return None  

# COMMAND ----------

def aggr_week1_feature(X,feature_suffix):
  X[f"""week1_{feature_suffix}"""] = X[f"""day0_{feature_suffix}"""] + \
                                     X[f"""day1_{feature_suffix}"""] + \
                                     X[f"""day2_{feature_suffix}"""] + \
                                     X[f"""day3_{feature_suffix}"""] + \
                                     X[f"""day4_{feature_suffix}"""] + \
                                     X[f"""day5_{feature_suffix}"""] + \
                                     X[f"""day6_{feature_suffix}"""]
  return X

def trailing_3day_avg(X,feature_suffix,from_day):
  day_1 = from_day-2
  day_2 = from_day-1
  day_3 = from_day
  X[f"""day{day_3}_trailing3day_avg_{feature_suffix}"""] = (X[f"""day{day_3}_{feature_suffix}"""] + 
                                                            X[f"""day{day_2}_{feature_suffix}"""] +
                                                            X[f"""day{day_1}_{feature_suffix}"""]) / 3
  return X

def aggregate_weeks(X,feature_suffix):
  X[f"""week_2to4_{feature_suffix}"""] = (X[f"""week2_{feature_suffix}"""] + 
                                          X[f"""week3_{feature_suffix}"""] +
                                          X[f"""week4_{feature_suffix}"""])
  return X

def slope_week1_feature(X,feature_suffix):
  X[f"""week1_{feature_suffix}_slope"""] = (X[f"""day7_trailing3day_avg_{feature_suffix}"""]/(X[f"""day6_trailing3day_avg_{feature_suffix}"""]+0.00000001)-1 + 
                                            X[f"""day6_trailing3day_avg_{feature_suffix}"""]/(X[f"""day5_trailing3day_avg_{feature_suffix}"""]+0.00000001)-1 + 
                                            X[f"""day5_trailing3day_avg_{feature_suffix}"""]/(X[f"""day4_trailing3day_avg_{feature_suffix}"""]+0.00000001)-1 + 
                                            X[f"""day4_trailing3day_avg_{feature_suffix}"""]/(X[f"""day3_trailing3day_avg_{feature_suffix}"""]+0.00000001)-1)/4                                 
  return X
