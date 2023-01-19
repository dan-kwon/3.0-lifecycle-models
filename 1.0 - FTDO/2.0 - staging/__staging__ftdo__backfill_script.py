# Databricks notebook source
import pandas as pd

# COMMAND ----------

dbutils.notebook.run("1.0 - lifecycle-FT-conversion", 60*45, {"test_begin_date":  "2022-02-01",
                                                              "test_end_date":    "2022-02-28",
                                                              "user_cohort":      "free trial drop out",
                                                              "target_event":     "converted free trial drop out",
                                                             })

# COMMAND ----------

dbutils.notebook.run("1.0 - lifecycle-FT-conversion", 60*45, {"test_begin_date":  "2022-01-01",
                                                              "test_end_date":    "2022-01-31",
                                                              "user_cohort":      "free trial drop out",
                                                              "target_event":     "converted free trial drop out",
                                                             })

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(signup_date)
# MAGIC from ds_staging.features__events_first_n_days;

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(signup_date)
# MAGIC from ds_staging.features__engagement_first_n_days;

# COMMAND ----------

for i in [str(x) for x in pd.date_range(start="2021-06-01", end="2021-12-01", freq="d").date]:
  print(i)
  #month_beg_date = i
  #month_end_date = (pd.to_datetime(i, format="%Y-%m-%d") + MonthEnd(1)).date()
  #dbutils.notebook.run("1.0 - PSM-first-n-days", 60*45, {"begin_date":   month_beg_date,
  #                                                       "end_date":     month_end_date,
  #                                                       "user_cohort":  "new users",
  #                                                       "target_event": "first course playback",
  #                                                      })

# COMMAND ----------


