# Databricks notebook source
# libraries
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from dateutil.relativedelta import relativedelta
# global variables
today = datetime.today()

# COMMAND ----------

DATE_7DAYS_AGO = str(today.date() - relativedelta(days=7))

# COMMAND ----------

# backfill (comment out after running and deploying to prod)
for i in [pd.to_datetime("2022-05-31").date() - timedelta(days=x) for x in range(31)]:
  dbutils.notebook.run("./prod__1.0-ftdo-lifecycle-model", 60*120, {"signup_date": str(i),
                                                                   "user_cohort": "free trial drop out",
                                                                   "target_event": "converted free trial drop out",
                                                                  })
  dbutils.notebook.run("prod__2.0-ftdo-braze-integration", 60*60, {"signup_date": str(i)})
