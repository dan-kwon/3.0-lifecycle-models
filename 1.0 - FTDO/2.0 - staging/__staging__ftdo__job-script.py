# Databricks notebook source
# libraries
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from dateutil.relativedelta import relativedelta
# global variables
today = datetime.today()

# COMMAND ----------

DATE_7DAYS_AGO = str(today.date() - relativedelta(days=7))

# COMMAND ----------

dbutils.notebook.run("_staging__1.0-lifecycle-model", 60*45, {"signup_date":  DATE_7DAYS_AGO,
                                                              "user_cohort":      "free trial drop out",
                                                              "target_event":     "converted free trial drop out",
                                                             })
