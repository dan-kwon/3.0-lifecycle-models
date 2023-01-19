# Databricks notebook source
# MAGIC %md
# MAGIC # Lifecycle Model - FT Conversion

# COMMAND ----------

# MAGIC %md
# MAGIC #### Libraries / Global Vars

# COMMAND ----------

# general use
import math
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from chaya_ai.chaya_ai import tracker 

# data retrieval / prep
from datetime import datetime
from pyspark.sql import functions as F
from sklearn.utils import resample
from imblearn.over_sampling import SMOTENC, RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler
import gender_guesser.detector as g

# pipeline
from sklearn.pipeline import make_pipeline, Pipeline
from sklearn.compose import ColumnTransformer
from sklearn import set_config

# feature processing and selection
from sklearn.preprocessing import OneHotEncoder, StandardScaler, PolynomialFeatures
from sklearn.feature_selection import VarianceThreshold, SelectFromModel, SelectKBest, chi2
from sklearn.decomposition import PCA

# training
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.calibration import CalibratedClassifierCV

# validation
from sklearn.calibration import CalibrationDisplay, calibration_curve
from sklearn.metrics import classification_report, PrecisionRecallDisplay
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import f1_score
from sklearn.metrics import auc

# matching / evaluation
from sklearn.neighbors import NearestNeighbors
from scipy import stats
import random

# global vars
today = datetime.today()
random_state = 1234

# COMMAND ----------

# MAGIC %md Loading additional UDFs

# COMMAND ----------

# MAGIC %run "./udf/prod__lifecycle-features"

# COMMAND ----------

# MAGIC %run "./udf/prod__lifecycle-target-events"

# COMMAND ----------

# MAGIC %run "./udf/prod__lifecycle-user-cohorts"

# COMMAND ----------

# MAGIC %md Creating widgets and assigning parameter values

# COMMAND ----------

query = """
  select max(signup_date)
  from ds_staging.features__engagement_first_7_days
  order by 1
  """

list_of_available_dates = [str(i.date()) for i in pd.to_datetime(spark.sql(query).toPandas().values[:,0])]

# COMMAND ----------

# define date range
dbutils.widgets.text(name="signup_date",
                         defaultValue=max(list_of_available_dates),
                         label="Signup Date")
# define cohort
dbutils.widgets.text(name="user_cohort", 
                         defaultValue="free trial drop out", 
                         label="User Cohort")
# define event of interest
dbutils.widgets.text(name="target_event", 
                     defaultValue="converted free trial drop out",
                     label="Event of Interest")

# COMMAND ----------

# parameters
USER_COHORT = dbutils.widgets.get("user_cohort")
TARGET_EVENT = dbutils.widgets.get("target_event")
SUBSCRIPTION_TYPE = 'b2c'
COUNTRY_CODE = 'us'
HORIZON_DAYS = 42

# date parameters
SIGNUP_DATE  = dbutils.widgets.get("signup_date")
TEST_END_DATE   = pd.to_datetime(dbutils.widgets.get("signup_date")).date()-pd.to_timedelta(HORIZON_DAYS+1, unit='d')
TEST_BEGIN_DATE = pd.to_datetime(dbutils.widgets.get("signup_date")).date()-pd.to_timedelta(HORIZON_DAYS+28, unit='d')
TRAIN_END_DATE   = pd.to_datetime(dbutils.widgets.get("signup_date")).date()-pd.to_timedelta(HORIZON_DAYS+29, unit='d')
TRAIN_BEGIN_DATE = pd.to_datetime(dbutils.widgets.get("signup_date")).date()-pd.to_timedelta(HORIZON_DAYS+29+365, unit='d')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data

# COMMAND ----------

# pull raw data from data warehouse
df_users = get_user_cohort(cohort=USER_COHORT, 
                           country_code=COUNTRY_CODE, 
                           subscription_type=SUBSCRIPTION_TYPE, 
                           begin_date=TRAIN_BEGIN_DATE,
                           end_date=TEST_END_DATE).alias("users")

df_engagement_features = get_engagement_features(USER_COHORT,TRAIN_BEGIN_DATE,TEST_END_DATE).alias("engagement")

df_event_features = get_event_features(USER_COHORT,TRAIN_BEGIN_DATE,TEST_END_DATE).alias("events")

df_targets = get_target_event(cohort=USER_COHORT,
                              country_code=COUNTRY_CODE, 
                              subscription_type=SUBSCRIPTION_TYPE,
                              begin_date=TRAIN_BEGIN_DATE,
                              end_date=TEST_END_DATE,
                              horizon_days=HORIZON_DAYS).alias("target")

# join resulting dataframes
df_all = df_users \
  .join(df_targets, how="inner", on=["hs_user_id","signup_date"]) \
  .join(df_engagement_features, how="left", on=["hs_user_id","signup_date"]) \
  .join(df_event_features, how="left", on=["hs_user_id","signup_date"]) \
  .withColumn("plan_renewal_term_months_c", F.expr("CASE WHEN plan_renewal_term_months = 1 THEN 'monthly' " +
                                                        "WHEN plan_renewal_term_months = 12 THEN 'annual' " +
                                                        "ELSE 'other' END")) \
  .withColumn("max_discount_offered_c", F.expr("CASE WHEN num_offers_40 > 0 THEN 40 " +
                                               "WHEN num_offers_30 > 0 THEN 30 " +
                                               "WHEN num_offers_20 > 0 THEN 20 " +
                                               "ELSE 0 END")) \
  .withColumn("renewal_c", F.expr("CASE WHEN renewal_count > 0 THEN 'returning' ELSE 'new' END")) \
  .toPandas() \
  .fillna(0)

df_all["signup_date"] = pd.to_datetime(df_all["signup_date"])
df_all["dt"] = pd.to_datetime(df_all["dt"])

# filter on 7 and 14 day free trial users 
df_all = df_all.loc[(df_all["free_trial_days"]==7)|(df_all["free_trial_days"]==14)].reset_index(drop=True)

# one hot encode experiment group
enc = OneHotEncoder()
df_enc_experiment_group = pd.DataFrame(enc.fit_transform(df_all[["experiment_group"]]).toarray())
df_enc_experiment_group.columns = enc.get_feature_names_out()
df_all = df_all.join(df_enc_experiment_group)

# aggregating daily features to weeks
def aggregate_into_weeks(df, week_num, feature_name):
  df[f"week{week_num}_{feature_name}"] = df[f"day{range(week_num*7-7, week_num*7)[0]}_{feature_name}"] + \
                                         df[f"day{range(week_num*7-7, week_num*7)[1]}_{feature_name}"] + \
                                         df[f"day{range(week_num*7-7, week_num*7)[2]}_{feature_name}"] + \
                                         df[f"day{range(week_num*7-7, week_num*7)[3]}_{feature_name}"] + \
                                         df[f"day{range(week_num*7-7, week_num*7)[4]}_{feature_name}"] + \
                                         df[f"day{range(week_num*7-7, week_num*7)[5]}_{feature_name}"] + \
                                         df[f"day{range(week_num*7-7, week_num*7)[6]}_{feature_name}"]
  return df
for i in [ i[5:] for i in df_all.filter(like='day0').columns if 'distinct' not in i ]:
  df_all = aggregate_into_weeks(df_all,1,i)
   
# split treatment and control groups
df_treatment = df_all.loc[(df_all["experiment_group"]=="TN")|(df_all["experiment_group"]=="TR")]
df_control   = df_all.loc[(df_all["experiment_group"]=="CN")|(df_all["experiment_group"]=="CR")]

# COMMAND ----------

# train sets
X_train_treatment_all_col = df_treatment[df_treatment["signup_date"] <= str(TRAIN_END_DATE)] \
  .drop(["experiment_group",
         "experiment_group_TR",
         "experiment_group_TN",
         "experiment_group_CR",
         "experiment_group_CN"], 
        axis=1)
y_train_treatment = df_treatment.loc[df_treatment["signup_date"] <= str(TRAIN_END_DATE),"experiment_group_TR"]

X_train_control_all_col = df_control[df_control["signup_date"] <= str(TRAIN_END_DATE)] \
  .drop(["experiment_group",
         "experiment_group_TR",
         "experiment_group_TN",
         "experiment_group_CR",
         "experiment_group_CN"], 
        axis=1)
y_train_control = df_control.loc[df_control["signup_date"] <= str(TRAIN_END_DATE),"experiment_group_CR"]

# test sets
X_test_all_col = df_all[df_all["signup_date"] > str(TRAIN_END_DATE)] \
  .drop(["experiment_group",
         "experiment_group_TR",
         "experiment_group_TN",
         "experiment_group_CR",
         "experiment_group_CN"], 
        axis=1)
X_test_treatment_all_col = df_treatment.loc[df_treatment["signup_date"] > str(TRAIN_END_DATE)] \
  .drop(["experiment_group",
         "experiment_group_TR",
         "experiment_group_TN",
         "experiment_group_CR",
         "experiment_group_CN"], 
        axis=1)
X_test_control_all_col = df_control[df_control["signup_date"] > str(TRAIN_END_DATE)] \
  .drop(["experiment_group",
         "experiment_group_TR",
         "experiment_group_TN",
         "experiment_group_CR",
         "experiment_group_CN"], 
        axis=1)
y_test_TR = df_all.loc[df_all["signup_date"] > str(TRAIN_END_DATE), "experiment_group_TR"]
y_test_CR = df_all.loc[df_all["signup_date"] > str(TRAIN_END_DATE), "experiment_group_CR"]
y_test_treatment = df_treatment.loc[df_treatment["signup_date"] > str(TRAIN_END_DATE), "experiment_group_TR"]
y_test_control = df_control.loc[df_control["signup_date"] > str(TRAIN_END_DATE), "experiment_group_CR"]

# COMMAND ----------

# specify categorical column names
categorical_col = ["plan_renewal_term_months_c", "subscription_platform", "renewal_c"] #, "num_offers_20", "num_offers_30", "num_offers_40"]

# get list of categorical + numeric features to include in model
X_col = categorical_col.copy()
X_col += [i for i in X_train_treatment_all_col.columns if 'week1' in i or 
                                                          'free_trial_days' in i or
                                                          'max_discount' in i or
                                                          'renewal_count' in i or
                                                          'free_trial_days' in i]
X_col.sort()
X_train_treatment = X_train_treatment_all_col[X_col]
X_train_control   = X_train_control_all_col[X_col]
X_test_treatment  = X_test_treatment_all_col[X_col]
X_test_control    = X_test_control_all_col[X_col]

X_test            = X_test_all_col[X_col]

# COMMAND ----------

# SMOTE oversampling the minority class
smote_oversample = SMOTENC(random_state=random_state, categorical_features=[X_train_treatment.columns.get_loc(col) for col in categorical_col])
X_train_treatment, y_train_treatment = smote_oversample.fit_resample(X_train_treatment, y_train_treatment)
X_train_control,   y_train_control   = smote_oversample.fit_resample(X_train_control,   y_train_control)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pipeline

# COMMAND ----------

numeric_preprocessor = Pipeline(
    steps=[
        ("scaler", StandardScaler()),
        ("pca", PCA()),
        ("variance threshold", VarianceThreshold())
    ])

categorical_preprocessor = Pipeline(
    steps=[
        ("onehot", OneHotEncoder(handle_unknown="ignore"))
    ])

preprocessor = ColumnTransformer(
    [
        ("categorical", categorical_preprocessor, categorical_col),
        ("numerical", numeric_preprocessor, list(set(X_col) - set(categorical_col))),
    ])

pipe_rf_t = Pipeline(
    steps=[("preprocessor", preprocessor),
           ("interactions", PolynomialFeatures(interaction_only=False)),
           ('classifier', CalibratedClassifierCV(base_estimator=RandomForestClassifier(min_samples_split=0.05, n_estimators = 150), cv=5, method='sigmoid', n_jobs=20))])

pipe_rf_c = Pipeline(
    steps=[("preprocessor", preprocessor),
           ("interactions", PolynomialFeatures(interaction_only=False)),
           ('classifier', CalibratedClassifierCV(base_estimator=RandomForestClassifier(min_samples_split=0.05, n_estimators = 150), cv=5, method='sigmoid', n_jobs=20))])

# grid search takes ~30 min and doesn't result in a meaningful increase in performance so commenting out for now
#pipe_rf = Pipeline(
#    steps=[("preprocessor", preprocessor),
#           ("interactions", PolynomialFeatures(interaction_only=False)),
#           ('classifier', RandomForestClassifier())
#          ])
#param_grid_rf = {
#  "classifier__n_estimators": [200, 400, 600],
#  "classifier__max_depth": [6, 8, 10, 12]
#}
#grid_search_rf = GridSearchCV(pipe_rf, param_grid_rf, n_jobs=20, scoring='f1_micro')
#calibrated_clf = CalibratedClassifierCV(grid_search_rf.best_estimator_)
#calibrated_clf.fit(X_train_smote, y_train_smote.values.ravel())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Training Model

# COMMAND ----------

# train on oversampled training set
set_config(display="diagram")

# active models
pipe_rf_t.fit(X_train_treatment, y_train_treatment.values.ravel())
pipe_rf_c.fit(X_train_control, y_train_control.values.ravel())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Testing Model

# COMMAND ----------

## test on treatment_group
y_scores_treatment = pipe_rf_t.predict_proba(X_test_treatment)[:,1]
y_pred_treatment = pipe_rf_t.predict(X_test_treatment)
#### performance
print("TREATMENT (imbalanced test set)"+"-"*22)
print(classification_report(y_test_treatment, y_pred_treatment))
y_scores_treatment = pipe_rf_t.predict_proba(X_test_treatment)[:,1]
precision, recall, thresholds = precision_recall_curve(y_test_treatment, y_scores_treatment)
print("auc for RF-SMOTE: "+str(auc(recall, precision)))

## test on control_group
y_scores_control = pipe_rf_c.predict_proba(X_test_control)[:,1]
y_pred_control = pipe_rf_c.predict(X_test_control)
#### performance
print("CONTROL (imbalanced test set)"+"-"*24)
print(classification_report(y_test_control, y_pred_control))
precision, recall, thresholds = precision_recall_curve(y_test_control, y_scores_control)
print("auc for RF-SMOTE: "+str(auc(recall, precision)))

# COMMAND ----------

from sklearn import metrics

#sns.lineplot(x=, y=metrics.roc_curve(y_test_treatment, y_scores_treatment))
print(metrics.roc_auc_score(y_test_treatment, y_scores_treatment))
fpr, tpr, thresholds = metrics.roc_curve(y_test_treatment, y_scores_treatment)

sns.lineplot(x=fpr, y=tpr)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Take Rates vs. Propensity

# COMMAND ----------

# plotting actual vs predicted take rates
prob_true, prob_pred = calibration_curve(y_test_treatment, y_scores_treatment, n_bins=10)

fig, ax1 = plt.subplots()
ax2 = ax1.twinx()

sns.lineplot(x=prob_pred, y=prob_true, color="red", ax=ax1)
sns.histplot(y_scores_treatment, stat="probability", color='blue', alpha=0.4, ax=ax2, bins=10)

ax1.set_xlabel("predicted propensity to respond to a discount")
ax1.set_ylabel("actual return rate")
ax2.set_ylabel("% of users")
plt.title("Predicted vs. Actual Propensities - Treatment Group")

# COMMAND ----------

# plotting actual vs predicted take rates
prob_true, prob_pred = calibration_curve(y_test_control, y_scores_control, n_bins=10)

fig, ax1 = plt.subplots()
ax2 = ax1.twinx()

sns.lineplot(x=prob_pred, y=prob_true, color="red", ax=ax1)
sns.histplot(y_scores_control, stat="probability", color='blue', alpha=0.4, ax=ax2, bins=10)

ax1.set_xlabel("predicted propensity to return without a discount")
ax1.set_ylabel("actual return rate")
ax2.set_ylabel("% of users")
plt.title("Predicted vs. Actual Propensities - Control Group")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Distribution of Propensities

# COMMAND ----------

# joining back to y_test to X_test and append model predictions
y_scores_TR = pipe_rf_t.predict_proba(X_test)[:,1]
y_scores_CR = pipe_rf_c.predict_proba(X_test)[:,1]

joined_test_set = X_test_all_col.join(y_test_TR).join(y_test_CR)
joined_test_set["propensity_TR"] = y_scores_TR.tolist()
joined_test_set["propensity_CR"] = y_scores_CR.tolist()
joined_test_set["propensity_TN"] = 1-joined_test_set["propensity_TR"]
joined_test_set["propensity_CN"] = 1-joined_test_set["propensity_CR"]
joined_test_set["uplift_score"] = joined_test_set["propensity_TR"] + joined_test_set["propensity_CN"] - joined_test_set["propensity_TN"] - joined_test_set["propensity_CR"]
joined_test_set['uplift_score_bin'] = pd.qcut(joined_test_set["uplift_score"], q=5, precision=0, labels=[5,4,3,2,1])
joined_test_set['propensity_TR_bin'] = pd.qcut(joined_test_set["propensity_TR"], q=5, precision=0, labels=[5,4,3,2,1])
joined_test_set['propensity_CN_bin'] = pd.qcut(joined_test_set["propensity_CN"], q=5, precision=0, labels=[5,4,3,2,1])

# COMMAND ----------

sns.histplot(joined_test_set["uplift_score"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scoring hold out users

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Pulling features

# COMMAND ----------

# pull raw data from data warehouse
df_users_to_score = get_user_cohort(cohort=USER_COHORT, 
                                    country_code=COUNTRY_CODE, 
                                    subscription_type=SUBSCRIPTION_TYPE, 
                                    begin_date=SIGNUP_DATE,
                                    end_date=SIGNUP_DATE).alias("users")

df_engagement_features_to_score = get_engagement_features(USER_COHORT,SIGNUP_DATE,SIGNUP_DATE).alias("engagement")

df_event_features_to_score = get_event_features(USER_COHORT,SIGNUP_DATE,SIGNUP_DATE).alias("events")

# join resulting dataframes
df_all_to_score = df_users_to_score \
  .join(df_engagement_features_to_score, how="left", on=["hs_user_id","signup_date"]) \
  .join(df_event_features_to_score, how="left", on=["hs_user_id","signup_date"]) \
  .withColumn("plan_renewal_term_months_c", F.expr("CASE WHEN plan_renewal_term_months = 1 THEN 'monthly' " +
                                                        "WHEN plan_renewal_term_months = 12 THEN 'annual' " +
                                                        "ELSE 'other' END")) \
  .withColumn("renewal_c", F.expr("CASE WHEN renewal_count > 0 THEN 'returning' ELSE 'new' END")) \
  .toPandas() \
  .fillna(0)
df_all_to_score["max_discount_offered_c"] = 20
df_all_to_score["signup_date"] = pd.to_datetime(df_all_to_score["signup_date"])
df_all_to_score["dt"] = pd.to_datetime(df_all_to_score["dt"])

# filter on 7 and 14 day free trial users 
df_all_to_score = df_all_to_score.loc[(df_all_to_score["free_trial_days"]==7)|(df_all_to_score["free_trial_days"]==14)].reset_index(drop=True)

# aggregating daily features to weeks
for i in [ i[5:] for i in df_all_to_score.filter(like='day0').columns if 'distinct' not in i ]:
  df_all_to_score = aggregate_into_weeks(df_all_to_score,1,i)

# COMMAND ----------

X_score = df_all_to_score[X_col]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Scoring users and appending to feature set

# COMMAND ----------

# joining back to y_test to X_test and append model predictions
df_all_to_score["propensity_TR"] = pipe_rf_t.predict_proba(X_score)[:,1].tolist()
df_all_to_score["propensity_CR"] = pipe_rf_c.predict_proba(X_score)[:,1].tolist()
df_all_to_score["propensity_TN"] = 1-df_all_to_score["propensity_TR"]
df_all_to_score["propensity_CN"] = 1-df_all_to_score["propensity_CR"]
df_all_to_score["uplift_score"] = df_all_to_score["propensity_TR"] + df_all_to_score["propensity_CN"] - df_all_to_score["propensity_TN"] - df_all_to_score["propensity_CR"]
df_all_to_score['uplift_score_bin'] = pd.qcut(df_all_to_score["uplift_score"],   q=10, precision=0, labels=[10,9,8,7,6,5,4,3,2,1])
df_all_to_score['propensity_TR_bin'] = pd.qcut(df_all_to_score["propensity_TR"], q=10, precision=0, labels=[10,9,8,7,6,5,4,3,2,1])
df_all_to_score['propensity_CN_bin'] = pd.qcut(df_all_to_score["propensity_CN"], q=10, precision=0, labels=[10,9,8,7,6,5,4,3,2,1])
df_all_to_score["signup_date"] = [str(i.date()) for i in df_all_to_score["signup_date"]]

# COMMAND ----------

## Convert into Spark DataFrame
spark_df = spark.createDataFrame(df_all_to_score[["hs_user_id",
                                                  "signup_date",
                                                  "propensity_TR",
                                                  "propensity_CR",
                                                  "propensity_TN",
                                                  "propensity_CN",
                                                  "uplift_score",
                                                  "uplift_score_bin"
                                                 ]])
## Write to databricks
spark_df \
  .write \
  .mode('append') \
  .option("mergeSchema", "true") \
  .saveAsTable("ds_staging.lifecycle_models__ftdo_propensity_scores")
