# Databricks notebook source
# MAGIC %md
# MAGIC # Lifecycle Models - Winback EDA

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
#from chaya_ai.chaya_ai import tracker 

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

# MAGIC %sql
# MAGIC select sum
# MAGIC from ds_staging.features__events_last_n_days
# MAGIC where 
# MAGIC ds_staging.lifecycle_models__lifecycle_models__winback_propensity_scores
