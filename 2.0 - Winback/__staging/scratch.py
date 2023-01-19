# Databricks notebook source
# MAGIC %md
# MAGIC #### user cohorts and targets for winback

# COMMAND ----------

# MAGIC %md
# MAGIC #### days until return - relative to previous subscription end date

# COMMAND ----------

# MAGIC %sql
# MAGIC with 
# MAGIC braze as (
# MAGIC     select external_user_id,
# MAGIC            case when canvas_variation_name = 'Control' then 'control' else 'treatment' end as experiment_group,
# MAGIC            min(case when event_name = 'users_messages_email_delivery' then b.dt else null end) as first_email_date,
# MAGIC            min(case when event_name = 'users_canvas_entry' then b.dt else null end) as canvas_entry_date,
# MAGIC            min(case when canvas_variation_name =  'Control' and event_name = 'users_canvas_entry' then b.dt 
# MAGIC                     when canvas_variation_name <> 'Control' and event_name = 'users_messages_email_delivery' then b.dt 
# MAGIC                     else null end) as test_date,
# MAGIC            count(case when b.canvas_step_name like '%10%' then 1 else null end) num_offers_10,
# MAGIC            count(case when b.canvas_step_name like '%20%' then 1 else null end) num_offers_20,
# MAGIC            count(case when b.canvas_step_name like '%30%' then 1 else null end) num_offers_30,
# MAGIC            count(case when b.canvas_step_name like '%40%' then 1 else null end) num_offers_40
# MAGIC     from bronze.braze_events b
# MAGIC     where lower(canvas_name) like '%email%winback%'
# MAGIC       and event_name in ('users_canvas_entry','users_messages_email_delivery')
# MAGIC     group by 1,2
# MAGIC     having (num_offers_10 + num_offers_20 + num_offers_30 + num_offers_40) > 0 
# MAGIC         or experiment_group = 'control'
# MAGIC ),
# MAGIC subscription as (
# MAGIC     select s.hs_user_id,
# MAGIC            s.dt,
# MAGIC            s.free_trial_start_date,
# MAGIC            s.free_trial_end_date,
# MAGIC            s.paid_subscription_start_date,
# MAGIC            s.paid_subscription_end_date,
# MAGIC            lead(s.free_trial_start_date, 1, '9999-12-31') over(partition by hs_user_id order by dt) next_free_trial_start_date,
# MAGIC            lead(s.free_trial_end_date, 1, '9999-12-31') over(partition by hs_user_id order by dt) next_free_trial_end_date,
# MAGIC            lead(s.paid_subscription_start_date, 1, '9999-12-31') over(partition by hs_user_id order by dt) next_paid_subscription_start_date,
# MAGIC            lead(s.paid_subscription_end_date, 1, '9999-12-31') over(partition by hs_user_id order by dt) next_paid_subscription_end_date,
# MAGIC            row_number() over(partition by hs_user_id order by dt) sub_rank
# MAGIC     from silver.fact_subscription s
# MAGIC     where s.hs_user_id is not null
# MAGIC       and lower(s.subscription_type) = 'b2c'
# MAGIC       and 
# MAGIC )
# MAGIC select s.hs_user_id,
# MAGIC        coalesce(s.paid_subscription_end_date,s.free_trial_end_date) subscription_end_date,
# MAGIC        b.experiment_group,
# MAGIC        b.num_offers_10,
# MAGIC        b.num_offers_20,
# MAGIC        b.num_offers_30,
# MAGIC        b.num_offers_40,
# MAGIC        datediff(b.canvas_entry_date, coalesce(s.paid_subscription_end_date, s.free_trial_end_date)) days_since_cancel,
# MAGIC        datediff(coalesce(s.paid_subscription_end_date, s.free_trial_end_date), s.dt) tenure_at_cancel,
# MAGIC        s.sub_rank-1 num_previous_subscriptions,
# MAGIC        case when datediff(s.next_paid_subscription_start_date, b.canvas_entry_date) <= 28 then 1 else 0 end target
# MAGIC from braze b
# MAGIC inner join subscription s
# MAGIC   on s.hs_user_id = b.external_user_id
# MAGIC   and b.canvas_entry_date between coalesce(s.paid_subscription_end_date, s.free_trial_end_date) and s.next_paid_subscription_start_date
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from silver.fact_subscription
# MAGIC where hs_user_id = 'HSUSER_01H3F4E2MD654TCBR'

# COMMAND ----------

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure

query_1 = f"""
with 
braze as (
    select external_user_id,
           canvas_variation_name,
           min(case when event_name = 'users_messages_email_delivery' then b.dt else null end) as first_email_date,
           min(case when event_name = 'users_canvas_entry' then b.dt else null end) as canvas_entry_date,
           min(case when canvas_variation_name =  'Control' and event_name = 'users_canvas_entry' then b.dt 
                    when canvas_variation_name <> 'Control' and event_name = 'users_messages_email_delivery' then b.dt 
                    else null end) as test_date,
           count(case when b.canvas_step_name like '%10%' then 1 else null end) num_offers_10,
           count(case when b.canvas_step_name like '%20%' then 1 else null end) num_offers_20,
           count(case when b.canvas_step_name like '%30%' then 1 else null end) num_offers_30,
           count(case when b.canvas_step_name like '%40%' then 1 else null end) num_offers_40
    from bronze.braze_events b
    where lower(canvas_name) like '%winback%'
      and event_name in ('users_canvas_entry','users_messages_email_delivery')
    group by 1,2
    having canvas_entry_date >= '2021-01-01'
),
subscriptions as (
    select s.hs_user_id,
           s.voucher_code,
           s.plan_initial_term_months,
           s.dt,
           s.free_trial_start_date,
           s.free_trial_end_date,
           s.paid_subscription_start_date,
           s.paid_subscription_end_date,
           s.created_timestamp,
           s.updated_timestamp,
           lead(s.paid_subscription_start_date) over (partition by s.hs_user_id order by s.paid_subscription_start_date) next_paid_subscription_start_date,
           lead(s.voucher_code) over (partition by hs_user_id order by s.dt, s.created_timestamp, s.updated_timestamp) next_voucher_code
    from silver.fact_subscription s
    where lower(s.country_code) = 'us'
      and lower(s.subscription_type) = 'b2c'
),
all_users as (
  select s.*,
       b.first_email_date,
       b.canvas_variation_name,
       b.num_offers_10,
       b.num_offers_20,
       b.num_offers_30,
       b.num_offers_40,
       b.canvas_entry_date,
       row_number() over(partition by s.hs_user_id order by datediff(b.canvas_entry_date,s.paid_subscription_end_date)) sub_rank
  from braze b
  inner join subscriptions s
    on b.external_user_id = s.hs_user_id
    and b.canvas_entry_date >= s.paid_subscription_end_date
    and s.paid_subscription_end_date between '2021-01-01' and '2021-12-31'
    and datediff(s.paid_subscription_end_date, s.paid_subscription_start_date) >= 28
)
select hs_user_id,
       plan_initial_term_months,
       datediff(paid_subscription_end_date, dt) tenure_at_cancellation,
       datediff(coalesce(next_paid_subscription_start_date, '9999-12-31'), paid_subscription_end_date) days_until_return__from_previous_subscription_end_date,
       datediff(coalesce(next_paid_subscription_start_date, '9999-12-31'), first_email_date) days_until_return__from_first_email_date
from all_users a
where canvas_variation_name = 'Control'
  and sub_rank = 1
  and paid_subscription_end_date is not null
  and next_paid_subscription_start_date >= paid_subscription_end_date
  and next_paid_subscription_start_date is not null
  and datediff(coalesce(next_paid_subscription_start_date, '9999-12-31'), paid_subscription_end_date) <= 365
"""

query_2 = f"""
with 
braze as (
    select external_user_id,
           canvas_variation_name,
           min(case when event_name = 'users_messages_email_delivery' then b.dt else null end) as first_email_date,
           min(case when event_name = 'users_canvas_entry' then b.dt else null end) as canvas_entry_date,
           min(case when canvas_variation_name =  'Control' and event_name = 'users_canvas_entry' then b.dt 
                    when canvas_variation_name <> 'Control' and event_name = 'users_messages_email_delivery' then b.dt 
                    else null end) as test_date,
           count(case when b.canvas_step_name like '%10%' then 1 else null end) num_offers_10,
           count(case when b.canvas_step_name like '%20%' then 1 else null end) num_offers_20,
           count(case when b.canvas_step_name like '%30%' then 1 else null end) num_offers_30,
           count(case when b.canvas_step_name like '%40%' then 1 else null end) num_offers_40
    from bronze.braze_events b
    where lower(canvas_name) like '%winback%'
      and event_name in ('users_canvas_entry','users_messages_email_delivery')
    group by 1,2
    having canvas_entry_date >= '2021-01-01'
),
subscriptions as (
    select s.hs_user_id,
           s.voucher_code,
           s.plan_initial_term_months,
           s.dt,
           s.free_trial_start_date,
           s.free_trial_end_date,
           s.paid_subscription_start_date,
           s.paid_subscription_end_date,
           s.created_timestamp,
           s.updated_timestamp,
           lead(s.paid_subscription_start_date) over (partition by s.hs_user_id order by s.paid_subscription_start_date) next_paid_subscription_start_date,
           lead(s.voucher_code) over (partition by hs_user_id order by s.dt, s.created_timestamp, s.updated_timestamp) next_voucher_code
    from silver.fact_subscription s
    where lower(s.country_code) = 'us'
      and lower(s.subscription_type) = 'b2c'
),
all_users as (
  select s.*,
       b.first_email_date,
       b.canvas_variation_name,
       b.num_offers_10,
       b.num_offers_20,
       b.num_offers_30,
       b.num_offers_40,
       b.canvas_entry_date,
       row_number() over(partition by s.hs_user_id order by datediff(b.canvas_entry_date,s.paid_subscription_end_date)) sub_rank
  from braze b
  inner join subscriptions s
    on b.external_user_id = s.hs_user_id
    and b.canvas_entry_date >= s.paid_subscription_end_date
    and s.paid_subscription_end_date between '2021-01-01' and '2021-12-31'
    and datediff(s.paid_subscription_end_date, s.paid_subscription_start_date) >= 28
)
select hs_user_id,
       plan_initial_term_months,
       datediff(paid_subscription_end_date, dt) tenure_at_cancellation,
       datediff(coalesce(next_paid_subscription_start_date, '9999-12-31'), paid_subscription_end_date) days_until_return__from_previous_subscription_end_date,
       datediff(coalesce(next_paid_subscription_start_date, '9999-12-31'), first_email_date) days_until_return__from_first_email_date,
       datediff(first_email_date, paid_subscription_end_date) days_until_email__from_subscription_end_date
from all_users a
where (lower(canvas_variation_name) like 'winback' or lower(canvas_variation_name) like '%10%' or lower(canvas_variation_name) like '%40%')
  and num_offers_10 + num_offers_20 + num_offers_30 + num_offers_40 > 0
  and sub_rank = 1
  and paid_subscription_end_date is not null
  and coalesce(next_paid_subscription_start_date,'9999-12-31') >= paid_subscription_end_date
  and datediff(coalesce(next_paid_subscription_start_date, '9999-12-31'), paid_subscription_end_date) <= 365
"""

# COMMAND ----------

control_responders_days_until_return   = spark.sql(query_1).toPandas()
treatment_responders_days_until_return = spark.sql(query_2).toPandas()

# COMMAND ----------

# tenure at cancellation
sns.set(rc={"figure.figsize":(15, 10)})

fig, ax = plt.subplots()

sns.histplot(control_responders_days_until_return.loc[control_responders_days_until_return["plan_initial_term_months"]==1,
                                                      "tenure_at_cancellation"], 
             stat="density", binwidth=28, label="control", color="orange", kde=True, ax=ax)
sns.histplot(treatment_responders_days_until_return.loc[treatment_responders_days_until_return["plan_initial_term_months"]==1,
                                                        "tenure_at_cancellation"], 
             stat="density", binwidth=28, label="treatment", color="blue", kde=True, ax=ax)
plt.legend()
plt.show()

# COMMAND ----------

# tenure at cancellation

sns.set(rc={"figure.figsize":(15, 10)})

fig, ax = plt.subplots()

sns.histplot(control_responders_days_until_return.loc[control_responders_days_until_return["plan_initial_term_months"]==12,
                                                      "tenure_at_cancellation"], 
             stat="density", binwidth=28, label="control", color="orange", kde=True, ax=ax)
sns.histplot(treatment_responders_days_until_return.loc[treatment_responders_days_until_return["plan_initial_term_months"]==12,
                                                        "tenure_at_cancellation"], 
             stat="density", binwidth=28, label="treatment", color="blue", kde=True, ax=ax)
plt.legend()
plt.show()

# COMMAND ----------

sns.set(rc={"figure.figsize":(15, 10)})

fig, ax = plt.subplots()

sns.histplot(treatment_responders_days_until_return.loc[(treatment_responders_days_until_return["days_until_return__from_previous_subscription_end_date"]<=35)&
                                                        (treatment_responders_days_until_return["plan_initial_term_months"]==1),
                                                        "tenure_at_cancellation"], 
             stat="density", binwidth=28, label="users that returned within winback window", color="blue", kde=True, ax=ax)
sns.histplot(treatment_responders_days_until_return.loc[(treatment_responders_days_until_return["days_until_return__from_previous_subscription_end_date"]>35)&
                                                        (treatment_responders_days_until_return["plan_initial_term_months"]==1),
                                                        "tenure_at_cancellation"], 
             stat="density", binwidth=28, label="users that returns outside of winback window", color="orange", kde=True, ax=ax)
plt.legend()
plt.show()

# COMMAND ----------

sns.set(rc={"figure.figsize":(15, 10)})

fig, ax = plt.subplots()

sns.scatterplot(treatment_responders_days_until_return.loc[(treatment_responders_days_until_return["plan_initial_term_months"]==1) &
                                                           (treatment_responders_days_until_return["tenure_at_cancellation"]>0),
                                                           "tenure_at_cancellation"],
                treatment_responders_days_until_return.loc[(treatment_responders_days_until_return["plan_initial_term_months"]==1) &
                                                           (treatment_responders_days_until_return["tenure_at_cancellation"]>0),
                                                           "days_until_return__from_previous_subscription_end_date"])
plt.legend()
plt.show()

# COMMAND ----------

sns.set(rc={"figure.figsize":(15, 10)})

fig, ax = plt.subplots()

sns.histplot(treatment_responders_days_until_return.loc[treatment_responders_days_until_return["days_until_return__from_previous_subscription_end_date"]>0,
                                                        "days_until_return__from_previous_subscription_end_date"], 
             stat="density", binwidth=3, label="", color="orange", kde=True, ax=ax)
sns.histplot(treatment_responders_days_until_return.loc[treatment_responders_days_until_return["days_until_return__from_previous_subscription_end_date"]>0,
                                                        "days_until_return__from_previous_subscription_end_date"], 
             stat="density", binwidth=3, label="treatment", color="blue", kde=True, ax=ax)
plt.legend()
plt.show()

# COMMAND ----------

sns.histplot(control_responders_days_until_return.loc[control_responders_days_until_return["days_until_return__from_previous_subscription_end_date"]>0,
                                                      "days_until_return__from_previous_subscription_end_date"], 
             stat="density", binwidth=3, label="control", color="blue", kde=True, ax=ax)

# COMMAND ----------

# MAGIC %md
# MAGIC #### days until return - relative to first email received date

# COMMAND ----------

sns.set(rc={"figure.figsize":(15, 10)})

sns.histplot(treatment_responders_days_until_return.loc[treatment_responders_days_until_return["days_until_return__from_previous_subscription_end_date"]>0,
                                                        "days_until_return__from_previous_subscription_end_date"], 
             stat="density", binwidth=3, label="treatment", color="blue", kde=True)
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### days off service - take rate

# COMMAND ----------

len([i for i in np.arange(5, 370, 5, dtype=int)])

# COMMAND ----------

cut_bins     = [i for i in np.arange(0, 365, 5, dtype=int)]
score_labels = [i for i in np.arange(5, 375, 5, dtype=int)]
len(cut_bins)
len(score_labels)

# COMMAND ----------

365*2

# COMMAND ----------

# MAGIC %sql
# MAGIC select percentile_approx(day0_content_starts +
# MAGIC                          day1_content_starts +
# MAGIC                          day2_content_starts +
# MAGIC                          day3_content_starts +
# MAGIC                          day4_content_starts +
# MAGIC                          day5_content_starts +
# MAGIC                          day6_content_starts +
# MAGIC                          day7_content_starts, 0.5)
# MAGIC from ds_staging.features__engagement_first_7_days
# MAGIC where signup_date >= '2022-01-01';

# COMMAND ----------

pd.qcut(treatment_responders_days_until_return['days_until_email__from_subscription_end_date'], 
         q=10,
         duplicates="drop")

# COMMAND ----------

treatment_responders_days_until_return[["days_until_email__from_subscription_end_date"]].mean(axis=1)

# COMMAND ----------

treatment_responders_days_until_return.head()

# COMMAND ----------

index_sort = treatment_responders_days_until_return[["days_until_email__from_subscription_end_date"]].mean().sort_values().index

# COMMAND ----------

index_sort

# COMMAND ----------

treatment_responders_days_until_return.head()

# COMMAND ----------

sns.set(rc={"figure.figsize":(25, 10)})
cut_bins     = [i for i in np.arange(0, 730, 5, dtype=int)]
score_labels = [i for i in np.arange(0, 725, 5, dtype=int)]

treatment_responders_days_until_return['days_until_email__from_subscription_end_date_binned'] = \
  pd.qcut(treatment_responders_days_until_return['days_until_email__from_subscription_end_date'], 
          q=10,
          labels=False,
          duplicates="drop").astype(str)

# sort on the basis of mean
index_sort = treatment_responders_days_until_return[["days_until_email__from_subscription_end_date"]].mean().sort_values().index
 
# now applying the sorted indices to the data
treatment_responders_days_until_return_sorted = treatment_responders_days_until_return[index_sort]
#treatment_responders_days_until_return.head()

ax = sns.boxplot(
  x="days_until_email__from_subscription_end_date_binned", 
  y="days_until_return__from_first_email_date",
  data=treatment_responders_days_until_return_sorted.loc[(treatment_responders_days_until_return_sorted["days_until_email__from_subscription_end_date"] > 0)])
plt.show()

# COMMAND ----------

sns.set(rc={"figure.figsize":(15, 10)})

fig, ax = plt.subplots()

sns.histplot(treatment_responders_days_until_return.loc[treatment_responders_days_until_return["days_until_return__from_first_email_date"] > 0,
                                                        "days_until_return__from_first_email_date"], 
             stat="density", binwidth=3, label="treatment", color="blue", kde=True, ax=ax)
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### adhoc - median content start for rachel

# COMMAND ----------

# MAGIC %sql
# MAGIC with temp as (
# MAGIC   select s.hs_user_id,
# MAGIC          case 
# MAGIC            when e.hs_user_id is not null then coalesce(day0_content_starts,0) +
# MAGIC                                               coalesce(day1_content_starts,0) +
# MAGIC                                               coalesce(day2_content_starts,0) +
# MAGIC                                               coalesce(day3_content_starts,0) +
# MAGIC                                               coalesce(day4_content_starts,0) +
# MAGIC                                               coalesce(day5_content_starts,0) +
# MAGIC                                               coalesce(day6_content_starts,0) +
# MAGIC                                               coalesce(day7_content_starts,0)
# MAGIC            else 0
# MAGIC            end as week1_content_starts
# MAGIC   from silver.fact_subscription s
# MAGIC   left join ds_staging.features__engagement_first_7_days e
# MAGIC     on e.signup_date >= '2022-01-01'
# MAGIC     and s.hs_user_id = e.hs_user_id
# MAGIC   where s.dt >= '2022-01-01'
# MAGIC     and lower(s.subscription_type) = 'b2c'
# MAGIC     and lower(s.country_code) = 'us'
# MAGIC )
# MAGIC select percentile_approx(week1_content_starts,0.5)
# MAGIC from temp
