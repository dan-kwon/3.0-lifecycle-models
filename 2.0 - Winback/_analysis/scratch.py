# Databricks notebook source
# MAGIC %md
# MAGIC #### Take rate vs. days after cancel

# COMMAND ----------

sql
with empty_rows as (
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n
  )
  
      select row_number() over(partition by e1.n order by e1.n) days
      from empty_rows e1
      join empty_rows e2
      join empty_rows e3

# COMMAND ----------

# MAGIC %sql
# MAGIC with 
# MAGIC   empty_rows as (
# MAGIC       select 1 n union all
# MAGIC       select 1 n union all
# MAGIC       select 1 n union all
# MAGIC       select 1 n union all
# MAGIC       select 1 n union all
# MAGIC       select 1 n union all
# MAGIC       select 1 n union all
# MAGIC       select 1 n union all
# MAGIC       select 1 n union all
# MAGIC       select 1 n
# MAGIC   ),
# MAGIC   days_after_cancel as (
# MAGIC       select row_number() over(partition by e1.n order by e1.n) days
# MAGIC       from empty_rows e1
# MAGIC       join empty_rows e2
# MAGIC       join empty_rows e3
# MAGIC   ),
# MAGIC   braze as (
# MAGIC       select external_user_id,
# MAGIC              canvas_variation_name,
# MAGIC              min(case when event_name = 'users_messages_email_delivery' then b.dt else null end) as first_email_date,
# MAGIC              min(case when event_name = 'users_canvas_entry' then b.dt else null end) as canvas_entry_date,
# MAGIC              min(case when canvas_variation_name =  'Control' and event_name = 'users_canvas_entry' then b.dt 
# MAGIC                       when canvas_variation_name <> 'Control' and event_name = 'users_messages_email_delivery' then b.dt 
# MAGIC                       else null end) as test_date,
# MAGIC              count(case when b.canvas_step_name like '%10%' then 1 else null end) num_offers_10,
# MAGIC              count(case when b.canvas_step_name like '%20%' then 1 else null end) num_offers_20,
# MAGIC              count(case when b.canvas_step_name like '%30%' then 1 else null end) num_offers_30,
# MAGIC              count(case when b.canvas_step_name like '%40%' then 1 else null end) num_offers_40
# MAGIC       from bronze.braze_events b
# MAGIC       where lower(canvas_name) like '%winback%'
# MAGIC         and event_name in ('users_canvas_entry','users_messages_email_delivery')
# MAGIC       group by 1,2
# MAGIC       having canvas_entry_date >= '2021-01-01'
# MAGIC   ),
# MAGIC   subscriptions as (
# MAGIC       select s.hs_user_id,
# MAGIC              s.voucher_code,
# MAGIC              s.plan_initial_term_months,
# MAGIC              s.dt,
# MAGIC              s.free_trial_start_date,
# MAGIC              s.free_trial_end_date,
# MAGIC              s.paid_subscription_start_date,
# MAGIC              s.paid_subscription_end_date,
# MAGIC              s.created_timestamp,
# MAGIC              s.updated_timestamp,
# MAGIC              lead(s.paid_subscription_start_date) over (partition by s.hs_user_id order by s.paid_subscription_start_date) next_paid_subscription_start_date,
# MAGIC              lead(s.voucher_code) over (partition by hs_user_id order by s.dt, s.created_timestamp, s.updated_timestamp) next_voucher_code
# MAGIC       from silver.fact_subscription s
# MAGIC       where lower(s.country_code) = 'us'
# MAGIC         and lower(s.subscription_type) = 'b2c'
# MAGIC   ),
# MAGIC   all_users as (
# MAGIC       select s.*,
# MAGIC            b.first_email_date,
# MAGIC            b.canvas_variation_name,
# MAGIC            b.num_offers_10,
# MAGIC            b.num_offers_20,
# MAGIC            b.num_offers_30,
# MAGIC            b.num_offers_40,
# MAGIC            b.canvas_entry_date,
# MAGIC            row_number() over(partition by s.hs_user_id order by datediff(b.canvas_entry_date,s.paid_subscription_end_date)) sub_rank
# MAGIC       from braze b
# MAGIC       inner join subscriptions s
# MAGIC         on b.external_user_id = s.hs_user_id
# MAGIC         and b.canvas_entry_date >= s.paid_subscription_end_date
# MAGIC         and s.paid_subscription_end_date between '2021-01-01' and '2022-05-01'
# MAGIC         and datediff(s.paid_subscription_end_date, s.paid_subscription_start_date) >= 28
# MAGIC   ),
# MAGIC tmp as (
# MAGIC     select hs_user_id,
# MAGIC            paid_subscription_end_date,
# MAGIC            next_paid_subscription_start_date,
# MAGIC            voucher_code,
# MAGIC            next_voucher_code,
# MAGIC            first_email_date,
# MAGIC            datediff(first_email_date, paid_subscription_end_date) days_since_cancel,
# MAGIC            case when datediff(coalesce(next_paid_subscription_start_date,'9999-12-31'), first_email_date) <=28 then 1 else 0 end convert_28day_ind
# MAGIC     from all_users a
# MAGIC     where (lower(canvas_variation_name) like '%winback%' or lower(canvas_variation_name) like '%10%' or lower(canvas_variation_name) like '%40%')
# MAGIC       and num_offers_10 + num_offers_20 + num_offers_30 + num_offers_40 > 0
# MAGIC       and sub_rank = 1
# MAGIC       and paid_subscription_end_date is not null
# MAGIC       and coalesce(next_paid_subscription_start_date,'9999-12-31') >= paid_subscription_end_date
# MAGIC   )
# MAGIC select next_voucher_code,
# MAGIC        count(distinct hs_user_id) num_returns
# MAGIC from tmp
# MAGIC group by 1

# COMMAND ----------

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure

query_1 = f"""
  with 
  empty_rows as (
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n
  ),
  days_after_cancel as (
      select row_number() over(partition by e1.n order by e1.n) days
      from empty_rows e1
      join empty_rows e2
      join empty_rows e3
  ),
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
        and s.paid_subscription_end_date between '2021-01-01' and '2022-05-01'
        and datediff(s.paid_subscription_end_date, s.paid_subscription_start_date) >= 28
  )
  --tmp as (
      select hs_user_id,
             paid_subscription_end_date,
             next_paid_subscription_start_date,
             first_email_date,
             datediff(first_email_date, paid_subscription_end_date) days_since_cancel,
             case when lower(coalesce(next_voucher_code,'NA')) like '%winback%' and datediff(coalesce(next_paid_subscription_start_date,'9999-12-31'), first_email_date) <=28 then 1 else 0 end convert_28day_ind
      from all_users a
      where (lower(canvas_variation_name) like '%winback%' or lower(canvas_variation_name) like '%10%' or lower(canvas_variation_name) like '%40%')
        and num_offers_10 + num_offers_20 + num_offers_30 + num_offers_40 > 0
        and sub_rank = 1
        and paid_subscription_end_date is not null
        and coalesce(next_paid_subscription_start_date,'9999-12-31') >= paid_subscription_end_date
  --)
  --select floor((days_since_cancel-1)/10)+1,
  --       avg(convert_28day_ind),
  --       count(distinct hs_user_id) num_users
  --from tmp
  --group by 1
  --order by 1
"""

query_2 = f"""
  with 
  empty_rows as (
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n union all
      select 1 n
  ),
  days_after_cancel as (
      select row_number() over(partition by e1.n order by e1.n) days
      from empty_rows e1
      join empty_rows e2
      join empty_rows e3
  ),
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
        and s.paid_subscription_end_date between '2021-01-01' and '2022-05-01'
        and datediff(s.paid_subscription_end_date, s.paid_subscription_start_date) >= 28
  )
  --tmp as (
      select hs_user_id,
             paid_subscription_end_date,
             next_paid_subscription_start_date,
             first_email_date,
             datediff(date_add(paid_subscription_end_date,d.days), paid_subscription_end_date) days_since_cancel,
             case when datediff(coalesce(next_paid_subscription_start_date,'9999-12-31'), date_add(paid_subscription_end_date,d.days)) <=28 then 1 else 0 end convert_28day_ind
      from all_users a
      join days_after_cancel d
        on d.days <= 365
      where lower(canvas_variation_name) like '%control%'
        and num_offers_10 + num_offers_20 + num_offers_30 + num_offers_40 = 0
        and sub_rank = 1
        and paid_subscription_end_date is not null
        and coalesce(next_paid_subscription_start_date,'9999-12-31') >= paid_subscription_end_date
  --)
  --select floor((days_since_cancel-1)/10)+1,
  --       avg(convert_28day_ind),
  --       count(distinct hs_user_id) num_users
  --from tmp
  --group by 1
  --order by 1
"""

df_treatment_group = spark.sql(query_1).toPandas()
df_control_group   = spark.sql(query_2).toPandas()

df_treatment_group = df_treatment_group.loc[(df_treatment_group["days_since_cancel"]>=0) & (df_treatment_group["days_since_cancel"]<=365),:]
df_treatment_group['days_since_cancel_bin'] = pd.cut(df_treatment_group['days_since_cancel'], bins=5)

df_control_group = df_control_group.loc[(df_control_group["days_since_cancel"]>=0) & (df_control_group["days_since_cancel"]<=365),:]
df_control_group['days_since_cancel_bin'] = pd.cut(df_control_group['days_since_cancel'], bins=5)

# COMMAND ----------

df_treatment_group['days_since_cancel_bin'] = pd.qcut(df_treatment_group['days_since_cancel'], q=8, duplicates="drop")
df_treatment_group.groupby(by="days_since_cancel_bin").mean("convert_28day_ind").reset_index(0, drop=False).head()

# COMMAND ----------

df_control_group['days_since_cancel_bin'] = pd.qcut(df_control_group['days_since_cancel'], q=8, duplicates="drop")
df_control_group.groupby(by="days_since_cancel_bin").mean("convert_28day_ind").reset_index(0, drop=False).head()

# COMMAND ----------

# take rate vs. days after cancellation at which email was sent
sns.set(rc={"figure.figsize":(15, 10)})
fig, ax = plt.subplots()
ax = sns.barplot(x="days_since_cancel_bin", 
                 y="convert_28day_ind", 
                 color="b",
                 alpha=0.6,
                 label="treatment",
                 data=df_treatment_group.groupby(by="days_since_cancel_bin").mean("convert_28day_ind").reset_index(0, drop=False))
#ax = sns.barplot(x="days_since_cancel", 
#                 y="convert_28day_ind", 
#                 color="r",
#                 alpha=0.6,
#                 label="control",
#                 data=df_control_group.groupby(by="days_since_cancel_bin").mean("convert_28day_ind").reset_index(0, drop=False))
plt.legend()
plt.show()

# COMMAND ----------

# take rate vs. days after cancellation at which email was sent
sns.set(rc={"figure.figsize":(15, 10)})
fig, ax = plt.subplots()
ax = sns.barplot(x="days_since_cancel_bin", 
                 y="convert_28day_ind", 
                 color="r",
                 alpha=0.6,
                 label="control",
                 data=df_control_group.groupby(by="days_since_cancel_bin").mean("convert_28day_ind").reset_index(0, drop=False))
plt.legend()
plt.show()

# COMMAND ----------

df_treatment_group[["days_since_cancel"]].head()

# COMMAND ----------

# take rate vs. days after cancellation at which email was sent
sns.set(rc={"figure.figsize":(15, 10)})
fig, ax = plt.subplots()
bins = np.histogram_bin_edges(df_treatment_group.loc[(df_treatment_group["days_since_cancel"]<=365),"days_since_cancel"], bins=50)
ax = sns.histplot(x="days_since_cancel", 
                  alpha=0.6,
                  data=df_treatment_group.loc[(df_treatment_group["days_since_cancel"]<=365) & 
                                              (df_treatment_group["convert_28day_ind"]==1)],
                  stat="probability",
                  label="converted",
                  bins=bins
                  #hue="convert_28day_ind"
                 )
ax = sns.histplot(x="days_since_cancel", 
                  alpha=0.4,
                  data=df_treatment_group.loc[(df_treatment_group["days_since_cancel"]<=365) & 
                                              (df_treatment_group["convert_28day_ind"]==0)],
                  stat="probability",
                  label="not converted",
                  color="red",
                  bins=bins
                 )
plt.legend()
plt.show()

# COMMAND ----------

bins

# COMMAND ----------


