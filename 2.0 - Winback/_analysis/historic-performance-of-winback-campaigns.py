# Databricks notebook source
# MAGIC %sql
# MAGIC with 
# MAGIC subscriptions as (
# MAGIC     select hs_user_id,
# MAGIC            dt subscription_start_date,
# MAGIC            free_trial_start_date,
# MAGIC            free_trial_end_date,
# MAGIC            paid_subscription_start_date,
# MAGIC            paid_subscription_end_date,
# MAGIC            lead(dt) over(partition by hs_user_id order by dt) next_subscription_start_date,
# MAGIC            lead(paid_subscription_start_date) over(partition by hs_user_id order by dt) next_paid_subscription_start_date,
# MAGIC            lead(paid_subscription_end_date) over(partition by hs_user_id order by dt) next_paid_subscription_end_date
# MAGIC     from silver.fact_subscription
# MAGIC ),
# MAGIC canvas as (
# MAGIC     select id canvas_id,
# MAGIC            name,
# MAGIC            created_at::date created_date,
# MAGIC            first_entry::date first_entry_date,
# MAGIC            last_entry::date last_entry_date
# MAGIC     from bronze.braze_canvas
# MAGIC     where lower(name) like '%winback%'
# MAGIC ),
# MAGIC canvas_entries as (
# MAGIC     select s.hs_user_id,
# MAGIC            e.dt canvas_entry_date,
# MAGIC            e.canvas_name,
# MAGIC            e.canvas_id,
# MAGIC            e.canvas_variation_name,
# MAGIC            c.created_date canvas_created_date,
# MAGIC            s.subscription_start_date,
# MAGIC            s.free_trial_start_date,
# MAGIC            s.free_trial_end_date,
# MAGIC            s.paid_subscription_start_date,
# MAGIC            s.paid_subscription_end_date,
# MAGIC            s.next_subscription_start_date,
# MAGIC            s.next_paid_subscription_start_date,
# MAGIC            s.next_paid_subscription_end_date
# MAGIC     from bronze.braze_events e
# MAGIC     inner join canvas c
# MAGIC       on e.canvas_id = c.canvas_id
# MAGIC     inner join subscriptions s
# MAGIC       on s.hs_user_id = e.external_user_id
# MAGIC       and e.dt between s.subscription_start_date and coalesce(s.next_subscription_start_date,'9999-12-31')
# MAGIC     where e.event_name = 'users_canvas_entry'
# MAGIC       --and e.dt between '2021-01-01' and '2021-12-31'
# MAGIC )
# MAGIC select c.canvas_name,
# MAGIC        c.canvas_created_date,
# MAGIC        count(case when lower(c.canvas_variation_name) <> 'control' and event_name = 'users_canvas_entry' then 1 else null end) treatment_canvas_entries,
# MAGIC        count(case when lower(c.canvas_variation_name) <> 'control' and event_name = 'users_messages_email_delivery' then 1 else null end) treatment_emails_delivered,
# MAGIC        count(case when lower(c.canvas_variation_name) <> 'control' and event_name = 'users_messages_email_open' then 1 else null end) treatment_emails_opened,
# MAGIC        count(case when lower(c.canvas_variation_name) <> 'control' and event_name = 'users_messages_email_click' then 1 else null end) treatment_emails_clicked,
# MAGIC        count(case when lower(c.canvas_variation_name) <> 'control' and event_name = 'users_messages_email_unsubscribe' then 1 else null end) treatment_emails_unsubscribed,
# MAGIC        count(case when lower(c.canvas_variation_name) <> 'control' and event_name = 'users_messages_email_mark_as_spam' then 1 else null end) treatment_emails_marked_as_spam,
# MAGIC        count(case when lower(c.canvas_variation_name) <> 'control' and event_name = 'users_messages_webhook_send' then 1 else null end) treatment_webhook_sent,
# MAGIC        count(case when lower(c.canvas_variation_name) <> 'control' and event_name = 'users_canvas_conversion' then 1 else null end) treatment_canvas_conversion,
# MAGIC        count(distinct case when lower(c.canvas_variation_name) <> 'control' and datediff(coalesce(next_paid_subscription_start_date, '9999-12-31'), canvas_entry_date) <= 28 then c.hs_user_id else null end) treatment_conversions,
# MAGIC        count(case when lower(c.canvas_variation_name) = 'control' and event_name = 'users_canvas_entry' then 1 else null end) control_canvas_entries,
# MAGIC        count(case when lower(c.canvas_variation_name) = 'control' and event_name = 'users_messages_email_delivery' then 1 else null end) control_emails_delivered,
# MAGIC        count(case when lower(c.canvas_variation_name) = 'control' and event_name = 'users_messages_email_open' then 1 else null end) control_emails_opened,
# MAGIC        count(case when lower(c.canvas_variation_name) = 'control' and event_name = 'users_messages_email_click' then 1 else null end) control_emails_clicked,
# MAGIC        count(case when lower(c.canvas_variation_name) = 'control' and event_name = 'users_messages_email_unsubscribe' then 1 else null end) control_emails_unsubscribed,
# MAGIC        count(case when lower(c.canvas_variation_name) = 'control' and event_name = 'users_messages_email_mark_as_spam' then 1 else null end) control_emails_marked_as_spam,
# MAGIC        count(case when lower(c.canvas_variation_name) = 'control' and event_name = 'users_messages_webhook_send' then 1 else null end) control_webhook_sent,
# MAGIC        count(case when lower(c.canvas_variation_name) = 'control' and event_name = 'users_canvas_conversion' then 1 else null end) control_canvas_conversion,
# MAGIC        count(distinct case when lower(c.canvas_variation_name) = 'control' and datediff(coalesce(next_paid_subscription_start_date, '9999-12-31'), canvas_entry_date) <= 28 then c.hs_user_id else null end) control_conversions
# MAGIC from canvas_entries c
# MAGIC inner join bronze.braze_events e
# MAGIC   on c.hs_user_id = e.external_user_id
# MAGIC   and c.canvas_id = e.canvas_id
# MAGIC group by 1,2
# MAGIC order by 2 desc

# COMMAND ----------


