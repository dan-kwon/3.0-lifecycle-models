# Databricks notebook source
def get_target_event(cohort, country_code, subscription_type, begin_date, end_date, horizon_days):
  if cohort == "free trial drop out":
    query = f"""
      with 
      braze as (
        select external_user_id,
               canvas_variation_name,       
               min(case when event_name = 'users_canvas_entry' then b.dt else null end) canvas_entry_date,
               count(case when b.canvas_step_name like '%20%' then 1 else null end) num_offers_20,
               count(case when b.canvas_step_name like '%30%' then 1 else null end) num_offers_30,
               count(case when b.canvas_step_name like '%40%' then 1 else null end) num_offers_40
        from bronze.braze_events b
        where canvas_id = '216b94ab-34e3-4468-8811-5cb10e24bda8'
          and event_name in ('users_canvas_entry','users_messages_email_delivery')
          and b.dt between '{begin_date}' and '{end_date}'
        group by 1,2
      ),
      all_users as (
        select s.hs_user_id,
               b.canvas_variation_name,
               b.num_offers_20,
               b.num_offers_30,
               b.num_offers_40,
               b.canvas_entry_date,
               s.voucher_code,
               lead(s.voucher_code) over (partition by hs_user_id order by s.dt, s.created_timestamp, s.updated_timestamp) next_voucher_code,
               s.dt signup_date,
               s.free_trial_start_date,
               s.free_trial_end_date,
               s.paid_subscription_start_date,
               s.paid_subscription_end_date,
               s.created_timestamp,
               s.updated_timestamp,
               lead(s.free_trial_start_date) over (partition by hs_user_id order by s.dt, s.created_timestamp, s.updated_timestamp) next_free_trial_start_date,
               lead(s.free_trial_end_date) over (partition by hs_user_id order by s.dt, s.created_timestamp, s.updated_timestamp) next_free_trial_end_date,
               lead(s.paid_subscription_start_date) over (partition by hs_user_id order by s.dt, s.created_timestamp, s.updated_timestamp) next_paid_subscription_start_date,
               lead(s.paid_subscription_end_date) over (partition by hs_user_id order by s.dt, s.created_timestamp, s.updated_timestamp) next_paid_subscription_end_date,
               --lead(s.subscriber_status) over (partition by hs_user_id order by s.dt, s.created_timestamp, s.updated_timestamp) next_subscriber_status,
               row_number() over(partition by s.hs_user_id order by datediff(s.dt, b.canvas_entry_date)) sub_rank 
        from braze b
        inner join silver.fact_subscription s
          on b.external_user_id = s.hs_user_id
          and s.dt between b.canvas_entry_date and date_add(b.canvas_entry_date,42)
      ),
      control_nonresponders as (
        select 'CN' experiment_group, 
               coalesce(int(regexp_extract(lower(a.next_voucher_code), '([0-9]{{2}})(?:off)',1)),0) discount_redeemed,
               a.*
        from all_users a
        where canvas_variation_name = 'Control'
          and sub_rank = 1
          and free_trial_start_date is not null
          and free_trial_end_date <> coalesce(paid_subscription_start_date,'9999-12-31')
          and datediff(coalesce(next_paid_subscription_start_date, '9999-12-31'), free_trial_end_date) > {horizon_days}
      ),
      treatment_nonresponders as (
        select 'TN' experiment_group, 
               coalesce(int(regexp_extract(lower(a.next_voucher_code), '([0-9]{{2}})(?:off)',1)),0) discount_redeemed,
               a.*
        from all_users a 
        where canvas_variation_name <> 'Control'
          and sub_rank = 1
          and free_trial_start_date        is not null
          and paid_subscription_start_date is null
          and datediff(coalesce(next_paid_subscription_start_date, '9999-12-31'), free_trial_end_date) > {horizon_days}
      ),
      control_responders as (
        select 'CR' experiment_group,
               coalesce(int(regexp_extract(lower(a.next_voucher_code), '([0-9]{{2}})(?:off)',1)),0) discount_redeemed,
               a.*
        from all_users a
        where canvas_variation_name = 'Control'
          and sub_rank = 1
          and free_trial_start_date        is not null
          and next_free_trial_start_date   is null
          and paid_subscription_start_date is null
          and datediff(coalesce(next_paid_subscription_start_date, '9999-12-31'), free_trial_end_date) <= {horizon_days}
          --and int(regexp_extract(lower(a.next_voucher_code), '([0-9]{{2}})(?:off)',1)) is null
      ),
      treatment_responders as (
        select 'TR' experiment_group, 
               int(regexp_extract(lower(a.next_voucher_code), '([0-9]{{2}})(?:off)',1)) discount_redeemed,
               a.*
        from all_users a 
        where canvas_variation_name <> 'Control'
          and sub_rank = 1
          and free_trial_start_date        is not null
          and next_free_trial_start_date   is null
          and paid_subscription_start_date is null
          and datediff(coalesce(next_paid_subscription_start_date, '9999-12-31'), free_trial_end_date) <= {horizon_days}
          --and int(regexp_extract(lower(a.next_voucher_code), '([0-9]{{2}})(?:off)',1)) is not null
      )
      select hs_user_id,
             signup_date,
             experiment_group,
             discount_redeemed,
             num_offers_20,
             num_offers_30,
             num_offers_40
      from control_nonresponders
      union all 
      select hs_user_id,
             signup_date,
             experiment_group,
             discount_redeemed,
             num_offers_20,
             num_offers_30,
             num_offers_40
      from treatment_nonresponders
      union all 
      select hs_user_id,
             signup_date,
             experiment_group,
             discount_redeemed,
             num_offers_20,
             num_offers_30,
             num_offers_40
      from control_responders
      union all 
      select hs_user_id,
             signup_date,
             experiment_group,
             discount_redeemed,
             num_offers_20,
             num_offers_30,
             num_offers_40
      from treatment_responders
      """
    return spark.sql(query)
  elif cohort == "winback":
    print("WIP")
    return None
  else:
    print("event of interest is not yet defined")
    return None  
