with

/******************************************************************************************************/
/******************************************Creditor Details********************************************/
/******************************************************************************************************/
creditor_details as (
select
	a.id as creditor_id
	,a.organisation_id
	,a.name as merchant_name
	,a.geo
	,a.merchant_category_code
	,a.merchant_category_code_description
	,a.creditor_risk_label_parent as merchant_risk_label
	,a.creditor_risk_label_detail as merchant_risk_label_description
	,a.most_recent_risk_label_created_at
	,case when a.creditor_risk_label_detail in ("in_administration","insolvency","restructuring","dissolved","liquidation","inactivity") then true else false end as insolvency_flag
	,a.creditor_created_date 
	,a.is_account_closed
	,a.is_payment_provider
  ,a.organisation_with_multiple_creditors
	,b.current_revenue_account_type as account_type
  ,b.current_state
  ,b.parent_account_id
  ,b.parent_account_name
	,b.is_cs_managed
  ,b.csm_owner_name
	,1 as var1
from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.d_creditor`  as a
left join dbt_core_model.d_organisation as b
on a.organisation_id = b.organisation_id
where not a.is_payment_provider)


/******************************************************************************************************/
/******************************************  FDS Exposure  ********************************************/
/******************************************************************************************************/
,exposure as (
select 
	creditor_id
	,amount_gbp as fds_exposure_current
from `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
qualify row_number() over (partition by creditor_id order by calculated_at_date desc) =1)


/******************************************************************************************************/
/******************************************   D&B Scores   ********************************************/
/******************************************************************************************************/
,db_failure as (
select 
    creditor_id
    ,dnb_assessment.failure_score.national_percentile as db_failure_score_current
    ,date(retrieved_at) as db_failure_score_current_date
    ,row_number() over (partition by creditor_id order by retrieved_at desc) as rowno

from  `gc-prd-risk-prod-gdia.dun_bradstreet_reports.dun_bradstreet_report__4` 
where dnb_assessment.failure_score.national_percentile is not null
qualify rowno = 1)


/******************************************************************************************************/
/******************************************   PD  Scores   ********************************************/
/******************************************************************************************************/
,PD_score as (
select 
  creditor_id
  ,prediction_date
  ,date(concat(substr(prediction_date,1,4),"-",substr(prediction_date,5,2),"-",substr(prediction_date,7,2))) as prediction_calendar_date
  ,probability as PD_score_latest
from `gc-prd-credit-risk-dev-81b5.pd_model.probability_of_default_model_predictions_historic`
qualify row_number() over (partition by creditor_id order by prediction_date desc)= 1 )


/******************************************************************************************************/
/******************************************  NB Balances   ********************************************/
/******************************************************************************************************/
,creditor_balances as (
select
	 owner_id as creditor_id 
	,calendar_date 
	,sum(balance_amount_sum_gbp) as balance_amount_sum_gbp
from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.scd_abacus_available_merchant_funds_daily`
where name = 'available_merchant_funds'
and calendar_date = current_date()-1
group by 1,2)


/******************************************************************************************************/
/******************************************    Payments    ********************************************/
/******************************************************************************************************/
,creditor_payments_temp as (select
	creditor_id
    ,sum(case when date(charge_date) between current_date() and current_date()+7 then amount_gbp else 0 end) as future_payments_7days

    ,sum(case when is_paid and date(charge_date) between current_date()-30 and current_date()-1 then 1 else 0 end) as merchant_payment_vol_last_30d
    ,sum(case when is_charged_back  and date(charged_back_date) between current_date()-30 and current_date()-1 then 1 else 0 end) as merchant_chargeback_vol_last_30d

    ,sum(case when is_paid and date(charge_date) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_payment_vol_last_90d
    ,sum(case when is_charged_back  and date(charged_back_date) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_chargeback_vol_last_90d
    ,sum(case when is_failed and date(failed_or_late_failure_date) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_failure_vol_last_90d
    ,sum(case when is_late_failure and date(failed_or_late_failure_date) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_late_failure_vol_last_90d
    ,sum(case when is_refunded  and date(refund_created_at) between current_date()-90 and current_date()-1 then 1  else 0 end) as merchant_refund_vol_last_90d

    ,sum(case when is_paid and date(charge_date)  between current_date()-365   and current_date()-1    then amount_gbp  else 0 end) as merchant_payment_amt_gbp_last_365d

from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.x_payments` 
where 
date(charge_date) between current_date()-365 and current_date()-1
or date(charged_back_date) between current_date()-90 and current_date()-1
or date(failed_or_late_failure_date) between current_date()-90 and current_date()-1
or date(refund_created_at) between current_date()-90 and current_date()-1

group by 1)


,creditor_payments as (
select
	creditor_id

	,future_payments_7days

	,SAFE_DIVIDE(merchant_chargeback_vol_last_30d,merchant_payment_vol_last_30d) as cb_rate_30days

	,SAFE_DIVIDE(merchant_chargeback_vol_last_90d,merchant_payment_vol_last_90d) as cb_rate_90days
	,SAFE_DIVIDE(merchant_failure_vol_last_90d,merchant_payment_vol_last_90d) as failure_rate_90days
	,SAFE_DIVIDE(merchant_late_failure_vol_last_90d,merchant_payment_vol_last_90d) as late_failure_rate_90days
	,SAFE_DIVIDE(merchant_refund_vol_last_90d,merchant_payment_vol_last_90d) as refund_rate_90days

	,merchant_payment_amt_gbp_last_365d

	from creditor_payments_temp)

/******************************************************************************************************/
/******************************   Additional CB Monitoring Data ***************************************/
/******************************************************************************************************/
,mcc_payments as (
									select 
										b.merchant_category_code
											,sum(case when is_paid          and date(charge_date)       between current_date()-365   and current_date()-1    then 1          else 0 end) as mcc_payment_vol_last_12m
											,sum(case when is_paid          and date(charge_date)       between current_date()-365   and current_date()-1    then amount_gbp else 0 end) as mcc_payment_amt_last_12m
											,sum(case when is_charged_back  and date(charged_back_date) between current_date()-365   and current_date()-1    then 1          else 0 end) as mcc_chargeback_vol_last_12m
											,sum(case when is_charged_back  and date(charged_back_date) between current_date()-365   and current_date()-1    then amount_gbp else 0 end) as mcc_chargeback_amt_last_12m
										,safe_divide(sum(case when is_charged_back  and date(charged_back_date) between current_date()-365   and current_date()-1    then 1          else 0 end),sum(case when is_paid          and date(charge_date)       between current_date()-365   and current_date()-1    then 1          else 0 end)) as mcc_chargeback_rate_vol_last_12m
									from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.x_payments` as a 
									left join (select * from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.d_creditor` where not is_payment_provider)  as b
									on a.creditor_id=b.id
									group by 1)

,portfolio_payments as (
												select
													1 as var1
														,sum(case when is_paid          and date(charge_date)       between current_date()-365   and current_date()-1    then 1          else 0 end) as portfolio_payment_vol_last_12m
														,sum(case when is_paid          and date(charge_date)       between current_date()-365   and current_date()-1    then amount_gbp else 0 end) as portfolio_payment_amt_last_12m
														,sum(case when is_charged_back  and date(charged_back_date) between current_date()-365   and current_date()-1    then 1          else 0 end) as portfolio_chargeback_vol_last_12m
														,sum(case when is_charged_back  and date(charged_back_date) between current_date()-365   and current_date()-1    then amount_gbp else 0 end) as portfolio_chargeback_amt_last_12m
													,safe_divide(sum(case when is_charged_back  and date(charged_back_date) between current_date()-365   and current_date()-1    then 1          else 0 end),sum(case when is_paid          and date(charge_date)       between current_date()-365   and current_date()-1    then 1          else 0 end)) as portfolio_chargeback_rate_vol_last_12m
												from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.x_payments` as a 
												left join (select * from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.d_creditor` where not is_payment_provider)  as b
												on a.creditor_id=b.id
												group by 1)

/******************************************************************************************************/
/*************************************  Historical Cases  *********************************************/
/******************************************************************************************************/
,chargeback_monitoring_tickets as (
																		select
																				JSON_VALUE(values, "$.creditor_id") AS creditor_id
																				,JSON_VALUE(values, "$.ticket_id") AS ticket_id
																				,cast(JSON_VALUE(values, "$.cb_rate_30days") as FLOAT64) AS chargeback_monitoring_rate_last
																				,date(runtime) AS chargeback_monitoring_date_last
																				,date_diff(current_date(),date(runtime),day) AS chargeback_monitoring_days_since
																				,true AS chargeback_monitoring_trigger_last

																		from `gc-prd-credit-risk-dev-81b5.otternet_dev.otternet_devlog` 
																		where logtype = "result"
																		and process_name = "credit_chargeback_monitoring"
																		qualify row_number() over (partition by creditor_id,process_name order by date(runtime) desc)=1 
)


/******************************************************************************************************/
/******************************************  Data Merge  **********************************************/
/******************************************************************************************************/
,data_merge as (
select 
	a.creditor_id 
	,a.organisation_id
	,a.merchant_name
	,a.geo
	,a.merchant_category_code
	,a.merchant_category_code_description
	,a.is_payment_provider
	,a.account_type
  ,a.merchant_risk_label
	,a.merchant_risk_label_description
	,a.most_recent_risk_label_created_at
	,a.insolvency_flag
  ,a.current_state
  ,a.is_cs_managed
	,a.csm_owner_name
  ,a.parent_account_id
  ,a.parent_account_name

	,b.fds_exposure_current

	,c.db_failure_score_current
	,c.db_failure_score_current_date
    
	,case when d.balance_amount_sum_gbp <0 then d.balance_amount_sum_gbp else 0 end as nb_balance_current

	,e.cb_rate_30days
	,e.merchant_payment_amt_gbp_last_365d
	,e.cb_rate_90days
	,e.failure_rate_90days as failure_rate_90days
	,e.late_failure_rate_90days as late_failure_rate_90days
	,e.refund_rate_90days as refund_rate_90days
	
	,f.mcc_payment_vol_last_12m
	,f.mcc_payment_amt_last_12m
	,f.mcc_chargeback_vol_last_12m
	,f.mcc_chargeback_amt_last_12m
	,f.mcc_chargeback_rate_vol_last_12m

	,g.portfolio_payment_vol_last_12m
	,g.portfolio_payment_amt_last_12m
	,g.portfolio_chargeback_vol_last_12m
	,g.portfolio_chargeback_amt_last_12m
	,g.portfolio_chargeback_rate_vol_last_12m

  ,case when f.mcc_chargeback_vol_last_12m >= 1000 then f.mcc_chargeback_rate_vol_last_12m else portfolio_chargeback_rate_vol_last_12m end as reference_cb_rate

  ,h.ticket_id
  ,h.chargeback_monitoring_rate_last
  ,h.chargeback_monitoring_date_last
  ,h.chargeback_monitoring_days_since
  ,h.chargeback_monitoring_trigger_last

	,i.PD_score_latest
	,i.prediction_calendar_date


from creditor_details  			               as a 
left join exposure   			               as b on a.creditor_id=b.creditor_id
left join db_failure 			               as c on a.creditor_id=c.creditor_id
left join creditor_balances		               as d on a.creditor_id=d.creditor_id
left join creditor_payments                    as e on a.creditor_id=e.creditor_id
left join mcc_payments			               as f on a.merchant_category_code=f.merchant_category_code
left join portfolio_payments                   as g on a.var1=g.var1
left join chargeback_monitoring_tickets  	   as h on a.creditor_id=h.creditor_id
left join PD_score													as i on a.creditor_id=i.creditor_id
)

