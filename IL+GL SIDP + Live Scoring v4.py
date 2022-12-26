#!/usr/bin/env python
# coding: utf-8

# In[10]:


#SIDP

import pandas as pd
import numpy as np
import re
get_ipython().system('pip install connectorx')
import connectorx as cx
import pickle
from google.colab import drive
drive.mount('/content/drive')
import urllib.parse
import sqlalchemy as db
get_ipython().system('pip install mysql.connector')
import mysql.connector as sql
get_ipython().system('pip install pymysql')
import pymysql
import logging
import datetime
from datetime import date
from dateutil.relativedelta import *
import xgboost
import math
import json
import requests
import pytz
local_tz = pytz.timezone('Asia/Yangon')

log = logging.getLogger("my-logger")
log.setLevel(logging.DEBUG)

# [2022-06-28, Naing] Dashboard database write access API - ONLY for hwa_cs_output and hwa_credit_score tables.
db_api_host = "https://webappapi.hanamicrofinance.net"
db_api_root = "api/v1"

db_connection_str = 'mysql://kebwebuser:u5erMeb%40pp@hana-production-analytics-read-replica.c0wcwq0ocdj3.ap-southeast-1.rds.amazonaws.com/kebhana_dashboard_db'

def try_float(x):
    try:
        return x.astype(float)
    except ValueError:
        return x

def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3

def age(born):
    today = datetime.date.today()
    return (today.year - born.year)

def removebrackets(x):
    try:
        return re.sub(r"\([^()]*\)", "", x).strip()
    except ValueError:
        return x


def processcomponent_efm_monthly_income(x):
    pattern = "(name: efm_monthly_income, label: လစဉ်ဝင်ငွေပမာဏ, value: \d+)"
    match= re.findall(pattern, x)
    matchflat = " ".join(match)  
    return sum([int(i) for i in re.findall(r'\d+', matchflat)])


def processcomponent_efm_relation(x):
    FAmatchlist = ["Spouse", "Children", "Parent","Sibling", "Nephew/Niece","Others","In-Laws"]
    FAtoZQ_map = {"Spouse":"Spouse", "Others": "Other relatives", "Sibling":"Siblings","Children":"Child", "Parent":"Parents", "In-Laws":"In-laws",
                  "Nephew/Niece":"Other relatives"}
    concatstr=""
    for item in FAmatchlist:
      if item in x:
        concatstr = concatstr +  FAtoZQ_map.get(item) + "\\n" 
    return concatstr

def processcomponent_efm_age(x):
    pattern = "(name: efm_age, label: အသက်, value: \d+)"
    match = re.findall(pattern, x)
    matchflat = " ".join(match)  
    return np.mean([int(i) for i in re.findall(r'\d+', matchflat)])


def get_closest_MCIX_date(row,MCIX_data):
    sorted_MCIX_filedates = sorted(MCIX_data["mcix_file_date"][MCIX_data["client_id"]==row["client_id"]])
    filteredlist = [x for x in sorted_MCIX_filedates if (x >= row["interview_date"]-datetime.timedelta(days=6*30)) and (x <= row["interview_date"])]
    if len(filteredlist) == 0:
      return np.nan
    else:    
      return np.max(filteredlist)


def generate_loanperf_inputs(row,longprocessingdf):
    sorted_reference_dates = (longprocessingdf[["client_id","reference_date","late_days_total","paid_pc_due_interestprincipal","paid_pc_paid","paid_pc","reschedule_count_total"]]                              [(longprocessingdf["client_id"]==row["client_id"]) & (longprocessingdf["reference_date"]<row["interview_date"])]).sort_values(by = ["reference_date"])
    
    #latedays
    totaldayslate_previousloans = sorted_reference_dates["late_days_total"].sum()
    if len(sorted_reference_dates) == 0:
      totaldayslate_lastloan = 0
    else:
      totaldayslate_lastloan = sorted_reference_dates["late_days_total"].tolist()[-1]

    #paid_pc
    totalpaidpc_previousloans = (sorted_reference_dates["paid_pc_paid"].sum() / sorted_reference_dates["paid_pc_due_interestprincipal"].sum())
    totalpaidpc_previousloans = np.nan_to_num(totalpaidpc_previousloans,nan=1)
    if len(sorted_reference_dates) == 0:
      totalpaidpc_lastloan = 1
    else:
      totalpaidpc_lastloan = sorted_reference_dates["paid_pc"].tolist()[-1]

    #reschedule_count
    totalreschedules_previousloans = sorted_reference_dates["reschedule_count_total"].sum()
    if len(sorted_reference_dates) == 0:
      reschedule_count_lastloan = 0
    else:
      reschedule_count_lastloan = sorted_reference_dates["reschedule_count_total"].tolist()[-1]

    return pd.Series([totaldayslate_previousloans,totaldayslate_lastloan,totalpaidpc_previousloans,totalpaidpc_lastloan,                      totalreschedules_previousloans,reschedule_count_lastloan])



#GL run_scorecard lists
GL_categorical_list =[
  'agri_expense_question_15',
  'agri_practices_question_5',
  'agri_practices_question_7',
  'ai_harvest_month',
  'app_install_time',
  'app_not_install_reason',
  'as_house_type',
  'bp_applicant_name_in_license',
  'bp_business_nature_agriculture',
  'bp_business_nature_livestock',
  'bp_business_nature_production',
  'bp_business_nature_service',
  'bp_business_nature_trading',
  'bp_business_ownership',
  'bp_business_ownership_premise',
  'bp_business_sector',
  'bp_competition',
  'bp_license_issuer',
  'bp_location',
  'bp_maintenance_of_records',
  'bp_natural_factor_impact',
  'bp_operation_length',
  'bp_owner_involvement',
  'bp_relationship_with_stakeholders',
  'bp_sale_method',
  'bp_start_year_in_current_location',
  'bp_who_help_difficulties',
  'br_business_operating_years',
  'br_client_business_experience',
  'br_client_own_business',
  'br_how_long_know_client',
  'br_relationship_with_stakeholders',
  'coa_education',
  'coa_gender',
  'coa_house_ownership',
  'coa_nrc_type',
  'coa_occupation',
  'coa_phone',
  'coa_relationship',
  'fr_client_own_house',
  'fr_family_background_character',
  'fr_how_long_he_stay_there',
  'fr_how_long_know_client',
  'guarantor_gender',
  'guarantor_nrc_type',
  'guarantor_phone',
  'guarantor_religion',
  'guarantor_salary_slip',
  'hd_family_size',
  'hd_num_of_dependents',
  'hd_num_of_healthy_family_member',
  'hd_num_of_students',
  'hv_house_doc',
  'hv_house_ownership',
  'hv_house_roof',
  'hv_house_type',
  'hv_house_type_story',
  'hv_years_stay',
  'ls_loan_product',
  'pd_bank_name',
  'pd_education_level',
  'pd_know_about_hana',
  'pd_marital_status',
  'pd_religion',
  'client_division',
  'client_gender_cv_id',
  'client_mobile_no',
  'client_type_cv_id']
GL_binary_list =['abp_grow_maize',
  'api_have_land_title',
  'api_own_land_plan',
  'api_use_certified_seeds',
  'app_client_app_install',
  'bp_bank_account',
  'bp_is_licensed',
  'bp_same_location_business_home',
  'br_comments',
  'cd_client_building_2',
  'cd_house_document_1',
  'cd_vehicle_1',
  'fr_comments',
  'guarantor_marital_status',
  'guarantor_salaried_employee',
  'ld_business_building',
  'ld_business_vehicle_1',
  'ld_business_vehicle_2',
  'ld_farm_document_1',
  'ld_free_photo_1',
  'ld_free_photo_2',
  'ld_free_photo_3',
  'ld_other_income_business_doc_1',
  'ld_other_income_business_doc_2',
  'ld_photo_house_front',
  'ld_photo_house_side',
  'ld_prove_house_doc',
  'ld_request_business_license',
  'ld_requested_business_photo_1',
  'ld_requested_business_photo_2',
  'pd_bank_account',
  'has_other_loans',
  'efm_age',
  'as_vehicles_owned_Bicycle',
  'as_vehicles_owned_Car',
  'as_vehicles_owned_Motorbike',
  'as_vehicles_owned_Others',
  'hv_household_assets_Bed',
  'hv_household_assets_Cupboard',
  'hv_household_assets_DVDMusicsystem',
  'hv_household_assets_Four-wheeler',
  'hv_household_assets_GasCylinder',
  'hv_household_assets_Microwave',
  'hv_household_assets_Others',
  'hv_household_assets_Refrigerator',
  'hv_household_assets_SewingMachine',
  'hv_household_assets_Sofa',
  'hv_household_assets_TableChair',
  'hv_household_assets_Two-wheeler',
  'hv_household_assets_Washingmachine',
  'hv_services_available_Gas',
  'hv_services_available_Landline',
  'ls_loan_specific_use_Inventory',
  'ls_loan_specific_use_WorkingCapital',
  'bp_good_time_for_visit_afternoon',
  'bp_good_time_for_visit_evening',
  'bp_good_time_for_visit_morning',
  'bp_location_advantages_Close to raw material/supplier',
  'bp_location_advantages_Market area',
  'bp_location_advantages_Transportation facilities']
GL_numerical_list=[
  'abp_land_size_in_acre',
  'agri_practices_question_3',
  'ai_total_yeild_last_year',
  'ail_cf_monthly_net_income',
  'api_distance_to_reach_plot',
  'api_land_size_of_plot_acre',
  'api_value_of_plot',
  'bil_cf_monthly_net_income',
  'bp_capital',
  'bp_num_employees',
  'bp_operation_length_agri',
  'hd_family_size_agri',
  'hv_house_value',
  'ls_loan_amount_requested',
  'ls_loan_term',
  'lta_farm_size',
  'client_dob',
  'loan_counter',
  'loan_officer_experience',
  'office_opening_date',
  'MCIX_Delinquent_Count',
  'MCIX_Overlap_Count',
  'repayment_rate',
  'par',
  'vehicle_count',
  'househould_asset_count',
  'househould_services_count',
  'loan_use_count',
  'efm_relation_count',
  'business_area',
  'house_area',
  'loanamount_suggested_requested_ratio',
  'num_business_types',
  'guarantor_monthly_disposable_income',
  'applicant_networth',
  'applicant_monthly_disposable_income',
  'applicant_income_to_expense_ratio',
  'applicant_months_of_disposableincome_to_assets',
  'applicant_months_of_disposableincome_for_totalrepayment',
  'house_value_ov_net_income',
  'house_value_ov_req_loan_amt',
  'farm_value_ov_net_income',
  'farm_value_ov_req_loan_amt',
  'farm_value_ov_farm_area',
  'housevalue_ov_house_area',
  'assets_ov_req_loan_amt',
  'guarantor_months_of_disposableincome_to_assets',
  'vehicle_value_ov_vehicle_count',
  "totaldayslate_previousloans","totaldayslate_lastloan","totalpaidpc_previousloans","totalpaidpc_lastloan",\
                      "totalreschedules_previousloans","reschedule_count_lastloan"]



#IL run_scorecard lists
IL_categorical_list =[
  'agri_expense_question_15',
  'agri_practices_question_5',
  'agri_practices_question_7',
  'ai_harvest_month',
  'app_install_time',
  'app_not_install_reason',
  'as_house_type',
  'bp_applicant_name_in_license',
  'bp_business_nature_agriculture',
  'bp_business_nature_livestock',
  'bp_business_nature_production',
  'bp_business_nature_service',
  'bp_business_nature_trading',
  'bp_business_ownership',
  'bp_business_ownership_premise',
  'bp_business_sector',
  'bp_competition',
  'bp_license_issuer',
  'bp_location',
  'bp_maintenance_of_records',
  'bp_natural_factor_impact',
  'bp_operation_length',
  'bp_owner_involvement',
  'bp_relationship_with_stakeholders',
  'bp_sale_method',
  'bp_start_year_in_current_location',
  'bp_who_help_difficulties',
  'br_business_operating_years',
  'br_client_business_experience',
  'br_client_own_business',
  'br_how_long_know_client',
  'br_relationship_with_stakeholders',
  'coa_education',
  'coa_gender',
  'coa_house_ownership',
  'coa_nrc_type',
  'coa_occupation',
  'coa_phone',
  'coa_relationship',
  'fr_client_own_house',
  'fr_family_background_character',
  'fr_how_long_he_stay_there',
  'fr_how_long_know_client',
  'guarantor_gender',
  'guarantor_nrc_type',
  'guarantor_phone',
  'guarantor_religion',
  'guarantor_salary_slip',
  'hd_family_size',
  'hd_num_of_dependents',
  'hd_num_of_healthy_family_member',
  'hd_num_of_students',
  'hv_house_doc',
  'hv_house_ownership',
  'hv_house_roof',
  'hv_house_type',
  'hv_house_type_story',
  'hv_years_stay',
  'ls_loan_product',
  'pd_bank_name',
  'pd_education_level',
  'pd_know_about_hana',
  'pd_marital_status',
  'pd_religion',
  'client_division',
  'client_gender_cv_id',
  'client_mobile_no',
  'client_type_cv_id']
IL_binary_list =['abp_grow_maize',
  'api_have_land_title',
  'api_own_land_plan',
  'api_use_certified_seeds',
  'app_client_app_install',
  'bp_bank_account',
  'bp_is_licensed',
  'bp_same_location_business_home',
  'br_comments',
  'cd_client_building_2',
  'cd_house_document_1',
  'cd_vehicle_1',
  'fr_comments',
  'guarantor_marital_status',
  'guarantor_salaried_employee',
  'ld_business_building',
  'ld_business_vehicle_1',
  'ld_business_vehicle_2',
  'ld_farm_document_1',
  'ld_free_photo_1',
  'ld_free_photo_2',
  'ld_free_photo_3',
  'ld_other_income_business_doc_1',
  'ld_other_income_business_doc_2',
  'ld_photo_house_front',
  'ld_photo_house_side',
  'ld_prove_house_doc',
  'ld_request_business_license',
  'ld_requested_business_photo_1',
  'ld_requested_business_photo_2',
  'pd_bank_account',
  'has_other_loans',
  'efm_age',
  'as_vehicles_owned_Bicycle',
  'as_vehicles_owned_Car',
  'as_vehicles_owned_Motorbike',
  'as_vehicles_owned_Others',
  'hv_household_assets_Bed',
  'hv_household_assets_Cupboard',
  'hv_household_assets_DVDMusicsystem',
  'hv_household_assets_Four-wheeler',
  'hv_household_assets_GasCylinder',
  'hv_household_assets_Microwave',
  'hv_household_assets_Others',
  'hv_household_assets_Refrigerator',
  'hv_household_assets_SewingMachine',
  'hv_household_assets_Sofa',
  'hv_household_assets_TableChair',
  'hv_household_assets_Two-wheeler',
  'hv_household_assets_Washingmachine',
  'hv_services_available_Gas',
  'hv_services_available_Landline',
  'ls_loan_specific_use_Inventory',
  'ls_loan_specific_use_WorkingCapital',
  'bp_good_time_for_visit_afternoon',
  'bp_good_time_for_visit_evening',
  'bp_good_time_for_visit_morning',
  'bp_location_advantages_Close to raw material/supplier',
  'bp_location_advantages_Market area',
  'bp_location_advantages_Transportation facilities']
IL_numerical_list=[
  'abp_land_size_in_acre',
  'agri_practices_question_3',
  'ai_total_yeild_last_year',
  'ail_cf_monthly_net_income',
  'api_distance_to_reach_plot',
  'api_land_size_of_plot_acre',
  'api_value_of_plot',
  'bil_cf_monthly_net_income',
  'bp_capital',
  'bp_num_employees',
  'bp_operation_length_agri',
  'hd_family_size_agri',
  'hv_house_value',
  'ls_loan_amount_requested',
  'ls_loan_term',
  'lta_farm_size',
  'client_dob',
  'loan_counter',
  'loan_officer_experience',
  'office_opening_date',
  'MCIX_Delinquent_Count',
  'MCIX_Overlap_Count',
  'repayment_rate',
  'par',
  'vehicle_count',
  'househould_asset_count',
  'househould_services_count',
  'loan_use_count',
  'efm_relation_count',
  'business_area',
  'house_area',
  'loanamount_suggested_requested_ratio',
  'num_business_types',
  'guarantor_monthly_disposable_income',
  'applicant_networth',
  'applicant_monthly_disposable_income',
  'applicant_income_to_expense_ratio',
  'applicant_months_of_disposableincome_to_assets',
  'applicant_months_of_disposableincome_for_totalrepayment',
  'house_value_ov_net_income',
  'house_value_ov_req_loan_amt',
  'farm_value_ov_net_income',
  'farm_value_ov_req_loan_amt',
  'farm_value_ov_farm_area',
  'housevalue_ov_house_area',
  'assets_ov_req_loan_amt',
  'guarantor_months_of_disposableincome_to_assets',
  'vehicle_value_ov_vehicle_count',"totaldayslate_previousloans","totaldayslate_lastloan","totalpaidpc_previousloans","totalpaidpc_lastloan",\
                      "totalreschedules_previousloans","reschedule_count_lastloan"]


def ILGL_part1_read_dashboard_SCG(interviewid_list):
  
#PART 1: SQL QUERIES
#-------------------
  print("Running SQL queries")
  log.debug("Running SQL queries")

  str_interview_id = "','".join([str(id) for id in interviewid_list])
 
  FOappdata=cx.read_sql(db_connection_str,                "SELECT     l.id loan_id, i.id interview_id,     ia.question_name, ia.value, ia.option_value_label,       case       when q.type in ('calculation','number','nrc','text','location','age','textarea','component-list-multiple')  then JSON_EXTRACT(ia.value,'$[0]')       when q.type in ('select','radio-group') then JSON_EXTRACT(ia.option_value_label,'$[0]')           when q.type in ('checkbox-group') then ia.option_value_label             else ia.value       end as answers     FROM kebhana_dashboard_db.db_interviews AS i   INNER JOIN kebhana_dashboard_db.db_interview_answers ia on ia.interview_id = i.id   INNER JOIN kebhana_dashboard_db.db_questions  q on ia.question_name = q.name   LEFT JOIN kebhana_middleware_db.m_loan AS l ON l.id=i.loan_id   WHERE l.loan_status_id NOT IN (100,200,500)  AND i.interview_status NOT IN ('reject')   AND i.id IN ('" +str_interview_id+"');")


  FOappdata = FOappdata.pivot_table( index='interview_id',columns='question_name', values='answers',aggfunc='first')

  additional_fields =  cx.read_sql(db_connection_str,"SELECT c.id AS client_id,i.id AS interview_id,l.id AS loan_id,u.user_name AS FO_username_DB,o1.name AS client_division,cv2.code_value AS client_gender_cv_id,c.mobile_no AS client_mobile_no ,cv1.code_value AS client_type_cv_id,i.created_at AS interview_date,s.id AS FO_ID,s.display_name AS FO_username_FF,l.loan_counter,s.joining_date AS lo_joining_date,o.name AS office_name,o.opening_date AS office_opening_date,c.date_of_birth as client_dob   FROM kebhana_dashboard_db.db_interviews AS i   LEFT JOIN kebhana_dashboard_db.users AS u ON u.id=i.created_by   LEFT JOIN kebhana_middleware_db.m_loan AS l ON l.id=i.loan_id   LEFT JOIN kebhana_middleware_db.m_client AS c ON c.id=l.client_id   left join kebhana_middleware_db.m_code_value cv1 on cv1.id=c.client_type_cv_id   left join kebhana_middleware_db.m_code_value cv2 on cv2.id=c.gender_cv_id   LEFT JOIN kebhana_middleware_db.m_office o ON o.id=c.office_id   LEFT JOIN kebhana_middleware_db.m_office AS o1 ON o.parent_id=o1.id   LEFT JOIN kebhana_middleware_db.m_staff s ON s.id=c.staff_id   WHERE l.loan_status_id NOT IN (100,200,500)  AND i.interview_status NOT IN ('reject');")

  additional_fields['office_opening_date'] = pd.to_datetime(additional_fields['office_opening_date'])
  additional_fields['interview_date'] = pd.to_datetime(additional_fields['interview_date'])
  additional_fields['lo_joining_date'] = pd.to_datetime(additional_fields['lo_joining_date'])
  additional_fields['client_dob'] = pd.to_datetime(additional_fields['client_dob'])

  additional_fields['loan_officer_experience']=(additional_fields['interview_date']-additional_fields['lo_joining_date']).dt.days/365
  additional_fields['office_opening_date'] = (( additional_fields['interview_date'] - additional_fields['office_opening_date'])/np.timedelta64(1, 'M'))

  additional_fields.loc[additional_fields['client_gender_cv_id']=="Male (ကျား)",'client_gender_cv_id']='Male'
  additional_fields.loc[additional_fields['client_gender_cv_id']=="Female",'client_gender_cv_id']='Female'
  additional_fields['office_name'] = additional_fields['office_name'].str.replace('Branch Office - ', '', regex=False)

  FOappdata = FOappdata.merge(additional_fields, how='left',left_on='interview_id',right_on='interview_id')

  print("SQL loaded, now converting into useable format")
  log.debug("SQL loaded, now converting into useable format")
 
  #convert JSON to useable format
  for column in FOappdata.columns:
      FOappdata[column]=FOappdata[column].astype(str).str.replace('[', "").str.replace(']', "").str.replace('"', "").replace("",np.nan)
      FOappdata[column] = FOappdata[column].replace("nan",np.nan).replace("<NA>",np.nan)
      FOappdata[column] = FOappdata[column].fillna(np.nan)
      FOappdata[column]=try_float(FOappdata[column])

  print("FO app JSON data converted to useable format")
  log.debug("FO app JSON data converted to useable format")

  return FOappdata


def ILGL_part1_read_dashboard_SCI(interviewid_list):
  #PART 1: SQL QUERIES
  #--------------------

  print("Running SQL queries")
  log.debug("Running SQL queries")

  str_interview_id = "','".join([str(id) for id in interviewid_list])

  FOappdata=cx.read_sql(db_connection_str,                                "SELECT     l.id loan_id, i.id interview_id,     ia.question_name, ia.value, ia.option_value_label,       case       when q.type in ('calculation','number','nrc','text','location','age','textarea','component-list-multiple') then JSON_EXTRACT(ia.value,'$[0]')       when q.type in ('select','radio-group') then JSON_EXTRACT(ia.option_value_label,'$[0]')           when q.type in ('checkbox-group') then ia.option_value_label             else ia.value       end as answers     FROM kebhana_dashboard_db.db_interviews AS i   INNER JOIN kebhana_dashboard_db.db_interview_answers ia on ia.interview_id = i.id   INNER JOIN kebhana_dashboard_db.db_questions  q on ia.question_name = q.name   LEFT JOIN kebhana_middleware_db.m_loan AS l ON l.id=i.loan_id   WHERE i.id IN ('" +str_interview_id+"');")

  FOappdata = FOappdata.pivot_table( index='interview_id',columns='question_name', values='answers',aggfunc='first')
  
  print("Generating additional fields")
  log.debug("Generating additional fields")

  # [2022-07-01, Naing] Instead of direct pull the Client ID from m_client by loan table, get the interview_client / appointment_client / and m_client combination for all live records.
  additional_fields = cx.read_sql(db_connection_str,"SELECT   IF(ic.client_id IS NULL OR ic.client_id = 0, IF(iac.finflux_id IS NULL OR iac.finflux_id =0, l.client_id, iac.finflux_id), ic.client_id) AS client_id,   i.id AS interview_id,l.id AS loan_id,u.user_name AS FO_username_DB,o1.name AS client_division,cv2.code_value AS client_gender_cv_id,c.mobile_no AS client_mobile_no ,   cv1.code_value AS client_type_cv_id,i.created_at AS interview_date,s.id AS FO_ID,s.display_name AS FO_username_FF,l.loan_counter,s.joining_date AS lo_joining_date,   o.name AS office_name,o.opening_date AS office_opening_date,c.date_of_birth as client_dob   FROM kebhana_dashboard_db.db_interviews AS i   LEFT JOIN kebhana_dashboard_db.db_interview_clients ic ON ic.appointment_client_id=i.appointment_client_id    LEFT JOIN kebhana_dashboard_db.db_appointment_clients iac ON iac.id=i.appointment_client_id   LEFT JOIN kebhana_dashboard_db.users AS u ON u.id=i.created_by   LEFT JOIN kebhana_middleware_db.m_loan AS l ON l.id=i.loan_id   LEFT JOIN kebhana_middleware_db.m_client AS c ON c.id=IF(ic.client_id IS NULL OR ic.client_id = 0, IF(iac.finflux_id IS NULL OR iac.finflux_id =0, l.client_id, iac.finflux_id), ic.client_id)    left join kebhana_middleware_db.m_code_value cv1 on cv1.id=c.client_type_cv_id   left join kebhana_middleware_db.m_code_value cv2 on cv2.id=c.gender_cv_id   LEFT JOIN kebhana_middleware_db.m_office o ON o.id=c.office_id   LEFT JOIN kebhana_middleware_db.m_office AS o1 ON o.parent_id=o1.id   LEFT JOIN kebhana_middleware_db.m_staff s ON s.id=c.staff_id   WHERE i.interview_status NOT IN ('reject')   AND i.id IN ('" +str_interview_id+"');")

  additional_fields['office_opening_date'] = pd.to_datetime(additional_fields['office_opening_date'])
  additional_fields['interview_date'] = pd.to_datetime(additional_fields['interview_date'])
  additional_fields['lo_joining_date'] = pd.to_datetime(additional_fields['lo_joining_date'])
  additional_fields['client_dob'] = pd.to_datetime(additional_fields['client_dob'])

  additional_fields['loan_officer_experience']=(additional_fields['interview_date']-additional_fields['lo_joining_date']).dt.days/365
  additional_fields['office_opening_date'] = (( additional_fields['interview_date'] - additional_fields['office_opening_date'])/np.timedelta64(1, 'M'))

  additional_fields.loc[additional_fields['client_gender_cv_id']=="Male (ကျား)",'client_gender_cv_id']='Male'
  additional_fields.loc[additional_fields['client_gender_cv_id']=="Female",'client_gender_cv_id']='Female'
  additional_fields['office_name'] = additional_fields['office_name'].str.replace('Branch Office - ', '', regex=False)

  FOappdata = FOappdata.merge(additional_fields, how='left',left_on='interview_id',right_on='interview_id')

  print("SQL loaded, now converting into useable format")
  log.debug("SQL loaded, now converting into useable format")

  #convert JSON to useable format
  for column in FOappdata.columns:
      FOappdata[column]=FOappdata[column].astype(str).str.replace('[', "").str.replace(']', "").str.replace('"', "").replace("",np.nan)
      FOappdata[column] = FOappdata[column].replace("nan",np.nan).replace("<NA>",np.nan)
      FOappdata[column] = FOappdata[column].fillna(np.nan)
      FOappdata[column]= try_float(FOappdata[column])

  print("FO app JSON data converted to useable format")
  log.debug("SQL loaded, now converting into useable format")

  return FOappdata



def ILGL_part1_readothertables(FOappdata,interviewid_list):

  #Late days / paid pc / reschedule input & output addition
  print("Merging late days / paid pc / reschedule")
  log.debug("Merging late days / paid pc / reschedule")

  client_id_list = FOappdata['client_id'].dropna().tolist()
  client_id_list = tuple(client_id_list)
  str_client_id = ",".join([str(id) for id in client_id_list])
  
  longprocessingdf = cx.read_sql(db_connection_str,"SELECT * FROM kebhana_middleware_db.hwa_late_day_fields ld WHERE ld.client_id IN (" +str_client_id+");")

  datecols = longprocessingdf.filter(like="date").columns.tolist()
  numericalcols = list(set(longprocessingdf.columns) - set(datecols))

  longprocessingdf[numericalcols] = longprocessingdf[numericalcols].apply(pd.to_numeric)
  longprocessingdf["reference_date"] = pd.to_datetime(longprocessingdf["reference_date"])

  FOappdata["interview_date"] = pd.to_datetime(FOappdata["interview_date"]).dt.normalize()

  FOappdata[["totaldayslate_previousloans","totaldayslate_lastloan","totalpaidpc_previousloans","totalpaidpc_lastloan",                        "totalreschedules_previousloans","reschedule_count_lastloan"]] = FOappdata.apply(generate_loanperf_inputs, longprocessingdf = longprocessingdf, axis=1)

  #MCIX addition
  print("Merging MCIX")
  log.debug("Merging MCIX")

  MCIX_data=cx.read_sql(db_connection_str,"SELECT id, client_id, mcix_file_date, thitsa_id, delinquent, overlap, writeoff, mfi_name FROM kebhana_middleware_db.hwa_mcix m WHERE m.client_id IN (" +str_client_id+");")

  #MCIX_data = pd.read_csv("/content/drive/My Drive/SID/MappingTables/MCIX_Production.csv")
  #MCIX_data.rename(columns = {'Delinquent':'delinquent', 'Overlap':'overlap', 'WriteOff': 'writeoff',"MCIX_filedate":"mcix_file_date" }, inplace = True)
  MCIX_data["mcix_file_date"] = pd.to_datetime(MCIX_data["mcix_file_date"])

  FOappdata["nearest_MCIX_date"] = FOappdata.apply(get_closest_MCIX_date, MCIX_data=MCIX_data, axis = 1)
  if FOappdata["nearest_MCIX_date"].isnull().all():
    FOappdata[["delinquent","overlap","writeoff"]]=np.nan
  else:
    FOappdata = FOappdata.merge(MCIX_data[["client_id","mcix_file_date","delinquent","overlap","writeoff"]],                  left_on=["client_id","nearest_MCIX_date"],right_on=["client_id","mcix_file_date"],how="left")

  FOappdata.rename(columns = {'delinquent':'MCIX_Delinquent_Count', 'overlap':'MCIX_Overlap_Count', 'writeoff': 'MCIX_WriteOff_Count' }, inplace = True)
  FOappdata[["MCIX_Delinquent_Count","MCIX_Overlap_Count","MCIX_WriteOff_Count"]] = FOappdata[["MCIX_Delinquent_Count","MCIX_Overlap_Count","MCIX_WriteOff_Count"]].apply(pd.to_numeric)

  #BranchRepayPAR addition
  print("Merging branch PAR and repayment rate")
  log.debug("Merging branch PAR and repayment rate")

  #branchPARrepay_df = pd.read_csv("/content/drive/My Drive/SID/MappingTables/MonthlyBranchPARRepayment.csv")

  office_name_list = FOappdata['office_name'].dropna().tolist()
  office_name_list = tuple(office_name_list)
  str_office_name = "','".join([str(id) for id in office_name_list])

  branchPARrepay_df=cx.read_sql(db_connection_str,"SELECT id, reporting_date, office_id, branch_name, repayment_rate, par FROM kebhana_middleware_db.hwa_monthly_branch_par_repayment bp WHERE bp.branch_name IN ('" +str_office_name+"');")

  branchPARrepay_df.drop(columns = ["id"],inplace=True)

  branchPARrepay_df["reporting_date"] = pd.to_datetime(branchPARrepay_df["reporting_date"])

  FOappdata["lastmonthbranchdata_date"] = FOappdata["interview_date"].apply(
      lambda date: min(date.replace(day=1)+ relativedelta(months=-1),\
                      branchPARrepay_df["reporting_date"].max())).dt.normalize()

  FOappdata = FOappdata.merge(branchPARrepay_df,                  left_on=["office_name","lastmonthbranchdata_date"],right_on=["branch_name","reporting_date"],how="left")                  .drop(columns = ["branch_name","reporting_date","office_id","lastmonthbranchdata_date"])

  FOappdata[["repayment_rate","par"]] = FOappdata[["repayment_rate","par"]].apply(pd.to_numeric)


  #read loan product name table
  print("Merging loan product type")

  product_categorisation = cx.read_sql(db_connection_str,"SELECT * FROM kebhana_middleware_db.hwa_loan_product_name_mapping").astype(str)
  product_categorisation = product_categorisation.replace("b'","",regex=True).replace("'","",regex=True)

  product_dictionary = dict(zip(product_categorisation["loan_product_name"], product_categorisation["repayment_type"]))

  FOappdata["ls_loan_product_mapped"] = FOappdata["ls_loan_product"].map(product_dictionary)
  needloanmapping = list(FOappdata["ls_loan_product"][FOappdata["ls_loan_product_mapped"].isna()].unique())
  if np.nan in needloanmapping:
    needloanmapping.remove(np.nan)

  if len(needloanmapping) > 0:
    needloanmappingstr = ", ".join(needloanmapping)
    print("Note: loan mapping missing for " + str(len(needloanmapping)) + " loan type")
    print("Please add loan mapping for " + needloanmappingstr)
    print("Now removing loans with no loan mappings")
    FOappdata = FOappdata[~FOappdata["ls_loan_product_mapped"].isna()]

  FOappdata["ls_loan_product"] = FOappdata["ls_loan_product_mapped"]
  FOappdata.drop(columns = "ls_loan_product_mapped", inplace=True)

  return FOappdata

def IL_part1_reorderfields(FOappdata):

  print("Reordering fields")
  log.debug("Reordering fields")

  fullfieldlistorder = ['client_id',
  'interview_id',
  'loan_id',
  'abp_grow_maize',
  'abp_how_many_plots_do_you_farm',
  'abp_land_size_in_acre',
  'agri_expense_question_15',
  'agri_practices_question_1',
  'agri_practices_question_2',
  'agri_practices_question_3',
  'agri_practices_question_4',
  'agri_practices_question_5',
  'agri_practices_question_7',
  'ai_harvest_month',
  'ai_total_yeild_last_year',
  'ail_cf_monthly_net_income',
  'api_distance_to_reach_plot',
  'api_have_land_title',
  'api_land_size_of_plot_acre',
  'api_own_land_plan',
  'api_use_certified_seeds',
  'api_value_of_plot',
  'app_client_app_install',
  'app_install_time',
  'app_not_install_reason',
  'as_house_type',
  'as_livestocks_owned',
  'as_vehicles_owned',
  'be_daily_cost_of_goods',
  'be_other',
  'be_rent',
  'be_salaries',
  'be_transportation',
  'be_utilities',
  'bi_daily_sale_income',
  'bil_cf_monthly_net_income',
  'bp_applicant_name_in_license',
  'bp_bank_account',
  'bp_business_area_length_in_feet',
  'bp_business_area_width_in_feet',
  'bp_business_nature_agriculture',
  'bp_business_nature_livestock',
  'bp_business_nature_production',
  'bp_business_nature_service',
  'bp_business_nature_trading',
  'bp_business_ownership',
  'bp_business_ownership_premise',
  'bp_business_sector',
  'bp_can_business_operate_without_owner',
  'bp_capital',
  'bp_competition',
  'bp_good_time_for_visit',
  'bp_is_licensed',
  'bp_license_issuer',
  'bp_location',
  'bp_location_advantages',
  'bp_maintenance_of_records',
  'bp_natural_factor_impact',
  'bp_num_employees',
  'bp_operation_length',
  'bp_operation_length_agri',
  'bp_owner_involvement',
  'bp_relationship_with_stakeholders',
  'bp_sale_method',
  'bp_same_location_business_home',
  'bp_start_year_in_current_location',
  'bp_who_help_difficulties',
  'br_avg_net_income_per_month',
  'br_business_always_open',
  'br_business_operating_years',
  'br_client_business_experience',
  'br_client_make_payment_ontime',
  'br_client_own_business',
  'br_comments',
  'br_how_long_know_client',
  'br_relationship_with_stakeholders',
  'ca_accounts_receivable',
  'ca_cash_in_bank_saving',
  'ca_cash_in_hand',
  'ca_inventory_value',
  'ca_other_assets',
  'cd_applicant_signature',
  'cd_client_building_1',
  'cd_client_building_2',
  'cd_client_household_list',
  'cd_client_nrc_back',
  'cd_client_nrc_front',
  'cd_client_photo',
  'cd_house_document_1',
  'cd_house_document_2',
  'cd_vehicle_1',
  'cd_vehicle_2',
  'cd_vehicle_document_1',
  'cd_vehicle_document_2',
  'clch_component',
  'clch_num_outstanding_loans',
  'coa_education',
  'coa_gender',
  'coa_house_ownership',
  'coa_nrc_type',
  'coa_occupation',
  'coa_phone',
  'coa_relationship',
  'efm_component',
  'efm_number',
  'evaluation_question_1',
  'evaluation_question_10',
  'evaluation_question_11',
  'evaluation_question_12',
  'evaluation_question_14',
  'evaluation_question_15',
  'evaluation_question_17',
  'evaluation_question_2',
  'evaluation_question_7',
  'evaluation_question_9',
  'FO_username_DB',
  'fr_client_own_house',
  'fr_comments',
  'fr_family_background_character',
  'fr_how_long_he_stay_there',
  'fr_how_long_know_client',
  'guarantor_asset_buildings',
  'guarantor_asset_business',
  'guarantor_asset_vehicles',
  'guarantor_gender',
  'guarantor_marital_status',
  'guarantor_monthly_business_expense',
  'guarantor_monthly_business_income',
  'guarantor_monthly_household_expense',
  'guarantor_monthly_salary_income',
  'guarantor_nrc_type',
  'guarantor_other_expense',
  'guarantor_other_income',
  'guarantor_phone',
  'guarantor_religion',
  'guarantor_salaried_employee',
  'guarantor_salary_slip',
  'hd_family_agree_taking_loan',
  'hd_family_size',
  'hd_family_size_agri',
  'hd_num_of_dependents',
  'hd_num_of_healthy_family_member',
  'hd_num_of_students',
  'hd_pregnant_family_member',
  'hv_house_area_length_feet',
  'hv_house_area_width_feet',
  'hv_house_doc',
  'hv_house_ownership',
  'hv_house_roof',
  'hv_house_type',
  'hv_house_type_story',
  'hv_house_value',
  'hv_household_assets',
  'hv_services_available',
  'hv_years_stay',
  'ilda_coapplicant_household_list',
  'ilda_coapplicant_nrc_back',
  'ilda_coapplicant_nrc_front',
  'ilda_coapplicant_photo',
  'ilda_coapplicant_signature',
  'ilda_guarantor_household_list',
  'ilda_guarantor_nrc_back',
  'ilda_guarantor_nrc_front',
  'ilda_guarantor_photo',
  'ilda_guarantor_signature',
  'ld_applicant_bank_book',
  'ld_business_bank_book',
  'ld_business_building',
  'ld_business_vehicle_1',
  'ld_business_vehicle_2',
  'ld_farm_document_1',
  'ld_farm_document_2',
  'ld_farm_document_3',
  'ld_farm_document_4',
  'ld_farm_document_5',
  'ld_free_photo_1',
  'ld_free_photo_2',
  'ld_free_photo_3',
  'ld_household_income_doc_1',
  'ld_household_income_doc_2',
  'ld_household_income_doc_3',
  'ld_other_income_business_doc_1',
  'ld_other_income_business_doc_2',
  'ld_other_income_business_doc3',
  'ld_photo_house_front',
  'ld_photo_house_side',
  'ld_prove_house_doc',
  'ld_request_business_license',
  'ld_requested_business_photo_1',
  'ld_requested_business_photo_2',
  'ld_vehicle_document',
  'liabilities_accounts_payable',
  'liabilities_others_payable',
  'ls_fo_suggested_amount',
  'ls_fo_suggestion_upon_cashflow',
  'ls_loan_amount_requested',
  'ls_loan_product',
  'ls_loan_specific_use',
  'ls_loan_term',
  'lta_buildings_lands',
  'lta_farm',
  'lta_farm_size',
  'lta_machinery_equipment',
  'lta_vehicles',
  'oi_agri_income',
  'oi_other_income',
  'oi_rent_income',
  'pd_bank_account',
  'pd_bank_name',
  'pd_education_level',
  'pd_know_about_hana',
  'pd_marital_status',
  'pd_religion',
  'pe_clothing',
  'pe_donation',
  'pe_education',
  'pe_festival',
  'pe_food',
  'pe_medical',
  'pe_other',
  'pe_rent',
  'pe_transportation',
  'pe_utility',
  'client_division',
  'client_gender_cv_id',
  'client_mobile_no',
  'client_type_cv_id',
  'interview_date',
  'FO_ID',
  'FO_username_FF',
  'late_days_in_183d',
  'late_days_in_365d',
  'late_days_in_730d',
  'late_days_in_90d',
  'loan_counter',
  'loan_officer_experience',
  'office_name',
  'office_opening_date',
  'paid_pc_183d',
  'paid_pc_365d',
  'paid_pc_730d',
  'paid_pc_90d',
  'reschedule_count_in_183d',
  'reschedule_count_in_365d',
  'reschedule_count_in_730d',
  'reschedule_count_in_90d',
  'MCIX_Delinquent_Count',
  'MCIX_Overlap_Count',
  'MCIX_WriteOff_Count',
  'repayment_rate',
  'par',
  "totaldayslate_previousloans","totaldayslate_lastloan","totalpaidpc_previousloans","totalpaidpc_lastloan",\
                        "totalreschedules_previousloans","reschedule_count_lastloan",
                        "client_dob"]

  FOappdata[list(set(fullfieldlistorder) - set(FOappdata.columns))] = np.nan
  return FOappdata[fullfieldlistorder]


def GL_part1_reorderfields(FOappdata):
    
  print("Reordering fields")
  log.debug("Reordering fields")

  fullfieldlistorder = ['client_id',
  'interview_id',
  'loan_id',
  'abp_grow_maize',
  'abp_how_many_plots_do_you_farm',
  'abp_land_size_in_acre',
  'agl_cf_monthly_net_income',
  'agri_expense_question_15',
  'agri_practices_question_1',
  'agri_practices_question_2',
  'agri_practices_question_3',
  'agri_practices_question_4',
  'agri_practices_question_5',
  'agri_practices_question_7',
  'ai_harvest_month',
  'ai_total_yeild_last_year',
  'api_distance_to_reach_plot',
  'api_have_land_title',
  'api_land_size_of_plot_acre',
  'api_own_land_plan',
  'api_use_certified_seeds',
  'api_value_of_plot',
  'app_client_app_install',
  'app_install_time',
  'app_not_install_reason',
  'as_house_type',
  'as_livestocks_owned',
  'as_vehicles_owned',
  'be_monthly_raw_cost',
  'be_other',
  'be_rent',
  'be_salaries',
  'be_transportation',
  'be_utilities',
  'bgl_cf_monthly_net_income',
  'bi_monthly_income',
  'bp_applicant_name_in_license',
  'bp_business_area_length_in_feet',
  'bp_business_area_width_in_feet',
  'bp_business_nature_agriculture',
  'bp_business_nature_livestock',
  'bp_business_nature_production',
  'bp_business_nature_service',
  'bp_business_nature_trading',
  'bp_business_ownership',
  'bp_business_ownership_premise',
  'bp_business_sector',
  'bp_capital',
  'bp_is_licensed',
  'bp_license_issuer',
  'bp_num_employees',
  'bp_operation_length',
  'bp_operation_length_agri',
  'bp_sale_method',
  'bp_same_location_business_home',
  'ca_cash_in_bank_saving',
  'ca_cash_in_hand',
  'ca_inventory_value',
  'ca_livestock',
  'ca_other_assets',
  'cd_applicant_signature',
  'cd_client_building_1',
  'cd_client_building_2',
  'cd_client_household_list',
  'cd_client_nrc_back',
  'cd_client_nrc_front',
  'cd_client_photo',
  'cd_house_document_1',
  'cd_house_document_2',
  'cd_vehicle_1',
  'cd_vehicle_2',
  'cd_vehicle_document_1',
  'cd_vehicle_document_2',
  'clch_component',
  'clch_num_outstanding_loans',
  'efm_component',
  'efm_number',
  'evaluation_question_1',
  'evaluation_question_10',
  'evaluation_question_11',
  'evaluation_question_12',
  'evaluation_question_14',
  'evaluation_question_15',
  'evaluation_question_17',
  'evaluation_question_2',
  'evaluation_question_5',
  'evaluation_question_6',
  'evaluation_question_7',
  'evaluation_question_8',
  'evaluation_question_9',
  'FO_username_DB',
  'guarantor_nrc_type',
  'guarantor_phone',
  'hd_family_agree_taking_loan',
  'hd_family_size',
  'hd_family_size_agri',
  'hd_num_of_dependents',
  'hd_num_of_healthy_family_member',
  'hd_num_of_students',
  'hd_pregnant_family_member',
  'hv_house_area_length_feet',
  'hv_house_area_width_feet',
  'hv_house_doc',
  'hv_house_ownership',
  'hv_house_roof',
  'hv_house_type',
  'hv_house_type_story',
  'hv_house_value',
  'hv_household_assets',
  'hv_services_available',
  'hv_years_stay',
  'ilda_guarantor_nrc_back',
  'ilda_guarantor_nrc_front',
  'ilda_guarantor_photo',
  'ilda_guarantor_signature',
  'ld_business_building',
  'ld_business_vehicle_1',
  'ld_business_vehicle_2',
  'ld_farm_document_1',
  'ld_farm_document_2',
  'ld_farm_document_3',
  'ld_farm_document_4',
  'ld_farm_document_5',
  'ld_free_photo_1',
  'ld_free_photo_2',
  'ld_free_photo_3',
  'ld_photo_house_front',
  'ld_photo_house_side',
  'ld_prove_house_doc',
  'ld_requested_business_photo_1',
  'ld_requested_business_photo_2',
  'ld_vehicle_document',
  'liabilities_accounts_payable',
  'ls_fo_suggested_amount',
  'ls_fo_suggestion_upon_cashflow',
  'ls_loan_amount_requested',
  'ls_loan_product',
  'ls_loan_specific_use',
  'ls_loan_term',
  'lta_buildings_lands',
  'lta_farm',
  'lta_farm_size',
  'lta_machinery_equipment',
  'lta_vehicles',
  'oi_component',
  'oi_number',
  'client_dob',
  'pd_bank_account',
  'pd_bank_name',
  'pd_education_level',
  'pd_know_about_hana',
  'pd_marital_status',
  'pd_religion',
  'pe_clothing',
  'pe_donation',
  'pe_education',
  'pe_festival',
  'pe_food',
  'pe_medical',
  'pe_other',
  'pe_rent',
  'pe_transportation',
  'pe_utility',
  'client_division',
  'client_gender_cv_id',
  'client_mobile_no',
  'client_type_cv_id',
  'interview_date',
  'FO_ID',
  'FO_username_FF',
  'late_days_in_183d',
  'late_days_in_365d',
  'late_days_in_730d',
  'late_days_in_90d',
  'loan_counter',
  'loan_officer_experience',
  'office_name',
  'office_opening_date',
  'paid_pc_183d',
  'paid_pc_365d',
  'paid_pc_730d',
  'paid_pc_90d',
  'reschedule_count_in_183d',
  'reschedule_count_in_365d',
  'reschedule_count_in_730d',
  'reschedule_count_in_90d',
  'MCIX_Delinquent_Count',
  'MCIX_Overlap_Count',
  'MCIX_WriteOff_Count',
  'repayment_rate',
  'par',
  "totaldayslate_previousloans","totaldayslate_lastloan","totalpaidpc_previousloans","totalpaidpc_lastloan",\
                        "totalreschedules_previousloans","reschedule_count_lastloan"]


  FOappdata[list(set(fullfieldlistorder) - set(FOappdata.columns))] = np.nan
  return FOappdata[fullfieldlistorder]

def IL_part2_byprocessingtype(FOappdata_processed):

  #PART 2: PROCESS EACH PROECESSING TYPE
  #-----------------------------------

  #Process Direct_process

  #do nothing

  #Process Output

  #see PART 1 for field list

  #do nothing

  #Process Derive_binary
  Derive_binary = ['br_comments',
  'cd_applicant_signature',
  'cd_client_building_1',
  'cd_client_building_2',
  'cd_client_household_list',
  'cd_client_nrc_back',
  'cd_client_nrc_front',
  'cd_client_photo',
  'cd_house_document_1',
  'cd_house_document_2',
  'cd_vehicle_1',
  'cd_vehicle_2',
  'cd_vehicle_document_1',
  'cd_vehicle_document_2',
  'evaluation_question_1',
  'evaluation_question_15',
  'evaluation_question_17',
  'fr_comments',
  'ilda_coapplicant_household_list',
  'ilda_coapplicant_nrc_back',
  'ilda_coapplicant_nrc_front',
  'ilda_coapplicant_photo',
  'ilda_coapplicant_signature',
  'ilda_guarantor_household_list',
  'ilda_guarantor_nrc_back',
  'ilda_guarantor_nrc_front',
  'ilda_guarantor_photo',
  'ilda_guarantor_signature',
  'ld_applicant_bank_book',
  'ld_business_bank_book',
  'ld_business_building',
  'ld_business_vehicle_1',
  'ld_business_vehicle_2',
  'ld_farm_document_1',
  'ld_farm_document_2',
  'ld_farm_document_3',
  'ld_farm_document_4',
  'ld_farm_document_5',
  'ld_free_photo_1',
  'ld_free_photo_2',
  'ld_free_photo_3',
  'ld_household_income_doc_1',
  'ld_household_income_doc_2',
  'ld_household_income_doc_3',
  'ld_other_income_business_doc_1',
  'ld_other_income_business_doc_2',
  'ld_other_income_business_doc3',
  'ld_photo_house_front',
  'ld_photo_house_side',
  'ld_prove_house_doc',
  'ld_request_business_license',
  'ld_requested_business_photo_1',
  'ld_requested_business_photo_2',
  'ld_vehicle_document',
  'ls_fo_suggestion_upon_cashflow']


  for column_name in Derive_binary:
    FOappdata_processed.loc[~FOappdata_processed[column_name].isna(),column_name]=1
    FOappdata_processed.loc[FOappdata_processed[column_name].isna(),column_name]=0

  #Process Derive_age_from_this_DOB
  Derive_age_from_this_DOB = ['client_dob']

  for column_name in Derive_age_from_this_DOB:
    FOappdata_processed[column_name] = pd.to_datetime(FOappdata_processed[column_name],format="%Y-%m-%d",errors='coerce').apply(age)

  #Process Derive_hasotherloans
  Derive_hasotherloans = ['clch_component', 'clch_num_outstanding_loans']

  FOappdata_processed["has_other_loans"] = "No"
  FOappdata_processed.loc[(~FOappdata_processed["clch_component"].isna())|(~FOappdata_processed["clch_num_outstanding_loans"].isna()),"has_other_loans"]="Yes"
  FOappdata_processed.drop(columns = Derive_hasotherloans, inplace = True)


  #process Derive_operator

  Derive_operator = ['coa_phone', 'guarantor_phone', 'client_mobile_no']

  for column_name in Derive_operator:
      FOappdata_processed[column_name] = FOappdata_processed[column_name].astype(str)
      FOappdata_processed[column_name]= FOappdata_processed[column_name].str.lstrip('0')
      FOappdata_processed[column_name] = FOappdata_processed[column_name].str[:3].replace("959","9") +           FOappdata_processed[column_name].str[3:15]
      FOappdata_processed[column_name] = FOappdata_processed[column_name].str.replace(".","")          .str.replace(' ','').str.replace('-','')
      FOappdata_processed.loc[FOappdata_processed[column_name].str[:1]!="9",column_name]=np.nan
      FOappdata_processed.loc[FOappdata_processed[column_name].str[:2]=="99",column_name]="Ooredoo"
      FOappdata_processed.loc[FOappdata_processed[column_name].str[:2]=="97",column_name]="Telenor"
      FOappdata_processed.loc[FOappdata_processed[column_name].str[:2]=="96",column_name]="Mytel"
      FOappdata_processed.loc[(FOappdata_processed[column_name]!="Ooredoo") & (FOappdata_processed[column_name]!="Telenor")                         & (FOappdata_processed[column_name]!="Mytel") & (~FOappdata_processed[column_name].isna()),column_name] = "MPT"


  #Process Derive_from_efm_component
  Derive_from_efm_component = ['efm_component', 'efm_number']

  FOappdata_processed["efm_monthly_income"] = FOappdata_processed["efm_component"].fillna("").apply(processcomponent_efm_monthly_income)
  FOappdata_processed["efm_relation"] = FOappdata_processed["efm_component"].fillna("").apply(processcomponent_efm_relation)
  FOappdata_processed["efm_age"] = FOappdata_processed["efm_component"].fillna("").apply(processcomponent_efm_age)
  FOappdata_processed.drop(columns = Derive_from_efm_component, inplace = True)


  #process Derive_multiselection

  Derive_multiselection = ['as_livestocks_owned',  'as_vehicles_owned',  'hv_household_assets',  'hv_services_available', 'ls_loan_specific_use','bp_good_time_for_visit', 'bp_location_advantages']

  Derive_multiselection.append("efm_relation")

  for column_name in Derive_multiselection:
      FOappdata_processed[column_name] = FOappdata_processed[column_name].fillna("").apply(removebrackets).replace("",np.nan,regex=False)
      FOappdata_processed[column_name] = FOappdata_processed[column_name].fillna("").str.replace("\\n","|",regex=False).str.replace(", ","|",regex=False).str.replace(",","|",regex=False).str.replace(" ","",regex=False)
      temp_dummy =  FOappdata_processed[column_name].str.get_dummies('|').add_prefix(column_name+"_")
      FOappdata_processed = pd.concat([FOappdata_processed,temp_dummy],axis=1)
      
      if column_name == "as_vehicles_owned":
          FOappdata_processed["vehicle_count"]=temp_dummy.sum(axis=1)
          
      if column_name == "ls_loan_specific_use":
          FOappdata_processed["loan_use_count"]=temp_dummy.sum(axis=1)
          
      if column_name == "hv_household_assets":
          FOappdata_processed["househould_asset_count"]=temp_dummy.sum(axis=1)

      if column_name == "hv_services_available":
        FOappdata_processed["househould_services_count"]=temp_dummy.sum(axis=1)
      
      if column_name == "efm_relation":
          FOappdata_processed["efm_relation_count"]=temp_dummy.sum(axis=1)

  FOappdata_processed.drop(columns = Derive_multiselection, inplace=True)

  #process Derive_area
  Derive_area = ['bp_business_area_length_in_feet',
  'bp_business_area_width_in_feet',
  'hv_house_area_length_feet',
  'hv_house_area_width_feet']

  FOappdata_processed["business_area"] = FOappdata_processed["bp_business_area_length_in_feet"]*FOappdata_processed["bp_business_area_width_in_feet"]
  FOappdata_processed["house_area"]= FOappdata_processed["hv_house_area_length_feet"]*FOappdata_processed["hv_house_area_width_feet"]
  FOappdata_processed.drop(columns = Derive_area, inplace=True)


  #process Derive_ls_loan_amount_requested_grtr_ls_fo_suggested_amount
  Derive_ls_loan_amount_requested_grtr_ls_fo_suggested_amount = ['ls_fo_suggested_amount', 'ls_loan_amount_requested']

  FOappdata_processed["loanamount_suggested_requested_ratio"] = FOappdata_processed["ls_fo_suggested_amount"] / FOappdata_processed["ls_loan_amount_requested"]


  #process Derive_count
  Derive_count = ['bp_business_nature_agriculture',
  'bp_business_nature_livestock',
  'bp_business_nature_production',
  'bp_business_nature_service',
  'bp_business_nature_trading']

  FOappdata_processed["num_business_types"] = FOappdata_processed[Derive_count].count(axis=1)


  #process Simplify
  Simplify = ['app_client_app_install', 'app_not_install_reason']

  FOappdata_processed.loc[FOappdata_processed["app_client_app_install"].astype(str).str.contains("Yes",na=False),"app_client_app_install"]=1
  FOappdata_processed.loc[FOappdata_processed["app_client_app_install"].astype(str).str.contains("No",na=False),"app_client_app_install"]=0

  FOappdata_processed.loc[FOappdata_processed["app_not_install_reason"].isna(),"app_not_install_reason"]="Blank"
  FOappdata_processed.loc[FOappdata_processed["app_not_install_reason"].str.contains("အင်တာနက်",na=False),"app_not_install_reason"]="No internet"
  FOappdata_processed.loc[(FOappdata_processed["app_not_install_reason"].str.contains("Keypad",na=False))                     | (FOappdata_processed["app_not_install_reason"].str.contains("ဖုန်း",na=False))                         | (FOappdata_processed["app_not_install_reason"].str.contains("iPhone",na=False)),"app_not_install_reason"]="No Android Phone"
  FOappdata_processed.loc[(FOappdata_processed["app_not_install_reason"]!="No internet") & (FOappdata_processed["app_not_install_reason"]!="No Android Phone" )                     & (FOappdata_processed["app_not_install_reason"]!="Blank" ),"app_not_install_reason"]="Other"
  FOappdata_processed["app_not_install_reason"] = FOappdata_processed["app_not_install_reason"].replace("Blank",np.nan)


  #Derive_guarantor_income_expense
  Derive_guarantor_income_expense = ['guarantor_monthly_business_expense',
  'guarantor_monthly_business_income',
  'guarantor_monthly_household_expense',
  'guarantor_monthly_salary_income',
  'guarantor_other_expense',
  'guarantor_other_income']

  FOappdata_processed["guarantor_monthly_disposable_income"] = 0

  for column_name in Derive_guarantor_income_expense:
    if ("income" in column_name):
        FOappdata_processed["guarantor_monthly_disposable_income"] = FOappdata_processed["guarantor_monthly_disposable_income"].add(FOappdata_processed[column_name].fillna(0),axis=0)
    elif ("expense" in column_name):
        FOappdata_processed["guarantor_monthly_disposable_income"] = FOappdata_processed["guarantor_monthly_disposable_income"].subtract(FOappdata_processed[column_name].fillna(0),axis=0)


  #Derive_income_expense

  Derive_income_expense = ['be_daily_cost_of_goods',
  'be_other',
  'be_rent',
  'be_salaries',
  'be_transportation',
  'be_utilities',
  'bi_daily_sale_income',
  'br_avg_net_income_per_month',
  'oi_agri_income',
  'oi_other_income',
  'oi_rent_income',
  'pe_clothing',
  'pe_donation',
  'pe_education',
  'pe_festival',
  'pe_food',
  'pe_medical',
  'pe_other',
  'pe_rent',
  'pe_transportation',
  'pe_utility']

  FOappdata_processed["monthly_income"] = 0
  FOappdata_processed["monthly_expense"] = 0

  for column_name in Derive_income_expense:
    if (column_name[:8]=="bi_daily"):
        FOappdata_processed["monthly_income"] = FOappdata_processed["monthly_income"].add(30*FOappdata_processed[column_name].fillna(0),axis=0)
    elif (((column_name[:3]=="bi_")&(column_name[:8]!="bi_daily"))|(column_name[:3]=="oi_")):
        FOappdata_processed["monthly_income"] = FOappdata_processed["monthly_income"].add(FOappdata_processed[column_name].fillna(0),axis=0)
    elif (column_name[:8]=="be_daily"):
        FOappdata_processed["monthly_expense"] = FOappdata_processed["monthly_expense"].add(30*FOappdata_processed[column_name].fillna(0),axis=0)
    elif (((column_name[:3]=="be_")&(column_name[:8]!="be_daily"))|(column_name[:3]=="pe_")):
        FOappdata_processed["monthly_expense"] = FOappdata_processed["monthly_expense"].add(FOappdata_processed[column_name].fillna(0),axis=0)
  Derive_income_expense.append("monthly_income")
  Derive_income_expense.append("monthly_expense")


  #Derive_balance_sheet
  Derive_balance_sheet = ['ca_accounts_receivable',
  'ca_cash_in_bank_saving',
  'ca_cash_in_hand',
  'ca_inventory_value',
  'ca_other_assets',
  'liabilities_accounts_payable',
  'liabilities_others_payable',
  'lta_buildings_lands',
  'lta_farm',
  'lta_machinery_equipment',
  'lta_vehicles']

  FOappdata_processed["totalassets"]=0
  FOappdata_processed["totalliabilities"]=0

  for column_name in Derive_balance_sheet:
    if ((column_name[:3]=="ca_")|(column_name[:3]=="lta")):
        FOappdata_processed["totalassets"] = FOappdata_processed["totalassets"].add(FOappdata_processed[column_name].fillna(0),axis=0)
    elif (column_name[:3]=="lia"):
        FOappdata_processed["totalliabilities"] = FOappdata_processed["totalliabilities"].add(FOappdata_processed[column_name].fillna(0),axis=0)
  Derive_balance_sheet.append("totalassets")
  Derive_balance_sheet.append("totalliabilities")

  #Derive_guarantor_balance_sheet
  Derive_guarantor_balance_sheet = ['guarantor_asset_buildings',
  'guarantor_asset_business',
  'guarantor_asset_vehicles']

  FOappdata_processed["guarantor_totalassets"]=0
  for column_name in Derive_guarantor_balance_sheet:
    FOappdata_processed["guarantor_totalassets"] = FOappdata_processed["guarantor_totalassets"].add(FOappdata_processed[column_name].fillna(0),axis=0)
  Derive_guarantor_balance_sheet.append("guarantor_totalassets")


  #Derive ratios
  FOappdata_processed["applicant_networth"] = FOappdata_processed["totalassets"] - FOappdata_processed["totalliabilities"]
  FOappdata_processed["applicant_monthly_disposable_income"] = FOappdata_processed["monthly_income"] - FOappdata_processed["monthly_expense"] 
  FOappdata_processed["applicant_income_to_expense_ratio"] = FOappdata_processed["monthly_income"] / FOappdata_processed["monthly_expense"] 
  FOappdata_processed["applicant_months_of_disposableincome_to_assets"] = FOappdata_processed["totalassets"].fillna(0) / FOappdata_processed["applicant_monthly_disposable_income"]
  FOappdata_processed["applicant_months_of_disposableincome_for_totalrepayment"] = FOappdata_processed["ls_loan_amount_requested"].fillna(0) / FOappdata_processed["applicant_monthly_disposable_income"]
  FOappdata_processed["house_value_ov_net_income"]=FOappdata_processed["hv_house_value"]/FOappdata_processed["applicant_monthly_disposable_income"]
  FOappdata_processed["house_value_ov_req_loan_amt"]=FOappdata_processed["hv_house_value"]/FOappdata_processed["ls_loan_amount_requested"]
  FOappdata_processed["farm_value_ov_net_income"]=FOappdata_processed["lta_farm"]/FOappdata_processed["applicant_monthly_disposable_income"]
  FOappdata_processed["farm_value_ov_req_loan_amt"]=FOappdata_processed["lta_farm"]/FOappdata_processed["ls_loan_amount_requested"]
  FOappdata_processed["farm_value_ov_farm_area"]=FOappdata_processed["lta_farm"]/FOappdata_processed["lta_farm_size"]
  FOappdata_processed["housevalue_ov_house_area"]=FOappdata_processed["hv_house_value"]/FOappdata_processed["house_area"]
  FOappdata_processed["assets_ov_req_loan_amt"]=FOappdata_processed["totalassets"]/FOappdata_processed["ls_loan_amount_requested"]
  FOappdata_processed["guarantor_months_of_disposableincome_to_assets"] = FOappdata_processed["guarantor_totalassets"] / FOappdata_processed["guarantor_monthly_disposable_income"]
  FOappdata_processed["vehicle_value_ov_vehicle_count"]=FOappdata_processed["lta_vehicles"]/FOappdata_processed["vehicle_count"]

  #Drop fields used in calculation of ratios
  FOappdata_processed.drop(columns=Derive_guarantor_balance_sheet,inplace=True)
  FOappdata_processed.drop(columns=Derive_income_expense,inplace=True)
  FOappdata_processed.drop(columns=Derive_guarantor_income_expense,inplace=True)
  FOappdata_processed.drop(columns=Derive_balance_sheet,inplace=True)
  FOappdata_processed.drop(columns = "ls_fo_suggested_amount", inplace=True)

  print("FA data processed")
  log.debug("FA data processed")

  return FOappdata_processed

def GL_part2_byprocessingtype(FOappdata_processed):
  #Process Direct_process

  #do nothing

  #Process Output

  #see PART 1 for field list

  #do nothing


  #process Derive_binary

  Derive_binary = ['cd_applicant_signature',
  'cd_client_building_1',
  'cd_client_building_2',
  'cd_client_household_list',
  'cd_client_nrc_back',
  'cd_client_nrc_front',
  'cd_client_photo',
  'cd_house_document_1',
  'cd_house_document_2',
  'cd_vehicle_1',
  'cd_vehicle_2',
  'cd_vehicle_document_1',
  'cd_vehicle_document_2',
  'evaluation_question_1',
  'evaluation_question_15',
  'evaluation_question_17',
  'ilda_guarantor_nrc_back',
  'ilda_guarantor_nrc_front',
  'ilda_guarantor_photo',
  'ilda_guarantor_signature',
  'ld_business_building',
  'ld_business_vehicle_1',
  'ld_business_vehicle_2',
  'ld_farm_document_1',
  'ld_farm_document_2',
  'ld_farm_document_3',
  'ld_farm_document_4',
  'ld_farm_document_5',
  'ld_free_photo_1',
  'ld_free_photo_2',
  'ld_free_photo_3',
  'ld_photo_house_front',
  'ld_photo_house_side',
  'ld_prove_house_doc',
  'ld_requested_business_photo_1',
  'ld_requested_business_photo_2',
  'ld_vehicle_document',
  'ls_fo_suggestion_upon_cashflow']

  for column_name in Derive_binary:
      FOappdata_processed.loc[~FOappdata_processed[column_name].isna(),column_name]=1
      FOappdata_processed.loc[FOappdata_processed[column_name].isna(),column_name]=0


  #process Derive_area

  Derive_area = ['bp_business_area_length_in_feet',
  'bp_business_area_width_in_feet',
  'hv_house_area_length_feet',
  'hv_house_area_width_feet']

  FOappdata_processed["business_area"] = FOappdata_processed["bp_business_area_length_in_feet"]*FOappdata_processed["bp_business_area_width_in_feet"]
  FOappdata_processed["house_area"]= FOappdata_processed["hv_house_area_length_feet"]*FOappdata_processed["hv_house_area_width_feet"]
  FOappdata_processed.drop(columns = Derive_area,inplace=True)

  #process Derive_operator
  Derive_operator = ['guarantor_phone', 'client_mobile_no']

  for column_name in Derive_operator:
      FOappdata_processed[column_name] = FOappdata_processed[column_name].astype(str)
      FOappdata_processed[column_name]= FOappdata_processed[column_name].str.lstrip('0')
      FOappdata_processed[column_name] = FOappdata_processed[column_name].str[:3].replace("959","9") +           FOappdata_processed[column_name].str[3:15]
      FOappdata_processed[column_name] = FOappdata_processed[column_name].str.replace(".","")          .str.replace(' ','').str.replace('-','')
      FOappdata_processed.loc[FOappdata_processed[column_name].str[:1]!="9",column_name]=np.nan
      FOappdata_processed.loc[FOappdata_processed[column_name].str[:2]=="99",column_name]="Ooredoo"
      FOappdata_processed.loc[FOappdata_processed[column_name].str[:2]=="97",column_name]="Telenor"
      FOappdata_processed.loc[FOappdata_processed[column_name].str[:2]=="96",column_name]="Mytel"
      FOappdata_processed.loc[(FOappdata_processed[column_name]!="Ooredoo") & (FOappdata_processed[column_name]!="Telenor")                         & (FOappdata_processed[column_name]!="Mytel") & (~FOappdata_processed[column_name].isna()),column_name] = "MPT"

  #process Derive_hasotherloans
  Derive_hasotherloans = ['clch_component', 'clch_num_outstanding_loans']

  FOappdata_processed["has_other_loans"] = "No"
  FOappdata_processed.loc[(~FOappdata_processed["clch_component"].isna())|(~FOappdata_processed["clch_num_outstanding_loans"].isna()),"has_other_loans"]="Yes"
  FOappdata_processed.drop(columns = Derive_hasotherloans, inplace = True)

  #process Derive_from_efm_component
  Derive_from_efm_component = ['efm_component', 'efm_number']
  FOappdata_processed["efm_monthly_income"] = FOappdata_processed["efm_component"].fillna("").apply(processcomponent_efm_monthly_income)
  FOappdata_processed["efm_relation"] = FOappdata_processed["efm_component"].fillna("").apply(processcomponent_efm_relation)
  FOappdata_processed["efm_age"] = FOappdata_processed["efm_component"].fillna("").apply(processcomponent_efm_age)
  FOappdata_processed = FOappdata_processed.drop(columns = Derive_from_efm_component)



  #process Derive_age_from_this_DOB
  Derive_age_from_this_DOB = ['client_dob']
  for column_name in Derive_age_from_this_DOB:
      FOappdata_processed[column_name] = pd.to_datetime(FOappdata_processed[column_name],format="%Y-%m-%d",errors='coerce').apply(age)


  #process Derive_multiselection
  Derive_multiselection = ['as_livestocks_owned',
  'as_vehicles_owned',
  'hv_household_assets',
  'hv_services_available',
  'ls_loan_specific_use']

  Derive_multiselection.append("efm_relation")

  for column_name in Derive_multiselection:
      FOappdata_processed[column_name] = FOappdata_processed[column_name].fillna("").apply(removebrackets).replace("",np.nan,regex=False)
      FOappdata_processed[column_name] = FOappdata_processed[column_name].fillna("").str.replace("\\n","|",regex=False).str.replace(", ","|",regex=False).str.replace(",","|",regex=False).str.replace(" ","",regex=False)
      temp_dummy =  FOappdata_processed[column_name].str.get_dummies('|').add_prefix(column_name+"_")
      FOappdata_processed = pd.concat([FOappdata_processed,temp_dummy],axis=1)
      
      if column_name == "as_vehicles_owned":
          FOappdata_processed["vehicle_count"]=temp_dummy.sum(axis=1)
          
      if column_name == "ls_loan_specific_use":
          FOappdata_processed["loan_use_count"]=temp_dummy.sum(axis=1)
          
      if column_name == "hv_household_assets":
          FOappdata_processed["househould_asset_count"]=temp_dummy.sum(axis=1)
          
      if column_name == "hv_services_available":
          FOappdata_processed["househould_services_count"]=temp_dummy.sum(axis=1)

      if column_name == "efm_relation":
          FOappdata_processed["efm_relation_count"]=temp_dummy.sum(axis=1)
      
  FOappdata_processed.drop(columns = Derive_multiselection, inplace=True)

  #process Derive_count
  Derive_count = ['bp_business_nature_agriculture',
  'bp_business_nature_livestock',
  'bp_business_nature_production',
  'bp_business_nature_service',
  'bp_business_nature_trading']

  FOappdata_processed["num_business_types"] = FOappdata_processed[Derive_count].count(axis=1)


  #process Derive_ls_loan_amount_requested_grtr_ls_fo_suggested_amount
  Derive_ls_loan_amount_requested_grtr_ls_fo_suggested_amount = ['ls_fo_suggested_amount', 'ls_loan_amount_requested']

  FOappdata_processed["loanamount_suggested_requested_ratio"] =       (FOappdata_processed["ls_fo_suggested_amount"]/FOappdata_processed["ls_loan_amount_requested"])

  #process Derive_balance_sheet

  Derive_balance_sheet = ['ca_cash_in_bank_saving',
  'ca_cash_in_hand',
  'ca_inventory_value',
  'ca_livestock',
  'ca_other_assets',
  'liabilities_accounts_payable',
  'lta_buildings_lands',
  'lta_farm',
  'lta_machinery_equipment',
  'lta_vehicles']

  FOappdata_processed[Derive_balance_sheet] = FOappdata_processed[Derive_balance_sheet].fillna(0)
  FOappdata_processed["totalassets"]=0
  FOappdata_processed["totalliabilities"]=0

  for column_name in Derive_balance_sheet:
      if ((column_name[:3]=="ca_")|(column_name[:3]=="lta")):
          FOappdata_processed["totalassets"] = FOappdata_processed["totalassets"].add(FOappdata_processed[column_name],axis=0)
      elif (column_name[:3]=="lia"):
          FOappdata_processed["totalliabilities"] = FOappdata_processed["totalliabilities"].add(FOappdata_processed[column_name],axis=0)
  Derive_balance_sheet.append("totalassets")
  Derive_balance_sheet.append("totalliabilities")

  #process Derive_oi_then_income_expense
  Derive_oi_then_income_expense = ['oi_component', 'oi_number']

  FOappdata_processed["oi_derived"]=FOappdata_processed["oi_component"].str.extract('(\d+)').fillna(0).astype(int)
  FOappdata_processed["oi_derived"] = FOappdata_processed[["oi_derived","oi_number"]].max(axis=1)
  FOappdata_processed.drop(columns = Derive_oi_then_income_expense, inplace=True)

  #process Derive_income_expense

  Derive_income_expense = ['be_monthly_raw_cost',
  'be_other',
  'be_rent',
  'be_salaries',
  'be_transportation',
  'be_utilities',
  'bi_monthly_income',
  'pe_clothing',
  'pe_donation',
  'pe_education',
  'pe_festival',
  'pe_food',
  'pe_medical',
  'pe_other',
  'pe_rent',
  'pe_transportation',
  'pe_utility']
  Derive_income_expense.append("oi_derived")

  FOappdata_processed[Derive_income_expense] = FOappdata_processed[Derive_income_expense].fillna(0)
  FOappdata_processed["monthly_income"]=0
  FOappdata_processed["monthly_expense"]=0

  for column_name in Derive_income_expense:
    if (column_name[:8]=="bi_daily"):
        FOappdata_processed["monthly_income"] = FOappdata_processed["monthly_income"].add(30*FOappdata_processed[column_name].fillna(0),axis=0)
    elif (((column_name[:3]=="bi_")&(column_name[:8]!="bi_daily"))|(column_name[:3]=="oi_")):
        FOappdata_processed["monthly_income"] = FOappdata_processed["monthly_income"].add(FOappdata_processed[column_name].fillna(0),axis=0)
    elif (column_name[:8]=="be_daily"):
        FOappdata_processed["monthly_expense"] = FOappdata_processed["monthly_expense"].add(30*FOappdata_processed[column_name].fillna(0),axis=0)
    elif (((column_name[:3]=="be_")&(column_name[:8]!="be_daily"))|(column_name[:3]=="pe_")):
        FOappdata_processed["monthly_expense"] = FOappdata_processed["monthly_expense"].add(FOappdata_processed[column_name].fillna(0),axis=0)

  Derive_income_expense.append("monthly_income")
  Derive_income_expense.append("monthly_expense")


  #Derive ratios
  FOappdata_processed["applicant_networth"]=FOappdata_processed["totalassets"].subtract(FOappdata_processed["totalliabilities"],axis=0)
  FOappdata_processed["applicant_monthly_disposable_income"]=FOappdata_processed["monthly_income"].subtract(FOappdata_processed["monthly_expense"],axis=0)
  FOappdata_processed["applicant_income_to_expense_ratio"]=FOappdata_processed["monthly_income"]/FOappdata_processed["monthly_expense"]
  FOappdata_processed["applicant_months_of_disposableincome_to_assets"]=FOappdata_processed["totalassets"]/FOappdata_processed["applicant_monthly_disposable_income"]
  FOappdata_processed["applicant_months_of_disposableincome_for_totalrepayment"]=FOappdata_processed["ls_loan_amount_requested"]/FOappdata_processed["applicant_monthly_disposable_income"]
  FOappdata_processed["house_value_ov_net_income"]=FOappdata_processed["hv_house_value"]/FOappdata_processed["applicant_monthly_disposable_income"]
  FOappdata_processed["house_value_ov_req_loan_amt"]=FOappdata_processed["hv_house_value"]/FOappdata_processed["ls_loan_amount_requested"]
  FOappdata_processed["farm_value_ov_net_income"]=FOappdata_processed["api_value_of_plot"]/FOappdata_processed["applicant_monthly_disposable_income"]
  FOappdata_processed["farm_value_ov_req_loan_amt"]=FOappdata_processed["api_value_of_plot"]/FOappdata_processed["ls_loan_amount_requested"]
  FOappdata_processed["farm_value_ov_farm_area"]=FOappdata_processed["api_value_of_plot"]/FOappdata_processed["lta_farm_size"]
  FOappdata_processed["housevalue_ov_house_area"]=FOappdata_processed["hv_house_value"]/FOappdata_processed["house_area"]
  FOappdata_processed["assets_ov_req_loan_amt"]=FOappdata_processed["totalassets"]/FOappdata_processed["ls_loan_amount_requested"]
  FOappdata_processed["vehicle_value_ov_vehicle_count"]=FOappdata_processed["lta_vehicles"]/FOappdata_processed["vehicle_count"]


  #Drop fields used in calculation of ratios
  FOappdata_processed.drop(columns = "ls_fo_suggested_amount", inplace=True)
  FOappdata_processed.drop(columns = Derive_income_expense, inplace=True)
  FOappdata_processed.drop(columns = Derive_balance_sheet, inplace=True)

  #process Simplify
  Simplify = ['app_client_app_install', 'app_not_install_reason']

  FOappdata_processed.loc[FOappdata_processed["app_client_app_install"].astype(str).str.contains("Yes",na=False),"app_client_app_install"]=1
  FOappdata_processed.loc[FOappdata_processed["app_client_app_install"].astype(str).str.contains("No",na=False),"app_client_app_install"]=0

  FOappdata_processed.loc[FOappdata_processed["app_not_install_reason"].isna(),"app_not_install_reason"]="Blank"
  FOappdata_processed.loc[FOappdata_processed["app_not_install_reason"].str.contains("အင်တာနက်",na=False),"app_not_install_reason"]="No internet"
  FOappdata_processed.loc[(FOappdata_processed["app_not_install_reason"].str.contains("Keypad",na=False))                     | (FOappdata_processed["app_not_install_reason"].str.contains("ဖုန်း",na=False))                         | (FOappdata_processed["app_not_install_reason"].str.contains("iPhone",na=False)),"app_not_install_reason"]="No Android Phone"
  FOappdata_processed.loc[(FOappdata_processed["app_not_install_reason"]!="No internet") & (FOappdata_processed["app_not_install_reason"]!="No Android Phone" )                     & (FOappdata_processed["app_not_install_reason"]!="Blank" ),"app_not_install_reason"]="Other"
  FOappdata_processed["app_not_install_reason"] = FOappdata_processed["app_not_install_reason"].replace("Blank",np.nan)

  return FOappdata_processed

def ILGL_part3_finalprocessing(alldf):
  
  #PART 3: OUTPUT SPECIFICATION A
  #-------------------------------
  alldf = alldf.applymap(lambda x: x.strip().lower() if isinstance(x, str) else x).fillna(np.nan)

  alldf = alldf.rename(columns=lambda x: removebrackets(x))

  highskew_binary_drop_candidate = []
  rejection_threshold = 0.1

  for column_name in alldf.columns:
      if (alldf[column_name].dtype==object) & (alldf[column_name].isin(["yes",1,0]).astype(int).sum()>0):
          alldf.loc[alldf[column_name] == "yes", column_name] = 1
          alldf.loc[alldf[column_name] == "no", column_name] = 0
          alldf.loc[alldf[column_name] == "don't know", column_name] = np.nan
          alldf[column_name] = alldf[column_name].astype(float)
      
      if (alldf[column_name].dtype==object) & (column_name != "interview_date"):
        alldf[column_name] = alldf[column_name].fillna("").apply(removebrackets).replace("",np.nan)
      
      if (alldf[column_name].nunique() <= 2) and ((alldf[column_name].dtype == "float32") or (alldf[column_name].dtype == "float64") or (alldf[column_name].dtype == "int32") or (alldf[column_name].dtype == "int64")):
          if (alldf[column_name].mean() <= rejection_threshold) or (alldf[column_name].mean() >= (1-rejection_threshold)):
            highskew_binary_drop_candidate.append(column_name)
      
      if ((alldf[column_name].dtype == "float32") or (alldf[column_name].dtype == "float64") or           (alldf[column_name].dtype == "int32") or (alldf[column_name].dtype == "int64")):
        alldf[column_name] = alldf[column_name].replace(np.inf,np.nan) #remove inf values to eliminate inf mean
        alldf.loc[alldf[column_name] <0, column_name] = np.nan #remove non-zero
        alldf[column_name] = np.trunc(10000 * alldf[column_name]) / 10000 

  #highskew_binary_drop = [x for x in highskew_binary_drop_candidate if x not in Output] #remove output variables from binary drop list


  highskew_binary_drop = ['abp_how_many_plots_do_you_farm',
  'agri_practices_question_1',
  'agri_practices_question_2',
  'agri_practices_question_4',
  'bp_can_business_operate_without_owner',
  'br_business_always_open',
  'br_client_make_payment_ontime',
  'cd_applicant_signature',
  'cd_client_building_1',
  'cd_client_household_list',
  'cd_client_nrc_back',
  'cd_client_nrc_front',
  'cd_client_photo',
  'cd_house_document_2',
  'cd_vehicle_2',
  'cd_vehicle_document_1',
  'cd_vehicle_document_2',
  'evaluation_question_1',
  'evaluation_question_10',
  'evaluation_question_11',
  'evaluation_question_12',
  'evaluation_question_14',
  'evaluation_question_15',
  'evaluation_question_17',
  'evaluation_question_2',
  'evaluation_question_7',
  'evaluation_question_9',
  'hd_family_agree_taking_loan',
  'hd_pregnant_family_member',
  'ilda_coapplicant_household_list',
  'ilda_coapplicant_nrc_back',
  'ilda_coapplicant_nrc_front',
  'ilda_coapplicant_photo',
  'ilda_coapplicant_signature',
  'ilda_guarantor_household_list',
  'ilda_guarantor_nrc_back',
  'ilda_guarantor_nrc_front',
  'ilda_guarantor_photo',
  'ilda_guarantor_signature',
  'ld_applicant_bank_book',
  'ld_business_bank_book',
  'ld_farm_document_2',
  'ld_farm_document_3',
  'ld_farm_document_4',
  'ld_farm_document_5',
  'ld_household_income_doc_1',
  'ld_household_income_doc_2',
  'ld_household_income_doc_3',
  'ld_other_income_business_doc3',
  'ld_vehicle_document',
  'ls_fo_suggestion_upon_cashflow',
  'MCIX_WriteOff_Count',
  'efm_monthly_income',
  'as_vehicles_owned_Bus',
  'as_vehicles_owned_Trishaw',
  'as_vehicles_owned_Truck',
  'hv_household_assets_Autorickshaw',
  'hv_household_assets_Television',
  'hv_services_available_Electricity',
  'hv_services_available_Water',
  'ls_loan_specific_use_AssetPurchase',
  'ls_loan_specific_use_Others',
  'ls_loan_specific_use_ShopRepair/Renovation',
  'efm_relation_Child',
  'efm_relation_In-laws',
  'efm_relation_Otherrelatives',
  'efm_relation_Parents',
  'efm_relation_Siblings',
  'efm_relation_Spouse',
  'bp_good_time_for_visit_NA',
  'bp_location_advantages_NA',
  'bp_location_advantages_None']


  alldf.drop(columns = highskew_binary_drop,inplace=True,errors='ignore')

  return alldf

def generate_onehotencoding(regrouped_data,binary_list,numerical_list,categorical_list):

  regrouped_data_pre_drop = regrouped_data

  not_used = ["office_name", "interview_id", "FO_ID","FO_username_FF","FO_username_DB","interview_date","loan_id"]
  regrouped_data = regrouped_data.drop(columns = not_used)

  #shorten lists based on what actually exists in current data
  binary_list = list(set.intersection(set(binary_list),set(regrouped_data.columns)))
  numerical_list = list(set.intersection(set(numerical_list),set(regrouped_data.columns)))
  categorical_list = list(set.intersection(set(categorical_list),set(regrouped_data.columns)))


  print("Generating dummies")
  log.debug("Generating dummies")
  #One hot encoding

  dummy_df = pd.DataFrame() #we will use this to hold the final dummy data
  current_dummy_df = pd.DataFrame() #we will use this to hold the dummy data currently being processed
  dummylist = categorical_list + binary_list

  runningmaxcount = 0
  for column_name in dummylist:
    runningmaxcount = max([regrouped_data[column_name].nunique(),runningmaxcount])

  print("Max unique items in columns: " + str(runningmaxcount))

  #create and append dummy columns
  for column_name in dummylist: #loop through only specific columns of the dataframe
      if regrouped_data[column_name].isna().sum()>0:
        current_dummy_df = pd.get_dummies(regrouped_data[column_name],drop_first=False, prefix = column_name, prefix_sep = "__", dummy_na=True) #use __ notation to work with scorecard generation code later in code
      else:
        current_dummy_df = pd.get_dummies(regrouped_data[column_name],drop_first=False, prefix = column_name, prefix_sep = "__", dummy_na=False)
      dummy_df = pd.concat([dummy_df,current_dummy_df],axis=1)

  #append numerical to dummy data
  for column_name in numerical_list:
      dummy_df = pd.concat([dummy_df,regrouped_data[column_name]],axis=1)

  #remove non-compliant characters in column names to allow XGBoost to run
  dummy_df= dummy_df.rename(columns=lambda s: s.replace("]", ")"))
  dummy_df= dummy_df.rename(columns=lambda s: s.replace("<", "less_than"))

  dummy_df = pd.concat([regrouped_data_pre_drop["interview_id"],dummy_df],axis = 1)

  print("SIDP complete")
  log.debug("SIDP complete")

  return dummy_df

#--------------------------------------------------
def validate_livescoring(output_during_SCbuild,validationloandata):
  output_during_SCbuild = output_during_SCbuild.sort_values(by = "interview_id")
  validationloandata = validationloandata.sort_values(by = "interview_id")
  validationloandata.rename(columns = {'pd':'Pd'}, inplace = True)
  
  output_during_SCbuild = output_during_SCbuild[validationloandata.columns].reset_index(drop=True)

  input_comparison_df = output_during_SCbuild.drop(columns="Pd").compare(validationloandata.drop(columns="Pd"))
  Pd_same = ((output_during_SCbuild["Pd"].sum() - validationloandata["Pd"].sum())<0.001)

  if (len(input_comparison_df) == 0) and (Pd_same == True):
    print("Validation: all inputs and outputs identical")
    log.info("Validation: all inputs and outputs identical")
  else:
    print("Validation: please resolve following differences in live scoring code")
    log.critical("Validation: please resolve following differences in live scoring code")
    display(input_comparison_df)

#-------------------------------------------------------
def get_interviewidlist(ILGL,model_path):

  startDate="2022-04-01"
  endDate=datetime.datetime.now(local_tz).strftime("%Y-%m-%d %H:%M")

  if ILGL == "IL":
    loan_type_str = "('individual','topup individual')"
  elif ILGL == "GL":
    loan_type_str = "('group','topup group')"

  # [2022-06-23, Naing] added scenario for  
  #  1. not to include already scored interviews
  #  2. failed to score interviews
  #  3. success to score but resubmit interviews

  # [2022-06-24, Naing] modified sql to get client id from appointment client table
  log.info("Get interview Ids - interview date from {0} to {1}".format(startDate, endDate))
  print("Get interview Ids - interview date from {0} to {1}".format(startDate, endDate))

  interviewid_list=cx.read_sql(db_connection_str,   "SELECT i.id interview_id,   IF(ic.client_id IS NULL OR ic.client_id = 0, IF(iac.finflux_id IS NULL OR iac.finflux_id =0, ic.client_id, iac.finflux_id), ic.client_id)  client_id,   i.created_at interview_date, lp.loan_product   FROM kebhana_dashboard_db.db_interviews AS i   LEFT JOIN (SELECT i.id as interview_id, JSON_EXTRACT(ia.value,'$[0]') as loan_product   FROM kebhana_dashboard_db.db_interviews AS i   LEFT JOIN kebhana_dashboard_db.db_interview_answers ia on ia.interview_id = i.id   LEFT JOIN kebhana_dashboard_db.db_questions q on ia.question_name = q.name   WHERE q.name = 'ls_loan_product'   ) AS lp on lp.interview_id=i.id   LEFT JOIN kebhana_dashboard_db.db_interview_clients ic ON ic.appointment_client_id=i.appointment_client_id   LEFT JOIN kebhana_dashboard_db.db_appointment_clients iac ON iac.id=i.appointment_client_id   LEFT JOIN kebhana_middleware_db.m_loan AS l ON l.id=i.loan_id   LEFT JOIN (	SELECT DISTINCT cxx.interview_id, cxx.status, cxx.created_at      FROM kebhana_dashboard_db.hwa_cs_output AS cxx      WHERE cxx.created_at = (SELECT max(cxo.created_at) FROM kebhana_dashboard_db.hwa_cs_output AS cxo WHERE cxo.interview_id = cxx.interview_id)   ) AS cso ON cso.interview_id = i.id   WHERE i.loan_type IN " + loan_type_str + "  AND (l.loan_status_id IS NULL OR l.loan_status_id IN (100) )   AND i.interview_status IN ('pending','updated')   AND (   IF(cso.status IS NULL, '', cso.status) NOT IN ('success','no-score')   OR (cso.status='success' And IF(i.updated_at IS NULL,'1099-01-01', i.updated_at) > cso.created_at)   OR (cso.status IS NULL AND cso.interview_id IS NULL)   )   AND i.created_at BETWEEN '"+startDate+"' AND '"+endDate+"';")

  interviewid_list["loan_product"] = interviewid_list["loan_product"].str.strip('"')

  product_categorisation = cx.read_sql(db_connection_str,"SELECT * FROM kebhana_middleware_db.hwa_loan_product_name_mapping").astype(str)
  product_categorisation = product_categorisation.replace("b'","",regex=True).replace("'","",regex=True)

  product_dictionary = dict(zip(product_categorisation["loan_product_name"], product_categorisation["repayment_type"]))

  interviewid_list["loan_product_mapped"] = interviewid_list["loan_product"].map(product_dictionary)

  found_interviewid_len = len(interviewid_list)

  interviewid_list_original = interviewid_list
  print("Total found: " + str(found_interviewid_len) + " interview_id")
  log.info("Total found: " + str(found_interviewid_len) + " interview_id")

  #no client id
  noclientid_interviewid = interviewid_list["interview_id"][interviewid_list["client_id"]==0]
  noclientid_df = pd.DataFrame()
  noclientid_df["interview_id"] = noclientid_interviewid
  noclientid_df["remark"] = "no client_id"
  log.warning("Excluded: Client id missing for " + str(len(noclientid_interviewid)) + " interview_id")
  interviewid_list = interviewid_list[interviewid_list["client_id"]!=0]

  #null loan product
  noloanproduct_interviewid = interviewid_list["interview_id"][interviewid_list["loan_product"].isna()]
  noloanproduct_df = pd.DataFrame()
  noloanproduct_df["interview_id"] = noloanproduct_interviewid
  noloanproduct_df["remark"] = "no loan product in interview form"
  log.warning("Excluded: Loan product not in interview form for " + str(len(noloanproduct_interviewid)) + " interview_id")
  interviewid_list = interviewid_list[~interviewid_list["loan_product"].isna()]

  #null loan product mapping
  noloanproductmapping_interviewid = interviewid_list["interview_id"][interviewid_list["loan_product_mapped"].isna()]
  noloanproductmapping_df = pd.DataFrame()
  noloanproductmapping_df["interview_id"] = noloanproductmapping_interviewid
  noloanproductmapping_df["remark"] = "no loan product mapping"

  needloanmapping = interviewid_list["loan_product"][interviewid_list["interview_id"].isin(noloanproductmapping_interviewid)].unique()
  if len(needloanmapping)>0:
    log.warning("Excluded: Loan product mapping missing for " + str(len(noloanproductmapping_interviewid)) + " interview_id") 
    log.warning("Add loan product mapping for: " + needloanmapping)
  interviewid_list = interviewid_list[~interviewid_list["loan_product_mapped"].isna()]

  #fullbullet loan product
  fullbullet_interviewid = interviewid_list["interview_id"][interviewid_list["loan_product_mapped"] == "FullBullet"]
  fullbullet_df = pd.DataFrame()
  fullbullet_df["interview_id"] = fullbullet_interviewid
  fullbullet_df["remark"] = "full bullet loan not scored"
  log.warning("Excluded: Full bullet " + str(len(fullbullet_interviewid)) + " interview_id")
  interviewid_list = interviewid_list[interviewid_list["loan_product_mapped"] != "FullBullet"]

  #staff loans
  staffloan_interviewid = interviewid_list["interview_id"][interviewid_list["loan_product_mapped"] == "HanaStaffLoan"]
  staffloan_df = pd.DataFrame()
  staffloan_df["interview_id"] = staffloan_interviewid
  staffloan_df["remark"] = "staff loans not scored"
  log.warning("Excluded: Staff loans " + str(len(staffloan_interviewid)) + " interview_id")
  interviewid_list = interviewid_list[interviewid_list["loan_product_mapped"] != "HanaStaffLoan"]

  #CHECK - add new exclusions to no_score_writetoDB_df
  created_at_x = datetime.datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")

  no_score_writetoDB_df = pd.concat([noclientid_df,noloanproduct_df,noloanproductmapping_df,fullbullet_df,staffloan_df]).reset_index(drop = True)
  no_score_writetoDB_df[["created_at", "updated_at"]] = created_at_x
  no_score_writetoDB_df["status"]="error"
  no_score_writetoDB_df["sc_name"] = re.search(r'([^\/]+$)',model_path).group(1).replace(".model","")
  no_score_writetoDB_df["display_text"]= """
<table class="table table-borderless">
<tbody>
    <tr><th>Scorecard Number</th><td>"""+no_score_writetoDB_df["sc_name"]+"""</td></tr>
    <tr><th>Message</th><td style="color:red">""" + no_score_writetoDB_df["status"] + " - " + no_score_writetoDB_df["remark"] + """</td></tr>
    <tr><th>Scored At</th><td>"""+no_score_writetoDB_df["created_at"] +"""</td></tr>
</tbody>
</table>
"""

  print("Total for scoring: " + str(len(interviewid_list))  + " interview_id")
  log.debug("Total for scoring: " + str(len(interviewid_list))  + " interview_id")

  
  log.warning("Total rejected: " +str(len(no_score_writetoDB_df)) + " interview_id")

  #CHECK - add new exclusions to this checksum
  if (len(no_score_writetoDB_df) != sum([len(staffloan_interviewid),len(noclientid_interviewid),len(noloanproduct_interviewid),                                        len(noloanproductmapping_interviewid),len(fullbullet_interviewid)])) or (
      len(interviewid_list) != found_interviewid_len - sum([len(staffloan_interviewid),len(noclientid_interviewid),len(noloanproduct_interviewid),\
                                                            len(noloanproductmapping_interviewid),len(fullbullet_interviewid)])):
    log.warning("ERROR: exclusion checksum mismatch - CHECK ")
  else:
    print("interview_id exclusion checksum OK")
    log.info("interview_id exclusion checksum OK")

  return interviewid_list["interview_id"].tolist(), no_score_writetoDB_df,interviewid_list_original


#-------------------------------------------------------
# [2022-06-28, Naing] Authenticate the API access.
# Whenever to call db_write_xxx functions, it needs access token from this function.
def db_api_login():
    token = ""
    
    log.info("Login to dashboard API.")
    login_url = "/".join([db_api_host, db_api_root,"login"])
    try:
        response = requests.post(login_url, data={"user_name":"", "password":""})
        
        if response.status_code == 200:
            log.info("Login success, getting access token.")
            data = json.loads(response.text)
            
            if (data):
                data = data["data"]
                if "verification_token" in data:
                    token = data["verification_token"]                    
                else:
                    raise Exception("API success reponse does not include verification_token.")
            else:
                raise Exception("API success reponse does not include data content.") 
            
        else:
            log.error("Cannot login to the dashboard API. Response Code: " + str(response.status_code) + response.text)
        
    except Exception as ex:
        log.error("Error happened while calling dashboard API. " + str(ex))
    
    return token

# [2022-06-28, Naing] Write data to hwa_cs_output table.
# This function will insert new record with the given data. Status can be either "success" or "error". 
def db_write_cs_output(data_to_write, status):
    if data_to_write.empty or len(data_to_write) == 0:
      log.info("No data to process.")
      return False

    log.info("Authenticate dashboard API.")
    
    try:
        token = db_api_login()
    
        if (token == None or token == ""):
            raise Exception("Access token did not receive.")
        else:
            log.info("Authenticated.")
        
        db_write_url = "/".join([db_api_host, db_api_root,"credit-score-output"])
    
        log.info("Iterate data rows to save. Total " + str(len(data_to_write)))
        
        for index, row in data_to_write.iterrows():
            interview_id = row["interview_id"]
            log.info("Saving Interview ID :" + interview_id + " to cs_output.")
            
            if status == "success":
                data = {
                    "interview_id" : interview_id,
                    "sc_name" : row["sc_name"],
                    "pd" : row["pd"],
                    "input_raw" : row["input_raw"],
                    "input_sidp" : row["input_sidp"],
                    "output" : row["output"],
                    "status" : row["status"],
                    "display_text": row["display_text"],
                    "remark" : "",
                    "created_at" : row["created_at"],
                    "updated_at" : row["updated_at"]
                }
            else:
                status_x = row["status"]
                remark_x = row["remark"]                 
                if "full bullet loan not scored" in remark_x or "staff loans not scored" in remark_x:
                  status_x = "no-score"
                data = {
                    "interview_id" : interview_id,
                    "sc_name" : row["sc_name"],
                    "pd" : 0,
                    "status" : status_x,
                    "display_text": row["display_text"],
                    "remark" : row["remark"],
                    "created_at" : row["created_at"],
                    "updated_at" : row["updated_at"]
                }

            request_headers = {
                "accept" : "application/json",
                "Content-Type" : "application/json",
                "Authorization" : "Bearer " + token
            }
            
            response = requests.post(db_write_url,headers=request_headers,json=data)
            
            if response.status_code == 200:
                log.info("Interview ID" + interview_id + " has been saved to cs_output.")
            else:
                log.error("Interview ID" + interview_id + " cannot be saved to cs_output. Code: " + str(response.status_code) + ". Message: " + response.text)
        
    except Exception as ex:
        log.error("Error happened while writing data to cs_output. " + str(ex))
        
    return True

# [2022-06-28, Naing] Write data to hwa_credit_score table.
# This function will insert new record with the given data. Status can be either "success" or "error". 
def db_write_cs_display(data_to_write, status):
    if data_to_write.empty or len(data_to_write) == 0:
      log.info("No data to process.")
      return False

    log.info("Authenticate dashboard API.")
    
    try:
        token = db_api_login()
    
        if (token == None or token == ""):
            raise Exception("Access token did not receive.")
        else:
            log.info("Authenticated.")
        
        db_write_url = "/".join([db_api_host, db_api_root,"credit-score-display"])
    
        log.info("Iterate data rows to save. Total " + str(len(data_to_write)))
        
        for index, row in data_to_write.iterrows():
            interview_id = row["interview_id"]
            log.info("Saving Interview ID :" + interview_id)
            
            if status == "success":
                data = {
                    "score" : row["pd"],
                    "score_percent": 0,
                    "eligible_amount" : 0,
                    "styled_score": "<span class='badge label-info'>" + str(round(row["pd"],4)) + "</span>",
                    "styled_risk_level" : "",
                    "styled_eligible_amount" : "",
                    "db_view_html" : row["display_text"],
                    "db_interviews_id" : interview_id,
                    "created_at" : row["created_at"],
                    "updated_at" : row["updated_at"]
                }
            else:
                data = {
                    "score" : 0,
                    "score_percent": 0,
                    "eligible_amount" : 0,
                    "styled_score": "<span class='badge label-info'>NA.</span>",
                    "styled_risk_level" : "",
                    "styled_eligible_amount" : "",
                    "db_view_html" : row["display_text"],
                    "db_interviews_id" : interview_id,
                    "created_at" : row["created_at"],
                    "updated_at" : row["updated_at"]
                }
            
            request_headers = {
                "accept" : "application/json",
                "Content-Type" : "application/json",
                "Authorization" : "Bearer " + token
            }
            
            response = requests.post(db_write_url,headers=request_headers,json=data)
            
            if response.status_code == 200:
                log.info("Interview ID" + interview_id + " has been saved to credit_score.")
            else:
                log.error("Interview ID" + interview_id + " cannot be saved to credit_score for display. Code: " + str(response.status_code) + ". Message: " + response.text)
        
    except Exception as ex:
        log.error("Error happened while writing data to cs_output. " + str(ex))
        
    return True

#------------------------------------------------------------
def showdb():
  # This function is to check whether the inserted data are saved to database.
  data = cx.read_sql(db_connection_str,"""SELECT * FROM kebhana_dashboard_db.hwa_cs_output ORDER BY ID DESC LIMIT 10;""")
  return data



#----------------------------------------------------
# LIVE SCORING MASTER FUNCTION

def createscore(interviewid_list,model_path,ILGL,val_live,definition):

  """
  #for easy debug purposes
  interviewid_list = IL_interviewid_list
  model_path=IL_model_path
  ILGL="IL"
  val_live="live"
  definition=IL_model_definition
  """

  print(ILGL + " now scoring " + str(len(interviewid_list)) + " loans " + val_live)
  log.debug(ILGL + " now scoring " + str(len(interviewid_list)) + " loans " + val_live)

  if val_live == "live":
    FOappdata = ILGL_part1_read_dashboard_SCI(interviewid_list)#nearly identical to SCG SIDP function, date filter replaced with interviewid filter
  elif val_live == "val":
    FOappdata = ILGL_part1_read_dashboard_SCG(interviewid_list)#nearly identical to SCG SIDP function, date filter replaced with interviewid filter

  FOappdata = ILGL_part1_readothertables(FOappdata,interviewid_list)#identical to SCG SIDP function

  if ILGL == "IL":
    FOappdata = IL_part1_reorderfields(FOappdata)# identical to SCG SIDP function
    SID = IL_part2_byprocessingtype(FOappdata)# identical to SCG SIDP function
    SID = ILGL_part3_finalprocessing(SID)# identical to SCG SIDP function
    scoringloandata = generate_onehotencoding(SID,IL_binary_list,IL_numerical_list,IL_categorical_list)

  elif ILGL == "GL":
    FOappdata = GL_part1_reorderfields(FOappdata) #identical to SCG SIDP function
    SID = GL_part2_byprocessingtype(FOappdata) #identical to SCG SIDP function
    SID = ILGL_part3_finalprocessing(SID)# identical to SCG SIDP function
    scoringloandata = generate_onehotencoding(SID,GL_binary_list,GL_numerical_list,GL_categorical_list)  

  loaded_model = pickle.load(open(model_path,"rb"))
  bstr = loaded_model.get_booster()
  cols_when_model_built = bstr.feature_names

  scoringloandata[list(set(cols_when_model_built) - set(scoringloandata.columns))] = np.nan

  Pd_df = pd.DataFrame()
  Pd_df["interview_id"] = scoringloandata["interview_id"]
  Pd_df["pd"] = loaded_model.predict_proba(scoringloandata[cols_when_model_built])[:,1]
  Pd_df["sc_name"] = re.search(r'([^\/]+$)',model_path).group(1).replace(".model","")
  Pd_df["input_sidp"] = SID.apply(lambda x: x.to_json(),axis=1)
  Pd_df["input_raw"] = FOappdata.apply(lambda x: x.to_json(),axis=1)
  Pd_df[["created_at", "updated_at"]] = datetime.datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
  Pd_df["status"] = "success"
  Pd_df["display_text"] = """
  <table class="table table-borderless">
  <tbody>
    <tr><th>Scorecard Number</th><td>""" + Pd_df["sc_name"] + """</td></tr>
    <tr><th>Pd Definition</th><td>""" + definition[0] + """</td></tr>
    <tr><th>Performance Window</th><td>""" + definition[1] + """</td></tr>
    <tr><th>Total Score</th><td>"""+ Pd_df["pd"].astype(str) + """</td></tr>
    <tr><th>Scored At</th><td>""" + Pd_df["created_at"] + """</td></tr>
  </tbody>
  </table>
  """

  #generate Xaddscores by tree, write json to Pd_df["output"]
  mdf = bstr.trees_to_dataframe()
  mdf = mdf.loc[mdf['Tree'] <= bstr.best_iteration]
  featurelist = mdf["Feature"].str.split("__",expand=True)[0].unique().tolist()
  featurelist.remove("Leaf")

  treenodelist = bstr.predict(xgboost.DMatrix(scoringloandata[cols_when_model_built]),pred_leaf=True)
  resultdf = pd.DataFrame(0, index = range(0,len(treenodelist)), columns = ["base_score"] + list(range(0,bstr.best_iteration+1)))
  resultdf["base_score"] = math.log(loaded_model.get_xgb_params()["base_score"]/(1-loaded_model.get_xgb_params()["base_score"]))

  for tree in range(0,bstr.best_iteration+1):
    treelist=[]
    for loan in range(0,len(treenodelist)):
      treelist.append(mdf["Gain"][(mdf["Tree"]==tree) & (mdf["Node"] == treenodelist[loan][tree])].values[0])
    resultdf[tree] = treelist

  resultdf["Implied Pd"] =resultdf[["base_score"]+list(range(0,bstr.best_iteration+1))].sum(axis=1).apply(lambda x: math.exp((x))/(1+math.exp((x))))
  resultdf["Check Pd Difference"] = (abs(resultdf["Implied Pd"] - Pd_df["pd"])).round(decimals = 5)
  Pd_df["output"] = resultdf.apply(lambda x: x.to_json(),axis=1)

  if val_live == "val":
    livescoring_validationloandata = SID[["interview_id","interview_date","client_id"] + featurelist]
    livescoring_validationloandata["pd"] =   Pd_df["pd"]
  else:
    livescoring_validationloandata = pd.DataFrame()

  print("Scoring complete for " + str(len(interviewid_list)) + " loans")
  log.debug("Scoring complete for " + str(len(interviewid_list)) + " loans")


  return Pd_df, livescoring_validationloandata


# In[11]:


# LIVE SCORING CODE BODY

IL_model_path = "/content/drive/MyDrive/Model Archive/IL_xgbmodel_SC0_20220628.model"
IL_model_definition = ["IL, >=10d latedays OR <=99% repaid OR >=1 reschedule","within 183 days"]

GL_model_path = "/content/drive/MyDrive/Model Archive/GL_xgbmodel_SC0_20220628.model"
GL_model_definition = ["GL, >=10d latedays OR <=99% repaid OR >=1 reschedule", "within 183 days"]

IL_interviewid_list, IL_reject_writetodb, _ = get_interviewidlist("IL",IL_model_path)
GL_interviewid_list, GL_reject_writetodb, _ = get_interviewidlist("GL",GL_model_path)

if len(IL_interviewid_list) > 0:
  IL_success_writetodb, _ = createscore(IL_interviewid_list,IL_model_path,"IL","live",IL_model_definition)
else:
  log.info("No interviews to score.")
  print("No interviews to score.")

if len(GL_interviewid_list) > 0:
  GL_success_writetodb, _ = createscore(GL_interviewid_list,GL_model_path,"GL","live",GL_model_definition)
else:
  log.info("No interviews to score.")
  print("No interviews to score.")
  

#write_to_DB(IL_success_writetodb)
#write_to_DB(IL_reject_writetodb)
#write_to_DB(GL_success_writetodb)
#write_to_DB(GL_reject_writetodf)
#data = showdb()

# [2022-06-28, Naing] Write results to database
# If value is "live", it will call database api to write results.
program_run_mode = "test" 
if program_run_mode == "live":
  print("Program is in live mode. Scroe results will be output to database.")
  # For IL
  db_write_cs_output(IL_success_writetodb, "success")
  db_write_cs_display(IL_success_writetodb, "success")
  db_write_cs_output(IL_reject_writetodb, "error")
  db_write_cs_display(IL_reject_writetodb, "error")

  # For GL
  db_write_cs_output(GL_success_writetodb, "success")
  db_write_cs_display(GL_success_writetodb, "success")
  db_write_cs_output(GL_reject_writetodb, "error")
  db_write_cs_display(GL_reject_writetodb, "error")
else:
  print("Program is in testing mode. Writing to database will be skipped.")


# # Validation
# 

# In[ ]:


#IL VALIDATION
validation_SCbuild = pd.read_csv("/content/drive/MyDrive/SCG Output/IL_SC0_20220628_validation.csv")
validation_interviewid_list = validation_SCbuild["interview_id"]
Pd_df, livescoring_validationloandata = createscore(validation_interviewid_list,"/content/drive/MyDrive/Model Archive/IL_xgbmodel_SC0_20220628.model","IL","val",IL_model_definition)
validate_livescoring(validation_SCbuild, livescoring_validationloandata)
datetime.datetime.now()


# In[ ]:


#GL VALIDATION
validation_SCbuild = pd.read_csv("/content/drive/MyDrive/SCG Output/GL_SC0_20220628_validation.csv")
validation_interviewid_list = validation_SCbuild["interview_id"]
Pd_df, livescoring_validationloandata = createscore(validation_interviewid_list,"/content/drive/MyDrive/Model Archive/GL_xgbmodel_SC0_20220628.model","GL","val", GL_model_definition)
validate_livescoring(validation_SCbuild, livescoring_validationloandata)
datetime.datetime.now()

