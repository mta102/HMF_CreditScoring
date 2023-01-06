#!/usr/bin/env python
# coding: utf-8

# In[1]:


#SIDP Declarations

import pandas as pd
import numpy as np
import re
get_ipython().system('pip install connectorx')
import connectorx as cx
import logging

import datetime
from datetime import date
from dateutil.relativedelta import *

from google.colab import drive
drive.mount('/content/drive')

SID_folderpath = "/content/drive/MyDrive/SID/"

db_connection_str = 'mysql://xxx:xxx%40pp@hana-production-analytics-read-replica.c0wcwq0ocdj3.ap-southeast-1.rds.amazonaws.com/kebhana_dashboard_db'

SIDPstartDate='2021-01-01'
SIDPendDate='2022-12-31'


#SCG DECLARATIONS

import matplotlib.pyplot as plt
import pickle
from datetime import timedelta

SC_folder_path = "/content/drive/My Drive/SCG Output/"
model_folder_path = "/content/drive/My Drive/Model Archive/"


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






#SIDP Functions
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
  import re
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
  import re
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


def ILGL_part1_read_dashboard_SCG(ILGL):
  
#PART 1: SQL QUERIES
#-------------------
  print("Running SQL queries")

  if ILGL == "IL":
    loan_type_str = "('individual','topup individual')"
  elif ILGL == "GL":
    loan_type_str = "('group','topup group')"
  
  FOappdata=cx.read_sql(db_connection_str,                "SELECT     l.id loan_id, i.id interview_id,     ia.question_name, ia.value, ia.option_value_label,       case       when q.type in ('calculation','number','nrc','text','location','age','textarea','component-list-multiple')  then JSON_EXTRACT(ia.value,'$[0]')       when q.type in ('select','radio-group') then JSON_EXTRACT(ia.option_value_label,'$[0]')           when q.type in ('checkbox-group') then ia.option_value_label             else ia.value       end as answers     FROM kebhana_dashboard_db.db_interviews AS i   INNER JOIN kebhana_dashboard_db.db_interview_answers ia on ia.interview_id = i.id   INNER JOIN kebhana_dashboard_db.db_questions  q on ia.question_name = q.name   LEFT JOIN kebhana_middleware_db.m_loan AS l ON l.id=i.loan_id   WHERE i.loan_type IN " + loan_type_str + "   AND l.loan_status_id NOT IN (100,200,500)  AND i.interview_status NOT IN ('reject')   AND i.created_at BETWEEN '"+SIDPstartDate+"' AND '"+SIDPendDate+"';")

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
 
  #convert JSON to useable format
  for column in FOappdata.columns:
      FOappdata[column]=FOappdata[column].astype(str).str.replace('[', "").str.replace(']', "").str.replace('"', "").replace("",np.nan)
      FOappdata[column] = FOappdata[column].replace("nan",np.nan).replace("<NA>",np.nan)
      FOappdata[column] = FOappdata[column].fillna(np.nan)
      FOappdata[column]=try_float(FOappdata[column])

  print("FO app JSON data converted to useable format")

  return FOappdata

def IL_part1_reorderfields(FOappdata):

  print("Reordering fields")

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

def ILGL_part1_readothertables(FOappdata):

  #Loan performance input & output addition
  print("Merging loan perf data")
  Output = ['late_days_in_183d',
  'late_days_in_365d',
  'late_days_in_730d',
  'late_days_in_90d',
  'paid_pc_183d',
  'paid_pc_365d',
  'paid_pc_730d',
  'paid_pc_90d',
  'reschedule_count_in_183d',
  'reschedule_count_in_365d',
  'reschedule_count_in_730d',
  'reschedule_count_in_90d']

  client_id_list = FOappdata['client_id'].tolist()
  client_id_list = tuple(client_id_list)
  str_client_id = ",".join([str(id) for id in client_id_list])

  longprocessingdf = cx.read_sql(db_connection_str,"SELECT * FROM kebhana_middleware_db.hwa_late_day_fields ld WHERE ld.client_id IN (" +str_client_id+");")

  datecols = longprocessingdf.filter(like="date").columns.tolist()
  numericalcols = list(set(longprocessingdf.columns) - set(datecols))

  longprocessingdf[numericalcols] = longprocessingdf[numericalcols].apply(pd.to_numeric)
  longprocessingdf["reference_date"] = pd.to_datetime(longprocessingdf["reference_date"])

  FOappdata["interview_date"] = pd.to_datetime(FOappdata["interview_date"]).dt.normalize()

  FOappdata[["totaldayslate_previousloans","totaldayslate_lastloan","totalpaidpc_previousloans","totalpaidpc_lastloan",                        "totalreschedules_previousloans","reschedule_count_lastloan"]] = FOappdata.apply(generate_loanperf_inputs,longprocessingdf = longprocessingdf,axis=1)

  FOappdata = FOappdata.merge(longprocessingdf[Output+["loan_id"]], how='left',left_on='loan_id',right_on='loan_id')
  FOappdata[Output] = FOappdata[Output].apply(pd.to_numeric)

  #MCIX addition
  print("Merging MCIX")

  MCIX_data=cx.read_sql(db_connection_str,"SELECT id, client_id, mcix_file_date, thitsa_id, delinquent, overlap, writeoff, mfi_name FROM kebhana_middleware_db.hwa_mcix;")

  #MCIX_data = pd.read_csv("/content/drive/My Drive/SID/MappingTables/MCIX_Production.csv")
  #MCIX_data.rename(columns = {'Delinquent':'delinquent', 'Overlap':'overlap', 'WriteOff': 'writeoff',"MCIX_filedate":"mcix_file_date" }, inplace = True)
  MCIX_data["mcix_file_date"] = pd.to_datetime(MCIX_data["mcix_file_date"])

  FOappdata["nearest_MCIX_date"] = FOappdata.apply(get_closest_MCIX_date, MCIX_data=MCIX_data, axis = 1)

  FOappdata = FOappdata.merge(MCIX_data[["client_id","mcix_file_date","delinquent","overlap","writeoff"]],                  left_on=["client_id","nearest_MCIX_date"],right_on=["client_id","mcix_file_date"],how="left")

  FOappdata.rename(columns = {'delinquent':'MCIX_Delinquent_Count', 'overlap':'MCIX_Overlap_Count', 'writeoff': 'MCIX_WriteOff_Count' }, inplace = True)
  FOappdata[["MCIX_Delinquent_Count","MCIX_Overlap_Count","MCIX_WriteOff_Count"]] = FOappdata[["MCIX_Delinquent_Count","MCIX_Overlap_Count","MCIX_WriteOff_Count"]].apply(pd.to_numeric)



  #BranchRepayPAR addition
  print("Merging branch PAR and repayment rate")

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

  FOappdata_processed.loc[FOappdata_processed["app_client_app_install"].str.contains("Yes",na=False),"app_client_app_install"]=1
  FOappdata_processed.loc[FOappdata_processed["app_client_app_install"].str.contains("No",na=False),"app_client_app_install"]=0

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

  FOappdata_processed.loc[FOappdata_processed["app_client_app_install"].str.contains("Yes",na=False),"app_client_app_install"]=1
  FOappdata_processed.loc[FOappdata_processed["app_client_app_install"].str.contains("No",na=False),"app_client_app_install"]=0

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

  #highskew_binary_drop_candidate = []
  #rejection_threshold = 0.1

  for column_name in alldf.columns:
      if (alldf[column_name].dtype==object) & (alldf[column_name].isin(["yes",1,0]).astype(int).sum()>0):
          alldf.loc[alldf[column_name] == "yes", column_name] = 1
          alldf.loc[alldf[column_name] == "no", column_name] = 0
          alldf.loc[alldf[column_name] == "don't know", column_name] = np.nan
          alldf[column_name] = alldf[column_name].astype(float)
      
      if (alldf[column_name].dtype==object) & (column_name != "interview_date"):
        alldf[column_name] = alldf[column_name].fillna("").apply(removebrackets).replace("",np.nan)
      
      #if (alldf[column_name].nunique() <= 2) and ((alldf[column_name].dtype == "float32") or (alldf[column_name].dtype == "float64") or (alldf[column_name].dtype == "int32") or (alldf[column_name].dtype == "int64")):
       #   if (alldf[column_name].mean() <= rejection_threshold) or (alldf[column_name].mean() >= (1-rejection_threshold)):
        #    highskew_binary_drop_candidate.append(column_name)
      
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


  #alldf.drop(columns = highskew_binary_drop,inplace=True,errors='ignore')

  return alldf



#SCG functions

def run_scorecard(regrouped_data,output,num_estimators,binary_list,numerical_list,categorical_list):

  #shorten lists based on what actually exists in current data
  binary_list = list(set.intersection(set(binary_list),set(regrouped_data.columns)))
  numerical_list = list(set.intersection(set(numerical_list),set(regrouped_data.columns)))
  categorical_list = list(set.intersection(set(categorical_list),set(regrouped_data.columns)))


  #One hot encoding

  dummy_df = pd.DataFrame() #we will use this to hold the final dummy data
  current_dummy_df = pd.DataFrame() #we will use this to hold the dummy data currently being processed
  dummylist = categorical_list + binary_list

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

  #separate train and test
  from sklearn.model_selection import train_test_split
  x_train, x_test, y_train, y_test = train_test_split(dummy_df,output,test_size = 0.4,random_state=123)

  ## Set feature interaction constraints such that one tree only contains one feature / variable
  x_constraints = []
  interaction_constraints = []

  dat_cols = pd.Index(x_train.columns)
  combined_list = numerical_list+categorical_list+binary_list
  combined_list = [sub.replace("]", ")") for sub in combined_list]
  combined_list = [sub.replace("<", "less_than") for sub in combined_list]
  ind_vars = sorted(combined_list)

  for iv in ind_vars:
      if np.any(dat_cols.isin([iv])):
          f_var = [dat_cols.get_loc(iv)]
          f_var_name = [dat_cols[f_var[0]]]
      else:
          f_var = []
          f_var_name = []
      
      x_constraints.append(f_var_name + list(dat_cols[np.where(dat_cols.str.startswith(iv+'__'))[0]]) )
      interaction_constraints.append(f_var + list(np.where(dat_cols.str.startswith(iv+'__'))[0]) )

  
  #simplified XGBoost hypertuning GridSearch with shallow trees (max_depth = 3)
  params = {
      'base_score': [output.mean()],
      'gamma': [1]               ## def: 0
      , 'learning_rate': [0.5,0.6,0.7,0.8,0.9,1,1.1,1.2,1.3]     ## def: 0.1
      , 'max_depth': [3]
      , 'min_child_weight': [25]  ## def: 1
      , 'n_estimators': [num_estimators]
      , 'objective': ['binary:logistic']
      , 'reg_alpha': [0]
      , 'reg_lambda': [1]          ## def: 1
      , 'verbosity': [0]
      , 'interaction_constraints': [interaction_constraints]
      , 'eval_metric': ['auc']
      , 'tree_method': ['hist']    ## def: 'auto'
  }


  print('now optimising model')
  from xgboost import XGBClassifier
  from sklearn.model_selection import GridSearchCV
  grid_search= GridSearchCV(XGBClassifier(),param_grid=params,cv=5,scoring='roc_auc',n_jobs=-1,verbose=0)
  grid_search.fit(x_train,y_train, eval_set=[(x_test, y_test)], early_stopping_rounds=100)

  print(grid_search.best_params_)
  print(grid_search.best_score_)

  print('now creating final model with optimised parameters')
  bst_params = grid_search.best_params_

  """
  #full XGBoost with Bayesian search hypertuning
  !pip install scikit-optimize

  from skopt.space import Real, Categorical, Integer

  search_space = {
      "learning_rate": Real(0.001, 1.0, "log-uniform"),
      "min_child_weight": Integer(0, 10, "uniform"),
      "max_depth": Integer(5, 15, "uniform"),
      "subsample": Real(0.01, 1.0, "uniform"),
      "colsample_bytree": Real(0.01, 1.0, "log-uniform"),
      "colsample_bylevel": Real(0.01, 1.0, "log-uniform"),
      "reg_lambda": Real(1e-9, 1000, "log-uniform"),
      "reg_alpha": Real(1e-9, 1.0, "log-uniform"),
      "gamma": Real(1e-9, 0.5, "log-uniform"),
      "scale_pos_weight": Real(1e-6, 500, "log-uniform"),
    "n_estimators" : Categorical([3000]),
    "eval_metric" : Categorical(['auc']),
      "base_score" : Categorical([output.mean()]),
      "tree_method" :Categorical(['gpu_hist']), #need GPU
      "objective" : Categorical(['binary:logistic']), 
      "verbosity" : Categorical([1])
  }

  print('now optimising model')

  from xgboost import XGBClassifier

  from skopt import BayesSearchCV 
  bayes=BayesSearchCV(XGBClassifier(),
                      search_space,n_iter=100,scoring='roc_auc',cv=5,random_state=42,n_jobs=-1,verbose=10)


  from skopt.utils import point_asdict
  def cb(v):
      last_point = v.x_iters[-1]
      p = point_asdict(search_space, last_point)
      print(p)


  bayes.fit(x_train,y_train,eval_set=[(x_test, y_test)], early_stopping_rounds=100, callback=cb)

  print(bayes.best_params_)
  print(bayes.best_score_)
  bst_params = bayes.best_params_
"""
  


  from xgboost import XGBClassifier
  xgbmodel = XGBClassifier(**bst_params, use_label_encoder=False)
  xgbmodel.fit(x_train, y_train, eval_set=[(x_test, y_test)], early_stopping_rounds=100)
  bstr = xgbmodel.get_booster()

  print((bstr.best_score, bstr.best_iteration, bstr.best_ntree_limit))


  ## Create Scorecard
  mdf = bstr.trees_to_dataframe()
  mdf = mdf.loc[mdf['Tree'] <= bstr.best_iteration]
  mdf_parents = mdf[mdf.Feature!='Leaf'].drop(columns=['Tree','Gain','Cover'])
  mdf_leafs = mdf[mdf.Feature=='Leaf'].drop(columns=['Feature','Split','Yes','No','Missing','Cover'])
  mdf_leafs.rename(columns={'ID': 'ID0', 'Node': 'Node0'}, inplace=True)

  tree_traceback = pd.DataFrame()
  itr = 0 
  itrs = str(itr)
  while mdf_leafs.shape[0] > 0: #while rows of mdf_leafs > 0
      NoSprout = pd.merge(mdf_leafs, mdf_parents, how='inner', left_on='ID'+itrs, right_on='No')
      YesSprout = pd.merge(mdf_leafs, mdf_parents, how='inner', left_on='ID'+itrs, right_on='Yes')
      MissingSprout = pd.merge(mdf_leafs, mdf_parents, how='inner', left_on='ID'+itrs, right_on='Missing')
      MissingSprout.Split = np.nan
      
      itr += 1
      itrs = str(itr)    
      NoSprout.insert(NoSprout.shape[1]-4, 'Sign'+itrs, '>=')
      YesSprout.insert(YesSprout.shape[1]-4, 'Sign'+itrs, '<')
      MissingSprout.insert(MissingSprout.shape[1]-4, 'Sign'+itrs, '.')
      mdf_leafs = pd.concat([NoSprout, YesSprout, MissingSprout]) #row-wise concat
      mdf_leafs.rename(columns={'ID':'ID'+itrs, 'Split':'Split'+itrs, 'Feature':'Feature'+itrs, 'Node':'Node'+itrs, 
                              'Yes':'Yes'+itrs, 'No':'No'+itrs, 'Missing':'Missing'+itrs}, inplace=True)
      
      tree_traceback = tree_traceback.append(mdf_leafs.loc[mdf_leafs['Node'+itrs]==0,:], sort=False)
      mdf_leafs = mdf_leafs[mdf_leafs['Node'+itrs]!=0]

  ttb_missing = tree_traceback.copy()
  ttb_non_missing = tree_traceback.copy()
  for i in range(1,itr+1): 
      ttb_missing = ttb_missing[(ttb_missing['Sign'+str(i)] == '.') | ttb_missing['Sign'+str(i)].isna()]
      ttb_non_missing = ttb_non_missing[ttb_non_missing['Sign'+str(i)] != '.']

  ttb = ttb_non_missing.copy()
  ttb.sort_values(['Tree', 'Split1', 'Sign1'], inplace=True, na_position='first')
  ttb.reset_index(drop=True, inplace=True)

  sc_df = ttb.iloc[:,:4].rename(columns={'ID0':'ID', 'Node0':'Node', 'Gain':'XAddEvidence'}).copy()
  sc_df['Feature'] = ttb.Feature1.values
  sc_df['Sign'] = ttb.Sign1.values
  sc_df['Split'] = ttb.Split1.values

  for i in range(1,itr): 
      replace_in_sc = ( ( sc_df['Sign']=='>=').values 
                          & (ttb['Split'+str(i)] < ttb['Split'+str(i+1)]).values 
                          & (ttb['Feature'+str(i)] == ttb['Feature'+str(i+1)]).values ) #generate boolean array meeting this condition
      sc_df.loc[replace_in_sc,'Sign'] = ttb['Sign'+str(i+1)][replace_in_sc].values
      sc_df.loc[replace_in_sc,'Split'] = ttb['Split'+str(i+1)][replace_in_sc].values

  sc_df['Inc_Missing'] = sc_df.ID.isin(ttb_missing.ID0).astype(int)    

  ## Move 'XAddEvidence' to the far right of sc_df
  cols = sc_df.columns.to_list()
  cols.pop(cols.index('XAddEvidence')) 
  sc_df = sc_df[cols+['XAddEvidence']] 

  ## Reformat categorical variables
  OTHER_CAT_IND = "OTHER"  ## The label for the all other items in categorical variable
  feature_decomp = sc_df.Feature.str.split('__', n=1, expand=True)
  cat_rows = ~feature_decomp[1].isna() #tilde is invert - to identify only categorical rows
  other_cat_rows = (cat_rows & (sc_df['Sign'] == '<')).values #< operator on a feature category means "OTHER"
  feat_categories = feature_decomp.iloc[:,1].copy()
  feat_categories.loc[other_cat_rows] = OTHER_CAT_IND
  sc_df.loc[cat_rows, 'Split'] = feat_categories[cat_rows].values #amend only specific (categorical) rows and cols
  sc_df.loc[cat_rows, 'Feature'] = feature_decomp[0][cat_rows].values
  sc_df.loc[cat_rows, 'Sign'] = "="
  sc_df.loc[cat_rows, 'Inc_Missing'] = 0

  sc_df.sort_values(['Tree', 'Sign', 'Split'], inplace=True, na_position='last')
  sc_df.set_index(['Tree'], inplace=True)

  scorecard = sc_df.drop(columns=['Node', 'ID'])

  return scorecard, xgbmodel, x_test, y_test


#------------------------------

def output_scorecard_csv(scorecard,output,fullpathfilename_string,display_write = 0):
    import numpy as np
    base_score_p = output.mean()
    base_score_log = np.log(base_score_p/(1-base_score_p))

    if display_write == 0:
      pd.set_option('display.max_rows', scorecard.shape[0]+1)
      print("base_score = "+ str(base_score_p))
      display(scorecard)
    
    else:
      #f = open(fullpathfilename_string, 'x')
      scorecard.to_csv(fullpathfilename_string,encoding='utf-8-sig')
      import csv
      with open(fullpathfilename_string, 'a') as f:
        writer = csv.writer(f)
        writer.writerow(["base_score (log)",str(base_score_log)])
      print("scorecard saved in " + fullpathfilename_string)

#-------------------------------


def get_scorecard_stats (xgbmodel,x_test, y_test,output):

  y_pred3 = xgbmodel.predict_proba(x_test)[:,1]

  #HISTOGRAM


  import matplotlib.pyplot as plt

  # An "interface" to matplotlib.axes.Axes.hist() method
  n, bins, patches = plt.hist(x=y_pred3, bins='auto', color='#0504aa',
                              alpha=0.7, rwidth=0.85)
  plt.grid(axis='y', alpha=0.75)
  plt.xlabel('Pd')
  plt.ylabel('Frequency')
  plt.title('Test dataset Pd distribution')
  plt.text(23, 45, r'$\mu=15, b=3$')
  maxfreq = n.max()
  # Set a clean upper y-axis limit.
  plt.ylim(ymax=np.ceil(maxfreq / 10) * 10 if maxfreq % 10 else maxfreq + 10)
  print("base score (distribution avg) = " + str(output.mean()))




  #confusion matrix
  from sklearn import metrics
  fpr,tpr,thresholds = metrics.roc_curve(y_test,y_pred3)
  auc3 = metrics.roc_auc_score(y_test,y_pred3)
  print("AUC = " + str(auc3))


  optimal_idx = np.argmax(tpr - fpr)
  optimal_threshold = thresholds[optimal_idx]
  threshold = optimal_threshold
  y_pred = (y_pred3 > threshold).astype('float')
  cm = metrics.confusion_matrix(y_test, y_pred)
  cm = cm/cm.sum()


  disp = metrics.ConfusionMatrixDisplay(confusion_matrix=cm)

  disp.plot()
  plt.show()
  print("Optimum cutoff: Pd = 1 if P >=" + str(optimal_threshold))
  print("accuracy (at optimum cutoff) = " + str(cm[1,1]+cm[0,0]))

#--------------------------------------------------------

def prepare_flexidate_input_output(late_day_threshold, paid_pc_threshold, reschedule_count_threshold, startdate, enddate,
                                   all_input_data,all_output_data, latedays_output_column, paidpc_output_column, reschedule_output_column):
  
  all_input_data['interview_date'] = pd.to_datetime(all_input_data['interview_date'])
  all_output_data['interview_date'] = pd.to_datetime(all_output_data['interview_date'])

  working_input = all_input_data[(all_input_data["interview_date"]>=startdate) & (all_input_data["interview_date"]<=enddate) ] 
  #working_input = working_input.drop(columns="interview_date")
  print("Input data shape")
  print(working_input.shape)


  working_output = all_output_data[[latedays_output_column, paidpc_output_column, reschedule_output_column,"interview_date"]].copy()
  working_output = working_output[(working_output["interview_date"]>=startdate)&(working_output["interview_date"]<=enddate)]
  
  working_output["OR3_output"] = 0
  working_output.loc[(working_output[latedays_output_column]>=late_day_threshold) | 
                     (working_output[paidpc_output_column]<=paid_pc_threshold)|
                     (working_output[reschedule_output_column]>=reschedule_count_threshold),"OR3_output"] = 1

  working_output.drop(columns= [latedays_output_column, paidpc_output_column, reschedule_output_column,"interview_date"],inplace=True)
  working_output=working_output.values.flatten()


  print("Output data shape")
  print(working_output.shape)
  print("Output base score = " + str(working_output.mean()))
  
  
  return working_input,working_output


#-------------------------------------------

def load_csv_and_initialize(data):
  """
  from google.colab import drive
  drive.mount('/content/drive')

  data = pd.read_csv(csvGDrivepath)
  """


  print("Imported data shape")
  print(data.shape)


  #SETUP INPUT & OUTPUT DATA
  output = ["late_days_in_183d","late_days_in_365d", "late_days_in_730d","late_days_in_90d",
            "reschedule_count_in_90d", "reschedule_count_in_183d", "reschedule_count_in_365d","reschedule_count_in_730d",
            "paid_pc_90d", "paid_pc_183d", "paid_pc_365d", "paid_pc_730d"]
  not_used = ["office_name", "FO_ID","FO_username_FF","FO_username_DB"]

  #remove bullet
  data = data[data["ls_loan_product"]!="FullBullet"]
  data = data[data["ls_loan_product"]!="HanaStaffLoan"]

  #remove loans with no interview date
  data = data[~data["interview_date"].isna()]

  all_output_data = pd.concat([data[output],data["interview_date"]],axis=1) #to allow interview date based row slicing on output
  all_input_data = data.drop(columns=output+not_used)
  all_input_data = all_input_data.fillna(np.nan)


  print("All output data shape")
  print(all_output_data.shape)


  print("All input data shape")
  print(all_input_data.shape)

  return all_input_data, all_output_data

#---------------------------------------------------------------------------------
def generate_validation_xtest(x_test,input,xgbmodel,fullpathfilename_string):

  import xgboost
  import math

  print("Creating validation data")

  y_pred_new = xgbmodel.predict_proba(x_test)[:,1]

  x_test = x_test.merge(input[["loan_id","interview_id","client_id","interview_date"]],left_index = True, right_index=True, how="left") 

  #generate features & Pd
  bstr = xgbmodel.get_booster()
  mdf = bstr.trees_to_dataframe()
  mdf = mdf.loc[mdf['Tree'] <= bstr.best_iteration]
  featurelist = mdf["Feature"].str.split("__",expand=True)[0].unique().tolist()
  featurelist.remove("Leaf")
  validation_col_list = ["loan_id","interview_id","client_id","interview_date"] + featurelist
  validation_output = x_test[["loan_id"]].merge(input[validation_col_list],on="loan_id",how="left")
  validation_output = pd.concat([validation_output,pd.DataFrame(y_pred_new,columns=["Pd"])],axis=1)

  """
  #generate Xaddscores
  treenodelist = bstr.predict(xgboost.DMatrix(x_test.drop(columns=["loan_id","interview_id","interview_date","client_id"])),pred_leaf=True)
  resultdf = pd.DataFrame(0, index = range(0,len(treenodelist)), columns = ["loan_id", "base_score"] + list(range(0,bstr.best_iteration+1)))
  resultdf["loan_id"] = x_test["loan_id"].tolist()
  resultdf["base_score"] = math.log(xgbmodel.get_xgb_params()["base_score"]/(1-xgbmodel.get_xgb_params()["base_score"]))

  for tree in range(0,bstr.best_iteration+1):
    treelist=[]
    for loan in range(0,len(treenodelist)):
      treelist.append(mdf["Gain"][(mdf["Tree"]==tree) & (mdf["Node"] == treenodelist[loan][tree])].values[0])

    resultdf[tree] = treelist
  resultdf["Implied Pd"] =resultdf[["base_score"]+list(range(0,bstr.best_iteration+1))].sum(axis=1).apply(lambda x: math.exp((x))/(1+math.exp((x))))
  resultdf["Check Pd Difference"] = (abs(resultdf["Implied Pd"] - validation_output["Pd"])).round(decimals = 5)
  validation_output = validation_output.merge(resultdf, how="left", on="loan_id")
  """

  validation_output.to_csv(fullpathfilename_string,encoding='utf-8-sig',index=False  )
  
  print("Validation files output to " + fullpathfilename_string)
#-------------------------------------------------------------------------------------
def check_output_distribution_bydate(all_output_data,indexstr):
  
  indexnum = int(indexstr[-1])
  working_output = all_output_data[[latedays_output_column[indexnum], paidpc_output_column[indexnum], reschedule_output_column[indexnum],"interview_date"]]

  working_output["OR3_output"] = 0
  working_output.loc[(working_output[latedays_output_column[indexnum]]>=lateday_limits[indexnum]) | 
                     (working_output[paidpc_output_column[indexnum]]<=paidpc_limits[indexnum])|
                     (working_output[reschedule_output_column[indexnum]]>=reschedulecount_limits[indexnum]),"OR3_output"] = 1

  print("OR3 outputs for " + latedays_output_column[indexnum][-4:] + " window") 
  display(working_output.groupby(working_output["interview_date"].dt.to_period("M"))                                 .agg(count=('OR3_output', 'size'), mean=('OR3_output', 'mean'))                                 .reset_index())

          


# In[9]:


#IL

#SIDP CODE BODY

IL_FOappdata = ILGL_part1_read_dashboard_SCG("IL") #identical copy in SCI code
IL_FOappdata = ILGL_part1_readothertables(IL_FOappdata) #identical to SCI code
IL_FOappdata = IL_part1_reorderfields(IL_FOappdata) #identical to SCI code
IL_FOappdata = IL_part2_byprocessingtype(IL_FOappdata) #identical to SCI code
IL_alldf = ILGL_part3_finalprocessing(IL_FOappdata) #identical to SCI code
#IL_alldf.to_csv(SID_folderpath + "IL_Data_Specification_A_data_"+date.today().strftime("%Y%m%d")+".csv",index=False,encoding='utf-8-sig')


#SCG CODE BODY


#output_columns for each scorecard
# latedays_output_column = ["late_days_in_90d","late_days_in_183d"]
# paidpc_output_column = ["paid_pc_90d","paid_pc_183d"]
# reschedule_output_column = ["reschedule_count_in_90d","reschedule_count_in_183d"]

#thresholds for each scorecard
# lateday_limits = [10,10]
# paidpc_limits = [0.99,0.99]
# reschedulecount_limits = [1,1]

latedays_output_column = ["late_days_in_183d"]
paidpc_output_column = ["paid_pc_183d"]
reschedule_output_column = ["reschedule_count_in_183d"]

lateday_limits = [10]
paidpc_limits = [0.99]
reschedulecount_limits = [1]

IL_all_input_data, IL_all_output_data = load_csv_and_initialize(IL_alldf)
check_output_distribution_bydate(IL_all_output_data,"SC0")

number_of_trees = 500

#declare SCG date threshold - please date-slice correctly depending on check_output_distribution_bydate results
# SCGstartdate = [datetime.datetime(year=2021, month=7, day=1),datetime.datetime(year=2021, month=7, day=1)]
# SCGenddate = [datetime.datetime(year=2022, month=1, day=31),datetime.datetime(year=2022, month=1, day=31)]

SCGstartdate = [datetime.datetime(year=2021, month=7, day=1)]
SCGenddate = [datetime.datetime(year=2022, month=1, day=31)]

#Generate all scorecards by iterating through each scorecard & creating dynamic variables
for i in range(0,len(latedays_output_column)):
  print("")
  print(f"Creating IL_SC{i}......")
  print(latedays_output_column[i][-4:]+" window")
  print("_________________________")

  globals()[f"IL_input_SC{i}"], globals()[f"IL_output_SC{i}"] = prepare_flexidate_input_output(
      lateday_limits[i], paidpc_limits[i], reschedulecount_limits[i],  SCGstartdate[i], SCGenddate[i], IL_all_input_data,IL_all_output_data,
      latedays_output_column[i],paidpc_output_column[i],reschedule_output_column[i])

  globals()[f"IL_scorecard_SC{i}"], globals()[f"IL_xgbmodel_SC{i}"], globals()[f"IL_x_test_SC{i}"], globals()[f"IL_y_test_SC{i}"] = run_scorecard(
      globals()[f"IL_input_SC{i}"].drop(columns=["loan_id","interview_id","interview_date","client_id"]),globals()[f"IL_output_SC{i}"], number_of_trees,IL_binary_list,IL_numerical_list,IL_categorical_list)

  output_scorecard_csv(globals()[f"IL_scorecard_SC{i}"],globals()[f"IL_output_SC{i}"],SC_folder_path+"IL_SC"+str(i)+"_"+date.today().strftime("%Y%m%d")+".csv",0) #last argument 0 = display, 1 = writecsv

  output_scorecard_csv(globals()[f"IL_scorecard_SC{i}"],globals()[f"IL_output_SC{i}"],SC_folder_path+"IL_SC"+str(i)+"_"+date.today().strftime("%Y%m%d")+".csv",1)

  get_scorecard_stats(globals()[f"IL_xgbmodel_SC{i}"],globals()[f"IL_x_test_SC{i}"], globals()[f"IL_y_test_SC{i}"], globals()[f"IL_output_SC{i}"])

  pickle.dump(globals()[f"IL_xgbmodel_SC{i}"], open(model_folder_path+"IL_xgbmodel_SC"+str(i)+"_"+date.today().strftime("%Y%m%d")+".model", "wb"))
  
  generate_validation_xtest(globals()[f"IL_x_test_SC{i}"],globals()[f"IL_input_SC{i}"],globals()[f"IL_xgbmodel_SC{i}"],SC_folder_path + f"IL_SC{i}_"+date.today().strftime("%Y%m%d")+"_validation.csv")  


# In[11]:


#GL

#SIDP CODE BODY
GL_FOappdata = ILGL_part1_read_dashboard_SCG("GL")
GL_FOappdata = ILGL_part1_readothertables(GL_FOappdata)
GL_FOappdata = GL_part1_reorderfields(GL_FOappdata)
GL_FOappdata = GL_part2_byprocessingtype(GL_FOappdata)
GL_alldf = ILGL_part3_finalprocessing(GL_FOappdata)
#GL_alldf.to_csv(SID_folderpath + "GL_Data_Specification_B_data_"+date.today().strftime("%Y%m%d")+".csv",index=False,encoding='utf-8-sig')

#SCG CODE BODY

# #output_columns for each scorecard
# latedays_output_column = ["late_days_in_90d","late_days_in_183d"]
# paidpc_output_column = ["paid_pc_90d","paid_pc_183d"]
# reschedule_output_column = ["reschedule_count_in_90d","reschedule_count_in_183d"]

# #thresholds for each scorecard
# lateday_limits = [10,10]
# paidpc_limits = [0.99,0.99]
# reschedulecount_limits = [1,1]

latedays_output_column = ["late_days_in_183d"]
paidpc_output_column = ["paid_pc_183d"]
reschedule_output_column = ["reschedule_count_in_183d"]

lateday_limits = [10]
paidpc_limits = [0.99]
reschedulecount_limits = [1]

GL_all_input_data, GL_all_output_data = load_csv_and_initialize(GL_alldf)
check_output_distribution_bydate(GL_all_output_data,"SC0")

number_of_trees = 500

#declare SCG date threshold - please date-slice correctly depending on check_output_distribution_bydate results
# SCGstartdate = [datetime.datetime(year=2021, month=7, day=1),datetime.datetime(year=2021, month=7, day=1)]
# SCGenddate = [datetime.datetime(year=2022, month=1, day=31),datetime.datetime(year=2022, month=1, day=31)]

SCGstartdate = [datetime.datetime(year=2021, month=7, day=1)]
SCGenddate = [datetime.datetime(year=2022, month=1, day=31)]

#Generate all scorecards by iterating through each scorecard & creating dynamic variables
for i in range(0,len(latedays_output_column)):
  print("")
  print(f"Creating GL_SC{i}......")
  print(latedays_output_column[i][-4:]+" window")
  print("_________________________")

  globals()[f"GL_input_SC{i}"], globals()[f"GL_output_SC{i}"] = prepare_flexidate_input_output(
      lateday_limits[i], paidpc_limits[i], reschedulecount_limits[i],  SCGstartdate[i], SCGenddate[i], GL_all_input_data,GL_all_output_data,
      latedays_output_column[i],paidpc_output_column[i],reschedule_output_column[i])

  globals()[f"GL_scorecard_SC{i}"], globals()[f"GL_xgbmodel_SC{i}"], globals()[f"GL_x_test_SC{i}"], globals()[f"GL_y_test_SC{i}"] = run_scorecard(
      globals()[f"GL_input_SC{i}"].drop(columns=["loan_id","interview_id","interview_date","client_id"]),globals()[f"GL_output_SC{i}"], number_of_trees,GL_binary_list,GL_numerical_list,GL_categorical_list)

  output_scorecard_csv(globals()[f"GL_scorecard_SC{i}"],globals()[f"GL_output_SC{i}"],SC_folder_path+"GL_SC"+str(i)+"_"+date.today().strftime("%Y%m%d")+".csv",0) #last argument 0 = display, 1 = writecsv

  output_scorecard_csv(globals()[f"GL_scorecard_SC{i}"],globals()[f"GL_output_SC{i}"],SC_folder_path+"GL_SC"+str(i)+"_"+date.today().strftime("%Y%m%d")+".csv",1)

  get_scorecard_stats(globals()[f"GL_xgbmodel_SC{i}"],globals()[f"GL_x_test_SC{i}"], globals()[f"GL_y_test_SC{i}"], globals()[f"GL_output_SC{i}"])

  pickle.dump(globals()[f"GL_xgbmodel_SC{i}"], open(model_folder_path+"GL_xgbmodel_SC"+str(i)+"_"+date.today().strftime("%Y%m%d")+".model", "wb"))
  
  generate_validation_xtest(globals()[f"GL_x_test_SC{i}"],globals()[f"GL_input_SC{i}"],globals()[f"GL_xgbmodel_SC{i}"],SC_folder_path + f"GL_SC{i}_"+date.today().strftime("%Y%m%d")+"_validation.csv")  


# In[12]:


check_output_distribution_bydate(GL_all_output_data,"SC0")

