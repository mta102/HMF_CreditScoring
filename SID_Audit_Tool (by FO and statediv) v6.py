#!/usr/bin/env python
# coding: utf-8

# In[1]:



#----------------------------------------------------------
def generate_prioritised_checklist_csv(within_date1_FOcompare_answer_df,within_date2_FOcompare_answer_df,date1date2_compare_answer_df,folderpath):
        
    allpossible_FOnames = np.unique(within_date1_FOcompare_answer_df.index.values.tolist() + within_date2_FOcompare_answer_df.index.values.tolist()).tolist()
    allpossible_FOnames.sort()  
    
    priorityFOs_df = pd.DataFrame(columns=["interdate_date1date2","interFO_date1", "interFO_date2"]).reindex(allpossible_FOnames)
    priorityFOs_df["interFO_date1"] = within_date1_FOcompare_answer_df.count(axis=1)
    priorityFOs_df["interFO_date2"] = within_date2_FOcompare_answer_df.count(axis=1)
    priorityFOs_df["interdate_date1date2"] = date1date2_compare_answer_df.count(axis=1)
    priorityFOs_df.sort_values(by="interdate_date1date2", inplace=True, ascending=False)
    
    allpossible_variablenames = np.unique(within_date1_FOcompare_answer_df.columns.tolist() + within_date2_FOcompare_answer_df.columns.tolist()).tolist()
    allpossible_variablenames.sort()
    
    
    priorityvariables_df = pd.DataFrame(columns=["interdate_date1date2", "interFO_date1", "interFO_date2"]).reindex(allpossible_variablenames)
    priorityvariables_df["interFO_date1"] = within_date1_FOcompare_answer_df.count(axis=0)
    priorityvariables_df["interFO_date2"] = within_date2_FOcompare_answer_df.count(axis=0)
    priorityvariables_df["interdate_date1date2"] = date1date2_compare_answer_df.count(axis=0)
    priorityvariables_df.sort_values(by="interdate_date1date2", inplace=True, ascending=False)
    
    
    priorityFOs_df.to_csv(folderpath + "priorityFOs_df by FO by statediv " + date.today().strftime("%Y%m%d") + ".csv",encoding='utf-8-sig')

    priorityvariables_df.to_csv(folderpath + "priorityvariables_df by FO by statediv " + date.today().strftime("%Y%m%d") + ".csv",encoding='utf-8-sig')
    
    print("Priority FOs/variables successfully output to " + folderpath)

#----------------------------------------------------
def get_lowloan_FOlist(date1data, date2data, FO_minimum_loans):    

    FO_count_pivot = pd.crosstab(index=date1data["FO_ID"],columns=[0],values=date1data["FO_ID"],aggfunc="count",dropna=False)
    exclude_FO_ID = []
    
    for row_name in FO_count_pivot.index:
        if FO_count_pivot[0][row_name]<FO_minimum_loans:
            exclude_FO_ID.append(row_name)
    
    FO_count_pivot = pd.crosstab(index=date2data["FO_ID"],columns=[0],values=date2data["FO_ID"],aggfunc="count",dropna=False)
    
    for row_name in FO_count_pivot.index:
        if FO_count_pivot[0][row_name]<FO_minimum_loans:
            exclude_FO_ID.append(row_name)
    
    exclude_FO_ID = np.unique(exclude_FO_ID)
    
    return exclude_FO_ID

#-----------------------------------------------------

def remove_low_loan_FOs(within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df,date1date2_compare_answer_df,exclude_FO_list):
    
    #remove empty indexes due to FOs with low loans / unexpected blank FOIDs
    date1date2_compare_answer_df = date1date2_compare_answer_df[date1date2_compare_answer_df.index.notnull()]
    within_date1_FOcompare_answer_df = within_date1_FOcompare_answer_df[within_date1_FOcompare_answer_df.index.notnull()]
    within_date2_FOcompare_answer_df = within_date2_FOcompare_answer_df[within_date2_FOcompare_answer_df.index.notnull()]

    if len(exclude_FO_list)>0:
        for row_name in exclude_FO_list:
          within_date1_FOcompare_answer_df.loc[row_name] = np.nan
          within_date2_FOcompare_answer_df.loc[row_name] = np.nan
          date1date2_compare_answer_df.loc[row_name] = np.nan

    within_date1_FOcompare_answer_df = within_date1_FOcompare_answer_df.drop(columns=["FO_ID"])
    within_date2_FOcompare_answer_df = within_date2_FOcompare_answer_df.drop(columns=["FO_ID"])
    date1date2_compare_answer_df = date1date2_compare_answer_df.drop(columns=["FO_ID"])
    
    return within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df,date1date2_compare_answer_df

#--------------------------------------------------

def outputcsv(within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df,date1date2_compare_answer_df,folderpath):
    

  within_date1_FOcompare_answer_df.to_csv(folderpath+"interFO_date1 by FO by statediv " + date.today().strftime("%Y%m%d") + ".csv",encoding='utf-8-sig')
  within_date2_FOcompare_answer_df.to_csv(folderpath+"interFO_date2 by FO by statediv " + date.today().strftime("%Y%m%d") + ".csv",encoding='utf-8-sig')
  date1date2_compare_answer_df.to_csv(folderpath+"interdate_comparison by FO by statediv " + date.today().strftime("%Y%m%d") + ".csv",encoding='utf-8-sig')
    
  print("All files successfully output to " + folderpath)

#---------------------------------------------------------------------

def classifycolumns(regrouped_data):

  #define lists
  categorical_list =[]
  binary_list =[]
  numerical_list=[]
  error_list=[]

  for column_name in regrouped_data.columns: #loop through every column
    datatype = regrouped_data[column_name].dtype
    unique_count = regrouped_data[column_name].nunique()

    isBinary = (unique_count <= 2) and ((datatype == int) or (datatype == float))
    isCategorical = (datatype == object)
    isNumerical =  (unique_count > 2) and ((datatype == int) or (datatype == float))
    isError = not(isBinary) and not(isCategorical) and not(isNumerical)

    if isBinary:
      binary_list.append(column_name)

    if isCategorical:
      categorical_list.append(column_name)

    if isNumerical:
      numerical_list.append(column_name)

    if isError:
      error_list.append(column_name)

  #checks

  checksumcols = sum([len(categorical_list),len(binary_list),len(numerical_list), len(error_list)]) - len(regrouped_data.columns)
  if checksumcols == 0:
    print ("Column checksum OK")
  else:
    print("There are " + str(checksumcols) + " columns that were not categorised. Please check")

  if len(error_list) > 0:
    print("There are " + str(len(error_list)) + " columns that were not categorised as binary, categ or num. Please check")
  else:
    print ("Error list OK")

  print("After categorisation...")
  print("categorical: " + str(len(categorical_list)))
  print("binary: " + str(len(binary_list)))
  print("numerical: " + str(len(numerical_list)))
  print("error: " + str(len(error_list)))
  print("total:" + str(sum([len(categorical_list),len(binary_list),len(numerical_list), len(error_list)])))

  return categorical_list,  binary_list, numerical_list, error_list


#-----------------------------------------------------------------------
def load_csv_and_initialize(data):

  for column in data.columns:
    data[column]=data[column].astype(str).str.replace('[', "").str.replace(']', "").str.replace('"', "").replace("",np.nan)
    data[column]=try_float(data[column])
    data[column] = data[column].replace("nan",np.nan)
    data[column] = data[column].fillna(np.nan)

    if (data[column].dtype==object):
      data[column] = data[column].fillna("").apply(removebrackets).replace("",np.nan)  
    
  print("Imported data shape")
  data.shape

  #SETUP INPUT & OUTPUT DATA

  other_drop = ["interview_id", "office_name","FO_username_FF","FO_username_DB","client_mobile_no","guarantor_phone",                "office_opening_date", "loan_officer_experience", "loan_counter", "ls_current_loan_cycle",                 "ls_loan_term", "ls_fo_suggested_amount", "ls_loan_amount_requested","lo_joining_date"]
  
  Derive_from_efm_component = ["efm_component", "efm_number"]
  Derive_multiselection = ["as_livestocks_owned", "as_vehicles_owned", "hv_household_assets", "hv_services_available","ls_loan_specific_use"]
  Derive_hasotherloans = ["clch_component", "clch_num_outstanding_loans"]
  Simplify = ["app_client_app_install", "app_not_install_reason"]

  #remove loans with no loanID, disb date, FOID, office_name
  data = data[~data["interview_date"].isna()]
  data = data[~data["office_name"].isna()]
  data = data[~data["loan_id"].isna()]


  #setup date format
  data['interview_date'] = pd.to_datetime(data['interview_date'])
  
  #setup easy to audit FO ID
  data['FO_ID'] =  data["FO_username_DB"] + "|" + data["FO_username_FF"]+   " ("+ data['office_name']+")"

  #Process Derive_from_efm_component
  data["efm_monthly_income"] = data["efm_component"].fillna("").apply(processcomponent_efm_monthly_income)
  data["efm_relation"] = data["efm_component"].fillna("").apply(processcomponent_efm_relation)
  data["efm_age"] = data["efm_component"].fillna("").apply(processcomponent_efm_age)
  data.drop(columns = Derive_from_efm_component, inplace = True)

  #process Derive_multiselection
  Derive_multiselection.append("efm_relation")
  for column_name in Derive_multiselection:
      data[column_name] = data[column_name].fillna("").apply(removebrackets).replace("",np.nan,regex=False)
      data[column_name] = data[column_name].fillna("").str.replace("\\n","|",regex=False).str.replace(", ","|",regex=False).str.replace(",","|",regex=False).str.replace(" ","",regex=False)
      temp_dummy =  data[column_name].str.get_dummies('|').add_prefix(column_name+"_")
      data = pd.concat([data,temp_dummy],axis=1)
  
  data.drop(columns = Derive_multiselection, inplace=True)
    
  #Process Derive_hasotherloans
  data["has_other_loans"] = "No"
  data.loc[(~data["clch_component"].isna())|(~data["clch_num_outstanding_loans"].isna()),"has_other_loans"]="Yes"
  data.drop(columns = Derive_hasotherloans, inplace = True)

  
  #process Simplify
  data.loc[data["app_client_app_install"].str.contains("Yes",na=False),"app_client_app_install"]=1
  data.loc[data["app_client_app_install"].str.contains("No",na=False),"app_client_app_install"]=0

  data.loc[data["app_not_install_reason"].isna(),"app_not_install_reason"]="Blank"
  data.loc[data["app_not_install_reason"].str.contains("အင်တာနက်",na=False),"app_not_install_reason"]="No internet"
  data.loc[(data["app_not_install_reason"].str.contains("Keypad",na=False))                     | (data["app_not_install_reason"].str.contains("ဖုန်း",na=False))                         | (data["app_not_install_reason"].str.contains("iPhone",na=False)),"app_not_install_reason"]="No Android Phone"
  data.loc[(data["app_not_install_reason"]!="No internet") & (data["app_not_install_reason"]!="No Android Phone" )                     & (data["app_not_install_reason"]!="Blank" ),"app_not_install_reason"]="Other"
  data["app_not_install_reason"] = data["app_not_install_reason"].replace("Blank",np.nan)

  #Process Derive_age_from_this_DOB
  data["pd_age"] = pd.to_datetime(data["pd_age"]).apply(age)


  data = data.drop(columns=other_drop)
  data = data.fillna(np.nan)

  print("All input data shape")
  print(data.shape)

  return data

#-------------------------------------------------------------------------
def try_float(x):
    try:
        return x.astype(float)
    except ValueError:
        return x

#---------------------------------------------------------------------
def removebrackets(x):
    try:
        return re.sub(r"\([^()]*\)", "", x).strip()
    except ValueError:
        return x

#---------------------------------------------------------------------

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

#-----------------------------------------------------------------------
def categorical_compare(column_name, date1data, date2data, within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df,date1date2_compare_answer_df):

  #print("Currently processing..." + column_name)
  data1pivot = pd.crosstab(index=date1data["FO_ID"],margins=True,columns=date1data[column_name].fillna("blank"),values=date1data["FO_ID"],aggfunc="count",normalize="index",dropna=False)
  data2pivot = pd.crosstab(index=date2data["FO_ID"],margins=True,columns=date2data[column_name].fillna("blank"),values=date2data["FO_ID"],aggfunc="count",normalize="index",dropna=False)

  #within date 1 category mean difference
  data1_mean_list = data1pivot.loc["All"]
  data1pivot = data1pivot.drop(index="All", axis=0)
  
  data1_mean_diff_bycategory_differences =  abs(data1pivot-data1_mean_list)

  data1_mean_diff_bycategory_mask = pd.DataFrame(columns=[column_name]).reindex(data1pivot.index)
 
  data1_mean_diff_bycategory_mask[column_name] = (data1_mean_diff_bycategory_differences.max(axis=1) > percent_threshold).tolist()

  templist=[]
  
  for ind in data1_mean_diff_bycategory_mask.index.values:
      if data1_mean_diff_bycategory_mask[column_name][ind]==True:
          templist.append(              data1_mean_list.round(2).astype(str)+"-->"+data1pivot.loc[ind].round(2).astype(str)                   + "(chg." + data1_mean_diff_bycategory_differences.loc[ind].round(2).astype(str)                       + " vs." + str(percent_threshold) + ")")
      elif data1_mean_diff_bycategory_mask[column_name][ind]==False:
          templist.append(np.nan)
  

  data1_mean_diff_bycategory_mask[column_name]=templist
  within_date1_FOcompare_answer_df[column_name] = data1_mean_diff_bycategory_mask[column_name]
  
 
  

  #within date 2 category mean difference
  data2_mean_list = data2pivot.loc["All"]
  data2pivot = data2pivot.drop(index="All", axis=0)  
  
  data2_mean_diff_bycategory_differences =  abs(data2pivot-data2_mean_list)
  
  data2_mean_diff_bycategory_mask = pd.DataFrame(columns=[column_name]).reindex(data2pivot.index)
 
  data2_mean_diff_bycategory_mask[column_name] = (data2_mean_diff_bycategory_differences.max(axis=1) > percent_threshold).tolist()

  templist=[]
  
  for ind in data2_mean_diff_bycategory_mask.index.values:
      if data2_mean_diff_bycategory_mask[column_name][ind]==True:
          templist.append(               data2_mean_list.round(2).astype(str)+"-->"+data2pivot.loc[ind].round(2).astype(str)                   + "(chg." + data2_mean_diff_bycategory_differences.loc[ind].round(2).astype(str)                       + " vs." + str(percent_threshold) + ")")
      elif data2_mean_diff_bycategory_mask[column_name][ind]==False:
          templist.append(np.nan)
  

  data2_mean_diff_bycategory_mask[column_name]=templist
  within_date2_FOcompare_answer_df[column_name] = data2_mean_diff_bycategory_mask[column_name]
 
    
  #date1 date2 difference
  overlapping_categories = np.unique(data1pivot.columns.tolist()+data2pivot.columns.tolist())
  overlapping_categories.sort()
  overlapping_rows = list(set(date1data["FO_ID"].unique()).intersection(date2data["FO_ID"].unique()))

  data2pivot_reorder=data2pivot.reindex(columns = overlapping_categories,fill_value=0,index=overlapping_rows)
  data1pivot_reorder=data1pivot.reindex(columns = overlapping_categories,fill_value=0,index=overlapping_rows)

  #date 1 date 2 difference
  max_category_diff_percent_list = abs(data1pivot_reorder.sub(data2pivot_reorder,fill_value=0))
  
  
  date1date2_diff_bycategory_mask = pd.DataFrame(columns=[column_name]).reindex(overlapping_rows)
 
  date1date2_diff_bycategory_mask[column_name] = (max_category_diff_percent_list.max(axis=1) > percent_threshold).tolist()
 
  templist=[]
  

  for ind in date1date2_diff_bycategory_mask.index.values:
      
      if date1date2_diff_bycategory_mask[column_name][ind]==True:
          templist.append( data1pivot_reorder.loc[ind].round(2).astype(str)+"-->"+data2pivot_reorder.loc[ind].round(2).astype(str)               + "(chg." + max_category_diff_percent_list.loc[ind].round(2).astype(str)                   + " vs." + str(percent_threshold) + ")")
      elif date1date2_diff_bycategory_mask[column_name][ind]==False:
          templist.append(np.nan)
  
  date1date2_diff_bycategory_mask[column_name] = templist
  date1date2_compare_answer_df[column_name] = date1date2_diff_bycategory_mask[column_name]

  return within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df,date1date2_compare_answer_df

#----------------------------------------------------------------------------------

def numerical_compare(numerical_list, date1data, date2data, within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df,date1date2_compare_answer_df):
  
  date1data[numerical_list] = date1data[numerical_list].replace(np.inf,np.nan) #remove inf values to eliminate inf mean
  date1data[numerical_list] = np.where(date1data[numerical_list]<0, np.nan,date1data[numerical_list])

  date2data[numerical_list] = date2data[numerical_list].replace(np.inf,np.nan) #remove inf values to eliminate inf mean
  date2data[numerical_list] = np.where(date2data[numerical_list]<0, np.nan,date2data[numerical_list])

  numerical_list.append("FO_ID")

  data1pivot_mean = date1data[numerical_list].groupby('FO_ID').mean()
  data2pivot_mean = date2data[numerical_list].groupby('FO_ID').mean()

  data1pivot_std = date1data[numerical_list].groupby('FO_ID').agg(np.std)
  #data2pivot_std = date2data[numerical_list].groupby('FO_ID').agg(np.std)

  numerical_list.remove("FO_ID")

  #date 1 data - detect sig diff between FOes
  all_FO_mean_date1 = date1data[numerical_list].mean(axis=0)
  all_FO_std_date1 = date1data[numerical_list].std(axis=0)

  date1_interFO_sig_diff = (abs(data1pivot_mean-all_FO_mean_date1)>=all_FO_std_date1*std_dev_multiple_threshold_interFO) 

  for column_name in numerical_list:
    date1_interFO_sig_diff[column_name].loc[date1_interFO_sig_diff[column_name]==True]='{:,.1f}'.format(all_FO_mean_date1[column_name]) +"-->"+ data1pivot_mean[column_name].map('{:,.1f}'.format)+"(chg " +       (abs(data1pivot_mean[column_name]-all_FO_mean_date1[column_name]).to_frame()).applymap('{:,.1f}'.format).squeeze() + ">" +           '{:,.1f}'.format(all_FO_std_date1[column_name]*std_dev_multiple_threshold_interFO) +")"
    date1_interFO_sig_diff[column_name].loc[date1_interFO_sig_diff[column_name]==False] = np.nan
    within_date1_FOcompare_answer_df[column_name] = date1_interFO_sig_diff[column_name]
   

  #date 2 data - detect sig diff between FOes
  all_FO_mean_date2 = date2data[numerical_list].mean(axis=0)
  all_FO_std_date2 = date2data[numerical_list].std(axis=0)

  date2_interFO_sig_diff = (abs(data2pivot_mean-all_FO_mean_date2)>=all_FO_std_date2*std_dev_multiple_threshold_interFO) 

  for column_name in numerical_list:
    date2_interFO_sig_diff[column_name].loc[date2_interFO_sig_diff[column_name]==True]='{:,.1f}'.format(all_FO_mean_date2[column_name]) +"-->"+ data2pivot_mean[column_name].map('{:,.1f}'.format)+"(chg " +       (abs(data2pivot_mean[column_name]-all_FO_mean_date2[column_name]).to_frame()).applymap('{:,.1f}'.format).squeeze() + ">" +           '{:,.1f}'.format(all_FO_std_date2[column_name]*std_dev_multiple_threshold_interFO) +")"
    
    within_date2_FOcompare_answer_df[column_name] = date2_interFO_sig_diff[column_name]
    date2_interFO_sig_diff[column_name].loc[date2_interFO_sig_diff[column_name]==False] = np.nan
    within_date2_FOcompare_answer_df[column_name] = date2_interFO_sig_diff[column_name]

    #date 1 date 2 change
    #rule - flag if mean has shifted by >= some multiple of s.d.

  overlapping_rows = list(set(date1data["FO_ID"].unique()).intersection(date2data["FO_ID"].unique()))
  data1pivot_mean_reorder=data1pivot_mean.reindex(overlapping_rows) 
  data2pivot_mean_reorder=data2pivot_mean.reindex(overlapping_rows) 

  data1pivot_std_reorder=data1pivot_std.reindex(overlapping_rows) 
  #data2pivot_std_reorder=data2pivot_std.reindex(overlapping_rows)

  date1date2_sig_diff = (abs(data1pivot_mean_reorder - data2pivot_mean_reorder)>=data1pivot_std_reorder*std_dev_multiple_threshold_date1date2)
    
  for column_name in numerical_list:
    date1date2_sig_diff[column_name].loc[date1date2_sig_diff[column_name]==True]=data1pivot_mean_reorder[column_name].map('{:,.1f}'.format) +"-->"+ data2pivot_mean_reorder[column_name].map('{:,.1f}'.format)+       "(chg " + (abs(data1pivot_mean_reorder[column_name] - data2pivot_mean_reorder[column_name])).map('{:,.1f}'.format) + ">" +       (data1pivot_std_reorder[column_name]*std_dev_multiple_threshold_date1date2).map('{:,.1f}'.format) +")"
    date1date2_sig_diff[column_name].loc[date1date2_sig_diff[column_name]==False]=np.nan 
    date1date2_compare_answer_df[column_name] = date1date2_sig_diff[column_name]
    
  return within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df,date1date2_compare_answer_df

#---------------------------------------------------------------------
def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3
#----------------------------------------------------------------------

def age(born):
    return (date.today().year - born.year)
#-------------------------------------------------------------------------------------------------------
def run_PHP_IL(startDate,endDate):
  db_connection_str = 'mysql+pymysql://kebwebuser:u5erMeb%40pp@hana-production-analytics-read-replica.c0wcwq0ocdj3.ap-southeast-1.rds.amazonaws.com/kebhana_dashboard_db'
  db_connection = create_engine(db_connection_str)

  print("Running SQL queries")
  il = pd.read_sql("SELECT     l.id loan_id, i.id interview_id,     ia.question_name, ia.value, ia.option_value_label,       case       when q.type in ('calculation','number','nrc','text','location','age','textarea','component-list-multiple') then JSON_EXTRACT(ia.value,'$[0]')       when q.type in ('select','radio-group') then JSON_EXTRACT(ia.option_value_label,'$[0]')           when q.type in ('checkbox-group') then ia.option_value_label             else ia.value       end as answers     FROM kebhana_dashboard_db.db_interviews AS i   INNER JOIN kebhana_dashboard_db.db_interview_answers ia on ia.interview_id = i.id   INNER JOIN kebhana_dashboard_db.db_questions  q on ia.question_name = q.name   LEFT JOIN kebhana_middleware_db.m_loan AS l ON l.id=i.loan_id   WHERE i.loan_type IN ('individual','topup individual')   AND l.loan_status_id NOT IN (100,200,500)   AND i.interview_status NOT IN ('reject')   AND i.created_at BETWEEN '"+startDate+"' AND '"+endDate+"';", con=db_connection)

  field_app_IL = il.pivot_table( index='interview_id',columns='question_name', values='answers',aggfunc='first')

  additional_fields = pd.read_sql_query("SELECT c.id AS client_id,i.id AS interview_id,l.id AS loan_id,c.display_name AS client_name,u.user_name AS FO_username_DB,o1.name AS client_division,cv2.code_value AS client_gender_cv_id,c.mobile_no AS client_mobile_no ,cv1.code_value AS client_type_cv_id,i.created_at AS interview_date,s.id AS FO_ID,s.display_name AS FO_username_FF,l.loan_counter,s.joining_date AS lo_joining_date,o.name AS office_name,o.opening_date AS office_opening_date   FROM kebhana_dashboard_db.db_interviews AS i   LEFT JOIN kebhana_dashboard_db.users AS u ON u.id=i.created_by   LEFT JOIN kebhana_middleware_db.m_loan AS l ON l.id=i.loan_id   LEFT JOIN kebhana_middleware_db.m_client AS c ON c.id=l.client_id   left join kebhana_middleware_db.m_code_value cv1 on cv1.id=c.client_type_cv_id   left join kebhana_middleware_db.m_code_value cv2 on cv2.id=c.gender_cv_id   LEFT JOIN kebhana_middleware_db.m_office o ON o.id=c.office_id   LEFT JOIN kebhana_middleware_db.m_office AS o1 ON o.parent_id=o1.id   LEFT JOIN kebhana_middleware_db.m_staff s ON s.id=c.staff_id   WHERE l.loan_status_id NOT IN (100,200,500)   AND i.interview_status NOT IN ('reject');", con=db_connection);

  additional_fields['office_opening_date'] = pd.to_datetime(additional_fields['office_opening_date'])
  additional_fields['interview_date'] = pd.to_datetime(additional_fields['interview_date'])
  additional_fields['lo_joining_date'] = pd.to_datetime(additional_fields['lo_joining_date'])

  additional_fields['loan_officer_experience']=(additional_fields['interview_date'] -additional_fields['lo_joining_date']).dt.days/365
  additional_fields['office_opening_date'] = (( additional_fields['interview_date'] - additional_fields['office_opening_date'])/np.timedelta64(1, 'M'))

  additional_fields.loc[additional_fields['client_gender_cv_id']=="Male (ကျား)",'client_gender_cv_id']='Male'
  additional_fields.loc[additional_fields['client_gender_cv_id']=="Female",'client_gender_cv_id']='Female'

  additional_fields['office_name'] = additional_fields['office_name'].str.replace('Branch Office - ', '', regex=False)
  FOappdata = field_app_IL.merge(additional_fields, how='left',left_on='interview_id',right_on='interview_id')

  print("SQL loaded")

  return FOappdata

#-------------------------------------------------------------------------------------------------------
def run_PHP_GL(startDate,endDate):
  db_connection_str = 'mysql+pymysql://kebwebuser:u5erMeb%40pp@hana-production-analytics-read-replica.c0wcwq0ocdj3.ap-southeast-1.rds.amazonaws.com/kebhana_dashboard_db'
  db_connection = create_engine(db_connection_str)

  print("Running SQL queries")
  gl = pd.read_sql("SELECT     l.id loan_id, i.id interview_id,     ia.question_name, ia.value, ia.option_value_label,       case       when q.type in ('calculation','number','nrc','text','location','age','textarea','component-list-multiple')  then JSON_EXTRACT(ia.value,'$[0]')       when q.type in ('select','radio-group') then JSON_EXTRACT(ia.option_value_label,'$[0]')           when q.type in ('checkbox-group') then ia.option_value_label             else ia.value       end as answers     FROM kebhana_dashboard_db.db_interviews AS i   INNER JOIN kebhana_dashboard_db.db_interview_answers ia on ia.interview_id = i.id   INNER JOIN kebhana_dashboard_db.db_questions  q on ia.question_name = q.name   LEFT JOIN kebhana_middleware_db.m_loan AS l ON l.id=i.loan_id   WHERE i.loan_type IN ('group','topup group')   AND l.loan_status_id NOT IN (100,200,500)   AND i.interview_status NOT IN ('reject')   AND i.created_at BETWEEN '"+startDate+"' AND '"+endDate+"';", con=db_connection)

  field_app_GL = gl.pivot_table( index='interview_id',columns='question_name', values='answers',aggfunc='first')

  additional_fields = pd.read_sql_query("SELECT c.id AS client_id,i.id AS interview_id,l.id AS loan_id,c.display_name AS client_name,u.user_name AS FO_username_DB,o1.name AS client_division,cv2.code_value AS client_gender_cv_id,c.mobile_no AS client_mobile_no ,cv1.code_value AS client_type_cv_id,i.created_at AS interview_date,s.id AS FO_ID,s.display_name AS FO_username_FF,l.loan_counter,s.joining_date AS lo_joining_date,o.name AS office_name,o.opening_date AS office_opening_date   FROM kebhana_dashboard_db.db_interviews AS i   LEFT JOIN kebhana_dashboard_db.users AS u ON u.id=i.created_by   LEFT JOIN kebhana_middleware_db.m_loan AS l ON l.id=i.loan_id   LEFT JOIN kebhana_middleware_db.m_client AS c ON c.id=l.client_id   left join kebhana_middleware_db.m_code_value cv1 on cv1.id=c.client_type_cv_id   left join kebhana_middleware_db.m_code_value cv2 on cv2.id=c.gender_cv_id   LEFT JOIN kebhana_middleware_db.m_office o ON o.id=c.office_id   LEFT JOIN kebhana_middleware_db.m_office AS o1 ON o.parent_id=o1.id   LEFT JOIN kebhana_middleware_db.m_staff s ON s.id=c.staff_id   WHERE l.loan_status_id NOT IN (100,200,500)   AND i.interview_status NOT IN ('reject');", con=db_connection);

  additional_fields['office_opening_date'] = pd.to_datetime(additional_fields['office_opening_date'])
  additional_fields['interview_date'] = pd.to_datetime(additional_fields['interview_date'])
  additional_fields['lo_joining_date'] = pd.to_datetime(additional_fields['lo_joining_date'])

  additional_fields['loan_officer_experience']=(additional_fields['interview_date']-additional_fields['lo_joining_date']).dt.days/365
  additional_fields['office_opening_date'] = (( additional_fields['interview_date'] - additional_fields['office_opening_date'])/np.timedelta64(1, 'M'))

  additional_fields.loc[additional_fields['client_gender_cv_id']=="Male (ကျား)",'client_gender_cv_id']='Male'
  additional_fields.loc[additional_fields['client_gender_cv_id']=="Female",'client_gender_cv_id']='Female'

  additional_fields['office_name'] = additional_fields['office_name'].str.replace('Branch Office - ', '', regex=False)
  FOappdata = field_app_GL.merge(additional_fields, how='left',left_on='interview_id',right_on='interview_id')
  print("SQL loaded")

  return FOappdata



# In[3]:


import numpy as np
import pandas as pd
import re
from datetime import date
from datetime import datetime
from dateutil.relativedelta import *
from sqlalchemy import create_engine
get_ipython().system('pip install pymysql')
import pymysql
from google.colab import drive
drive.mount('/content/drive')


#---------ONLY CHANGE THIS--------------------------------

output_folderpath = "/content/drive/MyDrive/SID Audit Output/"
percent_threshold = 0.4
std_dev_multiple_threshold_date1date2 = 1
std_dev_multiple_threshold_interFO = 1.3
FO_minimum_loans = 10 #remove FOs with loans less than this amount in date1 and date2

date1start = datetime(year=2021, month=10, day=1)
date1end = datetime(year=2022, month=3, day=31)

date2start = datetime(year=2022, month=4, day=1)
date2end = datetime(year=2022, month=6, day=30)

SQLstartDate=date1start.strftime("%Y-%m-%d") #do not change this
SQLendDate=date2end.strftime("%Y-%m-%d") #do not change this

FOappdata = run_PHP_IL(SQLstartDate,SQLendDate)
#FOappdata = run_PHP_GL(SQLstartDate,SQLendDate)

#---------ONLY CHANGE THIS--------------------------------



all_input_data = load_csv_and_initialize(FOappdata)
all_input_data.set_index(all_input_data.pop('interview_date'), inplace=True)
all_input_data.reset_index(inplace=True)

all_input_data.set_index(all_input_data.pop('FO_ID'), inplace=True)
all_input_data.reset_index(inplace=True)
all_input_data.set_index(all_input_data.pop('client_id'), inplace=True)
all_input_data.reset_index(inplace=True)
all_input_data.set_index(all_input_data.pop('client_name'), inplace=True)
all_input_data.reset_index(inplace=True)
all_input_data.set_index(all_input_data.pop('loan_id'), inplace=True)
all_input_data.reset_index(inplace=True)

date1data = all_input_data[(all_input_data["interview_date"]>=date1start) & (all_input_data["interview_date"]<=date1end)].drop(columns=['loan_id','client_id','client_name'], axis=1) 
date2data = all_input_data[(all_input_data["interview_date"]>=date2start) & (all_input_data["interview_date"]<=date2end)].drop(columns=['loan_id','client_id','client_name'], axis=1) 

exclude_FO_list = get_lowloan_FOlist(date1data, date2data, FO_minimum_loans)

overlapping_cols = list(set(date1data.columns).intersection(date2data.columns))
overlapping_cols.sort()

categorical_list,  binary_list, numerical_list, error_list = classifycolumns(all_input_data[overlapping_cols])

within_date1_FOcompare_answer_df_concat = pd.DataFrame(columns = overlapping_cols).reindex()
within_date2_FOcompare_answer_df_concat = pd.DataFrame(columns = overlapping_cols).reindex()
date1date2_compare_answer_df_concat = pd.DataFrame(columns = overlapping_cols).reindex()

overlapping_division = list(set(date1data["client_division"].unique()).intersection(date2data["client_division"].unique()))

for current_division in overlapping_division:
    print("Currently processing " + current_division)
    
    date1data = all_input_data[(all_input_data["client_division"]==current_division) & (all_input_data["interview_date"]>=date1start) & (all_input_data["interview_date"]<=date1end)] 
    date2data = all_input_data[(all_input_data["client_division"]==current_division) & (all_input_data["interview_date"]>=date2start) & (all_input_data["interview_date"]<=date2end)]
    date1data = date1data.drop(columns = ["interview_date"])
    date2data = date2data.drop(columns = ["interview_date"])

    #fill in blank FO_ID with current_division blank
    date1data["FO_ID"].loc[date1data["FO_ID"]=="nan"] = current_division+" no FO_ID"
    date2data["FO_ID"].loc[date2data["FO_ID"]=="nan"] = current_division+" no FO_ID"

    within_date1_FOcompare_answer_df = pd.DataFrame(columns=overlapping_cols).reindex()
    within_date2_FOcompare_answer_df = pd.DataFrame(columns=overlapping_cols).reindex()
    date1date2_compare_answer_df = pd.DataFrame(columns=overlapping_cols).reindex()

    #process categorical & binary list together
    for column_name in (categorical_list + binary_list):
      within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df,date1date2_compare_answer_df = categorical_compare(
            column_name, date1data, date2data, within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df, date1date2_compare_answer_df)

    #process numerical list
    within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df,date1date2_compare_answer_df = numerical_compare(
        numerical_list, date1data, date2data, within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df,date1date2_compare_answer_df)

    #append each state/division result through row-wise concatenation
    within_date1_FOcompare_answer_df_concat = pd.concat([within_date1_FOcompare_answer_df_concat,within_date1_FOcompare_answer_df],axis=0)
    within_date2_FOcompare_answer_df_concat = pd.concat([within_date2_FOcompare_answer_df_concat,within_date2_FOcompare_answer_df],axis=0)
    date1date2_compare_answer_df_concat = pd.concat([date1date2_compare_answer_df_concat,date1date2_compare_answer_df],axis=0)

#set concats to original cols so the rest of the code can be used unchanged
within_date1_FOcompare_answer_df = within_date1_FOcompare_answer_df_concat.copy()
within_date2_FOcompare_answer_df = within_date2_FOcompare_answer_df_concat.copy()
date1date2_compare_answer_df = date1date2_compare_answer_df_concat.copy()

within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df,date1date2_compare_answer_df = remove_low_loan_FOs(
    within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df,date1date2_compare_answer_df,exclude_FO_list)

within_date1_FOcompare_answer_df["interFO_date1_count"] = within_date1_FOcompare_answer_df.count(axis=1)
within_date1_FOcompare_answer_df = within_date1_FOcompare_answer_df.sort_values(by = "interFO_date1_count", ascending=False)
correctcolorder = within_date1_FOcompare_answer_df.columns.tolist()[-1:] + within_date1_FOcompare_answer_df.columns.tolist()[:-1]
within_date1_FOcompare_answer_df = within_date1_FOcompare_answer_df[correctcolorder]
within_date1_FOcompare_answer_df.to_csv(output_folderpath+"interFO_date1 by FO by statediv " + date.today().strftime("%Y%m%d") + ".csv",encoding='utf-8-sig')

within_date2_FOcompare_answer_df["interFO_date2_count"] = within_date2_FOcompare_answer_df.count(axis=1)
within_date2_FOcompare_answer_df = within_date2_FOcompare_answer_df.sort_values(by = "interFO_date2_count", ascending=False)
correctcolorder = within_date2_FOcompare_answer_df.columns.tolist()[-1:] + within_date2_FOcompare_answer_df.columns.tolist()[:-1]
within_date2_FOcompare_answer_df = within_date2_FOcompare_answer_df[correctcolorder]
within_date2_FOcompare_answer_df.to_csv(output_folderpath+"interFO_date2 by FO by statediv " + date.today().strftime("%Y%m%d") + ".csv",encoding='utf-8-sig')

date1date2_compare_answer_df["interdate_count"] = date1date2_compare_answer_df.count(axis=1)
date1date2_compare_answer_df = date1date2_compare_answer_df.sort_values(by = "interdate_count", ascending=False)
correctcolorder = date1date2_compare_answer_df.columns.tolist()[-1:] + date1date2_compare_answer_df.columns.tolist()[:-1]
date1date2_compare_answer_df = date1date2_compare_answer_df[correctcolorder]
date1date2_compare_answer_df.to_csv(output_folderpath+"interdate by FO by statediv " + date.today().strftime("%Y%m%d") + ".csv",encoding='utf-8-sig')

#generate_prioritised_checklist_csv(within_date1_FOcompare_answer_df,within_date2_FOcompare_answer_df,date1date2_compare_answer_df,output_folderpath)
#outputcsv(within_date1_FOcompare_answer_df, within_date2_FOcompare_answer_df,date1date2_compare_answer_df,output_folderpath)

all_input_data.to_csv(output_folderpath + "raw data " + date.today().strftime("%Y%m%d") + ".csv",encoding='utf-8-sig')

#note for data portal - there are 4 CSV output files


# In[ ]:




