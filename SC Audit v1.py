#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
get_ipython().system('pip install connectorx')
import connectorx as cx
from google.colab import drive 
drive.mount('/content/drive')

db_connection_str = 'mysql://xxx:xxx%40pp@hana-production-analytics-read-replica.c0wcwq0ocdj3.ap-southeast-1.rds.amazonaws.com/kebhana_dashboard_db'


#------------------------------------------------------------------------------------
def get_statistics(y_test,y_pred3):
  
  print ("base score: " + str(round(y_pred3.mean(),5)),file=file)

  
  #HISTOGRAM

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
  plt.savefig("/content/drive/MyDrive/SC Audit Output/dist.png")


  #confusion matrix
  from sklearn import metrics
  fpr,tpr,thresholds = metrics.roc_curve(y_test,y_pred3)
  auc3 = metrics.roc_auc_score(y_test,y_pred3)
  print("AUC = " + str(auc3),file = file)
  print("AUC = " + str(auc3))



  optimal_idx = np.argmax(tpr - fpr)
  optimal_threshold = thresholds[optimal_idx]
  threshold = optimal_threshold
  y_pred = (y_pred3 > threshold).astype('float')
  cm = metrics.confusion_matrix(y_test, y_pred)
  cm = cm/cm.sum()


  disp = metrics.ConfusionMatrixDisplay(confusion_matrix=cm)

  disp.plot()
  #plt.show()
  plt.savefig("/content/drive/MyDrive/SC Audit Output/confusion.png")
  #print("Optimum cutoff: Pd = 1 if P >=" + str(optimal_threshold),file = file)
  print("accuracy (at optimum cutoff) = " + str(cm[1,1]+cm[0,0]),file = file)


#--------------------------------------------------------------------
def create_IL_validation_df(startDate,endDate,requiredoutputs,late_day_threshold,paid_pc_threshold,reschedule_count_threshold,ILGL):

  # ILGL="IL"
  # requiredoutputs = requiredoutputs183

  str_output = ",ld.".join([str(id) for id in requiredoutputs])

  if ILGL == "IL":
    loan_type_str = "('individual','topup individual')"
  elif ILGL == "GL":
    loan_type_str = "('group','topup group')"
    

  loan_id_df = cx.read_sql(db_connection_str, "SELECT l.id loan_id, i.id interview_id,cs.pd AS Predicted,ld."+str_output+"     FROM kebhana_dashboard_db.db_interviews AS i     LEFT JOIN kebhana_middleware_db.m_loan AS l ON l.id=i.loan_id     LEFT JOIN ( SELECT DISTINCT cxx.interview_id, cxx.pd, cxx.status, cxx.created_at FROM kebhana_dashboard_db.hwa_cs_output cxx       WHERE cxx.created_at = (SELECT max(cxo.created_at) FROM kebhana_dashboard_db.hwa_cs_output cxo WHERE cxo.interview_id = cxx.interview_id)     ) AS cs ON cs.interview_id = i.id     LEFT JOIN kebhana_middleware_db.hwa_late_day_fields AS ld ON ld.loan_id=l.id   WHERE i.loan_type IN" + loan_type_str + "  AND l.loan_status_id NOT IN (100,200,500)   AND i.interview_status NOT IN ('reject')   AND l.disbursedon_date BETWEEN '"+startDate+"' AND '"+endDate+"';")


  loan_id_df = loan_id_df.apply(pd.to_numeric,errors='ignore')

  import itertools
  late_day_idx = [i for i, s in enumerate(requiredoutputs) if 'late_days' in s][0]
  paid_pc_idx = [i for i, s in enumerate(requiredoutputs) if 'paid_pc' in s][0]
  reschedule_idx = [i for i, s in enumerate(requiredoutputs) if 'reschedule' in s][0]

  loan_id_df["Actual"] = 0
  loan_id_df.loc[(loan_id_df[requiredoutputs[late_day_idx]]>=late_day_threshold) | 
                      (loan_id_df[requiredoutputs[paid_pc_idx]]<=paid_pc_threshold)|
                      (loan_id_df[requiredoutputs[reschedule_idx]]>=reschedule_count_threshold),"Actual"] = 1

  loan_id_df.drop(columns= requiredoutputs,inplace=True)
  print("Total loans found for validation: " + str(len(loan_id_df)),file = file)
  print("Loans missing actual data: " + str( loan_id_df["Actual"].isna().sum()),file = file)
  print("Loans missing predicted data: " + str( loan_id_df["Predicted"].isna().sum()),file = file)
  print("Now dropping loans with any missing predicted or actual data",file = file)
  loan_id_df = loan_id_df.dropna()

  print("Validation dataset consists of " + str(len(loan_id_df)) + " loans",file = file)

  return loan_id_df


# In[ ]:


#CODE BODY

startDate='2021-07-01'
endDate='2022-01-31'

#note for data portal - give 3 performance window options - 90d, 183d, 365d
requiredoutputs90 = ["late_days_in_90d", "paid_pc_90d", "reschedule_count_in_90d"]
requiredoutputs183 = ["late_days_in_183d", "paid_pc_183d", "reschedule_count_in_183d"]
requiredoutputs365 = ["late_days_in_365d", "paid_pc_365d", "reschedule_count_in_365d"]


#thresholds for each scorecard
late_day_threshold = 10
paid_pc_threshold = 0.99
reschedule_count_threshold = 1

ILGL = "GL"

file = open("/content/drive/MyDrive/SC Audit Output/output.txt", 'w')
print("SC Audit Report for " + ILGL + " for loans disbursed between " + startDate + " to " + endDate,file=file)

validation_df = create_IL_validation_df(startDate,endDate,requiredoutputs183,late_day_threshold,paid_pc_threshold,reschedule_count_threshold,ILGL) #need to account for GL selection
if validation_df["Actual"].sum()==0:
  print("Unable to run analysis - no client has defaulted yet",file=file)
  print("Unable to run analysis - no client has defaulted yet")
else:
  get_statistics(validation_df["Actual"],validation_df["Predicted"])
file.close()

print("Actual sum")
print(validation_df["Actual"].sum())
print("Predicted sum")
print(validation_df["Predicted"].sum())
print("Predicted count")
print(validation_df["Predicted"].count())

#note for data portal - there are 3 output files - 2 PNG, 1 txt 

