import oltp_staging as olap
import oltp_staging_reload as olap_reload
import staging_dwh as stDWH

import db_connections as dbConn

import main

import staging_clean

import importlib
import pandas as pd

if False:
    main.run_task()

if False:
    diff_data = pd.DataFrame([100015080857,], columns=["policy_id"])
     
    conn = dbConn.getStaging()

    # print(diff_data)
    trg = importlib.import_module("etl-objects.insis-hist_policy.reload_prem_inst_fract") 
    trg.execute(sourceConnection=None, destinationConnection=conn, diff_data=diff_data)

    conn.commit()

if False:
    etlPath = "python\data-sync\etl-objects\\"
    #task = "insis-policy"
    # task = "insis-gen_risk_covered"
    task = "insis-prem_inst_fract"

    conn = dbConn.getStaging()

    print(task)
    olap_reload.execute(etlPath + "\\" + task + "\\", conn, [(100014924542, )])

    conn.commit()

if False:
    etlPath = "python\data-sync\etl-objects-dwh"
    # task = "insis-prem_inst_fract"    
    # task = "dwh-act_gwp"
    # task = "dwh-act_claims"
    # task = "dwh-act_objects_dim"    
    # task = "dwh-act_upr"
    # task = "dwh-act_exp_units"
    # task = "dwh-act_claim_reserves"
    # task = "dwh-act_claim_payments"    
    # task = "dwh-act_regr_est"

    task = "dwh-act_regr_recovered"
    
    stDWH.execute(etlPath + "\\" + task + "\\")

if True:
    etlPath = "python\data-sync\etl-objects\\"
    # task = "insis-policy"
    # task = "insis-insured_object"
    # task = "insis-gen_annex"
    # task = "insis-hist_policy"
    # task = "insis-prem_inst_fract"
    # task = "insis-gen_risk_covered"
    # task = "insis-claim_objects"
    # task = "insis-hist_gen_annex"
    # task = "insis-hist_insured_object"
    # task = "insis-fgl_insis2gl"
    # task = "insis-policy_commissions"
    # task = "insis-hist_policy_commissions"
    # task = "insis-hist_o_car"
    # task = "insis-o_car"
    # task = "fintech.asr_coverages"
    # task = "insis-hist_policy_angent_change"
    # task = "insis-prem_inst"
    # task = "insis-p_agents"
    # task = "insis-gen_reserve_corp"
    # task = "insis-prem_inst_fract-by-period_id"
    # task = "insis-system_periods"      
    # task = "insis-quest_questions"
    # task = "insis-hist_claim_objects"
    # task = "insis-hist_quest_claim_policy"
    # task = "insis-claim"
    # task = "insis-hist_claim"
    # task = "insis-claim_inspections"
    # task = "insis-hist_claim_inspections"

    # task = "insis-hist_claim_request"
    # task = "insis-quest_questions"
    # task = "insis-claim_request"  

    # task = "insis-claim_payments"
    # task = "insis-hist_claim_payments"

    # task = "insis-wf_activities"
    # task = "insis-wf_ref_activities"
    # task = "insis-wf_notes"
    # task = "insis-intrf_documentum_docs"
    # task = "insis-cfg_nom_language_table"    
    # task = "insis-p_staff"
    # task = "insis-hist_p_staff"

    # task = "insis-claim_expenses"
    # task = "insis-hist_claim_expenses"

    # task = "dwh-insis_changed_policies-act_objects_dim"
    

    # staging_clean.clean(etlPath + "\\" + task + "\\")
    
    # task = "exp-units-grc-changed" 
    task =  "dwh-policy_changed"
    olap.execute(etlPath + "\\" + task + "\\")

    #task = "insis-insured_object"
    #olap.execute(etlPath + "\\" + task + "\\")


# from datetime import datetime, time
# import numpy as np

# working_hours = [9, 18, 19]

# print(datetime.now().time().hour)

# print(time(12).hour)

# print([time(h).hour for h in working_hours])

# if (datetime.now().time().hour in [time(h).hour for h in working_hours]):
#     print('a')

# print(np.arary(working_hours))

# print(time(working_hours))

