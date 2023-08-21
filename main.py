import oltp_staging as oltp
import datetime
import schedule
import time
from tqdm import tqdm
import mtpl_dwh

import compare_and_sync_table as snc
import db_connections as cons
import check_leasing

import staging_clean

import staging_dwh as stagingToDWH

etlPath    = "python\data-sync\etl-objects"
dwhEtlPath = "python\data-sync\etl-objects-dwh"

tasks = [
    "chestionar-mtpl",
    "fintech.asr_policy",
    "fintech.account",
    "fintech.ASR_MedicalConsumptionASR_MedicalConsumption",
    "fintech.ASR_MedicalConsumptionXSelectedRisks",
    "fintecth.ebs_ASR_MedicalService",
    "fintecth.EBS_ASR_MedicalClinic",
    "fintech.asr_coverages",
    
    
    "insis-system_periods",
    
    "insis.p_offices",
    "insis-cfg_nom_language_table",
    "insis-ueasi_cfg_class_of_business",
    "insis-cfg_gl_gen_fractions",
    # "insis.gen_reserve_corp",        

    "insis-fgl_insis2gl",

    "insis-hist_policy",
    "insis-hist_policy_angent_change",
    "insis-hist_policy_commissions",    
    "insis-hist_gen_annex",
    "insis-hist_insured_object",    
    "insis-policy",
    "insis-policy_commissions",    

    "insis-prem_inst",
    

    "insis-gen_risk_covered",
    "insis-prem_inst_fract",
    "insis-prem_inst_reversed",
    "insis-insured_object",
    "insis-gen_annex",
    "insis-gen_instalments",

    "insis-hist_o_car",
    "insis-o_car",    

    "insis-p_agents",
    
    "insis-claim",
    "insis-claim_inspections",
    "insis-claim_objects",   
    "insis-claim_request",
    "insis-claim_payments",
    "insis-claim_refuses",
    
    "insis-hist_claim",
    "insis-hist_claim_request",
    "insis-hist_claim_objects",
    "insis-hist_claim_inspections",
    "insis-hist_claim_payments",
    
    "insis-quest_questions",
    "insis-hist_quest_claim_policy",
    
    "insis-wf_activities",
    "insis-wf_ref_activities",
    "insis-wf_notes",
    "insis-intrf_documentum_docs",
    "insis-hist_p_staff",
    "insis-p_staff",

    "insis-claim_expenses",
    "insis-hist_claim_expenses",

    "insis-claim_regresses",
    "insis-claim_regresses_sum",
    "insis-claim_regresses_restore",
    "insis-hist_claim_regresses",

    "camelot.contract..ver_general",
    "camelot.interfaces..a1sub",
    "camelot-interfaces-SapProductLob",

    "dwh-insis_changed_policies-act_objects_dim",
    "objects-dim-grc-changed",

    "dwh-gwp_changed",
    "dwh-policy_changed",
    "dwh-gwp_object_changed",

    "exp-units-motor-changed",
    "exp-units-grc-changed"
]


# schedule.every(2).seconds.do(run_task)


stagingToDwhTasks = [
    "insis-prem_inst_fract",
    "insis-claim_request",
    "insis-claim_termene_plata",

    "insis-claim_regresses_sum",
    "insis-claim_regresses_restore",
    "insis-fgl-claim_regresses",

    "insis-ueasi_cfg_class_of_business",
    "camelot-SapProductLob",
    
    "insis-fgl-gwp",
    "insis-fgl-commission",    
    "insis-fgl-claim_payments",

    "camelot-a1sub-gwp",

    "dwh-act_objects_dim",
    "dwh-act_gwp",
    "dwh-act_upr",
    "dwh-act_exp_units",
    "dwh-act_claims",
    "dwh-act_claim_reserves",
    "dwh-act_claim_payments",
    "dwh-act_regr_est",
    "dwh-act_regr_recovered",


    "dwh-act-exp-units",
    "dwh-act-objects_dim",

    "insis-gen_reserve_corp"


]    


def run_task(logger):
    start_time = datetime.datetime.now()
    print("start ", start_time)
    
    print("OLTP -> Staging")
    # if (datetime.datetime.now().time().hour in [14] and datetime.datetime.now().day in [19]):
    if (datetime.datetime.now().time().hour in [7, 8, 9] and datetime.datetime.now().weekday() in [5, 6]):
        logger.info("check_leasing")
        check_leasing.main()

    for task in tasks:
        logger.info("clean " + task)
        staging_clean.clean(etlPath + "\\" + task + "\\")
        logger.info("load " + task)
        oltp.execute(etlPath + "\\" + task + "\\")
        
    print("Staging -> DWH")
    mtpl_dwh.main()


    for task in stagingToDwhTasks: 
        logger.info("staging -> DWH " + task)       
        stagingToDWH.execute(dwhEtlPath + "\\" + task + "\\")

    end_time = datetime.datetime.now()
    print("start", start_time, " end ", end_time)


    
# D E B U G
# run_task()
# schedule.every(1).hours.do(run_task)

# while True:
#     schedule.run_pending()
#     time.sleep(1)



def resyncTables():

    # snc.compare(oltpTableName="insis_gen_asi.claim", stagingTableName="insis_claim",
    #             key_column="claim_id", tag="insis_claim_reload", oltpConn=cons.getInsis(), dbEngine="ORACLE")
    # snc.compare(oltpTableName="insis_gen_asi.claim_objects", stagingTableName="insis_claim_objects",
    #             key_column="claim_obj_seq", tag="insis_claim_objects_reload", oltpConn=cons.getInsis(), dbEngine="ORACLE")
    # snc.compare(oltpTableName="insis_gen_asi.o_car", stagingTableName="insis_o_car",
    #             key_column="object_id", tag="inis_o_car_reload", oltpConn=cons.getInsis(), dbEngine="ORACLE")
    # snc.compare(oltpTableName="insis_gen_asi.policy", stagingTableName="insis_policy",
    #             key_column="policy_id", tag="insis_policy_reload", oltpConn=cons.getInsis(), dbEngine="ORACLE")
    # snc.compare(oltpTableName="contract..ver_general", stagingTableName="camelot_ver_general", key_column="num_contract",
    #             tag="camelot_ver_general_reload", oltpConn=cons.getCamelot(), dbEngine="sqlServer", keyType='uid')
    # snc.compare(oltpTableName="Interfaces..a1sub", stagingTableName="Camelot_a1sub", key_column="id", tag="camelot_a1sub_reload", oltpConn=cons.getCamelot(
    # ), dbEngine="sqlServer", ignore_columns=["Own", "ValoareCCY", "CURRENCY", "EXCHANGE_RATES", "CORRECTION", "TIP_CLIENT"], trunc_date_columns=["DATAINASG", "DATAOUTASG"])

    
    
    
    
    
    if False:
        snc.compare(oltpTableName="contract..ver_general", stagingTableName="camelot_ver_general", key_column="num_contract",
                tag="camelot_ver_general_reload", oltpConn=cons.getCamelotEngine(), dbEngine="sqlServer", keyType='uid')
        snc.compare(oltpTableName="Interfaces..a1sub", stagingTableName="Camelot_a1sub", key_column="id", tag="camelot_a1sub_reload", oltpConn=cons.getCamelotEngine(), dbEngine="sqlServer", ignore_columns=["Own", "ValoareCCY", "CURRENCY", "EXCHANGE_RATES", "CORRECTION", "TIP_CLIENT"], trunc_date_columns=["DATAINASG", "DATAOUTASG"])
        snc.compare(oltpTableName="insis_gen_asi.policy", stagingTableName="insis_policy",
                key_column="policy_id", tag="insis_policy_reload", oltpConn=cons.getInsisEngine(), dbEngine="ORACLE")    
        snc.compare(oltpTableName="insis_gen_asi.o_car", stagingTableName="insis_o_car",
                key_column="object_id", tag="inis_o_car_reload", oltpConn=cons.getInsisEngine(), dbEngine="ORACLE")
        snc.compare(oltpTableName="insis_gen_asi.claim", stagingTableName="insis_claim",
                key_column="claim_id", tag="insis_claim_reload", oltpConn=cons.getInsisEngine(), dbEngine="ORACLE")
        snc.compare(oltpTableName="insis_gen_asi.claim_objects", stagingTableName="insis_claim_objects",
                key_column="claim_obj_seq", tag="insis_claim_objects_reload", oltpConn=cons.getInsisEngine(), dbEngine="ORACLE")
    


    #compare(oltpTableName = "ebs.ASR_POLICY", stagingTableName = "EBS_ASR_POLICY", key_column = "ASR_Policyid", tag = "ebs_asr_policy_reload", oltpConn = cons.getFintech(), dbEngine = "sqlServer", keyType = 'uid')
