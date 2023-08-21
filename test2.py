import oltp_staging as olap
import staging_clean

etlPath = "python\data-sync\etl-objects"

# task = "insis-gen_risk_covered"
# task = "insis-insured_object"

# task = "insis-gen_instalments"
# task = "insis-prem_inst_fract"
# task = "insis-ueasi_cfg_class_of_business"
# task = "insis-fgl_insis2gl"
# task = "camelot-interfaces-SapProductLob"

# task = "dwh-insis_changed_policies-act_objects_dim"
# task = "insis-claim_refuses"


# task = "dwh-gwp_object"
# staging_clean.clean(etlPath + "\\" + task + "\\")
# task = "dwh-gwp_changed"
# olap.execute(etlPath + "\\" + task + "\\")


# task = "dwh-gwp_changed"
# olap.execute(etlPath + "\\" + task + "\\")

# task = "dwh-policy_changed"
# olap.execute(etlPath + "\\" + task + "\\")


# task = "dwh-gwp_object_changed"
# olap.execute(etlPath + "\\" + task + "\\")

# task = "camelot-cash-buchung"


# task = "exp-units"
# staging_clean.clean(etlPath + "\\" + task + "\\")

# task = "exp-units-motor-changed" 

# task = "dwh-gwp_object_changed"
# task =  "dwh-policy_changed"
# task = "dwh-gwp_changed"

# task = "exp-units-grc-changed" 
# task = "objects-dim-grc-changed"

# task = "insis-claim_regresses"
# task = "insis-claim_regresses_sum"
# task = "insis-claim_regresses_restore"
# task = "insis-hist_claim_regresses"
# task = "insis-quest_questions"
# task = "insis-claim_objects"

task = "dwh-gwp_changed"
task = "dwh-policy_changed"
task = "dwh-gwp_object_changed"

olap.execute(etlPath + "\\" + task + "\\")