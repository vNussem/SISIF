import staging_dwh as stDWH
import staging_clean

etlPath = "python\data-sync\etl-objects-dwh"

# task = "insis-claim_request"
# task = "insis-claim_termene_plata"
# task = "insis-ueasi_cfg_class_of_business"
# task = "insis-fgl-gwp"
# task = "insis-fgl-commission"

# task = "camelot-a1sub-gwp"
# task = "camelot-SapProductLob"
# task = "insis-prem_inst_fract"

# task = "insis-fgl-claim_payments"
# task = "insis-gen_reserve_corp"

# task = "dwh-act_objects_dim"

# task = "insis-prem_inst_reversed"

# etlPath = "python\data-sync\etl-objects"
# task = "exp-units"
# staging_clean.clean(etlPath + "\\" + task + "\\")

etlPath = "python\data-sync\etl-objects-dwh"
# task = "dwh-act-objects_dim"
# task = "dwh-act-objects_dim"
# task = "insis-claim_regresses_sum"
# task = "insis-claim_regresses_restore"
task = "insis-fgl-gwp"

stDWH.execute(etlPath + "\\" + task + "\\")