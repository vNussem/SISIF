import pandas as pd

def execute(stagingData, dwhData):
        

    for index, r in stagingData.iterrows():                
        if (pd.isnull(r["deleted_date"])):
            row = {}
            row["helper"] = "abis.plugin.asirom.dwh.DWHelperInsis_GenReserveCorp"
            row["staging_id"] = r["staging_id"]

            row["acc_date"] = r["reserve_date"]
            row["policy_id"] = r["policy_id"]
            row["preminst_id"] = r["preminst_id"]
            row["insured_obj_id"] = r["insured_obj_id"]
            row["object_id"] = r["object_id"]
            row["gwp_rate"] = r["gwp_rate"]            
            row["gwp_currency"] = r["gwp_currency"]
            row["cover_type"] = r["cover_type"]
            # row["insr_class"] = r["insr_class"]
            row["insr_type"] = r["insr_type"]
            row["pr_upr"] = r["pr_upr"]
            row["pr_ep"] = r["pr_ep"]
            row["com_dac"] = r["com_dac"]
            row["com_ec"] = r["com_ec"]

            dwhData.append(row)		
		
		
		
		
		
		
		
		
		
		
		
		
		

		
		
		
		
	
		