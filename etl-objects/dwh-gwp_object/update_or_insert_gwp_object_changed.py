
import oltp_staging_reload as relaod
import config as cfg
import pandas as pd
from datetime import datetime




def execute(sourceConnection, destinationConnection, diff_data):

    etlPath = cfg.getEtlPath()
    task = "dwh-gwp_object"
    

    ids = []
    

    for index, diff_row in diff_data.iterrows():                
        if (diff_row["helper"] != "abis.plugin.asirom.dwh.DWHelperFGL_GWP" and (diff_row["policy_id"], diff_row["helper"],) not in ids):
            # if (diff_row["policy_id"] == 1300165063):
            ids.append((diff_row["policy_id"], diff_row["helper"],))

    if (len(ids) == 0):
        return

    
    sysdate = datetime.now()
    
    relaod.execute(etlPath + "/" + task + "/", destinationConnection, ids, reloadQueryName="query_reload_by_policy_id.sql")

    query_check_key_exists = "\
        update gwp_object \
        set deleted_date = sysdate  \
        where (policy_id, helper) in ({0}) \
        and nvl(check_key_exists, '1-jan-1900') < :checkKeyExists \
        and deleted_date is null \
    "
    query_format = ",".join([str(p).replace(",)", ")") for p in ids])

    destinationCursor = destinationConnection.cursor()
    destinationCursor.execute(query_check_key_exists.format(query_format), {"checkKeyExists": sysdate})
    
    
    



MERGE INTO employees e
    USING (SELECT * FROM hr_records WHERE start_date > ADD_MONTHS(SYSDATE, -1)) h
    ON (e.id = h.emp_id)
  WHEN MATCHED THEN
    UPDATE SET e.address = h.address
  WHEN NOT MATCHED THEN
    INSERT (id, address)
    VALUES (h.emp_id, h.address);