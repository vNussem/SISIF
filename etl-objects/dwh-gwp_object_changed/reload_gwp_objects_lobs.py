import oltp_staging_reload as relaod
import config as cfg
import pandas as pd
from datetime import datetime




def execute(sourceConnection, destinationConnection, diff_data):

    etlPath = cfg.getEtlPath()
    task = "dwh-gwp_object_lobs"
    

    ids = []
    

    for index, diff_row in diff_data.iterrows():                
        # accDate = "to_date('" + str(pd.Timestamp(diff_row["acc_date"])) + "', 'yyyy-mm-dd hh24:mi:ss. ')"
        accDate = diff_row["acc_date_s"]
        if ((accDate, diff_row["insr_type"], diff_row["codcomp"],) not in ids):
            # if (diff_row["policy_id"] == 1300165063):
            ids.append((accDate, diff_row["insr_type"], diff_row["codcomp"],))

    if (len(ids) == 0):
        return

    
    sysdate = datetime.now()
    
    relaod.execute(etlPath + "/" + task + "/", destinationConnection, ids, reloadQueryName="query_reload.sql")

    query_check_key_exists = "\
        update gwp_object_lobs \
        set deleted_date = sysdate  \
        where (acc_date, insr_type, codcomp) in ({0}) \
        and nvl(check_key_exists, '1-jan-1900') < :checkKeyExists \
        and deleted_date is null \
    "
    query_format = ",".join([str(p).replace(",)", ")") for p in ids])

    destinationCursor = destinationConnection.cursor()
    destinationCursor.execute(query_check_key_exists.format(query_format), {"checkKeyExists": sysdate})
    
    #cleanup        
    # query_check_key_exists = "\
    #     delete from gwp_object_lobs \
    #     where (acc_date, insr_type) in ({0}) \
    #     and deleted_date is not null \
    # "
    # destinationCursor.execute(query_check_key_exists.format(query_format))

