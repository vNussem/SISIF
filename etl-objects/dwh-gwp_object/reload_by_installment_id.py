import oltp_staging_reload as relaod
import config as cfg
import pandas as pd
from datetime import datetime

def execute(sourceConnection, destinationConnection, diff_data):
    

    ids = []
    installIds = []
    

    for index, diff_row in diff_data.iterrows():                
        accDate = "to_date('" + str(pd.Timestamp(diff_row["acc_date"])) + "', 'yyyy-mm-dd hh24:mi:ss. ')"
        truncAccDate = "trunc(to_date('" + str(pd.Timestamp(diff_row["acc_date"])) + "', 'yyyy-mm-dd hh24:mi:ss. '), 'month'" + ")"

        # if (diff_row["install_id"] == 5696904590):
        #     a=1

        if ((diff_row["install_id"], accDate, diff_row["gwp_staging_id"], diff_row["cover_type"], diff_row["pif_staging_id"]) not in ids):
            ids.append((diff_row["install_id"], accDate, diff_row["gwp_staging_id"], diff_row["cover_type"], diff_row["pif_staging_id"]))
        if ((diff_row["install_id"], truncAccDate, diff_row["cover_type"]) not in installIds):
            installIds.append((diff_row["install_id"], truncAccDate, diff_row["cover_type"]))

    if (len(ids) == 0):
        return
    
     
    sysdate = datetime.now()    


    query_check_key_exists = "\
        update gwp_object \
        set check_key_exists = :sdate  \
        where (install_id, acc_date, gwp_staging_id, cover_type, pif_staging_id) in ({0}) \
        and deleted_date is null \
    "

    quer_delete_key_not_exists = "\
        update gwp_object \
        set deleted_date = sysdate  \
        where (install_id, trunc(acc_date, 'month'), cover_type) in ({0}) \
        and nvl(check_key_exists, '1-jan-1900') < :checkKeyExists \
    "

    query_format = ",".join([str(p).replace(",)", ")") for p in ids]).replace('"', "")

    destinationCursor = destinationConnection.cursor()
    destinationCursor.execute(query_check_key_exists.format(query_format), {"sdate": sysdate})


    query_format = ",".join([str(p).replace(",)", ")") for p in installIds]).replace('"', "")
    destinationCursor.execute(quer_delete_key_not_exists.format(query_format), {"checkKeyExists": sysdate})

    
    

    

