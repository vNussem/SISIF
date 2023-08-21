import db_connections as conn
import pandas as pd

def execute(etlObject):
    
    destinationConnection = conn.getDWH()

    query = "\
        update gwp_object \
        set deleted_date = sysdate \
        where helper = 'abis.plugin.asirom.dwh.DWHelperINSIS_GWP_Provisionally' \
        and acc_date < trunc(sysdate) \
        and deleted_date is null \
    "

    
    destinationCursor = destinationConnection.cursor()
    destinationCursor.execute(query)
    

    query = "\
        update gwp_object \
        set deleted_date = sysdate \
        where helper in ('abis.plugin.asirom.dwh.DWHelperCamelot_GWP_Provisionally') \
        and trunc(acc_date, 'month') <= (select max(acc_date) from insis_periods) \
        and deleted_date is null \
        and cover_type not in ('MI1') \
    "

    
    destinationCursor.execute(query)

    destinationConnection.commit()