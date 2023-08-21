import db_connections as conn
import pandas as pd

def execute(etlObject):
    

    query = "\
        update gwp_object \
        set deleted_date = sysdate \
        where helper in ('abis.plugin.asirom.dwh.DWHelperCamelot_GWP_Provisionally') \
        and trunc(acc_date, 'month') <= (select max(acc_date) from insis_periods) \
        and deleted_date is null \
        and cover_type not in ('MI1') \
    "

    destinationConnection = conn.getDWH()
    destinationCursor = destinationConnection.cursor()
    destinationCursor.execute(query)

    destinationConnection.commit()



