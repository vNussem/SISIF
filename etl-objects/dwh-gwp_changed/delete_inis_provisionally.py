import db_connections as conn
import pandas as pd

def execute(etlObject):
    

    query = "\
        update gwp_object \
        set deleted_date = sysdate \
        where helper = 'abis.plugin.asirom.dwh.DWHelperINSIS_GWP_Provisionally' \
        and acc_date < trunc(sysdate) \
        and deleted_date is null \
    "

    destinationConnection = conn.getDWH()
    destinationCursor = destinationConnection.cursor()
    destinationCursor.execute(query)

    destinationConnection.commit()

    