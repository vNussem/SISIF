import db_connections as conn
import pandas as pd

def execute(etlObject):
    

    
    statement = "\
        select count(*) cnt \
        from insis_gen_reserve_corp \
        where reserve_date >= '1-jan-2023' \
        and  deleted_date is null \
    "

    stgConn = conn.getStaging()    
    stg = pd.DataFrame(stgConn.cursor().execute(statement), columns=["cnt"])    
    
    statment = "\
        select count(*) cnt \
        from   upr \
        where acc_date >= '1-jan-2023' \
        and helper = 'abis.plugin.asirom.dwh.DWHelperInsis_GenReserveCorp' \
    "

    dwhConn = conn.getDWH()    

    dwh = pd.DataFrame(dwhConn.cursor().execute(statment), columns=["cnt"])
    

    diff = stg.join(dwh, lsuffix="_stg", rsuffix="_dwh")

    diff["cnt_diff"] = diff["cnt_stg"] - diff["cnt_dwh"]

    etlObject["integrityTrigger"]["result"] = []
    for index, row in diff.iterrows():
        etlObject["integrityTrigger"]["result"].append(
            {
                "cnt_stg": int(row["cnt_stg"]),
                "cnt_dwh": int(row["cnt_dwh"]),
                "cnt_diff": int(row["cnt_diff"]),
            }
        )
                

