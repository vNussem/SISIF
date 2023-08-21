import db_connections as conn
import pandas as pd

def execute(stagingData, dwhData):
    
    row = {}

    query =  " \
        select insr_type,  \
        cover_type, \
        fract_type  insr_class\
        from insis_gen_asi.cfg_gl_gen_fractions \
        where (insr_type,  cover_type) \
        in    ({0}) \
    "

    keys = []
    for index, r in stagingData.iterrows():
        keys.append((r["insr_type"], r["cover_type"],))

    if (len(keys) == 0):
        return

    query_format = ",".join([str(k).replace(",)", ")") for k in keys])

    insisConn = conn.getInsis()        
    insisCursor = insisConn.cursor()
    

    insisCursor.execute(query.format(query_format))

    insisData = pd.DataFrame.from_records(insisCursor.fetchall(), columns=["insr_type", "cover_type", "insr_class"])
   
    insisConn.close()

    insisData.set_index("insr_type")

    for r in dwhData:
        ic = insisData.loc[(insisData["insr_type"] == r["insr_type"]) & (insisData["cover_type"] == r["cover_type"])]
        if (len(ic) > 0):
            r["insr_class"] = ic.iloc[-1]["insr_class"]
        else:
            r["insr_class"] = None
       

        