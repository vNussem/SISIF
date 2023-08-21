import db_connections as conn
import pandas as pd

def execute(etlObject):
    

    statment = "\
        select acc_date \
        ,      codcomp \
        ,      insr_type \
        ,      sum(premium) premium\
        from ( \
            select acc_date \
            ,      codcomp \
            ,      insr_type \
            ,      sum(op_amnt_ron) premium \
            from gwp_object \
            where deleted_date is null \
            and acc_date > '1-jan-2021' \
            group by acc_date \
            ,      codcomp \
            ,      insr_type \
            union all \
            select acc_date \
            ,      codcomp \
            ,      insr_type \
            ,     - sum(premium) premium \
            from gwp_object_lobs \
            where deleted_date is null \
            and acc_date > '1-jan-2021' \
            group by acc_date \
            ,      codcomp \
            ,      insr_type \
        ) \
        group by acc_date \
        ,      codcomp \
        ,      insr_type \
        having sum(premium) <> 0 \
        order by acc_date \
        ,      codcomp \
        ,      insr_type \
    "

    oltpConn = conn.getDWH()    

    oltp = pd.DataFrame(oltpConn.cursor().execute(statment), columns=["acc_date", "codcomp", "insr_type", "premium"])
    

    
    etlObject["integrityTrigger"]["result"] = []
    for index, row in oltp.iterrows():        
        etlObject["integrityTrigger"]["result"].append(
            {
                # "year": int(row["year_oltp"]),
                "acc_date": str(row["acc_date"]),
                "codcomp": int(row["codcomp"]),
                "insr_type": int(row["insr_type"]),
                "premium": (row["premium"]),
            }
        )
            


# Reprocess 


# update gwp_object 
# set change_date = sysdate
# --where (acc_date, codcomp, insr_type) in (('01-JUL-21',	'080', 0))
# where (acc_date, codcomp, insr_type) in ( 
#     select acc_date         ,      codcomp         ,      insr_type         from (             select acc_date             ,      codcomp             ,      insr_type             ,      sum(op_amnt_ron) premium             from gwp_object
#            where deleted_date is null             and acc_date > '1-jan-2021'             group by acc_date             ,      codcomp             ,      insr_type             union all             select acc_date             ,      codcomp             ,      insr_type
#    ,     - sum(premium) premium             from gwp_object_lobs             where deleted_date is null             and acc_date > '1-jan-2021'             group by acc_date             ,      codcomp             ,      insr_type         )         
#    group by acc_date         
#    ,        codcomp         
#    ,       insr_type         
#   having   sum(premium) <> 0
# )