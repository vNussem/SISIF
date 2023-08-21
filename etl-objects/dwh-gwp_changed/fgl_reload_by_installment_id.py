import oltp_staging_reload as relaod
import config as cfg
import pandas as pd
from datetime import datetime




def execute(sourceConnection, destinationConnection, diff_data):

    etlPath = cfg.getEtlPath()
    task = "dwh-gwp_object"
    

    ids = []
    

    for index, diff_row in diff_data.iterrows():                
        if (diff_row["helper"] == "abis.plugin.asirom.dwh.DWHelperFGL_GWP" and (diff_row["install_id"],) not in ids):
            ids.append((diff_row["install_id"],))

    if (len(ids) == 0):
        return

    
    sysdate = datetime.now()
    
    relaod.execute(etlPath + "/" + task + "/", destinationConnection, ids, reloadQueryName="query_reload_by_install_id.sql")

    query_check_key_exists = "\
        update gwp_object \
        set deleted_date = sysdate  \
        where install_id in ({0}) \
        and nvl(check_key_exists, '1-jan-1900') < :checkKeyExists \
        and helper = 'abis.plugin.asirom.dwh.DWHelperFGL_GWP' \
    "
    query_format = ",".join([str(p).replace(",)", ")") for p in ids])

    destinationCursor = destinationConnection.cursor()
    destinationCursor.execute(query_check_key_exists.format(query_format), {"checkKeyExists": sysdate})
    
    # Delete GWP Prov
    query_delete_prov = " \
        update gwp_object \
        set deleted_date = sysdate \
        where install_id in ({0}) \
        and helper = 'abis.plugin.asirom.dwh.DWHelperINSIS_GWP_Provisionally' \
        and deleted_date is null \
    "

    destinationCursor.execute(query_delete_prov.format(query_format))


    # Errors
    query_errors = "insert into gwp_object ( \
            acc_date \
    ,       helper \
    ,       codcomp \
    ,       is_cancelled \
    ,       policy_id \
    ,       install_id \
    ,       preminst_ref_id \
    ,       insr_type \
    ,       cover_type \
    ,       insr_class \
    ,       currency \
    ,       op_amnt \
    ,       op_amnt_ron \
    ,       is_error \
    ,       inserted_date \
    ,       staging_id \
    )     \
    select acc_date \
    ,       helper \
    ,       codcomp \
    ,       is_cancelled \
    ,       policy_id \
    ,       install_id \
    ,       preminst_ref_id \
    ,       insr_type \
    ,       cover_type \
    ,       insr_class \
    ,       currency \
    ,       op_amnt \
    ,       op_amnt_ron \
    ,       1 \
    ,       sysdate \
    ,       gwp_changed_sq.nextval \
    from ( \
        select  acc_date \
        ,       helper \
        ,       codcomp \
        ,       is_cancelled \
        ,       policy_id \
        ,       install_id \
        ,       preminst_ref_id \
        ,       insr_type \
        ,       cover_type \
        ,       insr_class \
        ,       currency \
        ,       sum(nvl(acc_amnt, 0)) - nvl(sum(op_amnt), 0) op_amnt \
        ,       sum(nvl(acc_amnt_ron, 0)) - nvl(sum(op_amnt_ron), 0) op_amnt_ron \
        from gwp_object \
        where install_id in ({0}) \
        and deleted_date is null \
        having abs(sum(nvl(acc_amnt_ron, 0)) - sum(nvl(op_amnt_ron, 0)) ) > 0 \
        group by acc_date \
        ,       helper \
        ,       codcomp \
        ,       is_cancelled \
        ,       policy_id \
        ,       install_id \
        ,       preminst_ref_id \
        ,       currency \
        ,       insr_type \
        ,       cover_type \
        ,       insr_class \
    ) \
    "
    
    destinationCursor.execute(query_errors.format(query_format))
    

