with policy as (
    select  trunc(effective_date) acc_date
    ,       pif.RISK_AMNT premium
    ,       pif.gl_local_amnt
    ,       pif.currency 
    ,       pif.policy_id
    ,       pif.cover_type
    ,       pif.preminst_id
    -- ,       pif.object_id
    ,       pif.insured_obj_id
    ,       pif.staging_id
    ,       p.insr_type
    ,       p.AGENT_ID
    ,      nvl(pos.office_no, po.office_no) codcomp
    from INSIS_PREM_INST_FRACT pif
    left join insis_policy p
    on p.policy_id = pif.policy_id
    
    and p.deleted_date is null
    
    left join insis_p_offices po 
    on po.office_id = p.office_id
    left join insis_p_offices pos 
    on pos.office_id = po.parent_office
    where pif.policy_id in ({0}) 
    and p.policy_state not in (30)
    and pif.effective_date >= trunc(sysdate)        
    and pif.deleted_date is null
    and not exists (select install_id from INSIS_FGL_INSIS2GL a where a.install_id = pif.PREMINST_ID)
)
select distinct
   pol.acc_date acc_date
,  'abis.plugin.asirom.dwh.DWHelperINSIS_GWP_Provisionally' helper
,  LPAD(trim(pol.codcomp), 3, '0') codcomp 
,  pol.policy_id
,  pol.currency
,  pol.premium acc_amnt 
-- ,  (pol.premium * (select getExchangeRate(pol.currency, pol.acc_date) from dual) ) acc_amnt_ron
,  pol.gl_local_amnt acc_amnt_ron
,  pol.premium op_amnt 
-- ,  (pol.premium * (select getExchangeRate(pol.currency, pol.acc_date) from dual) ) op_amnt_ron 
,  pol.gl_local_amnt op_amnt_ron
,  pol.insr_type
,  gf.fract_type insr_class
,  preminst_id install_id
,  pol.cover_type
,  io.object_id
,  pol.insured_obj_id
,  pol.staging_id pif_staging_id
,  pol.agent_id fgl_agent_id
,  ue.class_of_business
,  0 is_cancelled
from policy pol
left join (
    select gf.*
    ,  DENSE_RANK() OVER (PARTITION BY insr_type, cover_type ORDER BY rownum) rank_no
    from insis_cfg_gl_gen_fractions gf
    where gf.deleted_date is null
) gf
on gf.insr_type = pol.insr_type
and gf.cover_type = pol.cover_type
and gf.rank_no = 1
left join (
    select gf.*
    ,  DENSE_RANK() OVER (PARTITION BY insr_type, fract_type ORDER BY rownum) rank_no
    from insis_ueasi_cfg_class_of_bsn gf
    where gf.deleted_date is null
) ue
on ue.insr_type = pol.insr_type
and ue.fract_type = gf.fract_type
and ue.rank_no = 1
left join insis_insured_object io
on     io.insured_obj_id = pol.insured_obj_id
and    io.deleted_date is null



     