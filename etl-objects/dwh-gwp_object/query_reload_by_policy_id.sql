with acc_gwp as (
    select max(acc_date) acc_date
    ,      helper
    ,      codcomp
    ,      fgl_office_id
    ,      fgl_agent_id
    ,      is_cancelled
    ,      install_id
    ,      policy_id    
    ,      insr_type
    ,      cover_type
    ,      sum(premium) amnt
    ,      currency
    ,      sum(ron_premium) amnt_ron    
    ,      insr_class
    ,      class_of_business
    ,      LISTAGG(staging_id, ',') WITHIN GROUP (ORDER BY staging_id) staging_id
    ,      max(change_date) change_date
    from gwp        
    where (policy_id, helper) in ({0})        
    and change_date is not null
    group by install_id
    ,        policy_id
    ,      insr_type
    ,      cover_type
    ,      currency
    ,      codcomp
    ,      fgl_office_id
    ,      fgl_agent_id
    ,      is_cancelled
    ,      helper
    ,      insr_class
    ,      class_of_business
    ,      trunc(acc_date, 'month')        
)           
select g.acc_date
,      g.helper
,      0 pif_staging_id
,      g.staging_id gwp_staging_id
,      g.codcomp
,      g.fgl_office_id
,      g.fgl_agent_id
,      g.is_cancelled
,      g.policy_id
,      0 object_id
,      0 insured_obj_id
,      nvl(g.install_id, 0) install_id
,      0 preminst_ref_id
,      nvl(g.insr_type, 0) insr_type
,      nvl(g.cover_type, '-') cover_type
,      g.insr_class insr_class
,      1 inst_rank
,      g.currency
,      g.amnt acc_amnt
,      g.amnt_ron acc_amnt_ron    
,      g.amnt op_amnt
,      g.amnt_ron op_amnt_ron    
,      class_of_business
,      g.change_date
from acc_gwp g
order by g.change_date