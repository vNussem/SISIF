with acc_gwp as (
        select max(acc_date) acc_date
        ,      helper
        ,      codcomp
        ,      fgl_office_id
        ,      max(fgl_agent_id) fgl_agent_id -- 02.06.2023
        ,      is_cancelled
        ,      install_id
        ,      policy_id    
        ,      insr_type
        ,      cover_type
        ,      sum(premium) amnt
        ,      currency
        ,      sum(ron_premium) amnt_ron    
        ,      max(insr_class) insr_class -- 02.06.2023
        ,      class_of_business
        -- ,      case when count(*) < 50 then LISTAGG(staging_id, ',') WITHIN GROUP (ORDER BY staging_id) else ' ' end staging_id
        ,      to_char(substr(rtrim(xmlagg(xmlelement(e, staging_id, ',').extract('//text()')  order by staging_id).getclobval(),', '), 0, 3999)) staging_id
        -- ,      rtrim(xmlagg(xmlelement(e, staging_id, ',').extract('//text()')  order by object_id).getclobval(),', ') staging_id
        ,      max(change_date) change_date
        from gwp        
        where install_id in ({0})        
        --and change_date is not null
        group by install_id
        ,        policy_id
        ,      insr_type
        ,      cover_type
        ,      currency
        ,      codcomp
        ,      fgl_office_id
        -- ,      fgl_agent_id
        ,      is_cancelled
        ,      helper
        -- ,      insr_class
        ,      class_of_business
        ,      trunc(acc_date, 'month')        
    )    
    , pif as (        
            select pif.preminst_id
            ,      pif.preminst_ref preminst_ref_id
            ,      pif.policy_id
            ,      pif.cover_type
            ,      pif.insr_type
            ,      pif.insr_class
            ,      pif.risk_amnt
            ,      round(pif.risk_amnt * pifr.currency_rate, 2) gl_local_amnt
            ,      pif.account_type
            ,      pif.insured_obj_id
            ,      pif.object_id
            ,      pif.annex_id
            ,      pif.aux_annex_id
            ,      pif.aux_annex_state
            ,      pif.preminst_ref
            ,      pif.staging_id
            
            ,      pif.helper
            ,      pif.period_id
            ,      pif.period_id_reverse
            
           
            from insis_prem_inst_fract pif 
            join (
                select preminst_id
                ,      insured_obj_id
                ,      cover_type
                ,     round(sum(gl_local_amnt) / sum(risk_amnt), 4)  currency_rate
                from  insis_prem_inst_fract
                group by preminst_id
                ,      insured_obj_id
                ,      cover_type
                having nvl(max(risk_amnt), 0) <> 0
            ) pifr
            on pifr.preminst_id = pif.preminst_ref
            and pifr.insured_obj_id = pif.insured_obj_id
            and pifr.cover_type = pif.cover_type        
            
            where pif.preminst_ref is not null                 
            and (pif.helper = 'abis.plugin.asirom.dwh.DWHelperInsis_PremInstFract' or pif.period_id <> pif.period_id_reverse)
            
            -- and  pif.preminst_id in (5697106750, 5697109296, 5697106748, 5697106749)
            and pif.preminst_id in ({0})       

            union all
            select pif.preminst_id
            ,      null preminst_ref_id
            ,      pif.policy_id
            ,      pif.cover_type
            ,      pif.insr_type
            ,      pif.insr_class
            ,      pif.risk_amnt
            ,      round(pif.gl_local_amnt, 2) gl_local_amnt
            ,      pif.account_type
            ,      pif.insured_obj_id
            ,      pif.object_id
            ,      pif.annex_id
            ,      pif.aux_annex_id
            ,      pif.aux_annex_state
            ,      pif.preminst_ref
            ,      pif.staging_id            
            ,      pif.helper
            ,      pif.period_id
            ,      pif.period_id_reverse            
            from (
                select pif.*
                from insis_prem_inst_fract pif
                left join insis_periods ps
                on ps.period_id = pif.period_id 
                left join insis_periods pr
                on pr.period_id = pif.period_id_reverse
            )    pif                        
            where preminst_ref is null        
            and (pif.helper = 'abis.plugin.asirom.dwh.DWHelperInsis_PremInstFract' or pif.period_id <> pif.period_id_reverse)

            -- and  pif.preminst_id in (5697106750, 5697109296, 5697106748, 5697106749)
            and pif.preminst_id in ({0})       
    )    
    select g.acc_date
    ,      g.helper
    ,      nvl(p.staging_id, 0) pif_staging_id
    ,      g.staging_id gwp_staging_id
    ,      g.codcomp
    ,      g.fgl_office_id
    ,      g.fgl_agent_id
    ,      nvl(g.is_cancelled, 0) is_cancelled
    ,      g.policy_id
    ,      nvl(p.object_id, 0) object_id
    ,      nvl(p.insured_obj_id, 0) insured_obj_id
    ,      g.install_id
    ,      p.preminst_ref_id
    ,      g.insr_type    
    ,      g.cover_type   
    ,      nvl(case when g.helper = 'abis.plugin.asirom.dwh.DWHelperFGL_GWP' then p.insr_class else g.insr_class end, '-') insr_class
    ,      case when g.helper = 'abis.plugin.asirom.dwh.DWHelperFGL_GWP' then DENSE_RANK() OVER (PARTITION BY g.acc_date, g.install_id, g.insr_type, g.cover_type ORDER BY p.insured_obj_id, rownum) 
           else null end inst_rank
    ,      g.currency
    ,      case when p.preminst_id is null or DENSE_RANK() OVER (PARTITION BY g.acc_date, g.install_id, g.insr_type, g.cover_type ORDER BY p.insured_obj_id, rownum) = 1 then g.amnt else 0 end acc_amnt
    ,      case when p.preminst_id is null or DENSE_RANK() OVER (PARTITION BY g.acc_date, g.install_id, g.insr_type, g.cover_type ORDER BY p.insured_obj_id, rownum) = 1 then g.amnt_ron else 0 end acc_amnt_ron    
    ,      case when g.helper = 'abis.plugin.asirom.dwh.DWHelperFGL_GWP' then abs(p.risk_amnt) * sign(g.amnt) else g.amnt end op_amnt
    ,      case when g.helper = 'abis.plugin.asirom.dwh.DWHelperFGL_GWP' then abs(p.gl_local_amnt) * sign(g.amnt) else g.amnt_ron end op_amnt_ron    
    ,      class_of_business
    ,      g.change_date
    
    from acc_gwp g
    left join pif p
    on g.install_id = p.preminst_id
    and  g.insr_type = p.insr_type
    and  g.cover_type = p.cover_type

    -- where g.install_id = 5697205491 and g.cover_type = '2010_CI8' 

    order by g.change_date