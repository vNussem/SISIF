    select m.acc_date
    ,      m.mis_class_id
    ,      m.codcomp
    ,      m.insr_class
    ,      m.insr_type
    ,      m.helper
    ,      nvl(p.distribution_channel, 0) distribution_channel
    ,      nvl(m.is_cancelled, 0) is_cancelled
    ,      nvl(p.is_fronting, 0) is_fronting
    ,      m.insured_man_comp
    ,      sum(nvl(m.op_amnt_ron, 0)) premium
    ,      nvl(p.is_leasing, 'N') is_leasing
    ,      nvl(p.LEASING_BID_ID, 0) LEASING_BID_ID
    ,      nvl(grc.is_renew, 0) is_renew
    ,      case when m.fgl_office_id = 5010003287 then 1
                    when m.fgl_office_id = 5010003288 then 2 
            else 0 end hq_team_id
    ,       m.not_booked
    from (
        select m.*
        ,      case when m.acc_date >= trunc(sysdate) and m.helper not in ('abis.plugin.asirom.dwh.DWHelperFGL_GWP', 'abis.plugin.asirom.dwh.DWHelperCamelot_GWP_Provisionally') then 1 else 0 end not_booked
        ,      (select get_insured_man_comp(m.helper, m.policy_Id, 0) from dual) insured_man_comp
        ,      (select getMIS_Class(m.acc_date, m.policy_id, m.INSR_CLASS, m.INSR_TYPE, m.cover_type, m.helper) from dual) mis_class_id
        from    gwp_object m
        where (acc_date, insr_type, codcomp) in ({0})
        and deleted_date is null
    ) m
    left join (
            select p.*
            ,      case when p.policy_type = 'Auction' then 2    
                    when p.policy_type like 'Leasing%' then 1
                    when p.is_leasing = 'Y' then 1
               else 0 end LEASING_BID_ID
            ,     1 prank
            ,      case when p.agent_type in (3, 12, 39, 40, 41, 42, 49, 65, 66) then 2 else 1 end distribution_channel
            from   policy p
        ) p
    on     m.policy_id = p.policy_id
    and    p.prank = 1
    left join (
                select policy_id
                ,       max(is_renew) is_renew
                ,       count(distinct object_id) policy_no_of_risks
                from (
                    select grc.policy_id
                    ,      grc.object_id
                    ,      grc.risk_state
                    ,      case when renewed_policy_id is null then 0 else 1 end is_renew
--                    ,       case when grc.insr_type in (2010, 2012) then DENSE_RANK() OVER (PARTITION BY grc.policy_id, grc.INSURED_OBJ_ID ORDER BY grc.annex_id desc) 
--                             else DENSE_RANK() OVER (PARTITION BY grc.policy_id ORDER BY grc.annex_id desc) 
--                           end annex_rank
                    from insis_gen_risk_covered grc
                    where policy_id in (
                        select policy_id 
                        from gwp_object 
                        where (acc_date, insr_type, codcomp) in ({0})
                    )
                ) grc
                where grc.risk_state in (0, 11, 12, 112)
                group by policy_id
    ) grc
    on grc.policy_id = m.policy_id   
    group by m.acc_date
    ,      m.codcomp
    ,      m.insr_class
    ,      m.insr_type
    ,      m.helper
    ,      nvl(m.is_cancelled, 0)
    ,      m.insured_man_comp
    ,      m.mis_class_id
    ,      nvl(p.is_leasing, 'N')
    ,      nvl(p.distribution_channel, 0)
    ,      nvl(p.is_fronting, 0) 
    ,      nvl(p.LEASING_BID_ID, 0)
    ,      nvl(grc.is_renew, 0)
    -- ,      m.fgl_office_id
    ,      case when m.fgl_office_id = 5010003287 then 1
                    when m.fgl_office_id = 5010003288 then 2
            else 0 end 
    ,      m.not_booked