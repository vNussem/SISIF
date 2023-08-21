with policy as (
    select distinct 
        ver.NUM_CONTRACT NrContract
    ,  ver.COD_STATE  Stare
    ,  vp.COD_ZW mod_plata
    ,  vp.COD_ZW
--    ,  vp.BET_NETTO_ZW prima_fre
    ,  ver.BET_BUCHUNGSSATZSUMME prima_fre
    ,  ver.DAT_BEGIN
    ,  ver.DAT_END
    ,  ver.COD_PRODUKT   
    ,  ver.data_aniv
    from (
        select  buc.BET_BUCHUNGSSATZSUMME 
        ,       buc.DAT_VORGESCHR_VON data_aniv
        ,       ver.*
        from camelot_ver_general ver
        join camelot_cash_buchung buc
        on buc.DOM_BETRAGSART = 'EEP'   
        and buc.num_vertrag = ver.num_contract
        and buc.deleted_date is null
        where ver.num_contract in ({0})
    ) ver
    LEFT join camelot_ver_premium vp 
    on ver.NUM_CONTRACT = vp.NUM_CONTRACT
    and vp.deleted_date is null
    left join camelot_portasproductmapping pm
    on pm.camelotcod = ver.cod_produkt
    and pm.deleted_date is null
    where trunc(data_aniv, 'month') > (select max(to_date(year || '-' || month || '-1', 'yyyy-mm-dd')) from insis_system_periods where status = 'CLOSE')
    or pm.portascod = '198CAM'    
),
agents as (
    select 	  pa.agent_type
                ,         pa.agent_no marcasap
                ,       CASE
                                WHEN nvl(pos.office_no, po.office_no) < 100 THEN TO_CHAR(nvl(pos.office_no, po.office_no), '099')
                                WHEN nvl(pos.office_no, po.office_no) >= 100 THEN TO_CHAR(nvl(pos.office_no, po.office_no), '999')
                                ELSE '0'
                            END                                                                                 CODCOMP
                from      insis_p_agents pa
                LEFT JOIN insis_P_OFFICES PO 
                ON        PO.OFFICE_ID = PA.OFFICE_ID
                LEFT JOIN insis_P_OFFICES POS 
                on        POS.OFFICE_ID = PO.PARENT_OFFICE
                where      pa.deleted_date is null
)
select distinct
   pol.data_aniv acc_date
,  'sisif.dwh-gwp_object-MaxInvest-top-up' helper
,  LPAD(trim(ag.codcomp), 3, '0') codcomp 
-- ,  cast(nrcontract as number default 0 on conversion error) policy_id
,   COALESCE(TO_NUMBER(REGEXP_SUBSTR(nrcontract, '^\d+(\.\d+)?')), 0) policy_id
,  'RON' currency
,  case when cod_zw = 99 then prima_fre else prima_fre * cod_zw end acc_amnt
,  case when cod_zw = 99 then prima_fre else prima_fre * cod_zw end acc_amnt_ron
,  case when cod_zw = 99 then prima_fre else prima_fre * cod_zw end op_amnt
,  case when cod_zw = 99 then prima_fre else prima_fre * cod_zw end op_amnt_ron 
-- ,  ag.agent_type
,  decode(
cod_produkt
, 'O1U', 1
, 'IP1U', 1
, 'O2U', 1
,  0
) insr_type
, cspl.productclass insr_class
, 0 install_id
, cod_produkt cover_type
, 0 object_id
, 0 insured_obj_id
, 0 pif_staging_id
, i_pa.agent_id fgl_agent_id
, cspl.productsaplob class_of_business
, 0 is_cancelled
from policy pol
left join camelot_VER_DISAG_DETAIL vd
ON pol.NrContract = vd.NUM_CONTRACT
and vd.deleted_date is null 
left join camelot_WERBER_FREMDNUMMER wf
ON wf.NUM_MASSCHEMA = vd.NUM_MASSCHEMA 
and wf.COD_FREMD = 'SAPID' 
left join camelot_portasproductmapping map
on map.camelotCod = pol.cod_produkt
join agents ag
on ag.marcasap = NVL(wf.NUM_FREMD, '')
left join camelot_sapproductlob cspl
on cspl.productcode = cod_produkt
and cspl.deleted_date is null
left join insis_p_agents i_pa
on i_pa.agent_no = ag.marcasap
and i_pa.deleted_date is null
where pol.Stare = 'AC' 
AND   pol.prima_fre <> 0
order by policy_id
, acc_date



     