 select install_id
 ,      acc_date
 ,      staging_id gwp_staging_id
 ,      change_date
 ,      helper
 ,      policy_id
from gwp
where change_date >= to_date(':crtKey', 'yyyy-mm-dd hh24:mi:ss. ') 
-- and cover_type not in ('MI1')
and nvl(cover_type, '-') not in ('MI1') -- 6 iun 2023
and policy_id is not null
-- and acc_date >= '1-may-2023'
-- and install_id = 5697268638

order by change_date
,   install_id
-- and helper = 'abis.plugin.asirom.dwh.DWHelperCamelot_GWP'


