select INSTALL_ID
,   STAGING_ID gwp_staging_id
,   change_date
from gwp
where change_date >= to_date(':crtKey', 'yyyy-mm-dd hh24:mi:ss. ')
order by change_date
