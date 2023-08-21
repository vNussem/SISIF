select  iio.object_id
,	r.*
from    insis_gen_reserve_corp r		
left join insis_insured_object iio
on      iio.insured_obj_id = r.insured_obj_id
and     iio.deleted_date is null
where  (
        r.dwh_inserted is null 
        or     (
                    r.deleted_date is not null 
                and r.dwh_deleted is null
        )
) 