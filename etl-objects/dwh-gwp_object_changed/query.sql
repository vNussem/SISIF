select *
from (
    select  to_char(acc_date, 'dd-mon-yyyy') acc_date_s
    ,       acc_date
    ,       insr_type
    ,       codcomp
    ,       max(change_date) change_date
    from (
        select greatest(greatest(g.inserted_date, nvl(g.deleted_date, '1-jan-1900')), nvl(g.change_date, '1-jan-1900')) change_date
        ,      g.acc_date
        ,      g.insr_type
        ,      g.codcomp
        from gwp_object g
        where greatest(greatest(inserted_date, nvl(g.deleted_date, '1-jan-1900')), nvl(g.change_date, '1-jan-1900')) >= to_date(':crtKey', 'yyyy-mm-dd hh24:mi:ss. ')        
        -- and acc_date = '15-MAY-23' and (insr_type, codcomp) in( (6902, 360)) 
        -- and acc_date = '15-APR-23' and (insr_type, codcomp) in( (7100,	530)) 
    )    
    group by acc_date
    ,        insr_type
    ,        codcomp
) 
order by change_date