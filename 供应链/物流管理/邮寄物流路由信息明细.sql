select 
    *
from (
    select
        *,
        case when fop_code='54' then (unix_timestamp()-unix_timestamp(fcreate_time))/3600 else 0 end as ftime, 
  		now(),
        row_number() over(partition by fmailno order by faccept_time asc) as num
    from drt.drt_my33310_recycle_t_route_info_record
    where fop_code!='80' and fop_code!='8000'
    ) t
where num=1
and faccept_time>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and fremark not like "%不成功%"
and ftime<=36
