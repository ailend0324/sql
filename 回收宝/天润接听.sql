select 
    fdate,
    case when fivr_name like "%验货担保%" or fivr_name like "%验机%" then "验机" 
         when fivr_name like "%寄卖%" then "寄卖"
    else "回收" end as "渠道",
    count(funique_id)
from ods.ods_kf_tianrun_describe_cdr_ib
where fdate>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
and fstatus='座席接听'
group by 1,2




