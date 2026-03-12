select 
    to_date(funpack_time) as ftimeby,
    case 
        when right(left(fseries_number,6),4)="0112" then "东莞仓"
        when left(fseries_number,3) like "%020%" or right(left(fseries_number,6),2)="16"then "杭州仓"
    else "深圳仓" end as fwarehouse,
    Fcategory_name,
    concat(ftype,"拆包") as ftype,
    count(fseries_number) as num
from dws.dws_instock_details 
where fseries_number is not null
and funpack_time>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
group by 1,2,3,4
union all 
select 
    to_date(freceive_time) as ftimeby,
    case 
        when right(left(fseries_number,6),4)="0112" then "东莞仓"
        when left(fseries_number,3) like "%020%" or right(left(fseries_number,6),2)="16" then "杭州仓"
    else "深圳仓" end as fwarehouse,
    Fcategory_name,
    concat(ftype,"收货") as ftype,
    count(fseries_number) as num
from dws.dws_instock_details 
where fseries_number is not null
and freceive_time>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
group by 1,2,3,4
union all 
select 
    to_date(fmain_photo_time) as ftimeby,
    case 
        when right(left(fseries_number,6),4)="0112" then "东莞仓"
        when left(fseries_number,3) like "%020%" or right(left(fseries_number,6),2)="16" then "杭州仓"
    else "深圳仓" end as fwarehouse,
    Fcategory_name,
    concat(ftype,"拍照") as ftype,
    count(fseries_number) as num
from dws.dws_instock_details 
where fseries_number is not null
and fmain_photo_time>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
group by 1,2,3,4
union all 
select 
    to_date(fstock_in_time) as ftimeby,
    case 
        when right(left(fseries_number,6),4)="0112" then "东莞仓"
        when left(fseries_number,3) like "%020%" or  right(left(fseries_number,6),2)="16" then "杭州仓"
    else "深圳仓" end as fwarehouse,
    Fcategory_name,
    "入库" as ftype,
    count(fseries_number) as num
from dws.dws_instock_details 
where fseries_number is not null
and fstock_in_time>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
group by 1,2,3,4
union all 
select 
    to_date(fsale_out_time) as ftimeby,
    case 
        when right(left(fseries_number,6),4)="0112" then "东莞仓"
        when left(fseries_number,3) like "%020%" or  right(left(fseries_number,6),2)="16" then "杭州仓"
    else "深圳仓" end as fwarehouse,
    Fcategory_name,
    "销售出库" as ftype,
    count(fseries_number) as num
from dws.dws_instock_details 
where fseries_number is not null
and fsale_out_time>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
group by 1,2,3,4
union all 
select 
    to_date(freturn_out_time) as ftimeby,
    case 
        when right(left(fseries_number,6),4)="0112" then "东莞仓"
        when left(fseries_number,3) like "%020%" or  right(left(fseries_number,6),2)="16" then "杭州仓"
    else "深圳仓" end as fwarehouse,
    Fcategory_name,
    "退货出库" as ftype,
    count(fseries_number) as num
from dws.dws_instock_details 
where fseries_number is not null
and freturn_out_time>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
group by 1,2,3,4
