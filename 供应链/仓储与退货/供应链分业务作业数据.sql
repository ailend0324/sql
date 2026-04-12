with seal_bag as (
select 
    *
from (
    select 
        a.fseries_number,
        a.fwarehouse_name,
        a.fadd_time,
        b.freal_name,
        row_number()over(partition by fseries_number order by fadd_time desc) as num
    from drt.drt_my33310_hsb_wms_t_seal_bag_log as a
    left join drt.drt_my33310_amcdb_t_user as b on a.fadd_user=b.fusername
)t where num=1
),
allot as (
select 
    *
from (
    select 
        *,
        row_number()over(partition by fbar_code order by fadd_time desc) as num
    from drt.drt_my33310_xywms_t_product_allot
)t where num=1
)
select 
    funpack_time as ftimeby,
    case when left(fseries_number,3) like "%010%" then "深圳仓"
         when left(fseries_number,3) like "%020%" or right(left(fseries_number,6),2)="16" then "杭州仓"
         when left(fseries_number,3) like "%050%" then "东莞仓"
         when right(left(fseries_number,6),4)="0112" then "东莞仓"
    else "深圳仓" end as "所在地",
    case
        when LEFT(fseries_number,2)="BM" then "寄卖"
    else ftype end as channel,
    count(fseries_number) as num
from dws.dws_instock_details
where fseries_number is not null
and funpack_time>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
and Funpack_user is not null
group by 1,2,3
union all 
select 
    freceive_time as ftimeby,
    case when left(fseries_number,3) like "%010%" then "深圳仓"
         when left(fseries_number,3) like "%020%" or right(left(fseries_number,6),2)="16" then "杭州仓"
         when left(fseries_number,3) like "%050%" then "东莞仓"
         when right(left(fseries_number,6),4)="0112" then "东莞仓"
    else "深圳仓" end as "所在地",
    case
        when LEFT(fseries_number,2)="BM" then "寄卖"
    else ftype end as channel,
    count(fseries_number) as num
from dws.dws_instock_details
where fseries_number is not null
and freceive_time>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
and freceive_user is not null
group by 1,2,3
union all 
select 
    fmain_photo_time as ftimeby,
    case when left(fseries_number,3) like "%010%" then "深圳仓"
         when left(fseries_number,3) like "%020%" or right(left(fseries_number,6),2)="16" then "杭州仓"
         when left(fseries_number,3) like "%050%" then "东莞仓"
         when right(left(fseries_number,6),4)="0112" then "东莞仓"
    else "深圳仓" end as "所在地",
    case
        when LEFT(fseries_number,2)="BM" then "寄卖"
    else ftype end as channel,
    count(fseries_number) as num
from dws.dws_instock_details
where fseries_number is not null
and fmain_photo_time>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
and fphoto_name is not null
group by 1,2,3
union all 
select 
    Ftamper_time as ftimeby,
    case when left(fseries_number,3) like "%010%" then "深圳仓"
         when left(fseries_number,3) like "%020%" or right(left(fseries_number,6),2)="16" then "杭州仓"
         when left(fseries_number,3) like "%050%" then "东莞仓"
         when right(left(fseries_number,6),4)="0112" then "东莞仓"
    else "深圳仓" end as "所在地",
    "寄卖" as channel,
    count(fseries_number) as num
from dws.dws_instock_details
where fseries_number is not null
and Ftamper_time>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
and Foperator is not null
and LEFT(fseries_number,2)='BM'
group by 1,2,3
union all
select 
    fplus_print_time as ftimeby,
    case when left(fseries_number,3) like "%010%" then "深圳仓"
         when left(fseries_number,3) like "%020%" or right(left(fseries_number,6),2)="16" then "杭州仓"
         when left(fseries_number,3) like "%050%" then "东莞仓"
         when right(left(fseries_number,6),4)="0112" then "东莞仓"
    else "深圳仓" end as "所在地",
    "寄卖" as channel,
    count(fseries_number) as num
from dws.dws_instock_details
where fseries_number is not null
and fplus_print_time>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
and fplus_printer is not null
group by 1,2,3
union all
select 
    fadd_time as ftimeby,
    case when left(fbar_code,3) like "%010%" then "深圳仓"
         when left(fbar_code,3) like "%020%" or right(left(fbar_code,6),2)="16" then "杭州仓"
         when left(fbar_code,3) like "%050%" then "东莞仓"
    else null end as "所在地",
    "验机" as channel,
    count(fbar_code) as num
from allot
where fadd_time>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
and fbar_code is not null
and fadd_user is not null
group by 1,2,3
union all
select 
    fupdate_time as ftimeby,
    case when left(fbar_code,3) like "%010%" then "深圳仓"
         when left(fbar_code,3) like "%020%" or right(left(fbar_code,6),2)="16" then "杭州仓"
         when left(fbar_code,3) like "%050%" then "东莞仓"
    else null end as "所在地",
    "验机" as channel,
    count(fbar_code) as num
from allot
where fupdate_time>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
and fbar_code is not null
and fupdate_user is not null
group by 1,2,3
