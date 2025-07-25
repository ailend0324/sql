with qianshou as (
select 
    *
from (
    select 
        *,
        row_number()over(partition by Flogistics_number order by fauto_create_time desc) as num
    from drt.drt_my33310_hsb_wms_t_sh_sign_log
    where to_date(fauto_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
    )t
where t.num=1
)

select 
    b.forder_id,
    b.fproduct_name,
    f.Fdeliver_province,
    f.Fdeliver_city,
    case when right(left(b.fseries_number,6),4)='0112' then "东莞仓" 
    	 when  right(left(b.fseries_number,6),2)="16" then "杭州仓"
    else "深圳仓" end as fwarehouse,
    case when e.fchannel_name like "%寄卖%" then "寄卖" else "回收" end as ftype,
    a.fauto_create_time as ffahuo_time,
    from_unixtime(d.Fadd_time) as fsign_time,
    (unix_timestamp(from_unixtime(d.Fadd_time),'yyyy-MM-dd HH:mm:ss')-unix_timestamp(a.fauto_create_time,'yyyy-MM-dd HH:mm:ss'))/3600/24 as fshixiao
from drt.drt_my33310_recycle_t_order_txn as a
left join drt.drt_my33310_recycle_t_order as b on a.forder_id=b.forder_id
left join drt.drt_my33310_recycle_t_logistics as c on b.flogistics_id=c.flogistics_id
left join qianshou as d on c.fchannel_id=d.Flogistics_number
left join drt.drt_my33310_recycle_t_channel as e on b.fchannel_id=e.fchannel_id
left join drt.drt_my33310_recycle_t_tms_logistics_recycle as f on a.forder_id=cast(f.forder_id as int)
where a.forder_status=20
and to_date(a.fauto_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and from_unixtime(d.Fadd_time) is not null
and b.ftest=0 
and b.frecycle_type=1
and b.fproduct_name not like "%清库存%"
and b.fproduct_name not like "%无效%"
and from_unixtime(d.Fadd_time)>a.fauto_create_time
union
select 
    b.forder_id,
    b.fhsb_product_name as fproduct_name,
    b.fseller_province_name as Fdeliver_province,
    b.fseller_city_name as Fdeliver_city,
    case when left(b.fhost_barcode,3)='020' then "杭州仓"
         when left(b.fhost_barcode,3)='050' then "东莞仓"
    else "深圳仓" end as fwarehouse,
    "验货宝" as ftype,
    a.fauto_create_time as ffahuo_time,
    b.fsigh_time,
    (unix_timestamp(b.fsigh_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(a.fauto_create_time,'yyyy-MM-dd HH:mm:ss'))/3600/24 as fshixiao
from drt.drt_my33315_xy_detect_t_xy_yhb_order_txn as a
left join dws.dws_xy_yhb_detail as b on a.forder_id=b.forder_id
where a.forder_status=15
and to_date(a.fauto_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and b.fsigh_time is not null
and b.fsigh_time>a.fauto_create_time
union
select 
    cast(fsales_order_num as int) as forder_id,
    fproduct_name,
    fprovince_name as Fdeliver_province,
    fcity_name as Fdeliver_city,
    "武汉小站" as fwarehouse,
    "小站异地上拍" as ftype,
    fsend_time as ffahuo_time,
    fdelivery_time as fsigh_time,
    (unix_timestamp(fdelivery_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(fsend_time,'yyyy-MM-dd HH:mm:ss'))/3600/24 as fshixiao
from dws.dws_jp_order_detail
where left(fseries_number,2)='JM'
and Fmerchant_jp=1
and to_date(fsend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and fshop_name like "%武汉%"  
and ((unix_timestamp(fdelivery_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(fsend_time,'yyyy-MM-dd HH:mm:ss'))/3600/24)<10

