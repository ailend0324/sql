with shouhuo as (
select 
    forder_id,
    fauto_create_time,
    foperator_name
from (
select 
    *,
    row_number()over(partition by forder_id order by fauto_create_time asc) as num
from drt.drt_my33310_recycle_t_order_txn 
where forder_status=40
and fauto_create_time>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
)t
where num=1
),
xieyi as (
select 
    fcheck_time,
    fcreate_time,
    case when fstatus=1 then "待签"
         when fstatus=2 then "已签待审"
         when fstatus=3 then "证件不通过"
         when fstatus=4 then "签字不通过"
         when fstatus=5 then "发票不通过"
         when fstatus=6 then "通过"
    else null end as fstatus,
    forder_id
from (
    select 
        *,
        row_number()over(partition by forder_id order by fcreate_time desc) as num
    from drt.drt_my33312_detection_t_gold_agreement 
    where fcreate_time>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
)t where num=1
),
product_info as (
select 
    forder_id,
    ftotal_price,
    fgross_weight,
    fsuttle_weight,
    ftotal_service_charge,
    ftask_goods_id
from (
    select 
        a.forder_id,
        a.ftotal_price,
        a.fgross_weight,
        a.fsuttle_weight,
        a.ftotal_service_charge,
        b.ftask_goods_id, 
        row_number()over(partition by a.forder_id order by a.fcreate_time desc) as num 
    from drt.drt_my33312_detection_t_gold_task as a
    left join drt.drt_my33312_detection_t_gold_task_goods as b on a.ftask_id=b.ftask_id
    where a.fcreate_time>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
)t
where num=1
),
confirm as (
select 
    ftask_goods_id,
    fupdate_user_name,
    fcreate_time
from (
    select 
        *,
        row_number()over(partition by ftask_goods_id order by fcreate_time desc) as num
    from drt.drt_my33312_detection_t_gold_task_record_confirmation_sample
    where fcreate_time>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
)t
where num=1
),
first_detect as (
select 
    ftask_goods_id,
    fupdate_user_name,
    fcreate_time
from (
    select 
        *,
        row_number()over(partition by ftask_goods_id order by fcreate_time desc) as num
    from drt.drt_my33312_detection_t_gold_task_record_initial_survey 
    where fcreate_time>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
)t
where num=1
),
metering as (
select 
    ftask_goods_id,
    fupdate_user_name,
    fcreate_time
from (
    select 
        *,
        row_number()over(partition by ftask_goods_id order by fcreate_time desc) as num
    from drt.drt_my33312_detection_t_gold_task_record_metering 
    where fcreate_time>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
)t
where num=1
),
shear as (
select 
    ftask_goods_id,
    fupdate_user_name,
    fcreate_time
from (
    select 
        *,
        row_number()over(partition by ftask_goods_id order by fcreate_time desc) as num
    from drt.drt_my33312_detection_t_gold_task_record_shear 
    where fcreate_time>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
)t
where num=1
),
smelt as (
select 
    ftask_goods_id,
    fupdate_user_name,
    fcreate_time
from (
    select 
        *,
        row_number()over(partition by ftask_goods_id order by fcreate_time desc) as num
    from drt.drt_my33312_detection_t_gold_task_record_smelt 
    where fcreate_time>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
)t
where num=1
),
weigh as (
select 
    ftask_goods_id,
    fupdate_user_name,
    fcreate_time
from (
    select 
        *,
        row_number()over(partition by ftask_goods_id order by fcreate_time desc) as num
    from drt.drt_my33312_detection_t_gold_task_record_weigh 
    where fcreate_time>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
)t
where num=1
)
select 
    a.forder_time,
    a.forder_id,
    a.fseries_number, 
    f.fauto_create_time as fgetin_time,
    a.fcheck_end_time,
    a.fcancel_time,
    a.fpay_out_time,
    a.fpay_out_price/100 as fpay_out_price,
    a.foperation_price/100 as foperation_price,
    e.forder_status_name,
    a.fsend_back_time,
    g.fcheck_time,
    g.fstatus,
    h.ftotal_price,
    h.fgross_weight,
    h.fsuttle_weight,
    h.ftotal_service_charge,
    i.fcreate_time as fconfirm_time,
    j.fcreate_time as ffirst_detect_time,
    k.fcreate_time as fmetering_time,
    l.fcreate_time as fshear_time,
    m.fcreate_time as fsmelt_time,
    n.fcreate_time as fweigh_time,
    now()
from drt.drt_my33310_recycle_t_order as a
left join drt.drt_my33310_recycle_t_xy_order_data as b on a.forder_id=b.forder_id
left join drt.drt_my33310_recycle_t_product as c on a.fproduct_id=c.fproduct_id
left join drt.drt_my33310_recycle_t_pdt_class as d on c.fclass_id=d.fid
left join drt.drt_my33310_recycle_t_order_status as e on a.forder_status=e.forder_status_id
left join shouhuo as f on a.forder_id=f.forder_id
left join xieyi as g on a.forder_id=g.forder_id
left join product_info as h on a.forder_id=h.forder_id
left join confirm as i on h.ftask_goods_id=i.ftask_goods_id
left join first_detect as j on h.ftask_goods_id=j.ftask_goods_id
left join metering as k on h.ftask_goods_id=k.ftask_goods_id
left join shear as l on h.ftask_goods_id=l.ftask_goods_id
left join smelt as m on h.ftask_goods_id=m.ftask_goods_id
left join weigh as n on h.ftask_goods_id=n.ftask_goods_id
where a.ftest=0 
and a.forder_time>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
and d.fname="黄金"
and a.frecycle_type=1
