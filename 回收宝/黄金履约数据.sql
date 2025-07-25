with confirm as (
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
shouhuo as (
select 
    forder_id,
    fauto_create_time,
    foperator_name
from (
select 
    a.forder_id,
    a.fauto_create_time,
    c.freal_name as foperator_name,
    a.forder_status,
    row_number()over(partition by a.forder_id order by a.fauto_create_time asc) as num
from drt.drt_my33310_recycle_t_order_txn as a
left join dws.dws_hs_order_detail as b on a.forder_id=b.forder_id
left join drt.drt_my33310_amcdb_t_user as c on a.foperator_name=c.fusername
where a.forder_status=40
and a.fauto_create_time>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
and b.fclass_name="黄金"
)t
where num=1
),
xieyi as (
select 
    fcheck_time,
    case when fstatus=1 then "待签"
         when fstatus=2 then "已签待审"
         when fstatus=3 then "证件不通过"
         when fstatus=4 then "签字不通过"
         when fstatus=5 then "发票不通过"
         when fstatus=6 then "通过"
    else null end as fstatus,
    forder_id,
    Fauditor_user_name
from (
    select 
        *,
        row_number()over(partition by forder_id order by fcheck_time desc) as num
    from drt.drt_my33312_detection_t_gold_agreement 
    where fcheck_time>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
)t where num=1
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
    ftask_goods_id,
    fupdate_user_name,
    fcreate_time,
    "样品确认" as ftype
from confirm
union all
select 
    ftask_goods_id,
    fupdate_user_name,
    fcreate_time,
    "初检" as ftype
from first_detect
union all
select 
    ftask_goods_id,
    fupdate_user_name,
    fcreate_time,
    "点测" as ftype
from metering
union all
select 
    ftask_goods_id,
    fupdate_user_name,
    fcreate_time,
    "剪破" as ftype
from shear 
union all
select 
    ftask_goods_id,
    fupdate_user_name,
    fcreate_time,
    "熔炼" as ftype 
from smelt
union all
select 
    ftask_goods_id,
    fupdate_user_name,
    fcreate_time,
    "称重" as ftype
from weigh
union all
select 
    forder_id as ftask_goods_id,
    foperator_name  as fupdate_user_name,
    fauto_create_time as fcreate_time,
    "收货" as ftype
from shouhuo
union all
select 
    forder_id as ftask_goods_id,
    Fauditor_user_name  as fupdate_user_name,
    fcheck_time as fcreate_time,
    "协议审核" as ftype
from xieyi
