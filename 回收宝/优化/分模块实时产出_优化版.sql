with detect_one as (
select 
    fserial_number,
    case when to_date(from_unixtime(fend_det_time))='2024-03-04' and freal_name="陈冬凡" then "周远鸿" 
         when to_date(from_unixtime(fend_det_time))='2024-03-04' and freal_name="胡家华" then "黄成水"
         when to_date(from_unixtime(fend_det_time))='2024-03-02' and freal_name="陈冬凡" then "李浩宇"
         when to_date(from_unixtime(fend_det_time))='2024-03-05' and freal_name="陈冬凡" then "周远鸿"
         when fbind_real_name is not null or fbind_real_name!="" then fbind_real_name
    else freal_name  end as fdetect_one_name,
    from_unixtime(fend_det_time) as fdetect_one_time,
    fbrand_name
from (
select 
    a.fserial_number,
    a.fbind_real_name,
    a.fend_det_time,
    a.fbrand_name,
    case when a.fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" else b.freal_name end as freal_name,
    row_number()over(partition by upper(a.fserial_number) order by a.fend_det_time desc) as num
from drt.drt_my33312_detection_t_automation_det_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
where fserial_number!=""
and fserial_number is not null
and to_date(from_unixtime(a.fend_det_time))=to_date(date_sub(from_unixtime(unix_timestamp()),0))
)t
where num=1
),
detect_two as (
select 
    fserial_number,
    case when fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" 
         when to_date(fcreate_time)='2024-03-04' and freal_name="陈冬凡" then "周远鸿" 
         when to_date(fcreate_time)='2024-03-04' and freal_name="胡家华" then "黄成水"
    else freal_name end as fdetect_two_name,
    fcreate_time as fdetect_two_time,
    fbrand_name
from (
select 
    a.fcreate_time,
    a.fuser_name,
    a.fserial_number,
    b.freal_name,
    a.fbrand_name,
    row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
from drt.drt_my33312_detection_t_det_app_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
where to_date(a.fcreate_time)=to_date(date_sub(from_unixtime(unix_timestamp()),0))
and fserial_number!=""
and fserial_number is not null)t
where num=1
),
detect_three as (
select 
    fserial_number,
    freal_name as fdetect_three_name,
    fcreate_time as fdetect_three_time,
    fbrand_name
from (
    select 
        a.fcreate_time,
        a.fserial_number,
        b.freal_name,
        a.fbrand_name,
        b.fdet_sop_task_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    where to_date(a.fend_time)=to_date(date_sub(from_unixtime(unix_timestamp()),0))
    and b.fdet_sop_task_name like "%外观%")t
where num=1
),
detect_four as (
select 
    fserial_number,
    freal_name as fdetect_four_name,
    fend_time as fdetect_four_time,
    fbrand_name
from (
    select 
        a.fend_time,
        a.fserial_number,
        b.freal_name,
        a.fbrand_name,
        b.fdet_sop_task_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    where to_date(a.fend_time)=to_date(date_sub(from_unixtime(unix_timestamp()),0))
    and b.fdet_sop_task_name like "%拆修%")t
where num=1
),
detail as (
select 
    to_date(fdetect_one_time) as fdate,
    hour(fdetect_one_time) as fhour,
    case when left(fserial_number,1)="0" then "验机" 
         when left(fserial_number,2)='BM' then "寄卖" 
         when (left(fserial_number,2)="CG" and fdetect_one_time>='2024-12-01') or left(fserial_number,2)="TL" then "太力"
         else "回收" end as ftype,
    case when left(fserial_number,2) in ('02') or right(left(fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    "模块一" as fdet_type,
    case when fbrand_name="苹果" then "苹果" else "安卓" end as fbrand,
    count(distinct fserial_number) as fcnt
from detect_one
group by 1,2,3,4,5,6
union all
select 
    to_date(fdetect_two_time) as fdate,
    hour(fdetect_two_time) as fhour,
    case when left(fserial_number,1)="0" then "验机" 
         when left(fserial_number,2)='BM' then "寄卖" 
         when (left(fserial_number,2)="CG" and fdetect_two_time>='2024-12-01') or left(fserial_number,2)="TL" then "太力"
         else "回收" end as ftype,
    case when left(fserial_number,2) in ('02') or right(left(fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    "模块二" as fdet_type,
    case when fbrand_name="Apple" then "苹果" else "安卓" end as fbrand,
    count(distinct fserial_number) as fcnt
from detect_two
group by 1,2,3,4,5,6
union all
select 
    to_date(fdetect_three_time) as fdate,
    hour(fdetect_three_time) as fhour,
    case when left(fserial_number,1)="0" then "验机" 
         when left(fserial_number,2)='BM' then "寄卖" 
         when (left(fserial_number,2)="CG" and fdetect_three_time>='2024-12-01') or left(fserial_number,2)="TL" then "太力"
         else "回收" end as ftype,
    case when left(fserial_number,2) in ('02') or right(left(fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    "模块三" as fdet_type,
    case when fbrand_name="苹果" then "苹果" else "安卓" end as fbrand,
    count(distinct fserial_number) as fcnt
from detect_three
group by 1,2,3,4,5,6
union all
select 
    to_date(fdetect_four_time) as fdate,
    hour(fdetect_four_time) as fhour,
    case when left(fserial_number,1)="0" then "验机" 
         when left(fserial_number,2)='BM' then "寄卖" 
         when (left(fserial_number,2)="CG" and fdetect_four_time>='2024-12-01') or left(fserial_number,2)="TL" then "太力"
         else "回收" end as ftype,
    case when left(fserial_number,2) in ('02') or right(left(fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    "模块四" as fdet_type,
    case when fbrand_name="苹果" then "苹果" else "安卓" end as fbrand,
    count(distinct fserial_number) as fcnt
from detect_four
group by 1,2,3,4,5,6
)
select 
    fdate,
    ftype,
    fwarehouse,
    fdet_type,
    sum(fcnt) as total_cnt
from detail
where fhour <= hour(current_timestamp())
group by 1,2,3,4
order by 
    fwarehouse,
    fdate,
    case when fdet_type="模块一" then 1
         when fdet_type="模块二" then 2
         when fdet_type="模块三" then 3
         when fdet_type="模块四" then 4
         else 99 end,
    ftype
;
