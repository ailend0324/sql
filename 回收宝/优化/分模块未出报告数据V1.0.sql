with cfg as (
select 365 as days -- 可配置的时间范围（天）
),
detect_one as (
select 
    upper(fserial_number) as fserial_number,
    case when to_date(from_unixtime(fend_det_time))='2025-01-26' and freal_name="黄雅如" then "兼职"
  	when fbind_real_name is not null and fbind_real_name!="" then fbind_real_name
    else freal_name end as fdetect_one_name,
    from_unixtime(fend_det_time) as fdetect_one_time,
    fbrand_name,
    fclass_name,
    case when left(fserial_number,1)='0' then "验机" else "回收" end as ftype
from (
select 
    a.fserial_number,
  	a.fend_det_time,
  	a.fbrand_name,
  	a.fbind_real_name,
    d.fclass_name as fclass_name,
    case when a.fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" else b.freal_name end as freal_name,
    row_number()over(partition by upper(a.fserial_number) order by a.fend_det_time desc) as num
from drt.drt_my33312_detection_t_automation_det_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
left join (
        select 
            *
        from drt.drt_my33310_detection_t_detect_record 
        where fdet_type=0
        and fis_deleted=0
      	and freport_type=0
    	and fverdict<>"测试单"
        and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),(select days from cfg)))
        ) as c on upper(a.fserial_number)=upper(c.fserial_number)
left join dws.dws_hs_order_detail as d on upper(a.fserial_number)=upper(d.fseries_number)
where a.fserial_number!=""
and a.fserial_number is not null
and c.freal_name is null
and to_date(from_unixtime(a.fend_det_time))>=to_date(date_sub(from_unixtime(unix_timestamp()),(select days from cfg))))t
where num=1
--and to_date(fcreate_time) between '2024-03-01' and '2024-03-31'
),

detect_two as (
select 
    upper(fserial_number) as fserial_number,
    case when fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" 
         when to_date(fcreate_time) BETWEEN '2025-01-24' and '2025-01-26' and freal_name="黄雅如" then "兼职"
    else freal_name end as fdetect_two_name,
    fcreate_time as fdetect_two_time,
    fbrand_name,
    fclass_name,
    case when fproduct_name in (
    '三星 Galaxy Fold',
    '三星 Galaxy Fold（5G）',
    '三星 W20（5G）',
    '华为 Mate Xs（5G）',
    '三星 Galaxy Z  Fold2（5G）',
    '三星 W21（5G）',
    '小米 MIX FOLD（5G）',
    '三星 Galaxy Z Fold3（5G）',
    '三星 W22（5G）',
    '荣耀 Magic V（5G）',
    'vivo X Fold（5G）',
    '小米 MIX FOLD2（5G）',
    '三星 Galaxy Z Fold4（5G）',
    'vivo X Fold+（5G）',
    '三星 W23（5G）',
    '荣耀 Magic Vs（5G）',
    '荣耀 Magic Vs 至臻版',
    '华为 Mate X3',
    '华为 Mate X2 典藏版',
    '华为 Mate X3 典藏版',
    'vivo X Fold 2（5G）',
    '谷歌 Pixel Fold',
    '荣耀 Magic V2（5G）',
    '荣耀 Magic V2 至臻版',
    '三星 Galaxy Z Fold5（5G）',
    '小米 MIX FOLD3（5G）',
    '华为 Mate X5',
    '华为 Mate X5 典藏版',
    '三星 W24（5G）',
    '荣耀 Magic Vs 2（5G）',
    'OPPO Find N3（5G）',
    '荣耀 Magic V2 RSR 保时捷设计',
    'vivo X Fold 3',
    'vivo X Fold3 Pro'
    ) then "是" else "否" end as fzhedie,
    case when left(fserial_number,1)='0' then "验机" else "回收" end as ftype
from (
select 
    a.fcreate_time,
    a.fuser_name,
    a.fserial_number,
    d.fproduct_name,
    a.fbrand_name,
    d.fclass_name,
    b.freal_name,
    row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
from drt.drt_my33312_detection_t_det_app_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
left join (
        select 
            *
        from drt.drt_my33310_detection_t_detect_record 
        where fdet_type=0
        and fis_deleted=0
      	and freport_type=0
    	and fverdict<>"测试单"
        and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),(select days from cfg)))
        ) as c on upper(a.fserial_number)=upper(c.fserial_number)
left join dws.dws_hs_order_detail as d on a.fserial_number=d.fseries_number
where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),(select days from cfg)))
and a.fserial_number!=""
and a.fserial_number is not null
and c.freal_name is null)t
where num=1
--and to_date(fcreate_time) between '2024-03-01' and '2024-03-31'
),

detect_three as (
select 
    upper(fserial_number) as fserial_number,
    freal_name as fdetect_three_name,
    fcreate_time as fdetect_three_time,
    fbrand_name,
    fclass_name,
    case when left(fserial_number,1)='0' then "验机" else "回收" end as ftype
from (
    select 
        a.fcreate_time,
        a.fserial_number,
        a.fbrand_name,
        d.fclass_name,
        b.freal_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    left join (
        select 
            *
        from drt.drt_my33310_detection_t_detect_record 
        where fdet_type=0
        and fis_deleted=0
      	and freport_type=0
    	and fverdict<>"测试单"
        and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),(select days from cfg)))
        ) as c on upper(a.fserial_number)=upper(c.fserial_number)
    left join dws.dws_hs_order_detail as d on upper(a.fserial_number)=upper(d.fseries_number)
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),(select days from cfg)))
    and b.fdet_sop_task_name like "%外观%"
    and c.freal_name is null)t
where num=1
--and to_date(fcreate_time) between '2024-03-01' and '2024-03-31'
),

detect_three_pingmu as (
select 
    upper(fserial_number) as fserial_number,
    freal_name as fdetect_three_name,
    fcreate_time as fdetect_three_time,
    fbrand_name,
    fclass_name,
    case when left(fserial_number,1)='0' then "验机" else "回收" end as ftype
from (
    select 
        a.fcreate_time,
        a.fserial_number,
        a.fbrand_name,
        d.fclass_name,
        b.freal_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    left join (
        select 
            *
        from drt.drt_my33310_detection_t_detect_record 
        where fdet_type=0
        and fis_deleted=0
      	and freport_type=0
    	and fverdict<>"测试单"
        and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),(select days from cfg)))
        ) as c on upper(a.fserial_number)=upper(c.fserial_number)
    left join dws.dws_hs_order_detail as d on upper(a.fserial_number)=upper(d.fseries_number)
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),(select days from cfg)))
    and b.fdet_sop_task_name like "%屏幕%"
    and b.fdet_sop_task_name!="外观屏幕"
    and c.freal_name is null)t
where num=1
--and to_date(fcreate_time) between '2024-03-01' and '2024-03-31'
),

detect_four as (
select 
    upper(fserial_number) as fserial_number,
    freal_name as fdetect_four_name,
    fcreate_time as fdetect_four_time,
    fbrand_name,
    fclass_name,
    case when left(fserial_number,1)='0' then "验机" else "回收" end as ftype
from (
    select 
        a.fcreate_time,
        a.fserial_number,
        a.fbrand_name,
        d.fclass_name,
        b.freal_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    left join (
        select 
            *
        from drt.drt_my33310_detection_t_detect_record 
        where fdet_type=0
        and fis_deleted=0
      	and freport_type=0
    	and fverdict<>"测试单"
        and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),(select days from cfg)))
        ) as c on upper(a.fserial_number)=upper(c.fserial_number)
    left join dws.dws_hs_order_detail as d on upper(a.fserial_number)=upper(d.fseries_number)
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),(select days from cfg)))
    and b.fdet_sop_task_name like "%拆修%"
    and c.freal_name is null)t
where num=1
--and to_date(fcreate_time) between '2024-03-01' and '2024-03-31'
)

select * from (
select 
    to_date(fdetect_one_time) as fdetect_one_time,
    fdetect_one_name,
    count(distinct case when fbrand_name='苹果' and ftype="回收" and fclass_name="手机" then fserial_number else null end) as "未1回收苹果手机",
    count(distinct case when fbrand_name!='苹果' and ftype="回收" and fclass_name="手机" then fserial_number else null end) as "未1回收安卓手机",
    count(distinct case when fbrand_name='苹果' and ftype="验机" and fclass_name="手机" then fserial_number else null end) as "未1验机苹果手机",
    count(distinct case when fbrand_name!='苹果' and ftype="验机" and fclass_name="手机" then fserial_number else null end) as "未1验机安卓手机",
    count(distinct case when fclass_name="平板" then fserial_number else null end) as "未1平板",
    null as "未2回收苹果手机",
    null as "未2回收安卓手机",
    null as "未2回收折叠手机",
    null as "未2验机苹果手机",
    null as "未2验机安卓手机",
    null as "未2验机折叠手机",
    null as "未2平板",
    null as "未3回收苹果手机",
    null as "未3回收安卓手机",
    null as "未3验机苹果手机",
    null as "未3验机安卓手机",
    null as "未3外观检测",
    null as "未3屏幕检测",
    null as "未3平板",
    null as "未4回收苹果手机",
    null as "未4回收安卓手机",
    null as "未4验机苹果手机",
    null as "未4验机安卓手机",
    null as "未4平板"
from detect_one 
group by 1,2
union all

-- 模块二统计
select 
    cast(null as timestamp) as fdetect_one_time,
    fdetect_two_name,
    null as "未1回收苹果手机",
    null as "未1回收安卓手机",
    null as "未1验机苹果手机",
    null as "未1验机安卓手机",
    null as "未1平板",
    count(distinct case when fbrand_name='Apple' and ftype="回收" and fclass_name="手机" then fserial_number else null end) as "未2回收苹果手机",
    count(distinct case when fbrand_name!='Apple' and ftype="回收" and fclass_name="手机" and fzhedie="否" then fserial_number else null end) as "未2回收安卓手机",
    count(distinct case when fbrand_name!='Apple' and ftype="回收" and fclass_name="手机" and fzhedie="是" then fserial_number else null end) as "未2回收折叠手机",
    count(distinct case when fbrand_name='Apple' and ftype="验机" and fclass_name="手机" then fserial_number else null end) as "未2验机苹果手机",
    count(distinct case when fbrand_name!='Apple' and ftype="验机" and fclass_name="手机" and fzhedie="否" then fserial_number else null end) as "未2验机安卓手机",
    count(distinct case when fbrand_name!='Apple' and ftype="验机" and fclass_name="手机" and fzhedie="是" then fserial_number else null end) as "未2验机折叠手机",
    count(distinct case when fclass_name="平板" then fserial_number else null end) as "未2平板",
    null as "未3回收苹果手机",
    null as "未3回收安卓手机",
    null as "未3验机苹果手机",
    null as "未3验机安卓手机",
    null as "未3外观检测",
    null as "未3屏幕检测",
    null as "未3平板",
    null as "未4回收苹果手机",
    null as "未4回收安卓手机",
    null as "未4验机苹果手机",
    null as "未4验机安卓手机",
    null as "未4平板"
from detect_two
group by 1,2
union all

-- 模块三统计
select 
    cast(null as timestamp) as fdetect_one_time,
    case when a.fdetect_three_name="李俊峰" then "李俊锋" else a.fdetect_three_name end as fdetect_three_name,
    null as "未1回收苹果手机",  
    null as "未1回收安卓手机",
    null as "未1验机苹果手机",
    null as "未1验机安卓手机",
    null as "未1平板",
    null as "未2回收苹果手机",
    null as "未2回收安卓手机",
    null as "未2回收折叠手机",
    null as "未2验机苹果手机",
    null as "未2验机安卓手机",
    null as "未2验机折叠手机",
    null as "未2平板",
    count(distinct case when a.fbrand_name='苹果' and a.ftype="回收" and a.fclass_name="手机" and b.fserial_number is null then a.fserial_number else null end) as "未3回收苹果手机",
    count(distinct case when a.fbrand_name!='苹果' and a.ftype="回收" and a.fclass_name="手机" and b.fserial_number is null then a.fserial_number else null end) as "未3回收安卓手机",
    count(distinct case when a.fbrand_name='苹果' and a.ftype="验机" and a.fclass_name="手机" and b.fserial_number is null then a.fserial_number else null end) as "未3验机苹果手机",
    count(distinct case when a.fbrand_name!='苹果' and a.ftype="验机" and a.fclass_name="手机" and b.fserial_number is null then a.fserial_number else null end) as "未3验机安卓手机",
    count(distinct case when b.fserial_number is not null then a.fserial_number else null end) as "未3外观检测",
    null as "未3屏幕检测",
    count(distinct case when a.fclass_name="平板" then a.fserial_number else null end) as "未3平板",
    null as "未4回收苹果手机",
    null as "未4回收安卓手机",
    null as "未4验机苹果手机",
    null as "未4验机安卓手机",
    null as "未4平板"
from detect_three as a
left join detect_three_pingmu as b on a.fserial_number=b.fserial_number
group by 1,2
union all

-- 模块三屏幕检测统计
select 
    cast(null as timestamp) as fdetect_one_time,
    case when fdetect_three_name="李俊峰" then "李俊锋" else fdetect_three_name end as fdetect_three_name,
    null as "未1回收苹果手机",
    null as "未1回收安卓手机",
    null as "未1验机苹果手机",
    null as "未1验机安卓手机",
    null as "未1平板",
    null as "未2回收苹果手机",
    null as "未2回收安卓手机",
    null as "未2回收折叠手机",
    null as "未2验机苹果手机",
    null as "未2验机安卓手机",
    null as "未2验机折叠手机",
    null as "未2平板",
    null as "未3回收苹果手机",
    null as "未3回收安卓手机",
    null as "未3验机苹果手机",
    null as "未3验机安卓手机",
    null as "未3外观检测",
    count(case when fdetect_three_time is not null then fserial_number else null end) as "未3屏幕检测",
    null as "未3平板",
    null as "未4回收苹果手机",
    null as "未4回收安卓手机",
    null as "未4验机苹果手机",
    null as "未4验机安卓手机",
    null as "未4平板"
from detect_three_pingmu 
group by 1,2
union all

-- 模块四统计
select 
    cast(null as timestamp) as fdetect_one_time,
    fdetect_four_name,
    null as "未1回收苹果手机",
    null as "未1回收安卓手机",
    null as "未1验机苹果手机",
    null as "未1验机安卓手机",
    null as "未1平板",
    null as "未2回收苹果手机",
    null as "未2回收安卓手机",
    null as "未2回收折叠手机",
    null as "未2验机苹果手机", 
    null as "未2验机安卓手机",
    null as "未2验机折叠手机",
    null as "未2平板",
    null as "未3回收苹果手机",
    null as "未3回收安卓手机",
    null as "未3验机苹果手机",
    null as "未3验机安卓手机",
    null as "未3外观检测",
    null as "未3屏幕检测",
    null as "未3平板",
    count(distinct case when fbrand_name='苹果' and ftype="回收" and fclass_name="手机" then fserial_number else null end) as "未4回收苹果手机",
    count(distinct case when fbrand_name!='苹果' and ftype="回收" and fclass_name="手机" then fserial_number else null end) as "未4回收安卓手机",
    count(distinct case when fbrand_name='苹果' and ftype="验机" and fclass_name="手机" then fserial_number else null end) as "未4验机苹果手机",
    count(distinct case when fbrand_name!='苹果' and ftype="验机" and fclass_name="手机" then fserial_number else null end) as "未4验机安卓手机",
    count(distinct case when fclass_name="平板" then fserial_number else null end) as "未4平板"
from detect_four
group by 1,2
) t

order by t.fdetect_one_time desc nulls last
