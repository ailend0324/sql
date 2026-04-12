select  
    *
from (
select 
    a.fcreate_time as fend_time,
    a.fserial_number,
    a.fclass_name,
    case when a.fbrand_name='苹果' then "苹果" else "安卓" end as fbrand_name,
  	case when left(fserial_number,2) in ('01','02') then "验机" 
  		 when left(fserial_number,2)="BM" then "寄卖"
  		when (left(fserial_number,2)="CG" and a.fcreate_time>='2024-12-01') or left(fserial_number,2)="TL" then "太力"
  else "回收" end as ftype,
  	case when left(fserial_number,2) in ('02') or right(left(fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
  	case when b.fdet_sop_task_name like "%外观%" then "模块三(外观)"
         when b.fdet_sop_task_name like "%屏幕%" then "模块三(屏幕)"
  	     when b.fdet_sop_task_name like "%拆修%" then "模块四(拆修)"
  	else null end as "模块内容",
    a.fproduct_name, 
    a.fimei,b.freal_name,
    b.fdet_sop_task_name,
    b.fiphotograph_status,
    row_number()over(partition by a.fserial_number order by a.fend_time desc) as num
from drt.drt_my33312_detection_t_det_task as a
left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),366))
and (b.fdet_sop_task_name like "%外观%"))t
where num=1
union 
select  
    *
from (
select 
    a.fcreate_time as fend_time,
    a.fserial_number,
    a.fclass_name,
    case when a.fbrand_name='苹果' then "苹果" else "安卓" end as fbrand_name,
  	case when left(fserial_number,2) in ('01','02') then "验机" 
  		 when left(fserial_number,2)="BM" then "寄卖"
  		when (left(fserial_number,2)="CG" and a.fcreate_time>='2024-12-01') or left(fserial_number,2)="TL" then "太力"
  else "回收" end as ftype,
  	case when left(fserial_number,2) in ('02') or right(left(fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
  	case when b.fdet_sop_task_name like "%外观%" then "模块三(外观)"
         when b.fdet_sop_task_name like "%屏幕%" then "模块三(屏幕)"
  	     when b.fdet_sop_task_name like "%拆修%" then "模块四(拆修)"
  	else null end as "模块内容",
    a.fproduct_name, 
    a.fimei,b.freal_name,
    b.fdet_sop_task_name,
    b.fiphotograph_status,
    row_number()over(partition by a.fserial_number order by a.fend_time desc) as num
from drt.drt_my33312_detection_t_det_task as a
left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),366))
and (b.fdet_sop_task_name like "%屏幕%")
and b.fdet_sop_task_name!="外观屏幕"
)t
where num=1
union 
select  
    *
from (
select 
    a.fend_time,
    a.fserial_number,
    a.fclass_name,
    case when a.fbrand_name='苹果' then "苹果" else "安卓" end as fbrand_name,
  	case when left(a.fserial_number,2) in ('01','02') then "验机" 
  		when left(a.fserial_number,2)="BM" then "寄卖"
  		when (left(a.fserial_number,2)="CG" and a.fend_time>='2024-12-01') or left(a.fserial_number,2)="TL" then "太力" 
  else "回收" end as ftype,
  	case when left(fserial_number,2) in ('02') or right(left(fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
  	case when b.fdet_sop_task_name like "%外观%" then "模块三(外观)"
  		 when b.fdet_sop_task_name like "%屏幕%" then "模块三(屏幕)"
  	     when b.fdet_sop_task_name like "%拆修%" then "模块四(拆修)"
  	else null end as "模块内容",
    a.fproduct_name, 
    a.fimei,b.freal_name,
    b.fdet_sop_task_name,
    b.fiphotograph_status,
    row_number()over(partition by a.fserial_number order by a.fend_time desc) as num
from drt.drt_my33312_detection_t_det_task as a
left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),366))
and b.fdet_sop_task_name like "%拆修%")t
where num=1
