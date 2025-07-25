select 
    to_date(fcreate_time) as fcreate_time,
    case when to_date(fcreate_time)='2023-10-16' and freal_name="刘俊" then "周利" 
    	 when (to_date(fcreate_time) BETWEEN '2023-11-01' AND '2023-11-21') and freal_name="郑佩文" then null 
         when to_date(fcreate_time)='2024-01-29' and freal_name="林嘉成" then null
         else freal_name end as freal_name,
    case when fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
    "模块一" as ftype,
    case when left(fserial_number,2) in ('01','02') then "验机" else "回收" end as fchannel,
    count(fserial_number) as num
from (
select 
    a.fcreate_time,
    a.fserial_number,
    b.freal_name,
    a.fbrand_name,
    row_number()over(partition by a.fserial_number order by a.fcreate_time desc) as num
from drt.drt_my33312_detection_t_automation_det_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_id=b.fuser_id
where a.fserial_number is not null and a.fserial_number!=""
--and b.freal_name is not null
and to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
)t
where num=1
group by 1,2,3,4,5
union all
select 
    to_date(fcreate_time) as fcreate_time,
    freal_name,
    case when fbrand_name='Apple' then "苹果" else "安卓" end as fbrand_name,
    "模块二" as ftype,
    case when left(fserial_number,2) in ('01','02') then "验机" else "回收" end as fchannel,
    count(fserial_number) as num
from (
select 
    a.fcreate_time,
    a.fserial_number,
    case when a.fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" 
  		 when a.freal_name="陈冬凡" and to_date(a.fcreate_time)='2024-05-13' and a.fbrand_name!="Apple" then "李俊锋"
  else b.freal_name end as freal_name,
    a.fbrand_name,
    row_number()over(partition by a.fserial_number order by a.fcreate_time desc) as num
from drt.drt_my33312_detection_t_det_app_record as a 
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_id=b.fuser_id
where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
--and b.freal_name is not null
and a.fserial_number is not null and a.fserial_number!=""
--and b.freal_name not in ('黄成水','张圳强','吴琼','冯铭焕','胡涛','李俊锋','黄雅如','朱惠萍','林红','陈映熹','张世梅')
)t
where num=1
group by 1,2,3,4,5
