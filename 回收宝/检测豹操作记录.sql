select
    t.*,
    hour(fcreate_time) as fhour,
    MINUTE(fcreate_time) as fminute,
    LEFT(t.fserial_number,2) as fchannel
from (
select 
a.fserial_number,
a.fcreate_time,
 case when left(a.fserial_number,2) in ('01','02') then "验机"
  	  when left(a.fserial_number,2)="BM" then "寄卖"
  	   when (left(a.fserial_number,2)="CG" and a.fcreate_time>='2024-12-01') or left(a.fserial_number,2)="TL" then "太力"
else "回收" end as "业务类型",
  case when left(a.fserial_number,2) in ('02') or right(left(a.fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
case when to_date(a.fcreate_time)="2023-10-18" and b.freal_name="成露露" then "江珊" 
     when a.fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" 
  	 when to_date(a.fcreate_time)='2024-03-04' and b.freal_name="陈冬凡" then "周远鸿" 
         when to_date(a.fcreate_time)='2024-03-04' and b.freal_name="胡家华" then "黄成水"
  	when b.freal_name="陈冬凡" and to_date(a.fcreate_time)='2024-05-13' and a.fbrand_name!="Apple" then "李俊锋"
  	when b.freal_name="周远鸿" and to_date(a.fcreate_time)='2024-05-15' then null
  else b.freal_name end as freal_name,
a.fproduct_name,
case when a.fbrand_name='Apple' or e.fname='苹果' then "苹果" else "安卓" end as fname,
a.foriginal_data,
a.ftransform_options,
row_number()over(partition by a.fserial_number order by a.fcreate_time desc) as num
from drt.drt_my33312_detection_t_det_app_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
left join drt.drt_my33310_recycle_t_order as c on a.fserial_number=c.fseries_number
left join drt.drt_my33310_recycle_t_product as d on c.fproduct_id=d.fproduct_id
left join drt.drt_my33310_recycle_t_pdt_brand as e on d.fclass_id=e.fid
where a.fserial_number!=""
and a.fserial_number is not null
and to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),366)))t
where num=1
