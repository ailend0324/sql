select 
    fcreate_time,
    left(fserial_number,2) as fchannel,
    case when left(fserial_number,2) in ('01','02') then "验机"
    	when left(fserial_number,2)='BM' then "寄卖"
        when (left(fserial_number,2)="CG" and fcreate_time>='2024-12-01') or left(fserial_number,2)="TL" then "太力"
    else "回收" end as "业务类型",
    case when left(fserial_number,2) in ('02') or right(left(fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    case when fdet_type=1 then "苹果" else "安卓" end as fbrand,
    case when to_date(fcreate_time)='2023-10-16' and freal_name="刘俊" then "周利" 
         when to_date(fcreate_time)='2024-03-04' and freal_name="陈冬凡" then "周远鸿" 
         when to_date(fcreate_time)='2024-03-04' and freal_name="胡家华" then "黄成水"
         when to_date(fcreate_time)='2024-03-02' and freal_name="陈冬凡" then "李浩宇"
         when to_date(fcreate_time)='2024-03-05' and freal_name="陈冬凡" then "周远鸿"
         when to_date(fcreate_time)='2024-03-14' and freal_name="严俊" then "林广泽"
         when fbind_real_name is not null or fbind_real_name="" then fbind_real_name
    else freal_name end as freal_name,
    case when fdet_type=1 then "插线检测" else "imel检测" end as fdet_type,
    fserial_number
from (
select 
    a.fcreate_time,
    a.fserial_number,
    a.fbind_real_name, 
    case when a.fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" else b.freal_name end as freal_name,
    a.fdet_type,
    row_number()over(partition by a.fserial_number order by a.fcreate_time desc) as num
from drt.drt_my33312_detection_t_automation_det_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_id=b.fuser_id
where a.fserial_number is not null and a.fserial_number!=""
and to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),300)))t
where num=1
