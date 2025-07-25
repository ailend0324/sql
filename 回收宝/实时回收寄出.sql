with a  as 
(
select 
case 
    when a.forder_time between  to_date(now())and now() 
    then "今日"
    when a.forder_time between  date_sub(to_date(now()),1) and  date_sub(now(),1) 
    then "昨日同期"
        
    when a.forder_time between  date_sub(to_date(now()),7) and  date_sub(now(),7) 
    then "上周同期"
    else "其他" end as forder_time
,case   when cast(a.fship_type as int)=1 then '邮寄'
        when cast(a.fship_type as int)=2 then '上门'
        when cast(a.fship_type as int)=3 then '到店'
    end as fship_type
,b.ftest 
,a.Fcategory
,a.Fxy_channel
,a.fsub_channel
,a.forder_id
from
 drt.drt_my33310_recycle_t_xy_order_data a
 inner join drt.drt_my33310_recycle_t_order b on a.forder_id=b.forder_id
 inner join drt.drt_my33310_recycle_t_xianyu_order_map c on a.forder_id=c.forder_id
 left join   drt.drt_my33310_recycle_t_order_status d on b.forder_status=d.forder_status_id
 
 where a.forder_time between date_sub(to_date(now()),7) and now()
 
 
 ),
b as 
 (select 
 b.forder_id
 ,case 
    when b.Fauto_create_time between  to_date(now())and now() 
    then "今日"
    when b.Fauto_create_time between  date_sub(to_date(now()),1) and  date_sub(now(),1) 
    then "昨日同期"
    when b.Fauto_create_time between  date_sub(to_date(now()),7) and  date_sub(now(),7) 
    then "上周同期"
    else "其他" end as forder_time
 from a
 inner join drt.drt_my33310_recycle_t_order_txn b
 on a.forder_id=b.forder_id
 where
 Forder_status=20

 
 union all
 select 
  b.forder_id
 ,case 
    when b.Fauto_create_time between  to_date(now())and now() 
    then "今日"
    when b.Fauto_create_time between  date_sub(to_date(now()),1) and  date_sub(now(),1) 
    then "昨日同期"
    when b.Fauto_create_time between  date_sub(to_date(now()),7) and  date_sub(now(),7) 
    then "上周同期"
    else "其他" end as forder_time
 from a
 inner join drt.drt_my33310_recycle_t_order_txn b
 on a.forder_id=b.forder_id
 where
 Forder_status=220

  
 union all
 select 
  b.forder_id
 ,case 
    when b.Fauto_create_time between  to_date(now())and now() 
    then "今日"
    when b.Fauto_create_time between  date_sub(to_date(now()),1) and  date_sub(now(),1) 
    then "昨日同期"
    when b.Fauto_create_time between  date_sub(to_date(now()),7) and  date_sub(now(),7) 
    then "上周同期"
    else "其他" end as forder_time
 from a
 inner join drt.drt_my33310_recycle_t_order_txn b
 on a.forder_id=b.forder_id
 where
 Forder_status=45

 
 
 ),
 c as 
 (select 
  b.forder_id
 ,case 
    when b.Fauto_create_time between  to_date(now())and now() 
    then "今日"
    when b.Fauto_create_time between  date_sub(to_date(now()),1) and  date_sub(now(),1) 
    then "昨日同期"
    when b.Fauto_create_time between  date_sub(to_date(now()),7) and  date_sub(now(),7) 
    then "上周同期"
    else "其他" end as forder_time
 from a
 inner join drt.drt_my33310_recycle_t_order_txn b
 on a.forder_id=b.forder_id
 where
 Forder_status=80
 )
 
 select 
    a.Forder_time 
    ,fship_type 
    ,ftest 
    ,Fcategory 
    ,Fxy_channel
    ,fsub_channel
    ,count(a.forder_id) as ordernum
    ,count(c.forder_id) as cancelnum
    ,count(b.forder_id) as sendnum
 
 from a left join b on a.forder_id=b.forder_id and a.forder_time=b.forder_time
        left join c on a.forder_id=c.forder_id and a.forder_time=c.forder_time
   
 group by 
    Forder_time 
    ,fship_type 
    ,ftest 
    ,Fcategory 
    ,Fxy_channel
    ,fsub_channel
    
