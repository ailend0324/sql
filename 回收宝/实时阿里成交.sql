select 
    to_date(b.Fsync_pay_out_time) as dt

    ,ftest
    ,case when b.Fship_type=1 then '邮寄' when b.Fship_type=2 then '上门' when b.Fship_type=3 then '到店'  else '其他' end as shiptype
    ,case when b.Fxy_channel not IN ( 'tmall-service','tm_recycle' ,'rm_recycle')  then '闲鱼' else '天猫' end as channel
    ,b.Fxy_channel
    ,case when b.Fcategory in ('','手机') then '手机' when b.Fcategory in ('平板','平板电脑') then '平板'
    when b.Fcategory in ('笔记本','笔记本电脑') then '笔记本'
    when b.Fcategory in ('耳机') then '耳机'
    when b.Fcategory in ('智能手表') then '智能手表'
    else null end as Fcategory
    ,count(a.Forder_id) as Forder_id
    ,sum(b.Fconfirm_fee/100) as Fconfirm_fee
    ,sum(b.Fconfirm_fee/100)/count(a.Forder_id) as perpay

FROM
	 drt.drt_my33310_recycle_t_order  AS a
	INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
	
		
where
    b.Fsync_pay_out_time between DATE_SUB(to_date(now()),1) and DATE_SUB(now(),1)
 
group by dt,ftest,shiptype,channel,Fxy_channel,Fcategory
 
 
 
 union all
 
 


select 
  to_date(a.fauto_create_time) as dt
-- date(b.Fsync_pay_out_time) as 完结时间
    ,0 as ftest
    ,'寄卖' as shiptype
    ,'寄卖' as channel
    ,'寄卖' as Fxy_channel
    ,case when a.fproduct_class_id=1 then '手机' when a.fproduct_class_id=3 then '平板'
    when a.fproduct_class_id=2 then '笔记本'
    when a.fproduct_class_id=17 then '耳机'
    when a.fproduct_class_id=5 then '智能手表'
    else null end as Fcategory
    ,count(a.Forder_id) as Forder_id
    ,sum(c.Fsales_amount/100) as Fconfirm_fee
    ,sum(c.Fsales_amount/100)/count(a.Forder_id) as perpay


from
drt.drt_my33310_recycle_t_xy_jimai_plus_order  c
inner join (
select *
from(
select 
a.Forder_id,b.fproduct_class_id,a.fauto_create_time
,row_number() over(partition by a.Forder_id order by a.fauto_create_time desc) as ranknum
from  drt.drt_my33310_recycle_t_order_txn a left join  drt.drt_my33310_recycle_t_order b on a.Forder_id=b.Forder_id
where  a.Forder_status in (714)
)a where ranknum=1
 
)a on a.Forder_id=c.Forder_id
 
 
		
where
    a.fauto_create_time between DATE_SUB(to_date(now()),1) and DATE_SUB(now(),1)
 
group by dt,ftest,shiptype,channel,Fxy_channel,Fcategory
 
 
 union all
 
 select 
    to_date(b.Fsync_pay_out_time) as dt

    ,ftest
    ,case when b.Fship_type=1 then '邮寄' when b.Fship_type=2 then '上门' when b.Fship_type=3 then '到店'  else '其他' end as shiptype
    ,case when b.Fxy_channel not IN ( 'tmall-service','tm_recycle' ,'rm_recycle')  then '闲鱼' else '天猫' end as channel
    ,b.Fxy_channel
    ,case when b.Fcategory in ('','手机') then '手机' when b.Fcategory in ('平板','平板电脑') then '平板'
    when b.Fcategory in ('笔记本','笔记本电脑') then '笔记本'
    when b.Fcategory in ('耳机') then '耳机'
    when b.Fcategory in ('智能手表') then '智能手表'
    else null end as Fcategory
    ,count(a.Forder_id) as Forder_id
    ,sum(b.Fconfirm_fee/100) as Fconfirm_fee
    ,sum(b.Fconfirm_fee/100)/count(a.Forder_id) as perpay

FROM
	 drt.drt_my33310_recycle_t_order  AS a
	INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
	
		
where
    b.Fsync_pay_out_time between to_date(now()) and now()
 
group by dt,ftest,shiptype,channel,Fxy_channel,Fcategory
 
 
 
 union all
 
 


select 
  to_date(a.fauto_create_time) as dt
-- date(b.Fsync_pay_out_time) as 完结时间
    ,0 as ftest
    ,'寄卖' as shiptype
    ,'寄卖' as channel
    ,'寄卖' as Fxy_channel
    ,case when a.fproduct_class_id=1 then '手机' when a.fproduct_class_id=3 then '平板'
    when a.fproduct_class_id=2 then '笔记本'
    when a.fproduct_class_id=17 then '耳机'
    when a.fproduct_class_id=5 then '智能手表'
    else null end as Fcategory
    ,count(a.Forder_id) as Forder_id
    ,sum(c.Fsales_amount/100) as Fconfirm_fee
    ,sum(c.Fsales_amount/100)/count(a.Forder_id) as perpay


from
drt.drt_my33310_recycle_t_xy_jimai_plus_order  c
inner join (
select *
from(
select 
a.Forder_id,b.fproduct_class_id,a.fauto_create_time
,row_number() over(partition by a.Forder_id order by a.fauto_create_time desc) as ranknum
from  drt.drt_my33310_recycle_t_order_txn a left join  drt.drt_my33310_recycle_t_order b on a.Forder_id=b.Forder_id
where  a.Forder_status in (714)
)a where ranknum=1
 
)a on a.Forder_id=c.Forder_id
 
 
		
where
    a.fauto_create_time between to_date(now()) and now()
 
group by dt,ftest,shiptype,channel,Fxy_channel,Fcategory
 
