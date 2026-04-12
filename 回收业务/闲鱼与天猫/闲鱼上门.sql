with
xiadan as 

(

select 
substr(b.Forder_time,1,10) as dt
,c.Fcity
,case when c.Fcity in (
'上海市',
'东莞市',
'佛山市',
'北京市',
'南京市',
'南昌市',
'合肥市',
'宁波市',
'广州市',
'成都市',
'杭州市',
'武汉市',
'沈阳市',
'深圳市',
'苏州市',
'西安市',
'郑州市',
'重庆市',
'长沙市',
'青岛市'

) then "top20城" else "非top20城" end as topcity
,case   when b.Fxy_channel  in  ('idle')  then "闲鱼"
        when b.Fxy_channel  in  ('tmall-service')  then "天猫以旧换新"
        else "其他" end as Fxy_channel
,case   when b.Fcategory in  ('平板','平板电脑')  then "平板"
        when b.Fcategory in  ('笔记本','笔记本电脑') then "笔记本"
        when b.Fcategory in  ('手机','') then "手机"
        else "其他" end as Fcategory

,count(if(b.Fship_type=2,b.Forder_id,null)) as 上门下单量
,count(b.Forder_id) as 下单量


	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
	inner join drt.drt_my33310_recycle_t_xianyu_order_map c on b.Forder_id = c.Forder_id 
		
	where
	 b.Forder_time >= to_date(date_sub(now(),365)) 


	and b.Fship_type in (1,2)
	
	and  b.Fxy_channel  in  ('idle','tmall-service') 
group by  dt,Fcity,Fxy_channel,Fcategory,topcity
),
t as 
(
select Fxy_order_id,Fcreate_dtime as Fsync_pay_out_time
from
drt.drt_my33310_recycle_t_xianyu_order_txn
where
Fxy_order_status=5
and Fmsg_deal_complete=1
and Fcreate_dtime>= to_date(date_sub(now(),365)) 
),

chengjiao as 

(

select 
substr(t.Fsync_pay_out_time,1,10) as dt
,c.Fcity
,case when c.Fcity in (
'上海市',
'东莞市',
'佛山市',
'北京市',
'南京市',
'南昌市',
'合肥市',
'宁波市',
'广州市',
'成都市',
'杭州市',
'武汉市',
'沈阳市',
'深圳市',
'苏州市',
'西安市',
'郑州市',
'重庆市',
'长沙市',
'青岛市'

) then "top20城" else "非top20城" end as topcity
,case   when b.Fxy_channel  in  ('idle')  then "闲鱼"
        when b.Fxy_channel  in  ('tmall-service')  then "天猫以旧换新"
        else "其他" end as Fxy_channel
,case   when b.Fcategory in  ('平板','平板电脑')  then "平板"
        when b.Fcategory in  ('笔记本','笔记本电脑') then "笔记本"
        when b.Fcategory in  ('手机','') then "手机"
        else "其他" end as Fcategory
,count(b.Forder_id) as 成交量
,sum(b.Fconfirm_fee)/100 as 成交价
,count(if((b.Fconfirm_fee<b.Fquote_price),b.Forder_id,0)) as 议价量
,sum(if((b.Fconfirm_fee<b.Fquote_price),b.Fconfirm_fee,0))/100 as 议价成交价
,sum(if((b.Fconfirm_fee<b.Fquote_price),b.Fquote_price,0))/100 as 议价预估价


,sum(if((b.Fconfirm_fee<b.Fquote_price) and b.Fship_type=2,b.Fconfirm_fee,0))/100 as 上门议价成交价
,sum(if((b.Fconfirm_fee<b.Fquote_price) and b.Fship_type=2,b.Fquote_price,0))/100 as 上门议价预估价

,count(if(b.Fship_type=2,b.Forder_id,null)) as 上门成交量
,sum(if(b.Fship_type=2,b.Fconfirm_fee/100,null)) as 上门成交价

,count(if(from_timestamp((Frate_time),'yyyy-MM-dd') !='0000-00-00' and Frate_grade in (0,2,5) and b.Fship_type=2,b.Forder_id,null)) as 上门差评量


	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map c on b.Forder_id = c.Forder_id 
		inner join t on c.Fxy_order_id = t.Fxy_order_id 
	where

	 t.Fsync_pay_out_time >= to_date(date_sub(now(),365)) 


	and b.Fship_type in (1,2)
  
	and  b.Fxy_channel  in  ('idle','tmall-service') 
group by dt,Fcity,Fxy_channel,Fcategory,topcity
),

indoortijiao as 


(

select 
substr(b.Forder_time,1,10) as dt
,c.Fcity
,case when c.Fcity in (
'上海市',
'东莞市',
'佛山市',
'北京市',
'南京市',
'南昌市',
'合肥市',
'宁波市',
'广州市',
'成都市',
'杭州市',
'武汉市',
'沈阳市',
'深圳市',
'苏州市',
'西安市',
'郑州市',
'重庆市',
'长沙市',
'青岛市'

) then "top20城" else "非top20城" end as topcity
,case   when b.Fxy_channel  in  ('idle')  then "闲鱼"
        when b.Fxy_channel  in  ('tmall-service')  then "天猫以旧换新"
        else "其他" end as Fxy_channel
,case   when b.Fcategory in  ('平板','平板电脑')  then "平板"
        when b.Fcategory in  ('笔记本','笔记本电脑') then "笔记本"
        when b.Fcategory in  ('手机','') then "手机"
        else "其他" end as Fcategory
,count(b.Forder_id) as 上门提交量


	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map c on b.Forder_id = c.Forder_id 
		
	where
	 b.Forder_time >= to_date(date_sub(now(),365)) 

	and b.Forder_id not in
	(
	select 
	Forder_id
 
		FROM drt.drt_my33310_recycle_t_xy_order_data
	
	where
	Fcancel_time !='0000-00-00 00:00:00'
	and  Forder_time >= to_date(date_sub(now(),365)) 
	and unix_timestamp(Fcancel_time)-unix_timestamp(Forder_time)<=600
	and Fship_type=2
	)
	and b.Fship_type in (2)

	and  b.Fxy_channel  in  ('idle','tmall-service') 
group by dt,Fcity,Fxy_channel,Fcategory,topcity
),

tijiao as 


(

select 
substr(b.Forder_time,1,10) as dt
,c.Fcity
,case when c.Fcity in (
'上海市',
'东莞市',
'佛山市',
'北京市',
'南京市',
'南昌市',
'合肥市',
'宁波市',
'广州市',
'成都市',
'杭州市',
'武汉市',
'沈阳市',
'深圳市',
'苏州市',
'西安市',
'郑州市',
'重庆市',
'长沙市',
'青岛市'

) then "top20城" else "非top20城" end as topcity
,case   when b.Fxy_channel  in  ('idle')  then "闲鱼"
        when b.Fxy_channel  in  ('tmall-service')  then "天猫以旧换新"
        else "其他" end as Fxy_channel
,case   when b.Fcategory in  ('平板','平板电脑')  then "平板"
        when b.Fcategory in  ('笔记本','笔记本电脑') then "笔记本"
        when b.Fcategory in  ('手机','') then "手机"
        else "其他" end as Fcategory
,count(b.Forder_id) as 提交量



	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map c on b.Forder_id = c.Forder_id 
		
	where
	 b.Forder_time >= to_date(date_sub(now(),365)) 

	and b.Forder_id not in
	(
	select 
	Forder_id
 
		FROM drt.drt_my33310_recycle_t_xy_order_data
	
	where
	Fcancel_time !='0000-00-00 00:00:00'
	and  Forder_time >= to_date(date_sub(now(),365)) 
	and unix_timestamp(Fcancel_time)-unix_timestamp(Forder_time)<=600
	and Fship_type in (1,2)
	)
	and b.Fship_type in (1,2)

	and  b.Fxy_channel  in  ('idle','tmall-service') 
group by dt,Fcity,Fxy_channel,Fcategory,topcity
),

pingjia as 

(

select 
substr(b.Frate_time,1,10) as dt
,c.Fcity
,case when c.Fcity in (
'上海市',
'东莞市',
'佛山市',
'北京市',
'南京市',
'南昌市',
'合肥市',
'宁波市',
'广州市',
'成都市',
'杭州市',
'武汉市',
'沈阳市',
'深圳市',
'苏州市',
'西安市',
'郑州市',
'重庆市',
'长沙市',
'青岛市'

) then "top20城" else "非top20城" end as topcity
,case   when b.Fxy_channel  in  ('idle')  then "闲鱼"
        when b.Fxy_channel  in  ('tmall-service')  then "天猫以旧换新"
        else "其他" end as Fxy_channel
,case   when b.Fcategory in  ('平板','平板电脑')  then "平板"
        when b.Fcategory in  ('笔记本','笔记本电脑') then "笔记本"
        when b.Fcategory in  ('手机','') then "手机"
        else "其他" end as Fcategory       
        
,count(if(b.Fship_type=2,b.Forder_id,null)) as 上门评价量
,count(if(b.Fship_type=2 and Frate_grade in (0,2,5),b.Forder_id,null)) as 上门评价差评量

	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b
	inner join drt.drt_my33310_recycle_t_xianyu_order_map c on b.Forder_id = c.Forder_id 
		
	where
	 b.Frate_time >= to_date(date_sub(now(),365)) 


	and b.Fship_type in (1,2)
	
	and  b.Fxy_channel  in  ('idle','tmall-service') 
group by  dt,Fcity,Fxy_channel,Fcategory,topcity
)

select 
xiadan.dt
,xiadan.Fcity
,xiadan.topcity
,xiadan.Fxy_channel
,xiadan.Fcategory
,xiadan.上门下单量
,xiadan.下单量

,成交量
,成交价
,议价量
,议价成交价
,议价预估价

,上门议价成交价
,上门议价预估价

,上门成交量
,上门成交价
,上门差评量


,上门提交量
,提交量
,上门评价量
,上门评价差评量




from xiadan 
left join chengjiao on xiadan.dt=chengjiao.dt and xiadan.fcity=chengjiao.fcity
and xiadan.fxy_channel=chengjiao.fxy_channel
and xiadan.Fcategory=chengjiao.Fcategory
and xiadan.topcity=chengjiao.topcity


left join tijiao on xiadan.dt=tijiao.dt and xiadan.fcity=tijiao.fcity
and xiadan.fxy_channel=tijiao.fxy_channel
and xiadan.Fcategory=tijiao.Fcategory
and xiadan.topcity=tijiao.topcity


left join indoortijiao on xiadan.dt=indoortijiao.dt and xiadan.fcity=indoortijiao.fcity
and xiadan.fxy_channel=indoortijiao.fxy_channel
and xiadan.Fcategory=indoortijiao.Fcategory
and xiadan.topcity=indoortijiao.topcity




left join pingjia on xiadan.dt=pingjia.dt and xiadan.fcity=pingjia.fcity
and xiadan.fxy_channel=pingjia.fxy_channel
and xiadan.Fcategory=pingjia.Fcategory
and xiadan.topcity=pingjia.topcity


