
with gujia_tod as 

(		
select  
				to_date(Feva_time) as  dt
				,a.fxy_product_name
  				,Fhsb_product_name
				,Fxy_channel 
				,fsub_channel
				,case when Fcategory in ('手机','') then '手机' 
	            when Fcategory in ('平板','平板电脑') then '平板'  when Fcategory in ('笔记本','笔记本电脑') then '笔记本' 
	            when Fcategory in ('耳机')  then '耳机'  when Fcategory in ('智能手表')  then '智能手表' else '其他' end as Fcategory
				,count(distinct Fxy_quote_id) as Fxy_quote_id
				
			
FROM
	     drt.drt_my33310_recycle_t_xy_eva_data  a 
  		left join drt.drt_my33310_recycle_t_xian_yu_product_map b on a.Fxy_product_name=b.Fxy_product_name
WHERE
	    Feva_time between to_date(now()) and now()
		AND Feva_result = 1
         and cast(Ftemplate_type as string) in ('1','3','')
    and Fbusiness_type in ('xyV1','xyV2','','xyV1Gold')
	
group by dt,Fxy_channel,fsub_channel,Fcategory,fxy_product_name,Fhsb_product_name
),

xiadan_tod as 
(
select 
    to_date(forder_time)  as dt
		,fxy_product_name
    ,Fxy_channel 
    ,fsub_channel
    ,case when Fcategory in ('手机','') then '手机' 
	when Fcategory in ('平板','平板电脑') then '平板'  when Fcategory in ('笔记本','笔记本电脑') then '笔记本' 
	 when Fcategory in ('耳机')  then '耳机'  when Fcategory in ('智能手表')  then '智能手表' else '其他' end as Fcategory
    ,Forder_id
  

from  drt.drt_my33310_recycle_t_xy_order_data 

where
    forder_time  between to_date(now()) and now()


),


xiadan_tod1 as 
(
select 
    to_date(a.dt)  as dt
		,fxy_product_name
    ,Fxy_channel 
    ,fsub_channel
    ,case when Fcategory in ('手机','') then '手机' 
	when Fcategory in ('平板','平板电脑') then '平板'  when Fcategory in ('笔记本','笔记本电脑') then '笔记本' 
	 when Fcategory in ('耳机')  then '耳机'  when Fcategory in ('智能手表')  then '智能手表' else '其他' end as Fcategory
    ,count(distinct a.Forder_id) as Forder_id
   

from  xiadan_tod a inner join  drt.drt_my33310_recycle_t_order b on a.forder_id=b.forder_id

where
    b.ftest=0

group by dt,Fxy_channel,fsub_channel,Fcategory,fxy_product_name
),




gujia_yes as 

(		
select  
				to_date(Feva_time) as  dt
				,a.fxy_product_name
  				,Fhsb_product_name
				,Fxy_channel 
				,fsub_channel
				,case when Fcategory in ('手机','') then '手机' 
	            when Fcategory in ('平板','平板电脑') then '平板'  when Fcategory in ('笔记本','笔记本电脑') then '笔记本' 
	            when Fcategory in ('耳机')  then '耳机'  when Fcategory in ('智能手表')  then '智能手表' else '其他' end as Fcategory
				,count(distinct Fxy_quote_id) as Fxy_quote_id
				
			
FROM
	     drt.drt_my33310_recycle_t_xy_eva_data 	a	left join drt.drt_my33310_recycle_t_xian_yu_product_map b on a.Fxy_product_name=b.Fxy_product_name 
WHERE
	    Feva_time between  date_sub(to_date(now()),1) and  date_sub(now(),1) 
		AND Feva_result = 1
         and cast(Ftemplate_type as string) in ('1','3','')
    and Fbusiness_type in ('xyV1','xyV2','','xyV1Gold')
	
group by dt,Fxy_channel,fsub_channel,Fcategory,fxy_product_name,Fhsb_product_name
),

xiadan_yes as 
(
select 
    to_date(forder_time)  as dt
	,fxy_product_name
    ,Fxy_channel 
    ,fsub_channel
    ,case when Fcategory in ('手机','') then '手机' 
	 when Fcategory in ('平板','平板电脑') then '平板'  when Fcategory in ('笔记本','笔记本电脑') then '笔记本' 
	 when Fcategory in ('耳机')  then '耳机'  when Fcategory in ('智能手表')  then '智能手表' else '其他' end as Fcategory
    ,Forder_id
   

from  drt.drt_my33310_recycle_t_xy_order_data 

where
    forder_time  between  date_sub(to_date(now()),1) and  date_sub(now(),1) 

),


xiadan_yes1 as 
(
select 
    to_date(a.dt)  as dt
	,fxy_product_name
    ,Fxy_channel 
    ,fsub_channel
    ,case when Fcategory in ('手机','') then '手机' 
	when Fcategory in ('平板','平板电脑') then '平板'  when Fcategory in ('笔记本','笔记本电脑') then '笔记本' 
	 when Fcategory in ('耳机')  then '耳机'  when Fcategory in ('智能手表')  then '智能手表' else '其他' end as Fcategory
     ,count(distinct a.Forder_id) as Forder_id
   

from  xiadan_yes a inner join  drt.drt_my33310_recycle_t_order b on a.forder_id=b.forder_id

where
    b.ftest=0

group by dt,Fxy_channel,fsub_channel,Fcategory,fxy_product_name
),

quxiao_yes as 
(
select 
    to_date(Fcancel_time)  as dt
	,fxy_product_name
    ,Fxy_channel 
    ,fsub_channel
    ,case when Fcategory in ('手机','') then '手机' 
	 when Fcategory in ('平板','平板电脑') then '平板'  when Fcategory in ('笔记本','笔记本电脑') then '笔记本' 
	 when Fcategory in ('耳机')  then '耳机'  when Fcategory in ('智能手表')  then '智能手表' else '其他' end as Fcategory
    ,count(distinct Forder_id) as Forder_id
  

from  drt.drt_my33310_recycle_t_xy_order_data 

where
    Fcancel_time  between  date_sub(to_date(now()),1) and  date_sub(now(),1) 

group by dt,Fxy_channel,fsub_channel,Fcategory,fxy_product_name
)





select 
a.dt
,a.Fhsb_product_name
,a.Fxy_channel 
,a.fsub_channel
,a.Fcategory
,a.Fxy_quote_id as gujiapv
,b.Forder_id as xiadanpv


from gujia_tod a left join xiadan_tod1 b 
on a.dt=b.dt and a.Fxy_channel =b.Fxy_channel
and a.fsub_channel=b.fsub_channel and a.Fcategory=b.Fcategory
and a.fxy_product_name=b.fxy_product_name


union all



select 
a.dt
,a.Fhsb_product_name
,a.Fxy_channel 
,a.fsub_channel
,a.Fcategory
,a.Fxy_quote_id as gujiapv
,b.Forder_id as xiadanpv


from gujia_yes a left join xiadan_yes1 b 
on a.dt=b.dt and a.Fxy_channel =b.Fxy_channel
and a.fsub_channel=b.fsub_channel and a.Fcategory=b.Fcategory
and a.fxy_product_name=b.fxy_product_name
