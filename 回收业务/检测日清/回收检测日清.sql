SELECT
a.qudao,
a.Fproject_name,
a.fwarehouse_code,
a.Ftime_byday,
a.fclass_name,
a.getnum,
a.noreasonreturn,
a.todaydetect,
detect.detectinnum
from
(
SELECT 
case 
when Frecycle_alltype='回收'and Fchannel_name like '%闲鱼%' and Fproject_name like '合作%' then '闲鱼回收'
when Frecycle_alltype='回收'and  Fchannel_name like '%天猫%'  and Fproject_name like '合作%' then '天猫'
when Frecycle_alltype='寄卖'and  Fchannel_name like '%闲鱼%'  and Fproject_name like '合作%' then '闲鱼寄卖'
when Frecycle_alltype='回收' and Fproject_name like '闲鱼小站%' then '闲鱼小站'
when Fchannel_name like '%B端%' then 'B端帮卖' 
else '其他'end qudao,
Fproject_name,
fwarehouse_code,
Ftime_byday,
fclass_name,
sum(getinnum) as getnum
,sum(todaydetect) as todaydetect
,sum(noreasonreturn) as noreasonreturn
FROM 
dm_gyl_rq_dimension
WHERE 
Ftime_byday >= from_timestamp(date_sub(current_timestamp(),interval 500 day),'yyyy-MM-dd')
  and Fchannel_id not in (10000427)
GROUP by 
qudao,Ftime_byday,Fproject_name,fwarehouse_code,
fclass_name
)a
left join 
(
SELECT 
case 
when Frecycle_alltype='回收'and Fchannel_name like '%闲鱼%' and Fproject_name like '合作%' then '闲鱼回收'
when Frecycle_alltype='回收'and  Fchannel_name like '%天猫%'  and Fproject_name like '合作%' then '天猫'
when Frecycle_alltype='寄卖'and  Fchannel_name like '%闲鱼%'  and Fproject_name like '合作%' then '闲鱼寄卖'
when Frecycle_alltype='回收'and Fproject_name like '闲鱼小站%' then '闲鱼小站'
when Fchannel_name like '%B端%' then 'B端帮卖' 
else '其他'end qudao,
case when Fproject_name="自有项目" and Fchannel_name="支付宝小程序" then "合作项目" else Fproject_name end as Fproject_name,
fwarehouse_code,
Ftime_byday,
fclass_name,
sum(detectinnum) as detectinnum
from
(
(SELECT
strleft(a.fdetect_time,10) as Ftime_byday,
'回收' as Frecycle_alltype,
a.fchannel_name,
case when Fproject_name="自有项目" and Fchannel_name="支付宝小程序" then "合作项目" else Fproject_name end as Fproject_name,
 b.fwarehouse_number as fwarehouse_code, 
 a.fclass_name,
sum(a.fpcs) as detectinnum
FROM
dws.dws_hs_order_detail AS a
left join dws.dws_instock_details as b on a.fseries_number=b.fseries_number
WHERE
a.fchannel_id not in (10000195,10000427)
and a.fchannel_name not like "%帮卖%"
and a.fdetect_time is not null
and a.ftest=0
GROUP BY 1,2,3,4,5,6
)
UNION
(
SELECT
strleft(a.fdetect_push_time,10) as Ftime_byday,
'寄卖' as Frecycle_alltype,
a.fchannel_name,
case when Fproject_name="自有项目" and Fchannel_name="支付宝小程序" then "合作项目" else Fproject_name end as Fproject_name,
b.fwarehouse_number as fwarehouse_code, 
a.fclass_name,
sum(a.fpcs) as detectinnum
FROM
dws.dws_jm_order_detail AS a
left join dws.dws_instock_details as b on a.fseries_number=b.fseries_number
WHERE
fdetect_push_time IS NOT NULL
GROUP BY 1,2,3,4,5,6
)
)c
group by qudao,Fproject_name,Ftime_byday,fwarehouse_code,fclass_name
)detect on detect.Ftime_byday=a.Ftime_byday and detect.qudao=a.qudao and a.Fproject_name=detect.Fproject_name and a.fwarehouse_code=detect.fwarehouse_code and detect.fclass_name=a.fclass_name

