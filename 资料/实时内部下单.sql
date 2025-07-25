
with gujia_tod as 

(		
select  
				to_date(Feva_time) as  dt
				,Fxy_channel 
				,fsub_channel
            ,case when fcategory in ('平板','平板电脑') then '平板'
when fcategory in ('笔记本','笔记本电脑') then '笔记本'
when fcategory in ('手机','') then '手机'
when fcategory in ('单反闪光灯',
'单反转接环',
'移动电源',
'移动硬盘',
'云台',
'拍照配件/云台',                
'增距镜') then '3C数码配件'

when fcategory in ('彩色激光多功能一体机',
'复印打印多功能一体机',
'激光打印机',
'墨盒',
'收款机',
'投影机',
'硒鼓粉盒',
'针式打印机') then '办公设备耗材'

when fcategory in ('CPU',
'电脑服务器',
'电脑固态硬盘',
'固态硬盘',                   
'电脑内存',
'内存条',                 
'电脑显卡',
'显卡',                   
'电脑硬件套装',
'电脑主板',
'键盘',
'品牌台机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件',
'游戏机') then '电玩'

when fcategory in ('单反套机',
'单反相机',
'拍立得',
'摄像机',
'摄影机',                   
'数码相机',
'微单相机',
'相机镜头',
'运动相机',
'单反/微单套机',
'单反/微单相机') then '相机/摄像机'

when fcategory in ('耳机',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔',
'按摩器',
'吹风机',
'磁吸式键盘',
'电子书',
'翻译器',
'风扇',
'加湿器',
'录音笔',
'美发器',
'手写笔',
'智能手写笔',                   
'无人机',
'吸尘器',
'学习机',
'智能办公本',
'智能配饰',
'智能摄像',
'智能手表',
'智能手环') then '智能设备'
when fcategory in ('黄金') then "黄金"

else  '其他' end as fcategory

				,count(Fxy_quote_id) as Fxy_quote_id
				,count(distinct Fuser_id) as Fuser_id
			
FROM
	     drt.drt_my33310_recycle_t_xy_eva_data  
WHERE
	    Feva_time between to_date(now()) and now()
		AND Feva_result = 1
       and cast(Ftemplate_type as string) in ('1','3','')
       and Fbusiness_type in ('xyV1','xyV2','','xyV1Gold')
	
group by dt,Fxy_channel,fsub_channel,Fcategory
),

xiadan_tod as 
(
select 
    to_date(forder_time)  as dt
    ,Fxy_channel 
    ,fsub_channel
,case when fcategory in ('平板','平板电脑') then '平板'
when fcategory in ('笔记本','笔记本电脑') then '笔记本'
when fcategory in ('手机','') then '手机'
when fcategory in ('单反闪光灯',
'单反转接环',
'移动电源',
'移动硬盘',
'云台',
'拍照配件/云台',                
'增距镜') then '3C数码配件'

when fcategory in ('彩色激光多功能一体机',
'复印打印多功能一体机',
'激光打印机',
'墨盒',
'收款机',
'投影机',
'硒鼓粉盒',
'针式打印机') then '办公设备耗材'

when fcategory in ('CPU',
'电脑服务器',
'电脑固态硬盘',
'固态硬盘',                   
'电脑内存',
'内存条',                 
'电脑显卡',
'显卡',                   
'电脑硬件套装',
'电脑主板',
'键盘',
'品牌台机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件',
'游戏机') then '电玩'

when fcategory in ('单反套机',
'单反相机',
'拍立得',
'摄像机',
'摄影机',                   
'数码相机',
'微单相机',
'相机镜头',
'运动相机',
'单反/微单套机',
'单反/微单相机') then '相机/摄像机'

when fcategory in ('耳机',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔',
'按摩器',
'吹风机',
'磁吸式键盘',
'电子书',
'翻译器',
'风扇',
'加湿器',
'录音笔',
'美发器',
'手写笔',
'智能手写笔',                   
'无人机',
'吸尘器',
'学习机',
'智能办公本',
'智能配饰',
'智能摄像',
'智能手表',
'智能手环') then '智能设备'
when fcategory in ('黄金') then "黄金"

else  '其他' end as fcategory


    ,Forder_id
    ,Fuser_id

from  drt.drt_my33310_recycle_t_xy_order_data 

where
    forder_time  between to_date(now()) and now()


),


xiadan_tod1 as 
(
select 
    to_date(a.dt)  as dt
    ,Fxy_channel 
    ,fsub_channel
    ,fcategory

    ,count(a.Forder_id) as Forder_id
    ,count(distinct a.Fuser_id) as Fuser_id
  

from  xiadan_tod a inner join  drt.drt_my33310_recycle_t_order b on a.forder_id=b.forder_id


where
    b.ftest=0

group by dt,Fxy_channel,fsub_channel,Fcategory
),






quxiao_tod as 
(
select 
    to_date(dt)  as dt
    ,Fxy_channel 
    ,fsub_channel
    ,fcategory


    ,count(a.Forder_id) as Forder_id
    ,count(distinct a.Fuser_id) as Fuser_id

from  xiadan_tod a inner join  drt.drt_my33310_recycle_t_order_txn  b on a.forder_id=b.forder_id
inner join  drt.drt_my33310_recycle_t_order c on a.forder_id=c.forder_id
where
    b.fauto_create_time  between to_date(now()) and now()
    and b.forder_status=80
    and c.ftest=0
group by dt,Fxy_channel,fsub_channel,Fcategory
),


gujia_yes as 

(		
select  
				to_date(Feva_time) as  dt
				,Fxy_channel 
				,fsub_channel
	,case when fcategory in ('平板','平板电脑') then '平板'
when fcategory in ('笔记本','笔记本电脑') then '笔记本'
when fcategory in ('手机','') then '手机'
when fcategory in ('单反闪光灯',
'单反转接环',
'移动电源',
'移动硬盘',
'云台',
'拍照配件/云台',                
'增距镜') then '3C数码配件'

when fcategory in ('彩色激光多功能一体机',
'复印打印多功能一体机',
'激光打印机',
'墨盒',
'收款机',
'投影机',
'硒鼓粉盒',
'针式打印机') then '办公设备耗材'

when fcategory in ('CPU',
'电脑服务器',
'电脑固态硬盘',
'固态硬盘',                   
'电脑内存',
'内存条',                 
'电脑显卡',
'显卡',                   
'电脑硬件套装',
'电脑主板',
'键盘',
'品牌台机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件',
'游戏机') then '电玩'

when fcategory in ('单反套机',
'单反相机',
'拍立得',
'摄像机',
'摄影机',                   
'数码相机',
'微单相机',
'相机镜头',
'运动相机',
'单反/微单套机',
'单反/微单相机') then '相机/摄像机'

when fcategory in ('耳机',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔',
'按摩器',
'吹风机',
'磁吸式键盘',
'电子书',
'翻译器',
'风扇',
'加湿器',
'录音笔',
'美发器',
'手写笔',
'智能手写笔',                   
'无人机',
'吸尘器',
'学习机',
'智能办公本',
'智能配饰',
'智能摄像',
'智能手表',
'智能手环') then '智能设备'
when fcategory in ('黄金') then "黄金"

else  '其他' end as fcategory


				,count(Fxy_quote_id) as Fxy_quote_id
				,count(distinct Fuser_id) as Fuser_id
			
FROM
	     drt.drt_my33310_recycle_t_xy_eva_data  
WHERE
	    Feva_time between  date_sub(to_date(now()),1) and  date_sub(now(),1) 
		AND Feva_result = 1
        and cast(Ftemplate_type as string) in ('1','3','')
        and Fbusiness_type in ('xyV1','xyV2','','xyV1Gold')
	
group by dt,Fxy_channel,fsub_channel,Fcategory
),

xiadan_yes as 
(
select 
    to_date(forder_time)  as dt
    ,Fxy_channel 
    ,fsub_channel
,case when fcategory in ('平板','平板电脑') then '平板'
when fcategory in ('笔记本','笔记本电脑') then '笔记本'
when fcategory in ('手机','') then '手机'
when fcategory in ('单反闪光灯',
'单反转接环',
'移动电源',
'移动硬盘',
'云台',
'拍照配件/云台',                
'增距镜') then '3C数码配件'

when fcategory in ('彩色激光多功能一体机',
'复印打印多功能一体机',
'激光打印机',
'墨盒',
'收款机',
'投影机',
'硒鼓粉盒',
'针式打印机') then '办公设备耗材'

when fcategory in ('CPU',
'电脑服务器',
'电脑固态硬盘',
'固态硬盘',                   
'电脑内存',
'内存条',                 
'电脑显卡',
'显卡',                   
'电脑硬件套装',
'电脑主板',
'键盘',
'品牌台机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件',
'游戏机') then '电玩'

when fcategory in ('单反套机',
'单反相机',
'拍立得',
'摄像机',
'摄影机',                   
'数码相机',
'微单相机',
'相机镜头',
'运动相机',
'单反/微单套机',
'单反/微单相机') then '相机/摄像机'

when fcategory in ('耳机',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔',
'按摩器',
'吹风机',
'磁吸式键盘',
'电子书',
'翻译器',
'风扇',
'加湿器',
'录音笔',
'美发器',
'手写笔',
'智能手写笔',                   
'无人机',
'吸尘器',
'学习机',
'智能办公本',
'智能配饰',
'智能摄像',
'智能手表',
'智能手环') then '智能设备'

when fcategory in ('黄金') then "黄金"
else  '其他' end as fcategory


    ,Forder_id
    ,Fuser_id

from  drt.drt_my33310_recycle_t_xy_order_data 

where
    forder_time  between  date_sub(to_date(now()),1) and  date_sub(now(),1) 

),


xiadan_yes1 as 
(
select 
    to_date(a.dt)  as dt
    ,Fxy_channel 
    ,fsub_channel
    ,fcategory

     ,count(a.Forder_id) as Forder_id
    ,count(distinct a.Fuser_id) as Fuser_id

from  xiadan_yes a inner join  drt.drt_my33310_recycle_t_order b on a.forder_id=b.forder_id

where
    b.ftest=0

group by dt,Fxy_channel,fsub_channel,Fcategory
),

quxiao_yes as 
(
select 
    to_date(dt)  as dt
    ,Fxy_channel 
    ,fsub_channel
    ,fcategory

    ,count(a.Forder_id) as Forder_id
    ,count(distinct a.Fuser_id) as Fuser_id

from  xiadan_yes a inner join  drt.drt_my33310_recycle_t_order_txn  b on a.forder_id=b.forder_id
inner join  drt.drt_my33310_recycle_t_order c on a.forder_id=c.forder_id
where
    b.fauto_create_time  between date_sub(to_date(now()),1) and  date_sub(now(),1) 
    and b.forder_status=80
    and c.ftest=0
    


group by dt,Fxy_channel,fsub_channel,Fcategory
),


gujia_las as 

(		
select  
		to_date(Feva_time) as  dt
		,Fxy_channel 
		,fsub_channel
	,case when fcategory in ('平板','平板电脑') then '平板'
when fcategory in ('笔记本','笔记本电脑') then '笔记本'
when fcategory in ('手机','') then '手机'
when fcategory in ('单反闪光灯',
'单反转接环',
'移动电源',
'移动硬盘',
'云台',
'拍照配件/云台',                
'增距镜') then '3C数码配件'

when fcategory in ('彩色激光多功能一体机',
'复印打印多功能一体机',
'激光打印机',
'墨盒',
'收款机',
'投影机',
'硒鼓粉盒',
'针式打印机') then '办公设备耗材'

when fcategory in ('CPU',
'电脑服务器',
'电脑固态硬盘',
'固态硬盘',                   
'电脑内存',
'内存条',                 
'电脑显卡',
'显卡',                   
'电脑硬件套装',
'电脑主板',
'键盘',
'品牌台机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件',
'游戏机') then '电玩'

when fcategory in ('单反套机',
'单反相机',
'拍立得',
'摄像机',
'摄影机',                   
'数码相机',
'微单相机',
'相机镜头',
'运动相机',
'单反/微单套机',
'单反/微单相机') then '相机/摄像机'

when fcategory in ('耳机',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔',
'按摩器',
'吹风机',
'磁吸式键盘',
'电子书',
'翻译器',
'风扇',
'加湿器',
'录音笔',
'美发器',
'手写笔',
'智能手写笔',                   
'无人机',
'吸尘器',
'学习机',
'智能办公本',
'智能配饰',
'智能摄像',
'智能手表',
'智能手环') then '智能设备'
when fcategory in ('黄金') then "黄金"

else  '其他' end as fcategory


		,count(Fxy_quote_id) as Fxy_quote_id
		,count(distinct Fuser_id) as Fuser_id
			
FROM
	     drt.drt_my33310_recycle_t_xy_eva_data  
WHERE
	    Feva_time between  date_sub(to_date(now()),7) and  date_sub(now(),7) 
		AND Feva_result = 1
        and cast(Ftemplate_type as string) in ('1','3','')
        and Fbusiness_type in ('xyV1','xyV2','','xyV1Gold')
	
group by dt,Fxy_channel,fsub_channel,Fcategory
),

xiadan_las as 
(
select 
    to_date(forder_time)  as dt
    ,Fxy_channel 
    ,fsub_channel
,case when fcategory in ('平板','平板电脑') then '平板'
when fcategory in ('笔记本','笔记本电脑') then '笔记本'
when fcategory in ('手机','') then '手机'
when fcategory in ('单反闪光灯',
'单反转接环',
'移动电源',
'移动硬盘',
'云台',
'拍照配件/云台',                
'增距镜') then '3C数码配件'

when fcategory in ('彩色激光多功能一体机',
'复印打印多功能一体机',
'激光打印机',
'墨盒',
'收款机',
'投影机',
'硒鼓粉盒',
'针式打印机') then '办公设备耗材'

when fcategory in ('CPU',
'电脑服务器',
'电脑固态硬盘',
'固态硬盘',                   
'电脑内存',
'内存条',                 
'电脑显卡',
'显卡',                   
'电脑硬件套装',
'电脑主板',
'键盘',
'品牌台机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件',
'游戏机') then '电玩'

when fcategory in ('单反套机',
'单反相机',
'拍立得',
'摄像机',
'摄影机',                   
'数码相机',
'微单相机',
'相机镜头',
'运动相机',
'单反/微单套机',
'单反/微单相机') then '相机/摄像机'

when fcategory in ('耳机',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔',
'按摩器',
'吹风机',
'磁吸式键盘',
'电子书',
'翻译器',
'风扇',
'加湿器',
'录音笔',
'美发器',
'手写笔',
'智能手写笔',                   
'无人机',
'吸尘器',
'学习机',
'智能办公本',
'智能配饰',
'智能摄像',
'智能手表',
'智能手环') then '智能设备'
when fcategory in ('黄金') then "黄金"

else  '其他' end as fcategory

    ,Forder_id
    ,Fuser_id

from  drt.drt_my33310_recycle_t_xy_order_data 

where
    forder_time  between  date_sub(to_date(now()),7) and  date_sub(now(),7) 


),


xiadan_las1 as 
(
select 
    to_date(a.dt)  as dt
    ,Fxy_channel 
    ,fsub_channel
    ,fcategory

    ,count(a.Forder_id) as Forder_id
    ,count(distinct a.Fuser_id) as Fuser_id

from  xiadan_las a inner join  drt.drt_my33310_recycle_t_order b on a.Forder_id=b.forder_id

where
    b.ftest=0

group by dt,Fxy_channel,fsub_channel,Fcategory
),


quxiao_las as 
(
select 
    to_date(dt)  as dt
    ,Fxy_channel 
    ,fsub_channel
    ,fcategory


    ,count(a.Forder_id) as Forder_id
    ,count(distinct a.Fuser_id) as Fuser_id
from  xiadan_las a inner join  drt.drt_my33310_recycle_t_order_txn  b on a.forder_id=b.forder_id
inner join  drt.drt_my33310_recycle_t_order c on a.forder_id=c.forder_id
where
    b.fauto_create_time  between date_sub(to_date(now()),7) and  date_sub(now(),7) 
    and b.forder_status=80
    and c.ftest=0


group by dt,Fxy_channel,fsub_channel,Fcategory

)


select 
a.dt
,a.Fxy_channel 
,a.fsub_channel
,a.Fcategory
,a.Fxy_quote_id as gujiapv
,a.Fuser_id as gujiauv
,b.Forder_id as xiadanpv
,b.Fuser_id as xiadanuv
,c.Forder_id as quxiaopv
,c.Fuser_id as quxiaouv


from gujia_tod a left join xiadan_tod1 b 
on a.dt=b.dt and a.Fxy_channel =b.Fxy_channel
and a.fsub_channel=b.fsub_channel and a.Fcategory=b.Fcategory
left join quxiao_tod c 
on a.dt=c.dt and a.Fxy_channel =c.Fxy_channel
and a.fsub_channel=c.fsub_channel and a.Fcategory=c.Fcategory


union all



select 
a.dt
,a.Fxy_channel 
,a.fsub_channel
,a.Fcategory
,a.Fxy_quote_id as gujiapv
,a.Fuser_id as gujiauv
,b.Forder_id as xiadanpv
,b.Fuser_id as xiadanuv
,c.Forder_id as quxiaopv
,c.Fuser_id as quxiaouv


from gujia_yes a left join xiadan_yes1 b 
on a.dt=b.dt and a.Fxy_channel =b.Fxy_channel
and a.fsub_channel=b.fsub_channel and a.Fcategory=b.Fcategory
left join quxiao_yes c 
on a.dt=c.dt and a.Fxy_channel =c.Fxy_channel
and a.fsub_channel=c.fsub_channel and a.Fcategory=c.Fcategory

union all 

select 
a.dt
,a.Fxy_channel 
,a.fsub_channel
,a.Fcategory
,a.Fxy_quote_id as gujiapv
,a.Fuser_id as gujiauv
,b.Forder_id as xiadanpv
,b.Fuser_id as xiadanuv
,c.Forder_id as quxiaopv
,c.Fuser_id as quxiaouv


from gujia_las a left join xiadan_las1 b 
on a.dt=b.dt and a.Fxy_channel =b.Fxy_channel
and a.fsub_channel=b.fsub_channel and a.Fcategory=b.Fcategory
left join quxiao_las c 
on a.dt=c.dt and a.Fxy_channel =c.Fxy_channel
and a.fsub_channel=c.fsub_channel and a.Fcategory=c.Fcategory
