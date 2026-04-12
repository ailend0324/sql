with
t as 
(
select Fxy_order_id,Fcreate_dtime as Fsync_pay_out_time
from
drt.drt_my33310_recycle_t_xianyu_order_txn
where
Fxy_order_status=5
and Fmsg_deal_complete=1
and Fcreate_dtime>= "2023-01-01"
),

chengjiao as 
(
select 
substr(b.Fsync_pay_out_time,1,10) as dt
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
'激光打印机','打印机',
'墨盒',
'收款机','投影仪',
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
'品牌台式机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件','游戏手柄',
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
'黑胶唱片机','MP3/MP4',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏','游戏卡',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔','VR虚拟现实',
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

when fcategory in ("黄金") then "黄金"



else  '其他' end as fcategory

-- ,fcategory as 子类目
  
    ,case when b.Fxy_channel  in ('tmall-service')  then '以旧换新'
   when b.Fxy_channel  in ('idle') then '闲鱼idle'
    else '闲鱼非idle' end as 渠道
,b.fship_type
,d.ftest
,count(b.Forder_id) as 成交量
,sum(b.Fconfirm_fee)/100 as 成交金额
,count(if(Frate_time>="2023-01-01",b.Forder_id,null)) as 完结评价量
,count(if(Frate_time>="2023-01-01" and Frate_grade in (1,5,8),b.Forder_id,null)) as 完结好评量



	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map c on b.Forder_id = c.Forder_id 
		-- inner join t on c.Fxy_order_id = t.Fxy_order_id 
		inner join  drt.drt_my33310_recycle_t_order d on  b.Forder_id = d.Forder_id 
	where
	 b.Fsync_pay_out_time  >= "2023-01-01"

	
    group by 1,2,3,4,5
),

xiadan as 
(
select 
substr(b.forder_time,1,10) as dt
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
'激光打印机','打印机',
'墨盒',
'收款机','投影仪',
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
'品牌台式机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件','游戏手柄',
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
'黑胶唱片机','MP3/MP4',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏','游戏卡',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔','VR虚拟现实',
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

when fcategory in ("黄金") then "黄金"

else  '其他' end as fcategory

-- ,fcategory as 子类目
  
  
   ,case when b.Fxy_channel  in ('tmall-service')  then '以旧换新'
   when b.Fxy_channel  in ('idle') then '闲鱼idle'
    else '闲鱼非idle' end as 渠道
,b.fship_type
,d.ftest
,count(b.Forder_id) as 下单量
,count(distinct if(d.forder_status in (10,220,210),b.forder_id,null)) as 未完结



	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map c on b.Forder_id = c.Forder_id 
		inner join  drt.drt_my33310_recycle_t_order d on  b.Forder_id = d.Forder_id 
	
	where
 b.forder_time  >= "2023-01-01"
 
	

group by 1,2,3,4,5
),
daodian as 
(
 select forder_id,Fauto_create_time as Fauto_create_time
from
 drt.drt_my33310_recycle_t_order_txn
where
Forder_status=45
and Fauto_create_time>= "2023-01-01"

 ), 
 
 zhijian as 
(
 select forder_id,Fauto_create_time as Fauto_create_time
from
 drt.drt_my33310_recycle_t_order_txn
where
Forder_status=250
and Fauto_create_time>= "2023-01-01"

 ), 
 
  quxiao as 
(
 select forder_id,Fauto_create_time as Fauto_create_time
from
 drt.drt_my33310_recycle_t_order_txn
where
Forder_status=80
and Fauto_create_time>= "2023-01-01"

 ), 
  
  
daohuo as 
(
select 
substr(d.Fgetin_time,1,10) as dt
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
'激光打印机','打印机',
'墨盒',
'收款机','投影仪',
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
'品牌台式机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件','游戏手柄',
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
'黑胶唱片机','MP3/MP4',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏','游戏卡',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔','VR虚拟现实',
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
when fcategory in ("黄金") then "黄金"


else  '其他' end as fcategory


-- ,fcategory as 子类目
  
   ,case when b.Fxy_channel  in ('tmall-service')  then '以旧换新'
   when b.Fxy_channel  in ('idle') then '闲鱼idle'
    else '闲鱼非idle' end as 渠道
-- ,c.Fcity
,b.fship_type
,d.ftest
,count(b.Forder_id) as 到货量



	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map c on b.Forder_id = c.Forder_id 
		inner join  drt.drt_my33310_recycle_t_order d on  b.Forder_id = d.Forder_id 
	
	where
 d.Fgetin_time  >= "2023-01-01"

	 and b.Fship_type=1
group by 1,2,3,4,5

	union all

SELECT
	
		substr(c.Fengineer_detection_time,1,10) as dt

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
'激光打印机','打印机',
'墨盒',
'收款机','投影仪',
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
'品牌台式机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件','游戏手柄',
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
'黑胶唱片机','MP3/MP4',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏','游戏卡',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔','VR虚拟现实',
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
when fcategory in ("黄金") then "黄金"


else  '其他' end as fcategory

-- ,fcategory as 子类目
  
   ,case when b.Fxy_channel  in ('tmall-service')  then '以旧换新'
   when b.Fxy_channel  in ('idle') then '闲鱼idle'
    else '闲鱼非idle' end as 渠道
			,b.fship_type
            ,a.ftest
		,count(a.Forder_id) as 到货量
		
	FROM
			 drt.drt_my33310_recycle_t_order  AS a
		INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
		left join drt.drt_my33310_recycle_t_xyxz_order c on a.Forder_id = c.Forder_id 
	WHERE
		 c.Fengineer_detection_time   >= "2023-01-01"
		
	 and b.Fship_type=2
	 	group by 1,2,3,4,5
  
  
  	union all

SELECT
	
		substr(d.Fauto_create_time,1,10) as dt

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
'激光打印机','打印机',
'墨盒',
'收款机','投影仪',
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
'品牌台式机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件','游戏手柄',
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
'黑胶唱片机','MP3/MP4',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏','游戏卡',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔','VR虚拟现实',
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
when fcategory in ("黄金") then "黄金"


else  '其他' end as fcategory

-- ,fcategory as 子类目
  
   ,case when b.Fxy_channel  in ('tmall-service')  then '以旧换新'
   when b.Fxy_channel  in ('idle') then '闲鱼idle'
    else '闲鱼非idle' end as 渠道
			,b.fship_type
            ,a.ftest
		,count(a.Forder_id) as 到货量
		
	FROM
			 drt.drt_my33310_recycle_t_order  AS a
		INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
  		inner join daodian d on a.forder_id=d.forder_id
		left join drt.drt_my33310_recycle_t_order_snapshot c on a.Forder_id = c.Forder_id 
	WHERE
		 d.Fauto_create_time   >= "2023-01-01"
		
	 and b.Fship_type=3
	 	group by 1,2,3,4,5
  
  
  
  
),


jiance as 
(
select 
substr(d.fcheck_end_time,1,10) as dt

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
'激光打印机','打印机',
'墨盒',
'收款机','投影仪',
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
'品牌台式机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件','游戏手柄',
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
'黑胶唱片机','MP3/MP4',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏','游戏卡',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔','VR虚拟现实',
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
 when fcategory in ("黄金") then "黄金"


else  '其他' end as fcategory


-- ,fcategory as 子类目
  
   ,case when b.Fxy_channel  in ('tmall-service')  then '以旧换新'
   when b.Fxy_channel  in ('idle') then '闲鱼idle'
    else '闲鱼非idle' end as 渠道
-- ,c.Fcity
,b.fship_type
,d.ftest
,count(b.Forder_id) as 检测量
,count(if(d.forder_status=80,b.Forder_id,null)) as 检测后取消量



	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map c on b.Forder_id = c.Forder_id 
		inner join  drt.drt_my33310_recycle_t_order d on  b.Forder_id = d.Forder_id 
	
	where
 d.fcheck_end_time  >= "2023-01-01"

	 and b.Fship_type=1
group by 1,2,3,4,5

	union all

SELECT
	
		substr(c.Fauto_create_time,1,10) as dt

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
'激光打印机','打印机',
'墨盒',
'收款机','投影仪',
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
'品牌台式机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件','游戏手柄',
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
'黑胶唱片机','MP3/MP4',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏','游戏卡',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔','VR虚拟现实',
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
when fcategory in ("黄金") then "黄金"


else  '其他' end as fcategory


-- ,fcategory as 子类目
  
   ,case when b.Fxy_channel  in ('tmall-service')  then '以旧换新'
   when b.Fxy_channel  in ('idle') then '闲鱼idle'
    else '闲鱼非idle' end as 渠道
			,b.fship_type
            ,a.ftest
		,count(a.Forder_id) as 检测量
		,count(if(a.forder_status=80,a.Forder_id,null)) as 检测后取消量
		
	FROM
			 drt.drt_my33310_recycle_t_order  AS a
		INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
		inner join zhijian c on a.Forder_id = c.Forder_id 
	WHERE
		 c.Fauto_create_time >= "2023-01-01"
		
	 and b.Fship_type in (2,3)
	 	group by 1,2,3,4,5
  


),

  
quxiaoliang as 
(
select 
substr(q.Fauto_create_time,1,10) as dt


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
'激光打印机','打印机',
'墨盒',
'收款机','投影仪',
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
'品牌台式机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件','游戏手柄',
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
'黑胶唱片机','MP3/MP4',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏','游戏卡',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔','VR虚拟现实',
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
 when fcategory in ("黄金") then "黄金"


else  '其他' end as fcategory


-- ,fcategory as 子类目
  
   ,case when b.Fxy_channel  in ('tmall-service')  then '以旧换新'
   when b.Fxy_channel  in ('idle') then '闲鱼idle'
    else '闲鱼非idle' end as 渠道
-- ,c.Fcity
,b.fship_type
,d.ftest
,count(b.Forder_id) as 取消量
,count(if((unix_timestamp(q.Fauto_create_time)-unix_timestamp(b.forder_time))<600,b.forder_id,null)) as 10分钟内取消量



	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map c on b.Forder_id = c.Forder_id 
		inner join  drt.drt_my33310_recycle_t_order d on  b.Forder_id = d.Forder_id 
		inner join  quxiao q on  b.Forder_id = q.Forder_id 
	
	where
 q.Fauto_create_time  >= "2023-01-01"

group by 1,2,3,4,5
)
,
pingjia as 
(
select 
substr(b.Frate_time,1,10) as dt
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
'激光打印机','打印机',
'墨盒',
'收款机','投影仪',
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
'品牌台式机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件','游戏手柄',
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
'黑胶唱片机','MP3/MP4',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏','游戏卡',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔','VR虚拟现实',
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
when fcategory in ("黄金") then "黄金"

else  '其他' end as fcategory

-- ,fcategory as 子类目
  
  
   ,case when b.Fxy_channel  in ('tmall-service')  then '以旧换新'
   when b.Fxy_channel  in ('idle') then '闲鱼idle'
    else '闲鱼非idle' end as 渠道
,b.fship_type
,d.ftest
,count(b.Forder_id) as 评价量
,count(distinct if(Frate_grade in (1,5,8),b.forder_id,null)) as 好评量



	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map c on b.Forder_id = c.Forder_id 
		inner join  drt.drt_my33310_recycle_t_order d on  b.Forder_id = d.Forder_id 
	
	where
 b.Frate_time  >= "2023-01-01"
 
	

group by 1,2,3,4,5
)

,
base as 
(
select 
case when fcategory in ('平板','平板电脑') then '平板'
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
'激光打印机','打印机',
'墨盒',
'收款机','投影仪',
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
'品牌台式机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件','游戏手柄',
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
'黑胶唱片机','MP3/MP4',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏','游戏卡',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔','VR虚拟现实',
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
 when fcategory in ("黄金") then "黄金"



else  '其他' end as fcategory

-- ,fcategory as 子类目
  
  
   ,case when b.Fxy_channel  in ('tmall-service')  then '以旧换新'
   when b.Fxy_channel  in ('idle') then '闲鱼idle'
    else '闲鱼非idle' end as 渠道
,b.fship_type
,ftest


	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
  inner join drt.drt_my33310_recycle_t_order a on b.forder_id=a.forder_id
	
	
	where
 b.forder_time  >= "2024-01-01"
 
group by 1,2,3,4



)

,
riqi as 
(
select 
substr(b.forder_time,1,10) as dt
	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
	where
 b.forder_time  >= "2023-01-01"
 
group by 1

)
,

neibu as 
(
select 
substr(d.fpay_out_time,1,10) as dt
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
'激光打印机','打印机',
'墨盒',
'收款机','投影仪',
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
'品牌台式机',
'无线鼠标',
'显示器',
'一体机',
'组装台机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件','游戏手柄',
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
'黑胶唱片机','MP3/MP4',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏','游戏卡',
'PS5游戏',
'Switch游戏') then '游戏卡'



when fcategory in ('VR眼镜头盔','VR虚拟现实',
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
  
when fcategory in ("黄金") then "黄金"


else  '其他' end as fcategory

-- ,fcategory as 子类目
  
  
   ,case when b.Fxy_channel  in ('tmall-service')  then '以旧换新'
   when b.Fxy_channel  in ('idle') then '闲鱼idle'
    else '闲鱼非idle' end as 渠道
,b.fship_type
,d.ftest
,count(b.Forder_id) as 内部成交量
,sum(d.fpay_out_price)/100 as 内部成交金额



	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map c on b.Forder_id = c.Forder_id 
		inner join  drt.drt_my33310_recycle_t_order d on  b.Forder_id = d.Forder_id 
	where
	 d.fpay_out_time  >= "2023-01-01"
 
	

group by 1,2,3,4,5
)



select 
riqi.dt
,base.Fcategory
-- ,a.子类目
,base.渠道

,case base.fship_type 
when 1 then "邮寄"
when 2 then "上门"
when 3 then "到店"
end as fship_type
,base.ftest
,下单量
,到货量
,成交量
,成交金额
,取消量
,检测量
,未完结 as 检测后取消量
,10分钟内取消量
,评价量
,好评量
,完结评价量
,完结好评量
,内部成交量
,内部成交金额


from
base 
left join 
xiadan a on  a.Fcategory=base.Fcategory and a.fship_type=base.fship_type and a.ftest=base.ftest
and a.渠道=base.渠道

right join 
riqi  on riqi.dt=a.dt

left join daohuo b  
on riqi.dt=b.dt and base.Fcategory=b.Fcategory and base.fship_type=b.fship_type and base.ftest=b.ftest
and base.渠道=b.渠道
-- and a.子类目=b.子类目

left join chengjiao  c 
on riqi.dt=c.dt and base.Fcategory=c.Fcategory and base.fship_type=c.fship_type and base.ftest=c.ftest
and base.渠道=c.渠道
-- and a.子类目=c.子类目

left join quxiaoliang  d
on riqi.dt=d.dt and base.Fcategory=d.Fcategory and base.fship_type=d.fship_type and base.ftest=d.ftest
and base.渠道=d.渠道
-- and a.子类目=d.子类目


left join jiance  e
on riqi.dt=e.dt and base.Fcategory=e.Fcategory and base.fship_type=e.fship_type and base.ftest=e.ftest
and base.渠道=e.渠道
-- and a.子类目=e.子类目

left join pingjia  f
on riqi.dt=f.dt and base.Fcategory=f.Fcategory and base.fship_type=f.fship_type and base.ftest=f.ftest
and base.渠道=f.渠道
-- and a.子类目=e.子类目

left join neibu  g
on riqi.dt=g.dt and base.Fcategory=g.Fcategory and base.fship_type=g.fship_type and base.ftest=g.ftest
and base.渠道=g.渠道
-- and a.子类目=e.子类目
