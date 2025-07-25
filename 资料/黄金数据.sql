with
t as 
(
select Fxy_order_id,Fcreate_dtime as Fsync_pay_out_time
from
drt.drt_my33310_recycle_t_xianyu_order_txn
where
Fxy_order_status=5
and Fmsg_deal_complete=1
and Fcreate_dtime>= to_date(date_sub(now(),366))
),

chengjiao as 
(
select 
substr(t.Fsync_pay_out_time,1,10) as dt
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
'收款机',
'投影机','投影仪',
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
'电脑硬件套装','硬件套装',
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

when fcategory in ('耳机','MP3/MP4',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏','游戏卡') then '游戏卡'



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

when fcategory in ('黄金') then '黄金'

else  fcategory end as fcategory

-- ,fcategory as 子类目
,b.fxy_channel

,b.fship_type

,count(b.Forder_id) as 成交量
,sum(b.Fconfirm_fee)/100 as 成交金额


	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
	inner join t on b.Fxy_order_id = t.Fxy_order_id 

	where
	 t.Fsync_pay_out_time  >= to_date(date_sub(now(),366))
    
   
    group by 1,2,3,4
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
'收款机',
'投影机','投影仪',
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
'电脑硬件套装','硬件套装',
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

when fcategory in ('耳机','MP3/MP4',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏','游戏卡') then '游戏卡'



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

when fcategory in ('黄金') then '黄金'

else  fcategory end as fcategory

,fxy_channel 
  

,b.fship_type

,count(b.Forder_id) as 下单量
,sum(b.fquote_price)/100 as 预估价


	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
	
	where
 b.forder_time  >= to_date(date_sub(now(),366))

group by 1,2,3,4
),

 
  quxiao as 
(
 select q.forder_id,q.Fauto_create_time as Fauto_create_time,fxy_channel
 ,if((unix_timestamp(q.Fauto_create_time)-unix_timestamp(a.forder_time))<600,a.forder_id,null) as 10分钟内取消量

from
 drt.drt_my33310_recycle_t_order_txn q inner join 
 drt.drt_my33310_recycle_t_xy_order_data  a on q.forder_id=a.forder_id
where
Forder_status=80
and q.Fauto_create_time>= to_date(date_sub(now(),366))

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
'收款机',
'投影机','投影仪',
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
'电脑硬件套装','硬件套装',
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

when fcategory in ('耳机','MP3/MP4',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏','游戏卡') then '游戏卡'



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

when fcategory in ('黄金') then '黄金'

else  fcategory end as fcategory

,fxy_channel
  

-- ,c.Fcity
,b.fship_type

,count(b.Forder_id) as 到货量



	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
		inner join  drt.drt_my33310_recycle_t_order d on  b.Forder_id = d.Forder_id 
	
	where
 d.Fgetin_time  >= to_date(date_sub(now(),366))
	 and b.Fship_type=1
group by 1,2,3,4

	union all

SELECT
	
		substr(ifnull(c.fengineer_arrive_time,c.fengineer_set_out_time),1,10) as dt

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
'收款机',
'投影机','投影仪',
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
'电脑硬件套装','硬件套装',
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

when fcategory in ('耳机','MP3/MP4',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏','游戏卡') then '游戏卡'



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

when fcategory in ('黄金') then '黄金'

else  fcategory end as fcategory
,b.fxy_channel
  

			,b.fship_type
          
		,count(a.Forder_id) as 到货量
		
	FROM
			 drt.drt_my33310_recycle_t_order  AS a
		INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
		left join drt.drt_my33310_recycle_t_xyxz_order c on a.Forder_id = c.Forder_id 
	WHERE
		ifnull(c.fengineer_arrive_time,c.fengineer_set_out_time)>= to_date(date_sub(now(),366))
	    and b.Fship_type in (2,3)
	
	 	group by 1,2,3,4
  
  /*
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


else  '其他' end as fcategory

-- ,fcategory as 子类目

			,b.fship_type
       
		,count(a.Forder_id) as 到货量
		
	FROM
			 drt.drt_my33310_recycle_t_order  AS a
		INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
  		inner join daodian d on a.forder_id=d.forder_id
		left join drt.drt_my33310_recycle_t_order_snapshot c on a.Forder_id = c.Forder_id 
	WHERE
		 d.Fauto_create_time   >= to_date(date_sub(now(),60))
		
	 and b.Fship_type=3
	 	group by 1,2,3
  
  */
  
  
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
'收款机',
'投影机','投影仪',
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
'电脑硬件套装','硬件套装',
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

when fcategory in ('耳机','MP3/MP4',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏','游戏卡') then '游戏卡'



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

when fcategory in ('黄金') then '黄金'

else  fcategory end as fcategory

 ,b.fxy_channel
  

-- ,c.Fcity
,b.fship_type

,count(b.Forder_id) as 取消量
,count(if((unix_timestamp(q.Fauto_create_time)-unix_timestamp(b.forder_time))<600,b.forder_id,null)) as 10分钟内取消量



	FROM
	  drt.drt_my33310_recycle_t_xy_order_data  AS b 
	    inner join  quxiao q on  b.Forder_id = q.Forder_id 
	
	where
         q.Fauto_create_time   >= to_date(date_sub(now(),366))
      --  and ( b.fsub_channel like "hjbt%" or b.fsub_channel in ("sspush","detail","dcpush"))

group by 1,2,3,4
)
,

 z as 
  (-- 估价成功下单表
    select 
    substr(Feva_time,1,10) as dt
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
'收款机',
'投影机','投影仪',
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
'电脑硬件套装','硬件套装',
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

when fcategory in ('耳机','MP3/MP4',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏','游戏卡') then '游戏卡'



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

when fcategory in ('黄金') then '黄金'

else  fcategory end as fcategory
     , Fxy_quote_id AS Fxy_quote_id
     ,fxy_channel

    FROM
    drt.drt_my33310_recycle_t_xy_eva_data  

    WHERE
      Feva_time  >= to_date(date_sub(now(),366))
      and fthe_month>=202409
      AND Feva_result = 1

     
	and cast(Ftemplate_type as string) in ('1','')
	and Fbusiness_type in ('xyV1','xyV2','','xyV1Gold')

  ) 
,	

p as
(
select 

     substr(a.forder_time,1,10) as dt
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
'收款机',
'投影机','投影仪',
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
'电脑硬件套装','硬件套装',
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

when fcategory in ('耳机','MP3/MP4',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏','游戏卡') then '游戏卡'



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

when fcategory in ('黄金') then '黄金'

else  fcategory end as fcategory

,fship_type
,a.fxy_channel
    
	,count(Fxy_quote_id) as ordernum
	,sum(a.fquote_price)/100 as fquote_price
	,count(if(Fsync_pay_out_time>=to_date(date_sub(now(),60)),a.Fxy_quote_id,null)) as dealnum
	,sum(if(Fsync_pay_out_time>=to_date(date_sub(now(),60)),fconfirm_fee/100,0)) as dealprice
	,count(if(c.fgetin_time is not null and a.fship_type =1,a.Fxy_quote_id,null)) as getin
	,count(if(ifnull(b.fengineer_arrive_time,b.fengineer_set_out_time) is not null and a.fship_type !=1,a.Fxy_quote_id,null)) as shangmen
	,count(quxiao.forder_id) as cancelnum
	,count(quxiao.10分钟内取消量) as 静态10分钟内取消量
	
	
	
from 
	drt.drt_my33310_recycle_t_xy_order_data a  
	left join drt.drt_my33310_recycle_t_xyxz_order b on a.forder_id=b.forder_id
	left join drt.drt_my33310_recycle_t_order c on a.forder_id=c.forder_id
	left join quxiao on a.forder_id=quxiao.forder_id
where 
	a.Forder_time >= to_date(date_sub(now(),366))
	and c.Forder_time >= to_date(date_sub(now(),366))
  
    group by 1,2,3,4

	)
,
gujia as 
(
select 
dt
,fcategory
,fxy_channel
,1 as fship_type
,count(Fxy_quote_id) as 估价量

from z
group by 1,2,3,4

union all
select 
dt
,fcategory
,fxy_channel
,2 as fship_type
,count(Fxy_quote_id) as 估价量

from z
group by 1,2,3,4
union all
select 
dt
,fcategory
,fxy_channel
,3 as fship_type
,count(Fxy_quote_id) as 估价量

from z
group by 1,2,3,4

)



select 
gujia.dt
,gujia.Fcategory

,case gujia.fship_type 
when 1 then "邮寄"
when 2 then "上门"
when 3 then "到店"
end as fship_type
,gujia.fxy_channel
,估价量
,ordernum
,fquote_price
,dealnum
,dealprice
,getin
,shangmen
,cancelnum
,10分钟内取消量

,下单量
,预估价
,到货量
,成交量
,成交金额
,取消量
,静态10分钟内取消量




from
gujia  left join xiadan a on gujia.dt=a.dt 
and gujia.Fcategory=a.Fcategory 
and gujia.fship_type=a.fship_type 
and gujia.fxy_channel=a.fxy_channel

left join daohuo b  
on a.dt=b.dt and a.Fcategory=b.Fcategory and a.fship_type=b.fship_type 
and a.fxy_channel=b.fxy_channel
left join chengjiao  c 
on a.dt=c.dt and a.Fcategory=c.Fcategory and a.fship_type=c.fship_type 
and a.fxy_channel=c.fxy_channel
left join quxiaoliang  d
on a.dt=d.dt and a.Fcategory=d.Fcategory and a.fship_type=d.fship_type 
and a.fxy_channel=d.fxy_channel

left join  p on a.dt=p.dt 
and a.Fcategory=p.Fcategory 
and a.fship_type=p.fship_type 
and a.fxy_channel=p.fxy_channel


