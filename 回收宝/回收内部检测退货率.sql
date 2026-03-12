

with a as 

(
select 
			 to_date(b.fcheck_end_time) as Fdetect_time
		    ,a.Fxy_channel
		    ,a.Fcategory
			,a.forder_id
  			,b.fproduct_name
		
					
from 
			 drt.drt_my33310_recycle_t_xy_order_data  a left join drt.drt_my33310_recycle_t_order  b on a.Forder_id=b.Forder_id
		
			
where 
	 b.fcheck_end_time  between   '2025-01-01 00:00:00' and now()
	 and b.ftest=0
)

,
b as 


(
select 
			 to_date(b.fcheck_end_time) as Fdetect_time
		    ,a.Fxy_channel
		    ,a.Fcategory
			,a.forder_id
  			,b.fproduct_name
		
					
from 
				 drt.drt_my33310_recycle_t_xy_order_data  a left join  drt.drt_my33310_recycle_t_order  b on a.Forder_id=b.Forder_id
			
where 
 b.fcheck_end_time  between   '2025-01-01 00:00:00' and now()
 and b.ftest=0
and b.Forder_status in (80,90,110,120)

)

select 
a.Fdetect_time


,case when a.fcategory in ('平板','平板电脑') then '平板'
when a.fcategory in ('笔记本','笔记本电脑') then '笔记本'
when a.fcategory in ('手机','') then '手机'
when a.fcategory in ('单反闪光灯',
'单反转接环',
'移动电源',
'移动硬盘',
'云台',
'拍照配件/云台',                
'增距镜') then '3C数码配件'

when a.fcategory in ('彩色激光多功能一体机',
'复印打印多功能一体机',
'激光打印机',
'墨盒',
'收款机',
'投影机',
'硒鼓粉盒',
'针式打印机') then '办公设备耗材'

when a.fcategory in ('CPU',
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

when a.fcategory in ('路由器') then '网络设备'

when a.fcategory in ('PS游戏光盘/软件',
'其他游戏配件',
'游戏机') then '电玩'

when a.fcategory in ('单反套机',
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

when a.fcategory in ('耳机',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'


when a.fcategory in ('PS4游戏',
'PS5游戏',
'Switch游戏') then '游戏卡'



when a.fcategory in ('VR眼镜头盔',
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


else  a.fcategory end as fcategory
,a.Fxy_channel
,a.fproduct_name
,count(a.forder_id) as checknum
,count(b.forder_id) as sendbacknum

from a left join b on a.forder_id=b.forder_id

group by a.Fdetect_time
,a.Fxy_channel
,a.Fcategory
,a.fproduct_name
