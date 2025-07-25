
with z as 
  (-- 估价成功下单表
  SELECT
        to_date(Feva_time) AS Feva_time,
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


else  fcategory end as fcategory,

      Fxy_quote_id AS Fxy_quote_id,
      Fuser_id AS Fuser_id
   
    FROM
    drt.drt_my33310_recycle_t_xy_eva_data  

    WHERE
      Feva_time between '2025-06-25 00:00:00' and '2025-07-24 23:59:59'
      and fthe_month>=202409
      AND Feva_result = 1
    
	and Fxy_channel in ('tmall-service') 
    and fsub_channel not in ('xunjian')
	and cast(Ftemplate_type as string) in ('1','3','')
    and Fbusiness_type in ('xyV1','xyV2','','xyV1Gold')
  ) 
,	

c as
(
select 
	a.forder_time,
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


else  fcategory end as fcategory
	,a.forder_id as 下单pv
	,fxy_order_id
	,a.fuser_id as 下单uv
  	,Fsync_pay_out_time
  	,if(Fsync_pay_out_time>='2025-06-25 00:00:00' ,(unix_timestamp(Fsync_pay_out_time)-unix_timestamp(a.forder_time))/3600,0) as diff
	,if(Fsync_pay_out_time>='2025-06-25 00:00:00' ,a.forder_id,null) as 成交pv
	,if(Fsync_pay_out_time>='2025-06-25 00:00:00' ,a.fuser_id,null) as 成交uv
  
  	,if(fpay_out_time>='2025-06-25 00:00:00' ,a.forder_id,null) as 内部成交pv
	,if(fpay_out_time>='2025-06-25 00:00:00' ,a.fuser_id,null) as 内部成交uv
  
  	,if(fpay_out_time>='2025-06-25 00:00:00' and Fquote_price>=fpay_out_price,Fquote_price,null) as 内部成交预估价
	,if(fpay_out_time>='2025-06-25 00:00:00' and Fquote_price>=fpay_out_price,fpay_out_price,null) as 内部成交价
  
  
  
	,if(Fsync_pay_out_time>='2025-06-25 00:00:00' and Fquote_price>=Fconfirm_fee,Fquote_price,null) as 成交预估价
	,if(Fsync_pay_out_time>='2025-06-25 00:00:00' and Fquote_price>=Fconfirm_fee,Fconfirm_fee,null) as 成交成交价
  
  
  
    ,if(Frate_time>='2025-06-25 00:00:00' and Fsync_pay_out_time>='2025-06-25 00:00:00'  ,a.forder_id,null) as 评价pv
    ,if(Frate_time>='2025-06-25 00:00:00' and Fsync_pay_out_time>='2025-06-25 00:00:00' and Frate_grade in (1,8),a.forder_id,null) as 好评pv
    ,if(Frate_time>='2025-06-25 00:00:00' and Fsync_pay_out_time>='2025-06-25 00:00:00' and Frate_grade in (5),a.forder_id,null) as 中评pv
	
	
	,if(Fgetin_time>='2025-06-25 00:00:00' and a.fship_type=1,a.forder_id,null) as 到货pv
	,if(Fgetin_time>='2025-06-25 00:00:00' and a.fship_type=1,a.fuser_id,null) as 到货uv
	

  	,if(Fsync_pay_out_time>='2025-06-25 00:00:00' ,Fquote_price,null) as 全成交预估价
	,if(Fsync_pay_out_time>='2025-06-25 00:00:00' ,Fconfirm_fee,null) as 全成交成交价
					
from 
	drt.drt_my33310_recycle_t_xy_order_data  a inner join  drt.drt_my33310_recycle_t_order b on a.forder_id=b.forder_id

where 
	a.Forder_time between '2025-06-25 00:00:00' and '2025-07-24 23:59:59'
   
		-- and a.Fxy_channel not in ('tmall-service','tm_recycle','rm_recycle') 
	and Fxy_channel in ('tmall-service') 
	and b.ftest=0
	
	
	
	
	
	

	),
	
shangmen as
(
select 
	to_date(a.forder_time) AS forder_time,
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


else  fcategory end as fcategory
	
	,if(fengineer_detection_time>='2025-06-25 00:00:00' and a.fship_type!=1,a.forder_id,null) as 上门检测pv
	,if(fengineer_detection_time>='2025-06-25 00:00:00' and a.fship_type!=1,a.fuser_id,null) as 上门检测uv
	
					
from 
	drt.drt_my33310_recycle_t_xy_order_data  a inner join  drt.drt_my33310_recycle_t_order b on a.forder_id=b.forder_id
    inner join drt.drt_my33310_recycle_t_xyxz_order c on a.forder_id=c.forder_id
where 
	a.Forder_time between '2025-06-25 00:00:00' and '2025-07-24 23:59:59'
   
		-- and a.Fxy_channel not in ('tmall-service','tm_recycle','rm_recycle') 
	and Fxy_channel in ('tmall-service') 
	and a.fship_type=2
		and b.ftest=0
	
	
	

	),
	
	
x as
(
select 
    to_date(c.forder_time) as forder_time
  
  	,c.Fcategory
    ,count(c.下单pv) as  下单pv
    ,count(distinct c.下单uv) as  下单uv
    ,count(c.成交pv) as  成交pv
    ,count(distinct c.成交uv) as  成交uv
    ,sum(成交预估价)/100 as 成交预估价
    ,sum(成交成交价)/100 as 成交成交价
    ,count(评价pv) as 评价pv
    ,count(好评pv) as 好评pv
  	,count(中评pv) as 中评pv
    
    ,count(c.到货pv) as  到货pv
    ,count(distinct c.到货uv) as 到货uv
    
    ,count(shangmen.上门检测pv) as  上门检测pv
    ,count(distinct shangmen.上门检测uv) as 上门检测uv
    ,sum(c.diff) as diff
  
  	,sum(全成交预估价)/100 as 全成交预估价
  	,sum(全成交成交价)/100 as 全成交成交价
  
  	 ,count(c.内部成交pv) as  内部成交pv
    ,count(distinct c.内部成交uv) as  内部成交uv
    ,sum(内部成交预估价)/100 as 内部成交预估价
    ,sum(内部成交价)/100 as 内部成交价
  
					
    from 
    	c left join shangmen on c.下单pv=shangmen.上门检测pv
    group by 1,2
	)
	,
g as 
(
    select 
    Feva_time
  	,Fcategory
    ,count(z.Fxy_quote_id) as 估价成功pv
    ,count(distinct z.Fuser_id ) as 估价成功uv
    from
    z
    group by 1,2
),


d as 
(
select 
to_date(forder_time) as forder_time,Fcategory,
count(a.Fxy_order_id) as 客诉量,
count(distinct c.下单uv) as 客诉uv
from drt.drt_my33310_recycle_t_tmall_fuwu_anomaly_basic_info a 
inner join c  on a.fxy_order_id=c.fxy_order_id
-- WHERE Fsync_pay_out_time>='2023-11-26 00:00:00' 
group by 1,2

)
,

chunhuishou as
(
select 
	to_date(a.forder_time) as forder_time,

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



when fcategory in (
'VR虚拟现实',
'VR眼镜头盔',
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
    


else  fcategory end as fcategory
  
  
  /*
	,a.forder_id as 下单pv
	,fxy_order_id
	,a.fuser_id as 下单uv
  	,Fsync_pay_out_time
	,if(Fsync_pay_out_time>='2025-06-25 00:00:00',a.forder_id,null) as 成交pv
	,if(Fsync_pay_out_time>='2025-06-25 00:00:00',a.fuser_id,null) as 成交uv
	*/
	,sum(Fquote_price)/100 as 天猫成交预估价
	,sum(Fconfirm_fee)/100 as 天猫成交成交价


					
from 
	drt.drt_my33310_recycle_t_xy_order_data  a inner join  drt.drt_my33310_recycle_t_order b on a.forder_id=b.forder_id

where 
	a.Forder_time between '2025-06-25 00:00:00' and '2025-07-24 23:59:59'
	and Fsync_pay_out_time>='2025-06-25 00:00:00'
	and Fquote_price>=Fconfirm_fee
    and a.Fxy_channel  in ('tmall-service','tm_recycle','rm_recycle') 
    and b.ftest=0
group by 1,2	
	
	
	
	

	)


SELECT
g.Feva_time as 日期,g.Fcategory
,g.估价成功pv,g.估价成功uv,下单pv,下单uv,成交pv,成交uv,成交预估价,成交成交价,评价pv,好评pv,中评pv,客诉量,客诉uv
,到货pv,到货uv,上门检测pv,上门检测uv,diff,全成交预估价,全成交成交价,天猫成交预估价,天猫成交成交价,内部成交pv,内部成交uv,内部成交预估价,内部成交价
from x inner join g on g.Feva_time=x.forder_time  and g.Fcategory=x.Fcategory 
left join d on g.Feva_time=d.forder_time and g.Fcategory=d.Fcategory
left join chunhuishou on g.Feva_time=chunhuishou.forder_time and g.Fcategory=chunhuishou.Fcategory

