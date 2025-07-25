-- 邮寄订单
with orders as 
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
-- ,Ftest
,b.Fship_type
,count(a.forder_id) as 下单量



FROM
	 drt.drt_my33310_recycle_t_order  AS a
	INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
	-- inner join drt.drt_my33310_recycle_t_xianyu_order_map c on a.Forder_id = c.Forder_id 

WHERE
	--  a.Ftest = 0
	b.fship_type=1
    and b.Fxy_channel  IN ('idle')
    and b.Forder_time >=date_sub(to_date(now()),180)
    and a.Forder_time >=date_sub(to_date(now()),180)
group by 1,2,3
),

 daohuo as 
(


select 
substr(a.Fgetin_time,1,10) as dt 
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
-- ,a.Ftest
,b.Fship_type
,count(a.forder_id) as 到货量

FROM
	 drt.drt_my33310_recycle_t_order  AS a
	INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 

WHERE
	 b.fship_type=1
    and b.Fxy_channel  IN ('idle')
    and a.Fgetin_time >=date_sub(to_date(now()),180)
    and b.Forder_time >=date_sub(to_date(now()),180)
group by 1,2,3
 
)

,

jiance as
(
select 
substr(d.fdetect_time,1,10) as dt 
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
-- ,a.Ftest
,b.Fship_type
,count(a.forder_id) as 检测量
,count(if(b.fquote_price>foperation_price,a.forder_id,null)) as 豁免后议价量
,count(if(b.fquote_price>foperation_price and fsync_pay_out_time>=date_sub(to_date(now()),180),a.forder_id,null)) as 豁免后议价成交量
,sum(if(b.fquote_price>foperation_price,b.fquote_price,0)) as 豁免后议价预估价
,sum(if(b.fquote_price>foperation_price,a.foperation_price,0)) as 豁免后议价检测价
,count(if(b.frate_time>=date_sub(to_date(now()),180) and frate_grade=2,a.forder_id,null)) as 检测差评量
/*
,b.fquote_price/100 as 检测预估价
,freal_detect_price/100 as 真实检测价
,a.foperation_price/100 as 最终检测价
,b.fconfirm_fee/100 as 成交价
*/



FROM
	 drt.drt_my33310_recycle_t_order  AS a
	INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
	inner join drt.drt_my33310_recycle_t_detection_info d on b.Forder_id = d.Forder_id 

WHERE
	 fship_type=1
	 and b.Fxy_channel  IN ('idle')
     and d.fdetect_time >=date_sub(to_date(now()),180)
     and a.Forder_time >= date_sub(to_date(now()),180)
     and b.Forder_time >= date_sub(to_date(now()),180)
     
group by 1,2,3 
)

,

chengjiao as
(
select 
substr(b.fsync_pay_out_time,1,10) as dt 
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
-- ,a.Ftest
,b.Fship_type
,count(a.forder_id) as 成交量
,count(if(b.fquote_price>b.fconfirm_fee,a.forder_id,null)) as 成交中议价量
,sum(if(b.fquote_price>fconfirm_fee,b.fquote_price,0)) as 成交中议价预估价
,sum(if(b.fquote_price>fconfirm_fee,b.fconfirm_fee,0)) as 成交中议价成交价
,count(if(b.frate_time>=date_sub(to_date(now()),180) and frate_grade=2,a.forder_id,null)) as 成交差评量
/*
,b.fquote_price/100 as 检测预估价
,freal_detect_price/100 as 真实检测价
,a.foperation_price/100 as 最终检测价
,b.fconfirm_fee/100 as 成交价
*/



FROM
	 drt.drt_my33310_recycle_t_order  AS a
	INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
	

WHERE
	 fship_type=1
	 and b.Fxy_channel  IN ('idle')
     and b.fsync_pay_out_time >=date_sub(to_date(now()),180)
     and a.Forder_time >= date_sub(to_date(now()),180)
    
     
group by 1,2,3
)



select 
orders.dt
,orders.fcategory
,orders.Fship_type
,下单量
,到货量
,检测量
,豁免后议价量
,豁免后议价成交量
,豁免后议价预估价
,豁免后议价检测价
,检测差评量

,成交量
,成交中议价量
,成交中议价预估价
,成交中议价成交价
,成交差评量


from
orders left join daohuo 
on orders.dt=daohuo.dt 
and orders.fcategory=daohuo.fcategory 
and orders.Fship_type=daohuo.Fship_type

left join jiance 
on orders.dt=jiance.dt 
and orders.fcategory=jiance.fcategory 
and orders.Fship_type=jiance.Fship_type

left join chengjiao
on orders.dt=chengjiao.dt 
and orders.fcategory=chengjiao.fcategory 
and orders.Fship_type=chengjiao.Fship_type

where
orders.fcategory in 
('相机/摄像机'
,'影音数码/电器'
,'手机'
,'平板'
,'笔记本'
,'智能设备'
 )
