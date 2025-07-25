
with


chengjiao as 

(
select 

Forder_create_time 
,Fship_type
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


else  fcategory end as fcategory
,fxy_channel
,ftest
,sum(cj) as cj
,sum(cjj) as cjj


from
(



select 
substr(a.fpay_out_time,1,10) as Forder_create_time
,case when Fship_type=2 then'上门' when Fship_type=3 then'到店' when Fship_type=1 then '邮寄' else '其他' end as Fship_type
-- ,case when Fship_type=2 then'上门' when Fship_type=3 then'到店' else '邮寄'end as Fship_type
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


else  fcategory end as fcategory
,case when fxy_channel in ('tmall-service','tm_recycle','rm_recycle')  then'天猫'  else '闲鱼'end as fxy_channel

,ftest
,count(a.forder_id) as cj
,sum(a.fpay_out_price)/100 as cjj
from
 drt.drt_my33310_recycle_t_order  a  inner join  drt.drt_my33310_recycle_t_xy_order_data  b on a.Forder_id=b.Forder_id
where
a.fpay_out_time >=to_date(date_sub(now(),45))
and b.Fxy_channel not in ('tbshop','HSBexternal','HSBexternal2','xyxcy','tmall-service','tm_recycle' ,'rm_recycle')


group by Forder_create_time 
,Fship_type
,Fcategory
,fxy_channel

,ftest

/*

union all


select 
substr(a.fauto_create_time,1,10) as Forder_create_time
,'寄卖' as Fship_type
,case when fproduct_class_id=2 then'笔记本' when  fproduct_class_id=3 then'平板'
 when  fproduct_class_id=1 then'手机' else '其他'end as Fcategory
,'寄卖' as fxy_channel

,0 as ftest
,count(distinct a.Forder_id) as cj
,sum(c.Fsales_amount/100) as cj

from
drt.drt_my33310_recycle_t_xy_jimai_plus_order  c
inner join (

select 
a.Forder_id,a.fauto_create_time,fproduct_class_id
from  drt.drt_my33310_recycle_t_order_txn a left join  drt.drt_my33310_recycle_t_order b on a.Forder_id=b.Forder_id
where  a.Forder_status in (714)
 and b.Forder_time>='2022-01-01 00:00:00'
and a.Fremark in ("")
  -- and b.Ftest = 0 

)a on a.Forder_id=c.Forder_id
where
a.fauto_create_time >=to_date(date_sub(now(),15))
group by 
 Forder_create_time 
,Fship_type
,Fcategory
,fxy_channel

,ftest
*/


)a

group by 
Forder_create_time 
,Fship_type
,Fcategory
,fxy_channel

,ftest


),






pingjia as 
(

select 
substr(b.Frate_time,1,10) as Forder_create_time
,case when Fship_type=2 then'上门' when Fship_type=3 then'到店' when Fship_type=1 then '邮寄' else '其他' end as Fship_type
-- ,case when Fship_type=2 then'上门' when Fship_type=3 then'到店' else '邮寄'end as Fship_type
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


else  fcategory end as fcategory
  
  
,case when a.fxy_channel in ('tmall-service','tm_recycle','rm_recycle')  then'天猫'  else '闲鱼'end as fxy_channel

,b.ftest
,sum(a.Fpcs) as pj
,sum(if(Frate_grade in (1,8),a.Fpcs,0)) as hp
,sum(if(Frate_grade in (0,2),a.Fpcs,0)) as cp

from
dws_hs_order_detail a  inner join dws_hs_order_detail_al b on a.Forder_id=b.Forder_id
where
b.Frate_time >=to_date(date_sub(now(),45))
and a.fpay_time>=to_date(date_sub(now(),45))
and a.Fxy_channel not in ('tbshop','HSBexternal','HSBexternal2','xyxcy','tmall-service','tm_recycle' ,'rm_recycle')




group by Forder_create_time 
,Fship_type
,Fcategory
,fxy_channel

,ftest




)


select 
c.Forder_create_time 
,c.Fship_type
,c.Fcategory
,c.fxy_channel

,c.ftest


,c.cj
,c.cjj

,e.pj
,e.hp
,e.cp




from  chengjiao c 


left join pingjia e on c.Forder_create_time=e.Forder_create_time
 and c.Fcategory=e.Fcategory and c.fxy_channel=e.fxy_channel 
 and c.Fship_type=e.Fship_type 
 and c.ftest=e.ftest

