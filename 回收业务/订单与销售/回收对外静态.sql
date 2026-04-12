SELECT
a.ftime_byday,
a.fxy_channel,
a.fsub_channel,
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
'投影仪',
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
'组装台机',
'品牌台式机') then '电脑硬件及周边'

when fcategory in ('路由器') then '网络设备'

when fcategory in ('PS游戏光盘/软件',
'其他游戏配件',
'游戏机',
'游戏卡',
'游戏手柄',
'PS4游戏',
'PS5游戏',
'Switch游戏') then '电玩'

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
'MP3/MP4',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'




when fcategory in ('VR眼镜头盔',
'VR虚拟现实',
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
 a.fbusiness_type,
a.fship_type,
sum(a.forder_qty) as ordernum,
sum(a.fget_qty) as getnum,
sum(a.fdetect_qty) as detenum,
sum(a.fpay_qty) as pay,
sum(a.freturn_qty) as returnqty,
sum(a.fgrate_qty) as grate,
sum(a.frate_qty) as rate,
sum(a.fpay_amount) as fpay_amount,
sum(a.forder_canal_qty) as forder_canal_qty,
sum(a.forder_cancel_inten_qty) as forder_cancel_inten_qty,
SUM(a.Fquote_bargin_amount) as Fquote_bargin_amount,
SUM(a.Fdetect_bargin_amount) as Fdetect_bargin_amount,
SUM(a.Fbargin_qty) as Fbargin_qty,
sum(a.fquote_pay_amount) as fquote_pay_amount,
sum(a.fdetect_pay_amount) as fdetect_pay_amount
FROM
dm.dm_hs_dimension_al  as a  
WHERE 
a.ftime_byday>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
GROUP BY a.ftime_byday,a.fxy_channel,a.fsub_channel,a.fcategory, a.fbusiness_type,
fship_type
