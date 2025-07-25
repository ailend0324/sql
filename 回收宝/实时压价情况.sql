
select
	Fsync_detect_time
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
'品牌台式机',
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


else "其他" end as fcategory
    ,case 
        a.Fship_type when 1 then "邮寄"
                    when 2 then "上门"
                    when 3 then "到店"
        end as 履约方
    ,a.Fxy_channel
    ,b.ftest
    ,count(a.Fxy_order_id) as 检测量
    ,count(if(Fquote_price > Fdetect_price,1,null)) as 议价量
    ,sum(if(Fquote_price > Fdetect_price,a.Fquote_price/100,0)) as Fquote_price
    ,sum(if(Fquote_price > Fdetect_price,a.Fdetect_price/100,0)) as Fdetect_price
    ,sum(a.Fconfirm_fee/100) as Fconfirm_fee

from
    drt. drt_my33310_recycle_t_xy_order_data a
LEFT  JOIN  
    drt. drt_my33310_recycle_t_order  b
    ON a.Forder_id = b.Forder_id
where
    Fsync_detect_time >='2024-01-01 00:00:00' 

group by  1,2,3,4,5
