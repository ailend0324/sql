select 
    to_date(a.fpay_time) as fpay_time,
    case when right(left(a.fseries_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    case when b.fcategory in ('平板','平板电脑') then '平板'
        when b.fcategory in ('笔记本','笔记本电脑') then '笔记本'
        when b.fcategory in ('手机','') then '手机'
        when b.fcategory in ('单反闪光灯',
        '单反转接环',
        '移动电源',
        '移动硬盘',
        '云台',
        '拍照配件/云台',                
        '增距镜') then '3C数码配件'
        when b.fcategory in ('彩色激光多功能一体机',
        '复印打印多功能一体机',
        '激光打印机',
        '墨盒',
        '收款机',
        '投影机',
        '投影仪',
        '硒鼓粉盒',
        '针式打印机') then '办公设备耗材'
        when b.fcategory in ('CPU',
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
        
        when b.fcategory in ('路由器') then '网络设备'
        when b.fcategory in ('PS游戏光盘/软件',
        '其他游戏配件',
        '游戏机',
        '游戏卡',
        '游戏手柄',
        'PS4游戏',
        'PS5游戏',
        'Switch游戏') then '电玩'
        when b.fcategory in ('单反套机',
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
        when b.fcategory in ('耳机',
        'MP3/MP4',
        '黑胶唱片机',
        '蓝牙耳机',
        '蓝牙音响/音箱',
        '麦克风/话筒',
        '影音播放器',
        '智能音响/音箱') then '影音数码/电器'
        when b.fcategory in ('VR眼镜头盔',
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
    else  b.fcategory end as fcategory,
    a.fchannel_name,
    case when a.frecycle_type=1 then "邮寄"
         when a.frecycle_type=2 then "上门"
         when a.frecycle_type=3 then "到店"
    else null end as frecycle_type,
    count(distinct a.forder_id) as fpay_num,
    count(distinct case when b.frate_time>='2022-01-01' and b.frate_grade in (1,8) then a.forder_id else null end) as fhaoping,
    count(distinct case when b.frate_time>='2022-01-01' and b.frate_grade in (5) then a.forder_id else null end) as fzhongping,
    count(distinct case when b.frate_time>='2022-01-01' and b.frate_grade not in (1,8,5) then a.forder_id else null end) as fchaping
from dws.dws_hs_order_detail as a
inner join drt.drt_my33310_recycle_t_xy_order_data as b on a.forder_id=b.forder_id
where a.ftest=0 
and to_date(a.fpay_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
group by 1,2,3,4,5
