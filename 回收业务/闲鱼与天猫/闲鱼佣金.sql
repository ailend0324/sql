
with a as 
(
SELECT
    substr(b.Forder_time,1,7)  AS 下单月份
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
        '投影仪',
        '打印机',
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
        '硬件套装',
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
        '游戏机',
        '游戏手柄') then '电玩'
        
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
        'MP3/MP4',
        '智能音响/音箱') then '影音数码/电器'
        
        
        when fcategory in ('PS4游戏',
        'PS5游戏',
        'Switch游戏') then '游戏卡'
        
        
        
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
        
        
        else  fcategory end as 类目
    ,ct2.fcity_name as 城市
    ,"上门" as 履约方
    ,a.ftest as 测试单
    ,a.Forder_id as 订单号
    ,cast(b.fxy_order_id as string) as 闲鱼订单号
    ,b.fuser_id as 闲鱼用户id
    ,b.fquote_price/100 as 预估价
    ,if(b.fquote_price*0.015/100<5,5,if(b.fquote_price*0.015/100>25,25,b.fquote_price*0.015/100)) as 佣金
    ,b.Forder_time
    ,b.Fxy_channel
    ,b.fconfirm_fee/100 as 成交价
		
	FROM
		drt.drt_my33310_recycle_t_order AS a
		INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
		inner join   drt.drt_my33310_recycle_t_order_status  c on a.Forder_status = c.Forder_status_id 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map d on a.Forder_id = d.Forder_id
		left join drt.drt_my33310_hjxmba_db_t_city ct2 on a.fcity_id = ct2.fcity_id
		
	WHERE

		 b.Forder_time >='2024-07-01 00:00:00'
		 -- AND b.Fxy_channel not IN ('tmall-service')
		 and  b.Fxy_channel  IN ('idle')
		 --  AND b.Fxy_channel not IN ('tmall-service','idle')
		--  and  b.Fxy_channel  IN ('idle')
		 and d.Fship_type in (2)
		 and a.fchannel_id !=10001191
   		and  b.fsub_channel not like "hjbt%" 
  		and b.fsub_channel not in ("sspush","detail","dcpush")
  		and fcategory not in ("黄金")
  -- and b.fxy_product_name not in ("跳过估价，一键下单")
		 -- and a.ftest=0 --  neibu
		 -- and d.fcity in ("深圳市","杭州市","上海市","合肥市","武汉市","沈阳市","哈尔滨市","西安市")
       --  and d.fcity in ("深圳市")
),
b as 
(
SELECT
    substr(b.Forder_time,1,7)  AS 下单月份
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
        '投影仪',
        '打印机',
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
        '硬件套装',
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
        '游戏机',
        '游戏手柄') then '电玩'
        
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
        'MP3/MP4',
        '智能音响/音箱') then '影音数码/电器'
        
        
        when fcategory in ('PS4游戏',
        'PS5游戏',
        'Switch游戏') then '游戏卡'
        
        
        
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
        
        
        else  fcategory end as 类目
    ,ct2.fcity_name as 城市
    ,"到店" as 履约方
    ,a.ftest as 测试单
    ,a.Forder_id as 订单号
    ,cast(b.fxy_order_id as string) as 闲鱼订单号
    ,b.fuser_id as 闲鱼用户id
    ,b.fquote_price/100 as 预估价
    ,if(b.fquote_price*0.015/100<5,5,if(b.fquote_price*0.015/100>25,25,b.fquote_price*0.015/100)) as 佣金
    ,b.Forder_time
     ,b.Fxy_channel
     ,b.fconfirm_fee/100 as 成交价
		
	FROM
		drt.drt_my33310_recycle_t_order AS a
		INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
		inner join   drt.drt_my33310_recycle_t_order_status  c on a.Forder_status = c.Forder_status_id 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map d on a.Forder_id = d.Forder_id
		left join drt.drt_my33310_recycle_t_xyxz_order xz ON a.Forder_id = xz.Forder_id 
        left join drt.drt_my33317_pub_server_merchant_center_db_t_merchant_shop_info shop 
        on shop.fshop_id = xz.fstore_id
        left join drt.drt_my33310_hjxmba_db_t_city ct2 on shop.Fcity_code = ct2.fcity_id

		-- inner join drt.drt_my33310_recycle_t_order_snapshot  e on a.Forder_id = e.Forder_id 
	WHERE

		 b.Forder_time >='2024-07-01 00:00:00'
		 -- AND b.Fxy_channel not IN ('tmall-service')
		   and  b.Fxy_channel  IN ('idle')
		 -- AND b.Fxy_channel not IN ('tmall-service','idle')
		 and d.Fship_type in (3)
		 and a.fchannel_id !=10001191
  		and  b.fsub_channel not like "hjbt%" 
  		and b.fsub_channel not in ("sspush","detail","dcpush")
  		and fcategory not in ("黄金")
  		-- and b.fxy_product_name  not in ("跳过估价，一键下单")
		 -- and a.ftest=0 --  neibu
		 -- and ct2.fcity_name in ("深圳市","杭州市","上海市","合肥市","武汉市","沈阳市","哈尔滨市","西安市")

),

c as 
(
SELECT
    substr(b.Forder_time,1,7)  AS 下单月份
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
        '投影仪',
        '打印机',
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
        '硬件套装',
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
        '游戏机',
        '游戏手柄') then '电玩'
        
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
        'MP3/MP4',
        '智能音响/音箱') then '影音数码/电器'
        
        
        when fcategory in ('PS4游戏',
        'PS5游戏',
        'Switch游戏') then '游戏卡'
        
        
        
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
        
        
        else  fcategory end as 类目
     ,d.fcity as 城市
    ,"邮寄" as 履约方
    ,a.ftest as 测试单
    ,a.Forder_id as 订单号
    ,cast(b.fxy_order_id as string) as 闲鱼订单号
    ,b.fuser_id as 闲鱼用户id
    ,b.fquote_price/100 as 预估价
    ,if(b.fquote_price*0.025/100<5,5,if(b.fquote_price*0.025/100>50,50,b.fquote_price*0.025/100)) as 佣金
    ,b.Forder_time
     ,b.Fxy_channel
     ,b.fconfirm_fee/100 as 成交价
		
	FROM
		drt.drt_my33310_recycle_t_order AS a
		INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
		inner join   drt.drt_my33310_recycle_t_order_status  c on a.Forder_status = c.Forder_status_id 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map d on a.Forder_id = d.Forder_id
	
	WHERE

		 b.Forder_time >='2024-07-01 00:00:00'
		 -- AND b.Fxy_channel not IN ('tmall-service')
		 and  b.Fxy_channel  IN ('idle')
		
		 and d.Fship_type in (1)
		 and a.fchannel_id !=10001191
  		and  b.fsub_channel not like "hjbt%" 
  		and b.fsub_channel not in ("sspush","detail","dcpush")
  		and fcategory not in ("黄金")
  	-- 	and b.fxy_product_name not in ("跳过估价，一键下单")
		 -- and a.ftest=0 --  neibu
		 -- and d.fcity in ("深圳市","杭州市","上海市","合肥市","武汉市","沈阳市","哈尔滨市","西安市")
       --  and d.fcity in ("深圳市")
),

a2 as 
(
select * from a 
union all
select * from b
union all
select * from c

),

xinyongjin as

(


select 
下单月份
,履约方
,类目
-- ,预估价
,count(订单号) as 新佣金量
,sum(佣金) as 新佣金
from
(
select *
from(
select  下单月份,类目,履约方,测试单,订单号,闲鱼用户id,闲鱼订单号,预估价
    ,佣金,Fxy_channel,成交价
    ,Forder_time,row_number()over(partition by 下单月份,类目,闲鱼用户id order by Forder_time asc) as ranknum 
from a2
)a  where ranknum <3
)a
group by 1,2,3
)
,



dongtai as 
(

select 
    substr(b.Forder_time,1,7)  AS 下单月份
    ,case 
    b.fship_type when 1 then "邮寄"
                 when 2 then "上门"
                 when 3 then "到店"
            end as 履约方
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
        '投影仪',
        '打印机',
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
        '硬件套装',
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
        '游戏机',
        '游戏手柄') then '电玩'
        
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
        'MP3/MP4',
        '智能音响/音箱') then '影音数码/电器'
        
        
        when fcategory in ('PS4游戏',
        'PS5游戏',
        'Switch游戏') then '游戏卡'
        
        
        
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
        
        
        else  fcategory end as 类目
        -- ,b.fquote_price/100 as 预估价
        ,count(if(b.fsync_pay_out_time>="2024-07-01 00:00:00",b.forder_id,null)) as 动态老佣金量
        ,sum(b.fconfirm_fee/100*0.05) as 动态老佣金
		
	FROM
		drt.drt_my33310_recycle_t_order AS a
		INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
		inner join   drt.drt_my33310_recycle_t_order_status  c on a.Forder_status = c.Forder_status_id 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map d on a.Forder_id = d.Forder_id
	
	WHERE

		 b.Forder_time >='2024-07-01 00:00:00'
		 -- AND b.Fxy_channel not IN ('tmall-service')
		 and  b.Fxy_channel  IN ('idle')
  		and b.fcategory not in ("黄金")
		
		 -- and d.Fship_type in (1)
		 and a.fchannel_id !=10001191
  		and  b.fsub_channel not like "hjbt%" 
  		and b.fsub_channel not in ("sspush","detail","dcpush")
		 -- and a.ftest=0 --  neibu
		 -- and d.fcity in ("深圳市","杭州市","上海市","合肥市","武汉市","沈阳市","哈尔滨市","西安市")
       --  and d.fcity in ("深圳市")
       group by 1,2,3
       ),
       
       
       
       
    
jingtai as 
(

select 
    substr(b.fsync_pay_out_time,1,7)  AS 成交月份
    ,case 
    b.fship_type when 1 then "邮寄"
                 when 2 then "上门"
                 when 3 then "到店"
            end as 履约方
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
        '投影仪',
        '打印机',
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
        '硬件套装',
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
        '游戏机',
        '游戏手柄') then '电玩'
        
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
        'MP3/MP4',
        '智能音响/音箱') then '影音数码/电器'
        
        
        when fcategory in ('PS4游戏',
        'PS5游戏',
        'Switch游戏') then '游戏卡'
        
        
        
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
        
        
        else  fcategory end as 类目
        -- ,b.fquote_price/100 as 预估价
        ,count(b.forder_id) as 静态老佣金量
  	
    ,sum(b.fconfirm_fee/100*0.05) as 静态老佣金
		
	FROM
		drt.drt_my33310_recycle_t_order AS a
		INNER JOIN  drt.drt_my33310_recycle_t_xy_order_data  AS b ON a.Forder_id = b.Forder_id 
		inner join   drt.drt_my33310_recycle_t_order_status  c on a.Forder_status = c.Forder_status_id 
		inner join drt.drt_my33310_recycle_t_xianyu_order_map d on a.Forder_id = d.Forder_id
	
	WHERE

		 b.fsync_pay_out_time  >='2024-07-01 00:00:00'
		 -- AND b.Fxy_channel not IN ('tmall-service')
		 and  b.Fxy_channel  IN ('idle')
  		and b.fcategory not in ("黄金")
		
		 -- and d.Fship_type in (1)
		 and a.fchannel_id !=10001191
 		 and  b.fsub_channel not like "hjbt%" 
  		and b.fsub_channel not in ("sspush","detail","dcpush")
		 -- and a.ftest=0 --  neibu
		 -- and d.fcity in ("深圳市","杭州市","上海市","合肥市","武汉市","沈阳市","哈尔滨市","西安市")
       --  and d.fcity in ("深圳市")
       group by 1,2,3
       )   

    select 
    a.下单月份
    ,a.履约方
    ,a.类目
    -- ,a.预估价
    ,a.新佣金
    ,静态老佣金
    ,动态老佣金
    
    ,新佣金量
    ,静态老佣金量
    ,动态老佣金量
    
    from
    xinyongjin a inner join 
    jingtai b on a.履约方=b.履约方 and a.下单月份=b.成交月份
    and a.类目=b.类目
    -- and a.预估价=b.预估价
    inner join 
    dongtai c on a.履约方=c.履约方 and a.下单月份=c.下单月份
    and a.类目=c.类目
    -- and a.预估价=c.预估价
    


