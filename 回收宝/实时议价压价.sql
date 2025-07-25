
with 
jiance_out as 

(
select 
	to_date(a.Fsync_detect_time) as Fsync_detect_time
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

,fcategory as 子类目
	,Ftest
	,a.Fship_type
	,a.Fxy_channel
	,case when  d.Fcity in 
 ('上海市',
'北京市',
'哈尔滨市',
'杭州市',
'沈阳市',
'青岛市',
'东莞市',
'佛山市',
'厦门市',
'广州市',
'惠州市',
'昆明市',
'深圳市',
'成都市',
'武汉市',
'西安市',
'郑州市',
'重庆市',
'长沙市',
'长春市',
'珠海市',
'烟台市',
'泉州市',
'江门市',
'汕头市',
'揭阳市',
'廊坊市',
'太原市',
'大连市',
'南昌市',
'南宁市',
'中山市',
'金华市',
'苏州市',
'福州市',
'石家庄市',
'温州市',
'济南市',
'无锡市',
'宁波市',
'合肥市',
'南京市',
'天津市',
'嘉兴市',
'常州市',
'南通市',
'绍兴市',
'盐城市',
'台州市',
'扬州市',
'徐州市',
'湖州市',
'唐山市',
'淮安市',
'沧州市',
'临沂市',
'保定市',
'潍坊市',
'镇江市',
'连云港市')
then '定向城市' else '非定向城市' end as city
    ,b.fproduct_name
	,count(a.Fxy_order_id) as jianceout
	,count(if((Fquote_price > Fdetect_price),a.forder_id,null)) as yijia_num
	,sum(if((Fquote_price > Fdetect_price),a.Fquote_price/100,null)) as Fquote_price
	,sum(if((Fquote_price > Fdetect_price),a.Fdetect_price/100,null)) as Fdetect_price
	
	,sum(a.Fquote_price/100) as 闲鱼质检预估价
	,sum(a.Fdetect_price/100) as 闲鱼质检价

	,count(if(a.fsync_pay_out_time>=to_date(date_sub(now(),120)),a.forder_id,null)) as 成交量	
	,count(if(a.fsync_pay_out_time>=to_date(date_sub(now(),120)) and a.frate_time>=to_date(date_sub(now(),120)) and frate_grade in (0,2),a.forder_id,null)) as 差评量	
			
			
from  drt.drt_my33310_recycle_t_xy_order_data  a
left join  drt.drt_my33310_recycle_t_order  b on a.Forder_id=b.Forder_id
left join drt.drt_my33310_recycle_t_xianyu_order_map d on a.Forder_id=d.Forder_id
			
where 
a.Fsync_detect_time  >=to_date(date_sub(now(),120))

group by Fsync_detect_time,city,Fcategory,子类目,Ftest,Fship_type,Fxy_channel,fproduct_name
     
    

),

jiance_in as 

(
select 
	to_date(c.fdetect_time) as fdetect_time
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

,fcategory as 子类目
	,Ftest
	,a.Fship_type
	,a.Fxy_channel
		,case when  d.Fcity in 
 ('上海市',
'北京市',
'哈尔滨市',
'杭州市',
'沈阳市',
'青岛市',
'东莞市',
'佛山市',
'厦门市',
'广州市',
'惠州市',
'昆明市',
'深圳市',
'成都市',
'武汉市',
'西安市',
'郑州市',
'重庆市',
'长沙市',
'长春市',
'珠海市',
'烟台市',
'泉州市',
'江门市',
'汕头市',
'揭阳市',
'廊坊市',
'太原市',
'大连市',
'南昌市',
'南宁市',
'中山市',
'金华市',
'苏州市',
'福州市',
'石家庄市',
'温州市',
'济南市',
'无锡市',
'宁波市',
'合肥市',
'南京市',
'天津市',
'嘉兴市',
'常州市',
'南通市',
'绍兴市',
'盐城市',
'台州市',
'扬州市',
'徐州市',
'湖州市',
'唐山市',
'淮安市',
'沧州市',
'临沂市',
'保定市',
'潍坊市',
'镇江市',
'连云港市')
then '定向城市' else '非定向城市' end as city
,b.fproduct_name
	,count(a.Fxy_order_id) as jiancein
	 ,count(if((Fquote_price > freal_detect_price),a.forder_id,null)) as yijia_num
	,sum(if((Fquote_price > freal_detect_price),Fquote_price/100,null)) as Fquote_price
	,sum(if((Fquote_price > freal_detect_price),freal_detect_price/100,null)) as freal_detect_price
			
from  drt.drt_my33310_recycle_t_xy_order_data  a
left join  drt.drt_my33310_recycle_t_order  b on a.Forder_id=b.Forder_id
left join  drt.drt_my33310_recycle_t_detection_info c on a.Forder_id=c.Forder_id
left join drt.drt_my33310_recycle_t_xianyu_order_map d on a.Forder_id=d.Forder_id
			
where 
c.fdetect_time  >=to_date(date_sub(now(),120))


group by fdetect_time,city,Fcategory,子类目,Ftest,Fship_type,Fxy_channel,fproduct_name
     


),


shangmen as 
(



select 
	to_date(c.fdetect_time) as fdetect_time
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

,fcategory as 子类目
	,Ftest
	,a.Fship_type
	,a.Fxy_channel
		,case when  d.Fcity in 
 ('上海市',
'北京市',
'哈尔滨市',
'杭州市',
'沈阳市',
'青岛市',
'东莞市',
'佛山市',
'厦门市',
'广州市',
'惠州市',
'昆明市',
'深圳市',
'成都市',
'武汉市',
'西安市',
'郑州市',
'重庆市',
'长沙市',
'长春市',
'珠海市',
'烟台市',
'泉州市',
'江门市',
'汕头市',
'揭阳市',
'廊坊市',
'太原市',
'大连市',
'南昌市',
'南宁市',
'中山市',
'金华市',
'苏州市',
'福州市',
'石家庄市',
'温州市',
'济南市',
'无锡市',
'宁波市',
'合肥市',
'南京市',
'天津市',
'嘉兴市',
'常州市',
'南通市',
'绍兴市',
'盐城市',
'台州市',
'扬州市',
'徐州市',
'湖州市',
'唐山市',
'淮安市',
'沧州市',
'临沂市',
'保定市',
'潍坊市',
'镇江市',
'连云港市')
then '定向城市' else '非定向城市' end as city
,b.fproduct_name
	,count(a.Fxy_order_id) as 内部上门质检量
	,count(if((Fquote_price > fdetect_price_rpt),a.forder_id,null)) as yijia_num
	,count(if((Fquote_price > fdetect_price_rpt),Fquote_price/100,null)) as 内部上门质检预估价
	,count(if((Fquote_price > fdetect_price_rpt),fdetect_price_rpt/100,null)) as 内部上门质检价


from  drt.drt_my33310_recycle_t_xy_order_data  a
inner join  drt.drt_my33310_recycle_t_order  b on a.Forder_id=b.Forder_id
inner join  dwd.dwd_detect_front_detect_detail c on a.Forder_id=c.Forder_id
inner join drt.drt_my33310_recycle_t_xianyu_order_map d on a.Forder_id=d.Forder_id
			
where 
fdetect_time >=to_date(date_sub(now(),120))
and a.fship_type=2

group by fdetect_time,city,Fcategory,子类目,Ftest,Fship_type,Fxy_channel,fproduct_name
     


)

/*,




xiadan as 

(
select 
	to_date(a.forder_time) as forder_time
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

,fcategory as 子类目
	,Ftest
	,a.Fship_type
	,a.Fxy_channel
	,case when  d.Fcity in 
 ('上海市',
'北京市',
'哈尔滨市',
'杭州市',
'沈阳市',
'青岛市',
'东莞市',
'佛山市',
'厦门市',
'广州市',
'惠州市',
'昆明市',
'深圳市',
'成都市',
'武汉市',
'西安市',
'郑州市',
'重庆市',
'长沙市',
'长春市',
'珠海市',
'烟台市',
'泉州市',
'江门市',
'汕头市',
'揭阳市',
'廊坊市',
'太原市',
'大连市',
'南昌市',
'南宁市',
'中山市',
'金华市',
'苏州市',
'福州市',
'石家庄市',
'温州市',
'济南市',
'无锡市',
'宁波市',
'合肥市',
'南京市',
'天津市',
'嘉兴市',
'常州市',
'南通市',
'绍兴市',
'盐城市',
'台州市',
'扬州市',
'徐州市',
'湖州市',
'唐山市',
'淮安市',
'沧州市',
'临沂市',
'保定市',
'潍坊市',
'镇江市',
'连云港市')
then '定向城市' else '非定向城市' end as city
    ,b.fproduct_name
	,count(a.Fxy_order_id) as 下单量

			
			
from  drt.drt_my33310_recycle_t_xy_order_data  a
left join  drt.drt_my33310_recycle_t_order  b on a.Forder_id=b.Forder_id
left join drt.drt_my33310_recycle_t_xianyu_order_map d on a.Forder_id=d.Forder_id
			
where 
a.forder_time  >=to_date(date_sub(now(),120))
group by forder_time,city,Fcategory,子类目,Ftest,Fship_type,Fxy_channel,fproduct_name
     

)




*/



select  
		jiance_out.Fsync_detect_time as dt
		,jiance_out.city
		,jiance_out.Fcategory
		,jiance_out.子类目
		,jiance_out.Ftest
		,jiance_out.Fship_type
		,jiance_out.Fxy_channel
		,jiance_out.fproduct_name
		
		
		
		
		,jianceout 
		,jiancein
		,jiance_in.yijia_num as yijiain
		
		,jiance_in.Fquote_price as jiancegujia
        ,jiance_in.freal_detect_price as jiancejiance
		
		,jiance_out.yijia_num as yijiaout
		,jiance_out.Fquote_price as yijiagujia
		,jiance_out.Fdetect_price as yijiajiance
		
		
	
		
		,闲鱼质检预估价
		,闲鱼质检价
		,成交量
		,差评量
		
		
		,内部上门质检量
		,内部上门质检预估价
        ,内部上门质检价
        ,0 as 下单量
		
		
    from jiance_out 
  
    left join jiance_in 
    on jiance_out.Fsync_detect_time=jiance_in.fdetect_time
    and jiance_out.city=jiance_in.city
    and jiance_out.Fcategory=jiance_in.Fcategory
    and jiance_out.子类目=jiance_in.子类目
    and jiance_out.Ftest=jiance_in.Ftest
    and jiance_out.Fship_type=jiance_in.Fship_type
    and jiance_out.Fxy_channel=jiance_in.Fxy_channel
    and jiance_out.fproduct_name=jiance_in.fproduct_name
    
 
    left join shangmen 
    on jiance_out.Fsync_detect_time=shangmen.fdetect_time
    and jiance_out.city=shangmen.city
    and jiance_out.Fcategory=shangmen.Fcategory
    and jiance_out.子类目=shangmen.子类目
    and jiance_out.Ftest=shangmen.Ftest
    and jiance_out.Fship_type=shangmen.Fship_type
    and jiance_out.Fxy_channel=shangmen.Fxy_channel
    and jiance_out.fproduct_name=shangmen.fproduct_name

