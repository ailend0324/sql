with time_params as (
	select 
		to_date(date_sub(from_unixtime(unix_timestamp()),365)) as start_date
),
category_info as (
	select 
		fserial_number,
		case when fclass_name in ("平板","平板电脑") then "平板"
			 when fclass_name in ("笔记本","笔记本电脑") then "笔记本"
			 when fclass_name in ("手机","") or fclass_name is null then "手机"
			 when fclass_name in ("单反闪光灯","单反转接环","移动电源","移动硬盘","云台","拍照配件/云台","增距镜") then "3C数码配件"
			 when fclass_name in ("彩色激光多功能一体机","复印打印多功能一体机","激光打印机","墨盒","收款机","投影机","投影仪","硒鼓粉盒","针式打印机") then "办公设备耗材"
			 when fclass_name in ("CPU","电脑服务器","电脑固态硬盘","固态硬盘","电脑内存","内存条","电脑显卡","显卡","电脑硬件套装","电脑主板","键盘","品牌台机","无线鼠标","显示器","一体机","组装台机","品牌台式机") then "电脑硬件及周边"
			 when fclass_name in ("路由器") then "网络设备"
			 when fclass_name in ("PS游戏光盘/软件","其他游戏配件","游戏机","游戏卡","游戏手柄","PS4游戏","PS5游戏","Switch游戏") then "电玩"
			 when fclass_name in ("单反套机","单反相机","拍立得","摄像机","摄影机","数码相机","微单相机","相机镜头","运动相机","单反/微单套机","单反/微单相机") then "相机/摄像机"
			 when fclass_name in ("耳机","MP3/MP4","黑胶唱片机","蓝牙耳机","蓝牙音响/音箱","麦克风/话筒","影音播放器","智能音响/音箱") then "影音数码/电器"
			 when fclass_name in ("VR眼镜头盔","VR虚拟现实","按摩器","吹风机","磁吸式键盘","电子书","翻译器","风扇","加湿器","录音笔","美发器","手写笔","智能手写笔","无人机","吸尘器","学习机","智能办公本","智能配饰","智能摄像","智能手表","智能手环") then "智能设备"
			 when fclass_name in ("黄金") then "黄金"
		else "手机" end as fclass
	from (
		select 
			fserial_number,
			fclass_name,
			fend_time,
			row_number() over(partition by fserial_number order by fend_time asc) as num
		from drt.drt_my33310_detection_t_detect_record 
		where fdet_type=0
		and fis_deleted=0
		and freport_type=0
		and fverdict<>"测试单"
		and to_date(fend_time) >= (select start_date from time_params)
	) t
	where num=1
)
select 
 		to_date(fgetin_time) as time_by,
 		case when left(fseries_number,2)='BM' then "寄卖"
        else "回收" end as ftype,
		case when right(left(fseries_number,6),4)="0112" then "东莞" 
		 	 when right(left(fseries_number,6),2)="16" then "杭州"
	   else "深圳" end as fwarehouse,
		coalesce(c.fclass, "手机") as fclass,
		count(fseries_number) as num 
from drt.drt_my33310_recycle_t_order 
left join category_info as c on fseries_number=c.fserial_number
where to_date(fgetin_time) >= (select start_date from time_params)
and ftest=0
group by 1,2,3,4
union all
select 
 		to_date(freceive_time) as time_by,
 		"验机" as ftype,
		case when left(fhost_barcode,3)="020" then "杭州"
		 	 when left(fhost_barcode,3)="010" then "深圳"
	            when left(fhost_barcode,3)="050" then "东莞"
	   	else "" end as fwarehouse,
		coalesce(c.fclass, "手机") as fclass,
		count(fhost_barcode) as num 
 from dws.dws_xy_yhb_detail as a
 left join category_info as c on a.fhost_barcode=c.fserial_number
where to_date(freceive_time) >= (select start_date from time_params)
group by 1,2,3,4
