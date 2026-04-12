with 
sales as (              --取销售数据
select 
    *
from (
    select 
        *,
        row_number()over(partition by fseries_number order by fstart_time desc)as num
    from(
        select 
            fstart_time,
      		fpay_time,
      		case when Forder_platform=5 then "鱼市" 
      			 when Forder_platform=1 then "自有"
      			 when Forder_platform=6 then "采货侠" 
      		 else "其他" end as fsale_platform,
            if(Fchannel_name="竞拍销售默认渠道号",fold_series_number, fseries_number) as fseries_number,
            fcost_price/100 as fcost_price,
            Foffer_price/100 as Foffer_price
        from dws.dws_jp_order_detail 
        where Forder_status in (2,3,4,6)
        and Fpay_time IS NOT NULL
        union all
        select 
            fstart_time,
            a.fpay_time,
      		"统货" as fsale_platform,
            if(a.Fchannel_name="竞拍销售默认渠道号",b.fold_fseries_number,a.fseries_number) as fseries_number,
            fcost_price/100 as fcost_price,
            a.Foffer_price/100 as Foffer_price
        from dws.dws_th_order_detail as a
        left join dws.dws_hs_order_detail as b on a.fseries_number=b.fseries_number
        where a.Forderoffer_status = 10
        and a.Fpay_time IS NOT NULL
        UNION 
  		select
        a.fcreate_time as fstart_time,
  		a.fcreate_time as fpay_time,
  		"线下" as fsale_platform,
        b.Fstock_no as fseries_number,
        d.fpay_out_price/100 as fcost_price,
        b.Fretail_price as foffer_price
    from drt.drt_my33312_hsb_sales_product_t_stock_order_saleout a
    inner join drt.drt_my33312_hsb_sales_product_t_stock_order_saleout_detail b on a.Fstock_order_sn = b.Fstock_order_sn
    left join drt.drt_my33310_recycle_t_order d on b.Fstock_no = d.fseries_number 
    where a.Fsource = 5 -- 手工单
    and a.fout_status=2
    and b.Fretail_price>0
    and d.fpay_out_price>0
        )a)t where num=1
  		
),
gongdan_buchang as (               --工单补偿金额明细，补充订单系统没有体现的金额
select 
    *
from (
    select
        a.fbarcode_sn, 
        from_unixtime(a.fadd_time),
        c.fmoney,
        row_number() over(partition by a.fbarcode_sn order by a.fadd_time desc) as num
    from drt.drt_my33310_csrdb_t_works as a
    left join drt.drt_my33310_csrdb_t_works_config_appeal as b on a.fappeal_type2=b.fid
    left join drt.drt_my33310_csrdb_t_works_compensation as c on a.fid=c.fwork_id
    where from_unixtime(a.Fadd_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),1200))
    and a.fwork_type in(1,2,3,4)
    and a.fappeal_type1<>0
    and b.fcontent like "%议价%"
    and a.fduty_content not like "%无效工单%"
)t 
where num=1
),
pjt as (
select 
    fseries_number,
    cast(Fpjt_price as int)/100 as Fpjt_price
from (
select 
    *,
    row_number() over(partition by fseries_number order by fcreate_time desc) as num
from drt.drt_my33312_detection_t_inquiry_log 
where fstatus=1
)t
where num=1
)
select 
    a.forder_create_time,
    a.fpay_time,
    c.fpay_time as "销售付款时间",
    c.fsale_platform,
    c.fstart_time,
    a.forder_id,
    a.fseries_number,
    case when a.fclass_name in ('平板','平板电脑') then '平板'
when a.fclass_name in ('笔记本','笔记本电脑') then '笔记本'
when a.fclass_name in ('手机','') then '手机'
when a.fclass_name in ('单反闪光灯',
'单反转接环',
'移动电源',
'移动硬盘',
'云台',
'拍照配件/云台',                
'增距镜') then '3C数码配件'

when a.fclass_name in ('彩色激光多功能一体机',
'复印打印多功能一体机',
'激光打印机',
'墨盒',
'收款机',
'投影机',
'投影仪',
'硒鼓粉盒',
'针式打印机') then '办公设备耗材'

when a.fclass_name in ('CPU',
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

when a.fclass_name in ('路由器') then '网络设备'

when a.fclass_name in ('PS游戏光盘/软件',
'其他游戏配件',
'游戏机',
'游戏卡',
'游戏手柄',
'PS4游戏',
'PS5游戏',
'Switch游戏') then '电玩'

when a.fclass_name in ('单反套机',
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

when a.fclass_name in ('耳机',
'MP3/MP4',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'




when a.fclass_name in ('VR眼镜头盔',
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


else  a.fclass_name end as fclass_name,
    a.fbrand_name,
    a.fproduct_name,
    a.fsender_phone,
    case when d.fxy_channel in ( 'tmall-service','rm_recycel','tm_recycle' ) then "天猫"
         when d.fxy_channel not in ( 'tmall-service','rm_recycel','tm_recycle' ) and d.fxy_channel is not null then "闲鱼"
         when a.fchannel_name like "%寄卖%" or a.fchannel_name like "%帮卖%" then "闲鱼寄卖plus"
         when (LEFT(a.fseries_number,2)="CG" and a.forder_create_time>='2024-12-01') or LEFT(a.fseries_number,2)="TL" then "太力"
    else "自有渠道" end as "业务所属",
    case 
    	when a.frecycle_type=1 then "闲鱼邮寄"
        when a.frecycle_type=2 then "闲鱼上门"
        when a.frecycle_type=3 then "闲鱼到店"
    else "" end as "履约方式",
    case when a.fchannel_name="支付宝小程序" then "合作项目" else a.fproject_name end as fproject_name,
    case 
    	when a.fdetect_price/100>0 and a.fdetect_price/100<=300 then "0-300"
        when a.fdetect_price/100>300 and a.fdetect_price/100<=500 then "300-500"
        when a.fdetect_price/100>500 and a.fdetect_price/100<=1000 then "500-1000"
        when a.fdetect_price/100>1000 and a.fdetect_price/100<=2000 then "1000-2000"
        when a.fdetect_price/100>2000 and a.fdetect_price/100<=3000 then "2000-3000"
        when a.fdetect_price/100>3000 and a.fdetect_price/100<=5000 then "3000-5000"
        when a.fdetect_price/100>5000 then "5000以上"
    else null end as "检测价区间",
    a.funit_price/100 as "预估价",
    a.fdetect_price/100 as "检测价",
    a.foperation_price/100 as "运营价",
    a.fpay_out_price/100 as "回收价",
    c.fcost_price as "成本价",
    c.foffer_price as "销售价",
    a.fcheck_item_group_level,
    b.fstock_in_num,
    if(a.fpay_out_price is not null,e.fmoney,null) as fmoney,
    a.fbargain_price/100 as fbargain_price,
    f.Fpjt_price
from dws.dws_hs_order_detail as a
left join sales as c on a.fseries_number=c.fseries_number
left join drt.drt_my33312_hsb_sales_product_t_pm_product as b on a.fseries_number=b.fserial_no
left join dws.dws_hs_order_detail_al as d on a.forder_id=d.forder_id
left join gongdan_buchang as e on a.fseries_number=e.fbarcode_sn
left join pjt as f on a.fseries_number=f.fseries_number
where a.forder_create_time>=to_date(date_sub(from_unixtime(unix_timestamp()),366))
and a.ftest=0
and (a.fpay_out_price>0 or a.fpay_time is not null)
--and LEFT(a.fseries_number,2) !="CG"
and LEFT(a.fseries_number,2) !="YZ"
