select
        to_date(a.fout_time) as fstart_time,
  		"线下销售" as fdifferent_sale,
        "正常销售" as fsales_type2,
        case when d.Fclass_name in ('平板','平板电脑') then '平板'
when d.Fclass_name in ('笔记本','笔记本电脑') then '笔记本'
when d.Fclass_name in ('手机','') then '手机'
when d.Fclass_name in ('单反闪光灯',
'单反转接环',
'移动电源',
'移动硬盘',
'云台',
'拍照配件/云台',                
'增距镜') then '3C数码配件'

when d.Fclass_name in ('彩色激光多功能一体机',
'复印打印多功能一体机',
'激光打印机',
'墨盒',
'收款机',
'投影机',
'投影仪',
'硒鼓粉盒',
'针式打印机') then '办公设备耗材'

when d.Fclass_name in ('CPU',
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

when d.Fclass_name in ('路由器') then '网络设备'

when d.Fclass_name in ('PS游戏光盘/软件',
'其他游戏配件',
'游戏机',
'游戏卡',
'游戏手柄',
'PS4游戏',
'PS5游戏',
'Switch游戏') then '电玩'

when d.Fclass_name in ('单反套机',
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

when d.Fclass_name in ('耳机',
'MP3/MP4',
'黑胶唱片机',
'蓝牙耳机',
'蓝牙音响/音箱',
'麦克风/话筒',
'影音播放器',
'智能音响/音箱') then '影音数码/电器'




when d.Fclass_name in ('VR眼镜头盔',
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


else  d.Fclass_name end as fclass, 
        d.fclass_name,
        if(d.fchannel_name="竞拍销售默认渠道号",e.fchannel_name,d.fchannel_name)as fchannel_name,
        f.fcity,
        case when if(d.fchannel_name="竞拍销售默认渠道号",e.fsupply_partner,d.fsupply_partner)=0 then "无"
             when if(d.fchannel_name="竞拍销售默认渠道号",e.fsupply_partner,d.fsupply_partner)=1 then "闪修侠"
             when if(d.fchannel_name="竞拍销售默认渠道号",e.fsupply_partner,d.fsupply_partner)=2 then "小站(自营)"
             when if(d.fchannel_name="竞拍销售默认渠道号",e.fsupply_partner,d.fsupply_partner)=3 then "小站(加盟)"
             when if(d.fchannel_name="竞拍销售默认渠道号",e.fsupply_partner,d.fsupply_partner)=4 then "速回收"
             when if(d.fchannel_name="竞拍销售默认渠道号",e.fsupply_partner,d.fsupply_partner)=5 then "小豹哥"
             when if(d.fchannel_name="竞拍销售默认渠道号",e.fsupply_partner,d.fsupply_partner)=6 then "顺丰"
        else "" end as "履约方",
        case when if(d.fchannel_name="竞拍销售默认渠道号",e.fchannel_name,d.fchannel_name)="支付宝小程序" then "支付宝小程序"
             when if(d.fchannel_name="竞拍销售默认渠道号",e.fchannel_name,d.fchannel_name) like '%天猫以旧换新%' then "天猫"
        else "闲鱼" end as "渠道大类",
        case when if(d.fchannel_name="竞拍销售默认渠道号",e.frecycle_type,d.frecycle_type)=1 then "邮寄"
             when if(d.fchannel_name="竞拍销售默认渠道号",e.frecycle_type,d.frecycle_type)=2 then "上门"
             when if(d.fchannel_name="竞拍销售默认渠道号",e.frecycle_type,d.frecycle_type)=3 then "到店"
        else "" end "回收方式",
        count(DISTINCT b.Fstock_no) as "销量",
        sum(if(b.fstock_no='TM0101230526001193',5437*100,if(b.fstock_no='XY0101240401000140',1595*100,b.Fretail_price*100))) as foffer_price,
        sum(if(d.fworkerorder_replenishment_price is not null,d.fpay_out_price+d.fworkerorder_replenishment_price*100,d.fpay_out_price)) as fcost_price,
        sum(d.Fdetection_price) as Fdetection_price
    from drt.drt_my33312_hsb_sales_product_t_stock_order_saleout a
    inner join drt.drt_my33312_hsb_sales_product_t_stock_order_saleout_detail b on a.Fstock_order_sn = b.Fstock_order_sn
    left join dws.dws_hs_order_detail  d on b.Fstock_no = d.fseries_number 
    left join dws.dws_hs_order_detail  e on d.fold_fseries_number=e.fseries_number
    inner join dws.dws_hs_order_detail_al as f on f.fseries_number=if(d.fchannel_name="竞拍销售默认渠道号",d.fold_fseries_number,d.fseries_number)
    where a.Fsource = 5 -- 手工单
    and a.fout_status=2
    and b.Fretail_price>0
    and d.fpay_out_price>0
    group by 1,2,3,4,5,6,7,8,9,10
