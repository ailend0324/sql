-- 基础维度：订单id，闲鱼订单id，fimei,闲鱼订单id，工程师，渠道，回收类型，闲鱼用户id
-- 履约维度：下单时间，上门时间，联系时间，付款时间，用户期望时间，期望时间_修正,下单时间修正，取消时间
-- 价格维度：下单价，检测价，竞拍价，
-- 指标维度：下单量，上门量，10min及时联系量，预约时间前下单取消量，准时上门量，
-- 售后维度：--20260420新增

with dws_channel_detail as (
-- 新增渠道明细表，链接（渠道，tag，maptag）
select
        tm.fp_id as fpid,
        tc.fchannel_id,
        tc.fchannel_name,
        case
            when zy.fchannel_id is not null then '自有渠道'
            when tc.fchannel_name like '%天猫上门%' then '天猫上门'
            when tc.fchannel_name like '%闲鱼上门竞价%' then '闲鱼上门竞价'
            when tc.fchannel_name = '闲鱼帮卖（上门）' or tc.fchannel_name = '闲鱼帮卖（到店）' then '闲鱼帮卖（到店&上门）'
            when tc.fchannel_name = "闲鱼保卖上门" then "保卖上门"

else '合作渠道' end as project,
        tt.ftag_id,
        tt.ftag_name
from drt.drt_my33310_recycle_t_channel     as tc
left join drt.drt_my33310_recycle_t_tag    as tt on tt.fchannel_id = tc.fchannel_id
left join drt.drt_my33310_recycle_t_maptag as tm on tm.ftag_id = tt.ftag_id
left join (select distinct fchannel_id from dwd.dwd_channel_detail_zy) as zy on tc.fchannel_id = zy.fchannel_id )


--- 预先设定订单id的取数范围
,t_xianyu_order_map as (
select 
	forder_id
	,fxy_order_id
from drt.drt_my33310_recycle_t_xianyu_order_map
where Fship_type = 8
and fauto_create_time >= to_date(months_sub(trunc(now(), 'month'), 6))
)


,order_id_xz as  (
select 
    t2.forder_id,t2.Frecycle_type,t2.fseries_number,t2.Fgoods_id,t2.fpay_out_time,t2.Fauto_create_time,t2.fpay_out_price,t2.fsender_phone,t2.ftest 
from t_xianyu_order_map t1
-- left join drt.drt_my33310_recycle_t_order t2 on t1.forder_id = t2.forder_id   --原代码
left join (select * from drt.drt_my33310_recycle_t_order where forder_id > 25043746 and to_date(forder_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),100))) t2 on t1.forder_id = t2.forder_id
--更新限制

where Frecycle_type in (2,3) -- 取上门&到店
and Fauto_create_time  >= to_date(months_sub(trunc(now(), 'month'), 6)) --订单数据时间范围近1个月
)

,t_xyxz_order_1 as (
select * from (
select 
    a.fpay_out_time as '付款时间',a.Fauto_create_time as '下单时间',a.fpay_out_price/100 as '付款金额',
    a.fsender_phone as '用户手机号',a.ftest,a.Frecycle_type,
    b.*,row_number() over (partition by b.forder_id order by b.Fid desc) num 
	,case when b.Fuser_expect_time is null or b.Fuser_expect_time = "0000-00-00 00:00:00:0" then ""
		when cast(substr(cast(b.Fuser_expect_time as string),12,2) as int) < 12 then cast(concat(substr(b.Fuser_expect_time,1,10)," 12:00:00") as timestamp)
		when cast(substr(cast(b.Fuser_expect_time as string),12,2) as int) < 15 then cast(concat(substr(b.Fuser_expect_time,1,10)," 15:00:00") as timestamp)
		when cast(substr(cast(b.Fuser_expect_time as string),12,2) as int) < 18 then cast(concat(substr(b.Fuser_expect_time,1,10)," 18:00:00") as timestamp)
		when cast(substr(cast(b.Fuser_expect_time as string),12,2) as int) = 18 then cast(concat(substr(b.Fuser_expect_time,1,10)," 19:00:00") as timestamp) 
		end Fhs_user_expect_end_time
		
from order_id_xz a
inner join drt.drt_my33310_recycle_t_xyxz_order b on a.forder_id = b.forder_id
where to_date(a.Fauto_create_time) <= "2026-01-19"
) a 
where a.num = 1 
) ---小站订单表去重

,t_xyxz_order_2 as (
select * from (
select 
    a.fpay_out_time as '付款时间',a.Fauto_create_time as '下单时间',a.fpay_out_price/100 as '付款金额',
    a.fsender_phone as '用户手机号',a.ftest,a.Frecycle_type,
    b.*,row_number() over (partition by b.forder_id order by b.Fid desc) num 
	,case when b.Fuser_expect_time is null or b.Fuser_expect_time = "0000-00-00 00:00:00:0" then ""
		when cast(substr(cast(b.Fuser_expect_time as string),12,2) as int) < 11 then cast(concat(substr(b.Fuser_expect_time,1,10)," 11:00:00") as timestamp)
		when cast(substr(cast(b.Fuser_expect_time as string),12,2) as int) < 13 then cast(concat(substr(b.Fuser_expect_time,1,10)," 13:00:00") as timestamp)
		when cast(substr(cast(b.Fuser_expect_time as string),12,2) as int) < 15 then cast(concat(substr(b.Fuser_expect_time,1,10)," 15:00:00") as timestamp)
		when cast(substr(cast(b.Fuser_expect_time as string),12,2) as int) < 17 then cast(concat(substr(b.Fuser_expect_time,1,10)," 17:00:00") as timestamp) 
		when cast(substr(cast(b.Fuser_expect_time as string),12,2) as int) < 19 then cast(concat(substr(b.Fuser_expect_time,1,10)," 19:00:00") as timestamp) 
		end Fhs_user_expect_end_time
		
from order_id_xz a
inner join drt.drt_my33310_recycle_t_xyxz_order b on a.forder_id = b.forder_id
where to_date(a.Fauto_create_time) > "2026-01-19"
) a 
where a.num = 1 
) ---小站订单表去重

,t_xyxz_order as (
select * from t_xyxz_order_1
union 
select * from t_xyxz_order_2
)
-- , t_xyxz_order as (

-- select 
--    a.fpay_out_time as '付款时间',a.Fauto_create_time as '下单时间',a.fpay_out_price/100 as '付款金额',
--    a.fsender_phone as '用户手机号',a.ftest,a.Frecycle_type,
--    b.*
--	,case when b.Fuser_expect_time is null or b.Fuser_expect_time = "0000-00-00 00:00:00:0" then ""
--		when cast(substr(cast(b.Fuser_expect_time as string),12,2) as int) < 12 then cast(concat(substr(b.Fuser_expect_time,1,10)," 12:00:00") as timestamp)
--		when cast(substr(cast(b.Fuser_expect_time as string),12,2) as int) < 15 then cast(concat(substr(b.Fuser_expect_time,1,10)," 15:00:00") as timestamp)
--		when cast(substr(cast(b.Fuser_expect_time as string),12,2) as int) < 18 then cast(concat(substr(b.Fuser_expect_time,1,10)," 18:00:00") as timestamp)
--		when cast(substr(cast(b.Fuser_expect_time as string),12,2) as int) = 18 then cast(concat(substr(b.Fuser_expect_time,1,10)," 19:00:00") as timestamp) 
--		end Fhs_user_expect_end_time
-- from order_id_xz a
-- inner join drt.drt_my33310_recycle_t_xyxz_order b on a.forder_id = b.forder_id
-- )  ---小站订单表去重

,fphone_num_list as (
select fdst_phone as fphone_num from drt.drt_my33313_recycle_t_xyxz_call_center_phone_reflect 
union all 
select distinct fphone_num from drt.drt_my33310_hjxmba_db_t_channel_user
)

-- 获取工程师首次外呼数据
,clerk_call as (
select 
    forder_id,fdialing_time
from (
    select 
        cast(forder_id as int) forder_id 
        ,fdialing_time
        ,row_number()over(partition by forder_id order by fdialing_time asc) rn
    from drt.drt_my33313_recycle_t_xyxz_call_center_info 
    where fcreate_time >= to_date(months_sub(trunc(now(), 'month'), 6))
    and fclerk_phone in (select fphone_num from fphone_num_list)
)a
where a.rn = 1
)


, dws_hs_order_detail as (
select * from (
select 
    b.*,row_number() over (partition by b.forder_id order by b.fdata_update_time desc) num 
from order_id_xz a
inner join dws.dws_hs_order_detail b on a.forder_id = b.forder_id
) a 
where a.num = 1 
) ---dws订单表去重

,recycle_ctox_rate as (
select * from 
(select 
    *,row_number()over(partition by forder_id order by fid desc) num
from drt.drt_my33310_recycle_t_xy_ctox_rate)a
where num = 1)  -- 回收评价表去重



, t_dispatch_order as (
select * from (
select *,row_number() over (partition by forder_id order by fdispatch_id desc) num from drt.drt_my33310_recycle_t_dispatch_order
) a 
where a.num = 1 
)
-----以下表格未使用-----
, dws_realtime_smhs_order_detail as (
select * from (
select *,row_number() over (partition by forder_id order by forder_time desc) num from dws.dws_realtime_smhs_order_detail
) a 
where a.num = 1 
)
----------以上表格未使用--------------------

, t_engineer_visit_log  as (
    select * from (
        select
            *, row_number() over ( partition by forder_number order by fid desc ) as num
        from drt. drt_my33314_replace_db_t_engineer_visit_log where ftype = 0 and fbusiness = 1) a
    where num = 1
    )
-- 加入工程师派单失败明细 type'类型【0：派单失败，1：派单成功，2：取消派单，3：待处理，4：待派单，5：派单中，6：订单完成，7：订单转派，8：顺丰回调异常】'

,order_remark_record as (
select 
Forder_id,group_concat(ifnull(Freason,Fremark)) as g_reason
from drt.drt_my33310_recycle_t_order_remark_record  
group by Forder_id

) -- 取消原因

, t_goods as (
select 
g.Fvaluation ,g.Flast_quote ,g.Fgoods_id 
from  order_id_xz xz 
left join drt.drt_my33310_recycle_t_goods  g on xz.Fgoods_id = g.Fgoods_id
where g.fcreate_time  >= to_date(months_sub(trunc(now(), 'month'), 6))
)--- 取下单价


, t_xy_order_data  as (
select 
xyd.forder_id 
,xyd.fxy_order_id
,if(xyd.frate_time > '2018-01-01',xyd.frate_time,null) as frate_time
,xyd.fuser_id
,if(xyd.frate_time > '2018-01-01',xyd.frate_grade,null) as frate_grade
,xyd.fclose_reason
,xyd.fsync_pay_out_time
from drt. drt_my33310_recycle_t_xy_order_data xyd
where Fauto_create_time  >= to_date(months_sub(trunc(now(), 'month'), 6))
) --- 闲鱼订单信息表


, dispatch_order_txn as (
select * from (
select 
tx.ftxn_id 
,tx.forder_id
,tx.fcreate_time
,row_number() over (partition by tx.forder_id order by tx.ftxn_id ) num
from order_id_xz xz 

left join drt.drt_my33310_recycle_t_dispatch_order_txn tx on xz.forder_id = tx.forder_id
where tx.fdispatch_status = 2 and to_date(tx.fcreate_time)  >= to_date(months_sub(trunc(now(), 'month'), 6))
) a 
where a.num = 1
) ---- 首次派单时间



, detect_detail_frist as (
select 
	fserial_number,COALESCE(fend_time,fdetect_date) fend_time,fdetect_price 
from (
select *,row_number() over (partition by fserial_number order by fdetect_record_id desc) num
from  dwd.dwd_detect_back_detect_detail 
where fuser_type = 1 and freport_type = 1 and Fdet_type = 0 
and fdetect_date > to_date(months_sub(trunc(now(), 'month'), 6))
and ds >= cast(to_date(months_sub(trunc(now(), 'month'), 6)) as string)
) a
where num = 1 
) -- 店员检测价


, detect_detail_last as (
select * from (
select 
    *,row_number() over (partition by fserial_number order by fdetect_record_id desc) num
from  dwd.dwd_detect_back_detect_detail 
where freport_type = 0 
and ds >= cast(to_date(months_sub(trunc(now(), 'month'), 6)) as string)
and  fdetect_date > to_date(months_sub(trunc(now(), 'month'), 6))
) a
where num = 1 
) -- 后端最后一次检测价

-- 类目调整
,t_pdt_class as (
select 
fid,fname as sub_fname,
case when fname in ('平板','平板电脑') then '平板'
		when fname in ('笔记本','笔记本电脑') then '笔记本'
		when fname in ('手机','') then '手机'
		when fname in ('单反闪光灯','单反转接环','移动电源','移动硬盘','云台','拍照配件/云台','增距镜') then '3C数码配件'
		when fname in ('彩色激光多功能一体机','复印打印多功能一体机','激光打印机','墨盒','收款机','投影机','硒鼓粉盒','针式打印机') then '办公设备耗材'

		when fname in ('CPU','电脑服务器','电脑固态硬盘','固态硬盘','电脑内存','内存条','电脑显卡',
							'显卡','电脑硬件套装','电脑主板','键盘','品牌台机','无线鼠标','显示器','一体机','组装台机') then '电脑硬件及周边'
		when fname in ('路由器') then '网络设备'
		when fname in ('PS游戏光盘/软件','其他游戏配件','游戏机') then '电玩'
		when fname in ('单反套机','单反相机','拍立得','摄像机','摄影机','数码相机','微单相机','相机镜头','运动相机','单反/微单套机','单反/微单相机') then '相机/摄像机'

		when fname in ('耳机','黑胶唱片机','蓝牙耳机','蓝牙音响/音箱','麦克风/话筒','影音播放器','智能音响/音箱') then '影音数码/电器'
		when fname in ('PS4游戏','PS5游戏','Switch游戏') then '游戏卡'
		when fname in ('VR眼镜头盔','按摩器','吹风机','磁吸式键盘','电子书','翻译器','风扇','加湿器','录音笔','美发器','手写笔','智能手写笔',                   
							'无人机','吸尘器','学习机','智能办公本','智能配饰','智能摄像','智能手表','智能手环') then '智能设备'
		else  fname end as fname
from drt.drt_my33310_recycle_t_pdt_class )

-- 是否新机单取消
, xianyu_neworder as (
select 
    fxy_order_id,fevent_type 
from dwd.dwd_recycle_t_xianyu_neworder_txn
where fevent_type = "CLOSED")

-- 下单城市对应的省份
, city_province as (
select 
    tc.fcity_name,tp.fprovince_name
from drt.drt_my33310_hjxmba_db_t_city tc
left join drt.drt_my33310_hjxmba_db_t_province tp on tc.fprovince_id = tp.fprovince_id
)

, sound_record as (

select a.forder_id,group_concat(concat('第',cast(a.num as string),"段: ",a.fsound_url),"  ;   ") as fsound_url_list 
  from (
select 
sr.forder_id,sr.fsound_url,row_number() over (partition by sr.forder_id order by sr.fid ) num
from order_id_xz xz 
left join drt.drt_my33313_xyxz_manage_db_t_xyxz_order_sound_record sr on sr.forder_id = xz.forder_id
where 
sr.fsound_url is not null and sr.fsound_url <> ''
) a 
group by a.forder_id
) ---- 录音保存链接地址


, checkin_dress as (
select * from (
select *,row_number() over (partition by forder_id order by fid desc) num
from 
drt.drt_my33313_xyxz_order_db_t_checkin_dress
) a 
where a.num = 1
) ---- 上门工程师装着抽查照片保存链接地址


,order_status as (
select forder_id,
		min(case forder_status when 80 then fupdate_time else null end) as Forder_cancel_time,-- 取消时间
		min(case forder_status when 100 then fupdate_time else null end) as Fbargain_time,-- 进入客服议价时间
		min(case forder_status when 60 then fupdate_time else null end) as Fwait_pay_time,-- 等待付款时间
		min(case forder_status when 350 then fupdate_time else null end) as Fwarehousing_time,--入库时间
		min(case forder_status when 130 then fupdate_time else null end) as Fuser_confirm_time,-- 用户确认收款时间
		min(case forder_status when 351 then fupdate_time else null end) as Freturn_finish_time,-- 退货完成时间
		min(case forder_status when 120 then fupdate_time else null end) as Freturn_conf_time,-- 用户已确认退货时间
		min(case forder_status when 220 then fupdate_time else null end) as Fstatus_door_time,-- 上门取件时间
		min(case forder_status when 52 then fupdate_time else null end) as Fstatus_firdetect_time,-- 首次检测时间
		min(case forder_status when 90 then fupdate_time else null end) as Freturn_begin_time,-- 发起退货时间
		min(case forder_status when 340 then fupdate_time else null end) as Fstatus_pending_time,-- 待处理时间
	    min(case forder_status when 24 then fupdate_time else null end) as Fsfsm_time, -- 顺丰上门时间
	    min(case forder_status when 20 then fupdate_time else null end) as Fsend_time,  -- 寄出时间 
	    min(case forder_status when 40 then fupdate_time else null end) as foff_detect_time,   -- 待检测 
	    now() 数据更新时间
	from
		drt.drt_my33310_recycle_t_order_txn where fauto_create_time>= to_date(date_sub(now(), 185))
group by forder_id
)

-- 新增取消类型，退单金额，退单时间，期望时间修正，是否重复订单
-- 用户主动取消       
, yonghu_quxiao as(
select 
    fxy_order_id,fhsb_order_id,fxy_order_status,fxy_status_desc
from drt.drt_my33310_recycle_t_xianyu_order_txn 
where fxy_order_status = 102 -- 用户主动取消
and substr(fcreate_dtime,1, 10) >= to_date(months_sub(trunc(now(), 'month'), 6))
)

-- 新增闲鱼回传节点判断
, zhijian as(
select 
    fxy_order_id,fhsb_order_id,fxy_order_status,fxy_status_desc
from drt.drt_my33310_recycle_t_xianyu_order_txn 
where fxy_order_status = 3 -- 已质检
and substr(fcreate_dtime,1, 10) >= to_date(months_sub(trunc(now(), 'month'), 6))
)

,hjx_order as (  -- 换机侠订单ID
    select
        order_no,
        xy_item_id,
        price,
        finalBidPrice
    from(
        select
            order_no,
            xy_item_id,
            id,
            price,
            get_json_object(ext_info, '$.finalBidPrice') finalBidPrice,
            row_number() over(partition by order_no order by id asc) as frn
        from drt.drt_my33321_hjx_hjx_order a
        where source = 2 -- 闲鱼竞价
        and fthe_month >= cast(substr(regexp_replace(cast(months_sub(trunc(now(), 'month'), 6) as STRING), '-', ''), 1, 6) AS INT)
    ) t where frn = 1
)


-- 匹配换机侠订单id，回收宝中标后订单id等信息
,jj_hjx_order as (
	select 
		t1.forder_id 
		,j.order_no hjx订单ID
		,c.fseries_number 回收宝中标后条码
		,c.forder_id 回收宝中标后的订单ID
	from drt.drt_my33310_recycle_t_xianyu_auction_order t1
	left join hjx_order j on t1.Fauction_goods_id = j.xy_item_id
-- 	left join  drt.drt_my33310_recycle_t_order c on j.order_no = c.fexternal_order_no  --原代码
    left join (select * from drt.drt_my33310_recycle_t_order where forder_id > 25043746 and to_date(forder_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),200))) c on j.order_no = c.fexternal_order_no
    --更新限制

)
,sf_order as (
select 
    t1.forder_id,t1.fseries_number,t2.Fchannel_id as 顺丰单号
-- from drt.drt_my33310_recycle_t_order t1   --原代码
from (select * from drt.drt_my33310_recycle_t_order where forder_id > 25043746 and to_date(forder_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),200))) t1
--更新限制
left join drt.drt_my33310_recycle_t_logistics t2 on t1.flogistics_id = t2.Flogistics_id  
where t1.Fauto_create_time  >= to_date(months_sub(trunc(now(), 'month'), 6))
)

, use_track as (
SELECT 
    cast(forder_id as int) forder_id
    ,group_concat(fremark ,'; ') as 备注内容
from 
    (select * FROM drt.drt_my33310_recycle_t_order_user_track 
    where `Fauto_create_time` >= to_date(months_sub(now(),6))
    and FoperType in (0,1)
    order by Fauto_create_time asc)a
group by forder_id)



-----基础字段--------------------------
, xz_order_all_detail_base as(

select
cast(o.forder_id as string) as forder_id 
,cast(xz.fstore_id as string) as '门店id'
,o.forder_num
,o.Fseries_number
,o.fimei
,o.fsender_phone as '用户手机号'
,regexp_replace(regexp_replace(xz.Fclerk_name,'[(|（].*',"")," ","") as '工程师'
,CASE
    WHEN (shop.fshop_name like '%闲鱼小站%' or shop.fshop_name = "回收宝手机数码回收（深圳店）") THEN '自营门店' 
    WHEN (shop.fshop_name like '回收宝%')  THEN '加盟门店' 
    ELSE "" END '履约方'
,REPLACE (
    REPLACE ( REPLACE ( REPLACE (shop.fshop_name, '闲鱼小站', '' ), '×', "" ), "回收宝（", "" ),
    "）",
    "" 
    ) '门店名称'
,cast(msm.fmerchant_id as string) as '商户ID'
,b.Fmerchant_name '商户名称'
,m.Fxy_channel '闲鱼渠道'
,m.Fsub_channel '闲鱼子渠道'
,case 
    when m.Fxy_channel = 'idle' and m.fsub_channel = 'HSBexternal' then '闲鱼上门竞价（挖单）'
    when m.Fxy_channel = 'idle' and m.fsub_channel = 'HSBditui' then '闲鱼上门竞价（地推）'
   	when m.Fxy_channel = 'idle' and m.fsub_channel = 'ccGuide' then '闲鱼上门竞价（CC）'
    else '闲鱼上门竞价' 
end as '竞价渠道'
,m.fseller_address '用户地址'
,m.Fseller_nick '用户昵称'
,m.Fseller_real_name '用户名称'
,cast(o.Fauto_create_time as timestamp) as '下单时间'
,g.flast_quote / 100  "下单金额"

,st.forder_status_name "下单状态"
,vl.fdesc '派单失败记录'
,cast(xz.Fallocation_time as timestamp) '派单时间'
,cast(dot.fcreate_time as timestamp)  '首次派单时间'
,if(m.Fxy_channel = "idle" and m.fsub_channel in ("HSBexternal","HSBditui") ,null ,cast(xz.Fuser_expect_time as timestamp)) '用户期望开始时间'
,if(m.Fxy_channel = "idle" and m.fsub_channel in ("HSBexternal","HSBditui") ,null ,Fhs_user_expect_end_time) as '用户期望结束时间' -- Fhs_user_expect_end_time
	
,if(m.Fxy_channel = "idle" and m.fsub_channel in ("HSBexternal","HSBditui") ,null ,xz.fuser_start_time) '用户约定开始时间'
,if(m.Fxy_channel = "idle" and m.fsub_channel in ("HSBexternal","HSBditui") ,null ,xz.Fuser_end_time) '用户约定结束时间'
,cast(xz.Fuser_call_time as timestamp) '呼出时间'

,cast(clc.fdialing_time as timestamp) '工程师外呼时间'

,cast(xz.Fengineer_arrive_time as timestamp) '工程师到达时间'
,cast(if(zj.fxy_order_status = 3, COALESCE(fd.fend_time,sm.Front_detect_time,xz.Fdetection_end_time) ,null) as timestamp)'检测上报时间'
,cast(if(zj.fxy_order_status = 3, xz.Fengineer_detection_time,null) as timestamp)'检测上报开始时间'
-- ,fd.fdetect_date '检测上报时间' --- t+1 数据
,if(zj.fxy_order_status = 3,COALESCE(fd.fdetect_price/100,sm.fclerk_detect_price/100,sm.front_detect_price_rpt/100),null) '工程师检测价'
,ld.fdetect_price/100'后端最后一次检测价'
,ld.fdetect_date '后端最后一次检测时间'
,hs.fdetect_price '订单最后检测价'
,cast(hs.fend_time as timestamp) '订单最后检测时间'
,cast(os.Forder_cancel_time as timestamp) '取消时间'
,cast(os.Fsend_time as timestamp) '顺丰上门时间'
,if(xyd.fclose_reason <> "",xyd.fclose_reason,grm.g_reason) as '取消原因'
,o.Fproduct_name "机型"
,cla.fname "类目"
,cla.sub_fname "二级类目"
,ca.Fcategory_name "品牌"
,case when ao.Fauction_type = 1 then "兜底回收" when ao.Fauction_type = 2 then "竞价回收" else "" end '拍卖类型'
,cast(ao.Fbuyer_order_id as string) "买家单号"
,cast(if(ao.Fpayment_time <> "0000-00-00 00:00:00.0",ao.Fpayment_time,xyd.fsync_pay_out_time) as timestamp) as "付款时间"
,o.Fpay_out_price / 100 "付款金额"
,ao.Fauction_price / 100 "竞价中标价"
,ao.Ftrans_price / 100 "竞价成交价"
,ao.Fstart_price / 100 "竞价兜底价"
,case when ao.Flogistics_status = 1 then "待寄出"
	when ao.Flogistics_status = 2 then "待发货"
	when ao.Flogistics_status = 3 then "已发货"
	when ao.Flogistics_status = 4 then "已签收" else null end "物流状态"
,case when ao.Fauction_type = 1 then sf.顺丰单号 when ao.Fauction_type = 2 then ao.Ftracking_no else "" end "物流运单号"
,ao.Fvideo_url "封箱视频"
,cast(ao.Fsend_time as timestamp) as "确认已发货时间"
,ao.Fmerchant_sn '商家条码'
,xz.Fstore_name '门店名称-全'
,case when o.Frecycle_type = 3 then ct2.fcity_name 
    else ct1.fcity_name end '下单城市'  -- 回收类型=到店,取到店城市,其他下单所在城市

,do.Fcity "派单城市"
,do.Fuser_addr '派单用户地址'
,cast(COALESCE(hsra.fxy_rate_time,xyd.frate_time ) as timestamp) '评价时间'  

,case when hsra.fxy_rate_time is not null then hsra.frate_grade
	when xyd.frate_time is not null then xyd.frate_grade
	else null end '评价等级'
,COALESCE(hsra.fcontent,cc.Frate_content)'评价内容'
,case o.Frecycle_type
    when 1 then '邮寄'
    when 2 then '上门'
    when 3 then '到店'
    when 4 then 'ATM'
    end '回收类型'

    
,cast(o.ftest as string ) as ftest
,NOW() as '数据更新时间'
,xyd.fuser_id '闲鱼用户id'
,cast(xyd.fxy_order_id as string) as  '闲鱼订单id'
,o.fexternal_order_no '外部订单号'
,case when o.fchannel_id = 10001016 then '合作渠道'
	else cd.project end as project
,cd.fchannel_name
,cast(cd.fchannel_id as string ) fchannel_id 
,cd.ftag_name


 ,case 
    when g.flast_quote/100 between 0 and 1000 then '0-1000'
    when g.flast_quote/100 between 1000 and 2000 then '1000-2000'
    when g.flast_quote/100 between 2000 and 3000 then '2000-3000'
    when g.flast_quote/100 between 3000 and 5000 then '3000-5000'
    when g.flast_quote/100 > 5000 then '5000+' 
    end as '下单价位段'

,if(os.Forder_cancel_time is not null,cast((unix_timestamp(os.Forder_cancel_time) -unix_timestamp(o.Fauto_create_time))/60 as int),null) as '取消时间间隔' ----分钟
,if(xz.Fuser_call_time is not null and xz.Fallocation_time is not null ,cast((unix_timestamp(xz.Fuser_call_time) -unix_timestamp(xz.Fallocation_time))/60 as int),null) as '联系时间间隔' ----分钟
,sr.fsound_url_list '录音链接'
,cds.fcontent '工程师着装抽查结果'
, regexp_replace( cds.fimages ,'(\\[|\\]|\\")',"") '工程师着装图片链接'
,ut.备注内容

,case
    when from_unixtime(unix_timestamp(o.Fauto_create_time),"HH:mm") between "00:00" and "09:20" then cast(concat(substr(o.Fauto_create_time,1,10),' 09:20:00') as timestamp)
    when from_unixtime(unix_timestamp(o.Fauto_create_time),"HH:mm") between "09:20" and "21:00" then cast(substr(o.Fauto_create_time,1,22) as timestamp)
    else cast(concat(substr(to_date(days_add(o.Fauto_create_time,1)),1,10),' 09:20:00') as timestamp)
    end as '下单时间_竞价_修正夜间'

,case when xz.Fuser_end_time  is null or xz.Fuser_end_time = "0000-00-00 00:00:00.0" then cast(xz.Fhs_user_expect_end_time as timestamp)
    else cast(xz.Fuser_end_time as timestamp) end 期望时间_修正
    
,count(o.forder_id)over(partition by o.fsender_phone,substr(cast(o.Fauto_create_time as string),1,7),project) 'chongfu量'


,case when os.Forder_cancel_time is not null and yq.fxy_status_desc = "卖家关闭订单" then "用户主动取消"
        when os.Forder_cancel_time is not null and yq.fxy_status_desc is null then "回收商&工程师取消" 
        else null end  '取消类型'
  
,jhjx.hjx订单ID
,jhjx.回收宝中标后条码
,cast(jhjx.回收宝中标后的订单ID as string) 回收宝中标后的订单ID
,if(jhjx.回收宝中标后的订单ID is not null ,jhjx.回收宝中标后的订单ID,o.forder_id) forder_id_correct

from  order_id_xz oxz

-- left join drt.drt_my33310_recycle_t_order o on o.forder_id = oxz.forder_id   --原代码
left join (select * from drt.drt_my33310_recycle_t_order where forder_id > 25043746 and to_date(forder_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),200))) o on o.forder_id = oxz.forder_id
--更新限制

left join dws_hs_order_detail hs on hs.forder_id = oxz.forder_id
LEFT JOIN drt.drt_my33310_recycle_t_order_status st ON o.Forder_status = st.Forder_status_id

left join dws_channel_detail cd on o.fpid=cd.fpid
-- left join drt.drt_my33310_recycle_t_product  p on o.Fproduct_id = p.Fproduct_id   --原代码
left join (select * from drt.drt_my33310_recycle_t_product where to_date(fauto_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),200))) p on o.Fproduct_id = p.Fproduct_id
--更新限制

left join drt.drt_my33310_recycle_t_category ca on ca.Fcategory_id = p.Fcategory_id 

left join t_pdt_class  cla on cla.Fid = p.Fclass_id
left join t_goods g on  o.Fgoods_id=g.Fgoods_id
left join t_xyxz_order xz on xz.forder_id = oxz.forder_id 
left join t_dispatch_order do on do.forder_id = oxz.forder_id
left join dws.dws_smhs_order_detail sm on sm.forder_id = o.forder_id
left join drt.drt_my33310_recycle_t_xianyu_order_map m on m.forder_id = oxz.forder_id and m.forder_id > 0 and m.fcreate_dtime  >= to_date(months_sub(trunc(now(), 'month'), 1))

LEFT JOIN (select distinct forder_id,fuser_id,Fxy_order_id,Fseller_nick,Fseller_phone,Fseller_real_name,Fseller_alipay_id,Frate_grade,Frate_time
            from  dws.dws_hs_order_detail_al) c on c.forder_id = o.forder_id
left join drt.drt_my33310_hjxmba_db_t_city ct1 on ct1.fcity_id = o.fcity_id 

left join t_engineer_visit_log vl on vl.forder_number = o.forder_num
left join order_remark_record grm on grm.forder_id = o.forder_id
left join t_xy_order_data xyd on xyd.forder_id = oxz.forder_id 

-- left join drt.drt_my33310_recycle_t_xianyu_cashback_record  AS cc on cc.Forder_id = oxz.Forder_id   --原代码
left join (select * from drt.drt_my33310_recycle_t_xianyu_cashback_record where to_date(fcreate_dtime)>=to_date(date_sub(from_unixtime(unix_timestamp()),200))) cc on cc.Forder_id = oxz.Forder_id 
--更新限制

left join (select fshop_id,fmerchant_id 
            from drt.drt_my33317_pub_server_merchant_center_db_t_merchant_staff_map group by fshop_id,fmerchant_id  ) msm on msm.fshop_id = xz.fstore_id
left join drt.drt_my33317_pub_server_merchant_center_db_t_merchant_base_info b on b.fmerchant_id  =  msm.fmerchant_id 

left join dispatch_order_txn dot on dot.forder_id = oxz.forder_id 
left join detect_detail_last ld on ld.fserial_number = oxz.Fseries_number
left join detect_detail_frist fd on fd.fserial_number = oxz.Fseries_number

left join drt.drt_my33317_pub_server_merchant_center_db_t_merchant_shop_info shop on shop.fshop_id = xz.fstore_id

left join drt.drt_my33310_hjxmba_db_t_city ct2 on shop.Fcity_code = ct2.fcity_id 

left join sound_record sr on sr.Forder_id = oxz.Forder_id
left join checkin_dress cds on cds.Forder_id  = cast(oxz.Forder_id as string)
left join recycle_ctox_rate hsra on o.forder_id = hsra.forder_id -- 关联回收评价表
left join order_status os on oxz.forder_id = os.forder_id  
-- left join drt.drt_my33310_recycle_t_xianyu_auction_order ao on oxz.forder_id = ao.forder_id   --原代码
left join (select * from drt.drt_my33310_recycle_t_xianyu_auction_order where to_date(fauto_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),200))) ao on oxz.forder_id = ao.forder_id
--更新限制


left join yonghu_quxiao yq on o.forder_id = yq.fhsb_order_id
left join clerk_call clc on o.forder_id = clc.forder_id 
left join jj_hjx_order jhjx on o.forder_id =  jhjx.forder_id 
left join sf_order sf on sf.forder_id = o.forder_id 
left join zhijian zj on o.forder_id = zj.fhsb_order_id 
left join use_track ut on o.forder_id  = ut.forder_id 
--------------------------------基础表过滤条件-------------------------------------------------------------------------
where o.Fauto_create_time  >= to_date(months_sub(trunc(now(), 'month'), 6))
and    o.Fvalid > 0 
and  (o.fbusiness_mode not in (14,13,1,2,7,8,12,17) --剔除非回收业务类型
or  cd.fchannel_id in (10001111,10001112)  --加入上门到店帮卖业务
)
and o.Frecycle_type in (2,3) 
--------------------------------基础表过滤条件-------------------------------------------------------------------------
)

------------------------------新增售后订单------------------------
,xy_hsb_after_sales_info as
(

-- 闲鱼竞价售后

SELECT
'闲鱼竞价售后' 售后订单来源
,cast(xyjj.fauto_create_time as timestamp) as 售后发起时间
-- ,freinspection_imei 串号
,cast(xy.forder_id as string) 订单ID
-- ,ret.fserial_number 条码
,xyjj.fxy_buyer_order_id 闲鱼卖家单ID
,od.fproduct_name 售后机型
,fxy_reason 售后发起原因
,fxy_remarks 售后备注
,fxy_applicate_certificate 售后凭证链接1
,faudit_deliver_url        售后凭证链接2
,fxy_amount/100 申请补差金额
,faudit_amount/100 成功售后补差金额
,cast(faudit_time as timestamp) 售后审核时间
,case faudit_result when 1 then '通过' when 2 then '不通过' else null end 售后审核结果
,faudit_remarks 售后审核说明
,od.forder_status_name 售后审核状态

FROM drt.drt_my33310_xyjj_after_sales_t_auction_after_sales_order_info xyjj

-- left join drt.drt_my33310_recycle_t_xyxz_order xy  on xyjj.fxy_imei = xy.fserial_number --原代码
left join (select * from drt.drt_my33310_recycle_t_xyxz_order where to_date(forder_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),200))) xy  on xyjj.fxy_imei = xy.fserial_number
--更新限制

-- left join dws.dws_hs_order_detail od  on xy.forder_id = od.forder_id  --原代码
left join (select * from dws.dws_hs_order_detail where to_date(forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),200))) od  on xy.forder_id = od.forder_id
--更新限制

where fxy_imei is NOT NULL
and xyjj.fxy_imei != '000000000000000'
and fxy_imei != ""
and xyjj.fhsb_auction_merchant_name = '回收宝小站（上门）'
and xyjj.forder_status_id != 6
and xyjj.forder_status_id != 99
and xyjj.fvalid = 1 
and od.forder_status_name != '已取消'
and xy.forder_status != 3
and od.fpid_name = '闲鱼上门竞价'
and xyjj.fauto_create_time >= to_date(date_sub(now(), 100))
and od.fpay_time is not null
-- order by xyjj.fauto_create_time desc

UNION ALL

-- 回收宝竞价售后
SELECT
'回收宝竞价售后' 售后订单来源
,cast(ret.fcreate_time as timestamp) as 售后发起时间
-- ,ffirstinspection_imei 串号
,cast(xy.forder_id as string) 订单ID
-- ,ret.fserial_number 条码
,null as 闲鱼卖家单ID
,ret.freinspection_product 售后机型
,bid.fdiff_options 售后发起原因
,ret.fsku_options 售后备注
,regexp_replace(regexp_replace(ret.fvideo_url, '(\\[|\\]|")', ''), '\\\\/', '/') AS 售后凭证链接1
,regexp_replace(regexp_replace(ret.fphoto_url, '(\\[|\\]|")', ''), '\\\\/', '/') AS 售后凭证链接2
,cast(ret.ffirstinspection_price as int)/100 - cast(ret.freinspection_price as int)/100 申请补差金额
,cast(bid.finternal_compensation as int)/100 成功售后补差金额
-- ,od.fshop_city_name 城市
-- ,xy.Fclerk_name 工程师
,case bid.fstatus when '1' then cast(bid.fupdate_time as timestamp) else '待处理' end 售后审核时间
,case fprocess_method when '1' then '不售后' when '2' then '闲鱼售后' when '3' then '内部补差价' else null end 售后审核结果
,case bidl.fdescribe when '创建记录' then '' else bidl.fdescribe end 售后审核说明
,od.forder_status_name 售后审核状态


FROM drt.drt_my33312_detection_t_bidding_after_sales_order bid

-- left join drt.drt_my33310_recycle_t_xyxz_order xy  on bid.forder_id = cast(xy.forder_id as string)  --原代码
left join (select * from drt.drt_my33310_recycle_t_xyxz_order where to_date(forder_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),200))) xy  on bid.forder_id = cast(xy.forder_id as string)
--更新限制

left join drt.drt_my33312_detection_t_xy_recheck_detect_task ret  on ret.fserial_number = bid.fserial_number
-- left join dws.dws_hs_order_detail od on xy.forder_id = od.forder_id  --原代码
left join (select * from dws.dws_hs_order_detail where to_date(forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),200))) od on xy.forder_id = od.forder_id
--更新限制

left join drt.drt_my33312_detection_t_bidding_after_sales_order_log bidl  on bid.fid = bidl.fbidding_after_sales_id and bidl.fupdate_time = bid.fupdate_time

where 
-- fxy_options is NOT NULL and 
ret.ffirstinspection_imei != '000000000000000'
and ret.ffirstinspection_imei is not null
and ret.ffirstinspection_imei != ''
and od.forder_status_name != '已取消'
and ret.fcreate_time >= to_date(date_sub(now(), 100))
-- and ret.fcreate_time >= '2026-04-13' and ret.fcreate_time <= '2026-04-20'
-- and o.下单状态 != '已取消'
-- and od.fpay_time is not null
-- order by ret.fcreate_time desc
)





----------------------------------------- 复合条件字段 --------------------------------------------------------


select 

o.* ---- 回收基础表所有字段  
,cast(substr(cast(o.期望时间_修正 as string),1,10) as timestamp) 期望时间_修正_年月日

,cp.fprovince_name  as '省份'
,case when o.用户约定开始时间 is not null and o.用户约定开始时间 <> "0000-00-00 00:00:00.0" then 1 else 0 end 工程师填写数量
,case when o.用户约定开始时间 is null or o.用户约定开始时间 = "0000-00-00 00:00:00.0" then 0
    when from_unixtime(unix_timestamp(o.用户约定结束时间),'yyyy-MM-dd HH:mm:ss') < from_unixtime(unix_timestamp(o.用户期望开始时间),'yyyy-MM-dd HH:mm:ss') then 1
    else 0 end '超前修改量'
	
,case when o.用户约定开始时间 is null or o.用户约定开始时间 = "0000-00-00 00:00:00.0" then 0
	when from_unixtime(unix_timestamp(o.用户约定结束时间),'yyyy-MM-dd HH:mm:ss') > from_unixtime(unix_timestamp(o.用户期望结束时间),'yyyy-MM-dd HH:mm:ss') then 1
    else 0 end '滞后修改量'
,case when o.用户约定开始时间 is not null and hour(from_unixtime(unix_timestamp(o.用户约定结束时间),'yyyy-MM-dd HH:mm:ss')) >= 19 then 1 else 0 end '超过19点数量'
,case when o.工程师到达时间 is null then null
	when o.检测上报时间 is null then 15
	else round((unix_timestamp(o.检测上报时间) -unix_timestamp(o.检测上报开始时间))/60 ,2) end '检测时间间隔'

,if(付款时间 is not null and 顺丰上门时间 is null, round((unix_timestamp(o.数据更新时间) -unix_timestamp(o.付款时间))/60 ,2),null) '已成交未寄出时间差'
,if(付款时间 is not null and 顺丰上门时间 is not null, round((unix_timestamp(顺丰上门时间) -unix_timestamp(o.付款时间))/60 ,2),null) '已成交已寄出时间差'
,if(付款时间 is not null,cast(concat(substr(to_date(days_add(o.付款时间,1)),1,10),' 12:00:00') as timestamp),NULL)  '应寄出时间'


,case when o.chongfu量 >= 2 and 付款时间 is null then 1 else 0 end '重复订单'
,case when o.取消类型 = '用户主动取消' and if(取消时间 is not null and 取消时间间隔 < 10,1,0) = 1 then '10min内取消'
      when o.chongfu量 >= 2 and 付款时间 is null then '重复订单'
      else '有效订单' end '有效订单筛选'


,if(竞价渠道 = "闲鱼上门竞价（挖单）",1,0) 竞价挖单量
,if(竞价渠道 = "闲鱼上门竞价（挖单）" and 付款金额 is not null,1,0) 挖单成交量
,if(检测上报时间 is not null and 竞价渠道 = "闲鱼上门竞价（挖单）",1,0) as '挖单上门后检测量'

,if(下单时间 is not null,1,0) as '下单量'
,if(下单时间 is not null and 竞价渠道 in ('闲鱼上门竞价','闲鱼上门竞价（CC）'), 1, 0) as '闲鱼上门竞价下单量'
,if(付款金额 is not null,1,0) as '成交量'
,if(呼出时间 is not null ,1,0) as '联系量'

,if(工程师外呼时间 is not null,1,0) as '工程师联系量'

,if(派单时间 is not null,1,0) as '派单量'
,if(工程师到达时间 is not null,1,0) as '上门量'

,if(工程师到达时间 is not null and 工程师到达时间 < 期望时间_修正 and 竞价渠道 in ('闲鱼上门竞价','闲鱼上门竞价（CC）'),1 ,0)  '准时上门量'

	
,if(工程师到达时间 is null and 取消时间 is not null and 取消时间 < 期望时间_修正 and 竞价渠道 in ('闲鱼上门竞价','闲鱼上门竞价（CC）'),1,0) '预约时间前下单取消量'
,if(派单时间 is not null and 工程师到达时间 is null and 取消时间 is not null and 取消时间 < 期望时间_修正 and 竞价渠道 in ('闲鱼上门竞价','闲鱼上门竞价（CC）'),1,0) '预约时间前派单取消量'


,if(检测上报时间 is not null,1,0) as '上门后检测量'

,if(取消时间 is not null and 工程师到达时间 is  null,1,0) as '上门前取消量'
,if(取消时间 is not null and 工程师到达时间 is not null and 工程师到达时间 < 取消时间,1,0) as '上门后取消量'

,if(取消时间 is not null and 派单时间 is  null ,1,0) as '派单前取消量'

,if(取消时间 is not null and 取消时间间隔 < 10,1,0) as '取消时间小于10分钟'
,if(取消时间 is not null and 取消时间间隔 < 30,1,0) as '取消时间小于30分钟'
,if(取消时间 is not null and 取消时间间隔 < 60,1,0) as '取消时间小于60分钟'

,if(if(呼出时间 is not null and 下单时间_竞价_修正夜间 is not null and 竞价渠道 in ('闲鱼上门竞价','闲鱼上门竞价（CC）'),cast((unix_timestamp(呼出时间) -unix_timestamp(下单时间_竞价_修正夜间))/60 as int),null)< 30,1,0) as '30min内联系_修正夜间' 
,if(if(呼出时间 is not null and 下单时间_竞价_修正夜间 is not null and 竞价渠道 in ('闲鱼上门竞价','闲鱼上门竞价（CC）'),cast((unix_timestamp(呼出时间) -unix_timestamp(下单时间_竞价_修正夜间))/60 as int),null)< 10,1,0) as '10min内联系_修正夜间'


,if(if(工程师外呼时间 is not null and 下单时间_竞价_修正夜间 is not null and 竞价渠道 in ('闲鱼上门竞价','闲鱼上门竞价（CC）'),cast((unix_timestamp(工程师外呼时间) -unix_timestamp(下单时间_竞价_修正夜间))/60 as int),null)< 30,1,0) as '30min内联系_修正夜间_工程师' 
,if(if(工程师外呼时间 is not null and 下单时间_竞价_修正夜间 is not null and 竞价渠道 in ('闲鱼上门竞价','闲鱼上门竞价（CC）'),cast((unix_timestamp(工程师外呼时间) -unix_timestamp(下单时间_竞价_修正夜间))/60 as int),null)< 10,1,0) as '10min内联系_修正夜间_工程师'


-- 只看完结订单的评价数据，取消订单的评价不做查看
,if(评价时间 is not null and 评价等级 in (1,8),1,0) as '好评数量'
,if(评价时间 is not null and 评价等级 in (0,2,5),1,0) as '差评数量'
,if(评价时间 is not null ,1,0) as '评价订单数量'
,case
    when 评价时间 is null then '未评价'
    when 评价等级 in (1,8) then '好评'
    when 评价等级 in (0,2) then '差评'
	when 评价等级 = 5 then "中评"
    end as '评价分类'

,if(工程师到达时间 is null and 取消时间 is  null and 付款金额 is  null,1,0) as '挂单数量'

--补充售后信息
,if(售后订单来源 is not null,1,0) as '售后数量'
,售后订单来源
,售后发起时间
,闲鱼卖家单ID
,售后机型
,售后发起原因
,售后备注
,售后凭证链接1
,售后凭证链接2
,申请补差金额
,成功售后补差金额
,售后审核时间
,售后审核结果
,售后审核说明
,售后审核状态

from xz_order_all_detail_base o 
left join city_province cp on cp.fcity_name = o.下单城市
left join xy_hsb_after_sales_info a on a.订单ID=o.forder_id
