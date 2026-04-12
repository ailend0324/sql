-- 目的：生成“验货宝履约明细”，整合订单、检测结论、商品等级、入/出库、费用、问密工单与各模块检测节点
-- 使用说明：
-- - 时间：主查询按 forder_time 近 365 天；各 CTE 内部也有限定（90/365 天）
-- - 序列号范围：仅 '01'、'02' 开头
-- - 结果粒度：按 dws.dws_xy_yhb_detail 的订单粒度，一单一行
with detect as (
-- CTE[detect]：聚合最近一次检测的外观/拆修等问题结论（按序列号），输出各问题类型的计数
select 
    a.fserial_number,
    get_json_object(a.fgoods_level,'$.levelName') as flevelname,
    count(case when b.fissue_name='屏幕外观划痕' and b.fanswer_name="细微" then a.fserial_number else null end) as fwaiguan_xiwei,
    count(case when b.fissue_name='外壳划痕' and b.fanswer_name="细微" then a.fserial_number else null end) as fwaikexiwei,
    count(case when b.fissue_name='外壳磕碰' and b.fanswer_name="细微" then a.fserial_number else null end) as fkepeng_xiwei,
    count(case when b.fissue_name='外壳印渍' and b.fanswer_name="细微" then a.fserial_number else null end) as fyinzi_xiwei,
    count(case when b.fissue_name='前摄像头外观' and b.fanswer_name like "%轻微偏移%" then a.fserial_number else null end) as fqianshe_qingweipianyi,
    count(case when b.fissue_name='前摄像头外观' and b.fanswer_name like "%轻微有灰%" then a.fserial_number else null end) as fqianshe_qingweiyouhui,
    count(case when b.fissue_name='后摄像头外观' and b.fanswer_name like "%轻微偏移%" then a.fserial_number else null end) as fhoushe_qingweipianyi,
    count(case when b.fissue_name='后摄像头外观' and b.fanswer_name like "%轻微有灰%" then a.fserial_number else null end) as fhoushe_qingweiyouhui,
    count(case when b.fissue_name='外壳掉漆' and b.fanswer_name="细微" then a.fserial_number else null end) as fdiaoqi_xiwei,
    count(case when b.fissue_name='外壳弯曲变形' and b.fanswer_name="细微" then a.fserial_number else null end) as fwanqu_xiwei,
    count(case when b.fissue_name='屏幕外观划痕' and b.fanswer_name="无" then a.fserial_number else null end) as fwaiguan_wu,
    count(case when b.fissue_name='外壳划痕' and b.fanswer_name="无" then a.fserial_number else null end) as fwaikewu,
    count(case when b.fissue_name='外壳磕碰' and b.fanswer_name="无" then a.fserial_number else null end) as fkepeng_wu,
    count(case when b.fissue_name='外壳印渍' and b.fanswer_name="无" then a.fserial_number else null end) as fyinzi_wu,
    count(case when b.fissue_name='前摄像头外观' and b.fanswer_name like "%正常%" then a.fserial_number else null end) as fqianshe_zhengchang,
    count(case when b.fissue_name='后摄像头外观' and b.fanswer_name like "%正常%" then a.fserial_number else null end) as fhoushe_zhengchang,
    count(case when b.fissue_name='外壳掉漆' and b.fanswer_name="无" then a.fserial_number else null end) as fdiaoqi_wu,
    count(case when b.fissue_name='显示老化/色差' and b.fanswer_name="无" then a.fserial_number else null end) as fsecha_wu,
    count(case when b.fissue_name='外壳弯曲变形' and b.fanswer_name="无" then a.fserial_number else null end) as fwanqu_wu,
    count(case when b.fissue_name='屏幕外观划痕' and b.fanswer_name="轻微" then a.fserial_number else null end) as fwaiguan_qingwei,
    count(case when b.fissue_name='外壳划痕' and b.fanswer_name="轻微" then a.fserial_number else null end) as fwaikeqingwei,
    count(case when b.fissue_name='外壳磕碰' and b.fanswer_name="轻微" then a.fserial_number else null end) as fkepeng_qingwei,
    count(case when b.fissue_name='外壳印渍' and b.fanswer_name="轻微" then a.fserial_number else null end) as fyinzi_qingwei,
    count(case when b.fissue_name='外壳掉漆' and b.fanswer_name="轻微" then a.fserial_number else null end) as fdiaoqi_qingwei,
    count(case when b.fissue_name='显示老化/色差' and b.fanswer_name="轻微" then a.fserial_number else null end) as fsecha_qingwei,
    count(case when b.fissue_name='外壳弯曲变形' and b.fanswer_name="轻微" then a.fserial_number else null end) as fwanqu_qingwei,
    count(case when b.fissue_name='屏幕外观划痕' and b.fanswer_name="明显" then a.fserial_number else null end) as fwaiguan_mingxian,
    count(case when b.fissue_name='外壳划痕' and b.fanswer_name="明显" then a.fserial_number else null end) as fwaikemingxian,
    count(case when b.fissue_name='外壳磕碰' and b.fanswer_name="明显" then a.fserial_number else null end) as fkepeng_mingxian,
    count(case when b.fissue_name='外壳印渍' and b.fanswer_name="明显" then a.fserial_number else null end) as fyinzi_mingxian,
    count(case when b.fissue_name='前摄像头外观' and b.fanswer_name like "%明显偏移%" then a.fserial_number else null end) as fqianshe_mingxianpianyi,
    count(case when b.fissue_name='前摄像头外观' and b.fanswer_name like "%明显有灰%" then a.fserial_number else null end) as fqianshe_mingxianyouhui,
    count(case when b.fissue_name='后摄像头外观' and b.fanswer_name like "%明显偏移%" then a.fserial_number else null end) as fhoushe_mingxianpianyi,
    count(case when b.fissue_name='后摄像头外观' and b.fanswer_name like "%明显有灰%" then a.fserial_number else null end) as fhoushe_mingxianyouhui,
    count(case when b.fissue_name='外壳掉漆' and b.fanswer_name="明显" then a.fserial_number else null end) as fdiaoqi_mingxian,
    count(case when b.fissue_name='显示老化/色差' and b.fanswer_name="明显" then a.fserial_number else null end) as fsecha_mingxian,
    count(case when b.fissue_name='外壳弯曲变形' and b.fanswer_name="明显" then a.fserial_number else null end) as fwanqu_mingxian,
    count(case when b.fissue_name='屏幕外观划痕' and b.fanswer_name="严重" then a.fserial_number else null end) as fwaiguan_yanzhong,
    count(case when b.fissue_name='外壳划痕' and b.fanswer_name="严重" then a.fserial_number else null end) as fwaikeyanzhong,
    count(case when b.fissue_name='外壳磕碰' and b.fanswer_name="严重" then a.fserial_number else null end) as fkepeng_yanzhong,
    count(case when b.fissue_name='外壳印渍' and b.fanswer_name="严重" then a.fserial_number else null end) as fyinzi_yanzhong,
    count(case when b.fissue_name='前摄像头外观' and b.fanswer_name like "%脏污%" then a.fserial_number else null end) as fqianshe_zangwu,
    count(case when b.fissue_name='后摄像头外观' and b.fanswer_name like "%脏污%" then a.fserial_number else null end) as fhoushe_zangwu,
    count(case when b.fissue_name='外壳掉漆' and b.fanswer_name="严重" then a.fserial_number else null end) as fdiaoqi_yanzhong,
    count(case when b.fissue_name='显示老化/色差' and b.fanswer_name="无法质检" then a.fserial_number else null end) as fsecha_wufazhijian,
    count(case when b.fissue_name='外壳弯曲变形' and b.fanswer_name="无法质检" then a.fserial_number else null end) as fwanqu_wufazhijian,
    count(case when b.fissue_name='外壳掉漆' and b.fanswer_name="无法质检" then a.fserial_number else null end) as fdiaoqi_wufazhijian,
    count(case when b.fissue_name='拆修痕迹' and b.fanswer_name="无任何维修" then a.fserial_number else null end) as fchaixiu_wurenheweixiu,
    count(case when b.fissue_name='拆修痕迹' and b.fanswer_name="无法拆机" then a.fserial_number else null end) as fchaixiu_wufachaiji,
    count(case when b.fissue_name='拆修痕迹' and b.fanswer_name="无第三方维修" then a.fserial_number else null end) as fchaixiu_wudisanfangweixiu,
    count(case when b.fissue_name='拆修痕迹' and b.fanswer_name="有拆机痕迹" then a.fserial_number else null end) as fchaixiu_youchaixiu,
    count(case when b.fissue_name='拆修痕迹' and b.fanswer_name="有维修痕迹" then a.fserial_number else null end) as fchaixiu_youweixiu
from (select 
            *,
            row_number() over(partition by fserial_number order by fend_time desc) as num
        from drt.drt_my33310_detection_t_detect_record 
        where fdet_type=0
        and fis_deleted=0
    	and fverdict<>"测试单"
        and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),90))
        and left(fserial_number,2) in ('01','02')) as a
left join dwd.dwd_detect_back_detection_issue_and_answer_v2 as b on a.frecord_id=b.fdetect_record_id
where left(a.fserial_number,2) in ('01','02')
and a.num=1
and b.field_source='fdet_norm_snapshot'
and b.ds>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
group by 1,2
),
detect_product as (
-- CTE[detect_product]：为每个序列号取最近一次检测的商品名、结束时间与等级
select 
    fserial_number,
    fproduct_name,
    fend_time,
    get_json_object(fgoods_level,'$.levelName') as levelname
from (
    select 
            *,
            row_number() over(partition by fserial_number order by fend_time desc) as num
        from drt.drt_my33310_detection_t_detect_record 
        where fdet_type=0
        and fis_deleted=0
    	and fverdict<>"测试单"
        and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),366))
        and left(fserial_number,2) in ('01','02')
)t where num=1
),
t_instock_inout as (                     -- 巨沃入库/销售出库时间节点
-- CTE[t_instock_inout]：从 WMS 通知表抽取每个序列号的首次入库时间与最近出库时间
    select
        upper(fserial_no) as fseries_number,
        min(case when fcmd = 'CGRK' then fchange_time else null end) as fstock_in_time,
        max(case when fcmd='JYCK' then fchange_time else null end) as fsale_out_time
    from drt.drt_my33312_hsb_sales_product_t_pm_wms_stock_notify
    where to_date(fchange_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),366))
    and left(fserial_no,2) in ('01','02')
    group by fserial_no
),
detect_one as (
-- CTE[detect_one]：模块一检测（自动化记录）最近一次的人员与完成时间，含个别日期的姓名纠偏
select 
    fserial_number,
    case when to_date(from_unixtime(fend_det_time))='2024-03-04' and freal_name="陈冬凡" then "周远鸿" 
         when to_date(from_unixtime(fend_det_time))='2024-03-04' and freal_name="胡家华" then "黄成水"
         when to_date(from_unixtime(fend_det_time))='2024-03-02' and freal_name="陈冬凡" then "李浩宇"
         when to_date(from_unixtime(fend_det_time))='2024-03-05' and freal_name="陈冬凡" then "周远鸿"
    else freal_name end as fdetect_one_name,
    from_unixtime(fend_det_time) as fdetect_one_time
from (
select 
    a.fserial_number,
    a.fend_det_time,
    case when a.fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" else b.freal_name end as freal_name,
    row_number()over(partition by upper(a.fserial_number) order by a.fend_det_time desc) as num
from drt.drt_my33312_detection_t_automation_det_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
where fserial_number!=""
and fserial_number is not null
and to_date(from_unixtime(a.fend_det_time))>=to_date(date_sub(from_unixtime(unix_timestamp()),365)))t
where num=1
),
detect_two as (
-- CTE[detect_two]：模块二检测（App 记录）最近一次的人员与时间，含个别日期的姓名纠偏
select 
    fserial_number,
    case when fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" 
         when to_date(fcreate_time)='2024-03-04' and freal_name="陈冬凡" then "周远鸿" 
         when to_date(fcreate_time)='2024-03-04' and freal_name="胡家华" then "黄成水"
    else freal_name end as fdetect_two_name,
    fcreate_time as fdetect_two_time
from (
select 
    a.fcreate_time,
    a.fuser_name,
    a.fserial_number,
    b.freal_name,
    row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
from drt.drt_my33312_detection_t_det_app_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and fserial_number!=""
and fserial_number is not null)t
where num=1
),
detect_three as (             -- 模块三检测对应人员
-- CTE[detect_three]：模块三（外观相关）任务最近一次处理人与完成时间
select 
    fserial_number,
    freal_name,
    fend_time as fdetect_three_time
from (
    select 
        a.fserial_number,
        b.freal_name,
        a.fcreate_time,
        b.fend_time,
        row_number()over(partition by a.fserial_number order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
    and b.fdet_sop_task_name like "%外观%")t
where num=1
),
detect_four as (
-- CTE[detect_four]：模块四（拆修相关）任务最近一次处理人与完成时间
select 
    fserial_number,
    freal_name as fdetect_four_name,
    fend_time as fdetect_four_time
from (
    select 
        a.fcreate_time,
        a.fserial_number,
        b.freal_name,
        a.fend_time,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
    and b.fdet_sop_task_name like "%拆修%")t
where num=1
),
wenmi as (                      -- 问密工单
-- CTE[wenmi]：每个条码最近一张有效问密工单（排除“无效工单”），保留创建/完结时间与处理人
    select 
    *
from(
    select 
    case when a.fwork_status=40 then from_unixtime(a.Fupdate_time) else null end as Fupdate_time,
    from_unixtime(a.Fadd_time) as Fadd_time,
    a.fbarcode_sn,
    a.fupdate_user,
    row_number() over(partition by a.fbarcode_sn order by Fadd_time desc) as num
from drt.drt_my33310_csrdb_t_works as a
where a.fwork_type=4
and a.fwork_source<>3
and a.fappeal_type1<>0
and a.fduty_content not like "%无效工单%"
and left(a.fbarcode_sn,2) in ('01','02'))t where num=1
and t.Fadd_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
)
SELECT 
    a.forder_id,
	a.fxy_order_id,
	a.forder_time,
	a.forder_status,
	a.fxy_channel,
	a.fsub_channel,
	CASE 
		WHEN a.fhsb_product_name IS NULL OR a.fhsb_product_name = "" 
		THEN c.fproduct_name 
		ELSE a.fhsb_product_name 
	END as fhsb_product_name,
	a.fend_status,
	a.fhost_barcode,
	a.fparts_barcode,
	a.flogistics_number,
	a.forder_type,
	a.fsigh_time,
	a.fsigh_user,
	a.freceiver,
	a.freceiver_address,
	a.freceive_time,
	CASE 
		WHEN c.fend_time > a.fdetect_put_time OR c.fend_time IS NULL 
		THEN a.fdetect_time 
		ELSE c.fend_time 
	END as fdetect_time,
	CASE 
		WHEN i.freal_name IS NOT NULL 
		THEN i.freal_name 
		ELSE a.fengineer_real_name 
	END as fengineer_real_name,
	a.fphoto_name,
	a.fphoto_time,
	a.fshooting_time,
	a.fshooting_user_name,
	a.fput_time,
	a.fput_user,
	a.fsale_put_time,
	a.frefund_put_time,
	a.fdata_update_time,
	a.fis_disassembly,
	a.fzhibao_create_time,
	a.fzhibao_update_time,
	a.fzhibao_status,
	a.fcomplain_add_time,
	a.fcomplain_duty,
	a.fcomplain_update_time,
	a.fcomplain_work_status,
	a.fsection_id,
	a.fseller_province_name,
	a.fseller_city_name,
	a.fbuyer_province_name,
	a.fbuyer_city_name,
    a.frequest_put_time,
    a.fdetect_put_time,
    b.*,
    c.levelname,
    d.fbuyer_pay_fee/100 as fbuyer_pay_fee,
    d.frecycle_refrence_price/100 as frecycle_refrence_price, 
    d.fisv_recycle,
    d.fsource,
    e.famount/100 as fpay_out_price,
    e.fresult,
    f.fstock_in_time,
    f.fsale_out_time,
    CASE 
    	WHEN f.fstock_in_time IS NOT NULL AND f.fsale_out_time IS NOT NULL 
    	THEN (unix_timestamp(f.fsale_out_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(f.fstock_in_time,'yyyy-MM-dd HH:mm:ss'))/3600/24
    	ELSE NULL 
    END as fsale_time,
    g.fseller_pay_fee/100 as fseller_paying_fee,
    g.fbuyer_pay_fee/100 as fbuyer_paying_fee,
    g.Fbuyer_pay_time,
    g.Fseller_pay_time,
    h.Fscence_desc,
    h.Fhave_fee,
    h.Fpay_role,
    j.Fadd_time as fwenmi_create_time,
    j.Fupdate_time as fwenmi_finish_time,
    j.fupdate_user as fwenmi_user,
    i.fdetect_three_time,
    k.fdetect_one_time,
    k.fdetect_one_name,
    l.fdetect_two_time,
    l.fdetect_two_name,
    m.fdetect_four_time,
    m.fdetect_four_name
from dws.dws_xy_yhb_detail as a
left join detect as b ON a.fhost_barcode=b.fserial_number
left join detect_product as c on a.fhost_barcode=c.fserial_number
left join drt.drt_my33315_xy_detect_t_xy_yhb_recycle_order as d on a.fxy_order_id=d.fxy_order_id
left join drt.drt_my33315_xy_detect_t_xy_yhb_recycle_pay_record as e on a.fxy_order_id=e.fxy_order_id
left join t_instock_inout as f on a.fhost_barcode=f.fseries_number
left join drt.drt_my33315_xy_detect_t_xy_yhb3_detect_fee as g on a.fxy_order_id=g.fxy_order_id
left join drt.drt_my33315_xy_detect_t_xy_yhb3_detect_fee_scence as h on g.Fscence_id=h.fid
left join detect_three as i on a.fhost_barcode=i.fserial_number
left join wenmi as j on a.fhost_barcode=j.fbarcode_sn
left join detect_one as k on a.fhost_barcode=k.fserial_number
left join detect_two as l on a.fhost_barcode=l.fserial_number
left join detect_four as m on a.fhost_barcode=m.fserial_number
where forder_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))


