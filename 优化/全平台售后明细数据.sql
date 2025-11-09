with config as (
    select 
        365 as DAYS_SALE_ORDER,      -- 销售订单时间范围（天数）
        400 as DAYS_DETECT_ORDER,    -- 检测订单时间范围（天数）
        400 as DAYS_DETECT_RECORD,   -- 检测记录时间范围（天数）
        -- 直接计算好日期，后面直接用
        to_date(date_sub(from_unixtime(unix_timestamp()), 365)) as date_sale_order,      -- 销售订单起始日期
        to_date(date_sub(from_unixtime(unix_timestamp()), 400)) as date_detect_order,    -- 检测订单起始日期
        to_date(date_sub(from_unixtime(unix_timestamp()), 400)) as date_detect_record    -- 检测记录起始日期
),
detect as (       --取最新检测明细数据，支持所有平台（竞拍、采货侠、B2B）
    select
        *
    from (
        select
            a.fcreate_time,
            upper(a.fserial_number) as fserial_number,
            a.Fdet_tpl,
            a.Freal_name,
            a.Fend_time,
            a.fbrand_name,
            a.Fdetection_object,
            a.fgoods_level,
            a.fwarehouse_code,
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time asc) as num
        from drt.drt_my33310_detection_t_detect_record as a
        left join (select
                        fseries_number,
                        forder_create_time
                   from (
                        select
                            fseries_number,
                            forder_create_time,
                        row_number() over(partition by fseries_number order by  forder_create_time desc) as num
                    from dws.dws_jp_order_detail
                    where ftest_show <> 1
                    and forder_status in (2,3,4,6)
                    and forder_create_time>=(select date_detect_order from config)
                    ) t where t.num=1) as b on upper(a.fserial_number)=b.fseries_number
        left join (
                    select
                        freal_name,
                        Fposition_id
                    from (select
                                *,
                                row_number() over(partition by freal_name order by fcreate_time desc) as num
                          from drt.drt_my33310_amcdb_t_user
                          )t
                    where num=1) as c on a.freal_name=c.freal_name
        where a.fis_deleted=0
        and to_date(a.fend_time)>=(select date_detect_order from config)
        and a.fend_time<b.forder_create_time
        and c.Fposition_id <>129            --剔除入库组缺陷拍照的人员
        --and fdetection_object<>3
            ) c
    where c.num=1
),
after_sale_detect as (
select
        *
    from (
        select
            *,
            row_number() over(partition by fserial_number order by fend_time asc) as num
        from drt.drt_my33310_detection_t_detect_record
        where fdet_type=0
        and fis_deleted=0
        and freport_type=0
        and fverdict<>"测试单"
        and to_date(fend_time)>=(select date_detect_order from config)
        and left(fserial_number,2) in ('YZ','NT','JM'))t
    where num=1
),

all_sale as(  --最新销售记录（支持所有平台：竞拍、采货侠、B2B）
    select
        *
    from (
        select
            *,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and to_date(forder_create_time)>=(select date_sale_order from config)
        and forder_status in (2,3,4,6)) t where num=1
),
first_sale as (  --第一次销售（支持所有平台）
        select
        *
    from (
        select
            *,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and to_date(forder_create_time)>=(select date_sale_order from config)
        and forder_status in (2,3,4,6)) t where num=1
),
jp_second_sale as (   --二次销售
        select
        *
    from (
        select
            b.fold_fseries_number as fold_fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            a.fstart_time
        from dws.dws_jp_order_detail as a
        left join dws.dws_hs_order_detail as b on a.fseries_number=b.fseries_number
        where ftest_show <> 1
        and to_date(a.forder_create_time)>=(select date_sale_order from config)
        and a.fchannel_name='竞拍销售默认渠道号'
        and a.forder_status in (2,3,4,6)
        union all
        select
            b.fold_fseries_number as fold_fseries_number,
            a.foffer_price,
            null as Fcity_name,
            null as Forder_address,
            null as Freceiver_id,
            null as Freceiver_name,
            null as Freceiver_phone,
            a.foffer_time as fstart_time
        from dws.dws_th_order_detail as a
        left join dws.dws_hs_order_detail as b on a.fseries_number=b.fseries_number
        where a.Fbd_status <>2
        and a.fchannel_name='竞拍销售默认渠道号') t
),
after_sale as (
    select
        *
    from (
        select
            a.*,
            b.fseries_number,
            row_number() over(partition by fsales_series_number order by a.fauto_create_time desc ) as num
        from drt.drt_my33310_recycle_t_after_sales_order_info as a
        left join drt.drt_my33310_recycle_t_order as b on a.fafter_sales_order_id=b.forder_id
        where a.fvalid=1
        --and Faftersales_owner<>3
        ) t where num=1
),
caihuoxia_after_sale as (
select
    *
from (
    select
        * ,
        row_number() over(partition by fbusiness_id order by fcreate_time desc)as num
    from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales
)t where num=1
),
caihuoxia_second_sale as (
select
    fstart_time,
    fseries_number,
    foffer_price/100 as foffer_price,
    Fcity_name,
    Forder_address,
    Freceiver_id,
    Freceiver_name,
    Freceiver_phone
from (
        select
            a.fstart_time,
            if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail as a
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no
        where ftest_show <> 1
        and to_date(a.forder_create_time)>=(select date_sale_order from config)
        and forder_platform=6
        and fmerchant_jp=0
        and forder_status in (2,3,4,6)) t where num=2
union all
select
    fstart_time,
    fseries_number,
    foffer_price/100 as foffer_price,
    Fcity_name,
    Forder_address,
    Freceiver_id,
    Freceiver_name,
    Freceiver_phone
from (
        select
            a.fstart_time,
            if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail as a
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no
        where ftest_show <> 1
        and forder_platform<>6
        and to_date(a.forder_create_time)>=(select date_sale_order from config)
        and fmerchant_jp=0
        and forder_status in (2,3,4,6)) t where num=1
union all
select
    fstart_time,
    fseries_number,
    foffer_price/100 as foffer_price,
    null as Fcity_name,
    null as Forder_address,
    null as Freceiver_id,
    null as Freceiver_name,
    null as Freceiver_phone
from (
    select
        a.fstart_time,
        if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number,
        a.foffer_price,
        row_number() over(partition by fseries_number order by forder_create_time asc) as num
    from dws.dws_th_order_detail as a
    left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no
    where Fbd_status <>2
    )t where num=1
),
detect_one as (
select
    upper(fserial_number) as fserial_number,
    freal_name as fdetect_one_name,
    from_unixtime(fend_det_time) as fdetect_one_time
from (
select
    a.fserial_number,
    a.fend_det_time,
    b.freal_name,
    row_number()over(partition by upper(a.fserial_number) order by a.fend_det_time desc) as num
from drt.drt_my33312_detection_t_automation_det_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
where fserial_number!=""
and fserial_number is not null
and to_date(from_unixtime(a.fend_det_time))>=(select date_detect_record from config))t
where num=1
),
detect_two as (
select
    upper(fserial_number) as fserial_number,
    freal_name as fdetect_two_name,
    fcreate_time as fdetect_two_time
from (
select
    a.fcreate_time,
    a.fserial_number,
    b.freal_name,
    row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
from drt.drt_my33312_detection_t_det_app_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
where to_date(a.fcreate_time)>=(select date_detect_record from config)
and fserial_number!=""
and fserial_number is not null)t
where num=1
),
detect_three as (
select
    upper(fserial_number) as fserial_number,
    freal_name as fdetect_three_name,
    fcreate_time as fdetect_three_time
from (
    select
        a.fcreate_time,
        a.fserial_number,
        b.freal_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    where to_date(a.fend_time)>=(select date_detect_record from config)
    and b.fdet_sop_task_name like "%外观%")t
where num=1
),
detect_three_pingmu as (
select
    upper(fserial_number) as fserial_number,
    case when freal_name="李俊峰" then "李俊锋" else freal_name end as fdetect_three_name_pingmu,
    fcreate_time as fdetect_three_time_pingmu
from (
    select
        a.fcreate_time,
        a.fserial_number,
        b.freal_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    where to_date(a.fend_time)>=(select date_detect_record from config)
    and b.fdet_sop_task_name like "%屏幕%"
    and b.fdet_sop_task_name!="外观屏幕")t
where num=1
),

b2b_after_sale as (
    select
        *
    from (
        select
            *,
            row_number() over(partition by fbusiness_id order by fcreate_time desc) as num
        from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales
    ) t where num=1
),

b2b_second_sale as (
select
    fstart_time,
    fseries_number,
    foffer_price/100 as foffer_price,
    Fcity_name,
    Forder_address,
    Freceiver_id,
    Freceiver_name,
    Freceiver_phone
from (
        select
            a.fstart_time,
            if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail as a
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no
        where ftest_show <> 1
        and to_date(a.forder_create_time)>=(select date_sale_order from config)
        and forder_platform=5
        and forder_status in (2,3,4,6)) t where num=2
union all
select
    fstart_time,
    fseries_number,
    foffer_price/100 as foffer_price,
    Fcity_name,
    Forder_address,
    Freceiver_id,
    Freceiver_name,
    Freceiver_phone
from (
        select
            a.fstart_time,
            if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail as a
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no
        where ftest_show <> 1
        and to_date(a.forder_create_time)>=(select date_sale_order from config)
        and forder_platform<>5
        and forder_status in (2,3,4,6)) t where num=1
union all
select
    fstart_time,
    fseries_number,
    foffer_price/100 as foffer_price,
    null as Fcity_name,
    null as Forder_address,
    null as Freceiver_id,
    null as Freceiver_name,
    null as Freceiver_phone
from (
    select
        a.fstart_time,
        if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number,
        a.foffer_price,
        row_number() over(partition by fseries_number order by forder_create_time asc) as num
    from dws.dws_th_order_detail as a
    left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no
    where Fbd_status <>2
    )t where num=1
),

detect_four as (
select
    upper(fserial_number) as fserial_number,
    freal_name as fdetect_four_name,
    fcreate_time as fdetect_four_time
from (
    select
        a.fcreate_time,
        a.fserial_number,
        b.freal_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    where to_date(a.fend_time)>=(select date_detect_record from config)
    and b.fdet_sop_task_name like "%拆修%")t
where num=1
)

select
    a.fstart_time,
    a.fseries_number,
    a.fclass_name,
    case when b.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
    a.fchannel_name,
    a.fproduct_name,
    a.fproject_name,
    a.Fcity_name,
    a.Forder_address,
    a.Freceiver_id,
    a.Freceiver_name,
    a.Freceiver_phone,
    f.freal_name as fsecond_detect_name,
    c.fseries_number as fafter_series_number,
    e.Fcity_name as fsecond_sale_city,
    e.Forder_address as fsecond_sale_address,
    e.Freceiver_id as fsecond_sale_id,
    e.Freceiver_name as fsecond_sale_name,
    e.Freceiver_phone as fsecond_sale_phone,
    "自有平台" as "销售渠道",
    left(a.fseries_number,2) as "渠道",
    a.fcost_price/100 as "成本价",
    a.foffer_price/100 as "当前出价",
    if((c.Ftotal_real_refund_amount>0 and a.foffer_price=c.Ftotal_real_refund_amount) or (a.fchannel_name="竞拍销售默认渠道号"),0,a.foffer_price/100) as "销售额",
    b.Fdet_tpl,
    b.Freal_name,
    b.Fend_time,
    b.Fdetection_object,
    case when b.fwarehouse_code='12' then "东莞仓"
         when right(left(a.fseries_number,6),2)="16" or left(a.fseries_number,3)="020" then "杭州仓"
    else "深圳仓" end as fwarehouse_code,
    c.Fauto_create_time,
    get_json_object(b.Fgoods_level,'$.levelName') as Fgoods_level,
    c.Fappeal_reason,
    cast(c.Ffirst_trial_result as string) as Ffirst_trial_result,
    c.Freexamine_result,
    c.Fdetection_price/100 as "检测价",
    c.Freinspection_price/100 as "二次检测价",
    c.Ftotal_diff_amount/100 as "检测差异金额",
    c.Ftotal_refundable_amount/100 as "总应退款金额",
    c.Ftotal_real_refund_amount/100 as "总实退款金额",
    case
        when b.Fdet_tpl=1 then "大检测"
        when (b.Fdet_tpl=0 or b.Fdet_tpl=2 or b.Fdet_tpl=6 or b.Fdet_tpl=7) then "竞拍检测"
    else '其他' end as "检测渠道",
    case
      when b.Fdet_tpl = 0 then '标准检'
      when b.Fdet_tpl = 1 then '大质检'
      when b.Fdet_tpl = 2 then '新标准检测'
      when b.Fdet_tpl = 3 then '产线检'
      when b.Fdet_tpl = 4 then '34项检测'
      when b.Fdet_tpl = 5 then '无忧购'
      when b.Fdet_tpl = 6 then '寄卖plus'
      when b.Fdet_tpl = 7 then '价格3.0的检测'
    else '其他' end as "检测模板",
    case when c.Ftotal_real_refund_amount>0 then 1 else 0 end as "售后数",
    case when c.Ftotal_real_refund_amount>0 then c.Freceived_audit_result_time else null end as "售后通过时间",
    case when c.Ftotal_real_refund_amount>0 and a.foffer_price<c.Ftotal_real_refund_amount and a.fstart_time>='2022-01-01' then 1
         when c.Ftotal_real_refund_amount>0 and a.foffer_price=c.Ftotal_real_refund_amount and a.fstart_time<'2022-01-01' then 1
    else 0 end as "退货数",
    case when c.Ftotal_real_refund_amount>0 and a.foffer_price>c.Ftotal_real_refund_amount then 1 else 0 end as "补差赔付",
    case when c.Ftotal_real_refund_amount>0 and a.foffer_price>c.Ftotal_real_refund_amount then c.Ftotal_real_refund_amount/100 else 0 end as "赔付金额",
    c.Fafter_sales_type,
    case when c.Fafter_sales_type=1 then "仅退款"
         when c.Fafter_sales_type=2 then "退货退款"
    else "其它" end as "售后类型",
    c.Faftersales_owner,
    d.foffer_price/100 as first_price,
    e.fstart_time as fsecond_sale_time,
    e.foffer_price/100 as second_price,
    if(c.Ftotal_real_refund_amount>0 and a.foffer_price=c.Ftotal_real_refund_amount and a.fstart_time>='2022-01-01',0,d.foffer_price/100-e.foffer_price/100) as "二次差价成本",
    if(g.fdetect_two_name is null,b.freal_name,g.fdetect_two_name) as fdetect_two_name,
    if(h.fdetect_three_name is null,b.freal_name,h.fdetect_three_name) as fdetect_three_name,
    j.fdetect_three_name_pingmu,
    if(g.fdetect_two_time is not null,"是","否") as "是否分模块",
    a.fanchor_level
from all_sale as a
left join detect as b on a.fseries_number=b.fserial_number
left join after_sale as c on a.fseries_number=c.Fsales_series_number
left join after_sale_detect as f on c.fseries_number=f.fserial_number
left join first_sale as d on a.fseries_number=d.fseries_number
left join jp_second_sale as e on a.fseries_number=e.fold_fseries_number
left join detect_two as g on a.fseries_number=g.fserial_number
left join detect_three as h on a.fseries_number=h.fserial_number
left join detect_three_pingmu as j on a.fseries_number=j.fserial_number
left join detect_four as i on a.fseries_number=i.fserial_number
where a.fstart_time>=(select date_detect_order from config)
union all
select
    a.fstart_time,
    a.fseries_number,
    a.fclass_name,
    case when b.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
    a.fchannel_name,
    a.fproduct_name,
    a.fproject_name,
    a.Fcity_name,
    a.Forder_address,
    a.Freceiver_id,
    a.Freceiver_name,
    a.Freceiver_phone,
    g.freal_name as fsecond_detect_name,
    c.fnew_serial_no as fafter_series_number,
    e.Fcity_name as fsecond_sale_city,
    e.Forder_address as fsecond_sale_address,
    e.Freceiver_id as fsecond_sale_id,
    e.Freceiver_name as fsecond_sale_name,
    e.Freceiver_phone as fsecond_sale_phone,
    "采货侠" as "销售渠道",
    left(a.fseries_number,2) as "渠道",
    a.fcost_price/100 as "成本价",
    a.foffer_price/100 as "当前出价",
    if(c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1,0,a.foffer_price/100) as "销售额",
    b.Fdet_tpl,
    b.Freal_name,
    b.Fend_time,
    b.Fdetection_object,
    case when b.fwarehouse_code='12' then "东莞仓"
         when right(left(a.fseries_number,6),2)="16" or left(a.fseries_number,3)="020" then "杭州仓"
    else "深圳仓" end as fwarehouse_code,
    c.fapply_time,
    get_json_object(b.Fgoods_level,'$.levelName') as Fgoods_level,
    c.Fjudge_reason as Fappeal_reason,
    c.Fjudge_result as Ffirst_trial_result,
    null as Freexamine_result,
    null as "检测价",
    null as "二次检测价",
    null as "检测差异金额",
    null as "总应退款金额",
    c.Forder_deal_price/100 as "总实退款金额",
    case
        when b.Fdet_tpl=1 then "大检测"
        when (b.Fdet_tpl=0 or b.Fdet_tpl=2 or b.Fdet_tpl=6 or b.Fdet_tpl=7) then "竞拍检测"
    else '其他' end as "检测渠道",
    case
      when b.Fdet_tpl = 0 then '标准检'
      when b.Fdet_tpl = 1 then '大质检'
      when b.Fdet_tpl = 2 then '新标准检测'
      when b.Fdet_tpl = 3 then '产线检'
      when b.Fdet_tpl = 4 then '34项检测'
      when b.Fdet_tpl = 5 then '无忧购'
      when b.Fdet_tpl = 6 then '寄卖plus'
      when b.Fdet_tpl = 7 then '价格3.0的检测'
    else '其他' end as "检测模板",
    case when c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1 then 1
         when f.fsrouce_serial_no is not null then 1 else 0 end as "售后数",
    case when c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1 then Fjudge_time
         when f.fsrouce_serial_no is not null then Fjudge_time else null end as "售后通过时间",
    case when c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1 then 1
         when f.fsrouce_serial_no is not null then 1 else 0 end as "退货数",
    0 as "补差赔付",
    0 as "赔付金额",
    null as Fafter_sales_type,
    case when c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1 then "退货退款"
         when f.fsrouce_serial_no is not null then "退货退款" else null end as "售后类型",
    null as Faftersales_owner,
    d.foffer_price/100 as first_price,
    e.fstart_time as fsecond_sale_time,
    e.foffer_price as second_price,
    if(c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1,d.foffer_price/100-e.foffer_price,if(f.fsrouce_serial_no is not null,d.foffer_price/100-e.foffer_price,0)) as "二次差价成本",
    if(h.fdetect_two_name is null,b.freal_name,h.fdetect_two_name) as fdetect_two_name,
    if(i.fdetect_three_name is null,b.freal_name,i.fdetect_three_name) as fdetect_three_name,
    k.fdetect_three_name_pingmu,
    if(h.fdetect_two_time is not null,"是","否") as "是否分模块",
    a.fanchor_level
from all_sale as a
left join detect as b on a.fseries_number=b.fserial_number
left join caihuoxia_after_sale as c on a.fseries_number=c.fbusiness_id
left join after_sale_detect as g on c.fnew_serial_no=g.fserial_number
left join first_sale as d on a.fseries_number=d.fseries_number
left join caihuoxia_second_sale as e on a.fseries_number=e.fseries_number
left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as f on a.fseries_number=upper(f.fsrouce_serial_no)
left join detect_two as h on a.fseries_number=h.fserial_number
left join detect_three as i on a.fseries_number=i.fserial_number
left join detect_three_pingmu as k on a.fseries_number=k.fserial_number
left join detect_four as j on a.fseries_number=j.fserial_number
where a.fstart_time>=(select date_detect_order from config)
union all
-- B2B 分支（平台=5，近365天，不拼历史表）
select
    a.fstart_time,
    a.fseries_number,
    a.fclass_name,
    case when b.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
    a.fchannel_name,
    a.fproduct_name,
    a.fproject_name,
    a.Fcity_name,
    a.Forder_address,
    a.Freceiver_id,
    a.Freceiver_name,
    a.Freceiver_phone,
    g.freal_name as fsecond_detect_name,
    cc.fnew_serial_no as fafter_series_number,
    e.Fcity_name as fsecond_sale_city,
    e.Forder_address as fsecond_sale_address,
    e.Freceiver_id as fsecond_sale_id,
    e.Freceiver_name as fsecond_sale_name,
    e.Freceiver_phone as fsecond_sale_phone,
    "鱼市B2B" as "销售渠道",
    left(a.fseries_number,2) as "渠道",
    a.fcost_price/100 as "成本价",
    a.foffer_price/100 as "当前出价",
    if(cc.frefund_total>0,0,a.foffer_price/100) as "销售额",
    b.Fdet_tpl,
    b.Freal_name,
    b.Fend_time,
    b.Fdetection_object,
    case when b.fwarehouse_code='12' then "东莞仓"
         when right(left(a.fseries_number,6),2)="16" or left(a.fseries_number,3)="020" then "杭州仓"
    else "深圳仓" end as fwarehouse_code,
    cc.fapply_time as Fauto_create_time,
    get_json_object(b.Fgoods_level,'$.levelName') as Fgoods_level,
    cc.fjudge_reason as Fappeal_reason,
    cc.fjudge_result as Ffirst_trial_result,
    0 as Freexamine_result,
    0 as "检测价",
    0 as "二次检测价",
    0 as "检测差异金额",
    0 as "总应退款金额",
    cc.frefund_total/100 as "总实退款金额",
    case
        when b.Fdet_tpl=1 then "大检测"
        when (b.Fdet_tpl=0 or b.Fdet_tpl=2 or b.Fdet_tpl=6 or b.Fdet_tpl=7) then "竞拍检测"
    else '其他' end as "检测渠道",
    case
      when b.Fdet_tpl = 0 then '标准检'
      when b.Fdet_tpl = 1 then '大质检'
      when b.Fdet_tpl = 2 then '新标准检测'
      when b.Fdet_tpl = 3 then '产线检'
      when b.Fdet_tpl = 4 then '34项检测'
      when b.Fdet_tpl = 5 then '无忧购'
      when b.Fdet_tpl = 6 then '寄卖plus'
      when b.Fdet_tpl = 7 then '价格3.0的检测'
    else '其他' end as "检测模板",
    case when cc.fapply_time is not null and cc.fapply_time !='0000-00-00 00:00:00.0' and cc.Fjudge_type=1 then 1
         when f.fsrouce_serial_no is not null then 1 else 0 end as "售后数",
    case when cc.fapply_time is not null and cc.fapply_time !='0000-00-00 00:00:00.0' and cc.Fjudge_type=1 then cc.Fjudge_time
         when f.fsrouce_serial_no is not null then cc.Fjudge_time else null end as "售后通过时间",
    case when cc.fapply_time is not null and cc.fapply_time !='0000-00-00 00:00:00.0' and cc.Fjudge_type=1 then 1
         when f.fsrouce_serial_no is not null then 1 else 0 end as "退货数",
    case when cc.frefund_total>0 and a.foffer_price>cc.frefund_total then 1 else 0 end as "补差赔付",
    case when cc.frefund_total>0 and a.foffer_price>cc.frefund_total then cc.frefund_total/100 else 0 end as "赔付金额",
    cc.faftersales_type as Fafter_sales_type,
    case when cc.fapply_time is not null and cc.fapply_time !='0000-00-00 00:00:00.0' and cc.Fjudge_type=1 then "退货退款"
         when f.fsrouce_serial_no is not null then "退货退款" else null end as "售后类型",
    null as Faftersales_owner,
    d.foffer_price/100 as first_price,
    e.fstart_time as fsecond_sale_time,
    e.foffer_price as second_price,
    if(cc.fapply_time is not null and cc.fapply_time !='0000-00-00 00:00:00.0' and cc.Fjudge_type=1,d.foffer_price/100-e.foffer_price,if(f.fsrouce_serial_no is not null,d.foffer_price/100-e.foffer_price,0)) as "二次差价成本",
    if(h.fdetect_two_name is null,b.freal_name,h.fdetect_two_name) as fdetect_two_name,
    if(i.fdetect_three_name is null,b.freal_name,i.fdetect_three_name) as fdetect_three_name,
    k.fdetect_three_name_pingmu,
    if(h.fdetect_two_time is not null,"是","否") as "是否分模块",
    a.fanchor_level
from all_sale as a
left join detect as b on a.fseries_number=b.fserial_number
left join b2b_after_sale as cc on a.fseries_number=cc.fbusiness_id
left join after_sale_detect as g on upper(coalesce(cc.fnew_serial_no,a.fseries_number))=upper(g.fserial_number)
left join detect_two as h on a.fseries_number=h.fserial_number
left join detect_three as i on a.fseries_number=i.fserial_number
left join detect_three_pingmu as k on a.fseries_number=k.fserial_number
left join first_sale as d on a.fseries_number=d.fseries_number
left join b2b_second_sale as e on a.fseries_number=e.fseries_number
left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as f on a.fseries_number=upper(f.fsrouce_serial_no)
where a.fstart_time>=(select date_sale_order from config)


