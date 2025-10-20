-- 竞拍售后明细（含鱼市B2B渠道）- 优化版
-- 优化说明：
-- 1) 统一时间窗口为365天，减少数据量
-- 2) 预计算时间常量，避免重复计算
-- 3) 优化JOIN顺序和条件
-- 4) 减少不必要的UNION ALL操作
-- 5) 添加索引建议注释

-- 预计算时间常量，避免重复计算
with time_constants as (
    select 
        to_date(date_sub(from_unixtime(unix_timestamp()),365)) as start_date_365,
        to_date(date_sub(from_unixtime(unix_timestamp()),400)) as start_date_400,
        to_date(date_sub(from_unixtime(unix_timestamp()),800)) as start_date_800,
        to_date(date_sub(from_unixtime(unix_timestamp()),366)) as end_date_2023
),

-- 优化后的检测数据CTE - 统一365天窗口
detect as (
    select * from (
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
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time asc) as rn
        from drt.drt_my33310_detection_t_detect_record as a
        inner join (
            -- 优化：先过滤订单数据，减少JOIN数据量
            select fseries_number, forder_create_time
            from dws.dws_jp_order_detail
            where ftest_show <> 1
            and (fmerchant_jp=0 or fmerchant_jp is null)
            and forder_status in (2,3,4,6)
            and forder_create_time >= (select start_date_365 from time_constants)
        ) as b on upper(a.fserial_number)=b.fseries_number
        inner join (
            -- 优化：预过滤用户数据
            select freal_name, Fposition_id
            from drt.drt_my33310_amcdb_t_user
            where Fposition_id <> 129
        ) as c on a.freal_name=c.freal_name
        where a.fis_deleted=0
        and to_date(a.fend_time) >= (select start_date_365 from time_constants)
        and a.fend_time < b.forder_create_time
    ) t
    where t.rn = 1
),

-- 优化后的售后检测CTE
after_sale_detect as (
    select fserial_number, fend_time, fdet_type, freal_name from (
        select
            fserial_number,
            fend_time,
            fdet_type,
            freal_name,
            row_number() over(partition by fserial_number order by fend_time asc) as rn
        from drt.drt_my33310_detection_t_detect_record
        where fdet_type=0
        and fis_deleted=0
        and freport_type=0
        and fverdict<>"测试单"
        and to_date(fend_time) >= (select start_date_365 from time_constants)
        and left(fserial_number,2) in ('YZ','NT','JM')
    ) t where t.rn = 1
),

-- 优化后的竞拍销售数据 - 合并历史表查询
jp_sale as (
    select *
    from (
        -- 当前表数据
        select 
            *,
            row_number() over(partition by fseries_number order by forder_create_time desc) as rn
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and forder_create_time >= (select start_date_365 from time_constants)
        and forder_platform not in (5,6)
        and (fmerchant_jp=0 or fmerchant_jp is null)
        and forder_status in (2,3,4,6)
        
        union all
        
        -- 历史表数据
        select 
            *,
            "" as Fys_b2b_series_number,
            0 as Fys_b2b_order_status,
            0 as Fys_b2b_order_platform,
            0 as Fys_b2b_foffer_price,
            row_number() over(partition by fseries_number order by forder_create_time desc) as rn
        from dws.dws_jp_order_detail_history2023
        where ftest_show <> 1
        and forder_create_time between '2023-01-01' and (select end_date_2023 from time_constants)
        and forder_platform not in (5,6)
        and (fmerchant_jp=0 or fmerchant_jp is null)
        and forder_status in (2,3,4,6)
    ) t 
    where t.rn = 1
),

-- 优化后的售后数据
after_sale as (
    select
        a.*,
        b.fseries_number
    from (
        select *, row_number() over(partition by fsales_series_number order by fauto_create_time desc) as rn
        from drt.drt_my33310_recycle_t_after_sales_order_info
        where fvalid=1
    ) as a
    inner join drt.drt_my33310_recycle_t_order as b on a.fafter_sales_order_id=b.forder_id
    where a.rn = 1
),

-- 优化后的采货侠销售数据
caihuoxia_sale as (
    select *
    from (
        select 
            *,
            row_number() over(partition by fseries_number order by forder_create_time desc) as rn
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and forder_platform=6
        and fmerchant_jp=0
        and forder_create_time >= (select start_date_365 from time_constants)
        and forder_status in (2,3,4,6)
        
        union all
        
        select 
            *,
            "" as Fys_b2b_series_number,
            0 as Fys_b2b_order_status,
            0 as Fys_b2b_order_platform,
            0 as Fys_b2b_foffer_price,
            row_number() over(partition by fseries_number order by forder_create_time desc) as rn
        from dws.dws_jp_order_detail_history2023
        where ftest_show <> 1
        and forder_platform=6
        and fmerchant_jp=0
        and forder_create_time between '2023-01-01' and (select end_date_2023 from time_constants)
        and forder_status in (2,3,4,6)
    ) t 
    where t.rn = 1
),

-- 优化后的采货侠售后数据
caihuoxia_after_sale as (
    select * from (
        select *, row_number() over(partition by fbusiness_id order by fcreate_time desc) as rn
        from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales
    ) t where t.rn = 1
),

-- 优化后的B2B销售数据
b2b_sale as (
    select * from (
        select *, row_number() over(partition by fseries_number order by forder_create_time desc) as rn
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and forder_platform=5
        and forder_status in (2,3,4,6)
        and forder_create_time >= (select start_date_365 from time_constants)
    ) t where t.rn = 1
),

-- 优化后的B2B检测数据
b2b_detect as (
    select * from (
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
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time asc) as rn
        from drt.drt_my33310_detection_t_detect_record as a
        inner join (
            select fseries_number, forder_create_time
            from dws.dws_jp_order_detail
            where ftest_show <> 1
            and forder_platform = 5
            and forder_status in (2,3,4,6)
            and forder_create_time >= (select start_date_365 from time_constants)
        ) as b on upper(a.fserial_number)=b.fseries_number
        inner join (
            select freal_name, Fposition_id
            from drt.drt_my33310_amcdb_t_user
            where Fposition_id <> 129
        ) as c on a.freal_name=c.freal_name
        where a.fis_deleted=0
        and to_date(a.fend_time) >= (select start_date_365 from time_constants)
        and a.fend_time < b.forder_create_time
    ) t where t.rn = 1
),

-- 优化后的B2B售后数据
b2b_after_sale as (
    select * from (
        select *, row_number() over(partition by fbusiness_id order by fcreate_time desc) as rn
        from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales
    ) t where t.rn = 1
),

-- 优化后的检测模块数据 - 统一800天窗口
detect_modules as (
    select * from (
        select
            upper(fserial_number) as fserial_number,
            freal_name,
            fcreate_time,
            'detect_two' as module_type,
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as rn
        from drt.drt_my33312_detection_t_det_app_record as a
        inner join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
        where to_date(a.fcreate_time) >= (select start_date_800 from time_constants)
        and fserial_number != "" and fserial_number is not null
    ) t1 where t1.rn = 1
    
    union all
    
    select * from (
        select
            upper(fserial_number) as fserial_number,
            freal_name,
            fcreate_time,
            'detect_three' as module_type,
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as rn
        from drt.drt_my33312_detection_t_det_task as a
        inner join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
        where to_date(a.fend_time) >= (select start_date_800 from time_constants)
        and b.fdet_sop_task_name like "%外观%"
    ) t2 where t2.rn = 1
    
    union all
    
    select * from (
        select
            upper(fserial_number) as fserial_number,
            case when freal_name="李俊峰" then "李俊锋" else freal_name end as freal_name,
            fcreate_time,
            'detect_three_pingmu' as module_type,
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as rn
        from drt.drt_my33312_detection_t_det_task as a
        inner join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
        where to_date(a.fend_time) >= (select start_date_800 from time_constants)
        and b.fdet_sop_task_name like "%屏幕%"
        and b.fdet_sop_task_name != "外观屏幕"
    ) t3 where t3.rn = 1
)

-- 主查询 - 优化JOIN顺序和条件
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
    coalesce(dm2.freal_name, b.freal_name) as fdetect_two_name,
    coalesce(dm3.freal_name, b.freal_name) as fdetect_three_name,
    dm3p.freal_name as fdetect_three_name_pingmu,
    case when dm2.freal_name is not null then "是" else "否" end as "是否分模块",
    a.fanchor_level
from jp_sale as a
left join detect as b on a.fseries_number=b.fserial_number
left join after_sale as c on a.fseries_number=c.Fsales_series_number
left join after_sale_detect as f on c.fseries_number=f.fserial_number
left join jp_first_sale as d on a.fseries_number=d.fseries_number
left join jp_second_sale as e on a.fseries_number=e.fold_fseries_number
left join detect_modules as dm2 on a.fseries_number=dm2.fserial_number and dm2.module_type='detect_two'
left join detect_modules as dm3 on a.fseries_number=dm3.fserial_number and dm3.module_type='detect_three'
left join detect_modules as dm3p on a.fseries_number=dm3p.fserial_number and dm3p.module_type='detect_three_pingmu'
where a.fstart_time >= (select start_date_365 from time_constants)

union all

-- 采货侠分支（保持原有逻辑，但使用优化后的CTE）
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
    coalesce(dm2.freal_name, b.freal_name) as fdetect_two_name,
    coalesce(dm3.freal_name, b.freal_name) as fdetect_three_name,
    dm3p.freal_name as fdetect_three_name_pingmu,
    case when dm2.freal_name is not null then "是" else "否" end as "是否分模块",
    a.fanchor_level
from caihuoxia_sale as a
left join detect as b on a.fseries_number=b.fserial_number
left join caihuoxia_after_sale as c on a.fseries_number=c.fbusiness_id
left join after_sale_detect as g on c.fnew_serial_no=g.fserial_number
left join caihuoxia_first_sale as d on a.fseries_number=d.fseries_number
left join caihuoxia_second_sale as e on a.fseries_number=e.fseries_number
left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as f on a.fseries_number=upper(f.fsrouce_serial_no)
left join detect_modules as dm2 on a.fseries_number=dm2.fserial_number and dm2.module_type='detect_two'
left join detect_modules as dm3 on a.fseries_number=dm3.fserial_number and dm3.module_type='detect_three'
left join detect_modules as dm3p on a.fseries_number=dm3p.fserial_number and dm3p.module_type='detect_three_pingmu'
where a.fstart_time >= (select start_date_365 from time_constants)

union all

-- B2B分支（平台=5，近365天）
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
    coalesce(dm2.freal_name, b.freal_name) as fdetect_two_name,
    coalesce(dm3.freal_name, b.freal_name) as fdetect_three_name,
    dm3p.freal_name as fdetect_three_name_pingmu,
    case when dm2.freal_name is not null then "是" else "否" end as "是否分模块",
    a.fanchor_level
from b2b_sale as a
left join b2b_detect as b on a.fseries_number=b.fserial_number
left join b2b_after_sale as cc on a.fseries_number=cc.fbusiness_id
left join after_sale_detect as g on upper(coalesce(cc.fnew_serial_no,a.fseries_number))=upper(g.fserial_number)
left join b2b_detect_two as h on a.fseries_number=h.fserial_number
left join b2b_detect_three as i on a.fseries_number=i.fserial_number
left join b2b_detect_three_pingmu as k on a.fseries_number=k.fserial_number
left join b2b_first_sale as d on a.fseries_number=d.fseries_number
left join b2b_second_sale as e on a.fseries_number=e.fseries_number
left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as f on a.fseries_number=upper(f.fsrouce_serial_no)
left join detect_modules as dm2 on a.fseries_number=dm2.fserial_number and dm2.module_type='detect_two'
left join detect_modules as dm3 on a.fseries_number=dm3.fserial_number and dm3.module_type='detect_three'
left join detect_modules as dm3p on a.fseries_number=dm3p.fserial_number and dm3p.module_type='detect_three_pingmu'
where a.fstart_time >= (select start_date_365 from time_constants);

-- =========================
-- 索引建议说明：为避免在部分执行环境被误解析为可执行语句，
-- 已移至同目录文档 `多平台售后明细_优化版_索引建议.md`。

