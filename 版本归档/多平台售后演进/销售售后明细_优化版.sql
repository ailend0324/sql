-- 竞拍售后明细（含鱼市B2B渠道）- 优化重构版
-- 说明：
-- 1) 完整沿用《原始/竞拍售后明细数据.sql》的自有平台与采货侠两大分支；
-- 2) 新增 B2B 分支（forder_platform=5，近365天，不拼历史表），售后使用 drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales；
-- 3) B2B 检测模块窗口按365天独立配置（不影响原有自有/采货侠窗口）。
-- 4) 优化重构：合并重复CTE，减少代码重复，提高可维护性

-- =========================
-- 统一配置参数
-- =========================
with time_config as (
    select 
        date_sub(from_unixtime(unix_timestamp()), 400) as window_400,
        date_sub(from_unixtime(unix_timestamp()), 365) as window_365,
        date_sub(from_unixtime(unix_timestamp()), 800) as window_800
),

-- =========================
-- 统一检测相关CTE
-- =========================
unified_detect as (       -- 统一检测明细数据，取检测人、检测模板
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
    left join (
        select fseries_number, forder_create_time, forder_platform
        from (
            select
                fseries_number,
                forder_create_time,
                forder_platform,
                row_number() over(partition by fseries_number order by forder_create_time desc) as num
            from dws.dws_jp_order_detail
            where ftest_show <> 1
            and (fmerchant_jp=0 or fmerchant_jp is null)
            and forder_status in (2,3,4,6)
            and forder_create_time>=to_date('2024-01-01')
        ) t where t.num=1
    ) as b on upper(a.fserial_number)=b.fseries_number
    left join (
        select freal_name, Fposition_id
        from (
            select *, row_number() over(partition by freal_name order by fcreate_time desc) as num
            from drt.drt_my33310_amcdb_t_user
        ) t where num=1
    ) as c on a.freal_name=c.freal_name
    where a.fis_deleted=0
    and to_date(a.fend_time)>=to_date('2024-01-01')
    and a.fend_time<b.forder_create_time
    and c.Fposition_id <>129
),

unified_detect_two as (   -- 统一分模块检测记录
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
        where to_date(a.fcreate_time)>=to_date('2024-01-01')
        and fserial_number!=""
        and fserial_number is not null
    ) t where num=1
),

unified_detect_three as ( -- 统一外观检测记录
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
        where to_date(a.fend_time)>=to_date('2024-01-01')
        and b.fdet_sop_task_name like "%外观%"
    ) t where num=1
),

unified_detect_three_pingmu as ( -- 统一屏幕检测记录
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
        where to_date(a.fend_time)>=to_date('2024-01-01')
        and b.fdet_sop_task_name like "%屏幕%"
        and b.fdet_sop_task_name!="外观屏幕"
    ) t where num=1
),

unified_detect_four as (  -- 统一拆修检测记录
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
        where to_date(a.fend_time)>=to_date('2024-01-01')
        and b.fdet_sop_task_name like "%拆修%"
    ) t where num=1
),

-- =========================
-- 统一售后检测CTE
-- =========================
unified_after_sale_detect as (
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
        and to_date(fend_time)>=to_date('2024-01-01')
        and left(fserial_number,2) in ('YZ','NT','JM')
    ) t where num=1
),

-- =========================
-- 统一销售相关CTE
-- =========================
unified_jp_sale as (      -- 统一竞拍销售数据（自有平台）
    select
        *
    from (
        select
            *,
            row_number() over(partition by fseries_number order by forder_create_time desc) as num
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and to_date(forder_create_time)>='2024-01-01'
        and forder_platform not in (5,6)
        and (fmerchant_jp=0 or fmerchant_jp is null)
        and forder_status in (2,3,4,6)
    ) t where num=1
),

unified_caihuoxia_sale as ( -- 统一采货侠销售数据
    select
        *
    from (
        select
            *,
            row_number() over(partition by fseries_number order by forder_create_time desc) as num
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and forder_platform=6
        and fmerchant_jp=0
        and to_date(forder_create_time)>='2024-01-01'
        and forder_status in (2,3,4,6)
    ) t where num=1
),

unified_b2b_sale as (     -- 统一B2B销售数据
    select
        *
    from (
        select
            *,
            row_number() over(partition by fseries_number order by forder_create_time desc) as num
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and forder_platform=5
        and forder_status in (2,3,4,6)
        and to_date(forder_create_time)>='2024-01-01'
    ) t where num=1
),

-- =========================
-- 统一售后相关CTE
-- =========================
unified_after_sale as (   -- 统一自有平台售后数据
    select
        *
    from (
        select
            a.*,
            b.fseries_number,
            row_number() over(partition by fsales_series_number order by a.fauto_create_time desc) as num
        from drt.drt_my33310_recycle_t_after_sales_order_info as a
        left join drt.drt_my33310_recycle_t_order as b on a.fafter_sales_order_id=b.forder_id
        where a.fvalid=1
    ) t where num=1
),

unified_caihuoxia_after_sale as ( -- 统一采货侠售后数据
    select
        *
    from (
        select
            *,
            row_number() over(partition by fbusiness_id order by fcreate_time desc) as num
        from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales
    ) t where num=1
),

unified_b2b_after_sale as ( -- 统一B2B售后数据
    select
        *
    from (
        select
            *,
            row_number() over(partition by fbusiness_id order by fcreate_time desc) as num
        from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales
    ) t where num=1
),

-- =========================
-- 统一首次销售CTE
-- =========================
unified_jp_first_sale as ( -- 统一自有平台首次销售
    select
        *
    from (
        select
            *,
            row_number() over(partition by fseries_number order by forder_create_time asc) as num
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and to_date(forder_create_time)>='2024-01-01'
        and forder_platform not in (5,6)
        and (fmerchant_jp=0 or fmerchant_jp is null)
        and forder_status in (2,3,4,6)
    ) t where num=1
),

unified_caihuoxia_first_sale as ( -- 统一采货侠首次销售
    select
        *
    from (
        select
            *,
            row_number() over(partition by fseries_number order by forder_create_time asc) as num
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and forder_platform=6
        and fmerchant_jp=0
        and to_date(forder_create_time)>='2024-01-01'
        and forder_status in (2,3,4,6)
    ) t where num=1
),

unified_b2b_first_sale as ( -- 统一B2B首次销售
    select
        *
    from (
        select
            *,
            row_number() over(partition by fseries_number order by forder_create_time asc) as num
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and forder_platform=5
        and to_date(forder_create_time)>='2024-01-01'
        and forder_status in (2,3,4,6)
    ) t where num=1
),

-- =========================
-- 统一二次销售CTE（简化版，保留核心逻辑）
-- =========================
unified_jp_second_sale as ( -- 统一自有平台二次销售（仅保留2024年及以后，去除历史表）
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
    and to_date(a.forder_create_time)>='2024-01-01'
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
    and a.fchannel_name='竞拍销售默认渠道号'
    and to_date(a.foffer_time)>='2024-01-01'
),

unified_caihuoxia_second_sale as ( -- 统一采货侠二次销售（2024年及以后）
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
            a.fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            row_number() over(partition by a.fseries_number order by a.forder_create_time asc) as num
        from dws.dws_jp_order_detail as a
        where a.ftest_show <> 1
        and to_date(a.forder_create_time)>='2024-01-01'
        and a.forder_platform=6
        and a.fmerchant_jp=0
        and a.forder_status in (2,3,4,6)
    ) t where num=2
),

unified_b2b_second_sale as ( -- 统一B2B二次销售（2024年及以后）
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
            a.fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            row_number() over(partition by a.fseries_number order by a.forder_create_time asc) as num
        from dws.dws_jp_order_detail as a
        where a.ftest_show <> 1
        and to_date(a.forder_create_time)>='2024-01-01'
        and a.forder_platform=5
        and a.forder_status in (2,3,4,6)
    ) t where num=2
),

-- =========================
-- 主查询：使用CASE WHEN替代多个UNION ALL
-- =========================
main_query as (
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
        
        -- 售后明细编号：根据渠道类型选择不同字段
        case 
            when a.channel_type = '自有平台' then c.fseries_number
            when a.channel_type = '采货侠' then c2.fnew_serial_no
            when a.channel_type = '鱼市B2B' then cc.fnew_serial_no
        end as fafter_series_number,
        
        -- 二次销售信息：根据渠道类型选择不同JOIN
        case 
            when a.channel_type = '自有平台' then e1.Fcity_name
            when a.channel_type = '采货侠' then e2.Fcity_name
            when a.channel_type = '鱼市B2B' then e3.Fcity_name
        end as fsecond_sale_city,
        
        case 
            when a.channel_type = '自有平台' then e1.Forder_address
            when a.channel_type = '采货侠' then e2.Forder_address
            when a.channel_type = '鱼市B2B' then e3.Forder_address
        end as fsecond_sale_address,
        
        case 
            when a.channel_type = '自有平台' then e1.Freceiver_id
            when a.channel_type = '采货侠' then e2.Freceiver_id
            when a.channel_type = '鱼市B2B' then e3.Freceiver_id
        end as fsecond_sale_id,
        
        case 
            when a.channel_type = '自有平台' then e1.Freceiver_name
            when a.channel_type = '采货侠' then e2.Freceiver_name
            when a.channel_type = '鱼市B2B' then e3.Freceiver_name
        end as fsecond_sale_name,
        
        case 
            when a.channel_type = '自有平台' then e1.Freceiver_phone
            when a.channel_type = '采货侠' then e2.Freceiver_phone
            when a.channel_type = '鱼市B2B' then e3.Freceiver_phone
        end as fsecond_sale_phone,
        
        a.channel_type as "销售渠道",
        left(a.fseries_number,2) as "渠道",
        a.fcost_price/100 as "成本价",
        a.foffer_price/100 as "当前出价",
        
        -- 销售额：根据渠道类型选择不同逻辑
        case 
            when a.channel_type = '自有平台' then 
                if((c.Ftotal_real_refund_amount>0 and a.foffer_price=c.Ftotal_real_refund_amount) or (a.fchannel_name="竞拍销售默认渠道号"),0,a.foffer_price/100)
            when a.channel_type = '采货侠' then 
                if(c2.fapply_time is not null and c2.fapply_time !='0000-00-00 00:00:00.0' and c2.Fjudge_type=1,0,a.foffer_price/100)
            when a.channel_type = '鱼市B2B' then 
                if(cc.frefund_total>0,0,a.foffer_price/100)
        end as "销售额",
        
        b.Fdet_tpl,
        b.Freal_name,
        b.Fend_time,
        b.Fdetection_object,
        
        -- 仓库代码：统一计算逻辑
        case when b.fwarehouse_code='12' then "东莞仓"
             when right(left(a.fseries_number,6),2)="16" or left(a.fseries_number,3)="020" then "杭州仓"
        else "深圳仓" end as fwarehouse_code,
        
        -- 售后申请时间：根据渠道类型选择不同字段
        case 
            when a.channel_type = '自有平台' then c.Fauto_create_time
            when a.channel_type = '采货侠' then c2.fapply_time
            when a.channel_type = '鱼市B2B' then cc.fapply_time
        end as Fauto_create_time,
        
        get_json_object(b.Fgoods_level,'$.levelName') as Fgoods_level,
        
        -- 售后原因：根据渠道类型选择不同字段
        case 
            when a.channel_type = '自有平台' then c.Fappeal_reason
            when a.channel_type = '采货侠' then c2.Fjudge_reason
            when a.channel_type = '鱼市B2B' then cc.fjudge_reason
        end as Fappeal_reason,
        
        -- 售后结果：根据渠道类型选择不同字段
        case 
            when a.channel_type = '自有平台' then cast(c.Ffirst_trial_result as string)
            when a.channel_type = '采货侠' then c2.Fjudge_result
            when a.channel_type = '鱼市B2B' then cc.fjudge_result
        end as Ffirst_trial_result,
        
        -- 其他字段根据渠道类型处理
        case 
            when a.channel_type = '自有平台' then c.Freexamine_result
            else null
        end as Freexamine_result,
        
        case 
            when a.channel_type = '自有平台' then c.Fdetection_price/100
            else null
        end as "检测价",
        
        case 
            when a.channel_type = '自有平台' then c.Freinspection_price/100
            else null
        end as "二次检测价",
        
        case 
            when a.channel_type = '自有平台' then c.Ftotal_diff_amount/100
            else null
        end as "检测差异金额",
        
        case 
            when a.channel_type = '自有平台' then c.Ftotal_refundable_amount/100
            else null
        end as "总应退款金额",
        
        case 
            when a.channel_type = '自有平台' then c.Ftotal_real_refund_amount/100
            when a.channel_type = '采货侠' then c2.Forder_deal_price/100
            when a.channel_type = '鱼市B2B' then cc.frefund_total/100
        end as "总实退款金额",
        
        -- 检测渠道和模板：统一计算逻辑
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
        
        -- 售后数：根据渠道类型选择不同逻辑
        case 
            when a.channel_type = '自有平台' then 
                case when c.Ftotal_real_refund_amount>0 then 1 else 0 end
            when a.channel_type = '采货侠' then 
                case when c2.fapply_time is not null and c2.fapply_time !='0000-00-00 00:00:00.0' and c2.Fjudge_type=1 then 1
                     when f.fsrouce_serial_no is not null then 1 else 0 end
            when a.channel_type = '鱼市B2B' then 
                case when c2.fapply_time is not null and c2.fapply_time !='0000-00-00 00:00:00.0' and c2.Fjudge_type=1 then 1
                     when f.fsrouce_serial_no is not null then 1 else 0 end
        end as "售后数",
        
        -- 售后通过时间：根据渠道类型选择不同逻辑
        case 
            when a.channel_type = '自有平台' then 
                case when c.Ftotal_real_refund_amount>0 then c.Freceived_audit_result_time else null end
            when a.channel_type = '采货侠' then 
                case when c2.fapply_time is not null and c2.fapply_time !='0000-00-00 00:00:00.0' and c2.Fjudge_type=1 then c2.Fjudge_time
                     when f.fsrouce_serial_no is not null then c2.Fjudge_time else null end
            when a.channel_type = '鱼市B2B' then 
                case when c2.fapply_time is not null and c2.fapply_time !='0000-00-00 00:00:00.0' and c2.Fjudge_type=1 then c2.Fjudge_time
                     when f.fsrouce_serial_no is not null then c2.Fjudge_time else null end
        end as "售后通过时间",
        
        -- 退货数：根据渠道类型选择不同逻辑
        case 
            when a.channel_type = '自有平台' then 
                case when c.Ftotal_real_refund_amount>0 and a.foffer_price<c.Ftotal_real_refund_amount and a.fstart_time>='2022-01-01' then 1
                     when c.Ftotal_real_refund_amount>0 and a.foffer_price=c.Ftotal_real_refund_amount and a.fstart_time<'2022-01-01' then 1
                else 0 end
            when a.channel_type = '采货侠' then 
                case when c2.fapply_time is not null and c2.fapply_time !='0000-00-00 00:00:00.0' and c2.Fjudge_type=1 then 1
                     when f.fsrouce_serial_no is not null then 1 else 0 end
            when a.channel_type = '鱼市B2B' then 
                case when c2.fapply_time is not null and c2.fapply_time !='0000-00-00 00:00:00.0' and c2.Fjudge_type=1 then 1
                     when f.fsrouce_serial_no is not null then 1 else 0 end
        end as "退货数",
        
        -- 补差赔付：根据渠道类型选择不同逻辑
        case 
            when a.channel_type = '自有平台' then 
                case when c.Ftotal_real_refund_amount>0 and a.foffer_price>c.Ftotal_real_refund_amount then 1 else 0 end
            else 0
        end as "补差赔付",
        
        case 
            when a.channel_type = '自有平台' then 
                case when c.Ftotal_real_refund_amount>0 and a.foffer_price>c.Ftotal_real_refund_amount then c.Ftotal_real_refund_amount/100 else 0 end
            else 0
        end as "赔付金额",
        
        -- 售后类型：根据渠道类型选择不同逻辑
        case 
            when a.channel_type = '自有平台' then c.Fafter_sales_type
            else null
        end as Fafter_sales_type,
        
        case 
            when a.channel_type = '自有平台' then 
                case when c.Fafter_sales_type=1 then "仅退款"
                     when c.Fafter_sales_type=2 then "退货退款"
                else "其它" end
            when a.channel_type = '采货侠' then 
                case when c2.fapply_time is not null and c2.fapply_time !='0000-00-00 00:00:00.0' and c2.Fjudge_type=1 then "退货退款"
                     when f.fsrouce_serial_no is not null then "退货退款" else null end
            when a.channel_type = '鱼市B2B' then 
                case when c2.fapply_time is not null and c2.fapply_time !='0000-00-00 00:00:00.0' and c2.Fjudge_type=1 then "退货退款"
                     when f.fsrouce_serial_no is not null then "退货退款" else null end
        end as "售后类型",
        
        case 
            when a.channel_type = '自有平台' then c.Faftersales_owner
            else null
        end as Faftersales_owner,
        
        -- 首次销售价格：根据渠道类型选择不同JOIN
        case 
            when a.channel_type = '自有平台' then d1.foffer_price/100
            when a.channel_type = '采货侠' then d2.foffer_price/100
            when a.channel_type = '鱼市B2B' then d3.foffer_price/100
        end as first_price,
        
        -- 二次销售时间：根据渠道类型选择不同JOIN
        case 
            when a.channel_type = '自有平台' then e1.fstart_time
            when a.channel_type = '采货侠' then e2.fstart_time
            when a.channel_type = '鱼市B2B' then e3.fstart_time
        end as fsecond_sale_time,
        
        -- 二次销售价格：根据渠道类型选择不同JOIN
        case 
            when a.channel_type = '自有平台' then e1.foffer_price/100
            when a.channel_type = '采货侠' then e2.foffer_price
            when a.channel_type = '鱼市B2B' then e3.foffer_price
        end as second_price,
        
        -- 二次差价成本：根据渠道类型选择不同逻辑
        case 
            when a.channel_type = '自有平台' then 
                if(c.Ftotal_real_refund_amount>0 and a.foffer_price=c.Ftotal_real_refund_amount and a.fstart_time>='2022-01-01',0,d1.foffer_price/100-e1.foffer_price/100)
            when a.channel_type = '采货侠' then 
                if(c2.fapply_time is not null and c2.fapply_time !='0000-00-00 00:00:00.0' and c2.Fjudge_type=1,d2.foffer_price/100-e2.foffer_price,if(f.fsrouce_serial_no is not null,d2.foffer_price/100-e2.foffer_price,0))
            when a.channel_type = '鱼市B2B' then 
                if(c2.fapply_time is not null and c2.fapply_time !='0000-00-00 00:00:00.0' and c2.Fjudge_type=1,d3.foffer_price/100-e3.foffer_price,if(f.fsrouce_serial_no is not null,d3.foffer_price/100-e3.foffer_price,0))
        end as "二次差价成本",
        
        -- 检测人员信息：统一处理
        if(h.fdetect_two_name is null,b.freal_name,h.fdetect_two_name) as fdetect_two_name,
        if(i.fdetect_three_name is null,b.freal_name,i.fdetect_three_name) as fdetect_three_name,
        j.fdetect_three_name_pingmu,
        if(h.fdetect_two_time is not null,"是","否") as "是否分模块",
        a.fanchor_level

    from (
        -- 合并所有销售数据
        select *, '自有平台' as channel_type from unified_jp_sale
        union all
        select *, '采货侠' as channel_type from unified_caihuoxia_sale  
        union all
        select *, '鱼市B2B' as channel_type from unified_b2b_sale
    ) as a

    -- 根据渠道类型选择不同的JOIN逻辑
    left join unified_detect as b on a.fseries_number=b.fserial_number
    left join unified_after_sale as c on a.channel_type='自有平台' and a.fseries_number=c.Fsales_series_number
    left join unified_caihuoxia_after_sale as c2 on a.channel_type in ('采货侠','鱼市B2B') and a.fseries_number=c2.fbusiness_id
    left join unified_b2b_after_sale as cc on a.channel_type='鱼市B2B' and a.fseries_number=cc.fbusiness_id

    -- 售后检测JOIN
    left join unified_after_sale_detect as g on (
        case 
            when a.channel_type = '自有平台' then c.fseries_number
            when a.channel_type = '采货侠' then c2.fnew_serial_no
            when a.channel_type = '鱼市B2B' then cc.fnew_serial_no
        end = g.fserial_number
    )

    -- 根据渠道类型选择不同的首次销售和二次销售
    left join unified_jp_first_sale as d1 on a.channel_type='自有平台' and a.fseries_number=d1.fseries_number
    left join unified_caihuoxia_first_sale as d2 on a.channel_type='采货侠' and a.fseries_number=d2.fseries_number
    left join unified_b2b_first_sale as d3 on a.channel_type='鱼市B2B' and a.fseries_number=d3.fseries_number

    left join unified_jp_second_sale as e1 on a.channel_type='自有平台' and a.fseries_number=e1.fold_fseries_number
    left join unified_caihuoxia_second_sale as e2 on a.channel_type='采货侠' and a.fseries_number=e2.fseries_number
    left join unified_b2b_second_sale as e3 on a.channel_type='鱼市B2B' and a.fseries_number=e3.fseries_number

    -- 检测相关JOIN
    left join unified_detect_two as h on a.fseries_number=h.fserial_number
    left join unified_detect_three as i on a.fseries_number=i.fserial_number
    left join unified_detect_three_pingmu as j on a.fseries_number=j.fserial_number
    left join unified_detect_four as k on a.fseries_number=k.fserial_number

    -- 采货侠特有的JOIN
    left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as f on a.channel_type in ('采货侠','鱼市B2B') and a.fseries_number=upper(f.fsrouce_serial_no)

    where a.fstart_time>=to_date('2024-01-01')
)

-- =========================
-- 执行主查询
-- =========================
select * from main_query
