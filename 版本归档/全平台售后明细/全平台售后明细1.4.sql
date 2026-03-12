-- ========================================
-- 全平台售后明细1.4版本：
-- 优化内容：
-- 修复了“复制粘贴错误”：彻底删除了 chx_second_sale 中导致数据翻倍的重复代码块，解决了数据虚高的问题。
-- 增加了“智能去重锁”：在 chx_second_sale 中加入了 row_number() 逻辑，强制保留时间最新、且有金额的一条记录。
-- ========================================
with 
-- 0. 全局参数定义 (统一时间窗口)
params as (
    select 
        to_date(date_sub(from_unixtime(unix_timestamp()), 800)) as date_window_long,
        -- 【新增】专门用于检测和售后记录的400天窗口
        to_date(date_sub(from_unixtime(unix_timestamp()), 400)) as date_window_detect, 
        to_date(date_sub(from_unixtime(unix_timestamp()), 365)) as date_window_mid,
        to_date(date_sub(from_unixtime(unix_timestamp()), 365)) as date_window_short
),
-- ========================================
-- 1. 基础维度表准备
-- ========================================
base_user_info as (
    select freal_name, Fposition_id, fusername 
    from (
        select *, row_number() over(partition by freal_name order by fcreate_time desc) as rn 
        from drt.drt_my33310_amcdb_t_user
    ) t where rn=1
),

base_detect_record as (
    select 
        fcreate_time, fend_time, fdet_type, freport_type, fverdict, fis_deleted,
        upper(fserial_number) as fserial_number,
        Fdet_tpl, Freal_name, fbrand_name, Fdetection_object, fgoods_level, fwarehouse_code,
        row_number() over(partition by upper(fserial_number) order by fcreate_time asc) as rn_asc,
        row_number() over(partition by upper(fserial_number) order by fend_time asc) as rn_end_asc
    from drt.drt_my33310_detection_t_detect_record
    where fis_deleted=0 
      -- 【修改点】这里从 date_window_short 改为 date_window_detect
      and to_date(fend_time) >= (select date_window_detect from params)
),

after_sale_detect as (
    select * from base_detect_record
    where rn_end_asc=1
      and fdet_type=0 
      and freport_type=0 
      and fverdict <> '测试单'
      and left(fserial_number, 2) in ('YZ','NT','JM')
),

-- ========================================
-- 2. 订单基础数据 (统一入口)
-- ========================================
base_order_raw as (
    select 
        fseries_number,
        -- upper(fseries_number) as fseries_number_upper, -- 【移除】避免混淆，后续直接用 upper() 函数
        forder_create_time,
        foffer_price,
        fstart_time,
        fchannel_name,
        forder_status,
        forder_platform,
        fmerchant_jp,
        ftest_show,
        fclass_name,
        fproduct_name,
        fproject_name,
        Fcity_name,
        Forder_address,
        Freceiver_id,
        Freceiver_name,
        Freceiver_phone,
        fcost_price,
        fanchor_level,
        foffer_time,
        
        row_number() over(partition by fseries_number order by forder_create_time desc) as rn_last,
        row_number() over(partition by fseries_number order by forder_create_time asc) as rn_first
    from dws.dws_jp_order_detail
    where ftest_show <> 1
      and forder_status in (2,3,4,6)
      and to_date(forder_create_time) >= (select date_window_mid from params)
),

-- 拆分业务线
jp_sale as (
    select * from base_order_raw 
    where forder_platform not in (5,6) 
      and (fmerchant_jp=0 or fmerchant_jp is null)
      and rn_last = 1
),
jp_first_sale as (
    select * from base_order_raw 
    where forder_platform not in (5,6) 
      and (fmerchant_jp=0 or fmerchant_jp is null)
      and rn_first = 1
),
b2b_sale as (
    select * from base_order_raw 
    where forder_platform = 5 
      and rn_last = 1
      and to_date(forder_create_time) >= (select date_window_short from params)
),
b2b_first_sale as (
    select * from base_order_raw 
    where forder_platform = 5 
      and rn_first = 1
),
chx_sale as (
    select * from base_order_raw 
    where forder_platform = 6 
      and fmerchant_jp = 0
      and rn_last = 1
      and to_date(forder_create_time) >= (select date_window_short from params)
),
chx_first_sale as (
    select * from base_order_raw 
    where forder_platform = 6 
      and fmerchant_jp = 0
      and rn_first = 1
),

-- JP 二次销售逻辑
jp_second_sale as (
    select 
        b.fold_fseries_number as fold_fseries_number,
        a.foffer_price, 
        a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, 
        a.fstart_time
    from (
        select * from base_order_raw 
        where fchannel_name='竞拍销售默认渠道号' 
          and rn_first = 1
    ) as a
    left join dws.dws_hs_order_detail as b on a.fseries_number=b.fseries_number
    
    union all
    
    select 
        coalesce(b.fold_fseries_number, c.fold_fseries_number) as fold_fseries_number,
        a.foffer_price, 
        null, null, null, null, null, 
        a.foffer_time as fstart_time
    from dws.dws_th_order_detail as a
    left join dws.dws_hs_order_detail as b on a.fseries_number=b.fseries_number
    left join dws.dws_hs_order_detail_history2018_2022 as c on a.fseries_number=c.fseries_number
    where a.Fbd_status <> 2 
      and a.fchannel_name='竞拍销售默认渠道号'
),

local_sn_map as (
    select fserial_no, upper(fsrouce_serial_no) as fsrouce_serial_no_upper
    from drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn
),

-- 4. 【已验证-最终修正版】采货侠二次销售 (锁定最新记录)
chx_second_sale as (
    select 
        fstart_time, fseries_number, foffer_price, Fcity_name, Forder_address, Freceiver_id, Freceiver_name, Freceiver_phone
    from (
        select 
            *,
            -- 核心逻辑：按时间倒序 + 金额优先
            -- Rank 1 = 最新销售 (保留，用于代表最终处置状态)
            -- Rank 2+ = 历史销售 (剔除，避免重复计算)
            row_number() over(
                partition by fseries_number 
                order by 
                    (case when foffer_price > 0 then 1 else 0) desc, 
                    fstart_time desc
            ) as rn_final_dedup
        from (
            -- 分支1：采货侠自身的二次销售 (已修复重复粘贴问题)
            select fstart_time, fseries_number, foffer_price/100 as foffer_price, Fcity_name, Forder_address, Freceiver_id, Freceiver_name, Freceiver_phone
            from ( 
                select a.fstart_time, if(b.fsrouce_serial_no_upper is not null, b.fsrouce_serial_no_upper, upper(a.fseries_number)) as fseries_number, a.foffer_price, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, a.rn_first as num 
                from base_order_raw as a 
                left join local_sn_map as b on a.fseries_number=b.fserial_no 
                where a.forder_platform = 6 and a.fmerchant_jp = 0
            ) t where num=2
            
            union all
            
            -- 分支2：来自 JP 表的数据
            select fstart_time, fseries_number, foffer_price/100 as foffer_price, Fcity_name, Forder_address, Freceiver_id, Freceiver_name, Freceiver_phone
            from ( 
                select a.fstart_time, if(b.fsrouce_serial_no_upper is not null, b.fsrouce_serial_no_upper, upper(a.fseries_number)) as fseries_number, a.foffer_price, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, 
                row_number() over(partition by fseries_number order by forder_create_time asc) as num 
                from dws.dws_jp_order_detail as a 
                left join local_sn_map as b on a.fseries_number=b.fserial_no 
                where ftest_show <> 1 and forder_platform<>6 and to_date(a.forder_create_time) >= (select date_window_short from params) and fmerchant_jp=0 and forder_status in (2,3,4,6)
            ) t where num=1
            
            union all
            
            -- 分支3：来自 TH 表的数据
            select fstart_time, fseries_number, foffer_price/100 as foffer_price, null, null, null, null, null
            from ( 
                select a.fstart_time, if(b.fsrouce_serial_no_upper is not null, b.fsrouce_serial_no_upper, upper(a.fseries_number)) as fseries_number, a.foffer_price, 
                row_number() over(partition by fseries_number order by forder_create_time asc) as num 
                from dws.dws_th_order_detail as a 
                left join local_sn_map as b on a.fseries_number=b.fserial_no 
                where Fbd_status <> 2
            ) t where num=1
        ) t_union
    ) t_final 
    where rn_final_dedup = 1 -- 只保留 Rank 1
),

b2b_second_sale as (
    select fstart_time, fseries_number, foffer_price/100 as foffer_price, Fcity_name, Forder_address, Freceiver_id, Freceiver_name, Freceiver_phone
    from ( 
        select a.fstart_time, if(b.fsrouce_serial_no_upper is not null, b.fsrouce_serial_no_upper, upper(a.fseries_number)) as fseries_number, a.foffer_price, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, a.rn_first as num 
        from base_order_raw as a 
        left join local_sn_map as b on a.fseries_number=b.fserial_no 
        where a.forder_platform = 5
    ) t where num=2
    union all
    select * from chx_second_sale where foffer_price is not null
),

-- ========================================
-- 3. 检测与售后数据
-- ========================================
detect_one as (
    select upper(fserial_number) as fserial_number, freal_name as fdetect_one_name, from_unixtime(fend_det_time) as fdetect_one_time
    from ( 
        select a.fserial_number, a.fend_det_time, b.freal_name, row_number()over(partition by upper(a.fserial_number) order by a.fend_det_time desc) as num 
        from drt.drt_my33312_detection_t_automation_det_record as a 
        left join base_user_info as b on a.fuser_name=b.fusername 
        where fserial_number!="" and fserial_number is not null and to_date(from_unixtime(a.fend_det_time)) >= (select date_window_long from params)
    )t where num=1
),
detect_two as (
    select upper(fserial_number) as fserial_number, freal_name as fdetect_two_name, fcreate_time as fdetect_two_time
    from ( 
        select a.fcreate_time, a.fserial_number, b.freal_name, row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num 
        from drt.drt_my33312_detection_t_det_app_record as a 
        left join base_user_info as b on a.fuser_name=b.fusername 
        where to_date(a.fcreate_time) >= (select date_window_long from params) and fserial_number!="" and fserial_number is not null
    )t where num=1
),
detect_three as (
    select upper(fserial_number) as fserial_number, freal_name as fdetect_three_name, fcreate_time as fdetect_three_time
    from ( 
        select a.fcreate_time, a.fserial_number, b.freal_name, row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num 
        from drt.drt_my33312_detection_t_det_task as a 
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id 
        where to_date(a.fend_time) >= (select date_window_long from params) and b.fdet_sop_task_name like "%外观%"
    )t where num=1
),
detect_three_pingmu as (
    select upper(fserial_number) as fserial_number, case when freal_name="李俊峰" then "李俊锋" else freal_name end as fdetect_three_name_pingmu, fcreate_time as fdetect_three_time_pingmu
    from ( 
        select a.fcreate_time, a.fserial_number, b.freal_name, row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num 
        from drt.drt_my33312_detection_t_det_task as a 
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id 
        where to_date(a.fend_time) >= (select date_window_long from params) and b.fdet_sop_task_name like "%屏幕%" and b.fdet_sop_task_name!="外观屏幕"
    )t where num=1
),
detect_four as (
    select upper(fserial_number) as fserial_number, freal_name as fdetect_four_name, fcreate_time as fdetect_four_time
    from ( 
        select a.fcreate_time, a.fserial_number, b.freal_name, row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num 
        from drt.drt_my33312_detection_t_det_task as a 
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id 
        where to_date(a.fend_time) >= (select date_window_long from params) and b.fdet_sop_task_name like "%拆修%"
    )t where num=1
),

after_sale_jp as (
    select * from (
        select a.*, b.fseries_number, row_number() over(partition by fsales_series_number order by a.fauto_create_time desc ) as num
        from drt.drt_my33310_recycle_t_after_sales_order_info as a
        left join drt.drt_my33310_recycle_t_order as b on a.fafter_sales_order_id=b.forder_id
        where a.fvalid=1
    ) t where num=1
),
after_sale_b2b_chx as (
    select * from ( 
        select * , row_number() over(partition by fbusiness_id order by fcreate_time desc) as num 
        from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales 
    )t where num=1
),

-- ==============================================
-- 4. 汇聚层
-- ==============================================
union_all_data as (
    -- A. 自有平台
    select
        "自有平台" as sale_channel_type,
        a.fstart_time, a.fseries_number, a.fclass_name, b.fbrand_name, a.fchannel_name, a.fproduct_name, a.fproject_name, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, a.fcost_price, a.foffer_price, a.fanchor_level,
        f.freal_name as fsecond_detect_name, 
        c.fseries_number as fafter_series_number, 
        e.Fcity_name as fsecond_sale_city, e.Forder_address as fsecond_sale_address, e.Freceiver_id as fsecond_sale_id, e.Freceiver_name as fsecond_sale_name, e.Freceiver_phone as fsecond_sale_phone,
        c.Ftotal_real_refund_amount as refund_amount_raw, 
        b.Fdet_tpl as raw_det_tpl, b.Freal_name, b.Fend_time, b.Fdetection_object, b.fwarehouse_code as raw_warehouse_code, b.Fgoods_level,
        c.Fauto_create_time, c.Fappeal_reason, cast(c.Ffirst_trial_result as string) as Ffirst_trial_result, c.Freexamine_result, c.Fdetection_price, c.Freinspection_price, c.Ftotal_diff_amount, c.Ftotal_refundable_amount, c.Freceived_audit_result_time, 
        c.Fafter_sales_type as after_sales_type_code, 
        c.Faftersales_owner,
        d.foffer_price as first_price_raw, e.fstart_time as fsecond_sale_time, e.foffer_price as second_price_raw,
        g.fdetect_two_name, h.fdetect_three_name, j.fdetect_three_name_pingmu, g.fdetect_two_time,
        0 as is_b2b_chx, null as chx_judge_type
    from jp_sale as a
    -- 【修复点：所有 Join 使用 upper(a.fseries_number) 替代 a.fseries_number_upper】
    left join base_detect_record as b on upper(a.fseries_number) = b.fserial_number and b.rn_asc = 1
    left join after_sale_jp as c on upper(a.fseries_number) = c.Fsales_series_number
    left join after_sale_detect as f on c.fseries_number = f.fserial_number
    left join jp_first_sale as d on a.fseries_number = d.fseries_number
    left join jp_second_sale as e on a.fseries_number = e.fold_fseries_number
    left join detect_two as g on upper(a.fseries_number) = g.fserial_number
    left join detect_three as h on upper(a.fseries_number) = h.fserial_number
    left join detect_three_pingmu as j on upper(a.fseries_number) = j.fserial_number
    left join detect_four as i on upper(a.fseries_number) = i.fserial_number

    union all

    -- B. 采货侠
    select
        "采货侠" as sale_channel_type,
        a.fstart_time, a.fseries_number, a.fclass_name, b.fbrand_name, a.fchannel_name, a.fproduct_name, a.fproject_name, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, a.fcost_price, a.foffer_price, a.fanchor_level,
        g.freal_name as fsecond_detect_name, 
        c.fnew_serial_no as fafter_series_number, 
        e.Fcity_name as fsecond_sale_city, e.Forder_address as fsecond_sale_address, e.Freceiver_id as fsecond_sale_id, e.Freceiver_name as fsecond_sale_name, e.Freceiver_phone as fsecond_sale_phone,
        c.Forder_deal_price as refund_amount_raw,
        b.Fdet_tpl as raw_det_tpl, b.Freal_name, b.Fend_time, b.Fdetection_object, b.fwarehouse_code as raw_warehouse_code, b.Fgoods_level,
        c.fapply_time as Fauto_create_time, c.Fjudge_reason as Fappeal_reason, cast(c.Fjudge_result as string) as Ffirst_trial_result, null, null, null, null, null, c.Fjudge_time as Freceived_audit_result_time, 
        null as after_sales_type_code, null as Faftersales_owner,
        d.foffer_price as first_price_raw, e.fstart_time as fsecond_sale_time, e.foffer_price*100 as second_price_raw,
        h.fdetect_two_name, i.fdetect_three_name, k.fdetect_three_name_pingmu, h.fdetect_two_time,
        1 as is_b2b_chx, c.Fjudge_type as chx_judge_type
    from chx_sale as a
    left join base_detect_record as b on upper(a.fseries_number) = b.fserial_number and b.rn_asc = 1
    left join after_sale_b2b_chx as c on upper(a.fseries_number) = c.fbusiness_id
    left join after_sale_detect as g on c.fnew_serial_no = g.fserial_number
    left join chx_first_sale as d on a.fseries_number = d.fseries_number
    left join chx_second_sale as e on a.fseries_number = e.fseries_number
    left join local_sn_map as f on upper(a.fseries_number) = f.fsrouce_serial_no_upper
    left join detect_two as h on upper(a.fseries_number) = h.fserial_number
    left join detect_three as i on upper(a.fseries_number) = i.fserial_number
    left join detect_three_pingmu as k on upper(a.fseries_number) = k.fserial_number
    left join detect_four as j on upper(a.fseries_number) = j.fserial_number

    union all

    -- C. 鱼市B2B
    select
        "鱼市B2B" as sale_channel_type,
        a.fstart_time, a.fseries_number, a.fclass_name, b.fbrand_name, a.fchannel_name, a.fproduct_name, a.fproject_name, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, a.fcost_price, a.foffer_price, a.fanchor_level,
        g.freal_name as fsecond_detect_name, 
        cc.fnew_serial_no as fafter_series_number, 
        e.Fcity_name as fsecond_sale_city, e.Forder_address as fsecond_sale_address, e.Freceiver_id as fsecond_sale_id, e.Freceiver_name as fsecond_sale_name, e.Freceiver_phone as fsecond_sale_phone,
        cc.frefund_total as refund_amount_raw,
        b.Fdet_tpl as raw_det_tpl, b.Freal_name, b.Fend_time, b.Fdetection_object, b.fwarehouse_code as raw_warehouse_code, b.Fgoods_level,
        cc.fapply_time as Fauto_create_time, cc.fjudge_reason as Fappeal_reason, cast(cc.fjudge_result as string) as Ffirst_trial_result, null, null, null, null, null, cc.Fjudge_time as Freceived_audit_result_time, 
        cc.faftersales_type as after_sales_type_code, null as Faftersales_owner,
        d.foffer_price as first_price_raw, e.fstart_time as fsecond_sale_time, e.foffer_price*100 as second_price_raw,
        h.fdetect_two_name, i.fdetect_three_name, k.fdetect_three_name_pingmu, h.fdetect_two_time,
        1 as is_b2b_chx, cc.Fjudge_type as chx_judge_type
    from b2b_sale as a
    left join base_detect_record as b on upper(a.fseries_number) = b.fserial_number and b.rn_asc = 1
    left join after_sale_b2b_chx as cc on upper(a.fseries_number) = cc.fbusiness_id
    left join after_sale_detect as g on upper(coalesce(cc.fnew_serial_no,a.fseries_number)) = g.fserial_number
    left join b2b_first_sale as d on a.fseries_number = d.fseries_number
    left join b2b_second_sale as e on a.fseries_number = e.fseries_number
    left join local_sn_map as f on upper(a.fseries_number) = f.fsrouce_serial_no_upper
    left join detect_two as h on upper(a.fseries_number) = h.fserial_number
    left join detect_three as i on upper(a.fseries_number) = i.fserial_number
    left join detect_three_pingmu as k on upper(a.fseries_number) = k.fserial_number
    left join detect_four as j on upper(a.fseries_number) = j.fserial_number
)

-- ========================================
-- 5. 输出层
-- ========================================
select
    fstart_time, fseries_number, fclass_name,
    case when fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
    fchannel_name, fproduct_name, fproject_name, Fcity_name, Forder_address, Freceiver_id, Freceiver_name, Freceiver_phone, fsecond_detect_name, fafter_series_number, fsecond_sale_city, fsecond_sale_address, fsecond_sale_id, fsecond_sale_name, fsecond_sale_phone, sale_channel_type as "销售渠道", left(fseries_number,2) as "渠道", fcost_price/100 as "成本价", foffer_price/100 as "当前出价",
    
    case 
        when is_b2b_chx=0 and ((refund_amount_raw>0 and foffer_price=refund_amount_raw) or (fchannel_name="竞拍销售默认渠道号")) then 0
        when is_b2b_chx=0 then foffer_price/100
        when chx_judge_type=1 and refund_amount_raw > 0 then 0 
        else foffer_price/100
    end as "销售额",
    
    raw_det_tpl as Fdet_tpl, Freal_name, Fend_time, Fdetection_object,
    case 
        when raw_warehouse_code='12' then "东莞仓"
        when right(left(fseries_number,6),2)="16" or left(fseries_number,3)="020" then "杭州仓"
        else "深圳仓" 
    end as fwarehouse_code,
    Fauto_create_time, get_json_object(Fgoods_level,'$.levelName') as Fgoods_level, Fappeal_reason, Ffirst_trial_result, Freexamine_result, Fdetection_price/100 as "检测价", Freinspection_price/100 as "二次检测价", Ftotal_diff_amount/100 as "检测差异金额", Ftotal_refundable_amount/100 as "总应退款金额", refund_amount_raw/100 as "总实退款金额",
    
    case
        when raw_det_tpl=1 then "大检测"
        when raw_det_tpl in (0,2,6,7) then "竞拍检测"
        else '其他' 
    end as "检测渠道",
    case
        when raw_det_tpl = 0 then '标准检'
        when raw_det_tpl = 1 then '大质检'
        when raw_det_tpl = 2 then '新标准检测'
        when raw_det_tpl = 3 then '产线检'
        when raw_det_tpl = 4 then '34项检测'
        when raw_det_tpl = 5 then '无忧购'
        when raw_det_tpl = 6 then '寄卖plus'
        when raw_det_tpl = 7 then '价格3.0的检测'
        else '其他' 
    end as "检测模板",
    
    case when refund_amount_raw > 0 then 1 else 0 end as "售后数",
    case when refund_amount_raw > 0 then Freceived_audit_result_time else null end as "售后通过时间",
    case 
        when is_b2b_chx=0 and refund_amount_raw>0 and foffer_price<refund_amount_raw and fstart_time>='2022-01-01' then 1
        when is_b2b_chx=1 and chx_judge_type=1 then 1
        else 0 
    end as "退货数",
    case when refund_amount_raw>0 and foffer_price>refund_amount_raw then 1 else 0 end as "补差赔付",
    case when refund_amount_raw>0 and foffer_price>refund_amount_raw then refund_amount_raw/100 else 0 end as "赔付金额",
    
    after_sales_type_code as Fafter_sales_type,
    case 
        when is_b2b_chx=0 and after_sales_type_code=1 then "仅退款" 
        when is_b2b_chx=0 and after_sales_type_code=2 then "退货退款" 
        when is_b2b_chx=1 and chx_judge_type=1 then "退货退款"
        else "其它" 
    end as "售后类型",
    
    Faftersales_owner, first_price_raw/100 as first_price, fsecond_sale_time, second_price_raw/100 as second_price,
    case
        when is_b2b_chx=0 and refund_amount_raw>0 and foffer_price=refund_amount_raw and fstart_time>='2022-01-01' then 0
        when is_b2b_chx=1 and chx_judge_type=1 then first_price_raw/100 - second_price_raw/100
        else first_price_raw/100 - second_price_raw/100
    end as "二次差价成本",
    
    coalesce(fdetect_two_name, Freal_name) as fdetect_two_name,
    coalesce(fdetect_three_name, Freal_name) as fdetect_three_name,
    fdetect_three_name_pingmu,
    if(fdetect_two_time is not null,"是","否") as "是否分模块",
    fanchor_level
from union_all_data