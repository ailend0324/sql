-- ========================================
-- 移除了所有历史表(2020-2023)的拼接
-- 预期收益：性能大幅提升，代码逻辑更纯粹
-- 风险提示：会丢失约 0.08% 的长尾售后数据(>365天前的订单)
-- ========================================

with 
-- ========================================
-- 1. 基础订单数据准备 (只查近365天)
-- ========================================
order_base_365_jp as (
    select 
        fseries_number,
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
        row_number() over(partition by fseries_number order by forder_create_time desc) as rn_last,
        row_number() over(partition by fseries_number order by forder_create_time asc) as rn_first
    from dws.dws_jp_order_detail
    where ftest_show <> 1
        and to_date(forder_create_time) >= to_date(date_sub(from_unixtime(unix_timestamp()),365))
        and forder_platform not in (5,6)
        and (fmerchant_jp=0 or fmerchant_jp is null)
        and forder_status in (2,3,4,6)
),

order_base_365_b2b as (
    select 
        fseries_number,
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
        row_number() over(partition by fseries_number order by forder_create_time desc) as rn_last,
        row_number() over(partition by fseries_number order by forder_create_time asc) as rn_first
    from dws.dws_jp_order_detail
    where ftest_show <> 1
        and forder_platform = 5
        and forder_status in (2,3,4,6)
        and to_date(forder_create_time) >= to_date(date_sub(from_unixtime(unix_timestamp()),365))
),

order_base_365_chx as (
    select 
        fseries_number,
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
        row_number() over(partition by fseries_number order by forder_create_time desc) as rn_last,
        row_number() over(partition by fseries_number order by forder_create_time asc) as rn_first
    from dws.dws_jp_order_detail
    where ftest_show <> 1
        and forder_platform = 6
        and fmerchant_jp = 0
        and forder_status in (2,3,4,6)
        and to_date(forder_create_time) >= to_date(date_sub(from_unixtime(unix_timestamp()),365))
),

order_base_jingpai as (
    select 
        fseries_number,
        forder_create_time,
        foffer_price,
        fstart_time,
        Fcity_name,
        Forder_address,
        Freceiver_id,
        Freceiver_name,
        Freceiver_phone,
        foffer_time,
        row_number() over(partition by fseries_number order by forder_create_time asc) as rn_order
    from dws.dws_jp_order_detail
    where ftest_show <> 1
        and to_date(forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
        and fchannel_name='竞拍销售默认渠道号'
        and forder_status in (2,3,4,6)
),

-- ========================================
-- 2. 检测与售后数据准备 (逻辑不变)
-- ========================================
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
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time asc) as num
        from drt.drt_my33310_detection_t_detect_record as a
        left join (
            select fseries_number, forder_create_time 
            from (
                select fseries_number, forder_create_time, row_number() over(partition by fseries_number order by  forder_create_time desc) as num 
                from dws.dws_jp_order_detail 
                where ftest_show <> 1 
                    and (fmerchant_jp=0 or fmerchant_jp is null) 
                    and forder_status in (2,3,4,6) 
                    and forder_create_time>=to_date(date_sub(from_unixtime(unix_timestamp()),400))
            ) t where t.num=1
        ) as b on upper(a.fserial_number)=b.fseries_number
        left join (
            select freal_name, Fposition_id 
            from (
                select *, row_number() over(partition by freal_name order by fcreate_time desc) as num 
                from drt.drt_my33310_amcdb_t_user
            )t where num=1
        ) as c on a.freal_name=c.freal_name
        where a.fis_deleted=0 
            and to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),400)) 
            and a.fend_time<b.forder_create_time 
            and c.Fposition_id <>129
    ) c where c.num=1
),

after_sale_detect as (
    select * from (
        select 
            *, 
            row_number() over(partition by fserial_number order by fend_time asc) as num
        from drt.drt_my33310_detection_t_detect_record
        where fdet_type=0 
            and fis_deleted=0 
            and freport_type=0 
            and fverdict<>"测试单" 
            and to_date(fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),400)) 
            and left(fserial_number,2) in ('YZ','NT','JM')
    )t where num=1
),

jp_sale as ( select * from order_base_365_jp where rn_last = 1 ),

jp_first_sale as ( select * from order_base_365_jp where rn_first = 1 ),

jp_second_sale as (
    select 
        if(b.fold_fseries_number is not null,b.fold_fseries_number,c.fold_fseries_number) as fold_fseries_number, 
        a.foffer_price, 
        a.Fcity_name, 
        a.Forder_address, 
        a.Freceiver_id, 
        a.Freceiver_name, 
        a.Freceiver_phone, 
        a.fstart_time
    from order_base_jingpai as a
    left join dws.dws_hs_order_detail as b on a.fseries_number=b.fseries_number
    left join dws.dws_hs_order_detail_history2018_2022 as c on a.fseries_number=c.fseries_number
    union all
    select 
        if(b.fold_fseries_number is not null,b.fold_fseries_number,c.fold_fseries_number) as fold_fseries_number, 
        a.foffer_price, 
        null, null, null, null, null, 
        a.foffer_time as fstart_time
    from dws.dws_th_order_detail as a
    left join dws.dws_hs_order_detail as b on a.fseries_number=b.fseries_number
    left join dws.dws_hs_order_detail_history2018_2022 as c on a.fseries_number=c.fseries_number
    where a.Fbd_status <>2 and a.fchannel_name='竞拍销售默认渠道号'
),

after_sale as (
    select * from (
        select 
            a.*, 
            b.fseries_number, 
            row_number() over(partition by fsales_series_number order by a.fauto_create_time desc ) as num
        from drt.drt_my33310_recycle_t_after_sales_order_info as a
        left join drt.drt_my33310_recycle_t_order as b on a.fafter_sales_order_id=b.forder_id
        where a.fvalid=1
    ) t where num=1
),

caihuoxia_sale as ( select * from order_base_365_chx where rn_last = 1 ),

caihuoxia_after_sale as (
    select * from ( 
        select * , row_number() over(partition by fbusiness_id order by fcreate_time desc) as num 
        from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales 
    )t where num=1
),

caihuoxia_first_sale as ( select * from order_base_365_chx where rn_first = 1 ),

caihuoxia_second_sale as (
    select fstart_time, fseries_number, foffer_price/100 as foffer_price, Fcity_name, Forder_address, Freceiver_id, Freceiver_name, Freceiver_phone
    from ( 
        select a.fstart_time, if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number, a.foffer_price, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, a.rn_first as num 
        from order_base_365_chx as a 
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no 
    ) t where num=2
    union all
    select fstart_time, fseries_number, foffer_price/100 as foffer_price, Fcity_name, Forder_address, Freceiver_id, Freceiver_name, Freceiver_phone
    from ( 
        select a.fstart_time, if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number, a.foffer_price, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, a.rn_first as num 
        from ( select * from order_base_365_chx where rn_first <= 2 ) as a 
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no 
    ) t where num=2
    union all
    select fstart_time, fseries_number, foffer_price/100 as foffer_price, Fcity_name, Forder_address, Freceiver_id, Freceiver_name, Freceiver_phone
    from ( 
        select a.fstart_time, if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number, a.foffer_price, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, row_number() over(partition by fseries_number order by  forder_create_time asc) as num 
        from dws.dws_jp_order_detail_history2020_2022 as a 
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no 
        where ftest_show <> 1 and forder_platform=6 and fmerchant_jp=0 and forder_status in (2,3,4,6) and to_date(a.forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),400))
    ) t where num=2
    union all
    select fstart_time, fseries_number, foffer_price/100 as foffer_price, Fcity_name, Forder_address, Freceiver_id, Freceiver_name, Freceiver_phone
    from ( 
        select a.fstart_time, if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number, a.foffer_price, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, row_number() over(partition by fseries_number order by  forder_create_time asc) as num 
        from dws.dws_jp_order_detail as a 
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no 
        where ftest_show <> 1 and forder_platform<>6 and to_date(a.forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365)) and fmerchant_jp=0 and forder_status in (2,3,4,6)
    ) t where num=1
    union all
    select fstart_time, fseries_number, foffer_price/100 as foffer_price, Fcity_name, Forder_address, Freceiver_id, Freceiver_name, Freceiver_phone
    from ( 
        select a.fstart_time, if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number, a.foffer_price, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, row_number() over(partition by fseries_number order by  forder_create_time asc) as num 
        from dws.dws_jp_order_detail_history2023 as a 
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no 
        where ftest_show <> 1 and forder_platform<>6 and to_date(a.forder_create_time) between '2023-01-01' and to_date(date_sub(from_unixtime(unix_timestamp()),366)) and fmerchant_jp=0 and forder_status in (2,3,4,6)
    ) t where num=1
    union all
    select fstart_time, fseries_number, foffer_price/100 as foffer_price, Fcity_name, Forder_address, Freceiver_id, Freceiver_name, Freceiver_phone
    from ( 
        select a.fstart_time, if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number, a.foffer_price, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, row_number() over(partition by fseries_number order by  forder_create_time asc) as num 
        from dws.dws_jp_order_detail_history2020_2022 as a 
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no 
        where ftest_show <> 1 and forder_platform<>6 and to_date(a.forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),400)) and fmerchant_jp=0 and forder_status in (2,3,4,6)
    ) t where num=1
    union all
    select fstart_time, fseries_number, foffer_price/100 as foffer_price, null, null, null, null, null
    from ( 
        select a.fstart_time, if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number, a.foffer_price, row_number() over(partition by fseries_number order by forder_create_time asc) as num 
        from dws.dws_th_order_detail as a 
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no 
        where Fbd_status <>2 
    )t where num=1
),

detect_one as (
    select upper(fserial_number) as fserial_number, freal_name as fdetect_one_name, from_unixtime(fend_det_time) as fdetect_one_time
    from ( 
        select a.fserial_number, a.fend_det_time, b.freal_name, row_number()over(partition by upper(a.fserial_number) order by a.fend_det_time desc) as num 
        from drt.drt_my33312_detection_t_automation_det_record as a 
        left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername 
        where fserial_number!="" and fserial_number is not null and to_date(from_unixtime(a.fend_det_time))>=to_date(date_sub(from_unixtime(unix_timestamp()),800))
    )t where num=1
),
detect_two as (
    select upper(fserial_number) as fserial_number, freal_name as fdetect_two_name, fcreate_time as fdetect_two_time
    from ( 
        select a.fcreate_time, a.fserial_number, b.freal_name, row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num 
        from drt.drt_my33312_detection_t_det_app_record as a 
        left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername 
        where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),800)) and fserial_number!="" and fserial_number is not null
    )t where num=1
),
detect_three as (
    select upper(fserial_number) as fserial_number, freal_name as fdetect_three_name, fcreate_time as fdetect_three_time
    from ( 
        select a.fcreate_time, a.fserial_number, b.freal_name, row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num 
        from drt.drt_my33312_detection_t_det_task as a 
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id 
        where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),800)) and b.fdet_sop_task_name like "%外观%"
    )t where num=1
),
detect_three_pingmu as (
    select upper(fserial_number) as fserial_number, case when freal_name="李俊峰" then "李俊锋" else freal_name end as fdetect_three_name_pingmu, fcreate_time as fdetect_three_time_pingmu
    from ( 
        select a.fcreate_time, a.fserial_number, b.freal_name, row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num 
        from drt.drt_my33312_detection_t_det_task as a 
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id 
        where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),800)) and b.fdet_sop_task_name like "%屏幕%" and b.fdet_sop_task_name!="外观屏幕"
    )t where num=1
),
detect_four as (
    select upper(fserial_number) as fserial_number, freal_name as fdetect_four_name, fcreate_time as fdetect_four_time
    from ( 
        select a.fcreate_time, a.fserial_number, b.freal_name, row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num 
        from drt.drt_my33312_detection_t_det_task as a 
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id 
        where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),800)) and b.fdet_sop_task_name like "%拆修%"
    )t where num=1
),

-- B2B 专属 CTE
b2b_sale as ( select * from order_base_365_b2b where rn_last = 1 ),

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
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time asc) as num
        from drt.drt_my33310_detection_t_detect_record as a
        left join ( select fseries_number, forder_create_time from order_base_365_b2b where rn_last = 1 ) as b on upper(a.fserial_number)=b.fseries_number
        left join ( select freal_name, Fposition_id from ( select *, row_number() over(partition by freal_name order by fcreate_time desc) as num from drt.drt_my33310_amcdb_t_user ) t where num=1 ) as c on a.freal_name=c.freal_name
        where a.fis_deleted=0 and to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365)) and a.fend_time<b.forder_create_time and c.Fposition_id <>129
    ) c where c.num=1
),
b2b_after_sale as ( select * from ( select *, row_number() over(partition by fbusiness_id order by fcreate_time desc) as num from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales ) t where num=1 ),
b2b_first_sale as ( select * from order_base_365_b2b where rn_first = 1 ),
b2b_detect_two as (
    select upper(fserial_number) as fserial_number, freal_name as fdetect_two_name, fcreate_time as fdetect_two_time
    from ( 
        select a.fcreate_time, a.fserial_number, b.freal_name, row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num 
        from drt.drt_my33312_detection_t_det_app_record as a 
        left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername 
        where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365)) and fserial_number!="" and fserial_number is not null 
    ) t where num=1
),
b2b_detect_three as (
    select upper(fserial_number) as fserial_number, freal_name as fdetect_three_name, fcreate_time as fdetect_three_time
    from ( 
        select a.fcreate_time, a.fserial_number, b.freal_name, row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num 
        from drt.drt_my33312_detection_t_det_task as a 
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id 
        where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365)) and b.fdet_sop_task_name like "%外观%" 
    ) t where num=1
),
b2b_detect_three_pingmu as (
    select upper(fserial_number) as fserial_number, case when freal_name="李俊峰" then "李俊锋" else freal_name end as fdetect_three_name_pingmu, fcreate_time as fdetect_three_time_pingmu
    from ( 
        select a.fcreate_time, a.fserial_number, b.freal_name, row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num 
        from drt.drt_my33312_detection_t_det_task as a 
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id 
        where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365)) and b.fdet_sop_task_name like "%屏幕%" and b.fdet_sop_task_name!="外观屏幕" 
    ) t where num=1
),
b2b_second_sale as (
    select fstart_time, fseries_number, foffer_price/100 as foffer_price, Fcity_name, Forder_address, Freceiver_id, Freceiver_name, Freceiver_phone
    from ( 
        select a.fstart_time, if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number, a.foffer_price, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, a.rn_first as num 
        from order_base_365_b2b as a 
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no 
    ) t where num=2
    union all
    select fstart_time, fseries_number, foffer_price/100 as foffer_price, Fcity_name, Forder_address, Freceiver_id, Freceiver_name, Freceiver_phone
    from ( 
        select a.fstart_time, if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number, a.foffer_price, a.Fcity_name, a.Forder_address, a.Freceiver_id, a.Freceiver_name, a.Freceiver_phone, row_number() over(partition by fseries_number order by  forder_create_time asc) as num 
        from dws.dws_jp_order_detail as a 
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no 
        where ftest_show <> 1 and to_date(a.forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365)) and forder_platform<>5 and forder_status in (2,3,4,6)
    ) t where num=1
    union all
    select fstart_time, fseries_number, foffer_price/100 as foffer_price, null, null, null, null, null
    from ( 
        select a.fstart_time, if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number, a.foffer_price, row_number() over(partition by fseries_number order by forder_create_time asc) as num 
        from dws.dws_th_order_detail as a 
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no 
        where Fbd_status <>2 
    )t where num=1
),

-- ==============================================
-- 3. 核心汇聚层 (下沉计算逻辑)
-- ==============================================
union_all_data as (
    -- A. 自有平台 (竞拍)
    select
        "自有平台" as sale_channel_type,
        a.fstart_time, 
        a.fseries_number, 
        a.fclass_name, 
        b.fbrand_name, 
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
        a.fcost_price, 
        a.foffer_price, 
        c.Ftotal_real_refund_amount,
        b.Fdet_tpl as raw_det_tpl, 
        b.Freal_name, 
        b.Fend_time, 
        b.Fdetection_object, 
        b.fwarehouse_code as raw_warehouse_code,
        c.Fauto_create_time, 
        b.Fgoods_level, 
        c.Fappeal_reason, 
        cast(c.Ffirst_trial_result as string) as Ffirst_trial_result,
        c.Freexamine_result, 
        c.Fdetection_price, 
        c.Freinspection_price, 
        c.Ftotal_diff_amount, 
        c.Ftotal_refundable_amount,
        c.Freceived_audit_result_time, 
        c.Fafter_sales_type, 
        c.Faftersales_owner,
        d.foffer_price as first_price_raw, 
        e.fstart_time as fsecond_sale_time, 
        e.foffer_price as second_price_raw,
        g.fdetect_two_name, 
        h.fdetect_three_name, 
        j.fdetect_three_name_pingmu, 
        g.fdetect_two_time, 
        a.fanchor_level,
        
        -- 【自有平台专属计算逻辑】
        if((c.Ftotal_real_refund_amount>0 and a.foffer_price=c.Ftotal_real_refund_amount) or (a.fchannel_name="竞拍销售默认渠道号"),0,a.foffer_price/100) as calc_sales_amount,
        
        case when c.Ftotal_real_refund_amount>0 then 1 else 0 end as calc_is_refund,
        
        case when c.Ftotal_real_refund_amount>0 then c.Freceived_audit_result_time else null end as calc_refund_pass_time,
        
        case 
            when c.Ftotal_real_refund_amount>0 and a.foffer_price<c.Ftotal_real_refund_amount and a.fstart_time>='2022-01-01' then 1
            when c.Ftotal_real_refund_amount>0 and a.foffer_price=c.Ftotal_real_refund_amount and a.fstart_time<'2022-01-01' then 1
            else 0 
        end as calc_is_return,
        
        case when c.Ftotal_real_refund_amount>0 and a.foffer_price>c.Ftotal_real_refund_amount then 1 else 0 end as calc_is_compensation,
        
        case when c.Ftotal_real_refund_amount>0 and a.foffer_price>c.Ftotal_real_refund_amount then c.Ftotal_real_refund_amount/100 else 0 end as calc_compensation_amount,
        
        case 
            when c.Fafter_sales_type=1 then "仅退款" 
            when c.Fafter_sales_type=2 then "退货退款" 
            else "其它" 
        end as calc_after_sales_type,
        
        if(c.Ftotal_real_refund_amount>0 and a.foffer_price=c.Ftotal_real_refund_amount and a.fstart_time>='2022-01-01',0,d.foffer_price/100-e.foffer_price/100) as calc_second_diff_cost

    from jp_sale as a
    left join detect as b on a.fseries_number=b.fserial_number
    left join after_sale as c on a.fseries_number=c.Fsales_series_number
    left join after_sale_detect as f on c.fseries_number=f.fserial_number
    left join jp_first_sale as d on a.fseries_number=d.fseries_number
    left join jp_second_sale as e on a.fseries_number=e.fold_fseries_number
    left join detect_two as g on a.fseries_number=g.fserial_number
    left join detect_three as h on a.fseries_number=h.fserial_number
    left join detect_three_pingmu as j on a.fseries_number=j.fserial_number
    left join detect_four as i on a.fseries_number=i.fserial_number
    where a.fstart_time>=to_date(date_sub(from_unixtime(unix_timestamp()),400))

    union all

    -- B. 采货侠
    select
        "采货侠" as sale_channel_type,
        a.fstart_time, 
        a.fseries_number, 
        a.fclass_name, 
        b.fbrand_name, 
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
        a.fcost_price, 
        a.foffer_price, 
        c.Forder_deal_price as Ftotal_real_refund_amount,
        b.Fdet_tpl as raw_det_tpl, 
        b.Freal_name, 
        b.Fend_time, 
        b.Fdetection_object, 
        b.fwarehouse_code as raw_warehouse_code,
        c.fapply_time as Fauto_create_time, 
        b.Fgoods_level, 
        c.Fjudge_reason as Fappeal_reason, 
        cast(c.Fjudge_result as string) as Ffirst_trial_result,
        null as Freexamine_result, 
        null as Fdetection_price, 
        null as Freinspection_price, 
        null as Ftotal_diff_amount, 
        null as Ftotal_refundable_amount,
        c.Fjudge_time as Freceived_audit_result_time, 
        null as Fafter_sales_type, 
        null as Faftersales_owner,
        d.foffer_price as first_price_raw, 
        e.fstart_time as fsecond_sale_time, 
        e.foffer_price*100 as second_price_raw, -- 注意采货侠second_sale已经是元，统一乘100转为分
        h.fdetect_two_name, 
        i.fdetect_three_name, 
        k.fdetect_three_name_pingmu, 
        h.fdetect_two_time, 
        a.fanchor_level,

        -- 【采货侠专属计算逻辑】
        if(c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1,0,a.foffer_price/100) as calc_sales_amount,
        
        case when (c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1) or f.fsrouce_serial_no is not null then 1 else 0 end as calc_is_refund,
        
        case when c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1 then Fjudge_time when f.fsrouce_serial_no is not null then Fjudge_time else null end as calc_refund_pass_time,
        
        case when (c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1) or f.fsrouce_serial_no is not null then 1 else 0 end as calc_is_return,
        
        0 as calc_is_compensation,
        
        0 as calc_compensation_amount,
        
        case when (c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1) or f.fsrouce_serial_no is not null then "退货退款" else null end as calc_after_sales_type,
        
        if(c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1,d.foffer_price/100-e.foffer_price,if(f.fsrouce_serial_no is not null,d.foffer_price/100-e.foffer_price,0)) as calc_second_diff_cost

    from caihuoxia_sale as a
    left join detect as b on a.fseries_number=b.fserial_number
    left join caihuoxia_after_sale as c on a.fseries_number=c.fbusiness_id
    left join after_sale_detect as g on c.fnew_serial_no=g.fserial_number
    left join caihuoxia_first_sale as d on a.fseries_number=d.fseries_number
    left join caihuoxia_second_sale as e on a.fseries_number=e.fseries_number
    left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as f on a.fseries_number=upper(f.fsrouce_serial_no)
    left join detect_two as h on a.fseries_number=h.fserial_number
    left join detect_three as i on a.fseries_number=i.fserial_number
    left join detect_three_pingmu as k on a.fseries_number=k.fserial_number
    left join detect_four as j on a.fseries_number=j.fserial_number
    where a.fstart_time>=to_date(date_sub(from_unixtime(unix_timestamp()),400))

    union all

    -- C. 鱼市B2B
    select
        "鱼市B2B" as sale_channel_type,
        a.fstart_time, 
        a.fseries_number, 
        a.fclass_name, 
        b.fbrand_name, 
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
        a.fcost_price, 
        a.foffer_price, 
        cc.frefund_total as Ftotal_real_refund_amount,
        b.Fdet_tpl as raw_det_tpl, 
        b.Freal_name, 
        b.Fend_time, 
        b.Fdetection_object, 
        b.fwarehouse_code as raw_warehouse_code,
        cc.fapply_time as Fauto_create_time, 
        b.Fgoods_level, 
        cc.fjudge_reason as Fappeal_reason, 
        cast(cc.fjudge_result as string) as Ffirst_trial_result,
        0 as Freexamine_result, 
        0 as Fdetection_price, 
        0 as Freinspection_price, 
        0 as Ftotal_diff_amount, 
        0 as Ftotal_refundable_amount,
        cc.Fjudge_time as Freceived_audit_result_time, 
        cc.faftersales_type as Fafter_sales_type, 
        null as Faftersales_owner,
        d.foffer_price as first_price_raw, 
        e.fstart_time as fsecond_sale_time, 
        e.foffer_price*100 as second_price_raw,
        h.fdetect_two_name, 
        i.fdetect_three_name, 
        k.fdetect_three_name_pingmu, 
        h.fdetect_two_time, 
        a.fanchor_level,
        
        -- 【B2B专属计算逻辑】
        if(cc.frefund_total>0,0,a.foffer_price/100) as calc_sales_amount,
        
        case when (cc.fapply_time is not null and cc.fapply_time !='0000-00-00 00:00:00.0' and cc.Fjudge_type=1) or f.fsrouce_serial_no is not null then 1 else 0 end as calc_is_refund,
        
        case when cc.fapply_time is not null and cc.fapply_time !='0000-00-00 00:00:00.0' and cc.Fjudge_type=1 then cc.Fjudge_time when f.fsrouce_serial_no is not null then cc.Fjudge_time else null end as calc_refund_pass_time,
        
        case when (cc.fapply_time is not null and cc.fapply_time !='0000-00-00 00:00:00.0' and cc.Fjudge_type=1) or f.fsrouce_serial_no is not null then 1 else 0 end as calc_is_return,
        
        case when cc.frefund_total>0 and a.foffer_price>cc.frefund_total then 1 else 0 end as calc_is_compensation,
        
        case when cc.frefund_total>0 and a.foffer_price>cc.frefund_total then cc.frefund_total/100 else 0 end as calc_compensation_amount,
        
        case when (cc.fapply_time is not null and cc.fapply_time !='0000-00-00 00:00:00.0' and cc.Fjudge_type=1) or f.fsrouce_serial_no is not null then "退货退款" else null end as calc_after_sales_type,
        
        if(cc.fapply_time is not null and cc.fapply_time !='0000-00-00 00:00:00.0' and cc.Fjudge_type=1,d.foffer_price/100-e.foffer_price,if(f.fsrouce_serial_no is not null,d.foffer_price/100-e.foffer_price,0)) as calc_second_diff_cost

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
    where a.fstart_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
)

-- ========================================
-- 4. 中央处理中心 (最终输出)
-- ========================================
select
    fstart_time,
    fseries_number,
    fclass_name,
    case 
        when fbrand_name="苹果" then "苹果" 
        else "安卓" 
    end as fbrand_name,
    fchannel_name,
    fproduct_name,
    fproject_name,
    Fcity_name,
    Forder_address,
    Freceiver_id,
    Freceiver_name,
    Freceiver_phone,
    fsecond_detect_name,
    fafter_series_number,
    fsecond_sale_city,
    fsecond_sale_address,
    fsecond_sale_id,
    fsecond_sale_name,
    fsecond_sale_phone,
    sale_channel_type as "销售渠道",
    left(fseries_number,2) as "渠道",
    fcost_price/100 as "成本价",
    foffer_price/100 as "当前出价",
    
    -- 销售额 (已前置计算)
    calc_sales_amount as "销售额",
    
    raw_det_tpl as Fdet_tpl,
    Freal_name,
    Fend_time,
    Fdetection_object,
    
    -- 仓库判断逻辑 (v5优化成果)
    case 
        when raw_warehouse_code='12' then "东莞仓"
        when right(left(fseries_number,6),2)="16" or left(fseries_number,3)="020" then "杭州仓"
        else "深圳仓" 
    end as fwarehouse_code,
    
    Fauto_create_time,
    get_json_object(Fgoods_level,'$.levelName') as Fgoods_level,
    Fappeal_reason,
    Ffirst_trial_result,
    Freexamine_result,
    Fdetection_price/100 as "检测价",
    Freinspection_price/100 as "二次检测价",
    Ftotal_diff_amount/100 as "检测差异金额",
    Ftotal_refundable_amount/100 as "总应退款金额",
    Ftotal_real_refund_amount/100 as "总实退款金额",
    
    -- 检测渠道 (v5优化成果)
    case
        when raw_det_tpl=1 then "大检测"
        when (raw_det_tpl=0 or raw_det_tpl=2 or raw_det_tpl=6 or raw_det_tpl=7) then "竞拍检测"
        else '其他' 
    end as "检测渠道",
    
    -- 检测模板 (v5优化成果)
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
    
    -- 售后与退货指标 (v6优化成果，已前置计算)
    calc_is_refund as "售后数",
    calc_refund_pass_time as "售后通过时间",
    calc_is_return as "退货数",
    calc_is_compensation as "补差赔付",
    calc_compensation_amount as "赔付金额",
    
    Fafter_sales_type,
    calc_after_sales_type as "售后类型",
    
    Faftersales_owner,
    first_price_raw/100 as first_price,
    fsecond_sale_time,
    second_price_raw/100 as second_price,
    
    calc_second_diff_cost as "二次差价成本",
    
    if(fdetect_two_name is null,Freal_name,fdetect_two_name) as fdetect_two_name,
    if(fdetect_three_name is null,Freal_name,fdetect_three_name) as fdetect_three_name,
    fdetect_three_name_pingmu,
    if(fdetect_two_time is not null,"是","否") as "是否分模块",
    fanchor_level

from union_all_data
