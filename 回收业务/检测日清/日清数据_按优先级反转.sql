-- =========================
-- 收货日清数据 - 优化重构版 (方案二：按优先级反转)
-- 说明：
-- 1) 统一时间配置参数，从 2024-01-01 开始
-- 2) 合并当前表与历史备份表，通过 row_number() 去重
-- 3) 优先级反转：将冷归档、已回填修正的历史表设为高优先级，把可能发生状态字段丢失的当前表设为低优先级
-- =========================

-- =========================
-- 统一配置参数
-- =========================
with time_config as (
    select 
        '2024-01-01' as default_window  -- 扩展到历史数据起始日期
),

-- =========================
-- 收货明细：当前表 + 历史备份表，按订单去重（历史表优先）
-- =========================
hs_order_detail_raw as (
    -- 2024历史表：数据最完整，作为首选源，优先级最高 (source_priority = 1)
    select 1 as source_priority, forder_id, fdetect_time, fpid, fpid_name, Fchannel_id, fchannel_name, Fuser_group_id, Fuser_group_name, Fproject_id, Fproject_name, Fclass_id, Fclass_name, forder_status_name, Fchecker_name, fbrand_name, fseries_number, fsender_phone, ftest, fgetin_time, forder_status
    from dws.dws_hs_order_detail_history2024
    
    union all
    
    -- 2025历史表：数据较完整，优先级中等 (source_priority = 2)
    select 2 as source_priority, forder_id, fdetect_time, fpid, fpid_name, Fchannel_id, fchannel_name, Fuser_group_id, Fuser_group_name, Fproject_id, Fproject_name, Fclass_id, Fclass_name, forder_status_name, Fchecker_name, fbrand_name, fseries_number, fsender_phone, ftest, fgetin_time, forder_status
    from dws.dws_hs_order_detail_history2025
    
    union all
    
    -- 当前表：作为2026年及最新未归档数据的补充，老数据可能缺失状态，优先级最低 (source_priority = 3)
    select 3 as source_priority, forder_id, fdetect_time, fpid, fpid_name, Fchannel_id, fchannel_name, Fuser_group_id, Fuser_group_name, Fproject_id, Fproject_name, Fclass_id, Fclass_name, forder_status_name, Fchecker_name, fbrand_name, fseries_number, fsender_phone, ftest, fgetin_time, forder_status
    from dws.dws_hs_order_detail
),
hs_order_detail as (
    select forder_id, fdetect_time, fpid, fpid_name, Fchannel_id, fchannel_name, Fuser_group_id, Fuser_group_name, Fproject_id, Fproject_name, Fclass_id, Fclass_name, forder_status_name, Fchecker_name, fbrand_name, fseries_number, fsender_phone, ftest, fgetin_time, forder_status
    from (
        select
            *,
            row_number() over(partition by forder_id order by source_priority) as rn
        from hs_order_detail_raw
    ) t
    where rn = 1
),

-- =========================
-- 问密工单数据
-- =========================
wenmi as (
    select 
        *
    from (
        select 
            from_unixtime(a.Fupdate_time) as Fupdate_time,
            from_unixtime(a.flast_time) as Flast_time,
            from_unixtime(a.Fadd_time) as Fadd_time,
            from_unixtime(a.fcompletion_time) as fcompletion_time,
            a.fbarcode_sn,
            a.fupdate_user,
            a.fwork_status,
            from_unixtime(b.ffeedback_time) as ffeedback_time, 
            row_number() over(partition by a.fbarcode_sn order by a.Fadd_time desc) as num
        from drt.drt_my33310_csrdb_t_works as a
        left join drt.drt_my33310_csrdb_t_work_device_pwd_consulting as b on a.fid=b.fwork_id
        where a.fwork_type=4
        and from_unixtime(a.Fadd_time) >= to_date((select default_window from time_config))
        and a.fwork_source<>3
        and a.fappeal_type1<>0
        and a.fduty_content not like "%无效工单%"
    ) t 
    where num=1
),

-- =========================
-- 工单备注数据
-- =========================
gongdan_beizhu as (
    select 
        *
    from (
        select
            *,
            row_number() over(partition by fwork_id order by Fadd_time desc) as num
        from drt.drt_my33310_csrdb_t_work_device_pwd_consulting 
    ) t 
    where num=1
),

-- =========================
-- 工单补偿金额明细
-- =========================
gongdan_buchang as (
    select 
        *
    from (
        select
            a.fbarcode_sn, 
            from_unixtime(a.fadd_time) as fadd_time,
            c.fmoney,
            d.Ftype_str,
            d.Ffeedback_str,
            row_number() over(partition by a.fbarcode_sn order by a.fadd_time desc) as num
        from drt.drt_my33310_csrdb_t_works as a
        left join drt.drt_my33310_csrdb_t_works_config_appeal as b on a.fappeal_type2=b.fid
        left join drt.drt_my33310_csrdb_t_works_compensation as c on a.fid=c.fwork_id
        left join gongdan_beizhu as d on a.fid=d.fwork_id
        where from_unixtime(a.Fadd_time) >= to_date((select default_window from time_config))
        and a.fwork_type in(1,2,3,4)
        and a.fappeal_type1<>0
        and a.fduty_content not like "%无效工单%"
    ) t 
    where num=1
),

-- =========================
-- 外呼数据
-- =========================
waihu as (
    select 
        fcustomer_number,
        min(from_unixtime(cast(fstart_time as int))) as ffirst_call_time,
        count(*) as fcall_num
    from ods.ods_kf_tianrun_describe_cdr_ob 
    where fdate >= to_date((select default_window from time_config))
    and fclient_name in ("杨香英","钟小慧","黄丽萍","田新月","赵婷婷","龚娟","严俊",'郑静敏',"陈仪","陈佳娜")
    group by 1
),

-- =========================
-- 主查询：当前数据
-- =========================
current_data as (
    SELECT
        from_unixtime(unix_timestamp(a.fgetin_time),'yyyy-MM-dd') AS Ftime_byday,
        a.forder_id,
        a.fdetect_time,
        a.fpid,
        a.fpid_name,
        a.Fchannel_id,
        a.Fchannel_name,
        a.Fuser_group_id,
        a.Fuser_group_name,
        a.Fproject_id,
        Fproject_name,
        a.Fclass_id,
        a.Fclass_name,
        '回收' AS Frecycle_alltype,
        a.forder_status_name,
        a.Fchecker_name,
        a.fbrand_name,
        d.Fadd_time,
        d.Flast_time,
        d.Fupdate_time,
        d.fwork_status,
        LEFT(a.fseries_number,2) as fn,
        a.fseries_number,
        c.fwarehouse_number,
        e.fmoney,
        e.Ftype_str,
        e.Ffeedback_str,
        d.ffeedback_time,
        d.fcompletion_time,
        f.ffirst_call_time,
        1 AS getinnum,
        
        -- 时效计算字段
        case 
            when e.Ffeedback_str="已填密码" and d.fwork_status=40 and d.Fadd_time is not null and d.ffeedback_time is not null 
            and (unix_timestamp(d.ffeedback_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(d.Fadd_time,'yyyy-MM-dd HH:mm:ss'))>0 
            then (unix_timestamp(d.ffeedback_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(d.Fadd_time,'yyyy-MM-dd HH:mm:ss'))/3600 
            else null 
        end as `创建工单-用户反馈时效(已填短信)`,
        
        case 
            when e.Ffeedback_str="已填密码" and d.fwork_status=40 and d.ffeedback_time is not null and d.Flast_time is not null 
            and (unix_timestamp(d.fcompletion_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(d.ffeedback_time,'yyyy-MM-dd HH:mm:ss'))>0 
            then (unix_timestamp(d.fcompletion_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(d.ffeedback_time,'yyyy-MM-dd HH:mm:ss'))/3600 
            else null 
        end as `用户反馈-完结工单时效(已填短信)`,
        
        case 
            when e.Ffeedback_str="已填密码" and a.fdetect_time is not null 
            and (unix_timestamp(a.fdetect_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(d.ffeedback_time,'yyyy-MM-dd HH:mm:ss'))>0 
            then (unix_timestamp(a.fdetect_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(d.ffeedback_time,'yyyy-MM-dd HH:mm:ss'))/3600 
            else null 
        end as `问密检测时效`,
        
        case 
            when a.fdetect_time is not null 
            and (unix_timestamp(a.fdetect_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(a.fgetin_time,'yyyy-MM-dd HH:mm:ss'))>0 
            then (unix_timestamp(a.fdetect_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(a.fgetin_time,'yyyy-MM-dd HH:mm:ss'))/3600 
            else null 
        end as `到货检测时效`,
        
        -- 业务逻辑字段
        IF(a.fdetect_time is null and (a.forder_status_name="待退货" or a.forder_status_name="已退货" or a.forder_status_name="已取消") 
           and ((e.Ffeedback_str!="环保处理" and e.Ftype_str!="公益机退还" and e.Ftype_str!="山寨机退还") 
           or((e.Ftype_str="公益机退还" and e.Ffeedback_str="退回") or (e.Ftype_str="山寨机退还" and e.Ffeedback_str="退回") or e.Ftype_str is null)),1,null) as beforedetect_refund,
        
        IF (from_unixtime(unix_timestamp(a.fgetin_time),'yyyy-MM-dd') = from_unixtime(unix_timestamp(a.fdetect_time),'yyyy-MM-dd'),1,NULL) AS todaydetect,
        
        IF ((a.forder_status = 80 or a.forder_status_name="待退货" or a.forder_status_name="已退货") AND a.fdetect_time IS NULL and e.fmoney is null 
            and ((e.Ffeedback_str!="环保处理" and e.Ftype_str!="公益机退还" and e.Ftype_str!="山寨机退还") 
            or((e.Ftype_str="公益机退还" and e.Ffeedback_str="退回") or (e.Ftype_str="山寨机退还" and e.Ffeedback_str="退回") or e.Ftype_str is null)),1,NULL) AS noreasonreturn,
        
        if((a.forder_status = 80 or a.forder_status_name="待退货" or a.forder_status_name="已退货") AND a.fdetect_time IS NULL 
           and (e.fmoney is not null or ((e.Ftype_str="公益机退还" and e.Ffeedback_str!="%退回%") and (e.Ftype_str="山寨机退还" and e.Ffeedback_str!="退回")) or e.Ffeedback_str="环保处理"),1,NULL) as huanbaohuishou,
        
        -- 当天问密逻辑
        if(from_unixtime(unix_timestamp(a.fgetin_time),'yyyy-MM-dd') = from_unixtime(unix_timestamp(a.fdetect_time),'yyyy-MM-dd'),null,
           IF (d.Fupdate_time is not null and to_date(a.fgetin_time)=to_date(d.Fadd_time) 
               and from_unixtime(unix_timestamp(a.fgetin_time),'yyyy-MM-dd') = from_unixtime(unix_timestamp(d.Fupdate_time),'yyyy-MM-dd') 
               and d.fwork_status=40 and a.forder_status != 80,null,
               if(d.Fadd_time is null,null,
                  if(d.Fadd_time is not null and a.forder_status_name!="待退货" and a.forder_status_name!="已退货" and a.forder_status!= 80,1,null)))) AS nottodaywenmi,
        
        -- 外呼时效计算
        case 
            when d.ffeedback_time>'2023-01-01' and f.ffirst_call_time is not null and f.ffirst_call_time>=d.ffeedback_time 
            then (unix_timestamp(f.ffirst_call_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(d.ffeedback_time,'yyyy-MM-dd HH:mm:ss'))/3600 
            else null 
        end as `提供密码-反馈用户时效`,
        
        case 
            when d.ffeedback_time>'2023-01-01' and f.ffirst_call_time is not null and f.ffirst_call_time>=d.ffeedback_time then "否" 
            when d.ffeedback_time>'2023-01-01' and f.ffirst_call_time is not null and f.ffirst_call_time<d.ffeedback_time then "无法判断"
            when d.ffeedback_time is null or d.ffeedback_time<'2023-01-01' then null 
            else "is_correct" 
        end as `提供密码是否正确`
        
    FROM hs_order_detail a
    left join dws.dws_instock_details as c on a.fseries_number=c.fseries_number
    left join wenmi as d on a.fseries_number=d.fbarcode_sn
    left join gongdan_buchang as e on a.fseries_number=e.fbarcode_sn
    left join waihu as f on a.fsender_phone=f.fcustomer_number
    WHERE a.ftest = 0
    and a.fchannel_name not like "%帮卖%"
    AND a.fgetin_time IS NOT NULL
    and from_unixtime(unix_timestamp(a.fgetin_time),'yyyy-MM-dd') >= to_date((select default_window from time_config))
)

-- =========================
-- 执行主查询 (明细输出)
-- =========================
select * from current_data;
