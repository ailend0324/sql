-- 回收宝QC明细查询 - 抽检差异项明细分析
-- 功能：对比一检和抽检结果，找出检测差异，用于质量控制分析

-- 抽检差异项明细分析查询
with first_detect as(
    -- CTE1: 提取一检（首次检测）的详细信息
    select 
        a.fend_time,                    -- 检测结束时间
        a.fdetect_record_id,            -- 检测记录ID（主键）
        a.fserial_number,               -- 设备序列号（业务主键）
        a.fclass_name,                  -- 产品类别名称（如手机、平板等）
        a.fwarehouse_code,              -- 仓库编码（12=东莞仓等）
        b.fissue_id,                    -- 检测问题ID（如外观、屏幕等问题的编号）
        b.fissue_name,                  -- 检测问题名称（如"外观划痕"、"屏幕显示"等）
        b.fanswer_id as fresult_id,     -- 检测结果ID（问题严重程度编号，数字越大越严重）
        b.fanswer_name as fresult_name  -- 检测结果名称（如"良好"、"轻微划痕"等）
    from( 
        -- 子查询：获取每个设备的最新一检记录
        select 
            fend_time,                  -- 检测结束时间
            fdetect_record_id,          -- 检测记录ID
            fserial_number,             -- 设备序列号
            fclass_name,                -- 产品类别
            fwarehouse_code,            -- 仓库编码
            -- 窗口函数：按设备序列号分组，按检测结束时间倒序排列，取最新的一条记录
            row_number()over(partition by fserial_number order by fend_time desc) as num
        from  dwd.dwd_detect_back_detect_detail  -- 检测明细表（数据明细层）
        where fdet_type=0               -- 0=一检（首次检测）
        and fis_deleted=0               -- 0=未删除的记录
        and freport_type=0              -- 0=正常报告类型
        -- 时间过滤：只查询最近180天的数据
        and to_date(fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
    )a
    -- 左连接检测问题答案表，获取具体的检测项目和结果
    left join (
        select 
            fdetect_record_id,          -- 检测记录ID（关联主键）
            fissue_id,                  -- 检测问题ID
            fissue_name,                -- 检测问题名称
            fanswer_id,                 -- 检测答案ID（结果严重程度）
            fanswer_name,               -- 检测答案名称（结果描述）
            field_source                -- 字段来源标识
        from dwd.dwd_detect_back_detection_issue_and_answer_v2  -- 检测问题答案表
        -- 按分区过滤：只查询最近180天的数据（ds是分区字段）
        where ds>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
    ) as b on a.fdetect_record_id=b.fdetect_record_id  -- 通过检测记录ID关联
    where a.num=1                       -- 只取每个设备的最新检测记录
    and b.field_source='fdet_norm_snapshot'  -- 只取标准检测快照数据
),
qc_detect as(
    -- CTE2: 提取QC抽检的详细信息
    select 
        a.fend_time as fqc_time,        -- QC检测时间
        a.fdetect_record_id,            -- QC检测记录ID
        a.fserial_number,               -- 设备序列号
        b.fissue_id,                    -- QC检测问题ID
        b.fissue_name,                  -- QC检测问题名称
        b.fanswer_id as fresult_id,     -- QC检测结果ID（严重程度编号）
        b.fanswer_name as fresult_name  -- QC检测结果名称
    from (
        -- 子查询：获取每个设备的最新QC检测记录
        select 
            fend_time,                  -- QC检测结束时间
            fdetect_record_id,          -- QC检测记录ID
            fserial_number,             -- 设备序列号
            -- 窗口函数：按设备分组，按时间倒序，取最新记录
            row_number()over(partition by fserial_number order by fend_time desc)as num
        from dwd.dwd_detect_back_detect_detail  -- 检测明细表
        where fis_deleted=0             -- 未删除记录
        and (fdet_type=1 or fdet_type=2)  -- 1=抽检, 2=二次抽检
        -- 注释掉的条件：and (freal_name like "%赖嘉琪%")
        -- QC检测人员名单过滤（只统计指定的QC检测员）
        and (freal_name like "%赖嘉琪%" or freal_name like "%李伟雪%" or freal_name like "%王封敏%" 
             or freal_name like "%王若桂%" or freal_name like "%范嘉庆%" or freal_name like "%陈斌%" 
             or freal_name like "%陈梓琦%" or freal_name like "%周雨%" or freal_name like "%郑君豪%" )
        -- 时间过滤：最近180天
        and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),180))
    )a
    -- 左连接获取QC检测的具体问题和答案
    left join (
        select 
            fdetect_record_id,          -- 检测记录ID
            fissue_id,                  -- 问题ID
            fissue_name,                -- 问题名称
            fanswer_id,                 -- 答案ID
            fanswer_name,               -- 答案名称
            field_source                -- 字段来源
        from dwd.dwd_detect_back_detection_issue_and_answer_v2
        -- 分区过滤：最近180天
        where ds>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
    ) as b on a.fdetect_record_id=b.fdetect_record_id  -- 通过检测记录ID关联
    where a.num=1                       -- 只取最新的QC记录
    and b.field_source='fdet_norm_snapshot'  -- 标准检测快照数据
),
detect_three as (
    -- CTE3: 获取三检外观检测人员信息
    select 
        upper(fserial_number) as fserial_number,  -- 序列号转大写（统一格式）
        -- 人名标准化：将"李俊峰"统一为"李俊锋"
        case when freal_name="李俊峰" then "李俊锋" else freal_name end as fdetect_three_name,
        fcreate_time as fdetect_three_time  -- 三检时间
    from (
        select 
            a.fcreate_time,             -- 任务创建时间
            a.fserial_number,           -- 设备序列号
            b.freal_name,               -- 检测人员姓名
            -- 窗口函数：按设备分组，按时间倒序，取最新的外观检测记录
            row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
        from drt.drt_my33312_detection_t_det_task as a        -- 检测任务表
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id  -- 检测任务记录表
        where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))  -- 最近180天
        and b.fdet_sop_task_name like "%外观%"  -- 筛选外观检测任务
    )t
    where num=1  -- 取最新记录
),
detect_three_pingmu as (
    -- CTE4: 获取三检屏幕检测人员信息
    select 
        upper(fserial_number) as fserial_number,  -- 序列号转大写
        -- 人名标准化
        case when freal_name="李俊峰" then "李俊锋" else freal_name end as fdetect_three_name_pingmu,
        fcreate_time as fdetect_three_time_pingmu  -- 屏幕检测时间
    from (
        select 
            a.fcreate_time,             -- 任务创建时间
            a.fserial_number,           -- 设备序列号
            b.freal_name,               -- 检测人员姓名
            -- 窗口函数：按设备分组，按时间倒序排列
            row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
        from drt.drt_my33312_detection_t_det_task as a        -- 检测任务表
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id  -- 任务记录表
        where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))  -- 最近180天
        and b.fdet_sop_task_name like "%屏幕%"    -- 筛选屏幕检测任务
        and b.fdet_sop_task_name!="外观屏幕"      -- 排除外观屏幕检测（避免重复）
    )t
    where num=1  -- 取最新记录
)
-- 主查询：关联所有CTE，生成最终的QC差异明细报告
select 
    a.fend_time,                        -- 一检结束时间
    b.fqc_time,                         -- QC检测时间
    a.fserial_number,                   -- 设备序列号
    left(a.fserial_number,2) as "渠道", -- 从序列号前2位识别渠道（如XY=闲鱼，TM=天猫）
    a.fclass_name,                      -- 产品类别名称
    -- 业务类型识别：通过序列号前缀判断业务类型
    case when left(a.fserial_number,3) like "%010%" or left(a.fserial_number,3) like "%020%" or left(a.fserial_number,3) like "%050%" then "验机"
         when left(a.fserial_number,2) like "%BM%" then "帮卖"
    else "竞拍" end as ftype,
    c.fdetect_three_name_pingmu as "屏幕检测人",  -- 屏幕检测人员
    d.fdetect_three_name as "外观检测人",         -- 外观检测人员
    a.fissue_name,                      -- 一检问题名称
    a.fresult_id,                       -- 一检结果ID（严重程度编号）
    a.fresult_name,                     -- 一检结果名称
    b.fissue_name as fqc_fissue_name,   -- QC检测问题名称
    b.fresult_id as fqc_fresult_id,     -- QC检测结果ID
    b.fresult_name as fqc_fresult_name, -- QC检测结果名称
    -- 模块分类：根据问题名称判断属于显示模块还是外观模块
    case when a.fissue_name like "%屏幕显示%" or a.fissue_name like "%副屏显示%" then "显示" else "外观" end as "模块所属",
    -- 负差统计：QC结果比一检结果更严格（结果ID更小表示问题更轻微）
    case when b.fresult_id<a.fresult_id then 1 else 0 end as "负差数",
    -- 正差统计：QC结果比一检结果更宽松（结果ID更大表示问题更严重）
    case when b.fresult_id>a.fresult_id then 1 else 0 end as "正差数",
    -- 仓库识别：通过序列号特定位置识别仓库
    case when a.fwarehouse_code='12' then "东莞仓" 
         when right(left(a.fserial_number,6),2)="16" or left(a.fserial_number,3)="020" then "杭州仓"  
         else "深圳仓" end as fwarehouse_code
from first_detect as a                  -- 主表：一检数据
left join qc_detect as b on a.fserial_number=b.fserial_number and a.fissue_id=b.fissue_id  -- 关联QC检测数据（同设备同问题）
left join detect_three_pingmu as c on a.fserial_number=c.fserial_number  -- 关联屏幕检测人员
left join detect_three as d on a.fserial_number=d.fserial_number         -- 关联外观检测人员
where b.fserial_number is not null      -- 只保留有QC检测数据的记录（即存在检测差异的）
-- 问题类型过滤：只关注外观和屏幕相关问题
and (a.fissue_name like "%外观%" or a.fissue_name like "%屏幕%" or a.fissue_name like "%边框%" 
     or a.fissue_name like "%显示%" or a.fissue_name like "%机身弯曲%" or a.fissue_name like "%折叠屏转轴%" 
     or a.fissue_name like "%折叠屏保护膜%")
and a.fissue_name not like "%维修%"     -- 排除维修相关问题
and a.fissue_name not like "%光线%"     -- 排除光线相关问题
and to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))  -- 最近180天数据

-- 查询结果说明：
-- 1. 本查询用于质量控制分析，对比一检和QC抽检的结果差异
-- 2. 正差表示QC检测比一检更严格，负差表示QC检测比一检更宽松
-- 3. 通过差异分析可以发现检测标准不一致的问题，用于改进检测流程
-- 4. 查询结果可用于统计各检测员的检测准确度和一致性