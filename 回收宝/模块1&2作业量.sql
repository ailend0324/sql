/*
📊 模块1&2检测作业量统计
用途：统计两个检测模块每天的工作量，按人员、品牌、渠道分类
就像统计"每个检测员每天检测了多少台设备，分别是什么类型"
*/

-- 第一部分：模块一的作业量统计
select 
    to_date(fcreate_time) as fcreate_time,  -- 检测日期
    case when to_date(fcreate_time)='2023-10-16' and freal_name="刘俊" then "周利" 
    	 when (to_date(fcreate_time) BETWEEN '2023-11-01' AND '2023-11-21') and freal_name="郑佩文" then null 
         when to_date(fcreate_time)='2024-01-29' and freal_name="林嘉成" then null
         else freal_name end as freal_name,
    -- ↑ 检测人员姓名（包含特殊日期的人员调整）
    -- 某些特定日期有人员变动或者请假，需要特殊处理
    -- 就像考勤记录中的临时调班或请假情况
    
    case when fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,  -- 品牌分类：苹果或安卓
    "模块一" as ftype,                      -- 标记为模块一
    case when left(fserial_number,2) in ('01','02') then "验机" else "回收" end as fchannel,
    -- ↑ 根据序列号前两位判断渠道：01、02开头是验机，其他是回收
    count(fserial_number) as num           -- 统计设备数量
from (
select 
    a.fcreate_time,                        -- 检测创建时间
    a.fserial_number,                      -- 设备序列号
    b.freal_name,                          -- 检测人员真实姓名
    a.fbrand_name,                         -- 设备品牌
    row_number()over(partition by a.fserial_number order by a.fcreate_time desc) as num
    -- ↑ 为每个设备的检测记录编号，取最新的一次
    -- 避免同一台设备被重复统计
    
from drt.drt_my33312_detection_t_automation_det_record as a  -- 从"自动检测记录表"
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_id=b.fuser_id  -- 关联用户表获取真实姓名
where a.fserial_number is not null and a.fserial_number!=""  -- 序列号不为空
and to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))  -- 最近180天
)t
where num=1                                -- 只统计每个设备最新的一次检测
group by 1,2,3,4,5                        -- 按日期、人员、品牌、模块、渠道分组

union all                                  -- 合并两部分统计结果

-- 第二部分：模块二的作业量统计
select 
    to_date(fcreate_time) as fcreate_time,  -- 检测日期
    freal_name,                            -- 检测人员姓名
    case when fbrand_name='Apple' then "苹果" else "安卓" end as fbrand_name,  -- 品牌分类
    "模块二" as ftype,                      -- 标记为模块二
    case when left(fserial_number,2) in ('01','02') then "验机" else "回收" end as fchannel,  -- 渠道分类
    count(fserial_number) as num           -- 统计设备数量
from (
select 
    a.fcreate_time,                        -- 检测创建时间
    a.fserial_number,                      -- 设备序列号
    case when a.fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" 
  		 when a.freal_name="陈冬凡" and to_date(a.fcreate_time)='2024-05-13' and a.fbrand_name!="Apple" then "李俊锋"
    else b.freal_name end as freal_name,   -- 检测人员（包含特殊用户名处理）
    a.fbrand_name,                         -- 设备品牌
    row_number()over(partition by a.fserial_number order by a.fcreate_time desc) as num
    -- ↑ 为每个设备编号，确保不重复统计
    
from drt.drt_my33312_detection_t_det_app_record as a   -- 从"检测APP记录表"（模块二）
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_id=b.fuser_id  -- 关联用户表
where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))  -- 最近180天
and a.fserial_number is not null and a.fserial_number!=""  -- 序列号不为空
)t
where num=1                                -- 只统计每个设备最新的一次检测
group by 1,2,3,4,5                        -- 按日期、人员、品牌、模块、渠道分组

/*
💡 简单解释：
这个查询就像工厂的生产统计表：
"统计最近6个月，每个检测员每天在两个检测模块分别检测了多少台设备，
按苹果/安卓、验机/回收等不同类型分类统计"

🔍 两个模块的区别：
- 模块一：使用自动检测设备（t_automation_det_record）
- 模块二：使用APP手动检测（t_det_app_record）
- 就像一个是机器检测，一个是人工检测

📊 统计维度：
- 时间：每天
- 人员：每个检测员
- 品牌：苹果 vs 安卓
- 模块：模块一 vs 模块二  
- 渠道：验机 vs 回收

🎯 业务用途：
- 考核检测员工作量
- 分析两个模块的效率
- 合理安排人员排班
- 优化检测流程

💡 关键处理：
- 同一设备多次检测只算最新一次（避免重复统计）
- 特殊日期的人员调整（临时代班、请假等）
- 统一品牌分类（Apple→苹果，其他→安卓）
*/
