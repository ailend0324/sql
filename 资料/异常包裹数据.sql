/*
⚠️ 异常包裹数据统计
用途：统计和分析出现异常的包裹，计算异常处理时效
就像统计"哪些快递包裹出了问题，处理这些问题花了多长时间"
*/

with yhb as (                           -- 第一步：整理验货宝包裹信息
select 
    flogistics_num,                     -- 物流单号
    fbar_code                           -- 条码
from (
    select 
        *,
        row_number()over(partition by flogistics_num order by fadd_time desc) as num
        -- ↑ 为每个物流单号的记录编号，按添加时间倒序，取最新记录
    from drt.drt_my33310_xywms_t_parcel     -- 验机包裹表
    where to_date(fadd_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),180))  -- 最近180天
    and fstatus=5                       -- 状态为5（特定状态）
    )t
where num=1                             -- 只保留每个物流单号的最新记录
)

-- 第一部分：验机异常包裹
select 
    a.fid,                              -- 工单ID
    "验机" as ftype,                     -- 业务类型标记为验机
    case when left(c.fbar_code,2)='01' then "深圳仓"
         when left(c.fbar_code,2)="02" then "杭州仓"
    else null end as fwarehouse,        -- 根据条码前缀判断仓库位置
    
    a.fadd_user,                        -- 添加用户（处理人员）
    b.flogistics_code,                  -- 物流编码
    c.fbar_code,                        -- 条码
    d.fdetect_time,                     -- 检测时间
    from_unixtime(a.fadd_time),         -- 工单创建时间（转换为可读格式）
    from_unixtime(a.fupdate_time),      -- 工单更新时间（转换为可读格式）
    d.freceive_time,                    -- 收货时间
    a.fupdate_user,                     -- 更新用户（最后处理人）
    case when d.freceive_time is not null and (unix_timestamp(d.freceive_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(from_unixtime(a.fupdate_time),'yyyy-MM-dd HH:mm:ss'))>0 
         then (unix_timestamp(d.freceive_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(from_unixtime(a.fupdate_time),'yyyy-MM-dd HH:mm:ss'))/3600 
         else null end as "异常包裹完结收货时效"
    -- ↑ 计算异常处理时效：
    -- 从工单更新时间到收货时间的小时数
    -- 就像计算"从发现问题到解决问题花了多少小时"
    
from drt.drt_my33310_csrdb_t_works as a        -- 客服工单表（异常处理记录）
left join drt.drt_my33312_csrdb_t_logistics_info as b on a.fid=b.fwork_id  -- 关联物流信息
left join yhb as c on upper(b.flogistics_code)=upper(c.flogistics_num)     -- 关联验货宝包裹（忽略大小写）
left join dws.dws_xy_yhb_detail as d on c.fbar_code=d.fhost_barcode        -- 关联验货宝详情
where from_unixtime(a.Fadd_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),180))  -- 最近180天的工单
and a.fwork_type in(3)                  -- 工单类型为3（特定异常类型）
and a.fwork_source<>3                   -- 工单来源不是3
and a.fappeal_type1<>0                  -- 申诉类型不是0（确实有异常）
and a.fduty_content not like "%无效工单%"  -- 排除无效工单
and a.fwork_status=40                   -- 工单状态为40（已完结）
and c.flogistics_num is not null        -- 确保有物流单号
and a.forder_system=1                   -- 订单系统为1

union all                               -- 合并第二部分数据

-- 第二部分：回收/帮卖异常包裹
select 
    a.fid,                              -- 工单ID
    case when left(c.fseries_number,2)="BM" then "帮卖" 
    	 when left(c.fseries_number,2)='TL' or (left(c.fseries_number,2)='CG' and c.fgetin_time>='2024-12-01') then "太力" 
    else "回收" end as ftype,            -- 根据序列号判断业务类型
    case when right(left(c.fseries_number,6),2)='16' then "杭州仓" else "深圳仓" end as fwarehouse,
    -- ↑ 根据序列号判断仓库位置
    a.fadd_user,                        -- 处理人员
    b.flogistics_code,                  -- 物流编码
    c.fseries_number,                   -- 设备序列号（不是条码）

/*
💡 简单解释：
这个查询就像快递异常处理统计表：
"统计最近6个月所有出现异常的包裹，
记录异常类型、处理人员、处理时间等信息"

🔍 两种异常包裹：
1. 验机异常：验货宝业务中的包裹异常
2. 回收异常：普通回收和帮卖业务中的异常

⚠️ 异常情况包括：
- 包裹丢失或损坏
- 物流延误
- 信息不符
- 客户投诉
- 其他申诉问题

📊 关键指标：
- 异常包裹完结收货时效：处理异常花费的时间
- 计算公式：收货时间 - 工单更新时间
- 单位：小时

🎯 业务价值：
- 监控异常包裹处理效率
- 分析不同仓库的异常率
- 考核客服处理能力
- 优化异常处理流程

💡 数据来源：
- 客服工单系统：记录异常和处理过程
- 物流系统：提供包裹轨迹信息
- 验货系统：验机业务的详细信息
- 回收系统：普通回收业务信息

🔧 筛选条件：
- 只看已完结的异常工单（状态=40）
- 排除无效工单和测试数据
- 确保有明确的异常申诉（申诉类型≠0）
- 时间范围：最近180天
*/
