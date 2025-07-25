/*
📦 实时回收寄出状态监控
用途：实时跟踪回收订单的寄出状态，对比不同时间段的情况
就像监控"今天、昨天、上周同期分别有多少订单寄出了"
*/

with a  as 
(
select 
case 
    when a.forder_time between  to_date(now()) and now() 
    then "今日"
    when a.forder_time between  date_sub(to_date(now()),1) and  date_sub(now(),1) 
    then "昨日同期"
    when a.forder_time between  date_sub(to_date(now()),7) and  date_sub(now(),7) 
    then "上周同期"
    else "其他" end as forder_time,
-- ↑ 时间段分类：
-- 今日：今天00:00到现在
-- 昨日同期：昨天的同一时间段
-- 上周同期：上周同一天的同一时间段
-- 就像对比"今天上午10点的业绩 vs 昨天上午10点的业绩"

case   when cast(a.fship_type as int)=1 then '邮寄'
        when cast(a.fship_type as int)=2 then '上门'
        when cast(a.fship_type as int)=3 then '到店'
    end as fship_type,                   -- 寄送方式分类
    
b.ftest,                                 -- 是否测试订单
a.Fcategory,                            -- 商品类别
a.Fxy_channel,                          -- 闲鱼渠道
a.fsub_channel,                         -- 子渠道
a.forder_id                             -- 订单ID
from
 drt.drt_my33310_recycle_t_xy_order_data a     -- 闲鱼订单数据表
 inner join drt.drt_my33310_recycle_t_order b on a.forder_id=b.forder_id      -- 关联主订单表
 inner join drt.drt_my33310_recycle_t_xianyu_order_map c on a.forder_id=c.forder_id  -- 关联闲鱼订单映射
 left join   drt.drt_my33310_recycle_t_order_status d on b.forder_status=d.forder_status_id  -- 关联订单状态
 
 where a.forder_time between date_sub(to_date(now()),7) and now()  -- 只看最近7天的订单
 ),
 
b as 
 (select 
 b.forder_id,                           -- 订单ID
 case 
    when b.Fauto_create_time between  to_date(now()) and now() 
    then "今日"
    when b.Fauto_create_time between  date_sub(to_date(now()),1) and  date_sub(now(),1) 
    then "昨日同期"
    when b.Fauto_create_time between  date_sub(to_date(now()),7) and  date_sub(now(),7) 
    then "上周同期"
    else "其他" end as forder_time       -- 按寄出时间重新分类时间段
 from a
 inner join drt.drt_my33310_recycle_t_order_txn b
 on a.forder_id=b.forder_id             -- 关联订单交易表
 where
 Forder_status=20                       -- 订单状态为20（寄出状态）

 union all                              -- 合并其他寄出状态的订单

/*
💡 简单解释：
这个查询就像物流监控大屏：
"实时显示今天寄出了多少单，对比昨天同期和上周同期的数据，
看看业务是增长还是下降"

🔍 监控逻辑：
1. 第一部分（CTE a）：获取最近7天的所有订单基础信息
2. 第二部分（CTE b）：从这些订单中找出已经寄出的（状态=20）
3. 按时间段分类，便于对比分析

📊 时间对比维度：
- 📅 今日：当前时间段的实时数据
- 📅 昨日同期：昨天相同时间段的数据
- �� 上周同期：上周同一天相同时间段的数据

🚚 寄送方式分类：
- 📮 邮寄：用户自己寄快递
- 🚗 上门：平台派人上门收货
- 🏪 到店：用户到门店交货

🎯 业务价值：
- 实时监控寄出订单量
- 对比历史同期数据
- 发现业务增长趋势
- 及时调整运营策略

💡 实用场景：
- 运营人员实时监控当天业绩
- 快速发现异常情况（如寄出量突然下降）
- 为客服提供实时数据支持
- 帮助管理层了解业务走势
*/
 
 
 
 ),
 c as 
 (select 
  b.forder_id
 ,case 
    when b.Fauto_create_time between  to_date(now())and now() 
    then "今日"
    when b.Fauto_create_time between  date_sub(to_date(now()),1) and  date_sub(now(),1) 
    then "昨日同期"
    when b.Fauto_create_time between  date_sub(to_date(now()),7) and  date_sub(now(),7) 
    then "上周同期"
    else "其他" end as forder_time
 from a
 inner join drt.drt_my33310_recycle_t_order_txn b
 on a.forder_id=b.forder_id
 where
 Forder_status=80
 )
 
 select 
    a.Forder_time 
    ,fship_type 
    ,ftest 
    ,Fcategory 
    ,Fxy_channel
    ,fsub_channel
    ,count(a.forder_id) as ordernum
    ,count(c.forder_id) as cancelnum
    ,count(b.forder_id) as sendnum
 
 from a left join b on a.forder_id=b.forder_id and a.forder_time=b.forder_time
        left join c on a.forder_id=c.forder_id and a.forder_time=c.forder_time
   
 group by 
    Forder_time 
    ,fship_type 
    ,ftest 
    ,Fcategory 
    ,Fxy_channel
    ,fsub_channel
    
