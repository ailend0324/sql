/*
🚚 顺丰正向物流时效分析
用途：分析顺丰快递从发货到签收的时效，按地区和业务类型统计
就像统计"从深圳发往全国各地的快递，平均几天能到达"
*/

with qianshou as (                      -- 第一步：整理签收记录
select 
    *
from (
    select 
        *,
        row_number()over(partition by Flogistics_number order by fauto_create_time desc) as num
        -- ↑ 为每个物流单号的签收记录编号，按时间倒序，取最新一次签收
        -- 避免重复签收记录影响统计
    from drt.drt_my33310_hsb_wms_t_sh_sign_log    -- 顺丰签收日志表
    where to_date(fauto_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))  -- 最近180天
    )t
where t.num=1                           -- 只保留每个包裹最新的签收记录
)

-- 第一部分：回收/寄卖订单的物流时效
select 
    b.forder_id,                        -- 订单ID
    b.fproduct_name,                    -- 产品名称
    f.Fdeliver_province,                -- 发货省份
    f.Fdeliver_city,                    -- 发货城市
    case when right(left(b.fseries_number,6),4)='0112' then "东莞仓" 
    	 when  right(left(b.fseries_number,6),2)="16" then "杭州仓"
    else "深圳仓" end as fwarehouse,    -- 根据序列号判断发货仓库
    
    case when e.fchannel_name like "%寄卖%" then "寄卖" else "回收" end as ftype,  -- 业务类型
    a.fauto_create_time as ffahuo_time, -- 发货时间
    from_unixtime(d.Fadd_time) as fsign_time,  -- 签收时间（转换为可读格式）
    (unix_timestamp(from_unixtime(d.Fadd_time),'yyyy-MM-dd HH:mm:ss')-unix_timestamp(a.fauto_create_time,'yyyy-MM-dd HH:mm:ss'))/3600/24 as fshixiao
    -- ↑ 计算物流时效（天数）：
    -- (签收时间 - 发货时间) ÷ 3600秒 ÷ 24小时 = 天数
    -- 就像计算"这个快递用了几天送达"
    
from drt.drt_my33310_recycle_t_order_txn as a      -- 订单交易表
left join drt.drt_my33310_recycle_t_order as b on a.forder_id=b.forder_id          -- 关联订单主表
left join drt.drt_my33310_recycle_t_logistics as c on b.flogistics_id=c.flogistics_id  -- 关联物流信息
left join qianshou as d on c.fchannel_id=d.Flogistics_number    -- 关联签收记录
left join drt.drt_my33310_recycle_t_channel as e on b.fchannel_id=e.fchannel_id    -- 关联渠道信息
left join drt.drt_my33310_recycle_t_tms_logistics_recycle as f on a.forder_id=cast(f.forder_id as int)  -- 关联TMS物流回收表
where a.forder_status=20                -- 订单状态为20（已发货）
and to_date(a.fauto_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))  -- 最近180天
and from_unixtime(d.Fadd_time) is not null  -- 确保有签收记录
and b.ftest=0                           -- 非测试订单
and b.frecycle_type=1                   -- 回收类型为1（邮寄）
and b.fproduct_name not like "%清库存%" -- 排除清库存订单
and b.fproduct_name not like "%无效%"   -- 排除无效订单
and from_unixtime(d.Fadd_time)>a.fauto_create_time  -- 确保签收时间晚于发货时间（逻辑校验）

union                                   -- 合并第二部分数据

-- 第二部分：验货宝订单的物流时效
select 
    b.forder_id,                        -- 订单ID
    b.fhsb_product_name as fproduct_name,  -- 产品名称（验货宝产品名）
    b.fseller_province_name as Fdeliver_province,  -- 发货省份
    b.fseller_city_name as Fdeliver_city,          -- 发货城市
    case when left(b.fhost_barcode,3)='020' then "杭州仓"
         when left(b.fhost_barcode,3)='050' then "东莞仓"
    else "深圳仓" end as fwarehouse,    -- 根据条码判断目标仓库
    "验货宝" as ftype,                  -- 业务类型标记为验货宝
    a.fauto_create_time as ffahuo_time, -- 发货时间

/*
💡 简单解释：
这个查询就像快递公司的时效统计报告：
"分析最近6个月所有通过顺丰发出的包裹，
统计从不同城市发往各个仓库的平均配送时间"

🚚 物流时效计算：
时效（天） = (签收时间 - 发货时间) ÷ 24小时
- 发货时间：订单状态变更为"已发货"的时间
- 签收时间：快递员/收货人确认签收的时间
- 就像计算从下单到收货花了几天

📊 分析维度：
- 🏭 发货仓库：深圳仓、杭州仓、东莞仓
- 🏙️ 发货地区：省份、城市
- 💼 业务类型：回收、寄卖、验货宝
- 📦 产品类型：不同商品类别

🎯 业务价值：
- 评估顺丰配送效率
- 分析不同地区的物流时效差异
- 为客户提供准确的到货预期
- 优化仓库布局和物流策略
- 识别物流异常和延误问题

💡 数据处理：
- 去重：每个包裹只取最新的签收记录
- 校验：确保签收时间晚于发货时间
- 过滤：排除测试订单和无效数据
- 分类：统一不同业务的时效计算标准

📈 应用场景：
- 客服告知客户预计到货时间
- 运营团队分析物流KPI
- 管理层制定物流政策
- 财务部门评估物流成本效益
*/

