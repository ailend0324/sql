/*
💰 寄卖付款明细查询
用途：查看寄卖业务的付款记录和相关费用明细
就像查看"帮别人卖东西的账单，包括卖了多少钱，收了多少手续费"
*/

select 
    if(a.fpay_out_time is not null,a.fpay_out_time,c.fpay_time) as fpay_time,
    -- ↑ 付款时间：优先用平台付款时间，如果没有就用买家付款时间
    -- 就像记录"钱到账的时间"
    
    a.forder_id,                            -- 订单ID
    cast(e.fxy_order_id as string) as fxy_order_id,  -- 闲鱼订单ID（转为字符串格式）
    a.forder_num,                           -- 订单编号
    a.fseries_number,                       -- 设备序列号
    b.Fbuyer_fee_order_id,                  -- 买家费用订单ID
    case when a.frecycle_type=1 then "邮寄"
    	 when a.frecycle_type=2 then "上门"
         when a.frecycle_type=3 then "到店"
    else null end as "回收方式",             -- 回收方式分类
    
    case when b.fsales_type=1 then "买家购买"
         when b.fsales_type=2 then "平台回收"
         when b.fsales_type=3 then "寄卖回收"
         when b.fsales_type=4 then "买家购买(一口价)"
         when b.fsales_type=0 and a.fpay_out_price>0 then "平台回收"
    else null end as fsales_type,           -- 销售类型分类
    -- ↑ 根据销售类型代码判断是什么销售方式：
    -- 1=买家购买，2=平台回收，3=寄卖回收，4=一口价购买
    -- 特殊情况：类型0但有付款金额也算平台回收
    
    b.fsales_amount/100 as fsales_amount,   -- 销售金额（除以100转换为元）
    case when b.fsales_type=2 then Frecycle_service_price/100 
         when b.fsales_type=0 and a.fpay_out_price>0 then Frecycle_service_price/100 
    else Fservice_price/100 end as Fservice_price,  -- 服务费（根据不同类型选择不同的服务费）
    
    case when Fuse_xy_detect_price=0 then "否"
         when Fuse_xy_detect_price=1 then "是"
    else null end as Fuse_xy_detect_price,  -- 是否使用闲鱼检测价格
    
    case when Fuse_xy_detect_price=1 then Fxy_starting_price/100 else Fnew_bottom_price/100 end as fstarting_price,
    -- ↑ 起拍价：如果使用闲鱼检测价格就用闲鱼起拍价，否则用新的底价
    
    a.fpay_out_price/100 as fpay_out_price, -- 付款金额（转为元）
    case when a.fsupply_partner=2 then "小站(自营)" 
         when a.fsupply_partner=3 then "小站(加盟)"
    else "回收宝" end as fsupply_partner,    -- 供应商类型
    
    d.fchannel_name,                        -- 渠道名称
    b.fxy_recycle_price/100 as fxy_recycle_price,    -- 闲鱼回收价格（转为元）
    case when b.fonly_auction=1 then "是" else "否" end as fonly_auction,        -- 是否只拍卖
    case when b.fuse_xy_recycle_price=1 then "是" else "否" end as fuse_xy_recycle_price,  -- 是否使用闲鱼回收价格
    b.flow_start_price/100 as flow_start_price,      -- 流程起始价格（转为元）
    b.fseller_fee_commission/100 as fseller_fee_commission  -- 卖家费用佣金（转为元）
    
from drt.drt_my33310_recycle_t_order as a           -- 主订单表
inner join drt.drt_my33310_recycle_t_xy_jimai_plus_order as b on a.forder_id=b.forder_id  -- 关联寄卖Plus订单表
left join (select                            -- 子查询：获取买家付款时间
        *
    from 
        (select 
            a.forder_id,
            a.fauto_update_time as fpay_time,    -- 自动更新时间作为付款时间
            row_number() over(partition by a.forder_id order by a.fauto_update_time desc) as num
            -- ↑ 为每个订单的交易记录编号，按时间倒序，取最新的一条
        from drt.drt_my33310_recycle_t_order_txn as a   -- 订单交易表
        inner join drt.drt_my33310_recycle_t_xy_jimai_plus_order as b on a.forder_id=b.forder_id
        where a.forder_status in (714,814)      -- 只看特定状态的订单（已付款状态）
        ) t
    where num=1                              -- 只保留最新的付款记录
    ) as c on a.forder_id=c.forder_id        -- 关联付款时间
left join drt.drt_my33310_recycle_t_channel as d on a.fchannel_id=d.fchannel_id  -- 关联渠道信息
left join drt.drt_my33318_xy_bangmai_t_xybangmai_order_map as e on a.forder_id=e.forder_id  -- 关联闲鱼帮卖订单映射
where (to_date(a.fpay_out_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),366)) 
       or to_date(c.fpay_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),366)))
-- ↑ 只看最近366天（约1年）有付款记录的订单

/*
💡 简单解释：
这个查询就像寄卖店的账单明细：
"给我看看最近1年所有寄卖订单的付款情况，
包括卖了多少钱、收了多少服务费、通过什么渠道卖的等详细信息"

🔍 寄卖业务流程：
1. 用户把闲置物品给平台代卖
2. 平台帮忙上架、宣传、销售
3. 卖出后扣除服务费，剩余金额付给用户
4. 就像委托拍卖行卖古董，成交后扣除佣金

📊 关键信息：
- 💰 金额：销售金额、服务费、付款金额
- 📅 时间：付款时间（优先用平台时间）
- 🏪 渠道：通过什么平台卖的
- 🚚 方式：邮寄、上门、到店
- 🎯 类型：买家购买、平台回收等

💡 特殊处理：
- 金额都除以100（数据库存储为分，显示为元）
- 付款时间优先用平台记录，备选买家记录
- 根据不同销售类型选择对应的服务费标准

🎯 业务价值：
- 财务对账和结算
- 分析寄卖业务盈利情况
- 统计不同渠道的表现
- 监控服务费收取情况
*/
