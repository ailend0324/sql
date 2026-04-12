-- 测试SQL：买家卖家信息获取方法对比
-- 目的：对比不同方法获取的买家/卖家ID，选择最佳方案
-- 测试范围：鱼市B端订单（平台类型=5）
-- 测试方法：方法1（竞拍订单表）+ 方法3（售后表-fmerchant_id比对）

-- 方法1：从竞拍订单表直接获取（保持不变）
with method1_buyer_seller as (
    select
        fseries_number,                    -- 商品序列号
        fdetail_id,                        -- 订单详情ID
        fstart_time,                       -- 开始时间
        fpay_time,                         -- 付款时间
        forder_platform,                   -- 订单平台
        fuser_id as fbuyer_merchant_id,   -- 方法1：买家商家ID
        Fmerchant_id_new as fseller_merchant_id,    -- 方法1：卖家商家ID
        Fmerchant_name_new as fseller_merchant_name, -- 方法1：卖家商家名称
        '方法1-竞拍订单表' as method_source
    from dws.dws_jp_order_detail 
    where ftest_show <> 1
    and forder_platform = 5  -- 只要鱼市B端订单
    and forder_status in (2,3,4,6)
    and fstart_time >= date_sub(from_unixtime(unix_timestamp()), 30)  -- 最近30天数据（Impala语法）
),

-- 方法3：从采货侠售后表获取（重点关注fmerchant_id的比对）
method3_after_sales as (
    select
        a.fseries_number,
        a.fdetail_id,
        a.fstart_time,
        a.fpay_time,
        a.forder_platform,
        a.fbuyer_merchant_id,
        a.fseller_merchant_id,
        a.fseller_merchant_name,
        
        -- 售后表中的关键字段
        cc.fafter_sale_no,                                  -- 售后订单号
        cc.frefund_total,                                   -- 退款总额
        cc.fmerchant_id as after_sales_merchant_id,         -- 售后表商家ID（重点比对字段）
        cc.falipay_uid,                                     -- 支付宝用户ID
        cc.fgoods_name,                                     -- 商品名称
        cc.fcreate_time as after_sales_create_time,         -- 售后创建时间
        cc.fapply_reason,                                   -- 申请原因
        cc.fjudge_result,                                   -- 判定结果
        
        -- 商家ID比对分析（重点）
        case 
            when cc.fmerchant_id = a.fbuyer_merchant_id then '售后表商家ID = 买家ID'
            when cc.fmerchant_id = a.fseller_merchant_id then '售后表商家ID = 卖家ID'
            when cc.fmerchant_id is not null and a.fbuyer_merchant_id is not null and a.fbuyer_merchant_id != 0 then '售后表商家ID ≠ 买家ID'
            when cc.fmerchant_id is not null and a.fseller_merchant_id is not null and a.fseller_merchant_id != 0 then '售后表商家ID ≠ 卖家ID'
            when cc.fmerchant_id is not null then '售后表有商家ID，但竞拍表无对应ID'
            else '售后表无商家ID' 
        end as merchant_id_comparison,
        
        -- 售后表商家ID与买家/卖家ID的关系（重点）
        case 
            when cc.fmerchant_id = a.fbuyer_merchant_id then '售后发起方是买家'
            when cc.fmerchant_id = a.fseller_merchant_id then '售后发起方是卖家'
            when cc.fmerchant_id is not null then '售后发起方是第三方'
            else '无售后信息' 
        end as after_sales_initiator,
        
        '方法3-售后表关联（重点比对fmerchant_id）' as method_source
    from method1_buyer_seller a
    left join drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales cc 
        on a.fseries_number = cc.fbusiness_id  -- 关联售后表
)

-- 最终对比结果：整合方法1和方法3的信息，重点关注fmerchant_id的比对
select 
    -- 基础信息
    a.fseries_number,                    -- 商品序列号
    a.fdetail_id,                        -- 订单详情ID
    a.fstart_time,                       -- 开始时间
    a.fpay_time,                         -- 付款时间
    a.forder_platform,                   -- 订单平台
    
    -- 方法1：竞拍订单表直接获取（保持不变）
    a.fbuyer_merchant_id as method1_buyer_id,           -- 方法1买家ID
    a.fseller_merchant_id as method1_seller_id,         -- 方法1卖家ID
    a.fseller_merchant_name as method1_seller_name,     -- 方法1卖家名称
    
    -- 方法3：售后表关联获取（重点关注fmerchant_id比对）
    m3.fafter_sale_no as method3_after_sale_no,          -- 方法3售后订单号
    m3.frefund_total as method3_refund_total,            -- 方法3退款总额
    m3.after_sales_merchant_id as method3_merchant_id,   -- 方法3售后表商家ID（重点字段）
    m3.falipay_uid as method3_alipay_uid,                -- 方法3支付宝用户ID
    m3.fgoods_name as method3_goods_name,                -- 方法3商品名称
    m3.fapply_reason as method3_apply_reason,            -- 方法3申请原因
    m3.merchant_id_comparison,                           -- 商家ID比对结果（重点）
    m3.after_sales_initiator,                            -- 售后发起方分析（重点）
    
    -- 数据质量评估
    case 
        when a.fbuyer_merchant_id is not null and a.fbuyer_merchant_id != 0 then '有买家ID'
        else '无买家ID' 
    end as buyer_id_status,
    
    case 
        when a.fseller_merchant_id is not null and a.fseller_merchant_id != 0 then '有卖家ID'
        else '无卖家ID' 
    end as seller_id_status,
    
    case 
        when m3.fafter_sale_no is not null then '有售后记录'
        else '无售后记录' 
    end as after_sales_status,
    
    -- fmerchant_id比对分析（重点）
    case 
        when m3.after_sales_merchant_id = a.fbuyer_merchant_id then '售后发起方是买家'
        when m3.after_sales_merchant_id = a.fseller_merchant_id then '售后发起方是卖家'
        when m3.after_sales_merchant_id is not null then '售后发起方是第三方'
        else '无售后信息' 
    end as final_after_sales_initiator

from method1_buyer_seller a
left join method3_after_sales m3 on a.fseries_number = m3.fseries_number

-- 筛选条件：只选择有买家或卖家信息的记录
where (a.fbuyer_merchant_id is not null and a.fbuyer_merchant_id != 0)
   or (a.fseller_merchant_id is not null and a.fseller_merchant_id != 0)

-- 按时间倒序排列，便于查看最新数据
order by a.fstart_time desc

-- 限制返回记录数，便于测试
limit 50;