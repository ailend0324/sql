-- 二手电商运营SQL模板库
-- 适用于运营总监日常数据分析需求
-- 使用说明：根据实际表名和字段名调整以下模板

-- ========================================
-- 用户分析模板
-- ========================================

-- 1. 新用户注册趋势（日/周/月）
-- 业务价值：监控获客效果，发现增长趋势
SELECT 
    DATE_FORMAT(register_time, '%Y-%m-%d') as date,  -- 按日统计，可改为'%Y-%u'按周，'%Y-%m'按月
    COUNT(*) as new_users,
    COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY DATE_FORMAT(register_time, '%Y-%m-%d')) as growth
FROM users 
WHERE register_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)  -- 最近30天，可调整
GROUP BY DATE_FORMAT(register_time, '%Y-%m-%d')
ORDER BY date;

-- 2. 用户活跃度分析
-- 业务价值：了解用户粘性，识别流失风险
SELECT 
    CASE 
        WHEN last_login_time >= DATE_SUB(NOW(), INTERVAL 1 DAY) THEN '今日活跃'
        WHEN last_login_time >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN '本周活跃'
        WHEN last_login_time >= DATE_SUB(NOW(), INTERVAL 30 DAY) THEN '本月活跃'
        ELSE '非活跃用户'
    END as user_activity,
    COUNT(*) as user_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM users), 2) as percentage
FROM users
GROUP BY 
    CASE 
        WHEN last_login_time >= DATE_SUB(NOW(), INTERVAL 1 DAY) THEN '今日活跃'
        WHEN last_login_time >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN '本周活跃'
        WHEN last_login_time >= DATE_SUB(NOW(), INTERVAL 30 DAY) THEN '本月活跃'
        ELSE '非活跃用户'
    END;

-- 3. 用户地域分布
-- 业务价值：了解市场分布，指导推广策略
SELECT 
    city,
    COUNT(*) as user_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM users), 2) as percentage
FROM users
WHERE register_time >= DATE_SUB(NOW(), INTERVAL 90 DAY)  -- 最近3个月新用户
GROUP BY city
ORDER BY user_count DESC
LIMIT 20;

-- 4. 用户分层分析（RFM模型简化版）
-- 业务价值：用户精细化运营，个性化营销
SELECT 
    user_id,
    DATEDIFF(NOW(), MAX(order_time)) as recency,  -- 最近购买距今天数
    COUNT(*) as frequency,  -- 购买频次
    SUM(order_amount) as monetary,  -- 购买金额
    CASE 
        WHEN DATEDIFF(NOW(), MAX(order_time)) <= 30 AND COUNT(*) >= 3 THEN '高价值用户'
        WHEN DATEDIFF(NOW(), MAX(order_time)) <= 30 THEN '新活跃用户' 
        WHEN COUNT(*) >= 3 THEN '老客户'
        ELSE '潜在流失用户'
    END as user_segment
FROM orders
WHERE order_time >= DATE_SUB(NOW(), INTERVAL 365 DAY)  -- 最近一年
GROUP BY user_id;

-- ========================================
-- 商品分析模板  
-- ========================================

-- 5. 热销商品排行
-- 业务价值：识别爆款，优化商品运营策略
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    COUNT(o.order_id) as sales_count,
    SUM(o.amount) as total_revenue,
    AVG(o.amount) as avg_price,
    RANK() OVER (ORDER BY COUNT(o.order_id) DESC) as sales_rank
FROM products p
JOIN orders o ON p.product_id = o.product_id
WHERE o.order_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)  -- 最近30天
GROUP BY p.product_id, p.product_name, p.category, p.brand
ORDER BY sales_count DESC
LIMIT 50;

-- 6. 商品分类表现对比
-- 业务价值：了解不同品类的表现，调整运营重点
SELECT 
    category,
    COUNT(DISTINCT product_id) as product_count,  -- 在售商品数量
    COUNT(order_id) as sales_count,  -- 销售件数
    SUM(amount) as total_revenue,  -- 总收入
    AVG(amount) as avg_price,  -- 平均价格
    SUM(amount) / COUNT(order_id) as avg_order_value,  -- 客单价
    ROUND(COUNT(order_id) * 100.0 / SUM(COUNT(order_id)) OVER(), 2) as sales_share  -- 销量占比
FROM products p
LEFT JOIN orders o ON p.product_id = o.product_id 
    AND o.order_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY category
ORDER BY total_revenue DESC;

-- 7. 商品价格区间分析
-- 业务价值：了解用户价格偏好，制定定价策略
SELECT 
    CASE 
        WHEN amount < 100 THEN '0-100元'
        WHEN amount < 300 THEN '100-300元'
        WHEN amount < 500 THEN '300-500元'
        WHEN amount < 1000 THEN '500-1000元'
        ELSE '1000元以上'
    END as price_range,
    COUNT(*) as order_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM orders WHERE order_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)), 2) as percentage
FROM orders
WHERE order_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY 
    CASE 
        WHEN amount < 100 THEN '0-100元'
        WHEN amount < 300 THEN '100-300元'
        WHEN amount < 500 THEN '300-500元'
        WHEN amount < 1000 THEN '500-1000元'
        ELSE '1000元以上'
    END
ORDER BY MIN(amount);

-- ========================================
-- 交易分析模板
-- ========================================

-- 8. GMV趋势分析（核心指标）
-- 业务价值：监控业务健康度，发现增长机会
SELECT 
    DATE_FORMAT(order_time, '%Y-%m-%d') as date,
    COUNT(*) as order_count,  -- 订单量
    COUNT(DISTINCT user_id) as buyer_count,  -- 购买用户数
    SUM(amount) as gmv,  -- 总交易额
    AVG(amount) as avg_order_value,  -- 客单价
    SUM(amount) / COUNT(DISTINCT user_id) as arpu  -- 用户平均价值
FROM orders
WHERE order_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY DATE_FORMAT(order_time, '%Y-%m-%d')
ORDER BY date;

-- 9. 订单状态分析
-- 业务价值：监控订单履约情况，发现流程问题
SELECT 
    order_status,
    COUNT(*) as order_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM orders WHERE order_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)), 2) as percentage,
    SUM(amount) as total_amount
FROM orders
WHERE order_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)  -- 最近7天
GROUP BY order_status
ORDER BY order_count DESC;

-- 10. 支付方式分析
-- 业务价值：了解用户支付偏好，优化支付流程
SELECT 
    payment_method,
    COUNT(*) as usage_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM orders WHERE order_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)), 2) as usage_rate,
    AVG(amount) as avg_amount
FROM orders
WHERE order_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY payment_method
ORDER BY usage_count DESC;

-- ========================================
-- 运营活动分析模板
-- ========================================

-- 11. 促销活动效果分析
-- 业务价值：评估营销投入产出，优化活动策略
-- 注意：需要根据实际的促销标识字段调整
SELECT 
    '活动期间' as period,
    COUNT(*) as order_count,
    SUM(amount) as revenue,
    AVG(amount) as avg_order_value,
    COUNT(DISTINCT user_id) as active_users
FROM orders 
WHERE order_time BETWEEN '2024-01-15 00:00:00' AND '2024-01-20 23:59:59'  -- 活动时间，需调整
    AND (coupon_id IS NOT NULL OR discount_amount > 0)  -- 有使用优惠券或折扣

UNION ALL

SELECT 
    '对比期间' as period,
    COUNT(*) as order_count,
    SUM(amount) as revenue,
    AVG(amount) as avg_order_value,
    COUNT(DISTINCT user_id) as active_users
FROM orders 
WHERE order_time BETWEEN '2024-01-08 00:00:00' AND '2024-01-13 23:59:59'  -- 对比时间，需调整
    AND (coupon_id IS NULL AND discount_amount = 0);  -- 无优惠

-- 12. 优惠券使用分析
-- 业务价值：评估优惠券策略效果
SELECT 
    coupon_type,
    COUNT(*) as usage_count,
    SUM(discount_amount) as total_discount,
    AVG(discount_amount) as avg_discount,
    SUM(amount) as generated_revenue,
    ROUND(SUM(amount) / SUM(discount_amount), 2) as roi  -- 投入产出比
FROM orders o
JOIN coupons c ON o.coupon_id = c.coupon_id
WHERE o.order_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY coupon_type
ORDER BY usage_count DESC;

-- ========================================
-- 渠道分析模板
-- ========================================

-- 13. 获客渠道效果分析
-- 业务价值：评估不同渠道质量，优化投放策略
SELECT 
    u.register_channel,
    COUNT(DISTINCT u.user_id) as total_users,  -- 总用户数
    COUNT(DISTINCT o.user_id) as paying_users,  -- 付费用户数
    ROUND(COUNT(DISTINCT o.user_id) * 100.0 / COUNT(DISTINCT u.user_id), 2) as conversion_rate,  -- 转化率
    COUNT(o.order_id) as total_orders,  -- 总订单数
    SUM(o.amount) as total_revenue,  -- 总收入
    ROUND(SUM(o.amount) / COUNT(DISTINCT u.user_id), 2) as arpu,  -- 用户平均价值
    ROUND(COUNT(o.order_id) * 1.0 / COUNT(DISTINCT o.user_id), 2) as orders_per_user  -- 人均订单数
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id 
    AND o.order_time >= u.register_time  -- 注册后的订单
WHERE u.register_time >= DATE_SUB(NOW(), INTERVAL 90 DAY)  -- 最近3个月新用户
GROUP BY u.register_channel
ORDER BY total_revenue DESC;

-- ========================================
-- 用户生命周期分析模板
-- ========================================

-- 14. 用户复购分析
-- 业务价值：了解用户粘性，制定留存策略
SELECT 
    purchase_frequency,
    user_count,
    ROUND(user_count * 100.0 / SUM(user_count) OVER(), 2) as percentage,
    cumulative_revenue
FROM (
    SELECT 
        CASE 
            WHEN order_count = 1 THEN '单次购买'
            WHEN order_count BETWEEN 2 AND 3 THEN '2-3次购买'
            WHEN order_count BETWEEN 4 AND 6 THEN '4-6次购买'
            WHEN order_count >= 7 THEN '7次以上购买'
        END as purchase_frequency,
        COUNT(*) as user_count,
        SUM(total_amount) as cumulative_revenue
    FROM (
        SELECT 
            user_id,
            COUNT(*) as order_count,
            SUM(amount) as total_amount
        FROM orders
        WHERE order_time >= DATE_SUB(NOW(), INTERVAL 365 DAY)  -- 最近一年
        GROUP BY user_id
    ) user_orders
    GROUP BY 
        CASE 
            WHEN order_count = 1 THEN '单次购买'
            WHEN order_count BETWEEN 2 AND 3 THEN '2-3次购买'
            WHEN order_count BETWEEN 4 AND 6 THEN '4-6次购买'
            WHEN order_count >= 7 THEN '7次以上购买'
        END
) frequency_analysis
ORDER BY MIN(user_count) DESC;

-- 15. 用户流失预警
-- 业务价值：提前识别流失用户，进行挽回
SELECT 
    user_id,
    last_order_time,
    DATEDIFF(NOW(), last_order_time) as days_since_last_order,
    historical_order_count,
    historical_total_amount,
    CASE 
        WHEN DATEDIFF(NOW(), last_order_time) > 90 THEN '高流失风险'
        WHEN DATEDIFF(NOW(), last_order_time) > 60 THEN '中流失风险'
        WHEN DATEDIFF(NOW(), last_order_time) > 30 THEN '低流失风险'
        ELSE '活跃用户'
    END as churn_risk
FROM (
    SELECT 
        user_id,
        MAX(order_time) as last_order_time,
        COUNT(*) as historical_order_count,
        SUM(amount) as historical_total_amount
    FROM orders
    GROUP BY user_id
    HAVING COUNT(*) >= 2  -- 至少有过2次购买的用户才纳入流失分析
) user_activity
WHERE DATEDIFF(NOW(), last_order_time) > 30  -- 30天以上未购买
ORDER BY historical_total_amount DESC, days_since_last_order DESC;

-- ========================================
-- 使用说明
-- ========================================

/*
模板使用指南：

1. 替换表名和字段名：
   - users: 用户表
   - orders: 订单表  
   - products: 商品表
   - coupons: 优惠券表

2. 常用字段对应关系：
   - register_time: 注册时间
   - order_time: 下单时间
   - amount: 订单金额
   - user_id: 用户ID
   - product_id: 商品ID

3. 时间范围调整：
   - DATE_SUB(NOW(), INTERVAL 30 DAY): 最近30天
   - 可调整为7 DAY, 90 DAY, 365 DAY等

4. 业务逻辑调整：
   - 根据公司实际的业务状态、分类等字段调整
   - 价格区间、用户分层标准可根据业务实际情况修改

5. 性能优化：
   - 在时间字段上建立索引
   - 大数据量时考虑分页查询
   - 复杂查询可以创建临时表

使用建议：
- 先从简单的查询开始，逐步掌握复杂查询
- 每次使用前确认表名和字段名
- 建议保存自己常用的查询模板
- 定期与数据同事交流，优化查询效率
*/ 