-- =============================================
-- 回收宝财务营收分析SQL
-- 分析维度：时间、产品类别、渠道、业务类型
-- 创建时间：2024年
-- =============================================

-- 第一部分：回收业务收入分析
WITH 回收收入 AS (
    SELECT
        -- 时间维度
        substr(a.forder_time, 1, 7) AS 回收月份,
        substr(a.forder_time, 1, 10) AS 回收日期,
        
        -- 产品分类
        CASE 
            WHEN b.fcategory IN ('平板','平板电脑') THEN '平板'
            WHEN b.fcategory IN ('笔记本','笔记本电脑') THEN '笔记本'
            WHEN b.fcategory IN ('手机','') THEN '手机'
            WHEN b.fcategory IN ('单反相机','数码相机','微单相机') THEN '相机'
            WHEN b.fcategory IN ('耳机','蓝牙耳机','音响') THEN '音频设备'
            WHEN b.fcategory IN ('智能手表','智能手环') THEN '智能穿戴'
            ELSE '其他数码'
        END AS 产品类别,
        
        -- 渠道分类
        CASE 
            WHEN left(a.fseries_number, 2) = 'XY' THEN '闲鱼'
            WHEN left(a.fseries_number, 2) = 'ZF' THEN '支付宝'
            WHEN left(a.fseries_number, 2) = 'TM' THEN '天猫'
            WHEN left(a.fseries_number, 2) = 'JD' THEN '京东'
            ELSE '其他渠道'
        END AS 回收渠道,
        
        -- 业务指标
        COUNT(*) AS 回收订单数,
        SUM(a.fpay_out_price) / 100 AS 回收总金额_元,
        AVG(a.fpay_out_price) / 100 AS 平均回收单价_元,
        
        -- 状态统计
        SUM(CASE WHEN a.forder_status = 1 THEN 1 ELSE 0 END) AS 已完成订单数,
        SUM(CASE WHEN a.forder_status = 2 THEN 1 ELSE 0 END) AS 处理中订单数,
        SUM(CASE WHEN a.forder_status = 3 THEN 1 ELSE 0 END) AS 已取消订单数
        
    FROM drt.drt_my33310_recycle_t_order a
    LEFT JOIN drt.drt_my33310_recycle_t_xy_order_data b ON a.Forder_id = b.Forder_id
    WHERE a.forder_time >= '2024-01-01 00:00:00'
        AND a.ftest = 0  -- 排除测试订单
        AND a.fpay_out_price > 0  -- 有实际支付金额
    GROUP BY 
        substr(a.forder_time, 1, 7),
        substr(a.forder_time, 1, 10),
        CASE 
            WHEN b.fcategory IN ('平板','平板电脑') THEN '平板'
            WHEN b.fcategory IN ('笔记本','笔记本电脑') THEN '笔记本'
            WHEN b.fcategory IN ('手机','') THEN '手机'
            WHEN b.fcategory IN ('单反相机','数码相机','微单相机') THEN '相机'
            WHEN b.fcategory IN ('耳机','蓝牙耳机','音响') THEN '音频设备'
            WHEN b.fcategory IN ('智能手表','智能手环') THEN '智能穿戴'
            ELSE '其他数码'
        END,
        CASE 
            WHEN left(a.fseries_number, 2) = 'XY' THEN '闲鱼'
            WHEN left(a.fseries_number, 2) = 'ZF' THEN '支付宝'
            WHEN left(a.fseries_number, 2) = 'TM' THEN '天猫'
            WHEN left(a.fseries_number, 2) = 'JD' THEN '京东'
            ELSE '其他渠道'
        END
),

-- 第二部分：销售业务收入分析
销售收入 AS (
    SELECT
        -- 时间维度
        substr(s.fpay_time, 1, 7) AS 销售月份,
        substr(s.fpay_time, 1, 10) AS 销售日期,
        
        -- 销售平台
        CASE 
            WHEN s.Forder_platform = 5 THEN '鱼市'
            WHEN s.Forder_platform = 1 THEN '自有平台'
            WHEN s.Forder_platform = 6 THEN '采货侠'
            ELSE '其他平台'
        END AS 销售平台,
        
        -- 产品分类（通过关联回收订单获取）
        CASE 
            WHEN x.fcategory IN ('平板','平板电脑') THEN '平板'
            WHEN x.fcategory IN ('笔记本','笔记本电脑') THEN '笔记本'
            WHEN x.fcategory IN ('手机','') THEN '手机'
            WHEN x.fcategory IN ('单反相机','数码相机','微单相机') THEN '相机'
            WHEN x.fcategory IN ('耳机','蓝牙耳机','音响') THEN '音频设备'
            WHEN x.fcategory IN ('智能手表','智能手环') THEN '智能穿戴'
            ELSE '其他数码'
        END AS 产品类别,
        
        -- 业务指标
        COUNT(*) AS 销售订单数,
        SUM(s.Foffer_price) / 100 AS 销售总金额_元,
        AVG(s.Foffer_price) / 100 AS 平均销售单价_元,
        SUM(s.fcost_price) / 100 AS 成本总金额_元,
        SUM(s.Foffer_price - s.fcost_price) / 100 AS 毛利润_元,
        
        -- 利润率计算
        CASE 
            WHEN SUM(s.fcost_price) > 0 
            THEN ROUND(SUM(s.Foffer_price - s.fcost_price) * 100.0 / SUM(s.fcost_price), 2)
            ELSE 0 
        END AS 毛利率_百分比
        
    FROM dws.dws_jp_order_detail s
    LEFT JOIN drt.drt_my33310_recycle_t_order o ON s.fseries_number = o.fseries_number
    LEFT JOIN drt.drt_my33310_recycle_t_xy_order_data x ON o.Forder_id = x.Forder_id
    WHERE s.Forder_status IN (2,3,4,6)  -- 已完成的销售订单
        AND s.Fpay_time IS NOT NULL
        AND s.Fpay_time >= '2024-01-01 00:00:00'
    GROUP BY 
        substr(s.fpay_time, 1, 7),
        substr(s.fpay_time, 1, 10),
        CASE 
            WHEN s.Forder_platform = 5 THEN '鱼市'
            WHEN s.Forder_platform = 1 THEN '自有平台'
            WHEN s.Forder_platform = 6 THEN '采货侠'
            ELSE '其他平台'
        END,
        CASE 
            WHEN x.fcategory IN ('平板','平板电脑') THEN '平板'
            WHEN x.fcategory IN ('笔记本','笔记本电脑') THEN '笔记本'
            WHEN x.fcategory IN ('手机','') THEN '手机'
            WHEN x.fcategory IN ('单反相机','数码相机','微单相机') THEN '相机'
            WHEN x.fcategory IN ('耳机','蓝牙耳机','音响') THEN '音频设备'
            WHEN x.fcategory IN ('智能手表','智能手环') THEN '智能穿戴'
            ELSE '其他数码'
        END
),

-- 第三部分：渠道佣金收入分析
渠道佣金 AS (
    SELECT
        -- 时间维度
        substr(b.Forder_time, 1, 7) AS 佣金月份,
        substr(b.Forder_time, 1, 10) AS 佣金日期,
        
        -- 渠道分类
        CASE 
            WHEN b.Fxy_channel = 'idle' THEN '闲鱼'
            WHEN b.Fxy_channel = 'tmall-service' THEN '天猫服务'
            ELSE '其他渠道'
        END AS 佣金渠道,
        
        -- 产品分类
        CASE 
            WHEN b.fcategory IN ('平板','平板电脑') THEN '平板'
            WHEN b.fcategory IN ('笔记本','笔记本电脑') THEN '笔记本'
            WHEN b.fcategory IN ('手机','') THEN '手机'
            WHEN b.fcategory IN ('单反相机','数码相机','微单相机') THEN '相机'
            WHEN b.fcategory IN ('耳机','蓝牙耳机','音响') THEN '音频设备'
            WHEN b.fcategory IN ('智能手表','智能手环') THEN '智能穿戴'
            ELSE '其他数码'
        END AS 产品类别,
        
        -- 佣金计算
        COUNT(*) AS 佣金订单数,
        SUM(b.fquote_price) / 100 AS 预估总金额_元,
        SUM(
            CASE 
                WHEN b.fquote_price * 0.015 / 100 < 5 THEN 5
                WHEN b.fquote_price * 0.015 / 100 > 25 THEN 25
                ELSE b.fquote_price * 0.015 / 100
            END
        ) AS 佣金总金额_元,
        AVG(
            CASE 
                WHEN b.fquote_price * 0.015 / 100 < 5 THEN 5
                WHEN b.fquote_price * 0.015 / 100 > 25 THEN 25
                ELSE b.fquote_price * 0.015 / 100
            END
        ) AS 平均佣金_元
        
    FROM drt.drt_my33310_recycle_t_order a
    INNER JOIN drt.drt_my33310_recycle_t_xy_order_data b ON a.Forder_id = b.Forder_id
    WHERE b.Forder_time >= '2024-01-01 00:00:00'
        AND b.Fxy_channel IN ('idle', 'tmall-service')
        AND a.fchannel_id != 10001191  -- 排除特定渠道
        AND b.fcategory NOT IN ('黄金')  -- 排除黄金业务
    GROUP BY 
        substr(b.Forder_time, 1, 7),
        substr(b.Forder_time, 1, 10),
        CASE 
            WHEN b.Fxy_channel = 'idle' THEN '闲鱼'
            WHEN b.Fxy_channel = 'tmall-service' THEN '天猫服务'
            ELSE '其他渠道'
        END,
        CASE 
            WHEN b.fcategory IN ('平板','平板电脑') THEN '平板'
            WHEN b.fcategory IN ('笔记本','笔记本电脑') THEN '笔记本'
            WHEN b.fcategory IN ('手机','') THEN '手机'
            WHEN b.fcategory IN ('单反相机','数码相机','微单相机') THEN '相机'
            WHEN b.fcategory IN ('耳机','蓝牙耳机','音响') THEN '音频设备'
            WHEN b.fcategory IN ('智能手表','智能手环') THEN '智能穿戴'
            ELSE '其他数码'
        END
),

-- 第四部分：综合财务指标汇总
财务汇总 AS (
    SELECT
        COALESCE(r.回收月份, s.销售月份, c.佣金月份) AS 统计月份,
        COALESCE(r.产品类别, s.产品类别, c.产品类别) AS 产品类别,
        
        -- 回收业务指标
        SUM(COALESCE(r.回收订单数, 0)) AS 回收订单数,
        SUM(COALESCE(r.回收总金额_元, 0)) AS 回收总金额_元,
        SUM(COALESCE(r.平均回收单价_元, 0)) AS 平均回收单价_元,
        
        -- 销售业务指标
        SUM(COALESCE(s.销售订单数, 0)) AS 销售订单数,
        SUM(COALESCE(s.销售总金额_元, 0)) AS 销售总金额_元,
        SUM(COALESCE(s.成本总金额_元, 0)) AS 成本总金额_元,
        SUM(COALESCE(s.毛利润_元, 0)) AS 毛利润_元,
        AVG(COALESCE(s.毛利率_百分比, 0)) AS 平均毛利率_百分比,
        
        -- 渠道佣金指标
        SUM(COALESCE(c.佣金订单数, 0)) AS 佣金订单数,
        SUM(COALESCE(c.佣金总金额_元, 0)) AS 佣金总金额_元,
        
        -- 综合财务指标
        SUM(COALESCE(s.毛利润_元, 0)) + SUM(COALESCE(c.佣金总金额_元, 0)) AS 总利润_元,
        
        -- 业务转化率
        CASE 
            WHEN SUM(COALESCE(r.回收订单数, 0)) > 0 
            THEN ROUND(SUM(COALESCE(s.销售订单数, 0)) * 100.0 / SUM(COALESCE(r.回收订单数, 0)), 2)
            ELSE 0 
        END AS 回收转销售转化率_百分比
        
    FROM 回收收入 r
    FULL OUTER JOIN 销售收入 s ON r.回收月份 = s.销售月份 AND r.产品类别 = s.产品类别
    FULL OUTER JOIN 渠道佣金 c ON COALESCE(r.回收月份, s.销售月份) = c.佣金月份 
                                   AND COALESCE(r.产品类别, s.产品类别) = c.产品类别
    GROUP BY 
        COALESCE(r.回收月份, s.销售月份, c.佣金月份),
        COALESCE(r.产品类别, s.产品类别, c.产品类别)
)

-- 最终输出：财务营收分析报表
SELECT 
    统计月份,
    产品类别,
    
    -- 回收业务
    回收订单数,
    ROUND(回收总金额_元, 2) AS 回收总金额_元,
    ROUND(平均回收单价_元, 2) AS 平均回收单价_元,
    
    -- 销售业务
    销售订单数,
    ROUND(销售总金额_元, 2) AS 销售总金额_元,
    ROUND(成本总金额_元, 2) AS 成本总金额_元,
    ROUND(毛利润_元, 2) AS 毛利润_元,
    ROUND(平均毛利率_百分比, 2) AS 平均毛利率_百分比,
    
    -- 渠道佣金
    佣金订单数,
    ROUND(佣金总金额_元, 2) AS 佣金总金额_元,
    
    -- 综合指标
    ROUND(总利润_元, 2) AS 总利润_元,
    ROUND(回收转销售转化率_百分比, 2) AS 回收转销售转化率_百分比,
    
    -- 利润率分析
    CASE 
        WHEN 销售总金额_元 > 0 
        THEN ROUND(总利润_元 * 100.0 / 销售总金额_元, 2)
        ELSE 0 
    END AS 综合利润率_百分比
    
FROM 财务汇总
ORDER BY 统计月份 DESC, 产品类别
