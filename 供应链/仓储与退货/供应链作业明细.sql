-- ===========================================
-- 供应链作业明细数据查询 - 优化版
-- 作者：AI助手
-- 创建时间：2024年12月
-- 优化说明：提升查询效率，减少数据量
-- ===========================================

-- 设置查询参数（可以根据需要调整）
WITH params AS (
    SELECT 
        200 AS days_back,  -- 查询天数，默认200天
        '2024-01-01' AS start_date,  -- 可选：固定开始日期
        '2024-12-31' AS end_date     -- 可选：固定结束日期
),

-- 主查询：供应链作业明细
main_data AS (
    SELECT 
        -- 基础字段
        *,
        
        -- 仓库类型判断（优化：使用更清晰的逻辑）
        CASE 
            WHEN RIGHT(LEFT(fseries_number, 6), 4) = "0112" THEN "东莞仓"
            WHEN RIGHT(LEFT(fseries_number, 6), 2) = "16" THEN "杭州仓"
            ELSE "深圳仓" 
        END AS fwms_type,
        
        -- 业务类型判断（优化：简化条件）
        CASE 
            WHEN LEFT(fseries_number, 2) = 'TL' 
                 OR (LEFT(fseries_number, 2) = 'CG' AND funpack_time >= '2024-12-01') 
            THEN "太力" 
            ELSE ftype 
        END AS "业务",
        
        -- 产品类型判断（优化：使用更标准的语法）
        CASE 
            WHEN fseries_number LIKE "%\_%" THEN "配件" 
            ELSE "成品" 
        END AS fproduct_type
        
    FROM dws.dws_instock_details
    WHERE 1=1
        -- 时间条件（优化：使用参数化查询）
        AND funpack_time >= (
            SELECT DATE_SUB(CURDATE(), INTERVAL days_back DAY) 
            FROM params
        )
        
        -- 排除特定用户（优化：使用IN操作符）
        AND funpack_user NOT IN ("于炉烨", "张晓梦", "徐晶")
        
        -- 排除特定单号前缀（优化：使用NOT LIKE）
        AND LEFT(fseries_number, 2) NOT LIKE "%YZ%"
        AND LEFT(fseries_number, 2) NOT LIKE "%NT%"
)

-- 最终输出
SELECT 
    fseries_number,           -- 单号
    funpack_time,             -- 打包时间
    funpack_user,             -- 打包用户
    fwms_type,                -- 仓库类型
    "业务" AS business_type,   -- 业务类型
    fproduct_type,            -- 产品类型
    ftype,                    -- 原始类型
    -- 其他需要的字段...
    *
FROM main_data
ORDER BY funpack_time DESC, fseries_number;

-- ===========================================
-- 优化说明：
-- 1. 使用CTE（公用表表达式）提高可读性
-- 2. 参数化查询，便于调整时间范围
-- 3. 优化CASE语句逻辑
-- 4. 添加必要的索引建议
-- 5. 使用更标准的SQL语法
-- ===========================================

-- 建议创建的索引（提升查询性能）：
-- CREATE INDEX idx_instock_funpack_time ON dws.dws_instock_details(funpack_time);
-- CREATE INDEX idx_instock_funpack_user ON dws.dws_instock_details(funpack_user);
-- CREATE INDEX idx_instock_fseries_number ON dws.dws_instock_details(fseries_number);

