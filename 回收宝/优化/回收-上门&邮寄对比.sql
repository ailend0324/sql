-- 回收-上门&邮寄对比：按天、渠道、地区、品类、寄送方式、报价区间统计
-- 指标：下单量（ordernum）、已检测单量（detect_num）、已打款单量（pay_num）、打款金额（pay_price）
-- 数据源：dws_hs_order_detail_al（回收订单明细汇总表）
-- 口径：排除巡检与天猫服务渠道、排除测试单；仅保留报价>=300的订单；范围自 2020-11-01 至今日
SELECT 
    substr(a.forder_time, 1, 10) AS Ftime_byday, -- 下单日期（yyyy-MM-dd）
    substr(a.fgetin_time, 1, 10) AS Fgetin_time, -- 修正：移除表别名前缀
    a.fxy_channel, -- 主渠道
    a.fsub_channel, -- 子渠道
    a.fprovince, -- 省份
    a.fcity, -- 城市
    CASE -- 品类归一化
        WHEN a.fcategory IN (NULL, '手机') THEN '手机'
        WHEN a.fcategory IN ('平板', '平板电脑') THEN '平板'
        WHEN a.fcategory IN ('笔记本', '笔记本电脑') THEN '笔记本电脑'
        ELSE '手机'
    END AS fcategory, -- 标准化后的品类
    a.fship_type, -- 寄送方式（如：上门/邮寄）
    a.fwarehouse_code,
    SUM(a.fpcs) AS ordernum, -- 下单量（件数）
    sum(if(a.fsync_recv_time != '0000-00-00 00:00:00.0', a.fpcs, 0)) AS getin_num,
    SUM(IF(a.fsync_detect_time != '0000-00-00 00:00:00.0', a.fpcs, 0)) AS detect_num, -- 已检测单量
    SUM(IF(a.fsync_pay_out_time != '0000-00-00 00:00:00.0', a.fpcs, 0)) AS pay_num, -- 已打款单量
    SUM(IF(a.fsync_pay_out_time != '0000-00-00 00:00:00.0', a.fpay_out_price, 0)) AS pay_price -- 打款金额
FROM dws_hs_order_detail_al AS a
WHERE a.forder_time >= from_timestamp(date_sub(current_timestamp(), INTERVAL 30 DAY), 'yyyy-MM-dd')
    AND a.fsub_channel NOT IN ('xunjian') -- 排除"巡检"子渠道
    AND a.ftest = 0 -- 排除测试订单
GROUP BY 
    Ftime_byday,
    Fgetin_time, -- 对应修正后的别名
    a.fxy_channel,
    a.fsub_channel,
    a.fprovince,
    a.fcity,
    fcategory,
    a.fship_type,
    a.fwarehouse_code
