-- 数据诊断查询：检查9月17日数据情况
-- 目的：诊断为什么看不到9月17日的数据

-- 1. 检查forder_time（订单时间）的最新数据
SELECT 
    'forder_time检查' as 检查项,
    MAX(forder_time) as 最新订单时间,
    COUNT(*) as 记录数,
    MIN(forder_time) as 最早订单时间
FROM dws.dws_xy_yhb_detail 
WHERE forder_time >= to_date(date_sub(from_unixtime(unix_timestamp()),7))
AND left(fhost_barcode,2) in ('01','02')

UNION ALL

-- 2. 检查detect_put_time（检测放入时间）的最新数据
SELECT 
    'detect_put_time检查' as 检查项,
    MAX(fdetect_put_time) as 最新检测放入时间,
    COUNT(*) as 记录数,
    MIN(fdetect_put_time) as 最早检测放入时间
FROM dws.dws_xy_yhb_detail 
WHERE forder_time >= to_date(date_sub(from_unixtime(unix_timestamp()),7))
AND left(fhost_barcode,2) in ('01','02')

UNION ALL

-- 3. 检查最近3天的数据分布
SELECT 
    '最近3天分布' as 检查项,
    to_date(forder_time) as 日期,
    COUNT(*) as 记录数,
    NULL as 最早时间
FROM dws.dws_xy_yhb_detail 
WHERE forder_time >= to_date(date_sub(from_unixtime(unix_timestamp()),3))
AND left(fhost_barcode,2) in ('01','02')
GROUP BY to_date(forder_time)
ORDER BY 日期 DESC

UNION ALL

-- 4. 检查detect_put_time的最近3天分布
SELECT 
    'detect_put_time最近3天' as 检查项,
    to_date(fdetect_put_time) as 日期,
    COUNT(*) as 记录数,
    NULL as 最早时间
FROM dws.dws_xy_yhb_detail 
WHERE forder_time >= to_date(date_sub(from_unixtime(unix_timestamp()),3))
AND left(fhost_barcode,2) in ('01','02')
AND fdetect_put_time IS NOT NULL
GROUP BY to_date(fdetect_put_time)
ORDER BY 日期 DESC;
