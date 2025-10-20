-- 机器人播报3调整版：汇总统计回收业务和验货宝业务数据
-- 功能：按仓库汇总显示各业务类型的数量和总数（去掉寄卖业务）

SELECT 
    fwarehouse,
    SUM(CASE WHEN ftype="回收" THEN getnum ELSE 0 END) AS huishou,
    SUM(CASE WHEN ftype="太力" THEN getnum ELSE 0 END) AS taili,
    SUM(CASE WHEN ftype="验货宝" THEN getnum ELSE 0 END) AS yanji,
    SUM(getnum) AS 总数
FROM (
    -- 第一部分：验货宝业务数据
    SELECT
        CASE 
            WHEN left(a.fhost_barcode,3) LIKE "%020%" THEN "杭州仓"
            ELSE "深圳仓" 
        END AS fwarehouse, 
        "验货宝" AS ftype,
        COUNT(a.fhost_barcode) AS getnum
    FROM drt.drt_my33315_xy_detect_t_xy_hsb_order AS a
    LEFT JOIN drt.drt_my33315_xy_detect_t_xy_detect_receive_record AS b ON a.forder_id=b.forder_id
    WHERE b.Freceive_time >= to_date(date_sub(from_unixtime(unix_timestamp()),0))
    GROUP BY 1,2
    
    UNION ALL
    
    -- 第二部分：回收业务数据
    SELECT 
        CASE 
            WHEN right(left(a.fseries_number,6),2)="16" THEN "杭州仓"   
            ELSE "深圳仓" 
        END AS fwarehouse,
        CASE 
            WHEN left(a.fseries_number,2)="CG" OR left(a.fseries_number,2)="TL" THEN "太力"
            ELSE "回收" 
        END AS ftype,
        COUNT(a.fseries_number) AS getnum
    FROM drt.drt_my33310_recycle_t_order AS a
    LEFT JOIN drt.drt_my33310_recycle_t_channel AS b ON a.fchannel_id=b.fchannel_id
    WHERE a.Ftest=0
        AND a.Frecycle_type=1
        AND a.Fgetin_time >= to_date(date_sub(from_unixtime(unix_timestamp()),0))
        AND left(a.fseries_number,2) <>"BB" 
    GROUP BY 1,2
) A
GROUP BY fwarehouse
ORDER BY fwarehouse;
