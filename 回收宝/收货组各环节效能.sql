WITH seal_bag AS (
  SELECT
    *
  FROM (
    SELECT
      a.fseries_number,
      a.fwarehouse_name,
      a.fadd_time,
      b.freal_name,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY fadd_time DESC) AS num
    FROM drt.drt_my33310_hsb_wms_t_seal_bag_log AS a
    LEFT JOIN drt.drt_my33310_amcdb_t_user        AS b
      ON a.fadd_user = b.fusername
  ) t
  WHERE num = 1
),
allot AS (
  SELECT
    *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fbar_code ORDER BY fadd_time DESC) AS num
    FROM drt.drt_my33310_xywms_t_product_allot
  ) t
  WHERE num = 1
)

-- ① 验机/回收拆包
SELECT
  funpack_time                                           AS ftimeby,
  LEFT(fseries_number, 2)                                AS channel,
  CASE
    WHEN LEFT(fseries_number, 2) IN ('02')
      OR RIGHT(LEFT(fseries_number, 6), 2) = '16' THEN '杭州'
    ELSE '深圳'
  END                                                    AS fwarehouse,
  CASE
    WHEN Funpack_user = '张雄均' THEN '张小凤'
    ELSE funpack_user
  END                                                    AS operator,
  CASE
    WHEN ftype = '回收'
      THEN IF(CONCAT(ftype, '拆包', Fcategory_name) IS NULL,
              CONCAT(ftype, '拆包'),
              CONCAT(ftype, '拆包', Fcategory_name))
    WHEN ftype = '验机' 
       AND Fcategory_name IN ('平板', '平板电脑', 'iPad') THEN
    '验机拆包平板'
    ELSE CONCAT(ftype, '拆包')
  END                                                    AS ftype,
  COUNT(
    IF(
      ftype = '验机' AND TO_DATE(funpack_time) >= '2023-09-20',
      NULL,
      IF(ftype = '回收' AND TO_DATE(funpack_time) >= '2023-10-21', NULL, fseries_number)
    )
  )                                                      AS num,
  COUNT(IF(HOUR(funpack_time) <  18, fseries_number, NULL)) AS "加班前数量",
  COUNT(IF(HOUR(funpack_time) >= 18, fseries_number, NULL)) AS "加班后数量"
FROM dws.dws_instock_details
WHERE fseries_number IS NOT NULL
  AND funpack_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
  AND Funpack_user IS NOT NULL
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- ② 验机配件收货（包裹）
SELECT
  t.freceive_time                                        AS ftimeby,
  LEFT(a.fparts_bar_code, 2)                             AS channel,
  CASE
    WHEN LEFT(a.fparts_bar_code, 2) IN ('02')
      OR RIGHT(LEFT(a.fparts_bar_code, 6), 2) = '16' THEN '杭州'
    ELSE '深圳'
  END                                                    AS fwarehouse,
  CASE
    WHEN t.freceive_user = '张雄均' THEN '张小凤'
    ELSE t.freceive_user
  END                                                    AS operator,
  '验机配件收货'                                           AS ftype,
  COUNT(DISTINCT a.fparts_bar_code)                      AS num,
  COUNT(DISTINCT IF(HOUR(t.freceive_time) <  18, a.fparts_bar_code, NULL)) AS "加班前数量",
  COUNT(DISTINCT IF(HOUR(t.freceive_time) >= 18, a.fparts_bar_code, NULL)) AS "加班后数量"
FROM drt.drt_my33310_xywms_t_parcel AS a
LEFT JOIN (
  SELECT
    fparcel_id,
    fadd_time  AS freceive_time,
    fadd_user  AS freceive_user,
    ROW_NUMBER() OVER (PARTITION BY fparcel_id ORDER BY fadd_time DESC) AS num
  FROM drt.drt_my33310_xywms_t_parcel_log
  WHERE ftype = 3
) t
  ON a.fid = t.fparcel_id
WHERE t.num = 1
  AND t.freceive_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
  AND a.fparts_bar_code IS NOT NULL
  AND a.fparts_bar_code <> ''
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- ③ 收货（主机）
SELECT
  freceive_time                                          AS ftimeby,
  LEFT(fseries_number, 2)                                AS channel,
  CASE
    WHEN LEFT(fseries_number, 2) IN ('02')
      OR RIGHT(LEFT(fseries_number, 6), 2) = '16' THEN '杭州'
    ELSE '深圳'
  END                                                    AS fwarehouse,
  CASE
    WHEN TO_DATE(freceive_time) = '2024-05-29'
      AND freceive_user = '朱小洋'
      AND LEFT(fseries_number, 2) = '01' THEN '李伟豪'
    WHEN freceive_user = '张雄均' THEN '张小凤'
    ELSE freceive_user
  END                                                    AS operator,
  CASE
    WHEN fseries_number LIKE '%\_%' THEN CONCAT(ftype, '配件收货')
    WHEN LEFT(fseries_number, 2) = 'TL' THEN '太力收货'
    WHEN ftype = '回收'
      AND fcategory_name IN ('CPU','内存条','显卡','智能手写笔','游戏机','电子书','电脑主板','固态硬盘','显示器')
      THEN CONCAT(ftype, '收货', '电脑配件')
    WHEN ftype = '回收'
      AND fcategory_name NOT IN ('手机','平板','平板电脑','笔记本','笔记本电脑','耳机','配件','充电套装','智能手表','智能手环')
      THEN CONCAT(ftype, '收货', '相机及其它')
    WHEN ftype = '回收'
      THEN IF(CONCAT(ftype, '收货', Fcategory_name) IS NULL,
               CONCAT(ftype, '收货'),
               CONCAT(ftype, '收货', Fcategory_name))
    ELSE CONCAT(ftype, '收货')
  END                                                    AS ftype,
  COUNT(fseries_number)                                  AS num,
  COUNT(IF(HOUR(freceive_time) <  18, fseries_number, NULL)) AS "加班前数量",
  COUNT(IF(HOUR(freceive_time) >= 18, fseries_number, NULL)) AS "加班后数量"
FROM dws.dws_instock_details
WHERE fseries_number IS NOT NULL
  AND freceive_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
  AND freceive_user IS NOT NULL
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- ④ 主图拍照
SELECT
  fmain_photo_time                                       AS ftimeby,
  LEFT(fseries_number, 2)                                AS channel,
  CASE
    WHEN LEFT(fseries_number, 2) IN ('02')
      OR RIGHT(LEFT(fseries_number, 6), 2) = '16' THEN '杭州'
    ELSE '深圳'
  END                                                    AS fwarehouse,
  CASE
    WHEN TO_DATE(fmain_photo_time) = '2024-02-03'
      AND fphoto_name = '杨泽文' THEN '朱小洋'
    WHEN (TO_DATE(fmain_photo_time) BETWEEN '2024-02-26' AND '2024-02-27')
      AND fphoto_name = '陈乐娟' THEN '高华铎'
    WHEN TO_DATE(fmain_photo_time) = '2024-03-04'
      AND fphoto_name = '陈冬凡' THEN '周远鸿'
    WHEN TO_DATE(fmain_photo_time) = '2024-03-04'
      AND fphoto_name = '胡家华' THEN '黄成水'
    WHEN fphoto_name = '张雄均' THEN '张小凤'
    WHEN TO_DATE(fmain_photo_time) = '2024-11-05'
      AND HOUR(fmain_photo_time) < 16
      AND fphoto_name = '汤珂' THEN NULL
    ELSE fphoto_name
  END                                                    AS operator,
  CASE
    WHEN ftype = '验机' THEN CONCAT(ftype, '拍照')
    WHEN fcategory_name IN ('CPU','内存条','显卡','智能手写笔','游戏机','电子书','电脑主板','固态硬盘','显示器')
      THEN CONCAT('拍照', '电脑配件')
    WHEN fcategory_name NOT IN ('手机','平板','平板电脑','笔记本','笔记本电脑','耳机','配件','充电套装','智能手表','智能手环')
      THEN CONCAT('拍照', '相机及其它')
    ELSE IF(CONCAT('拍照', Fcategory_name) IS NULL, '拍照', CONCAT('拍照', Fcategory_name))
  END                                                    AS ftype,
  COUNT(fseries_number)                                  AS num,
  COUNT(IF(HOUR(fmain_photo_time) <  18, fseries_number, NULL)) AS "加班前数量",
  COUNT(IF(HOUR(fmain_photo_time) >= 18, fseries_number, NULL)) AS "加班后数量"
FROM dws.dws_instock_details
WHERE fseries_number IS NOT NULL
  AND fmain_photo_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
  AND fphoto_name IS NOT NULL
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- ⑤ 回收转寄卖拍照（缺陷照）
SELECT
  fdefect_photo_time                                     AS ftimeby,
  LEFT(fseries_number, 2)                                AS channel,
  CASE
    WHEN LEFT(fseries_number, 2) IN ('02')
      OR RIGHT(LEFT(fseries_number, 6), 2) = '16' THEN '杭州'
    ELSE '深圳'
  END                                                    AS fwarehouse,
  fdefect_photo_name                                     AS operator,
  '回收转寄卖拍照'                                          AS ftype,
  COUNT(fseries_number)                                  AS num,
  COUNT(IF(HOUR(fdefect_photo_time) <  18, fseries_number, NULL)) AS "加班前数量",
  COUNT(IF(HOUR(fdefect_photo_time) >= 18, fseries_number, NULL)) AS "加班后数量"
FROM dws.dws_instock_details
WHERE fseries_number IS NOT NULL
  AND fdefect_photo_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
  AND fdefect_photo_name IS NOT NULL
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- ⑥ 防拆标
SELECT
  Ftamper_time                                           AS ftimeby,
  LEFT(fseries_number, 2)                                AS channel,
  CASE
    WHEN LEFT(fseries_number, 2) IN ('02')
      OR RIGHT(LEFT(fseries_number, 6), 2) = '16' THEN '杭州'
    ELSE '深圳'
  END                                                    AS fwarehouse,
  Foperator                                              AS operator,
  '防拆标'                                                AS ftype,
  COUNT(fseries_number)                                  AS num,
  COUNT(IF(HOUR(Ftamper_time) <  18, fseries_number, NULL)) AS "加班前数量",
  COUNT(IF(HOUR(Ftamper_time) >= 18, fseries_number, NULL)) AS "加班后数量"
FROM dws.dws_instock_details
WHERE fseries_number IS NOT NULL
  AND Ftamper_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
  AND Foperator IS NOT NULL
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- ⑦ 寄卖打印
SELECT
  fplus_print_time                                       AS ftimeby,
  LEFT(fseries_number, 2)                                AS channel,
  CASE
    WHEN LEFT(fseries_number, 2) IN ('02')
      OR RIGHT(LEFT(fseries_number, 6), 2) = '16' THEN '杭州'
    ELSE '深圳'
  END                                                    AS fwarehouse,
  CASE
    WHEN TO_DATE(fplus_print_time) = '2024-06-05'
      AND fplus_printer = '吴依卓' THEN NULL
    ELSE fplus_printer
  END                                                    AS operator,
  '寄卖打印'                                               AS ftype,
  COUNT(fseries_number)                                  AS num,
  COUNT(IF(HOUR(fplus_print_time) <  18, fseries_number, NULL)) AS "加班前数量",
  COUNT(IF(HOUR(fplus_print_time) >= 18, fseries_number, NULL)) AS "加班后数量"
FROM dws.dws_instock_details
WHERE fseries_number IS NOT NULL
  AND fplus_print_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
  AND fplus_printer IS NOT NULL
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- ⑧ 装密封袋（取 seal_bag）
SELECT
  FROM_UNIXTIME(fadd_time)                               AS ftimeby,
  LEFT(fseries_number, 2)                                AS channel,
  CASE
    WHEN LEFT(fseries_number, 2) IN ('02')
      OR RIGHT(LEFT(fseries_number, 6), 2) = '16' THEN '杭州'
    ELSE '深圳'
  END                                                    AS fwarehouse,
  CASE
    WHEN TO_DATE(FROM_UNIXTIME(fadd_time)) = '2024-06-05'
      AND freal_name = '吴依卓' THEN NULL
    ELSE freal_name
  END                                                    AS operator,
  '装密封袋'                                               AS ftype,
  COUNT(fseries_number)                                  AS num,
  COUNT(IF(HOUR(FROM_UNIXTIME(fadd_time)) <  18, fseries_number, NULL)) AS "加班前数量",
  COUNT(IF(HOUR(FROM_UNIXTIME(fadd_time)) >= 18, fseries_number, NULL)) AS "加班后数量"
FROM seal_bag
WHERE fseries_number IS NOT NULL
  AND FROM_UNIXTIME(fadd_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
  AND freal_name IS NOT NULL
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- ⑨ 验机出库
SELECT
  fput_time                                              AS ftimeby,
  LEFT(fhost_barcode, 2)                                 AS channel,
  CASE
    WHEN LEFT(fhost_barcode, 2) IN ('02')
      OR RIGHT(LEFT(fhost_barcode, 6), 2) = '16' THEN '杭州'
    ELSE '深圳'
  END                                                    AS fwarehouse,
  fput_user                                              AS operator,
  '验机出库'                                               AS ftype,
  COUNT(fhost_barcode)                                   AS num,
  COUNT(IF(HOUR(fput_time) <  18, fhost_barcode, NULL))  AS "加班前数量",
  COUNT(IF(HOUR(fput_time) >= 18, fhost_barcode, NULL))  AS "加班后数量"
FROM dws.dws_xy_yhb_detail
WHERE fhost_barcode IS NOT NULL
  AND fput_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
  AND fput_user IS NOT NULL
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- ⑩ 验机调拨（allot：新增）
SELECT
  fadd_time                                              AS ftimeby,
  LEFT(fbar_code, 2)                                     AS channel,
  CASE
    WHEN LEFT(fbar_code, 2) IN ('02')
      OR RIGHT(LEFT(fbar_code, 6), 2) = '16' THEN '杭州'
    ELSE '深圳'
  END                                                    AS fwarehouse,
  fadd_user                                              AS operator,
  '验机调拨'                                               AS ftype,
  COUNT(fbar_code)                                       AS num,
  COUNT(IF(HOUR(fadd_time) <  18, fbar_code, NULL))      AS "加班前数量",
  COUNT(IF(HOUR(fadd_time) >= 18, fbar_code, NULL))      AS "加班后数量"
FROM allot
WHERE fadd_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
  AND fbar_code IS NOT NULL
  AND fadd_user IS NOT NULL
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- ⑪ 验机上架（allot：修改）
SELECT
  fupdate_time                                           AS ftimeby,
  LEFT(fbar_code, 2)                                     AS channel,
  CASE
    WHEN LEFT(fbar_code, 2) IN ('02')
      OR RIGHT(LEFT(fbar_code, 6), 2) = '16' THEN '杭州'
    ELSE '深圳'
  END                                                    AS fwarehouse,
  fupdate_user                                           AS operator,
  '验机上架'                                               AS ftype,
  COUNT(fbar_code)                                       AS num,
  COUNT(IF(HOUR(fupdate_time) <  18, fbar_code, NULL))   AS "加班前数量",
  COUNT(IF(HOUR(fupdate_time) >= 18, fbar_code, NULL))   AS "加班后数量"
FROM allot
WHERE fupdate_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
  AND fbar_code IS NOT NULL
  AND fupdate_user IS NOT NULL
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- ⑫ 自动化（模块一/验机）
SELECT
  fcreate_time                                           AS ftimeby,
  LEFT(fserial_number, 2)                                AS fchannel,
  CASE
    WHEN LEFT(fserial_number, 2) IN ('02')
      OR RIGHT(LEFT(fserial_number, 6), 2) = '16' THEN '杭州'
    ELSE '深圳'
  END                                                    AS fwarehouse,
  CASE
    WHEN TO_DATE(fcreate_time) = '2023-10-16'
      AND Fbind_real_name = '刘俊' THEN '周利'
    WHEN TO_DATE(fcreate_time) BETWEEN '2023-11-01' AND '2023-11-21'
      AND Fbind_real_name = '郑佩文' THEN NULL
    WHEN TO_DATE(fcreate_time) = '2024-01-29'
      AND Fbind_real_name = '林嘉成' THEN NULL
    WHEN Fbind_real_name = '张雄均' THEN '张小凤'
    WHEN TO_DATE(fcreate_time) = '2025-04-14'
      AND Fbind_real_name = '严俊' THEN '林广泽'
    ELSE Fbind_real_name
  END                                                    AS freal_name,
  CASE
    WHEN LEFT(fserial_number, 2) IN ('01','02') THEN '验机-自动化检测'
    WHEN LEFT(fserial_number, 2) NOT IN ('01','02') AND fbrand_name = '苹果' THEN '回收-模块一-苹果'
    ELSE '回收-模块一-安卓'
  END                                                    AS ftype,
  COUNT(fserial_number)                                  AS num,
  COUNT(IF(HOUR(fcreate_time) <  18, fserial_number, NULL)) AS "加班前数量",
  COUNT(IF(HOUR(fcreate_time) >= 18, fserial_number, NULL)) AS "加班后数量"
FROM (
  SELECT
    a.fcreate_time,
    a.fserial_number,
    a.Fbind_real_name,
    a.fbrand_name,
    ROW_NUMBER() OVER (PARTITION BY a.fserial_number ORDER BY a.fcreate_time DESC) AS num
  FROM drt.drt_my33312_detection_t_automation_det_record AS a
  LEFT JOIN drt.drt_my33310_amcdb_t_user                 AS b
    ON a.fuser_id = b.fuser_id
  WHERE a.fserial_number IS NOT NULL
    AND a.fserial_number <> ''
    AND a.Fbind_real_name IS NOT NULL
    AND TO_DATE(a.fcreate_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
) t
WHERE num = 1
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- ⑬ 模块二（App 记录）
SELECT
  fcreate_time                                           AS ftimeby,
  LEFT(fserial_number, 2)                                AS fchannel,
  CASE
    WHEN LEFT(fserial_number, 2) IN ('02')
      OR RIGHT(LEFT(fserial_number, 6), 2) = '16' THEN '杭州'
    ELSE '深圳'
  END                                                    AS fwarehouse,
  CASE
    WHEN freal_name = '张雄均' THEN '张小凤'
    ELSE freal_name
  END                                                    AS freal_name,
  CASE
    WHEN LEFT(fserial_number, 2) IN ('01','02') THEN '验机-模块二'
    WHEN LEFT(fserial_number, 2) NOT IN ('01','02') AND fbrand_name = 'Apple' THEN '回收-模块二-苹果'
    ELSE '回收-模块二-安卓'
  END                                                    AS ftype,
  COUNT(fserial_number)                                  AS num,
  COUNT(IF(HOUR(fcreate_time) <  18, fserial_number, NULL)) AS "加班前数量",
  COUNT(IF(HOUR(fcreate_time) >= 18, fserial_number, NULL)) AS "加班后数量"
FROM (
  SELECT
    a.fcreate_time,
    a.fserial_number,
    b.freal_name,
    a.fbrand_name,
    ROW_NUMBER() OVER (PARTITION BY a.fserial_number ORDER BY a.fcreate_time DESC) AS num
  FROM drt.drt_my33312_detection_t_det_app_record AS a
  LEFT JOIN drt.drt_my33310_amcdb_t_user          AS b
    ON a.fuser_id = b.fuser_id
  WHERE TO_DATE(a.fcreate_time) >= '2024-04-01'
    AND b.freal_name IS NOT NULL
    AND a.fserial_number IS NOT NULL
    AND a.fserial_number <> ''
    AND b.freal_name NOT IN ('黄成水','张圳强','吴琼','冯铭焕','胡涛','李俊锋','黄雅如','朱惠萍','林红','陈映熹','张世梅')
) t
WHERE num = 1
GROUP BY 1, 2, 3, 4, 5;