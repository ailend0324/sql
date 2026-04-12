-- ============================================
-- 竞拍售后明细（含鱼市 B2B 渠道）
-- 说明：
-- 1) 完整沿用《原始/竞拍售后明细数据.sql》的自有平台与采货侠两大分支；
-- 2) 新增 B2B 分支（forder_platform=5，近365天，不拼历史表），售后使用 drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales；
-- 3) B2B 检测模块窗口按365天独立配置（不影响原有自有/采货侠窗口）。
-- ============================================

WITH
/* =========================
 * A. 公共“检测/售后检测” CTE
 * ========================= */
detect AS (  -- 取最新检测明细数据，取检测人、检测模板（原口径保留）
  SELECT *
  FROM (
    SELECT
      a.fcreate_time,
      UPPER(a.fserial_number) AS fserial_number,
      a.fdet_tpl,
      a.freal_name,
      a.fend_time,
      a.fbrand_name,
      a.fdetection_object,
      a.fgoods_level,
      a.fwarehouse_code,
      ROW_NUMBER() OVER (PARTITION BY UPPER(a.fserial_number) ORDER BY a.fcreate_time ASC) AS num
    FROM drt.drt_my33310_detection_t_detect_record AS a
    LEFT JOIN (
      SELECT fseries_number, forder_create_time
      FROM (
        SELECT
          fseries_number,
          forder_create_time,
          ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time DESC) AS num
        FROM dws.dws_jp_order_detail
        WHERE ftest_show <> 1
          AND (fmerchant_jp = 0 OR fmerchant_jp IS NULL)
          AND forder_status IN (2,3,4,6)
          AND forder_create_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 400))
      ) t
      WHERE t.num = 1
    ) AS b ON UPPER(a.fserial_number) = b.fseries_number
    LEFT JOIN (
      SELECT freal_name, fposition_id
      FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY freal_name ORDER BY fcreate_time DESC) AS num
        FROM drt.drt_my33310_amcdb_t_user
      ) t
      WHERE num = 1
    ) AS c ON a.freal_name = c.freal_name
    WHERE a.fis_deleted = 0
      AND TO_DATE(a.fend_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 400))
      AND a.fend_time < b.forder_create_time
      AND c.fposition_id <> 129       -- 剔除入库组缺陷拍照的人员
  ) c
  WHERE c.num = 1
),

after_sale_detect AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fserial_number ORDER BY fend_time ASC) AS num
    FROM drt.drt_my33310_detection_t_detect_record
    WHERE fdet_type = 0
      AND fis_deleted = 0
      AND freport_type = 0
      AND fverdict <> '测试单'
      AND TO_DATE(fend_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 400))
      AND LEFT(fserial_number, 2) IN ('YZ','NT','JM')
  ) t
  WHERE num = 1
),

detect_one AS (
  SELECT
    UPPER(fserial_number)         AS fserial_number,
    freal_name                    AS fdetect_one_name,
    FROM_UNIXTIME(fend_det_time)  AS fdetect_one_time
  FROM (
    SELECT
      a.fserial_number,
      a.fend_det_time,
      b.freal_name,
      ROW_NUMBER() OVER (PARTITION BY UPPER(a.fserial_number) ORDER BY a.fend_det_time DESC) AS num
    FROM drt.drt_my33312_detection_t_automation_det_record AS a
    LEFT JOIN drt.drt_my33310_amcdb_t_user AS b ON a.fuser_name = b.fusername
    WHERE fserial_number <> ''
      AND fserial_number IS NOT NULL
      AND TO_DATE(FROM_UNIXTIME(a.fend_det_time)) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 800))
  ) t
  WHERE num = 1
),

detect_two AS (
  SELECT
    UPPER(fserial_number) AS fserial_number,
    freal_name            AS fdetect_two_name,
    fcreate_time          AS fdetect_two_time
  FROM (
    SELECT
      a.fcreate_time,
      a.fserial_number,
      b.freal_name,
      ROW_NUMBER() OVER (PARTITION BY UPPER(a.fserial_number) ORDER BY a.fcreate_time DESC) AS num
    FROM drt.drt_my33312_detection_t_det_app_record AS a
    LEFT JOIN drt.drt_my33310_amcdb_t_user AS b ON a.fuser_name = b.fusername
    WHERE TO_DATE(a.fcreate_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 800))
      AND fserial_number <> ''
      AND fserial_number IS NOT NULL
  ) t
  WHERE num = 1
),

detect_three AS (
  SELECT
    UPPER(fserial_number) AS fserial_number,
    freal_name            AS fdetect_three_name,
    fcreate_time          AS fdetect_three_time
  FROM (
    SELECT
      a.fcreate_time,
      a.fserial_number,
      b.freal_name,
      ROW_NUMBER() OVER (PARTITION BY UPPER(a.fserial_number) ORDER BY a.fcreate_time DESC) AS num
    FROM drt.drt_my33312_detection_t_det_task AS a
    LEFT JOIN drt.drt_my33312_detection_t_det_task_record AS b ON a.ftask_id = b.ftask_id
    WHERE TO_DATE(a.fend_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 800))
      AND b.fdet_sop_task_name LIKE '%外观%'
  ) t
  WHERE num = 1
),

detect_three_pingmu AS (
  SELECT
    UPPER(fserial_number) AS fserial_number,
    CASE WHEN freal_name = '李俊峰' THEN '李俊锋' ELSE freal_name END AS fdetect_three_name_pingmu,
    fcreate_time          AS fdetect_three_time_pingmu
  FROM (
    SELECT
      a.fcreate_time,
      a.fserial_number,
      b.freal_name,
      ROW_NUMBER() OVER (PARTITION BY UPPER(a.fserial_number) ORDER BY a.fcreate_time DESC) AS num
    FROM drt.drt_my33312_detection_t_det_task AS a
    LEFT JOIN drt.drt_my33312_detection_t_det_task_record AS b ON a.ftask_id = b.ftask_id
    WHERE TO_DATE(a.fend_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 800))
      AND b.fdet_sop_task_name LIKE '%屏幕%'
      AND b.fdet_sop_task_name <> '外观屏幕'
  ) t
  WHERE num = 1
),

detect_four AS (
  SELECT
    UPPER(fserial_number) AS fserial_number,
    freal_name            AS fdetect_four_name,
    fcreate_time          AS fdetect_four_time
  FROM (
    SELECT
      a.fcreate_time,
      a.fserial_number,
      b.freal_name,
      ROW_NUMBER() OVER (PARTITION BY UPPER(a.fserial_number) ORDER BY a.fcreate_time DESC) AS num
    FROM drt.drt_my33312_detection_t_det_task AS a
    LEFT JOIN drt.drt_my33312_detection_t_det_task_record AS b ON a.ftask_id = b.ftask_id
    WHERE TO_DATE(a.fend_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 800))
      AND b.fdet_sop_task_name LIKE '%拆修%'
  ) t
  WHERE num = 1
),

/* =========================
 * B. 竞拍（自有平台）及二次销售 CTE
 * ========================= */
jp_sale AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time DESC) AS num
    FROM dws.dws_jp_order_detail
    WHERE ftest_show <> 1
      AND TO_DATE(forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND forder_platform NOT IN (5,6)
      AND (fmerchant_jp = 0 OR fmerchant_jp IS NULL)
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 1

  UNION ALL

  SELECT *
  FROM (
    SELECT
      *,
      '' AS fys_b2b_series_number,
      0  AS fys_b2b_order_status,
      0  AS fys_b2b_order_platform,
      0  AS fys_b2b_foffer_price,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time DESC) AS num
    FROM dws.dws_jp_order_detail_history2023
    WHERE ftest_show <> 1
      AND TO_DATE(forder_create_time) BETWEEN '2023-01-01' AND TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 366))
      AND forder_platform NOT IN (5,6)
      AND (fmerchant_jp = 0 OR fmerchant_jp IS NULL)
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 1
),

jp_first_sale AS (  -- 第一次销售
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_jp_order_detail
    WHERE ftest_show <> 1
      AND TO_DATE(forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND forder_platform NOT IN (5,6)
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 1

  UNION ALL

  SELECT *
  FROM (
    SELECT
      *,
      '' AS fys_b2b_series_number,
      0  AS fys_b2b_order_status,
      0  AS fys_b2b_order_platform,
      0  AS fys_b2b_foffer_price,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_jp_order_detail_history2023
    WHERE ftest_show <> 1
      AND TO_DATE(forder_create_time) BETWEEN '2023-01-01' AND TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 366))
      AND forder_platform NOT IN (5,6)
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 1
),

jp_second_sale AS (  -- 二次销售
  SELECT
    *
  FROM (
    SELECT
      IF(b.fold_fseries_number IS NOT NULL, b.fold_fseries_number, c.fold_fseries_number) AS fold_fseries_number,
      a.foffer_price,
      a.fcity_name,
      a.forder_address,
      a.freceiver_id,
      a.freceiver_name,
      a.freceiver_phone,
      a.fstart_time
    FROM dws.dws_jp_order_detail AS a
    LEFT JOIN dws.dws_hs_order_detail              AS b ON a.fseries_number = b.fseries_number
    LEFT JOIN dws.dws_hs_order_detail_history2018_2022 AS c ON a.fseries_number = c.fseries_number
    WHERE ftest_show <> 1
      AND TO_DATE(a.forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND a.fchannel_name = '竞拍销售默认渠道号'
      AND a.forder_status IN (2,3,4,6)

    UNION ALL

    SELECT
      IF(b.fold_fseries_number IS NOT NULL, b.fold_fseries_number,
         IF(d.fold_fseries_number IS NOT NULL, d.fold_fseries_number, c.fold_fseries_number)) AS fold_fseries_number,
      a.foffer_price,
      a.fcity_name,
      a.forder_address,
      a.freceiver_id,
      a.freceiver_name,
      a.freceiver_phone,
      a.fstart_time
    FROM dws.dws_jp_order_detail_history2023 AS a
    LEFT JOIN dws.dws_hs_order_detail              AS b ON a.fseries_number = b.fseries_number
    LEFT JOIN dws.dws_hs_order_detail_history2018_2022 AS c ON a.fseries_number = c.fseries_number
    LEFT JOIN dws.dws_hs_order_detail_history2023  AS d ON a.fseries_number = d.fseries_number
    WHERE ftest_show <> 1
      AND TO_DATE(a.forder_create_time) BETWEEN '2023-01-01' AND TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 366))
      AND a.fchannel_name = '竞拍销售默认渠道号'
      AND a.forder_status IN (2,3,4,6)

    UNION ALL

    SELECT
      IF(b.fold_fseries_number IS NOT NULL, b.fold_fseries_number, c.fold_fseries_number) AS fold_fseries_number,
      a.foffer_price,
      a.fcity_name,
      a.forder_address,
      a.freceiver_id,
      a.freceiver_name,
      a.freceiver_phone,
      a.fstart_time
    FROM dws.dws_jp_order_detail_history2020_2022 AS a
    LEFT JOIN dws.dws_hs_order_detail              AS b ON a.fseries_number = b.fseries_number
    LEFT JOIN dws.dws_hs_order_detail_history2018_2022 AS c ON a.fseries_number = c.fseries_number
    WHERE ftest_show <> 1
      AND TO_DATE(a.forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 400))
      AND a.fchannel_name = '竞拍销售默认渠道号'
      AND a.forder_status IN (2,3,4,6)

    UNION ALL

    SELECT
      IF(b.fold_fseries_number IS NOT NULL, b.fold_fseries_number, c.fold_fseries_number) AS fold_fseries_number,
      a.foffer_price,
      NULL AS fcity_name,
      NULL AS forder_address,
      NULL AS freceiver_id,
      NULL AS freceiver_name,
      NULL AS freceiver_phone,
      a.foffer_time AS fstart_time
    FROM dws.dws_th_order_detail AS a
    LEFT JOIN dws.dws_hs_order_detail              AS b ON a.fseries_number = b.fseries_number
    LEFT JOIN dws.dws_hs_order_detail_history2018_2022 AS c ON a.fseries_number = c.fseries_number
    WHERE a.fbd_status <> 2
      AND a.fchannel_name = '竞拍销售默认渠道号'
  ) t
),

/* =========================
 * C. 采货侠（平台=6）售后 / 销售 CTE
 * ========================= */
after_sale AS (
  SELECT *
  FROM (
    SELECT
      a.*,
      b.fseries_number,
      ROW_NUMBER() OVER (PARTITION BY fsales_series_number ORDER BY a.fauto_create_time DESC) AS num
    FROM drt.drt_my33310_recycle_t_after_sales_order_info AS a
    LEFT JOIN drt.drt_my33310_recycle_t_order AS b ON a.fafter_sales_order_id = b.forder_id
    WHERE a.fvalid = 1
  ) t
  WHERE num = 1
),

caihuoxia_sale AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time DESC) AS num
    FROM dws.dws_jp_order_detail
    WHERE ftest_show <> 1
      AND forder_platform = 6
      AND fmerchant_jp = 0
      AND TO_DATE(forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 1

  UNION ALL

  SELECT *
  FROM (
    SELECT
      *,
      '' AS fys_b2b_series_number,
      0  AS fys_b2b_order_status,
      0  AS fys_b2b_order_platform,
      0  AS fys_b2b_foffer_price,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time DESC) AS num
    FROM dws.dws_jp_order_detail_history2023
    WHERE ftest_show <> 1
      AND forder_platform = 6
      AND fmerchant_jp = 0
      AND TO_DATE(forder_create_time) BETWEEN '2023-01-01' AND TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 366))
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 1
),

caihuoxia_after_sale AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fbusiness_id ORDER BY fcreate_time DESC) AS num
    FROM drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales
  ) t
  WHERE num = 1
),

caihuoxia_first_sale AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_jp_order_detail
    WHERE ftest_show <> 1
      AND forder_platform = 6
      AND fmerchant_jp = 0
      AND TO_DATE(forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 1

  UNION ALL

  SELECT *
  FROM (
    SELECT
      *,
      '' AS fys_b2b_series_number,
      0  AS fys_b2b_order_status,
      0  AS fys_b2b_order_platform,
      0  AS fys_b2b_foffer_price,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_jp_order_detail_history2023
    WHERE ftest_show <> 1
      AND forder_platform = 6
      AND fmerchant_jp = 0
      AND TO_DATE(forder_create_time) BETWEEN '2023-01-01' AND TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 366))
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 1
),

caihuoxia_second_sale AS (
  SELECT
    fstart_time,
    fseries_number,
    foffer_price / 100 AS foffer_price,
    fcity_name,
    forder_address,
    freceiver_id,
    freceiver_name,
    freceiver_phone
  FROM (
    SELECT
      a.fstart_time,
      IF(b.fsrouce_serial_no IS NOT NULL, UPPER(b.fsrouce_serial_no), a.fseries_number) AS fseries_number,
      a.foffer_price,
      a.fcity_name,
      a.forder_address,
      a.freceiver_id,
      a.freceiver_name,
      a.freceiver_phone,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_jp_order_detail AS a
    LEFT JOIN drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn AS b ON a.fseries_number = b.fserial_no
    WHERE ftest_show <> 1
      AND TO_DATE(a.forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND forder_platform = 6
      AND fmerchant_jp = 0
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 2

  UNION ALL

  SELECT
    fstart_time,
    fseries_number,
    foffer_price / 100 AS foffer_price,
    fcity_name,
    forder_address,
    freceiver_id,
    freceiver_name,
    freceiver_phone
  FROM (
    SELECT
      a.fstart_time,
      IF(b.fsrouce_serial_no IS NOT NULL, UPPER(b.fsrouce_serial_no), a.fseries_number) AS fseries_number,
      a.foffer_price,
      a.fcity_name,
      a.forder_address,
      a.freceiver_id,
      a.freceiver_name,
      a.freceiver_phone,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_jp_order_detail_history2023 AS a
    LEFT JOIN drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn AS b ON a.fseries_number = b.fserial_no
    WHERE ftest_show <> 1
      AND TO_DATE(a.forder_create_time) BETWEEN '2023-01-01' AND TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 366))
      AND forder_platform = 6
      AND fmerchant_jp = 0
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 2

  UNION ALL

  SELECT
    fstart_time,
    fseries_number,
    foffer_price / 100 AS foffer_price,
    fcity_name,
    forder_address,
    freceiver_id,
    freceiver_name,
    freceiver_phone
  FROM (
    SELECT
      a.fstart_time,
      IF(b.fsrouce_serial_no IS NOT NULL, UPPER(b.fsrouce_serial_no), a.fseries_number) AS fseries_number,
      a.foffer_price,
      a.fcity_name,
      a.forder_address,
      a.freceiver_id,
      a.freceiver_name,
      a.freceiver_phone,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_jp_order_detail_history2020_2022 AS a
    LEFT JOIN drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn AS b ON a.fseries_number = b.fserial_no
    WHERE ftest_show <> 1
      AND TO_DATE(a.forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 400))
      AND forder_platform = 6
      AND fmerchant_jp = 0
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 2

  UNION ALL

  SELECT
    fstart_time,
    fseries_number,
    foffer_price / 100 AS foffer_price,
    fcity_name,
    forder_address,
    freceiver_id,
    freceiver_name,
    freceiver_phone
  FROM (
    SELECT
      a.fstart_time,
      IF(b.fsrouce_serial_no IS NOT NULL, UPPER(b.fsrouce_serial_no), a.fseries_number) AS fseries_number,
      a.foffer_price,
      a.fcity_name,
      a.forder_address,
      a.freceiver_id,
      a.freceiver_name,
      a.freceiver_phone,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_jp_order_detail AS a
    LEFT JOIN drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn AS b ON a.fseries_number = b.fserial_no
    WHERE ftest_show <> 1
      AND forder_platform <> 6
      AND TO_DATE(a.forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND fmerchant_jp = 0
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 1

  UNION ALL

  SELECT
    fstart_time,
    fseries_number,
    foffer_price / 100 AS foffer_price,
    fcity_name,
    forder_address,
    freceiver_id,
    freceiver_name,
    freceiver_phone
  FROM (
    SELECT
      a.fstart_time,
      IF(b.fsrouce_serial_no IS NOT NULL, UPPER(b.fsrouce_serial_no), a.fseries_number) AS fseries_number,
      a.foffer_price,
      a.fcity_name,
      a.forder_address,
      a.freceiver_id,
      a.freceiver_name,
      a.freceiver_phone,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_jp_order_detail_history2023 AS a
    LEFT JOIN drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn AS b ON a.fseries_number = b.fserial_no
    WHERE ftest_show <> 1
      AND forder_platform <> 6
      AND TO_DATE(a.forder_create_time) BETWEEN '2023-01-01' AND TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 366))
      AND fmerchant_jp = 0
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 1

  UNION ALL

  SELECT
    fstart_time,
    fseries_number,
    foffer_price / 100 AS foffer_price,
    fcity_name,
    forder_address,
    freceiver_id,
    freceiver_name,
    freceiver_phone
  FROM (
    SELECT
      a.fstart_time,
      IF(b.fsrouce_serial_no IS NOT NULL, UPPER(b.fsrouce_serial_no), a.fseries_number) AS fseries_number,
      a.foffer_price,
      a.fcity_name,
      a.forder_address,
      a.freceiver_id,
      a.freceiver_name,
      a.freceiver_phone,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_jp_order_detail_history2020_2022 AS a
    LEFT JOIN drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn AS b ON a.fseries_number = b.fserial_no
    WHERE ftest_show <> 1
      AND forder_platform <> 6
      AND TO_DATE(a.forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 400))
      AND fmerchant_jp = 0
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 1

  UNION ALL

  SELECT
    fstart_time,
    fseries_number,
    foffer_price / 100 AS foffer_price,
    NULL AS fcity_name,
    NULL AS forder_address,
    NULL AS freceiver_id,
    NULL AS freceiver_name,
    NULL AS freceiver_phone
  FROM (
    SELECT
      a.fstart_time,
      IF(b.fsrouce_serial_no IS NOT NULL, UPPER(b.fsrouce_serial_no), a.fseries_number) AS fseries_number,
      a.foffer_price,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_th_order_detail AS a
    LEFT JOIN drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn AS b ON a.fseries_number = b.fserial_no
    WHERE fbd_status <> 2
  ) t
  WHERE num = 1
),

/* =========================
 * D. 鱼市 B2B（平台=5） 专属 CTE
 * ========================= */
b2b_sale AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time DESC) AS num
    FROM dws.dws_jp_order_detail
    WHERE ftest_show <> 1
      AND forder_platform = 5
      AND forder_status IN (2,3,4,6)
      AND TO_DATE(forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
  ) t
  WHERE num = 1
),

b2b_detect AS (  -- B2B 初检：窗口365天，平台=5
  SELECT *
  FROM (
    SELECT
      a.fcreate_time,
      UPPER(a.fserial_number) AS fserial_number,
      a.fdet_tpl,
      a.freal_name,
      a.fend_time,
      a.fbrand_name,
      a.fdetection_object,
      a.fgoods_level,
      a.fwarehouse_code,
      ROW_NUMBER() OVER (PARTITION BY UPPER(a.fserial_number) ORDER BY a.fcreate_time ASC) AS num
    FROM drt.drt_my33310_detection_t_detect_record AS a
    LEFT JOIN (
      SELECT fseries_number, forder_create_time
      FROM (
        SELECT
          fseries_number,
          forder_create_time,
          ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time DESC) AS num
        FROM dws.dws_jp_order_detail
        WHERE ftest_show <> 1
          AND forder_platform = 5
          AND forder_status IN (2,3,4,6)
          AND forder_create_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      ) t
      WHERE t.num = 1
    ) AS b ON UPPER(a.fserial_number) = b.fseries_number
    LEFT JOIN (
      SELECT freal_name, fposition_id
      FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY freal_name ORDER BY fcreate_time DESC) AS num
        FROM drt.drt_my33310_amcdb_t_user
      ) t
      WHERE num = 1
    ) AS c ON a.freal_name = c.freal_name
    WHERE a.fis_deleted = 0
      AND TO_DATE(a.fend_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND a.fend_time < b.forder_create_time
      AND c.fposition_id <> 129
  ) c
  WHERE c.num = 1
),

b2b_after_sale AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fbusiness_id ORDER BY fcreate_time DESC) AS num
    FROM drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales
  ) t
  WHERE num = 1
),

b2b_first_sale AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_jp_order_detail
    WHERE ftest_show <> 1
      AND forder_platform = 5
      AND TO_DATE(forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 1
),

b2b_detect_two AS (
  SELECT
    UPPER(fserial_number) AS fserial_number,
    freal_name            AS fdetect_two_name,
    fcreate_time          AS fdetect_two_time
  FROM (
    SELECT
      a.fcreate_time,
      a.fserial_number,
      b.freal_name,
      ROW_NUMBER() OVER (PARTITION BY UPPER(a.fserial_number) ORDER BY a.fcreate_time DESC) AS num
    FROM drt.drt_my33312_detection_t_det_app_record AS a
    LEFT JOIN drt.drt_my33310_amcdb_t_user AS b ON a.fuser_name = b.fusername
    WHERE TO_DATE(a.fcreate_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND fserial_number <> ''
      AND fserial_number IS NOT NULL
  ) t
  WHERE num = 1
),

b2b_detect_three AS (
  SELECT
    UPPER(fserial_number) AS fserial_number,
    freal_name            AS fdetect_three_name,
    fcreate_time          AS fdetect_three_time
  FROM (
    SELECT
      a.fcreate_time,
      a.fserial_number,
      b.freal_name,
      ROW_NUMBER() OVER (PARTITION BY UPPER(a.fserial_number) ORDER BY a.fcreate_time DESC) AS num
    FROM drt.drt_my33312_detection_t_det_task AS a
    LEFT JOIN drt.drt_my33312_detection_t_det_task_record AS b ON a.ftask_id = b.ftask_id
    WHERE TO_DATE(a.fend_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND b.fdet_sop_task_name LIKE '%外观%'
  ) t
  WHERE num = 1
),

b2b_detect_three_pingmu AS (
  SELECT
    UPPER(fserial_number) AS fserial_number,
    CASE WHEN freal_name = '李俊峰' THEN '李俊锋' ELSE freal_name END AS fdetect_three_name_pingmu,
    fcreate_time          AS fdetect_three_time_pingmu
  FROM (
    SELECT
      a.fcreate_time,
      a.fserial_number,
      b.freal_name,
      ROW_NUMBER() OVER (PARTITION BY UPPER(a.fserial_number) ORDER BY a.fcreate_time DESC) AS num
    FROM drt.drt_my33312_detection_t_det_task AS a
    LEFT JOIN drt.drt_my33312_detection_t_det_task_record AS b ON a.ftask_id = b.ftask_id
    WHERE TO_DATE(a.fend_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND b.fdet_sop_task_name LIKE '%屏幕%'
      AND b.fdet_sop_task_name <> '外观屏幕'
  ) t
  WHERE num = 1
),

b2b_second_sale AS (
  SELECT
    fstart_time,
    fseries_number,
    foffer_price / 100 AS foffer_price,
    fcity_name,
    forder_address,
    freceiver_id,
    freceiver_name,
    freceiver_phone
  FROM (
    SELECT
      a.fstart_time,
      IF(b.fsrouce_serial_no IS NOT NULL, UPPER(b.fsrouce_serial_no), a.fseries_number) AS fseries_number,
      a.foffer_price,
      a.fcity_name,
      a.forder_address,
      a.freceiver_id,
      a.freceiver_name,
      a.freceiver_phone,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_jp_order_detail AS a
    LEFT JOIN drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn AS b ON a.fseries_number = b.fserial_no
    WHERE ftest_show <> 1
      AND TO_DATE(a.forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND forder_platform = 5
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 2

  UNION ALL

  SELECT
    fstart_time,
    fseries_number,
    foffer_price / 100 AS foffer_price,
    fcity_name,
    forder_address,
    freceiver_id,
    freceiver_name,
    freceiver_phone
  FROM (
    SELECT
      a.fstart_time,
      IF(b.fsrouce_serial_no IS NOT NULL, UPPER(b.fsrouce_serial_no), a.fseries_number) AS fseries_number,
      a.foffer_price,
      a.fcity_name,
      a.forder_address,
      a.freceiver_id,
      a.freceiver_name,
      a.freceiver_phone,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_jp_order_detail AS a
    LEFT JOIN drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn AS b ON a.fseries_number = b.fserial_no
    WHERE ftest_show <> 1
      AND TO_DATE(a.forder_create_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365))
      AND forder_platform <> 5
      AND forder_status IN (2,3,4,6)
  ) t
  WHERE num = 1

  UNION ALL

  SELECT
    fstart_time,
    fseries_number,
    foffer_price / 100 AS foffer_price,
    NULL AS fcity_name,
    NULL AS forder_address,
    NULL AS freceiver_id,
    NULL AS freceiver_name,
    NULL AS freceiver_phone
  FROM (
    SELECT
      a.fstart_time,
      IF(b.fsrouce_serial_no IS NOT NULL, UPPER(b.fsrouce_serial_no), a.fseries_number) AS fseries_number,
      a.foffer_price,
      ROW_NUMBER() OVER (PARTITION BY fseries_number ORDER BY forder_create_time ASC) AS num
    FROM dws.dws_th_order_detail AS a
    LEFT JOIN drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn AS b ON a.fseries_number = b.fserial_no
    WHERE fbd_status <> 2
  ) t
  WHERE num = 1
)

/* =========================
 * 最终结果（自有平台 ∪ 采货侠 ∪ B2B）
 * ========================= */
SELECT
  a.fstart_time,
  a.fseries_number,
  a.fclass_name,
  CASE WHEN b.fbrand_name = '苹果' THEN '苹果' ELSE '安卓' END AS fbrand_name,
  a.fchannel_name,
  a.fproduct_name,
  a.fproject_name,
  a.fcity_name,
  a.forder_address,
  a.freceiver_id,
  a.freceiver_name,
  a.freceiver_phone,
  f.freal_name AS fsecond_detect_name,
  c.fseries_number AS fafter_series_number,
  e.fcity_name    AS fsecond_sale_city,
  e.forder_address AS fsecond_sale_address,
  e.freceiver_id   AS fsecond_sale_id,
  e.freceiver_name AS fsecond_sale_name,
  e.freceiver_phone AS fsecond_sale_phone,
  '自有平台' AS 销售渠道,
  LEFT(a.fseries_number, 2) AS 渠道,
  a.fcost_price / 100 AS 成本价,
  a.foffer_price / 100 AS 当前出价,
  IF(
    (c.ftotal_real_refund_amount > 0 AND a.foffer_price = c.ftotal_real_refund_amount)
    OR (a.fchannel_name = '竞拍销售默认渠道号'),
    0, a.foffer_price / 100
  ) AS 销售额,
  b.fdet_tpl,
  b.freal_name,
  b.fend_time,
  b.fdetection_object,
  CASE
    WHEN b.fwarehouse_code = '12' THEN '东莞仓'
    WHEN RIGHT(LEFT(a.fseries_number,6),2) = '16' OR LEFT(a.fseries_number,3) = '020' THEN '杭州仓'
    ELSE '深圳仓'
  END AS fwarehouse_code,
  c.fauto_create_time,
  GET_JSON_OBJECT(b.fgoods_level, '$.levelName') AS fgoods_level,
  c.fappeal_reason,
  CAST(c.ffirst_trial_result AS STRING) AS ffirst_trial_result,
  c.freexamine_result,
  c.fdetection_price/100      AS 检测价,
  c.freinspection_price/100   AS 二次检测价,
  c.ftotal_diff_amount/100    AS 检测差异金额,
  c.ftotal_refundable_amount/100 AS 总应退款金额,
  c.ftotal_real_refund_amount/100 AS 总实退款金额,
  CASE
    WHEN b.fdet_tpl = 1 THEN '大检测'
    WHEN (b.fdet_tpl IN (0,2,6,7)) THEN '竞拍检测'
    ELSE '其他'
  END AS 检测渠道,
  CASE
    WHEN b.fdet_tpl = 0 THEN '标准检'
    WHEN b.fdet_tpl = 1 THEN '大质检'
    WHEN b.fdet_tpl = 2 THEN '新标准检测'
    WHEN b.fdet_tpl = 3 THEN '产线检'
    WHEN b.fdet_tpl = 4 THEN '34项检测'
    WHEN b.fdet_tpl = 5 THEN '无忧购'
    WHEN b.fdet_tpl = 6 THEN '寄卖plus'
    WHEN b.fdet_tpl = 7 THEN '价格3.0的检测'
    ELSE '其他'
  END AS 检测模板,
  CASE WHEN c.ftotal_real_refund_amount > 0 THEN 1 ELSE 0 END AS 售后数,
  CASE WHEN c.ftotal_real_refund_amount > 0 THEN c.freceived_audit_result_time ELSE NULL END AS 售后通过时间,
  CASE
    WHEN c.ftotal_real_refund_amount > 0 AND a.foffer_price <  c.ftotal_real_refund_amount AND a.fstart_time >= '2022-01-01' THEN 1
    WHEN c.ftotal_real_refund_amount > 0 AND a.foffer_price =  c.ftotal_real_refund_amount AND a.fstart_time <  '2022-01-01' THEN 1
    ELSE 0
  END AS 退货数,
  CASE WHEN c.ftotal_real_refund_amount > 0 AND a.foffer_price > c.ftotal_real_refund_amount THEN 1 ELSE 0 END AS 补差赔付,
  CASE WHEN c.ftotal_real_refund_amount > 0 AND a.foffer_price > c.ftotal_real_refund_amount THEN c.ftotal_real_refund_amount/100 ELSE 0 END AS 赔付金额,
  c.fafter_sales_type,
  CASE
    WHEN c.fafter_sales_type = 1 THEN '仅退款'
    WHEN c.fafter_sales_type = 2 THEN '退货退款'
    ELSE '其它'
  END AS 售后类型,
  c.faftersales_owner,
  d.foffer_price/100 AS first_price,
  e.fstart_time      AS fsecond_sale_time,
  e.foffer_price/100 AS second_price,
  IF(
    c.ftotal_real_refund_amount > 0
    AND a.foffer_price = c.ftotal_real_refund_amount
    AND a.fstart_time >= '2022-01-01',
    0, d.foffer_price/100 - e.foffer_price/100
  ) AS 二次差价成本,
  IF(g.fdetect_two_name   IS NULL, b.freal_name, g.fdetect_two_name)   AS fdetect_two_name,
  IF(h.fdetect_three_name IS NULL, b.freal_name, h.fdetect_three_name) AS fdetect_three_name,
  j.fdetect_three_name_pingmu,
  IF(g.fdetect_two_time IS NOT NULL, '是', '否') AS 是否分模块,
  a.fanchor_level
FROM jp_sale AS a
LEFT JOIN detect                AS b ON a.fseries_number = b.fserial_number
LEFT JOIN after_sale            AS c ON a.fseries_number = c.fsales_series_number
LEFT JOIN after_sale_detect     AS f ON c.fseries_number = f.fserial_number
LEFT JOIN jp_first_sale         AS d ON a.fseries_number = d.fseries_number
LEFT JOIN jp_second_sale        AS e ON a.fseries_number = e.fold_fseries_number
LEFT JOIN detect_two            AS g ON a.fseries_number = g.fserial_number
LEFT JOIN detect_three          AS h ON a.fseries_number = h.fserial_number
LEFT JOIN detect_three_pingmu   AS j ON a.fseries_number = j.fserial_number
LEFT JOIN detect_four           AS i ON a.fseries_number = i.fserial_number
WHERE a.fstart_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 400))

UNION ALL

SELECT
  a.fstart_time,
  a.fseries_number,
  a.fclass_name,
  CASE WHEN b.fbrand_name = '苹果' THEN '苹果' ELSE '安卓' END AS fbrand_name,
  a.fchannel_name,
  a.fproduct_name,
  a.fproject_name,
  a.fcity_name,
  a.forder_address,
  a.freceiver_id,
  a.freceiver_name,
  a.freceiver_phone,
  g.freal_name AS fsecond_detect_name,
  c.fnew_serial_no AS fafter_series_number,
  e.fcity_name     AS fsecond_sale_city,
  e.forder_address AS fsecond_sale_address,
  e.freceiver_id   AS fsecond_sale_id,
  e.freceiver_name AS fsecond_sale_name,
  e.freceiver_phone AS fsecond_sale_phone,
  '采货侠' AS 销售渠道,
  LEFT(a.fseries_number, 2) AS 渠道,
  a.fcost_price / 100 AS 成本价,
  a.foffer_price / 100 AS 当前出价,
  IF(c.fapply_time IS NOT NULL AND c.fapply_time <> '0000-00-00 00:00:00.0' AND c.fjudge_type = 1, 0, a.foffer_price / 100) AS 销售额,
  b.fdet_tpl,
  b.freal_name,
  b.fend_time,
  b.fdetection_object,
  CASE
    WHEN b.fwarehouse_code = '12' THEN '东莞仓'
    WHEN RIGHT(LEFT(a.fseries_number,6),2) = '16' OR LEFT(a.fseries_number,3) = '020' THEN '杭州仓'
    ELSE '深圳仓'
  END AS fwarehouse_code,
  c.fapply_time,
  GET_JSON_OBJECT(b.fgoods_level, '$.levelName') AS fgoods_level,
  c.fjudge_reason   AS fappeal_reason,
  c.fjudge_result   AS ffirst_trial_result,
  NULL              AS freexamine_result,
  NULL              AS 检测价,
  NULL              AS 二次检测价,
  NULL              AS 检测差异金额,
  NULL              AS 总应退款金额,
  c.forder_deal_price / 100 AS 总实退款金额,
  CASE
    WHEN b.fdet_tpl = 1 THEN '大检测'
    WHEN (b.fdet_tpl IN (0,2,6,7)) THEN '竞拍检测'
    ELSE '其他'
  END AS 检测渠道,
  CASE
    WHEN b.fdet_tpl = 0 THEN '标准检'
    WHEN b.fdet_tpl = 1 THEN '大质检'
    WHEN b.fdet_tpl = 2 THEN '新标准检测'
    WHEN b.fdet_tpl = 3 THEN '产线检'
    WHEN b.fdet_tpl = 4 THEN '34项检测'
    WHEN b.fdet_tpl = 5 THEN '无忧购'
    WHEN b.fdet_tpl = 6 THEN '寄卖plus'
    WHEN b.fdet_tpl = 7 THEN '价格3.0的检测'
    ELSE '其他'
  END AS 检测模板,
  CASE
    WHEN c.fapply_time IS NOT NULL AND c.fapply_time <> '0000-00-00 00:00:00.0' AND c.fjudge_type = 1 THEN 1
    WHEN f.fsrouce_serial_no IS NOT NULL THEN 1 ELSE 0
  END AS 售后数,
  CASE
    WHEN c.fapply_time IS NOT NULL AND c.fapply_time <> '0000-00-00 00:00:00.0' AND c.fjudge_type = 1 THEN c.fjudge_time
    WHEN f.fsrouce_serial_no IS NOT NULL THEN c.fjudge_time ELSE NULL
  END AS 售后通过时间,
  CASE
    WHEN c.fapply_time IS NOT NULL AND c.fapply_time <> '0000-00-00 00:00:00.0' AND c.fjudge_type = 1 THEN 1
    WHEN f.fsrouce_serial_no IS NOT NULL THEN 1 ELSE 0
  END AS 退货数,
  0 AS 补差赔付,
  0 AS 赔付金额,
  NULL AS fafter_sales_type,
  CASE
    WHEN c.fapply_time IS NOT NULL AND c.fapply_time <> '0000-00-00 00:00:00.0' AND c.fjudge_type = 1 THEN '退货退款'
    WHEN f.fsrouce_serial_no IS NOT NULL THEN '退货退款' ELSE NULL
  END AS 售后类型,
  NULL AS faftersales_owner,
  d.foffer_price/100 AS first_price,
  e.fstart_time      AS fsecond_sale_time,
  e.foffer_price     AS second_price,
  IF(
    c.fapply_time IS NOT NULL AND c.fapply_time <> '0000-00-00 00:00:00.0' AND c.fjudge_type = 1,
    d.foffer_price/100 - e.foffer_price,
    IF(f.fsrouce_serial_no IS NOT NULL, d.foffer_price/100 - e.foffer_price, 0)
  ) AS 二次差价成本,
  IF(h.fdetect_two_name   IS NULL, b.freal_name, h.fdetect_two_name)   AS fdetect_two_name,
  IF(i.fdetect_three_name IS NULL, b.freal_name, i.fdetect_three_name) AS fdetect_three_name,
  k.fdetect_three_name_pingmu,
  IF(h.fdetect_two_time IS NOT NULL, '是', '否') AS 是否分模块,
  a.fanchor_level
FROM caihuoxia_sale AS a
LEFT JOIN detect              AS b ON a.fseries_number = b.fserial_number
LEFT JOIN caihuoxia_after_sale AS c ON a.fseries_number = c.fbusiness_id
LEFT JOIN after_sale_detect    AS g ON c.fnew_serial_no = g.fserial_number
LEFT JOIN caihuoxia_first_sale AS d ON a.fseries_number = d.fseries_number
LEFT JOIN caihuoxia_second_sale AS e ON a.fseries_number = e.fseries_number
LEFT JOIN drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn AS f ON a.fseries_number = UPPER(f.fsrouce_serial_no)
LEFT JOIN detect_two            AS h ON a.fseries_number = h.fserial_number
LEFT JOIN detect_three          AS i ON a.fseries_number = i.fserial_number
LEFT JOIN detect_three_pingmu   AS k ON a.fseries_number = k.fserial_number
LEFT JOIN detect_four           AS j ON a.fseries_number = j.fserial_number
WHERE a.fstart_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 400))

UNION ALL

-- B2B 分支（平台=5，近365天，不拼历史表）
SELECT
  a.fstart_time,
  a.fseries_number,
  a.fclass_name,
  CASE WHEN b.fbrand_name = '苹果' THEN '苹果' ELSE '安卓' END AS fbrand_name,
  a.fchannel_name,
  a.fproduct_name,
  a.fproject_name,
  a.fcity_name,
  a.forder_address,
  a.freceiver_id,
  a.freceiver_name,
  a.freceiver_phone,
  g.freal_name AS fsecond_detect_name,
  cc.fnew_serial_no AS fafter_series_number,
  e.fcity_name     AS fsecond_sale_city,
  e.forder_address AS fsecond_sale_address,
  e.freceiver_id   AS fsecond_sale_id,
  e.freceiver_name AS fsecond_sale_name,
  e.freceiver_phone AS fsecond_sale_phone,
  '鱼市B2B' AS 销售渠道,
  LEFT(a.fseries_number, 2) AS 渠道,
  a.fcost_price / 100 AS 成本价,
  a.foffer_price / 100 AS 当前出价,
  IF(cc.frefund_total > 0, 0, a.foffer_price / 100) AS 销售额,
  b.fdet_tpl,
  b.freal_name,
  b.fend_time,
  b.fdetection_object,
  CASE
    WHEN b.fwarehouse_code = '12' THEN '东莞仓'
    WHEN RIGHT(LEFT(a.fseries_number,6),2) = '16' OR LEFT(a.fseries_number,3) = '020' THEN '杭州仓'
    ELSE '深圳仓'
  END AS fwarehouse_code,
  cc.fapply_time AS fauto_create_time,
  GET_JSON_OBJECT(b.fgoods_level, '$.levelName') AS fgoods_level,
  cc.fjudge_reason AS fappeal_reason,
  cc.fjudge_result AS ffirst_trial_result,
  0 AS freexamine_result,
  0 AS 检测价,
  0 AS 二次检测价,
  0 AS 检测差异金额,
  0 AS 总应退款金额,
  cc.frefund_total / 100 AS 总实退款金额,
  CASE
    WHEN b.fdet_tpl = 1 THEN '大检测'
    WHEN (b.fdet_tpl IN (0,2,6,7)) THEN '竞拍检测'
    ELSE '其他'
  END AS 检测渠道,
  CASE
    WHEN b.fdet_tpl = 0 THEN '标准检'
    WHEN b.fdet_tpl = 1 THEN '大质检'
    WHEN b.fdet_tpl = 2 THEN '新标准检测'
    WHEN b.fdet_tpl = 3 THEN '产线检'
    WHEN b.fdet_tpl = 4 THEN '34项检测'
    WHEN b.fdet_tpl = 5 THEN '无忧购'
    WHEN b.fdet_tpl = 6 THEN '寄卖plus'
    WHEN b.fdet_tpl = 7 THEN '价格3.0的检测'
    ELSE '其他'
  END AS 检测模板,
  CASE
    WHEN cc.fapply_time IS NOT NULL AND cc.fapply_time <> '0000-00-00 00:00:00.0' AND cc.fjudge_type = 1 THEN 1
    WHEN f.fsrouce_serial_no IS NOT NULL THEN 1 ELSE 0
  END AS 售后数,
  CASE
    WHEN cc.fapply_time IS NOT NULL AND cc.fapply_time <> '0000-00-00 00:00:00.0' AND cc.fjudge_type = 1 THEN cc.fjudge_time
    WHEN f.fsrouce_serial_no IS NOT NULL THEN cc.fjudge_time ELSE NULL
  END AS 售后通过时间,
  CASE
    WHEN cc.fapply_time IS NOT NULL AND cc.fapply_time <> '0000-00-00 00:00:00.0' AND cc.fjudge_type = 1 THEN 1
    WHEN f.fsrouce_serial_no IS NOT NULL THEN 1 ELSE 0
  END AS 退货数,
  CASE WHEN cc.frefund_total > 0 AND a.foffer_price > cc.frefund_total THEN 1 ELSE 0 END AS 补差赔付,
  CASE WHEN cc.frefund_total > 0 AND a.foffer_price > cc.frefund_total THEN cc.frefund_total / 100 ELSE 0 END AS 赔付金额,
  cc.faftersales_type AS fafter_sales_type,
  CASE
    WHEN cc.fapply_time IS NOT NULL AND cc.fapply_time <> '0000-00-00 00:00:00.0' AND cc.fjudge_type = 1 THEN '退货退款'
    WHEN f.fsrouce_serial_no IS NOT NULL THEN '退货退款' ELSE NULL
  END AS 售后类型,
  NULL AS faftersales_owner,
  d.foffer_price/100 AS first_price,
  e.fstart_time      AS fsecond_sale_time,
  e.foffer_price     AS second_price,
  IF(
    cc.fapply_time IS NOT NULL AND cc.fapply_time <> '0000-00-00 00:00:00.0' AND cc.fjudge_type = 1,
    d.foffer_price/100 - e.foffer_price,
    IF(f.fsrouce_serial_no IS NOT NULL, d.foffer_price/100 - e.foffer_price, 0)
  ) AS 二次差价成本,
  IF(h.fdetect_two_name   IS NULL, b.freal_name, h.fdetect_two_name)   AS fdetect_two_name,
  IF(i.fdetect_three_name IS NULL, b.freal_name, i.fdetect_three_name) AS fdetect_three_name,
  k.fdetect_three_name_pingmu,
  IF(h.fdetect_two_time IS NOT NULL, '是', '否') AS 是否分模块,
  a.fanchor_level
FROM b2b_sale AS a
LEFT JOIN b2b_detect              AS b  ON a.fseries_number = b.fserial_number
LEFT JOIN b2b_after_sale          AS cc ON a.fseries_number = cc.fbusiness_id
LEFT JOIN after_sale_detect       AS g  ON UPPER(COALESCE(cc.fnew_serial_no, a.fseries_number)) = UPPER(g.fserial_number)
LEFT JOIN b2b_detect_two          AS h  ON a.fseries_number = h.fserial_number
LEFT JOIN b2b_detect_three        AS i  ON a.fseries_number = i.fserial_number
LEFT JOIN b2b_detect_three_pingmu AS k  ON a.fseries_number = k.fserial_number
LEFT JOIN b2b_first_sale          AS d  ON a.fseries_number = d.fseries_number
LEFT JOIN b2b_second_sale         AS e  ON a.fseries_number = e.fseries_number
LEFT JOIN drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn AS f ON a.fseries_number = UPPER(f.fsrouce_serial_no)
WHERE a.fstart_time >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 365));
