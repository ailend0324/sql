/*
================================================================================
📦 收货到货数据 · 同期对比（2024 vs 2025 · 09-15 ~ 11-30）
================================================================================
用途：按「天」统计仓库实际收到货的单量，拆分为
      ① 回收-闲鱼  ② 回收-天猫  ③ 验货宝（验机）
      （附带输出「回收-其他」「寄卖」，方便和收货组总量对账，不需要可在 HTML 里过滤）

对比区间：
      2024-09-15 ~ 2024-11-30   （77 天）
      2025-09-15 ~ 2025-11-30   （77 天）

口径说明：
      · 回收：drt.drt_my33310_recycle_t_order.fgetin_time = 仓库收货时间，剔除测试单
      · 验机：dws.dws_xy_yhb_detail.freceive_time      = 验货宝签收时间
      · 单位：条码数（去重），一条码 = 一台机器

执行环境：Hue（Hive / Impala 均可，已统一使用 substr 避免 left/strleft 方言差异）
配套文件：供应链/收货管理/收货到货同期对比看板.html
          （把本查询结果在 Hue 里 Download → CSV，再拖进那个 HTML 即可出图）
================================================================================
*/

with
-- ────────────────────────────────────────────────────────────────────────────
-- 第一步：回收 / 寄卖收货明细（一条码一行）
-- ────────────────────────────────────────────────────────────────────────────
recycle_recv as (
    select
        to_date(a.fgetin_time)                        as fdate,
        a.fseries_number                              as fbarcode,

        -- 仓库：靠条码第 3~6 位判断（沿用《回收寄卖验货宝各仓收货数据》口径）
        case when substr(a.fseries_number, 3, 4) in ('0112', '0118') then '东莞仓'
             when substr(a.fseries_number, 5, 2) = '16'              then '杭州仓'
             else '深圳仓' end                        as fwarehouse,

        -- 业务线：条码前缀优先，前缀认不出来时再回退到「渠道名 + 项目名」
        case
            -- 寄卖 / 帮卖（BM=闲鱼寄卖，BB=B端帮卖）
            when substr(a.fseries_number, 1, 2) in ('BM', 'BB')            then '寄卖'
            when c.fchannel_name like '%寄卖%'                              then '寄卖'
            when c.fchannel_name like '%帮卖%'                              then '寄卖'
            -- 售后退回，不算新到货
            when c.fchannel_name = '竞拍销售默认渠道号'                       then '售后退回'
            -- 回收 · 闲鱼（XY=2C闲鱼，YJ=闲鱼验机单）
            when substr(a.fseries_number, 1, 2) in ('XY', 'YJ')            then '回收-闲鱼'
            -- 回收 · 天猫（TM/TY=天猫以旧换新）
            when substr(a.fseries_number, 1, 2) in ('TM', 'TY')            then '回收-天猫'
            -- 前缀兜底：合作项目下按渠道名认闲鱼 / 天猫
            when d.fproject_name like '合作%' and c.fchannel_name like '%闲鱼%' then '回收-闲鱼'
            when d.fproject_name like '合作%' and c.fchannel_name like '%天猫%' then '回收-天猫'
            else '回收-其他'
        end                                           as fbiz
    from drt.drt_my33310_recycle_t_order a
    left join drt.drt_my33310_recycle_t_channel c
           on a.fchannel_id = c.fchannel_id
    left join drt.drt_my33310_pub_server_channel_center_db_t_pid_info d
           on a.fpid = d.fpid
    where a.ftest = 0                                        -- 剔除测试单
      and a.fgetin_time is not null                          -- 必须真的收到货
      and substr(a.fseries_number, 1, 2) not in ('YZ', 'NT') -- 剔除验证单 / 内测单
      and (
            (to_date(a.fgetin_time) >= '2024-09-15' and to_date(a.fgetin_time) <= '2024-11-30')
         or (to_date(a.fgetin_time) >= '2025-09-15' and to_date(a.fgetin_time) <= '2025-11-30')
          )
),

-- ────────────────────────────────────────────────────────────────────────────
-- 第二步：验货宝（验机）收货明细
-- ────────────────────────────────────────────────────────────────────────────
yhb_recv as (
    select
        to_date(freceive_time)                        as fdate,
        fhost_barcode                                 as fbarcode,
        case when substr(fhost_barcode, 1, 3) = '010' then '深圳仓'
             when substr(fhost_barcode, 1, 3) = '020' then '杭州仓'
             when substr(fhost_barcode, 1, 3) = '050' then '东莞仓'
             else '其他仓' end                         as fwarehouse,
        '验货宝'                                       as fbiz
    from dws.dws_xy_yhb_detail
    where fhost_barcode is not null
      and freceive_time is not null
      and (
            (to_date(freceive_time) >= '2024-09-15' and to_date(freceive_time) <= '2024-11-30')
         or (to_date(freceive_time) >= '2025-09-15' and to_date(freceive_time) <= '2025-11-30')
          )
),

-- ────────────────────────────────────────────────────────────────────────────
-- 第三步：两块业务合并
-- ────────────────────────────────────────────────────────────────────────────
all_recv as (
    select fdate, fbarcode, fwarehouse, fbiz from recycle_recv
    union all
    select fdate, fbarcode, fwarehouse, fbiz from yhb_recv
)

-- ────────────────────────────────────────────────────────────────────────────
-- 第四步：按「年份 × 日期 × 业务线 × 仓库」出到货量
--         day_index = 距离 09-15 的天数，用来把 2024 / 2025 对齐同一根横轴
-- ────────────────────────────────────────────────────────────────────────────
select
    substr(fdate, 1, 4)                                       as fyear,        -- 年份：2024 / 2025
    fdate                                                     as fdate,        -- 收货日期 yyyy-MM-dd
    substr(fdate, 6, 5)                                       as fmd,          -- 月-日 MM-dd（同期对齐用）
    case when substr(fdate, 1, 4) = '2024'
         then datediff(fdate, '2024-09-15')
         else datediff(fdate, '2025-09-15') end               as day_index,    -- 第 N 天，0 = 09-15
    fbiz                                                      as fbiz,         -- 业务线
    fwarehouse                                                as fwarehouse,   -- 仓库
    count(distinct fbarcode)                                  as recv_cnt      -- 到货量（条码去重）
from all_recv
group by
    substr(fdate, 1, 4),
    fdate,
    substr(fdate, 6, 5),
    case when substr(fdate, 1, 4) = '2024'
         then datediff(fdate, '2024-09-15')
         else datediff(fdate, '2025-09-15') end,
    fbiz,
    fwarehouse
order by fbiz, fdate, fwarehouse;


/*
================================================================================
【附】校验查询：只看三条主业务线的区间总量，跑完主查询后拿它对一下数
================================================================================

with recycle_recv as ( ...同上... ), yhb_recv as ( ...同上... )
select
    substr(fdate,1,4) as fyear,
    fbiz,
    count(distinct fbarcode) as recv_cnt,
    count(distinct fdate)    as day_cnt
from (
    select fdate, fbarcode, fbiz from recycle_recv
    union all
    select fdate, fbarcode, fbiz from yhb_recv
) t
where fbiz in ('回收-闲鱼','回收-天猫','验货宝')
group by substr(fdate,1,4), fbiz
order by fbiz, fyear;

================================================================================
【使用步骤】
  1. 在 Hue 里粘贴上面的主查询 → 执行
  2. 结果区右下角 Download → 选 CSV（务必带表头）
  3. 打开 供应链/收货管理/收货到货同期对比看板.html，把 CSV 拖进去
  4. 看板会自动按天画出 2024 vs 2025 的到货曲线、同比、周汇总和明细表

【常见改动】
  · 想按「品类」再拆一层：join drt.drt_my33310_detection_t_detect_record 取 fclass_name
    （写法参考同目录 回收验机收货数.sql 的 category_info 部分）
  · 想换时间区间：全文替换 4 处 '2024-09-15' / '2024-11-30' / '2025-09-15' / '2025-11-30'
    以及 select 里 datediff 的两个基准日期
  · 数据量大跑不动：先把 where 里的年份区间拆成两次单独跑，再在 HTML 里合并两个 CSV
================================================================================
*/
