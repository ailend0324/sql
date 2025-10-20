-- 目的：
--   从“自有检测”相关表里，拿到近1个月内的订单及其对应的“检测缺陷图片”信息。
--   输出的每一行包含：订单ID（字符串形式）、设备序列号、缺陷位置编码、缺陷图片URL。
-- 适用场景：
--   做质检问题回溯、图片抽检、质检看板等。
-- 时间范围：
--   以当前时间所在月份为基准，取“上一个整月”到现在（近1个月）。
-- 关键逻辑概览：
--   1) 先用 CTE（with 子句）圈定“自有订单”的基础范围（近1个月、有效、非测试、排除部分合作渠道、排除特定业务模式、且有序列号）。
--   2) 再与“检测缺陷图片表”按序列号关联，拿到缺陷位置与图片。
--   3) 关联渠道明细表以补充渠道维度（当前仅作为范围过滤或后续拓展使用）。
--   4) 最后按设备序列号倒序展示。
with

order_id_zy as  (
-- 1) 先在订单表里取“基础字段”，形成一个自有订单集合
select forder_id,                 -- 订单ID
       fseries_number,            -- 设备序列号（后续与检测表关联的关键字段）
       fpid,                      -- 渠道/平台ID（后续与渠道明细表关联）
       forder_time,               -- 下单时间
       fgoods_id,                 -- 商品ID
       forder_num ,               -- 下单数量
       fpay_out_time,             -- 付款时间
       Flogistics_id,             -- 物流单号/ID
       Foperation_price           -- 操作价格/成交价

from drt.drt_my33310_recycle_t_order 
where
  -- 时间范围：取近1个月（从上一个月的月初起算）
  forder_time  >= to_date(months_sub(trunc(now(), 'month'),1)) -- 订单数据时间范围近1个月

  and Fvalid = 1                 -- 仅保留有效订单
  and Ftest = 0                  -- 剔除测试订单
  and fchannel_id not in (
    10000918,10000335,10000325,10000135,10001111,10001118,10000343,10000427,10000326,10000935,10000195
  )                              -- 粗略排除合作渠道（避免把合作方的数据混入）
  and Fbusiness_mode not in (1,7,12,17) -- 排除不在统计口径内的业务模式
  and fseries_number is not null -- 必须有设备序列号，才能与检测表对上
) ----基本粗略的订单取数范围



-- 2) 联合查询：把订单与“缺陷图片表”、渠道明细表拼起来
select 
  cast (o.forder_id as string) as forder_id, -- 转成字符串，便于下游一致性处理
  o.fseries_number,                          -- 设备序列号
  fposition_code,                            -- 缺陷位置编码（如：屏幕、后盖、边框等）
  fpic                                       -- 缺陷图片URL（或存储路径）

from  order_id_zy o
inner join (
  -- 缺陷图片明细：近1个月内产生的缺陷图片记录
  select fseries_number,        -- 设备序列号（与订单对齐）
         fposition_code,        -- 缺陷位置编码
         fpic                   -- 缺陷图片URL
  from drt.drt_my33310_detection_t_photo_defect
  where fcreate_time  >= to_date(months_sub(trunc(now(), 'month'),1))
) pd on pd.fseries_number = o.fseries_number -- 按设备序列号关联

inner join dwd.dwd_channel_detail_zy cd      -- 渠道明细（保留字段便于后续扩展统计）
        on cd.fpid = o.fpid

order by fseries_number desc
