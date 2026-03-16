-- 问密操作退货且未入库的机器人播报
-- 需求：
-- 1. 有问密工单
-- 2. 订单状态是已取消的
-- 3. >2小时未入库 (从客服操作退货指令/退货指令生成时间开始计算)
-- 4. 播报时间：10:00, 14:00, 17:00 (由调度系统控制，SQL只需查出符合条件的数据)

with wenmi_works as (
    -- 1. 查找最近的问密工单 (fwork_type=4 表示问密工单)
    select 
        a.fbarcode_sn as fseries_number,
        a.forder_id,
        a.fadd_time,
        a.fcompletion_time,
        a.fwork_status,
        row_number() over(partition by a.fbarcode_sn order by a.fadd_time desc) as num
    from drt.drt_my33310_csrdb_t_works as a
    where a.fwork_type=4
    -- 限制时间范围，比如最近30天内的问密工单
    and from_unixtime(a.fadd_time) >= to_date(date_sub(from_unixtime(unix_timestamp()), 30))
),
stock_in_record as (
    -- 2. 查找入库记录 (CGRK 表示采购入库)
    select
        upper(fserial_no) as fseries_number,
        max(fchange_time) as last_stock_in_time
    from drt.drt_my33312_hsb_sales_product_t_pm_wms_stock_notify
    where fchange_time >= to_date(date_sub(from_unixtime(unix_timestamp()), 30))
    and fcmd = 'CGRK'
    group by fserial_no
),
return_request as (
    -- 3. 查找退货指令生成时间 (订单状态变更为"待退货"或"已取消"的时间)
    select
        x.forder_id,
        x.fauto_create_time as frequest_endtime
    from (
        select
            a.forder_id,
            a.fauto_create_time,
            row_number() over (partition by a.forder_id order by a.fauto_create_time desc) as num
        from drt.drt_my33310_recycle_t_order_txn a
        inner join drt.drt_my33310_recycle_t_order_status b
            on a.forder_status = b.forder_status_id
        where b.forder_status_name in ('待退货', '已取消')
          and a.fauto_create_time >= to_date(date_sub(now(), 30))
    ) x
    where x.num = 1
)
select 
    -- 按照播报格式要求，输出相关信息
    1 as groupkey,
    count(*) as counts,
    group_concat(o.fseries_number) as fseries_number,
    -- 计算超时小时数：当前时间减去退货指令生成时间
    group_concat(cast(round((unix_timestamp(now()) - unix_timestamp(r.frequest_endtime))/3600, 1) as string)) as fchaoshi,
    group_concat(
        case when right(left(o.fseries_number,6),4)="0112" then "东莞仓" 
             when right(left(o.fseries_number,6),2)="16" then "杭州仓"
             else "深圳仓" end
    ) as fwms_type
from drt.drt_my33310_recycle_t_order as o
-- 关联订单状态表
left join drt.drt_my33310_recycle_t_order_status as s on o.forder_status = s.forder_status_id
-- 关联问密工单表
inner join wenmi_works as w on o.fseries_number = w.fseries_number and w.num = 1
-- 关联退货指令生成时间
inner join return_request as r on o.forder_id = r.forder_id
-- 关联入库记录表
left join stock_in_record as i on o.fseries_number = i.fseries_number
where 
    -- 订单状态是已取消的
    s.forder_status_name like '%已取消%'
    -- 未入库：没有入库记录，或者入库时间早于退货指令生成时间（说明退货指令生成后还没入库）
    and (i.last_stock_in_time is null or i.last_stock_in_time < r.frequest_endtime)
    -- >2小时未入库 (从退货指令生成时间开始计算)
    and (unix_timestamp(now()) - unix_timestamp(r.frequest_endtime)) / 3600 > 2
    -- 排除测试单
    and o.ftest = 0
group by 1;
