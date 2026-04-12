-- 问密操作退货且未入库的机器人播报
-- 需求：
-- 1. 有问密工单（问密客服介入过的订单）
-- 2. 问密客服操作了“取消订单”
-- 3. 当前时刻仍未入库/上架的订单（只要有入库记录就排除，不管入库时间），且取消时间已经超过2小时
-- 4. 播报时间：10:00, 14:00, 17:00 (由调度系统控制，SQL只需查出符合条件的数据)

with wenmi_works as (
    -- 1. 查找有问密工单的订单 (fwork_type=4 表示问密工单)
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
cancel_action as (
    -- 2. 查找问密客服操作“取消订单”的记录
    select
        x.forder_id,
        x.fauto_create_time as cancel_time
    from (
        select
            a.forder_id,
            a.fauto_create_time,
            a.foperator_name,
            -- 取最早的一次取消操作时间
            row_number() over (partition by a.forder_id order by a.fauto_create_time asc) as num
        from drt.drt_my33310_recycle_t_order_txn a
        inner join drt.drt_my33310_recycle_t_order_status b
            on a.forder_status = b.forder_status_id
        where b.forder_status_name in ('取消中', '已取消')
          and a.fauto_create_time >= to_date(date_sub(now(), 30))
          -- 确保是人工操作的取消，排除系统自动取消或异步通知
          and a.foperator_name not in ('系统', '异步通知', '异步取消')
          and a.foperator_name is not null
          and a.foperator_name != ''
    ) x
    where x.num = 1
),
stock_in_record as (
    -- 3. 查找入库记录 (包含普通入库 freceive_time 和上架入库 fstock_in_time)
    select
        upper(fseries_number) as fseries_number,
        max(freceive_time) as last_receive_time,
        max(fstock_in_time) as last_stock_in_time,
        max(freturn_out_time) as last_return_out_time,
        max(fsale_out_time) as last_sale_out_time
    from dws.dws_instock_details
    where fseries_number is not null
    group by upper(fseries_number)
)
select 
    -- 按照播报格式要求，输出相关信息
    1 as groupkey,
    count(*) as counts,
    group_concat(o.fseries_number) as fseries_number,
    -- 计算超时小时数：当前时间减去取消操作时间
    group_concat(cast(round((unix_timestamp(now()) - unix_timestamp(c.cancel_time))/3600, 1) as string)) as fchaoshi,
    group_concat(
        case when right(left(o.fseries_number,6),4)="0112" then "东莞仓" 
             when right(left(o.fseries_number,6),2)="16" then "杭州仓"
             else "深圳仓" end
    ) as fwms_type
from drt.drt_my33310_recycle_t_order as o
-- 关联订单状态表
left join drt.drt_my33310_recycle_t_order_status as s on o.forder_status = s.forder_status_id
-- 1. 有问密工单
inner join wenmi_works as w on o.fseries_number = w.fseries_number and w.num = 1
-- 2. 问密客服操作了取消订单
inner join cancel_action as c on o.forder_id = c.forder_id
-- 关联入库记录表
left join stock_in_record as i on o.fseries_number = i.fseries_number
where 
    -- 订单当前状态是已取消或取消中
    s.forder_status_name in ('取消中', '已取消')
    -- 3. 当前时刻仍未入库/上架
    -- 只要有任何入库记录（不管时间早晚），一律排除
    and i.last_stock_in_time is null
    and i.last_receive_time is null
    -- 只要有任何出库记录，一律排除
    and i.last_return_out_time is null
    and i.last_sale_out_time is null
    -- >2小时未入库 (从取消操作时间开始计算)
    and (unix_timestamp(now()) - unix_timestamp(c.cancel_time)) / 3600 > 2
    -- 排除测试单
    and o.ftest = 0
group by 1;
