-- 入库/退货出库记录
with t_instock_inout as (
    select
        upper(fserial_no)                                                   as fseries_number,
        min(case when fcmd = 'CGRK' then fchange_time end)                  as fstock_in_time,
        max(case when fcmd = 'CGTH' then fchange_time end)                  as freturn_out_time
    from drt.drt_my33312_hsb_sales_product_t_pm_wms_stock_notify
    where fchange_time >= to_date(date_sub(now(), 30))
      and fcmd in ('CGTH', 'CGRK')
    group by upper(fserial_no)
),

-- 退货/取消申请
stock_out_request as (
    select
        x.forder_id,
        c.fseries_number,
        x.fauto_create_time                                                 as frequest_endtime
    from (
        select
            a.forder_id,
            a.fauto_create_time,
            row_number() over (partition by a.forder_id order by a.fauto_create_time desc) as num
        from drt.drt_my33310_recycle_t_order_txn a
        inner join drt.drt_my33310_recycle_t_order_status b
            on a.forder_status = b.forder_status_id
        where b.forder_status_name in ('待退货', '已取消')
          and a.fauto_create_time >= to_date(date_sub(now(), 14))
    ) x
    inner join drt.drt_my33310_recycle_t_order c
        on x.forder_id = c.forder_id
    where x.num = 1
      and c.fseries_number is not null
      and length(regexp_replace(c.fseries_number, '\\s+', '')) > 0
),

-- 排除型备注
remark as (
    select
        forder_id,
        fremark
    from (
        select
            r.*,
            row_number() over (partition by r.forder_id order by r.fcreate_time desc) as num
        from drt.drt_my33310_recycle_t_order_remark r
        where r.fcreate_time >= to_date(date_sub(now(), 50))
          and r.fremark is not null
          and (
                 r.fremark like "%拆坏%"
              or r.fremark like "%清库存%"
              or r.fremark like "%自行处理%"
              or r.fremark like "%环保%"
              or r.fremark like "%虚拟出库%"
              or r.fremark like "%退回物流单号%"
              or r.fremark like "%无实物%"
              or r.fremark like "%预付款已操作入库%"
              or r.fremark like "%刷单%"
              or r.fremark like "%测试%"
              or r.fremark like "%批量取消%"
              or r.fremark like "%超额运费未扣%"
          )
    ) t
    where t.num = 1
),

-- 退货预警表
tuihuo as (
    select
        1                                                                   as groupkey,
        to_date(b.frequest_endtime)                                         as `退货时间`,
        b.fseries_number,
        case when substr(b.fseries_number, 1, 2) = 'BM' 
             then '寄卖' else '回收' end                                    as `业务`,
        case when substr(b.fseries_number, 3, 4) = '0112' then '东莞仓'
             when substr(b.fseries_number, 5, 2) = '16'   then '杭州仓'
             else '深圳仓' end                                               as fwms_type,
        b.frequest_endtime,
        round((unix_timestamp(now()) - unix_timestamp(b.frequest_endtime)) / 3600, 1) 
                                                                           as fchaoshi
    from stock_out_request b
    left join t_instock_inout a
        on a.fseries_number = b.fseries_number
    inner join drt.drt_my33310_recycle_t_order c
        on b.fseries_number = c.fseries_number
    inner join drt.drt_my33310_recycle_t_order_status e
        on c.forder_status = e.forder_status_id
    where b.frequest_endtime >= to_date(date_sub(now(), 7))
      and (e.forder_status_name like "%退货%" or e.forder_status_name like "%已取消%")
      and e.forder_status_name != '已退货'
      and c.ftest = 0
      and a.freturn_out_time is null
      and not exists (select 1 from remark d where d.forder_id = c.forder_id)
      and substr(b.fseries_number, 1, 2) not in ('YZ','AS','NT','BB','CG','TL')
      and (unix_timestamp(now()) - unix_timestamp(b.frequest_endtime)) / 3600 >= 20
      and b.fseries_number != 'BM0101191103001998'
      and (substr(b.fseries_number, 1, 2) = 'BM' or c.frecycle_type = 1)
      and not (
          substr(b.fseries_number, 1, 2) = 'BM'
          and year(c.forder_time) < 2025
      )
      and length(regexp_replace(b.fseries_number, '\\s+', '')) > 0
)

-- 最终输出
select
    groupkey,
    count(*)                                                               as counts,
    group_concat(regexp_replace(fseries_number, '\\s+', ''), '\n')         as fseries_number,
    group_concat(cast(cast(fchaoshi as int) as string), '\n')              as fchaoshi,
    group_concat(fwms_type, '\n')                                          as fwms_type
from tuihuo
group by groupkey;