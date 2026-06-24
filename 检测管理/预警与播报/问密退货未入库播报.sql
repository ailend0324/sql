-- 问密与抽检退货未入库的机器人播报
-- 需求：
-- 1. 已取消/待退货 订单状态＞2小时未入库
-- 2. 抓取问题类型（类别：问密/抽检等）并汇总

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
    -- 限制时间范围
    and from_unixtime(a.fadd_time) >= to_date(date_sub(now(), 30))
),
qc_diff_phones as (
    -- 2. 查找有抽检记录且抽检结果为“选项不一致”的设备条码
    -- (即 QC抽检检测选项与一检检测选项不一致)
    select distinct a.fserial_number 
    from (
        select b.fserial_number, a.fissue_name, a.fanswer_name, a.fdetect_record_id
        from dwd.dwd_detect_back_detection_issue_and_answer_v2 as a
        inner join dwd.dwd_detect_back_detect_detail as b on a.fdetect_record_id=b.fdetect_record_id
        where b.fdet_type=0 
          and a.field_source='fdet_norm_snapshot'
          and a.ds >= to_date(date_sub(now(), 30))
          and b.fserial_number is not null
    ) as a
    inner join (
        select b.fserial_number, a.fissue_name, a.fanswer_name, a.fdetect_record_id
        from dwd.dwd_detect_back_detection_issue_and_answer_v2 as a
        inner join dwd.dwd_detect_back_detect_detail as b on a.fdetect_record_id=b.fdetect_record_id
        where b.fdet_type in (1, 2) 
          and a.field_source='fdet_norm_snapshot'
          and a.ds >= to_date(date_sub(now(), 30))
          and b.fserial_number is not null
    ) as b on a.fserial_number = b.fserial_number and a.fissue_name = b.fissue_name
    where a.fanswer_name <> b.fanswer_name
),
cancel_action as (
    -- 3. 查找订单进入取消/退货链路的时间
    select
        x.forder_id,
        x.cancel_time
    from (
        select
            a.forder_id,
            a.fauto_create_time as cancel_time,
            a.foperator_name,
            -- 取最早一次进入取消/退货链路的操作时间，保持与原版超时口径一致
            row_number() over (partition by a.forder_id order by a.fauto_create_time asc) as num
        from drt.drt_my33310_recycle_t_order_txn a
        inner join drt.drt_my33310_recycle_t_order_status b
            on a.forder_status = b.forder_status_id
        where b.forder_status_name in ('取消中', '已取消', '待退货')
          and a.fauto_create_time >= to_date(date_sub(now(), 30))
          -- 确保是人工操作的，排除系统自动取消或异步通知
          and a.foperator_name not in ('系统', '异步通知', '异步取消')
          and a.foperator_name is not null
          and a.foperator_name != ''
    ) x
    where x.num = 1
),
stock_in_record as (
    -- 4. 查找入库记录 (包含普通入库 freceive_time 和上架入库 fstock_in_time) DWS离线表 T+1
    select
        upper(fseries_number) as fseries_number,
        max(freceive_time) as last_receive_time,
        max(fstock_in_time) as last_stock_in_time,
        max(freturn_out_time) as last_return_out_time,
        max(fsale_out_time) as last_sale_out_time
    from dws.dws_instock_details
    where fseries_number is not null
    group by upper(fseries_number)
),
base_data as (
    -- 5. 组装基础宽表记录
    select 
        o.fseries_number,
        o.forder_id,
        cast(round((unix_timestamp(now()) - unix_timestamp(c.cancel_time))/3600, 1) as string) as fchaoshi,
        case 
            when right(left(o.fseries_number,6),4)="0112" then "东莞仓" 
            when right(left(o.fseries_number,6),2)="16" then "杭州仓"
            else "深圳仓" 
        end as fwms_type,
        case 
            when w.fseries_number is not null and q.fserial_number is not null then '问密,抽检'
            when w.fseries_number is not null then '问密'
            when q.fserial_number is not null then '抽检'
            else '未知'
        end as category_type
    from drt.drt_my33310_recycle_t_order as o
    left join drt.drt_my33310_recycle_t_order_status as s on o.forder_status = s.forder_status_id
    -- 退货状态变更时间
    inner join cancel_action as c on o.forder_id = c.forder_id
    -- 问题类型判定源 
    left join wenmi_works as w on o.fseries_number = w.fseries_number and w.num = 1
    left join qc_diff_phones as q on upper(o.fseries_number) = upper(q.fserial_number)
    -- 关联入库记录表
    left join stock_in_record as i on upper(o.fseries_number) = upper(i.fseries_number)
    where 
        -- 订单当前状态是 已取消 或 待退货
        s.forder_status_name in ('已取消', '待退货')
        -- 只保留属于目标问题类别的记录
        and (w.fseries_number is not null or q.fserial_number is not null)
        -- 当前时刻仍未入库/上架。只要有任何入出库记录（离线），一律排除
        and i.last_stock_in_time is null
        and i.last_receive_time is null
        and i.last_return_out_time is null
        and i.last_sale_out_time is null
        -- >2小时未入库 (从取消操作时间开始计算)
        and (unix_timestamp(now()) - unix_timestamp(c.cancel_time)) / 3600 > 2
        -- 排除测试单
        and o.ftest = 0
)
select 
    1 as groupkey,
    count(*) as counts,
    concat('[', group_concat(concat('\'', fseries_number, '\''), ','), ']') as fseries_number,
    concat('[', group_concat(concat('\'', fchaoshi, '\''), ','), ']') as fchaoshi,
    concat('[', group_concat(concat('\'', category_type, '\''), ','), ']') as category_type
from base_data
group by 1;
