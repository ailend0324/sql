select 
    *,
    case when right(left(fserial_no,6),2)='16' then '杭州' else '深圳' end as fwarehouse,
    left(fserial_no,2) as fchannel,
    case
        when fcmd='JYCK' then '发货'
        when fcmd='CGRK' then '收货'
        when fcmd='CGTH' then '退货'
        when fcmd='move' then '移库'
        else fcmd
    end as fbiz_type,
    case
        -- 发货（JYCK）
        when fcmd='JYCK' and (
            locate('T57', warehouseid) > 0 or
            locate('T58', warehouseid) > 0 or
            locate('T59', warehouseid) > 0 or
            locate('T60', warehouseid) > 0 or
            locate('H', warehouseid) > 0 or
            locate('J', warehouseid) > 0 or
            locate('K', warehouseid) > 0 or
            locate('L', warehouseid) > 0
        ) then '回收销售出库(电脑)'
        when fcmd='JYCK' then '回收销售出库'
        -- 收货（CGRK）
        when fcmd='CGRK' and (
            locate('01DN', warehouseid) > 0 or
            locate('I1', warehouseid) > 0 or
            locate('J1', warehouseid) > 0 or
            locate('A1', warehouseid) > 0 or
            locate('K1', warehouseid) > 0 or
            locate('L1', warehouseid) > 0 or
            locate('T', warehouseid) > 0 or
            locate('Z94-10', warehouseid) > 0 or
            locate('Z94-11', warehouseid) > 0 or
            locate('S27-10', warehouseid) > 0 or
            locate('S27-01', warehouseid) > 0 or
            locate('S27-09', warehouseid) > 0 or
            locate('C77-09', warehouseid) > 0
        ) then '其它库位'
        when fcmd='CGRK' then 'B-H&N库位'
        -- 退货（CGTH）
        when fcmd='CGTH' and (
            locate('T57', warehouseid) > 0 or
            locate('T58', warehouseid) > 0 or
            locate('T59', warehouseid) > 0 or
            locate('T60', warehouseid) > 0 or
            locate('H', warehouseid) > 0 or
            locate('J', warehouseid) > 0 or
            locate('K', warehouseid) > 0 or
            locate('L', warehouseid) > 0
        ) then '回收退货出库(电脑)'
        when fcmd='CGTH' then '回收退货出库'
        -- 移库（move）
        when fcmd='move' and (
            locate('01DN', warehouseid) > 0 or
            locate('I1', warehouseid) > 0 or
            locate('J1', warehouseid) > 0 or
            locate('A1', warehouseid) > 0 or
            locate('K1', warehouseid) > 0 or
            locate('L1', warehouseid) > 0 or
            locate('T', warehouseid) > 0 or
            locate('Z94-10', warehouseid) > 0 or
            locate('Z94-11', warehouseid) > 0 or
            locate('S27-10', warehouseid) > 0 or
            locate('S27-01', warehouseid) > 0 or
            locate('S27-09', warehouseid) > 0 or
            locate('C77-09', warehouseid) > 0
        ) then '其它库位-移库'
        when fcmd='move' then 'B-H&N库位-移库'
        else null
    end as ftype
from dwd.dwd_t_pm_wms_stock_notify 
where to_date(fcreate_time) >= TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 30))
and fcmd in ('JYCK','CGRK','CGTH','move');


