with t_instock_inout as (                     
    select
        upper(fserial_no) as fseries_number,
        min(case when fcmd = 'CGRK' then fchange_time else null end) as fstock_in_time,
        max(case when fcmd = 'CGTH' then fchange_time else null end) as freturn_out_time,
        max(case when fcmd = 'JYCK' then fchange_time else null end) as fsale_out_time
    from drt.drt_my33312_hsb_sales_product_t_pm_wms_stock_notify
    where fchange_time>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
    and fcmd in ( 'CGTH','CGRK')
    group by fserial_no
), 

stock_out_request as(
select
    forder_id,
    fseries_number,
    fauto_create_time as frequest_endtime
from (
    select 
        a.forder_id,
        c.fseries_number,
        a.fauto_create_time,
        row_number()over(partition by a.forder_id order by a.fauto_create_time desc) as num
    from drt.drt_my33310_recycle_t_order_txn as a
    left join drt.drt_my33310_recycle_t_order_status as b on a.forder_status=b.forder_status_id
    left join drt.drt_my33310_recycle_t_order as c on a.forder_id=c.forder_id
    where b.forder_status_name in ("待退货","已取消")
    and a.fauto_create_time>=to_date(date_sub(from_unixtime(unix_timestamp()),14))
)t where num=1
),

remark as (
select 
    forder_id,
    fremark
from (
    select 
        *,
        row_number()over(partition by forder_id order by fcreate_time desc) as num
    from drt.drt_my33310_recycle_t_order_remark
    where fcreate_time>=to_date(date_sub(from_unixtime(unix_timestamp()),50))
    and fremark is not null
    and (fremark like "%拆坏%"           
    or fremark like "%清库存%"          
    or fremark like "%自行处理%"        
    or fremark like "%环保%"            
    or fremark like "%虚拟出库%"        
    or fremark like "%退回物流单号%"    
    or fremark like "%无实物%"          
    or fremark like "%预付款已操作入库%" 
    or fremark like "%刷单%"            
    or fremark like "%测试%"            
    or fremark like "%批量取消%"        
    or fremark like "%超额运费未扣%")   
    )t
where num=1
)
,

tuihuo as (
select 
    1 as groupkey,
    to_date(b.frequest_endtime) as "退货时间",
    b.fseries_number,
    case when left(b.fseries_number,2)='BM' then "寄卖" else "回收" end as "业务",
    case when right(left(b.fseries_number,6),4)="0112" then "东莞仓" 
    	 when right(left(b.fseries_number,6),2)="16" then "杭州仓"
    else "深圳仓" end as fwms_type,
    b.frequest_endtime,
    d.fremark,
    round((unix_timestamp(from_timestamp(now(),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(b.frequest_endtime,'yyyy-MM-dd HH:mm:ss'))/3600,1) as fchaoshi
from stock_out_request as b
left join t_instock_inout as a on a.fseries_number=b.fseries_number
left join drt.drt_my33310_recycle_t_order as c on b.fseries_number=c.fseries_number
left join remark as d on c.forder_id=d.forder_id
left join drt.drt_my33310_recycle_t_xy_order_data as f on c.Forder_id=f.forder_id
left join drt.drt_my33310_recycle_t_order_status as e on c.forder_status=e.forder_status_id
where b.frequest_endtime>=to_date(date_sub(from_unixtime(unix_timestamp()),7))
and (e.forder_status_name like "%退货%" or e.forder_status_name like "%已取消%")
and e.forder_status_name!="已退货"
and c.ftest=0
and a.freturn_out_time is null
and d.fremark is null
and left(b.fseries_number,2)!='YZ'  
and left(b.fseries_number,2)!='AS'  
and left(b.fseries_number,2)!='NT'  
and left(b.fseries_number,2)!='BB'  
and left(b.fseries_number,2)!='CG'  
and left(b.fseries_number,2)!='TL'  
and (unix_timestamp(from_timestamp(now(),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(b.frequest_endtime,'yyyy-MM-dd HH:mm:ss'))/3600>=20
and b.fseries_number!='BM0101191103001998'
and c.frecycle_type=1
and not (
    left(b.fseries_number,2)='BM' 
    and year(c.forder_time)<2025
)
order by b.frequest_endtime asc
)

select 
    groupkey,
    count(*)  as counts,
    group_concat(fseries_number) as fseries_number,
    group_concat(cast(cast(fchaoshi as int) as string)) as fchaoshi,
    group_concat(fwms_type) as fwms_type
from  tuihuo group by groupkey