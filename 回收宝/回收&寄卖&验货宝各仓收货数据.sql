select 
    a.fgetin_time,
    a.forder_id,
    a.fseries_number,
    c.fchannel_name,
    f.fname as fclass_name,
    f.fname as fbrand_name,
    case when b.fwarehouse_number=12 then "东莞仓" 
    	 when right(left(a.fseries_number,6),2)="16" then "杭州仓"
    else "深圳仓" end as "仓库",
    case 
        when c.fchannel_name='竞拍销售默认渠道号' then "售后"
    else d.fproject_name end as "业务方",
    case 
        when c.fchannel_name='闲鱼寄卖plus' then "寄卖"
        when c.fchannel_name='竞拍销售默认渠道号' then "售后退回"
        when LEFT(a.fseries_number,2)='AS' then "鱼市售后"
        when ((LEFT(a.fseries_number,2)='CG' and a.fgetin_time>='2024-12-01') or LEFT(a.fseries_number,2)='TL') then "太力项目"
    else "回收" end as "业务类型"
from drt.drt_my33310_recycle_t_order as a
left join dws.dws_instock_details as b on a.fseries_number=b.fseries_number
left join drt.drt_my33310_recycle_t_channel as c on a.fchannel_id=c.fchannel_id
left join drt.drt_my33310_pub_server_channel_center_db_t_pid_info as d on a.fpid=d.fpid
left join drt.drt_my33310_recycle_t_product as e on a.fproduct_id=e.fproduct_id
left join drt.drt_my33310_recycle_t_pdt_class as f on e.fclass_id=f.fid
left join drt.drt_my33310_recycle_t_pdt_brand as g on e.fbrand_id=g.fid
where fgetin_time>=to_date(date_sub(from_unixtime(unix_timestamp()),1100))
and a.ftest=0
and left(a.fseries_number,2)!="YZ"
and left(a.fseries_number,2)!="NT"
union all 
select 
    freceive_time as fgetin_time,
    forder_id,
    fhost_barcode as fseries_number,
    "验货宝" as fchannel_name,
    "手机" as fclass_name,
    null as fbrand_name,
    case 
        when left(fhost_barcode,3) like "%010%" then "深圳仓"
        when left(fhost_barcode,3) like "%020%" then "杭州仓"
        when left(fhost_barcode,3) like "%050%" then "东莞仓"
    else "" end as "仓库",
    "合作项目" as "业务方",
    "验机" as "业务类型"
from dws.dws_xy_yhb_detail 
where freceive_time>=to_date(date_sub(from_unixtime(unix_timestamp()),1100))
and fhost_barcode is not null

