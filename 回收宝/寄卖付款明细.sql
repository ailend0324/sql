select 
    if(a.fpay_out_time is not null,a.fpay_out_time,c.fpay_time) as fpay_time,
    a.forder_id,
    cast(e.fxy_order_id as string) as fxy_order_id,
    a.forder_num,
    a.fseries_number, 
    b.Fbuyer_fee_order_id,
    case when a.frecycle_type=1 then "邮寄"
    	 when a.frecycle_type=2 then "上门"
         when a.frecycle_type=3 then "到店"
    else null end as "回收方式",
    case when b.fsales_type=1 then "买家购买"
         when b.fsales_type=2 then "平台回收"
         when b.fsales_type=3 then "寄卖回收"
         when b.fsales_type=4 then "买家购买(一口价)"
         when b.fsales_type=0 and a.fpay_out_price>0 then "平台回收"
    else null end as fsales_type,
    b.fsales_amount/100 as fsales_amount,
    case when b.fsales_type=2 then Frecycle_service_price/100 
         when b.fsales_type=0 and a.fpay_out_price>0 then Frecycle_service_price/100 else Fservice_price/100 end as Fservice_price,
    case when Fuse_xy_detect_price=0 then "否"
         when Fuse_xy_detect_price=1 then "是"
    else null end as Fuse_xy_detect_price,
    case when Fuse_xy_detect_price=1 then Fxy_starting_price/100 else Fnew_bottom_price/100 end as fstarting_price,
    a.fpay_out_price/100 as fpay_out_price,
    case when a.fsupply_partner=2 then "小站(自营)" 
         when a.fsupply_partner=3 then "小站(加盟)"
    else "回收宝" end as fsupply_partner,
    d.fchannel_name,
    b.fxy_recycle_price/100 as fxy_recycle_price,
    case when b.fonly_auction=1 then "是" else "否" end as fonly_auction,
    case when b.fuse_xy_recycle_price=1 then "是" else "否" end as fuse_xy_recycle_price,
    b.flow_start_price/100 as flow_start_price,
    b.fseller_fee_commission/100 as fseller_fee_commission
from drt.drt_my33310_recycle_t_order as a
inner join drt.drt_my33310_recycle_t_xy_jimai_plus_order as b on a.forder_id=b.forder_id
left join (select                            --取买家付款时间节点                        
        *
    from 
        (select 
            a.forder_id,
            a.fauto_update_time as fpay_time,
            row_number() over(partition by a.forder_id order by a.fauto_update_time desc) as num
        from drt.drt_my33310_recycle_t_order_txn as a
        inner join drt.drt_my33310_recycle_t_xy_jimai_plus_order as b on a.forder_id=b.forder_id
        where a.forder_status in (714,814)) t
    where num=1
    ) as c on a.forder_id=c.forder_id
left join drt.drt_my33310_recycle_t_channel as d on a.fchannel_id=d.fchannel_id
left join drt.drt_my33318_xy_bangmai_t_xybangmai_order_map as e on a.forder_id=e.forder_id
where (to_date(a.fpay_out_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),366)) or to_date(c.fpay_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),366)))
