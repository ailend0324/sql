with detect as (
    select
        fserial_number,
        fclass_name
    from (
        select 
            *,
            row_number() over(partition by fserial_number order by fend_time asc) as num
        from drt.drt_my33310_detection_t_detect_record 
        where fdet_type=0
        and fis_deleted=0
      	and freport_type=0
    	and fverdict<>"测试单"
        and to_date(fend_time) >='2021-01-01')t
    where num=1
),
deal as (
select 
    to_date(fpay_out_time) as fdate,
    case when right(left(fseries_number,6),4)='0112' or right(left(fseries_number,6),4)='0118' then "东莞仓" 
  		when right(left(fseries_number,6),2)="16" then "杭州仓"
  else "深圳仓" end as place,
    case when left(fseries_number,2)="BB" then "寄卖" else "回收" end as ftype,
    case when a.frecycle_type=2 and fsupply_partner=2 then "闲鱼小站-自营上门"
         when a.frecycle_type=2 and fsupply_partner=3 then "闲鱼小站-加盟门店-上门"
         when a.frecycle_type=3 and fsupply_partner=2 then "闲鱼小站-自营门店-到店"
         when a.frecycle_type=3 and fsupply_partner=3 then "闲鱼小站-加盟门店-到店"
         when left(fseries_number,2) in ('XY','YJ') then "2C闲鱼"
         when left(fseries_number,2) in ('TM','TY') then "天猫以旧换新"
         when left(fseries_number,2)='ZF' then "支付宝小程序"
         when left(fseries_number,2)='CG' then "外采"
         when left(fseries_number,2)='QT' and c.fchannel_name like "%闲鱼小站%" then "2C闲鱼"
         when left(fseries_number,2)="BB" then "换机侠B端帮卖"
         when left(fseries_number,2)='ZY' and b.faccount='wendylei@huishoubao.com.cn' then "滞留单"
    else "自有渠道" end as fchannel,
    case when a.frecycle_type=1 then "邮寄"
         when a.frecycle_type=2 then "上门"
         when a.frecycle_type=3 then "到店"
    else null end as "回收方式",
    case when fsupply_partner=2 then "小站(自营)"
         when fsupply_partner=3 then "小站(加盟)"
    else "回收宝" end as "履约方",
    fseries_number,
    forder_num,
    forder_id,
    if(e.fname is null,f.fclass_name,e.fname) as fname
from drt.drt_my33310_recycle_t_order as a
left join drt.drt_my33310_recycle_t_account_info as b on a.faccount_id=b.faccount_id
left join drt.drt_my33310_recycle_t_channel as c on a.fchannel_id=c.fchannel_id
left join drt.drt_my33310_recycle_t_product as d on a.fproduct_id=d.fproduct_id
left join drt.drt_my33310_recycle_t_pdt_class as e on d.fclass_id=e.fid
left join detect as f on a.fseries_number=f.fserial_number
where to_date(fpay_out_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
and ftest=0
and left(fseries_number,2) not in ('YZ','BM')
and forder_status not in (88)
and forder_id not in (
select 
    distinct(forder_id)
from drt.drt_my33310_recycle_t_order_txn 
where forder_status=351
)
union all
select                   --取自有渠道已退款状态无付款时间的数量
    to_date(b.fpay_time) as fdate,
    case when right(left(fseries_number,6),4)='0112' or right(left(fseries_number,6),4)='0118' then "东莞仓" 
  		when right(left(fseries_number,6),2)="16" then "杭州仓"
  else "深圳仓" end as place,
    case when left(fseries_number,2)="BB" then "寄卖" else "回收" end as ftype,
    case when a.frecycle_type=2 and fsupply_partner=2 then "闲鱼小站-自营上门"
         when a.frecycle_type=2 and fsupply_partner=3 then "闲鱼小站-加盟门店-上门"
         when a.frecycle_type=3 and fsupply_partner=2 then "闲鱼小站-自营门店-到店"
         when a.frecycle_type=3 and fsupply_partner=3 then "闲鱼小站-加盟门店-到店"
         when left(fseries_number,2) in ('XY','YJ') then "2C闲鱼"
         when left(fseries_number,2) in ('TM','TY') then "天猫以旧换新"
         when left(fseries_number,2)='ZF' then "支付宝小程序"
         when left(fseries_number,2)='QT' and d.fchannel_name like "%闲鱼小站%" then "2C闲鱼"
         when left(fseries_number,2)="BB" then "换机侠B端帮卖"
         when left(fseries_number,2)='CG' then "外采"
         when left(fseries_number,2)='ZY' and c.faccount='wendylei@huishoubao.com.cn' then "滞留单"
    else "自有渠道" end as fchannel,
    case when a.frecycle_type=1 then "邮寄"
         when a.frecycle_type=2 then "上门"
         when a.frecycle_type=3 then "到店"
    else null end as "回收方式",
    case when fsupply_partner=2 then "小站(自营)"
         when fsupply_partner=3 then "小站(加盟)"
    else "回收宝" end as "履约方",
    fseries_number,
    forder_num,
    a.forder_id,
    if(f.fname is null,g.fclass_name,f.fname) as fname
from drt.drt_my33310_recycle_t_order as a
left join (
    select 
        *
    from (
        select 
            forder_id,
            fauto_update_time as fpay_time,
            row_number() over(partition by forder_id order by fauto_update_time asc) as num
        from drt.drt_my33310_recycle_t_order_txn
        where forder_status in (71))t
    where num=1
) as b on a.forder_id=b.forder_id
left join drt.drt_my33310_recycle_t_account_info as c on a.faccount_id=c.faccount_id
left join drt.drt_my33310_recycle_t_channel as d on a.fchannel_id=d.fchannel_id
left join drt.drt_my33310_recycle_t_product as e on a.fproduct_id=e.fproduct_id
left join drt.drt_my33310_recycle_t_pdt_class as f on e.fclass_id=f.fid
left join detect as g on a.fseries_number=g.fserial_number
where to_date(b.fpay_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
and ftest=0
and a.fpay_out_time is null
and left(fseries_number,2) not in ('YZ','BM')
and forder_status not in (88)
and a.forder_id not in (
select 
    distinct(forder_id)
from drt.drt_my33310_recycle_t_order_txn 
where forder_status=351
)
union all
select                        --验机转回收
    to_date(fauto_create_time) as fdate,
    "深圳仓" as place,
    "验机" as ftype,
    "验货宝" as fchannel,
    "邮寄" as "回收方式",
    "回收宝" as "履约方",
    fhost_barcode,
    cast(a.fxy_order_id as string) as fxy_order_id,
    forder_id,
    "手机" as fname
from drt.drt_my33315_xy_detect_t_xy_yhb_recycle_pay_record as a 
left join drt.drt_my33315_xy_detect_t_xy_hsb_order as b on a.fxy_order_id=b.fxy_order_id
where to_date(fauto_create_time) between '2023-09-01' and '2023-09-30'
union all 
select                                          --寄卖订单
    to_date(d.fpay_time) as fdate,
    case when right(left(fseries_number,6),4)='0112' or right(left(fseries_number,6),4)='0118' then "东莞仓" 
  		 when right(left(fseries_number,6),2)="16" then "杭州仓"
  else "深圳仓" end as place,
    "寄卖" as ftype,
    case when a.frecycle_type=2 and fsupply_partner=2 and d.forder_status=815 then "闲鱼小站-自营上门-一口价"
         when a.frecycle_type=2 and fsupply_partner=2 and d.forder_status=714 then "闲鱼小站-自营上门-帮卖成交"
         when a.frecycle_type=2 and fsupply_partner=3 and d.forder_status=815 then "闲鱼小站-加盟门店-上门-一口价"
         when a.frecycle_type=2 and fsupply_partner=3 and d.forder_status=714 then "闲鱼小站-加盟门店-上门-帮卖成交"
         when a.frecycle_type=3 and fsupply_partner=2 and d.forder_status=815 then "闲鱼小站-自营门店-到店-一口价"
         when a.frecycle_type=3 and fsupply_partner=2 and d.forder_status=714 then "闲鱼小站-自营门店-到店-帮卖成交"
         when a.frecycle_type=3 and fsupply_partner=3 and d.forder_status=815 then "闲鱼小站-加盟门店-到店-一口价"
         when a.frecycle_type=3 and fsupply_partner=3 and d.forder_status=714 then "闲鱼小站-加盟门店-到店-帮卖成交"
         when c.fchannel_name="小豹帮卖" then "自有渠道"
         when d.forder_status=815 and a.frecycle_type=1 then "闲鱼寄卖plus-一口价"
    else "闲鱼寄卖plus-帮卖成交" end as fchannel,
    case when a.frecycle_type=1 then "邮寄"
         when a.frecycle_type=2 then "上门"
         when a.frecycle_type=3 then "到店"
    else null end as "回收方式",
    case when a.fsupply_partner=2 then "小站(自营)"
         when a.fsupply_partner=3 then "小站(加盟)"
    else "回收宝" end as "履约方",
    a.fseries_number,
    a.forder_num,
    a.forder_id,
    f.fname
    from drt.drt_my33310_recycle_t_order as a
    left join 
    (select                            --取买家付款时间节点                        
        *
    from 
        (select 
            a.forder_id,
            a.fauto_update_time as fpay_time,
            a.forder_status,
            row_number() over(partition by a.forder_id order by a.fauto_update_time asc) as num
        from drt.drt_my33310_recycle_t_order_txn as a
        inner join drt.drt_my33310_recycle_t_xy_jimai_plus_order as b on a.forder_id=b.forder_id
        where a.forder_status in (714,815)) t
    where num=1
    ) as d on a.forder_id=d.forder_id
    left join drt.drt_my33310_recycle_t_channel as c on a.fchannel_id=c.fchannel_id
    left join drt.drt_my33310_recycle_t_product as e on a.fproduct_id=e.fproduct_id
left join drt.drt_my33310_recycle_t_pdt_class as f on e.fclass_id=f.fid
where to_date(d.fpay_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
and ftest=0
and left(fseries_number,2) in ("BM")
and a.forder_status not in (90,110)
union all
select                             --取寄卖兜底回收
    to_date(b.fpay_time) as fdate,
    case when right(left(fseries_number,6),4)='0112' or right(left(fseries_number,6),4)='0118' then "东莞仓" 
  		 when right(left(fseries_number,6),2)="16" then "杭州仓"
  else "深圳仓" end as place,
    "寄卖" as ftype,
    case when a.frecycle_type=2 and fsupply_partner=2 then "闲鱼小站-自营上门-公司兜底"
         when a.frecycle_type=2 and fsupply_partner=3 then "闲鱼小站-加盟门店-上门-加盟商兜底"
         when a.frecycle_type=3 and fsupply_partner=2 then "闲鱼小站-自营门店-到店-公司兜底"
         when a.frecycle_type=3 and fsupply_partner=3 then "闲鱼小站-加盟门店-到店-加盟商兜底"
         when c.fchannel_name="小豹帮卖" then "自有渠道"
    else "闲鱼寄卖plus-公司兜底" end as fchannel,
    case when a.frecycle_type=1 then "邮寄"
         when a.frecycle_type=2 then "上门"
         when a.frecycle_type=3 then "到店"
    else null end as "回收方式",
    case when a.fsupply_partner=2 then "小站(自营)"
         when a.fsupply_partner=3 then "小站(加盟)"
    else "回收宝" end as "履约方",
    a.fseries_number,
    a.forder_num,
    a.forder_id,
    f.fname
from drt.drt_my33310_recycle_t_order as a 
left join 
    (select                            --取买家付款时间节点                        
        *
    from 
        (select 
            a.forder_id,
            a.fauto_update_time as fpay_time,
            row_number() over(partition by a.forder_id order by a.fauto_update_time asc) as num
        from drt.drt_my33310_recycle_t_order_txn as a
        inner join drt.drt_my33310_recycle_t_xy_jimai_plus_order as b on a.forder_id=b.forder_id
         left join drt.drt_my33310_recycle_t_order as c on a.forder_id=c.forder_id 
        where a.forder_status in (714,815)
        and c.forder_status not in (90,110)
        ) t
    where num=1
    ) as d on a.forder_id=d.forder_id
    left join drt.drt_my33310_recycle_t_channel as c on a.fchannel_id=c.fchannel_id
    left join (
    select 
        *
    from (
    select 
        forder_id,
        fauto_create_time as fpay_time,
        row_number()over(partition by forder_id order by fauto_create_time asc) as num
    from drt.drt_my33310_recycle_t_order_txn 
    where forder_status in (260,261,130))t
    where num=1
    ) as b on a.forder_id=b.forder_id
    left join drt.drt_my33310_recycle_t_product as e on a.fproduct_id=e.fproduct_id
    left join drt.drt_my33310_recycle_t_pdt_class as f on e.fclass_id=f.fid
where to_date(b.fpay_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
and ftest=0
and d.fpay_time is null
and left(fseries_number,2) in ('BM')
union all
select 
    case when to_date(b.Fseller_pay_time)>='2023-01-01' then to_date(b.fseller_pay_time) else to_date(b.fbuyer_pay_time) end as fdate,
    case when left(fhost_barcode,3) like "%050%" then "东莞仓"
         when left(fhost_barcode,3) like "%020%" then "杭州仓"
         when left(fhost_barcode,3) like "%010%" then "深圳仓"
    else "深圳仓" end as place,
    "验机" as ftype,
    "验货宝" as fchannel,
    "邮寄" as "回收方式",
    "回收宝" as "履约方",
    fhost_barcode,
    cast(a.fxy_order_id as string) as fxy_order_id,
    a.forder_id,
    "手机" as fname
from dws.dws_xy_yhb_detail as a 
left join drt.drt_my33315_xy_detect_t_xy_yhb3_detect_fee as b on a.fxy_order_id=b.fxy_order_id
where (to_date(b.Fseller_pay_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),720)) or to_date(b.Fbuyer_pay_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),720)))
and a.fxy_order_id not in (
    select 
        distinct(fxy_order_id)
    from drt.drt_my33315_xy_detect_t_xy_yhb_recycle_pay_record 
)
union all
select 
    to_date(fsale_put_time) as fdate,
    case when left(fhost_barcode,3) like "%050%" then "东莞仓"
         when left(fhost_barcode,3) like "%020%" then "杭州仓"
         when left(fhost_barcode,3) like "%010%" then "深圳仓"
    else null end as place,
    "验机" as ftype,
    "验货宝" as fchannel,
    "邮寄" as "回收方式",
    "回收宝" as "履约方",
    fhost_barcode,
    cast(fxy_order_id as string) as fxy_order_id,
    forder_id,
    "手机" as fname
from dws.dws_xy_yhb_detail
where to_date(fsale_put_time) between to_date(date_sub(from_unixtime(unix_timestamp()),720)) and '2023-03-31'
)
select 
    fdate,
    place,
    case when ftype="寄卖" and fchannel='换机侠B端帮卖' then "回收"
         when ftype="寄卖" and fchannel='自有渠道' then "回收"
    else ftype end as ftype,
    "检测" as fdepart,
    count(distinct case when ftype="寄卖" and fchannel like "%闲鱼小站-自营上门%" then null else fseries_number end) as num
from deal
where fchannel not like "%闲鱼小站%" 
and fchannel not in ('滞留单')
group by 1,2,3,4
union all
select 
    fdate,
    place,
    case when ftype="寄卖" and fchannel='换机侠B端帮卖' then "回收"
         when ftype="寄卖" and fchannel='自有渠道' then "回收"
    else ftype end as ftype,
    "仓库" as fdepart,
    count(distinct fseries_number) as num
from deal
where fchannel not like "%闲鱼小站-加盟门店%" 
and fchannel not in ('滞留单','闲鱼小站-自营门店-到店-一口价','闲鱼小站-自营上门-帮卖成交','闲鱼小站-自营上门-一口价','闲鱼小站-自营门店-到店-帮卖成交')
group by 1,2,3,4
union all
select 
    fdate,
    place,
    case when ftype="寄卖" and fchannel='换机侠B端帮卖' then "回收"
         when ftype="寄卖" and fchannel='自有渠道' then "回收"
    else ftype end as ftype,
    "客服" as fdepart,
    count(distinct fseries_number) as num
from deal
where fchannel not like "%闲鱼小站-加盟门店%" 
and fchannel not like "%闲鱼小站-自营%" 
and fchannel not in ('滞留单','外采')
group by 1,2,3,4
