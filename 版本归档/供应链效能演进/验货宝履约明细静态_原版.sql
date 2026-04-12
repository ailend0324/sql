with put as (
select                  --验货宝 取 出库请求时间节点,跟 dws_xy_yhb_detail 用forder_id 进行左连接匹配
    a.forder_id,
    b.fauto_create_time as frequest_put_time  --出库请求时间
from drt.drt_my33315_xy_detect_t_xy_hsb_order as a
left join (
    select 
        *
    from(
    select 
        *,
        row_number()over(partition by forder_id order by fauto_create_time desc) as num
    from drt.drt_my33315_xy_detect_t_xy_yhb_order_txn 
    where forder_status_name in ("待平台发货","待平台退货")) t where num=1
) as b on a.forder_id=b.forder_id
)
select 
    to_date(forder_time) as ftime_by,
    case 
        when left(fhost_barcode,3) like "%010%" then "深圳龙华仓"
        when left(fhost_barcode,3) like "%020%" then "杭州仓"
        when left(fhost_barcode,3) like "%050%" then "东莞仓"
    else "" end as stock_name,
    case when HOUR(forder_time)>0 and HOUR(forder_time)<=20 then "0-20" else ">20" end as "时段",
    count(forder_id) as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
    from dws.dws_xy_yhb_detail
    where forder_time>='2021-01-01'
    group by 1,2,3
union all
select 
    to_date(freceive_time) as ftime_by,
    case 
        when left(fhost_barcode,3) like "%010%" then "深圳龙华仓"
        when left(fhost_barcode,3) like "%020%" then "杭州仓"
        when left(fhost_barcode,3) like "%050%" then "东莞仓"
    else "" end as stock_name,
    case when HOUR(freceive_time)>0 and HOUR(freceive_time)<=20 then "0-20" else ">20" end as "时段",
    null as "下单数",
    count(forder_id) as "收货数",
    count(IF(to_date(fdetect_time)=to_date(freceive_time),forder_id,null)) as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from dws.dws_xy_yhb_detail
where freceive_time>=to_date(date_sub(from_unixtime(unix_timestamp()),1200))
group by 1,2,3
union all
select 
    to_date(fdetect_time) as ftime_by,
    case 
        when left(fhost_barcode,3) like "%010%" then "深圳龙华仓"
        when left(fhost_barcode,3) like "%020%" then "杭州仓"
        when left(fhost_barcode,3) like "%050%" then "东莞仓"
    else "" end as stock_name,
    case when HOUR(fdetect_time)>0 and HOUR(fdetect_time)<=20 then "0-20" else ">20" end as "时段",
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    count(forder_id) as "检测数",
    count(if(Fis_disassembly=1,forder_id,null)) as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from dws.dws_xy_yhb_detail
where fdetect_time>=to_date(date_sub(from_unixtime(unix_timestamp()),1200))
group by 1,2,3
union all
select 
    to_date(fsale_put_time) as ftime_by,
    case 
        when left(fhost_barcode,3) like "%010%" then "深圳龙华仓"
        when left(fhost_barcode,3) like "%020%" then "杭州仓"
        when left(fhost_barcode,3) like "%050%" then "东莞仓"
    else "" end as stock_name,
    case when HOUR(fsale_put_time)>0 and HOUR(fsale_put_time)<=20 then "0-20" else ">20" end as "时段",
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    count(fsale_put_time) as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from dws.dws_xy_yhb_detail
where fsale_put_time>=to_date(date_sub(from_unixtime(unix_timestamp()),1200))
group by 1,2,3
union all
select 
    to_date(frefund_put_time) as ftime_by,
    case 
        when left(fhost_barcode,3) like "%010%" then "深圳龙华仓"
        when left(fhost_barcode,3) like "%020%" then "杭州仓"
        when left(fhost_barcode,3) like "%050%" then "东莞仓"
    else "" end as stock_name,
    case when HOUR(frefund_put_time)>0 and HOUR(frefund_put_time)<=20 then "0-20" else ">20" end as "时段",
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    count(forder_id) as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from dws.dws_xy_yhb_detail
where frefund_put_time>=to_date(date_sub(from_unixtime(unix_timestamp()),1200))
group by 1,2,3
union all
select 
    to_date(fput_time) as ftime_by,
    case 
        when left(fhost_barcode,3) like "%010%" then "深圳龙华仓"
        when left(fhost_barcode,3) like "%020%" then "杭州仓"
        when left(fhost_barcode,3) like "%050%" then "东莞仓"
    else "" end as stock_name,
    case when HOUR(fput_time)>0 and HOUR(fput_time)<=20 then "0-20" else ">20" end as "时段",
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    count(forder_id) as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from dws.dws_xy_yhb_detail
where fput_time>=to_date(date_sub(from_unixtime(unix_timestamp()),1200))
group by 1,2,3
union all
select 
    to_date(fzhibao_create_time) as ftime_by,
    case 
        when left(fhost_barcode,3) like "%010%" then "深圳龙华仓"
        when left(fhost_barcode,3) like "%020%" then "杭州仓"
        when left(fhost_barcode,3) like "%050%" then "东莞仓"
    else "" end as stock_name,
    case when HOUR(fzhibao_create_time)>0 and HOUR(fzhibao_create_time)<=20 then "0-20" else ">20" end as "时段",
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    count(forder_id) as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from dws.dws_xy_yhb_detail
where fzhibao_create_time>=to_date(date_sub(from_unixtime(unix_timestamp()),1200))
group by 1,2,3
union all
select 
    to_date(fzhibao_update_time) as ftime_by,
    case 
        when left(fhost_barcode,3) like "%010%" then "深圳龙华仓"
        when left(fhost_barcode,3) like "%020%" then "杭州仓"
        when left(fhost_barcode,3) like "%050%" then "东莞仓"
    else "" end as stock_name,
    case when HOUR(fzhibao_update_time)>0 and HOUR(fzhibao_update_time)<=20 then "0-20" else ">20" end as "时段",
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    count(forder_id) as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from dws.dws_xy_yhb_detail
where fzhibao_update_time>=to_date(date_sub(from_unixtime(unix_timestamp()),1200))
and fzhibao_status=50 or fzhibao_status=60
group by 1,2,3
union all
select 
    to_date(fcomplain_add_time) as ftime_by,
    case 
        when left(fhost_barcode,3) like "%010%" then "深圳龙华仓"
        when left(fhost_barcode,3) like "%020%" then "杭州仓"
        when left(fhost_barcode,3) like "%050%" then "东莞仓"
    else "" end as stock_name,
    case when HOUR(fcomplain_add_time)>0 and HOUR(fcomplain_add_time)<=20 then "0-20" else ">20" end as "时段",
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    count(forder_id) as "客诉数",
    count(case when fcomplain_add_time is not null and fcomplain_duty=2 then forder_id else null end) as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from dws.dws_xy_yhb_detail
where fcomplain_add_time>=to_date(date_sub(from_unixtime(unix_timestamp()),1200))
group by 1,2,3
union all
select 
    to_date(b.frequest_put_time) as ftime_by,
    case 
        when left(fhost_barcode,3) like "%010%" then "深圳龙华仓"
        when left(fhost_barcode,3) like "%020%" then "杭州仓"
        when left(fhost_barcode,3) like "%050%" then "东莞仓"
    else "" end as stock_name,
    case when HOUR(b.frequest_put_time)>0 and HOUR(b.frequest_put_time)<=20 then "0-20" else ">20" end as "时段",
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    count(a.forder_id) as "出库请求数",
    count(IF(to_date(a.fput_time)=to_date(b.frequest_put_time),a.forder_id,null)) as "当日请求当日出库数"
from dws.dws_xy_yhb_detail as a
left join put as b on a.forder_id=b.forder_id
where b.frequest_put_time>=to_date(date_sub(from_unixtime(unix_timestamp()),1200))
group by 1,2,3
