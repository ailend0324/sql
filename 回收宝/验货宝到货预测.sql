with t_parcel as(                        --验机条码明细              --验机包裹物流时效、预计到达时间整理
select 
    fid, 
    Forder_id,
    fseries_number,
    fexpress_number
from(
    select 
        fid, 
        Forder_id,
        upper(Fbar_code) as fseries_number,
        flogistics_num as fexpress_number,
        row_number() over(partition by fbar_code order by fupdate_time desc) as num
    from drt.drt_my33310_xywms_t_parcel 
) t where num=1
),
t_parcel_receive as(                      --验机签收
select 
    fparcel_id,
    fsign_time,
    freceive_user
from(
    select 
        fparcel_id,
        fadd_time as fsign_time,
        fadd_user as freceive_user,
        row_number() over(partition by fparcel_id  order by fadd_time desc) as num
    from drt.drt_my33310_xywms_t_parcel_log
    where ftype=1
) t where num=1),
wuliu_shixiao as( 
select 
    d.fseller_city_name,
    ceil(avg(case when c.fsign_time>a.forder_time then (unix_timestamp(c.fsign_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(a.forder_time,'yyyy-MM-dd HH:mm:ss'))/(3600*24) else null end)) as avg_day
from drt.drt_my33315_xy_detect_t_xy_hsb_order as a
left join t_parcel as b on a.flogistics_number=b.fexpress_number
left join t_parcel_receive as c on b.fid=c.fparcel_id
left join drt.drt_my33315_xy_detect_t_xy_yhb_detect_order as d on a.fxy_order_id=d.fxy_order_id
where c.fsign_time >=to_date(date_sub(from_unixtime(unix_timestamp()),31))
and d.fseller_city_name <> "" 
group by 1)
select 
    a.fxy_order_id,
    a.flogistics_number, 
    a.fhost_barcode,
    d.fseller_city_name,
    case 
        when d.Fseller_province_name like "%北京%" then "杭州仓"
        when d.Fseller_province_name like "%山东%" then "杭州仓"
        when d.Fseller_province_name like "%上海%" then "杭州仓" 
        when d.Fseller_province_name like "%江苏%" then "杭州仓"
        when d.Fseller_province_name like "%浙江%" then "杭州仓" else "深圳仓" end as "仓库",
    d.forder_dtime,
    a.forder_time, 
    c.fsign_time,
    e.avg_day
from drt.drt_my33315_xy_detect_t_xy_hsb_order as a
left join t_parcel as b on a.flogistics_number=b.fexpress_number
left join t_parcel_receive as c on b.fid=c.fparcel_id
left join drt.drt_my33315_xy_detect_t_xy_yhb_detect_order as d on a.fxy_order_id=d.fxy_order_id
left join wuliu_shixiao as e on d.fseller_city_name=e.fseller_city_name
where a.forder_time>=to_date(date_sub(from_unixtime(unix_timestamp()),31))
and d.fseller_city_name != "" 
and c.fsign_time is null


