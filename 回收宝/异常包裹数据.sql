with yhb as (
select 
    flogistics_num,
    fbar_code
from (
    select 
        *,
        row_number()over(partition by flogistics_num order by fadd_time desc) as num
    from drt.drt_my33310_xywms_t_parcel 
    where to_date(fadd_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),180))
    and fstatus=5
    )t
where num=1
)
select 
    a.fid,
    "验机" as ftype,
    case when left(c.fbar_code,2)='01' then "深圳仓"
         when left(c.fbar_code,2)="02" then "杭州仓"
    else null end as fwarehouse,
    a.fadd_user,
    b.flogistics_code,
    c.fbar_code,
    d.fdetect_time,
    from_unixtime(a.fadd_time),
    from_unixtime(a.fupdate_time),
    d.freceive_time,
    a.fupdate_user,
    case when d.freceive_time is not null and (unix_timestamp(d.freceive_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(from_unixtime(a.fupdate_time),'yyyy-MM-dd HH:mm:ss'))>0 then (unix_timestamp(d.freceive_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(from_unixtime(a.fupdate_time),'yyyy-MM-dd HH:mm:ss'))/3600 else null end as "异常包裹完结收货时效"
from drt.drt_my33310_csrdb_t_works as a
left join drt.drt_my33312_csrdb_t_logistics_info as b on a.fid=b.fwork_id
left join yhb as c on upper(b.flogistics_code)=upper(c.flogistics_num)
left join dws.dws_xy_yhb_detail as d on c.fbar_code=d.fhost_barcode
where from_unixtime(a.Fadd_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and a.fwork_type in(3)
and a.fwork_source<>3
and a.fappeal_type1<>0
and a.fduty_content not like "%无效工单%"
and a.fwork_status=40
and c.flogistics_num is not null
and a.forder_system=1
union all
select 
    a.fid,
    case when left(c.fseries_number,2)="BM" then "帮卖" 
    	 when left(c.fseries_number,2)='TL' or (left(c.fseries_number,2)='CG' and c.fgetin_time>='2024-12-01') then "太力" else "回收" end as ftype,
    case when right(left(c.fseries_number,6),2)='16' then "杭州仓" else "深圳仓" end as fwarehouse,
    a.fadd_user,
    b.flogistics_code,
    c.fseries_number,
    c.fdetect_time,
    from_unixtime(a.fadd_time),
    from_unixtime(a.fupdate_time),
    c.fgetin_time,
    a.fupdate_user,
    case when c.fgetin_time is not null and (unix_timestamp(c.fgetin_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(from_unixtime(a.fupdate_time),'yyyy-MM-dd HH:mm:ss'))>0 then (unix_timestamp(c.fgetin_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(from_unixtime(a.fupdate_time),'yyyy-MM-dd HH:mm:ss'))/3600 else null end as "异常包裹完结收货时效"
from drt.drt_my33310_csrdb_t_works as a
left join drt.drt_my33312_csrdb_t_logistics_info as b on a.fid=b.fwork_id
left join dws.dws_hs_order_detail as c on a.forder_sn=c.forder_num
where from_unixtime(a.Fadd_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and a.fwork_type in(3)
and a.fwork_source<>3
and a.fappeal_type1<>0
and a.fduty_content not like "%无效工单%"
and a.fwork_status=40
and c.forder_num is not null
and a.forder_system!=1
