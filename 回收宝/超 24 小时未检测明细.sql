select 
    a.fseries_number,
    a.fgetin_time as "收货时间",
    from_unixtime(b.fadd_time) as "问密时间",
    e.fname,
    a.fproduct_name,
    a.forder_status,
    c.foperator_name,
    c.fremark,
    (unix_timestamp(from_timestamp(now(),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(a.fgetin_time,'yyyy-MM-dd HH:mm:ss'))/3600/24 as fchaoshi
from drt.drt_my33310_recycle_t_order as a
left join 
    (select 
        forder_id,
        max(fadd_time) as fadd_time
    from drt.drt_my33310_csrdb_t_works
    where from_unixtime(fadd_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),10))
    and Fwork_type=4    
    group by 1) as b on a.forder_id=b.forder_id
left join (
    select 
        *
    from (
    select 
        forder_id,
        fremark,
        foperator_name,
        row_number()over(partition by forder_id order by fcreate_time desc) as num
    from drt.drt_my33310_recycle_t_order_remark
    where fcreate_time>=to_date(date_sub(from_unixtime(unix_timestamp()),10))
    )t
    where num=1
) as c on c.forder_id=a.forder_id
left join drt.drt_my33310_recycle_t_product as d on a.fproduct_id=d.fproduct_id
left join drt.drt_my33310_recycle_t_pdt_class as e on d.fclass_id=e.fid
where to_date(a.fgetin_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),10))
and a.ftest=0 
and a.Fvalid=1
and a.fcheck_end_time is null 
and ((unix_timestamp(from_timestamp(now(),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(a.fgetin_time,'yyyy-MM-dd HH:mm:ss'))/3600/24)>=0.5
and left(a.fseries_number,2) not in ('XZ','JM','YZ','CG','BB')
and a.forder_status in (40)
union all
select 
    a.fhost_barcode,
    a.fupdate_status_time,
    from_unixtime(b.fadd_time) as "问密时间",
    "手机" as fname,
    null as fproduct_name,
    a.forder_status,
    null as foperator_name,
    null as fremark,
    ((unix_timestamp(from_timestamp(now(),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(a.fupdate_status_time,'yyyy-MM-dd HH:mm:ss'))/3600/24) as fchaoshi
from drt.drt_my33315_xy_detect_t_xy_hsb_order as a 
left join 
    (select 
        fbarcode_sn,
        max(fadd_time) as fadd_time
    from drt.drt_my33310_csrdb_t_works
    where from_unixtime(fadd_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),10))
    and Fwork_type=4    
    group by 1) as b on a.fhost_barcode=b.fbarcode_sn
where a.forder_status=20
and a.forder_time>=to_date(date_sub(from_unixtime(unix_timestamp()),10))
and ((unix_timestamp(from_timestamp(now(),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(a.fupdate_status_time,'yyyy-MM-dd HH:mm:ss'))/3600/24)>=0.5
