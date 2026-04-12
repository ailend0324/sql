with t_sh_receive_product as(          -- 收货表(回收测)：条码重复，取最新收货记录
    select
        *
    from(
        select
            forder_id,
            Forder_sn,
            upper(Fproduct_code) as fseries_number,
            Flogistics_number as fexpress_number,
            fbrand,
            Funpack_user,
            Fcategory_name,
            Fadd_user as freceive_user,
            Fdepot_id as fwarehouse_number,
            from_unixtime(Funpack_time) as Funpack_time,
            from_unixtime(Fadd_time) as freceive_time,
            row_number() over(partition by Fproduct_code order by Fadd_time desc) as rownumber
        from drt.drt_my33310_hsb_wms_t_sh_receive_product
        where Fis_del <> 2
    ) t where rownumber = 1
),
huishou_jimai as(                              --回收转验机订单
    select 
        *
    from(
        select
            Fupdate_time,
            upper(Fseries_number) as Fseries_number,
            fcreate_user,
            row_number()over(partition by Fseries_number order by Fcreate_time desc) as num
        from drt.drt_my33310_detection_t_afresh_detect_apply 
        where Fcreate_user not in (select DISTINCT(freal_name) 
                                   from drt.drt_my33310_amcdb_t_user 
                                   where Fposition_id in(
	                                                     select Fposition_id 
	                                                     from drt.drt_my33310_amcdb_t_position 
	                                                     where Fname like "%检测%"))
    ) t where num=1
),
t_plus_print as(                           --闲鱼寄卖打印数据
    select 
        *
    from (select 
                upper(fseries_number) as fseries_number,
                fcreator as fplus_printer,
                fauto_update_time as fplus_print_time,
                row_number() over(partition by fseries_number order by fauto_update_time desc) as num
           from drt.drt_my33310_hsb_wms_t_plus_print_log 
    ) t 
    where num=1
),
t_parcel as(                        --验机条码明细
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
t_parcel_unpack as(                      --验机拆包
select 
    fparcel_id,
    Funpack_time,
    Funpack_user
from(
    select 
        fparcel_id,
        fadd_time as Funpack_time,
        fadd_user as Funpack_user,
        row_number() over(partition by fparcel_id  order by fadd_time desc) as num
    from drt.drt_my33310_xywms_t_parcel_log
    where ftype=2
) t where num=1
),
t_parcel_receive as(                      --验机收货
select 
    fparcel_id,
    freceive_time,
    freceive_user
from(
    select 
        fparcel_id,
        fadd_time as freceive_time,
        fadd_user as freceive_user,
        row_number() over(partition by fparcel_id  order by fadd_time desc) as num
    from drt.drt_my33310_xywms_t_parcel_log
    where ftype=3
) t where num=1
),
t_detect_photo as(          --拍照
    select
        fseries_number,
        fmain_photo_time,
        fphoto_name
    from(
        select
            upper(a.Fserial_number) as fseries_number,
            a.fphoto_time as fmain_photo_time,
            b.freal_name as fphoto_name,
            ROW_NUMBER() OVER (PARTITION BY a.Fserial_number ORDER BY a.fphoto_time desc) AS rownumber
        from drt.drt_my33310_detection_t_detect_photo as a
        left join drt.drt_my33310_amcdb_t_user as b on a.Fuser_name=b.fusername
        where a.fphoto_time is not null
    ) t where rownumber= 1
),
t_tamper as(             -- 防拆标
    select 
        upper(Fseries_number) as Fseries_number,
        Foperator,
        Fauto_update_time as Ftamper_time
    from drt.drt_my33310_hsb_wms_t_tamper
),
t_detect as(              --检测数据，取检测条码对应的品牌，类目
    select
        upper(Fserial_number) as Fserial_number,
        Fclass_name,
        Fbrand_name
    from (select 
                *,
                row_number() over(partition by Fserial_number order by Fcreate_time desc) as num
          from drt.drt_my33310_detection_t_detect_record
        ) t where num=1
),
t_ruku as (      --入库
select
    *
from (
select 
    *,
    row_number()over(partition by fserial_no order by fcreate_time desc) as num
from dwd.dwd_t_pm_wms_stock_notify 
where fcmd='CGRK'
and operator is not null)t
where num=1
),
allot as (
select 
    *
from (
    select 
        *,
        row_number()over(partition by fbar_code order by fadd_time desc) as num
    from drt.drt_my33310_xywms_t_product_allot
)t 
where num=1
),
t_instock as(              --回收测&验机入库流程数据
select 
        a.forder_id,
        a.fseries_number,
        a.fexpress_number,
        e.Fbrand_name as fbrand,
        e.Fclass_name as Fcategory_name,
        "回收" as ftype,
        a.Funpack_user,
        a.freceive_user,
        a.fwarehouse_number,
        a.Funpack_time,
        a.freceive_time,
        b.fmain_photo_time,
        b.fphoto_name,
        c.Foperator,
        c.Ftamper_time,
        d.Fupdate_time as flost_photo_time,
        d.fcreate_user as flost_photo_name,
        f.fplus_printer,
        f.fplus_print_time
from t_sh_receive_product as a
left join t_detect_photo as b on a.fseries_number=b.fseries_number
left join t_tamper as c on a.fseries_number=c.Fseries_number
left join huishou_jimai as d on a.fseries_number=d.Fseries_number
left join t_detect as e on a.fseries_number=e.Fserial_number
left join t_plus_print as f on a.fseries_number=f.Fseries_number
union all 
select 
    a.Forder_id,
    a.fseries_number,
    a.fexpress_number,
    e.Fbrand_name as fbrand,
    e.Fclass_name as Fcategory_name,
    "验机" as ftype,
    b.Funpack_user,
    c.freceive_user,
    null as fwarehouse_number,
    b.Funpack_time,
    c.freceive_time,
    d.fmain_photo_time,
    d.fphoto_name,
    null as Foperator,
    null as Ftamper_time,
    null as flost_photo_time,
    null as flost_photo_name,
    null as fplus_printer,
    null as fplus_print_time
from t_parcel as a
left join t_parcel_unpack as b on a.fid=b.fparcel_id
left join t_parcel_receive as c on a.fid=c.fparcel_id
left join t_detect_photo as d on a.fseries_number=d.fseries_number
left join t_detect as e on a.fseries_number=e.Fserial_number
)
select 
    to_date(freceive_time) as ftimeby,
    hour(freceive_time) as fhour,
    minute(freceive_time) as fminute,
    month(freceive_time) as fmonth,
    freceive_user as operator,
    "拆包&收货" as ftype,
    count(fseries_number) as num
from t_instock
where fseries_number is not null
and to_date(freceive_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and freceive_user is not null
group by 1,2,3,4,5,6
union all 
select 
    to_date(fmain_photo_time) as ftimeby,
    hour(fmain_photo_time) as fhour,
    minute(fmain_photo_time) as fminute,
    month(fmain_photo_time) as fmonth,
    fphoto_name as operator,
    "拍照" as ftype,
    count(fseries_number) as num
from t_instock
where fseries_number is not null
and to_date(fmain_photo_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and fphoto_name is not null
group by 1,2,3,4,5,6
union all
select 
    to_date(fcreate_time),
    hour(fcreate_time),
    minute(fcreate_time),
    month(fcreate_time) as fmonth,
    case when to_date(fcreate_time)='2023-10-16' and freal_name="刘俊" then "周利" 
    	 when (to_date(fcreate_time) BETWEEN '2023-11-01' AND '2023-11-21') and freal_name="郑佩文" then null else freal_name end as freal_name,
    "自动化检测" as ftype,
    count(fserial_number) as num
from (
select 
    a.fcreate_time,
    a.fserial_number,
    b.freal_name,
    row_number()over(partition by a.fserial_number order by a.fcreate_time desc) as num
from drt.drt_my33312_detection_t_automation_det_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_id=b.fuser_id
where a.fserial_number is not null and a.fserial_number!=""
and b.freal_name is not null
and to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180)))t
where num=1
group by 1,2,3,4,5,6
union all
select 
    to_date(fcreate_time),
    hour(fcreate_time),
    minute(fcreate_time),
    month(fcreate_time),
    operator,
    "入库" as ftype,
    count(fserial_no) as num
from t_ruku 
where to_date(fcreate_time)>='2023-11-10'
group by 1,2,3,4,5,6
union all
select 
    to_date(fadd_time),
    hour(fadd_time),
    minute(fadd_time),
    month(fadd_time),
    fupdate_user, 
    "验机上架" as ftype,
    count(fbar_code) as num
from allot
where to_date(fadd_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
group by 1,2,3,4,5,6