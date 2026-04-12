with detect as (       --取最新检测明细数据，取检测人、检测模板
    select
        *
    from (
        select
            a.fcreate_time,
            upper(a.fserial_number) as fserial_number,
            a.Fdet_tpl,
            a.Freal_name,
            a.Fend_time,
      		a.fbrand_name,
            a.Fdetection_object,
            a.fgoods_level,
      		a.fwarehouse_code,
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time asc) as num
        from drt.drt_my33310_detection_t_detect_record as a
        left join (select
                        fseries_number,
                        forder_create_time
                   from (
                        select
                            fseries_number,
                            forder_create_time,
                        row_number() over(partition by fseries_number order by  forder_create_time desc) as num
                    from dws.dws_jp_order_detail
                    where ftest_show <> 1
                    and (fmerchant_jp=0 or fmerchant_jp is null)
                    and forder_status in (2,3,4,6)
                    and forder_create_time>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
                    ) t where t.num=1) as b on upper(a.fserial_number)=b.fseries_number
        left join (
                    select
                        freal_name,
                        Fposition_id
                    from (select
                                *,
                                row_number() over(partition by freal_name order by fcreate_time desc) as num
                          from drt.drt_my33310_amcdb_t_user
                          )t
                    where num=1) as c on a.freal_name=c.freal_name
        where a.fis_deleted=0
        and to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
        and a.fend_time<b.forder_create_time
        and c.Fposition_id <>129            --剔除入库组缺陷拍照的人员
        --and fdetection_object<>3
            ) c
    where c.num=1
),
after_sale_detect as (
select
        *
    from (
        select
            *,
            row_number() over(partition by fserial_number order by fend_time asc) as num
        from drt.drt_my33310_detection_t_detect_record
        where fdet_type=0
        and fis_deleted=0
      	and freport_type=0
    	and fverdict<>"测试单"
        and to_date(fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
        and left(fserial_number,2) in ('YZ','NT'))t
    where num=1
),
jp_sale as(
    select
        *
    from (
        select
            *,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and to_date(forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
        and forder_platform not in (6)  -- 修改：只排除采货侠，保留鱼市B2B(5)和自有(1)
        and (fmerchant_jp=0 or fmerchant_jp is null)
        and forder_status in (2,3,4,6)) t where num=1
    union all
    select
        *
    from (
        select
            *,
            "" as Fys_b2b_series_number
           , 0  as Fys_b2b_order_status
           , 0  as Fys_b2b_order_platform
           , 0  as Fys_b2b_foffer_price,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num
            
        from dws.dws_jp_order_detail_history2023
        where ftest_show <> 1
        and to_date(forder_create_time) between '2023-01-01' and to_date(date_sub(from_unixtime(unix_timestamp()),366))
        and forder_platform not in (6)  -- 修改：只排除采货侠，保留鱼市B2B(5)和自有(1)
        and (fmerchant_jp=0 or fmerchant_jp is null)
        and forder_status in (2,3,4,6)) t where num=1
),
jp_first_sale as (  --第一次销售
        select
        *
    from (
        select
            *,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and to_date(forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
        and forder_platform not in (6)  -- 修改：只排除采货侠
        and forder_status in (2,3,4,6)) t where num=1
    union all
    select
        *
    from (
        select
            *,
            "" as Fys_b2b_series_number
           , 0  as Fys_b2b_order_status
           , 0  as Fys_b2b_order_platform
           , 0  as Fys_b2b_foffer_price,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail_history2023
        where ftest_show <> 1
        and to_date(forder_create_time) between '2023-01-01' and to_date(date_sub(from_unixtime(unix_timestamp()),366))
        and forder_platform not in (6)  -- 修改：只排除采货侠
        and forder_status in (2,3,4,6)) t where num=1
),
jp_second_sale as (   --二次销售
        select
        *
    from (
        select
            if(b.fold_fseries_number is not null,b.fold_fseries_number,c.fold_fseries_number) as fold_fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            a.fstart_time
        from dws.dws_jp_order_detail as a
        left join dws.dws_hs_order_detail as b on a.fseries_number=b.fseries_number
        left join dws.dws_hs_order_detail_history2018_2022 as c on a.fseries_number=c.fseries_number
        where ftest_show <> 1
        and to_date(a.forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
        and a.fchannel_name='竞拍销售默认渠道号'
        and a.forder_status in (2,3,4,6)
        union all
        select
            if(b.fold_fseries_number is not null,b.fold_fseries_number,if(d.fold_fseries_number is not null,d.fold_fseries_number,c.fold_fseries_number)) as fold_fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            a.fstart_time
        from dws.dws_jp_order_detail_history2023 as a
        left join dws.dws_hs_order_detail as b on a.fseries_number=b.fseries_number
        left join dws.dws_hs_order_detail_history2018_2022 as c on a.fseries_number=c.fseries_number
        left join dws.dws_hs_order_detail_history2023 as d on a.fseries_number=d.fseries_number
        where ftest_show <> 1
        and to_date(a.forder_create_time) between '2023-01-01' and to_date(date_sub(from_unixtime(unix_timestamp()),366))
        and a.fchannel_name='竞拍销售默认渠道号'
        and a.forder_status in (2,3,4,6)
        union all
        select
            if(b.fold_fseries_number is not null,b.fold_fseries_number,c.fold_fseries_number) as fold_fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            a.fstart_time
        from dws.dws_jp_order_detail_history2020_2022 as a
        left join dws.dws_hs_order_detail as b on a.fseries_number=b.fseries_number
        left join dws.dws_hs_order_detail_history2018_2022 as c on a.fseries_number=c.fseries_number
        where ftest_show <> 1
        and to_date(a.forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
        and a.fchannel_name='竞拍销售默认渠道号'
        and a.forder_status in (2,3,4,6)
        union all
        select
            if(b.fold_fseries_number is not null,b.fold_fseries_number,c.fold_fseries_number) as fold_fseries_number,
            a.foffer_price,
            null as Fcity_name,
            null as Forder_address,
            null as Freceiver_id,
            null as Freceiver_name,
            null as Freceiver_phone,
            a.foffer_time as fstart_time
        from dws.dws_th_order_detail as a
        left join dws.dws_hs_order_detail as b on a.fseries_number=b.fseries_number
        left join dws.dws_hs_order_detail_history2018_2022 as c on a.fseries_number=c.fseries_number
        where a.Fbd_status <>2
        and a.fchannel_name='竞拍销售默认渠道号') t
),
after_sale as (
    select
        *
    from (
        select
            a.*,
            b.fseries_number,
            row_number() over(partition by fsales_series_number order by a.fauto_create_time desc ) as num
        from drt.drt_my33310_recycle_t_after_sales_order_info as a
        left join drt.drt_my33310_recycle_t_order as b on a.fafter_sales_order_id=b.forder_id
        where a.fvalid=1
        --and Faftersales_owner<>3
        ) t where num=1
),
-- 新增：鱼市售后数据CTE
yushi_after_sale as (
    select
        *
    from (
        select
            * ,
            row_number() over(partition by fbusiness_id order by fcreate_time desc)as num
        from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales
        where Fsource = 2  -- 鱼市B2B售后
    )t where num=1
),
caihuoxia_sale as (
    select
        *
    from (
        select
            *,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and forder_platform=6
        and fmerchant_jp=0
        and to_date(forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
        and forder_status in (2,3,4,6)) t where num=1
    union all
    select
        *
    from (
        select
            *,
                        "" as Fys_b2b_series_number
           , 0  as Fys_b2b_order_status
           , 0  as Fys_b2b_order_platform
           , 0  as Fys_b2b_foffer_price,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num
        from dws.dws_jp_order_detail_history2023
        where ftest_show <> 1
        and forder_platform=6
        and fmerchant_jp=0
        and to_date(forder_create_time) between '2023-01-01' and to_date(date_sub(from_unixtime(unix_timestamp()),366))
        and forder_status in (2,3,4,6)) t where num=1
),
caihuoxia_after_sale as (
select
    *
from (
    select
        * ,
        row_number() over(partition by fbusiness_id order by fcreate_time desc)as num
    from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales
    where Fsource = 1  -- 采货侠售后
)t where num=1
),
caihuoxia_first_sale as(
select
    *
from (
        select
            *,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and forder_platform=6
        and fmerchant_jp=0
        and to_date(forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
        and forder_status in (2,3,4,6)) t where num=1
union all
select
    *
from (
        select
            *,
                        "" as Fys_b2b_series_number
           , 0  as Fys_b2b_order_status
           , 0  as Fys_b2b_order_platform
           , 0  as Fys_b2b_foffer_price,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail_history2023
        where ftest_show <> 1
        and forder_platform=6
        and fmerchant_jp=0
        and to_date(forder_create_time) between '2023-01-01' and to_date(date_sub(from_unixtime(unix_timestamp()),366))
        and forder_status in (2,3,4,6)) t where num=1
),
caihuoxia_second_sale as (
select
    fstart_time,
    fseries_number,
    foffer_price/100 as foffer_price,
    Fcity_name,
    Forder_address,
    Freceiver_id,
    Freceiver_name,
    Freceiver_phone
from (
        select
            a.fstart_time,
            if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail as a
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no
        where ftest_show <> 1
        and to_date(a.forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
        and forder_platform=6
        and fmerchant_jp=0
        and forder_status in (2,3,4,6)) t where num=2
union all
select
    fstart_time,
    fseries_number,
    foffer_price/100 as foffer_price,
    Fcity_name,
    Forder_address,
    Freceiver_id,
    Freceiver_name,
    Freceiver_phone
from (
        select
            a.fstart_time,
            if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail_history2023 as a
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no
        where ftest_show <> 1
        and to_date(a.forder_create_time) between '2023-01-01' and to_date(date_sub(from_unixtime(unix_timestamp()),366))
        and forder_platform=6
        and fmerchant_jp=0
        and forder_status in (2,3,4,6)) t where num=2
union all
select
    fstart_time,
    fseries_number,
    foffer_price/100 as foffer_price,
    Fcity_name,
    Forder_address,
    Freceiver_id,
    Freceiver_name,
    Freceiver_phone
from (
        select
            a.fstart_time,
            if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail_history2020_2022 as a
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no
        where ftest_show <> 1
        and to_date(a.forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
        and forder_platform=6
        and fmerchant_jp=0
        and forder_status in (2,3,4,6)) t where num=2
union all
select
    fstart_time,
    fseries_number,
    foffer_price/100 as foffer_price,
    Fcity_name,
    Forder_address,
    Freceiver_id,
    Freceiver_name,
    Freceiver_phone
from (
        select
            a.fstart_time,
            if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail as a
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no
        where ftest_show <> 1
        and forder_platform<>6
        and to_date(a.forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
        and fmerchant_jp=0
        and forder_status in (2,3,4,6)) t where num=1
union all
select
    fstart_time,
    fseries_number,
    foffer_price/100 as foffer_price,
    Fcity_name,
    Forder_address,
    Freceiver_id,
    Freceiver_name,
    Freceiver_phone
from (
        select
            a.fstart_time,
            if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail_history2023 as a
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no
        where ftest_show <> 1
        and forder_platform<>6
        and to_date(a.forder_create_time) between '2023-01-01' and to_date(date_sub(from_unixtime(unix_timestamp()),366))
        and fmerchant_jp=0
        and forder_status in (2,3,4,6)) t where num=1
union all
select
    fstart_time,
    fseries_number,
    foffer_price/100 as foffer_price,
    Fcity_name,
    Forder_address,
    Freceiver_id,
    Freceiver_name,
    Freceiver_phone
from (
        select
            a.fstart_time,
            if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number,
            a.foffer_price,
            a.Fcity_name,
            a.Forder_address,
            a.Freceiver_id,
            a.Freceiver_name,
            a.Freceiver_phone,
            row_number() over(partition by fseries_number order by  forder_create_time asc) as num
        from dws.dws_jp_order_detail_history2020_2022 as a
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no
        where ftest_show <> 1
        and forder_platform<>6
        and to_date(a.forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
        and fmerchant_jp=0
        and forder_status in (2,3,4,6)) t where num=1
union all
select
    fstart_time,
    fseries_number,
    foffer_price/100 as foffer_price,
    null as Fcity_name,
    null as Forder_address,
    null as Freceiver_id,
    null as Freceiver_name,
    null as Freceiver_phone
from (
    select
        a.fstart_time,
        if(b.fsrouce_serial_no is not null,upper(b.fsrouce_serial_no),a.fseries_number) as fseries_number,
        a.foffer_price,
        row_number() over(partition by fseries_number order by forder_create_time asc) as num
    from dws.dws_th_order_detail as a
    left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as b on a.fseries_number=b.fserial_no
    where Fbd_status <>2
    )t where num=1
),
detect_one as (
select
    upper(fserial_number) as fserial_number,
    freal_name as fdetect_one_name,
    from_unixtime(fend_det_time) as fdetect_one_time
from (
select
    a.fserial_number,
  	a.fend_det_time,
    b.freal_name,
    row_number()over(partition by upper(a.fserial_number) order by a.fend_det_time desc) as num
from drt.drt_my33312_detection_t_automation_det_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
where fserial_number!=""
and fserial_number is not null
and to_date(from_unixtime(a.fend_det_time))>=to_date(date_sub(from_unixtime(unix_timestamp()),30)))t
where num=1
),
detect_two as (
select
    upper(fserial_number) as fserial_number,
    freal_name as fdetect_two_name,
    fcreate_time as fdetect_two_time
from (
select
    a.fcreate_time,
    a.fserial_number,
    b.freal_name,
    row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
from drt.drt_my33312_detection_t_det_app_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
and fserial_number!=""
and fserial_number is not null)t
where num=1
),
detect_three as (
select
    upper(fserial_number) as fserial_number,
    freal_name as fdetect_three_name,
    fcreate_time as fdetect_three_time
from (
    select
        a.fcreate_time,
        a.fserial_number,
        b.freal_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
    and b.fdet_sop_task_name like "%外观%")t
where num=1
),
detect_three_pingmu as (
select
    upper(fserial_number) as fserial_number,
    case when freal_name="李俊峰" then "李俊锋" else freal_name end as fdetect_three_name_pingmu,
    fcreate_time as fdetect_three_time_pingmu
from (
    select
        a.fcreate_time,
        a.fserial_number,
        b.freal_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
    and b.fdet_sop_task_name like "%屏幕%"
    and b.fdet_sop_task_name!="外观屏幕")t
where num=1
),
detect_four as (
select
    upper(fserial_number) as fserial_number,
    freal_name as fdetect_four_name,
    fcreate_time as fdetect_four_time
from (
    select
        a.fcreate_time,
        a.fserial_number,
        b.freal_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
    and b.fdet_sop_task_name like "%拆修%")t
where num=1
),
-- module_classification as (
    select 
        fsales_series_number,
        fserial_number as fnew_fseries_number,
        fissue_name,
        fanswer_name,
        case when fbrand_name='苹果' then "苹果" else "安卓" end as fbrand_name,
        case when fissue_name in ('保修情况','网络类型','机身内存','iCloud账号','iCloud账号:','存储容量','购买渠道','颜色','是否全新','制式','拆封情况','开机情况','开机情况:','型号','保修期') then "模块一"
             when fissue_name in ("有线充电",'有线充电:') and fbrand_name="苹果" then "模块一"
             when fissue_name in ('Y3声音','HOME键','Y3光线感应','无线功能（WIFI/蓝牙）','无线功能:','扬声器','Y3麦克风','Y3振动','振动','振动:','SIM 卡2','声音功能（麦克风/扬声器/听筒）','声音功能（麦克风/扬声器/听筒）:','Y3蓝牙','听筒','距离感应','重力感应','通话功能','通话功能:','Y3通信功能','静音键','Y3NFC','Y3Wi-Fi','侧键功能','侧键功能:','Y3原彩功能','Y3陀螺仪','屏幕传感器功能（光线/距离感应）','屏幕传感器功能（光线/距离感应）:','触摸','Face ID','Face ID:','闪光灯功能','蓝牙功能','Y3按键功能','Y3触摸功能','面部识别','Y3充电功能','触屏功能','触屏功能:','SIM 卡1','音量增键','NFC功能','NFC功能:','音量减键','指南针','指南针:','WIFI功能','底部麦克风','Y3距离感应','电源键','副屏-触摸功能','副屏-触摸功能:') then "模块二"
             when fissue_name in ("有线充电",'有线充电:') and fbrand_name!="苹果" then "模块二"
             when fissue_name in ('面容识别','面容识别:','Y3面容功能') and fbrand_name='苹果' then "模块二"
             when fissue_name in ('指纹识别','指纹解锁','指纹解锁:','Y3指纹功能') and fbrand_name="苹果" then "模块二"
             when fissue_name in ('外壳印渍','外壳划痕','其他显示问题','显示气泡','壳内掉漆','外壳缝隙','显示漏液','Y3屏幕显示','外壳磕碰','后摄像头外观','闪光灯外观','内屏掉漆','正面麦克风','前摄像头外观','屏幕显示','屏幕显示:','显示图像/文字印痕','光线感应','其他按键','屏幕外观','屏幕外观:','屏幕外观碎裂','指纹按键外观','屏幕外观/其他','边框背板','边框背板:','外壳破损','显示进灰','显示老化/色差','Y3屏下异物','显示色斑/压伤','Y3弯曲情况','机身弯曲','机身弯曲:','屏幕外观划痕','显示亮点/坏点','外壳弯曲变形','Y3机身外观','其他外壳外观','外壳脱胶','外壳掉漆','Y3外屏损伤','副屏-屏幕外观','副屏-屏幕外观:','折叠屏保护膜情况','折叠屏保护膜情况:','Y3折叠屏保护膜','转轴状况','转轴状况:','副屏-屏幕显示','副屏-屏幕显示:') then "模块三"
             when fissue_name in ('主板拆修情况','后置摄像头','后置摄像头:','无线充电','后壳维修','后壳维修:','账号','账号:','电池更换情况','电池维修情况:','数据接口','进水/受潮','进水/受潮:','软件检测','Y3账号','后摄像头维修情况','后摄像头维修情况:','Y3电池健康度','屏幕拆修情况','电池信息情况','拆修痕迹','Y3无线充电功能','无线充电功能:','Y3前置摄像头维修','耳机接口','售后案例情况','售后案例情况:','前置摄像头','前置摄像头:','Y3基带功能','Y3屏幕维修','Y3配件情况','Y3前置摄像头','ID 锁','Y3后置摄像头','Y3尾插','浸液痕迹','Y3售后案例','Y3系统使用','摄像头维修情况','Y3后置摄像头维修','机身拆修情况','尾插螺丝','音频网罩','USB联机','USB联机:','前置摄像头功能','屏幕维修情况','屏幕维修情况:','后置摄像头功能','前摄像头维修情况','前摄像头维修情况:','Y3零件维修情况','Y3进水情况','Y3主板维修','Y3定位功能','卡托','组件拆修情况','系统情况','Y3机身维修','主板维修情况','主板维修情况:','电池健康度','电池健康度:','是否可恢复出厂设置','是否可恢复出厂设置:','其他零部件情况','其他零部件情况:','副屏-维修情况','副屏-维修情况:','虹膜识别','Y3其他功能') then "模块四" 
             when fissue_name in ('面容识别','面容识别:','Y3面容功能') and fbrand_name!='苹果' then "模块四" 
             when fissue_name in ('指纹识别','指纹解锁','指纹解锁:','Y3指纹功能') and fbrand_name!="苹果" then "模块四" 
        else null end as ftype
    from (
        select 
            a.*,
            b.fserial_number,
            b.fbrand_name,
            case when left(b.fserial_number,2)='NT' and e.fbusiness_id is null then upper(f.fsrouce_serial_no)
                 when left(b.fserial_number,2)='NT' and e.fbusiness_id is not null then e.fbusiness_id 
                 else d.Fsales_series_number end as fsales_series_number,
            row_number()over(partition by b.fserial_number,a.fissue_name order by a.fdetect_record_id desc) as num
        from dwd.dwd_detect_back_detection_issue_and_answer_v2 as a
        left join dwd.dwd_detect_back_detect_detail as b on a.fdetect_record_id=b.fdetect_record_id
        left join drt.drt_my33310_recycle_t_order as c on b.fserial_number=c.fseries_number
        left join drt.drt_my33310_recycle_t_after_sales_order_info as d on c.forder_id=d.Fafter_sales_order_id
        left join drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales as e on b.fserial_number=e.fnew_serial_no
        left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as f on b.fserial_number=upper(f.fserial_no)
        where left(b.fserial_number,2) in ('NT','YZ')
        and a.field_source='fdet_norm_snapshot'
        and a.ds>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
    )t
    where num=1
    and fsales_series_number is not null
    and fanswer_name not like "%完好%"
    and fanswer_name not like "%正常%"
-- ),
-- module_summary as (
    select 
        fsales_series_number,
        sum(case when ftype='模块一' then 1 else 0 end) as module1_issues,
        sum(case when ftype='模块二' then 1 else 0 end) as module2_issues,
        sum(case when ftype='模块三' then 1 else 0 end) as module3_issues,
        sum(case when ftype='模块四' then 1 else 0 end) as module4_issues
    from module_classification
    group by fsales_series_number
-- )

-- 主查询1：自有平台和鱼市B2B数据（修改后）
select
    a.fstart_time,
    a.fseries_number,
    a.fclass_name,
    case when b.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
    a.fchannel_name,
    a.fproduct_name,
    a.fproject_name,
    a.Fcity_name,
    a.Forder_address,
    a.Freceiver_id,
    a.Freceiver_name,
    a.Freceiver_phone,
    f.freal_name as fsecond_detect_name,
    case when a.forder_platform = 1 then c.fseries_number
         when a.forder_platform = 5 then yushi_c.fnew_serial_no
    else null end as fafter_series_number,
    e.Fcity_name as fsecond_sale_city,
    e.Forder_address as fsecond_sale_address,
    e.Freceiver_id as fsecond_sale_id,
    e.Freceiver_name as fsecond_sale_name,
    e.Freceiver_phone as fsecond_sale_phone,
    -- 修改：动态判断销售渠道
    case when a.forder_platform = 1 then "自有平台"
         when a.forder_platform = 5 then "鱼市B2B"
         when a.forder_platform = 7 then "鱼市同售"
    else "其他平台" end as "销售渠道",
    left(a.fseries_number,2) as "渠道",
    a.fcost_price/100 as "成本价",
    a.foffer_price/100 as "当前出价",
    case when a.forder_platform = 1 and ((c.Ftotal_real_refund_amount>0 and a.foffer_price=c.Ftotal_real_refund_amount) or (a.fchannel_name="竞拍销售默认渠道号")) then 0
         when a.forder_platform = 5 and (yushi_c.fapply_time is not null and yushi_c.fapply_time !='0000-00-00 00:00:00.0' and yushi_c.Fjudge_type=1) then 0
    else a.foffer_price/100 end as "销售额",
    b.Fdet_tpl,
    b.Freal_name,
    b.Fend_time,
    b.Fdetection_object,
    case when b.fwarehouse_code='12' then "东莞仓"
    	 when right(left(a.fseries_number,6),2)="16" or left(a.fseries_number,3)="020" then "杭州仓"
    else "深圳仓" end as fwarehouse_code,
    case when a.forder_platform = 1 then c.Fauto_create_time
         when a.forder_platform = 5 then yushi_c.fapply_time
    else null end as Fauto_create_time,
    get_json_object(b.Fgoods_level,'$.levelName') as Fgoods_level,
    case when a.forder_platform = 1 then c.Fappeal_reason
         when a.forder_platform = 5 then yushi_c.Fjudge_reason
    else null end as Fappeal_reason,
    case when a.forder_platform = 1 then cast(c.Ffirst_trial_result as string)
         when a.forder_platform = 5 then yushi_c.Fjudge_result
    else null end as Ffirst_trial_result,
    case when a.forder_platform = 1 then c.Freexamine_result
    else null end as Freexamine_result,
    case when a.forder_platform = 1 then c.Fdetection_price/100
    else null end as "检测价",
    case when a.forder_platform = 1 then c.Freinspection_price/100
    else null end as "二次检测价",
    case when a.forder_platform = 1 then c.Ftotal_diff_amount/100
    else null end as "检测差异金额",
    case when a.forder_platform = 1 then c.Ftotal_refundable_amount/100
    else null end as "总应退款金额",
    case when a.forder_platform = 1 then c.Ftotal_real_refund_amount/100
         when a.forder_platform = 5 then yushi_c.Forder_deal_price/100
    else null end as "总实退款金额",
    case
        when b.Fdet_tpl=1 then "大检测"
        when (b.Fdet_tpl=0 or b.Fdet_tpl=2 or b.Fdet_tpl=6 or b.Fdet_tpl=7) then "竞拍检测"
    else '其他' end as "检测渠道",
    case
      when b.Fdet_tpl = 0 then '标准检'
      when b.Fdet_tpl = 1 then '大质检'
      when b.Fdet_tpl = 2 then '新标准检测'
  	  when b.Fdet_tpl = 3 then '产线检'
      when b.Fdet_tpl = 4 then '34项检测'
      when b.Fdet_tpl = 5 then '无忧购'
      when b.Fdet_tpl = 6 then '寄卖plus'
      when b.Fdet_tpl = 7 then '价格3.0的检测'
    else '其他' end as "检测模板",
    case when a.forder_platform = 1 and c.Ftotal_real_refund_amount>0 then 1
         when a.forder_platform = 5 and (yushi_c.fapply_time is not null and yushi_c.fapply_time !='0000-00-00 00:00:00.0' and yushi_c.Fjudge_type=1) then 1
    else 0 end as "售后数",
    case when a.forder_platform = 1 and c.Ftotal_real_refund_amount>0 then c.Freceived_audit_result_time
         when a.forder_platform = 5 and (yushi_c.fapply_time is not null and yushi_c.fapply_time !='0000-00-00 00:00:00.0' and yushi_c.Fjudge_type=1) then yushi_c.Fjudge_time
    else null end as "售后通过时间",
    case when a.forder_platform = 1 and c.Ftotal_real_refund_amount>0 and a.foffer_price<c.Ftotal_real_refund_amount and a.fstart_time>='2022-01-01' then 1
         when a.forder_platform = 1 and c.Ftotal_real_refund_amount>0 and a.foffer_price=c.Ftotal_real_refund_amount and a.fstart_time<'2022-01-01' then 1
         when a.forder_platform = 5 and (yushi_c.fapply_time is not null and yushi_c.fapply_time !='0000-00-00 00:00:00.0' and yushi_c.Fjudge_type=1) then 1
    else 0 end as "退货数",
    case when a.forder_platform = 1 and c.Ftotal_real_refund_amount>0 and a.foffer_price>c.Ftotal_real_refund_amount then 1
    else 0 end as "补差赔付",
    case when a.forder_platform = 1 and c.Ftotal_real_refund_amount>0 and a.foffer_price>c.Ftotal_real_refund_amount then c.Ftotal_real_refund_amount/100
    else 0 end as "赔付金额",
    case when a.forder_platform = 1 then c.Fafter_sales_type
    else null end as Fafter_sales_type,
    case when a.forder_platform = 1 and c.Fafter_sales_type=1 then "仅退款"
         when a.forder_platform = 1 and c.Fafter_sales_type=2 then "退货退款"
         when a.forder_platform = 5 and (yushi_c.fapply_time is not null and yushi_c.fapply_time !='0000-00-00 00:00:00.0' and yushi_c.Fjudge_type=1) then "退货退款"
    else "其它" end as "售后类型",
    case when a.forder_platform = 1 then c.Faftersales_owner
    else null end as Faftersales_owner,
    d.foffer_price/100 as first_price,
    e.fstart_time as fsecond_sale_time,
    e.foffer_price/100 as second_price,
    case when a.forder_platform = 1 and c.Ftotal_real_refund_amount>0 and a.foffer_price=c.Ftotal_real_refund_amount and a.fstart_time>='2022-01-01' then 0
         when a.forder_platform = 5 and (yushi_c.fapply_time is not null and yushi_c.fapply_time !='0000-00-00 00:00:00.0' and yushi_c.Fjudge_type=1) then d.foffer_price/100-e.foffer_price/100
    else d.foffer_price/100-e.foffer_price/100 end as "二次差价成本",
    if(g.fdetect_two_name is null,b.freal_name,g.fdetect_two_name) as fdetect_two_name,
    if(h.fdetect_three_name is null,b.freal_name,h.fdetect_three_name) as fdetect_three_name,
    j.fdetect_three_name_pingmu,
    if(g.fdetect_two_time is not null,"是","否") as "是否分模块",
    a.fanchor_level
from jp_sale as a
left join detect as b on a.fseries_number=b.fserial_number
left join after_sale as c on a.fseries_number=c.Fsales_series_number and a.forder_platform = 1  -- 只关联自有平台售后
left join yushi_after_sale as yushi_c on a.fseries_number=yushi_c.fbusiness_id and a.forder_platform = 5  -- 只关联鱼市售后
left join after_sale_detect as f on (c.fseries_number=f.fserial_number or yushi_c.fnew_serial_no=f.fserial_number)
left join jp_first_sale as d on a.fseries_number=d.fseries_number
left join jp_second_sale as e on a.fseries_number=e.fold_fseries_number
left join detect_two as g on a.fseries_number=g.fserial_number
left join detect_three as h on a.fseries_number=h.fserial_number
left join detect_three_pingmu as j on a.fseries_number=j.fserial_number
left join detect_four as i on a.fseries_number=i.fserial_number
-- left join module_summary as ms on a.fseries_number = ms.fsales_series_number  -- 关联模块统计
where a.fstart_time>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
-- 添加数据量限制，用于测试
limit 10000

union all

-- 主查询2：采货侠数据（保持不变）
select
    a.fstart_time,
    a.fseries_number,
    a.fclass_name,
    case when b.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
    a.fchannel_name,
    a.fproduct_name,
    a.fproject_name,
    a.Fcity_name,
    a.Forder_address,
    a.Freceiver_id,
    a.Freceiver_name,
    a.Freceiver_phone,
    g.freal_name as fsecond_detect_name,
    c.fnew_serial_no as fafter_series_number,
    e.Fcity_name as fsecond_sale_city,
    e.Forder_address as fsecond_sale_address,
    e.Freceiver_id as fsecond_sale_id,
    e.Freceiver_name as fsecond_sale_name,
    e.Freceiver_phone as fsecond_sale_phone,
    "采货侠" as "销售渠道",
    left(a.fseries_number,2) as "渠道",
    a.fcost_price/100 as "成本价",
    a.foffer_price/100 as "当前出价",
    if(c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1,0,a.foffer_price/100) as "销售额",
    b.Fdet_tpl,
    b.Freal_name,
    b.Fend_time,
    b.Fdetection_object,
    case when b.fwarehouse_code='12' then "东莞仓"
    	 when right(left(a.fseries_number,6),2)="16" or left(a.fseries_number,3)="020" then "杭州仓"
    else "深圳仓" end as fwarehouse_code,
    c.fapply_time,
    get_json_object(b.Fgoods_level,'$.levelName') as Fgoods_level,
    c.Fjudge_reason as Fappeal_reason,
    c.Fjudge_result as Ffirst_trial_result,
    null as Freexamine_result,
    null as "检测价",
    null as "二次检测价",
    null as "检测差异金额",
    null as "总应退款金额",
    c.Forder_deal_price/100 as "总实退款金额",
    case
        when b.Fdet_tpl=1 then "大检测"
        when (b.Fdet_tpl=0 or b.Fdet_tpl=2 or b.Fdet_tpl=6 or b.Fdet_tpl=7) then "竞拍检测"
    else '其他' end as "检测渠道",
    case
      when b.Fdet_tpl = 0 then '标准检'
      when b.Fdet_tpl = 1 then '大质检'
      when b.Fdet_tpl = 2 then '新标准检测'
  	  when b.Fdet_tpl = 3 then '产线检'
      when b.Fdet_tpl = 4 then '34项检测'
      when b.Fdet_tpl = 5 then '无忧购'
      when b.Fdet_tpl = 6 then '寄卖plus'
      when b.Fdet_tpl = 7 then '价格3.0的检测'
    else '其他' end as "检测模板",
    case when c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1 then 1
         when f.fsrouce_serial_no is not null then 1 else 0 end as "售后数",
    case when c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1 then Fjudge_time
         when f.fsrouce_serial_no is not null then Fjudge_time else null end as "售后通过时间",
    case when c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1 then 1
         when f.fsrouce_serial_no is not null then 1 else 0 end as "退货数",
    0 as "补差赔付",
    0 as "赔付金额",
    null as Fafter_sales_type,
    case when c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1 then "退货退款"
         when f.fsrouce_serial_no is not null then "退货退款" else null end as "售后类型",
    null as Faftersales_owner,
    d.foffer_price/100 as first_price,
    e.fstart_time as fsecond_sale_time,
    e.foffer_price as second_price,
    if(c.fapply_time is not null and c.fapply_time !='0000-00-00 00:00:00.0' and c.Fjudge_type=1,d.foffer_price/100-e.foffer_price,if(f.fsrouce_serial_no is not null,d.foffer_price/100-e.foffer_price,0)) as "二次差价成本",
    if(h.fdetect_two_name is null,b.freal_name,h.fdetect_two_name) as fdetect_two_name,
    if(i.fdetect_three_name is null,b.freal_name,i.fdetect_three_name) as fdetect_three_name,
    k.fdetect_three_name_pingmu,
    if(h.fdetect_two_time is not null,"是","否") as "是否分模块",
    a.fanchor_level
from caihuoxia_sale as a
left join detect as b on a.fseries_number=b.fserial_number
left join caihuoxia_after_sale as c on a.fseries_number=c.fbusiness_id
left join after_sale_detect as g on c.fnew_serial_no=g.fserial_number
left join caihuoxia_first_sale as d on a.fseries_number=d.fseries_number
left join caihuoxia_second_sale as e on a.fseries_number=e.fseries_number
left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as f on a.fseries_number=upper(f.fsrouce_serial_no)
left join detect_two as h on a.fseries_number=h.fserial_number
left join detect_three as i on a.fseries_number=i.fserial_number
left join detect_three_pingmu as k on a.fseries_number=k.fserial_number
left join detect_four as j on a.fseries_number=j.fserial_number
-- left join module_summary as ms on a.fseries_number = ms.fsales_series_number  -- 关联模块统计
where a.fstart_time>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
-- 添加数据量限制，用于测试
limit 10000