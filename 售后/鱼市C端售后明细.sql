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
            a.Fdetection_object,
            get_json_object(a.fgoods_level,'$.levelName') as fgoods_level,
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time asc) as num
        from drt.drt_my33310_detection_t_detect_record as a
        left join (select                            --取上拍时间节点                        
                        fseries_number,
                        fauto_update_time
                   from 
                        (select 
                            a.forder_id,
                            c.fseries_number,
                            a.fauto_update_time,
                            row_number() over(partition by a.forder_id order by a.fauto_update_time asc) as num
                        from drt.drt_my33310_recycle_t_order_txn as a
                        inner join drt.drt_my33310_recycle_t_xy_jimai_plus_order as b on a.forder_id=b.forder_id
                        left join drt.drt_my33310_recycle_t_order as c on a.forder_id=c.forder_id
                        where a.forder_status in (341,441)) t
                    where num=1) as b on a.fserial_number=b.fseries_number
        left join (
                    select 
                        freal_name,
                        Fposition_id
                    from (select 
                                *,
                                row_number() over(partition by freal_name order by fcreate_time desc) as num
                          from drt.drt_my33310_amcdb_t_user)t
                    where num=1) as c on a.freal_name=c.freal_name
        where a.fis_deleted=0 
        and (a.fend_time<b.fauto_update_time or b.fauto_update_time is null) 
      	and a.fend_time>=to_date(date_sub(from_unixtime(unix_timestamp()),900))
        and c.Fposition_id <>129            --剔除入库组缺陷拍照的人员
        --and fdetection_object<>3
            ) c 
    where c.num=1
),
detect_one as (
select 
    fserial_number,
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
and to_date(from_unixtime(a.fend_det_time))>=to_date(date_sub(from_unixtime(unix_timestamp()),800)))t
where num=1
),
detect_two as (
select 
    fserial_number,
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
where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),800))
and fserial_number!=""
and fserial_number is not null)t
where num=1
),
detect_three as (
select 
    fserial_number,
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
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),800))
    and b.fdet_sop_task_name like "%外观%")t
where num=1
),
detect_four as (
select 
    fserial_number,
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
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),800))
    and b.fdet_sop_task_name like "%拆修%")t
where num=1
)
select 
	a.fxy_order_id,
    a.fsales_order_num,
    a.forder_id,
    a.fseries_number,
    a.forder_status,
    a.forder_status_name,
    a.frecycle_type,
    a.fproduct_name,
    a.fclass_name,
    case when a.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
    a.forder_time,
    a.Fgetin_time,
    a.fcheck_end_time, 
    a.fsales_amount,
    a.frecycle_price,
    a.fauction_price,
    a.ffirst_deposit,
    a.fsecond_deposit,
    b.fauto_update_time as fshangpai_time,
    c.fauto_update_time as fpaichu_time,
    d.fauto_update_time as fbuyer_pay_time,
    a.fdetect_channel,
    a.fdetect_mode,
    a.Fdet_tpl,
    a.Freal_name,
    a.Fend_time,
    a.Fdetection_object,
    a.fshop_name,
    a.Fsupply_partner,
    a.fgoods_level,
    if(e.fdetect_two_name is null,a.freal_name,e.fdetect_two_name) as fdetect_two_name,
    if(f.fdetect_three_name is null,a.freal_name,f.fdetect_three_name) as fdetect_three_name,
    if(g.fdetect_four_name is not null,"是","否") as "是否分模块",
    a.fbuyer_name,
    a.fbuyer_phone
from 
    (select                              --取出闲鱼寄卖plus数据
     	cast(a.fxy_order_id as string) as fxy_order_id,
     	b.fsales_order_num,
        b.forder_id,
        d.forder_status,
        c.forder_status_name,
        d.fproduct_name,
        f.fname as fclass_name,
        g.fname as fbrand_name,
        case when d.frecycle_type=1 then "邮寄"
     	     when d.frecycle_type=2 then "上门"
             when d.frecycle_type=3 then "到店"
    	else null end as frecycle_type,
        d.forder_time,
        d.Fgetin_time,
        d.fcheck_end_time, 
        d.fseries_number,
        b.fsales_amount/100 as fsales_amount,
        b.frecycle_price/100 as frecycle_price,
        b.fauction_price/100 as fauction_price,
        b.ffirst_deposit/100 as ffirst_deposit,
        b.fsecond_deposit/100 as fsecond_deposit,
        case 
            when h.Fdet_tpl=1 then "大检测"
            when (h.Fdet_tpl=0 or h.Fdet_tpl=2 or h.Fdet_tpl=6 or h.Fdet_tpl=7) then "竞拍检测"
        else '其他' end as fdetect_channel,
        case 
            when h.Fdet_tpl = 0 then '标准检'
            when h.Fdet_tpl = 1 then '大质检'
            when h.Fdet_tpl = 2 then '新标准检测'
  	        when h.Fdet_tpl = 3 then '产线检'
            when h.Fdet_tpl = 4 then '34项检测'
            when h.Fdet_tpl = 5 then '无忧购'
            when h.Fdet_tpl = 6 then '寄卖plus'
            when h.Fdet_tpl = 7 then '价格3.0的检测'
        else '其他' end as fdetect_mode,
        h.Fdet_tpl,
        h.Freal_name,
        h.Fend_time,
        h.Fdetection_object,
     	h.fgoods_level,
     	i.fshop_name,
     	case when d.Fsupply_partner=2 then "小站(自营)" 
     		 when d.Fsupply_partner=3 then "小站(加盟)"
    	else null end as Fsupply_partner,
    	b.fbuyer_name,
    	b.fbuyer_phone
    from drt.drt_my33310_recycle_t_xy_jimai_plus_order as b 
    left join drt.drt_my33318_xy_bangmai_t_xybangmai_order_map as a on b.forder_id=a.forder_id
    left join drt.drt_my33310_recycle_t_order as d on b.forder_id=d.forder_id
    left join dws.dws_hs_order_detail as i on b.forder_id=i.forder_id
    left join drt.drt_my33310_recycle_t_order_status as c on d.forder_status=c.forder_status_id
    left join drt.drt_my33310_recycle_t_product as e on d.fproduct_id=e.fproduct_id
    left join drt.drt_my33310_recycle_t_pdt_class as f on e.fclass_id=f.fid
    left join drt.drt_my33310_recycle_t_pdt_brand as g on e.fbrand_id=g.fid
    left join detect as h on d.fseries_number=h.fserial_number
where d.forder_time>=to_date(date_sub(from_unixtime(unix_timestamp()),900))) as a
left join 
    (select                            --取上拍时间节点                        
        *
    from 
        (select 
            a.forder_id,
            a.fauto_update_time,
            row_number() over(partition by a.forder_id order by a.fauto_update_time desc) as num
        from drt.drt_my33310_recycle_t_order_txn as a
        inner join drt.drt_my33310_recycle_t_xy_jimai_plus_order as b on a.forder_id=b.forder_id
        where a.forder_status in (341,441)
        and a.fauto_update_time>=to_date(date_sub(from_unixtime(unix_timestamp()),900))) t
    where num=1
    ) as b on a.forder_id=b.forder_id
left join 
    (select                            --取拍出时间节点                        
        *
    from 
        (select 
            a.forder_id,
            a.fauto_update_time,
            row_number() over(partition by a.forder_id order by a.fauto_update_time desc) as num
        from drt.drt_my33310_recycle_t_order_txn as a
        inner join drt.drt_my33310_recycle_t_xy_jimai_plus_order as b on a.forder_id=b.forder_id
        where a.forder_status in (713,813)
        and a.fauto_update_time>=to_date(date_sub(from_unixtime(unix_timestamp()),900))) t
    where num=1
    ) as c on a.forder_id=c.forder_id
left join 
    (select                            --取买家付款时间节点                        
        *
    from 
        (select 
            a.forder_id,
            a.fauto_update_time,
            row_number() over(partition by a.forder_id order by a.fauto_update_time desc) as num
        from drt.drt_my33310_recycle_t_order_txn as a
        inner join drt.drt_my33310_recycle_t_xy_jimai_plus_order as b on a.forder_id=b.forder_id
        where a.forder_status in (714,814,261)
        and a.fauto_update_time>=to_date(date_sub(from_unixtime(unix_timestamp()),900))) t
    where num=1
    ) as d on a.forder_id=d.forder_id
left join detect_two as e on a.fseries_number=e.fserial_number
left join detect_three as f on a.fseries_number=f.fserial_number
left join detect_four as g on a.fseries_number=g.fserial_number
