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
                    and forder_create_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
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
        and to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365)) 
        and a.fend_time<b.forder_create_time
        and c.Fposition_id <>129            --剔除入库组缺陷拍照的人员
        --and fdetection_object<>3
            ) c 
    where c.num=1
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
        and forder_platform=5
        and forder_status in (2,3,4,6)) t where num=1
)
,detect_two as (
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
where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
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
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
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
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
    and b.fdet_sop_task_name like "%屏幕%"
    and b.fdet_sop_task_name!="外观屏幕")t
where num=1
)
select 
    a.fstart_time,
    a.fpay_time,
    a.fseries_number,
    a.fclass_name,
    a.fchannel_name,
    a.fproduct_name, 
    a.fproject_name,
    a.fshop_name,
    case when d.fseries_number is not null then "阿里回流" else "其它渠道" end as "业务渠道",
    case when c.frecycle_type=1 then "邮寄"
         when c.frecycle_type=2 then "上门"
         when c.frecycle_type=3 then "到店"
    else null end as "回收类型",
    case when Fmerchant_jp=0 then "否" else "是" end as "是否异地上拍",
    left(a.fseries_number,2) as "渠道",
    a.fcost_price/100 as "成本价", 
    a.foffer_price/100 as "当前出价", 
    b.Fdet_tpl,
    b.Freal_name,
    b.Fend_time,
    b.Fdetection_object,
    get_json_object(b.Fgoods_level,'$.levelName') as Fgoods_level,
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
	if(g.fdetect_two_name is null,b.freal_name,g.fdetect_two_name) as fdetect_two_name,
    if(h.fdetect_three_name is null,b.freal_name,h.fdetect_three_name) as fdetect_three_name,
    j.fdetect_three_name_pingmu,
    if(g.fdetect_two_time is not null,"是","否") as "是否分模块",
	case when b.fwarehouse_code='12' then "东莞仓" 
    	 when right(left(a.fseries_number,6),2)="16" or left(a.fseries_number,3)="020" then "杭州仓"
    else "深圳仓" end as fwarehouse_code,
    case when b.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
	case when cc.frefund_total>0 then 1 else 0 end as "售后数",
	case when cc.frefund_total>0 and a.foffer_price>cc.frefund_total then cc.frefund_total/100 else 0 end as "赔付金额",
	cc.fjudge_reason as fapply_desc,
	case cc.faftersales_type
        when 1 then '仅退款'
        when 2 then '退货退款'
    end as faftersales_type
	
from jp_sale as a
left join detect as b on a.fseries_number=b.fserial_number
left join dws.dws_hs_order_detail as c on a.fseries_number=c.fseries_number
left join dws.dws_hs_order_detail_al as d on a.fseries_number=d.fseries_number
left join detect_two as g on a.fseries_number=g.fserial_number
left join detect_three as h on a.fseries_number=h.fserial_number
left join detect_three_pingmu as j on a.fseries_number=j.fserial_number
left join drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales as  cc on a.fseries_number=cc.fbusiness_id
where a.fstart_time>=to_date(date_sub(from_unixtime(unix_timestamp()),720))





