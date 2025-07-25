--合作项目邮寄客服议价看板(动态)
with 
huishouyijia as(
    select 
        a.forder_id,
        a.funit_price, 
        a.fdetection_price, 
        a.fdetect_price, 
        a.freal_detect_price,
        a.fbargain_price, 
        a.fpay_out_price, 
        a.foperation_price, 
        a.forder_status, 
        a.frecycle_type, 
        a.fclass_name, 
        a.fproduct_name,
        a.fbargain_time,
        a.fbrand_name, 
        a.fseries_number,
        if(a.flast_bargain_name is not null,a.flast_bargain_name,a.Fbargain_submitter) as operator_name,
        a.freturn_begin_time,
        a.forder_create_time, 
  		a.fgetin_time,
        a.fdetect_time,
        a.fdetect_push_time,
        a.fwait_pay_time,
        a.fadd_price,
        a.forder_cancel_time,
        a.fpay_time, 
        a.fproject_name,
        a.fchannel_name, 
  		a.freturn_finish_time,
        a.fdet_tpl, 
        a.fbargin_time, 
        a.fxy_channel, 
        a.fsub_channel, 
        b.fhsb_product_name, 
        a.Fis_exempt,
        a.fbusiness_attribute_name,
        a.fbargain_qty as call_num,
        a.freturn_retention_time, 
        if(a.fbargin_time is not null,a.fbargin_time,a.flast_bargain_time) as bargin_new_time,
        if(a.fbargin_time is not null and a.fbargain_time is not null and unix_timestamp(a.fbargin_time,'yyyy-MM-dd HH:mm:ss')<=unix_timestamp(a.fbargain_time,'yyyy-MM-dd HH:mm:ss'),a.fbargin_time,if(a.fbargain_time is null,a.fbargin_time,a.fbargain_time)) as bargin_first_time,
  		a.fsender_phone,
  		a.forder_status_name
    from dws.dws_hs_order_detail as a
    inner join dws.dws_hs_order_detail_al as b on a.forder_id=b.forder_id
    where a.ftest=0
    and a.frecycle_type=1
    and a.fgetin_time is not null
),
lanjie as(
    select                           --拦截报告自生成工单
        forder_id,
        fadd_time
    from
        (select 
            a.forder_id,
            from_unixtime(a.fadd_time)as fadd_time,
            row_number() over(partition by a.forder_id order by from_unixtime(a.fadd_time) desc) as num
        from drt.drt_my33310_csrdb_t_works as a
        left join drt.drt_my33310_csrdb_t_works_config_appeal as d on a.fappeal_type2=d.fid
        where from_unixtime(a.Fadd_time) >='2020-01-01'
        and a.fwork_type in(1,2,3,4)
        and a.fwork_source=3
        and a.fappeal_type1<>0
        and a.fduty_content not like "%无效工单%"
        and d.fcontent='已检测状态挽留')t
    where num=1
),
gongdan as (
    select 
        *
    from (
    select 
        a.Forder_id,
        from_unixtime(a.fadd_time) as fadd_time,
        b.ffeedback_str, 
        row_number() over(partition by a.forder_id order by a.fadd_time desc) as num
    from drt.drt_my33310_csrdb_t_works as a 
    left join drt.drt_my33310_csrdb_t_work_device_pwd_consulting as b on a.fid=b.fwork_id
    where a.Fwork_type in(1,2,3,4)
    and a.fwork_source<>3
    and a.fappeal_type1<>0
    and a.fduty_content<>"无效工单"
    and a.fupdate_user in(SELECT 
                        distinct(freal_name) 
                    from drt.drt_my33310_amcdb_t_user 
                    where (Fdepartment_id in(61) or Freal_name in("曾丽婷"))
                    and Fis_lock=1))t 
    where num=1
),
gongdan_buchang as (
select 
    *
from (
    select
        a.fbarcode_sn, 
        from_unixtime(a.fadd_time),
        c.fmoney,
        row_number() over(partition by a.fbarcode_sn order by a.fadd_time desc) as num
    from drt.drt_my33310_csrdb_t_works as a
    left join drt.drt_my33310_csrdb_t_works_config_appeal as b on a.fappeal_type2=b.fid
    left join drt.drt_my33310_csrdb_t_works_compensation as c on a.fid=c.fwork_id
    where from_unixtime(a.Fadd_time) >='2020-01-01'
    and a.fwork_type in(1,2,3,4)
    and a.fappeal_type1<>0
    and b.fcontent like "%议价%"
    and a.fduty_content not like "%无效工单%"
)t 
where num=1
),
sales as (
select 
    *
from (
    select 
        *,
        row_number()over(partition by fseries_number order by fstart_time desc)as num
    from(
        select 
            fstart_time,
            if(Fchannel_name="竞拍销售默认渠道号",fold_series_number, fseries_number) as fseries_number,
            fcost_price/100 as fcost_price,
            Foffer_price/100 as Foffer_price
        from dws.dws_jp_order_detail 
        where Forder_status in (2,3,4,6)
        and Fpay_time IS NOT NULL
        union all
        select 
            fstart_time,
            if(a.Fchannel_name="竞拍销售默认渠道号",b.fold_fseries_number,a.fseries_number) as fseries_number,
            fcost_price/100 as fcost_price,
            a.Foffer_price/100 as Foffer_price
        from dws.dws_th_order_detail as a
        left join dws.dws_hs_order_detail as b on a.fseries_number=b.fseries_number
        where a.Forderoffer_status = 10
        and a.Fpay_time IS NOT NULL)a)t where num=1
)
select
    to_date(fgetin_time) as fgetin_time,
    to_date(fdetect_time) as detect_time,
    --Fbargin_time,
    --fbargain_time,
    bargin_new_time,
    fsender_phone,
    bargin_first_time,
    Freturn_retention_time,
    fdetect_push_time,
    a.freturn_finish_time,
    b.fadd_time,
    d.fadd_time as lanjie_time,
    a.forder_id,
    a.fseries_number,
    a.fhsb_product_name,
    a.Fproduct_name,
    fbrand_name,
    a.forder_status_name,
    case when fclass_name in ('单反/微单套机','单反/微单相机','单反套机','单反相机','微单相机','拍立得','摄影机','数码相机','相机镜头','运动相机') then "相机/摄像机"
        when fclass_name in ('CPU','固态硬盘','电脑主板','显示器','内存条','显卡','电脑内存','电脑固态硬盘','电脑显卡') then "电脑硬件及周边"
    else fclass_name end as fclass_name, 
    fxy_channel,
    fsub_channel,
    operator_name,
    e.fcost_price,
    e.foffer_price,
    case
        when fchannel_name="支付宝小程序" then "支付宝小程序"
         when fxy_channel in('tmall-service','tm_recycle' ,'rm_recycle') then "天猫"
    else "闲鱼" end as "渠道",
    case when funit_price/100=0.01 then "跳过估价"
    	 when funit_price/100<=200 then "0-200"
         when funit_price/100>200 and funit_price/100<=500 then "200-500"
         when funit_price/100>500 and funit_price/100<=1000 then "500-1000"
         when funit_price/100>1000 and funit_price/100<=2000 then "1000-2000"
         when funit_price/100>2000 then "2000以上"
    else null end as "预估价区间",
    funit_price/100 as funit_price,
    fbargain_price/100 as bargain_price,
    fadd_price/100 as fadd_price,
    fdetect_price/100 as fdetect_price, 
    freal_detect_price/100 as freal_detect_price,
    foperation_price/100 as foperation_price,
    fpay_out_price/100 as fpay_out_price,
    if(fpay_out_price is not null,c.fmoney,0) as fmoney,
    1 as "收货数",
    case when a.fdetect_time is not null then 1 else 0 end as "检测数",
    case when a.fdetect_time is null and (a.forder_status_name="已退货" or a.forder_status_name="已取消") and b.ffeedback_str not like "%环保%" then 1 else 0 end as "检测前退货数",
    case when a.fdetect_time is null and (a.forder_status_name="已退货" or a.forder_status_name="已取消") and b.ffeedback_str like "%环保%" then 1 else 0 end as "检测前环保回收数",
    case when a.fdetect_time is not null and a.funit_price<=a.freal_detect_price and a.funit_price/100!=0.01 and Fis_exempt!=1 and a.fhsb_product_name=a.Fproduct_name then 1 else 0 end as "价格一致成交量",
    case when a.fdetect_time is not null and Fis_exempt=1 and a.funit_price<=foperation_price and a.funit_price/100!=0.01 and a.fhsb_product_name=a.Fproduct_name then 1 else 0 end as "豁免成交量",
    case
        when a.fdetect_time is not null and foperation_price is not null and foperation_price<funit_price then 1
        when a.fdetect_time is not null and foperation_price is null and fdetection_price is not null and fdetection_price<funit_price then 1
        when a.fdetect_time is not null and funit_price/100=0.01 then 1
        when a.fdetect_time is not null and a.fhsb_product_name!=a.Fproduct_name then 1
    else 0 end as "需议价量",
    case 
        when foperation_price is not null and foperation_price<funit_price and bargin_new_time is not null and freturn_retention_time is not null then 1
        when foperation_price is null and fdetection_price is not null and fdetection_price<funit_price and bargin_new_time is not null and freturn_retention_time is not null then 1
        when funit_price/100=0.01 and bargin_new_time is not null and freturn_retention_time is not null then 1
        when a.fhsb_product_name!=a.Fproduct_name and bargin_new_time is not null and freturn_retention_time is not null then 1
    else 0 end as "退货需挽留数",
    case 
        when foperation_price is not null and foperation_price<funit_price and bargin_new_time is not null and freturn_retention_time is not null and (fwait_pay_time is not null or fpay_time is not null) then 1
        when foperation_price is null and fdetection_price is not null and fdetection_price<funit_price and bargin_new_time is not null and freturn_retention_time is not null and (fwait_pay_time is not null or fpay_time is not null) then 1
        when funit_price/100=0.01 and bargin_new_time is not null and freturn_retention_time is not null and (fwait_pay_time is not null or fpay_time is not null) then 1
        when a.fhsb_product_name!=a.Fproduct_name and bargin_new_time is not null and freturn_retention_time is not null and (fwait_pay_time is not null or fpay_time is not null) then 1
    else 0 end as "退货挽留成功数",
    case 
        when foperation_price is not null and foperation_price<funit_price and bargin_new_time is not null and (freturn_retention_time is null or freturn_retention_time>bargin_first_time) then 1
        when foperation_price is null and fdetection_price is not null and fdetection_price<funit_price and bargin_new_time is not null and (freturn_retention_time is null or freturn_retention_time>bargin_first_time) is null then 1
        when funit_price/100=0.01 and bargin_new_time is not null and (freturn_retention_time is null or freturn_retention_time>bargin_first_time) then 1
        when a.fhsb_product_name!=a.Fproduct_name and bargin_new_time is not null and (freturn_retention_time is null or freturn_retention_time>bargin_first_time) then 1
    else 0 end as "人工主动议价数",
    case 
        when foperation_price is not null and foperation_price<funit_price and bargin_new_time is not null and freturn_retention_time is null and (fwait_pay_time is not null or fpay_time is not null) then 1
        when foperation_price is null and fdetection_price is not null and fdetection_price<funit_price and bargin_new_time is not null and freturn_retention_time is null and (fwait_pay_time is not null or fpay_time is not null) then 1
        when funit_price/100=0.01 and bargin_new_time is not null and freturn_retention_time is null and (fwait_pay_time is not null or fpay_time is not null) then 1
        when a.fhsb_product_name!=a.Fproduct_name and bargin_new_time is not null and freturn_retention_time is null and (fwait_pay_time is not null or fpay_time is not null) then 1
    else 0 end as "人工主动议价成功数",
    case 
        when foperation_price is not null and foperation_price<funit_price and bargin_new_time is not null and (fwait_pay_time is not null or fpay_time is not null)  then 1
        when foperation_price is null and fdetection_price is not null and fdetection_price<funit_price and bargin_new_time is not  null and (fwait_pay_time is not null or fpay_time is not null) then 1
        when funit_price/100=0.01 and bargin_new_time is not  null and (fwait_pay_time is not null or fpay_time is not null) then 1
        when a.fhsb_product_name!=a.Fproduct_name and bargin_new_time is not  null and (fwait_pay_time is not null or fpay_time is not null) then 1
    else 0 end as "人工议价成功量",
    case 
        when foperation_price is not null and foperation_price<funit_price and bargin_new_time is not null then fbargain_price 
        when foperation_price is null and fdetection_price is not null and fdetection_price<funit_price and bargin_new_time is not null then fbargain_price
        when funit_price/100=0.01 and bargin_new_time is not null then fbargain_price
        when a.fhsb_product_name!=a.Fproduct_name and bargin_new_time is not null then fbargain_price
    else 0 end/100 as "总加价金额",
    case when a.forder_status_name="已付款" or a.forder_status_name="待付款" then 1 else 0 end as "成交数"
from huishouyijia as a
left join gongdan as b on a.forder_id=b.forder_id
left join gongdan_buchang as c on a.fseries_number=c.fbarcode_sn
left join lanjie as d on a.forder_id=d.forder_id
left join sales as e on a.fseries_number=e.fseries_number
where to_date(a.fgetin_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),366))
order by a.fgetin_time desc


