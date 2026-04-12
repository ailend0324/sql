--邮寄议价-成交看板(动态)
with yijia_name as (
    select
        forder_id,
        fuser_name as fyijia_name,
        --第一次介入议价人员
        fremark
    from
        (
            select
                a.forder_id,
                case
                    when a.Foperator_id = 2707 then "吴超勇"
                    when a.Foperator_id = 2699 then "唐云云"
                    when a.Foperator_id = 2705 then "覃兴璜"
                    else b.fuser_name
                end as fuser_name,
                --议价人员名字
                a.fcreate_time,
                a.fremark,
                row_number() over(
                    partition by a.forder_id
                    order by
                        a.fcreate_time desc
                ) as num
            from
                drt.drt_my33310_recycle_t_order_bargain_remark as a
                left join drt.drt_my33310_csrdb_t_kefu as b on a.Foperator_id = b.fuser_id
            where
                (
                    (
                        b.fdepartment_id like "%45%"
                        and b.fid not in (136, 107,171,174,173)
                        and b.Fstatus = 1
                    )
                    or a.Foperator_id in (2707, 2699, 2705)
                ) --剔除非议价人员
                and to_date(a.fcreate_time) >= to_date(date_sub(from_unixtime(unix_timestamp()), 365))
        ) t
    where
        num = 1
       --   AND fuser_name not in('杨杰','王曼玉','何贵英')
),
huishouyijia as(
    select
        *
    from
        (
            select
                *,
                row_number() over(
                    partition by forder_id
                    order by
                        forder_create_time
                ) as num
            from
                (
                    select
                        a.forder_id,
                        a.funit_price,
                        a.fdetection_price,
                        a.fdetect_price,
                        a.fexempt_price,
                        a.fbargain_price,
                        a.fpay_out_price,
                        a.foperation_price,
                        a.forder_status,
                        a.forder_status_name,
                        a.frecycle_type,
                        a.fclass_name,
                        a.fproduct_name,
                        a.fbargain_time,
                        a.fbrand_name,
                        a.fseries_number,
                        a.freturn_begin_time,
                        a.forder_create_time,
                        a.fdetect_time,
                        a.fsender_phone,
                        a.fdetect_push_time,
                        a.fwait_pay_time,
                        a.fadd_price,
                        a.forder_cancel_time,
                        b.fcancel_time,
                        b.frequire_back_time,
                        a.fsend_back_time,
                        a.fpay_time,
                        a.fproject_name,
                        a.fchannel_name,
                        a.fdet_tpl,
                        if(
                            a.flast_bargain_time is not null,
                            a.flast_bargain_time,
                            a.fbargin_time
                        ) as flast_bargain_time,
                        a.freturn_retention_time,
                        a.fxy_channel,
                        a.fsub_channel,
                        a.fbusiness_attribute_name,
                        if(
                            c.fyijia_name is not null,
                            c.fyijia_name,
                            a.Fbargain_submitter
                        ) as fbargin_name,
                        a.fcheck_item_group_level,
                        c.fremark
                    from
                        dws.dws_hs_order_detail as a
                        left join dws.dws_hs_order_detail_al as b on a.forder_id = b.forder_id
                        left join yijia_name as c on a.forder_id = c.forder_id
                    where
                        a.ftest = 0
                        and a.frecycle_type = 1
                        and a.fdetect_time is not null
                        and left(a.fseries_number, 2) not in ("BM", "CG")
                       -- AND  a.Fbargain_submitter not in('杨杰','王曼玉','何贵英')
                        and to_date(a.forder_create_time) >= to_date(date_sub(from_unixtime(unix_timestamp()), 365))
                    union
                    all
                    select
                        a.forder_id,
                        a.funit_price,
                        a.fdetection_price,
                        a.fdetect_price,
                        a.fexempt_price,
                        a.fbargain_price,
                        a.fpay_out_price,
                        a.foperation_price,
                        a.forder_status,
                        a.forder_status_name,
                        a.frecycle_type,
                        a.fclass_name,
                        a.fproduct_name,
                        a.fbargain_time,
                        a.fbrand_name,
                        a.fseries_number,
                        a.freturn_begin_time,
                        a.forder_create_time,
                        a.fdetect_time,
                        a.fsender_phone,
                        a.fdetect_push_time,
                        a.fwait_pay_time,
                        a.fadd_price,
                        a.forder_cancel_time,
                        b.fcancel_time,
                        b.frequire_back_time,
                        a.fsend_back_time,
                        a.fpay_time,
                        a.fproject_name,
                        a.fchannel_name,
                        a.fdet_tpl,
                        if(
                            a.flast_bargain_time is not null,
                            a.flast_bargain_time,
                            a.fbargin_time
                        ) as flast_bargain_time,
                        a.freturn_retention_time,
                        a.fxy_channel,
                        a.fsub_channel,
                        a.fbusiness_attribute_name,
                        if(
                            a.flast_bargain_name is not null,
                            a.flast_bargain_name,
                            a.Fbargain_submitter
                        ) as fbargin_name,
                        a.fcheck_item_group_level,
                        null as fremark
                    from
                        dws.dws_hs_order_detail_history2023 as a
                        left join dws.dws_hs_order_detail_al as b on a.forder_id = b.forder_id
                    where
                        a.ftest = 0
                        and to_date(a.forder_create_time) between '2023-01-01'
                        and to_date(date_sub(from_unixtime(unix_timestamp()), 366))
                        and a.frecycle_type = 1
                        and a.fdetect_time is not null
                        and left(a.fseries_number, 2) not in ("BM", "CG")
                    union
                    all
                    select
                        a.forder_id,
                        a.funit_price,
                        a.fdetection_price,
                        a.fdetect_price,
                        a.fexempt_price,
                        a.fbargain_price,
                        a.fpay_out_price,
                        a.foperation_price,
                        a.forder_status,
                        a.forder_status_name,
                        a.frecycle_type,
                        a.fclass_name,
                        a.fproduct_name,
                        a.fbargain_time,
                        a.fbrand_name,
                        a.fseries_number,
                        a.freturn_begin_time,
                        a.forder_create_time,
                        a.fdetect_time,
                        a.fsender_phone,
                        a.fdetect_push_time,
                        a.fwait_pay_time,
                        a.fadd_price,
                        a.forder_cancel_time,
                        b.fcancel_time,
                        b.frequire_back_time,
                        a.fsend_back_time,
                        a.fpay_time,
                        a.fproject_name,
                        a.fchannel_name,
                        a.fdet_tpl,
                        if(
                            a.flast_bargain_time is not null,
                            a.flast_bargain_time,
                            a.fbargin_time
                        ) as flast_bargain_time,
                        a.freturn_retention_time,
                        a.fxy_channel,
                        a.fsub_channel,
                        a.fbusiness_attribute_name,
                        if(
                            a.flast_bargain_name is not null,
                            a.flast_bargain_name,
                            a.Fbargain_submitter
                        ) as fbargin_name,
                        a.fcheck_item_group_level,
                        null as fremark
                    from
                        dws.dws_hs_order_detail_history2022 as a
                        left join dws.dws_hs_order_detail_al as b on a.forder_id = b.forder_id
                    where
                        a.ftest = 0
                        and to_date(a.forder_create_time) between to_date(date_sub(from_unixtime(unix_timestamp()), 1200))
                        and '2022-12-31'
                        and a.frecycle_type = 1
                        and a.fdetect_time is not null
                        and left(a.fseries_number, 2) not in ("BM", "CG")
                ) t
        ) a
    where
        a.num = 1
),
sales as (
    select
        *
    from
        (
            select
                *,
                row_number() over(
                    partition by fseries_number
                    order by
                        fstart_time desc
                ) as num
            from
(
                    select
                        fstart_time,
                        fseries_number,
                        fcost_price / 100 as fcost_price,
                        Foffer_price / 100 as Foffer_price
                    from
                        dws.dws_jp_order_detail
                    where
                        Forder_status in (2, 3, 4, 6)
                        and Fpay_time IS NOT NULL
                        and fseries_number is not null
                        and Fpay_time >= to_date(date_sub(from_unixtime(unix_timestamp()), 600))
                    union
                    all
                    select
                        fstart_time,
                        a.fseries_number,
                        fcost_price / 100 as fcost_price,
                        a.Foffer_price / 100 as Foffer_price
                    from
                        dws.dws_th_order_detail as a
                        left join dws.dws_hs_order_detail as b on a.fseries_number = b.fseries_number
                    where
                        a.Forderoffer_status = 10
                        and a.Fpay_time IS NOT NULL
                        and a.fseries_number is not null
                        and a.Fpay_time >= to_date(date_sub(from_unixtime(unix_timestamp()), 700))
                    UNION
                    all
                    select
                        a.fcreate_time as fstart_time,
                        b.Fstock_no as fseries_number,
                        d.fpay_out_price / 100 as fcost_price,
                        b.Fretail_price as foffer_price
                    from
                        drt.drt_my33312_hsb_sales_product_t_stock_order_saleout a
                        inner join drt.drt_my33312_hsb_sales_product_t_stock_order_saleout_detail b on a.Fstock_order_sn = b.Fstock_order_sn
                        left join drt.drt_my33310_recycle_t_order d on b.Fstock_no = d.fseries_number
                    where
                        a.Fsource = 5 -- 手工单
                        and b.Fretail_price > 0
                        and d.fpay_out_price > 0
                        and b.Fstock_no is not null
                        and a.fcreate_time >= to_date(date_sub(from_unixtime(unix_timestamp()), 700))
                ) a
        ) t
    where
        num = 1
),
gongdan_buchang as (
    --工单补偿金额明细，补充订单系统没有体现的金额
    select
        *
    from
        (
            select
                a.fbarcode_sn,
                from_unixtime(a.fadd_time),
                c.fmoney,
                row_number() over(
                    partition by a.fbarcode_sn
                    order by
                        a.fadd_time desc
                ) as num
            from
                drt.drt_my33310_csrdb_t_works as a
                left join drt.drt_my33310_csrdb_t_works_config_appeal as b on a.fappeal_type2 = b.fid
                left join drt.drt_my33310_csrdb_t_works_compensation as c on a.fid = c.fwork_id
            where
                from_unixtime(a.Fadd_time) >= to_date(date_sub(from_unixtime(unix_timestamp()), 700))
                and a.fwork_type in(1, 2, 3, 4)
                and a.fappeal_type1 <> 0
                and b.fcontent like "%议价%"
                and a.fduty_content not like "%无效工单%"
        ) t
    where
        num = 1
),
lanjie as(
    select
        --拦截报告自生成工单\人工议价单
        forder_id,
        fadd_time,
        fcontent
    from
        (
            select
                a.forder_id,
                from_unixtime(a.fadd_time) as fadd_time,
                d.fcontent,
                row_number() over(
                    partition by a.forder_id
                    order by
                        from_unixtime(a.fadd_time) desc
                ) as num
            from
                drt.drt_my33310_csrdb_t_works as a
                left join drt.drt_my33310_csrdb_t_works_config_appeal as d on a.fappeal_type2 = d.fid
            where
                from_unixtime(a.Fadd_time) >= to_date(date_sub(from_unixtime(unix_timestamp()), 1200))
                and a.fwork_type in(1, 2, 3, 4, 7) --and a.fwork_source=3             --订单自生成
                and a.fappeal_type1 <> 0
                and a.fduty_content not like "%无效工单%"
                and d.fcontent in ('已检测状态挽留', '人工议价', '用户要求议价')
        ) t
    where
        num = 1
),
pjt as (
    select
        fseries_number,
        cast(Fpjt_price as int) / 100 as Fpjt_price
    from
        (
            select
                *,
                row_number() over(
                    partition by fseries_number
                    order by
                        fcreate_time desc
                ) as num
            from
                drt.drt_my33312_detection_t_inquiry_log
            where
                fstatus = 1
                and fcreate_time >= to_date(date_sub(from_unixtime(unix_timestamp()), 700))
        ) t
    where
        num = 1
),
chongjian_gongdan as (
    select
        t.fbarcode_sn,
        t.fadd_time as fchongjian_work_time
    from
        (
            select
                to_date(from_unixtime(a.fadd_time)) as fadd_time,
                d.fcontent,
                a.fduty_content,
                a.fbarcode_sn,
                row_number() over(
                    partition by a.fbarcode_sn
                    order by
                        a.fadd_time desc
                ) as num
            from
                drt.drt_my33310_csrdb_t_works as a
                left join drt.drt_my33310_csrdb_t_works_config_appeal as d on a.fappeal_type2 = d.fid
                left join drt.drt_my33310_amcdb_t_user as c on a.fadd_user = c.fusername
            where
                d.fcontent in ('找机重检')
                and to_date(from_unixtime(a.fadd_time)) >= to_date(date_sub(from_unixtime(unix_timestamp()), 180))
                and a.forder_system = 2
                and a.fduty_content != "无效工单" --and c.freal_name in ("黄奕锋","周晓薇","郑春玲","梁椿灏","何嫣红","林宁","徐小利","朱小露",'苍雅婷')
        ) t
    where
        num = 1
),
detect_level as (
    select
        *
    from
        (
            select
                a.fserial_number,
                b.fitem_group_level,
                row_number() over(
                    partition by a.fserial_number
                    order by
                        a.fend_time desc
                ) as num
            from
                drt.drt_my33310_detection_t_detect_record as a
                left join drt.drt_my33311_recycle_t_eva_record as b on cast(a.fevaluate_id as string) = concat(
                    substr(cast(b.Fthe_month AS STRING), 3),
                    cast(b.Fid AS STRING)
                )
            where
                b.fcreate_time >= to_date(date_sub(from_unixtime(unix_timestamp()), 90))
                and b.feva_type = 3
                and left(a.fserial_number, 2) not in ('JM', 'XZ', 'BB', 'BG')
        ) t
    where
        num = 1
)
select
    to_date(fdetect_time) as detect_time,
    HOUR(fdetect_time) as fsdetect_time,
    fdetect_time as ftdetect_time,
    Forder_create_time,
    --Fbargin_time,
    fbargain_time,
    if(
        flast_bargain_time > fpay_time,
        fpay_time,
        flast_bargain_time
    ) as flast_bargain_time,
    fbargin_name,
    forder_cancel_time,
    fcancel_time,
    fwait_pay_time,
    if(
        freturn_retention_time is not null,
        freturn_retention_time,
        d.fadd_time
    ) as gongdan_time,
    d.fadd_time as flanjie_time,
    f.fchongjian_work_time,
    fpay_time,
    fchannel_name,
    freturn_begin_time,
    fremark,
    frequire_back_time,
    freturn_retention_time,
    fbusiness_attribute_name,
    funit_price / 100 as funit_price,
    fdetection_price / 100 as fdetection_price,
    fpay_out_price / 100 as fpay_out_price,
    fbargain_price / 100 as fbargain_price,
    if(fpay_out_price is not null, c.fmoney, 0) as fmoney,
    foperation_price / 100 as foperation_price,
    b.fcost_price,
    b.foffer_price,
    a.forder_id,
    e.Fpjt_price,
    forder_status_name,
    d.fcontent,
    a.fseries_number,
    fbrand_name,
    case
        when fclass_name in (
            '单反/微单套机',
            '单反/微单相机',
            '单反套机',
            '单反相机',
            '微单相机',
            '拍立得',
            '摄影机',
            '数码相机',
            '相机镜头',
            '运动相机'
        ) then "相机/摄像机"
        when fclass_name in (
            'CPU',
            '固态硬盘',
            '电脑主板',
            '显示器',
            '内存条',
            '显卡',
            '电脑内存',
            '电脑固态硬盘',
            '电脑显卡'
        ) then "电脑硬件及周边"
        else fclass_name
    end as fclass_name,
    fproduct_name,
    g.fitem_group_level as fcheck_item_group_level,
    fxy_channel,
    fsender_phone,
    case
        when funit_price / 100 >= 0
        and funit_price / 100 <= 200 then "0-200"
        when funit_price / 100 > 200
        and funit_price / 100 <= 500 then "200-500"
        when funit_price / 100 > 500
        and funit_price / 100 <= 1000 then "500-1000"
        when funit_price / 100 > 1000
        and funit_price / 100 <= 2000 then "1000-2000"
        when funit_price / 100 > 2000
        and funit_price / 100 <= 3000 then "2000-3000"
        when funit_price / 100 > 3000
        and funit_price / 100 <= 4000 then "3000-4000"
        when funit_price / 100 > 4000
        and funit_price / 100 <= 5000 then "4000-5000"
        when funit_price / 100 > 5000 then ">5000"
        else null
    end as "预估金额梯度",
    case
        when funit_price - fdetection_price <= 0 then "0%"
        when (funit_price - fdetection_price) / funit_price > 0
        and (funit_price - fdetection_price) / funit_price <= 0.1 then "0%-10%"
        when (funit_price - fdetection_price) / funit_price > 0.1
        and (funit_price - fdetection_price) / funit_price <= 0.2 then "10%-20%"
        when (funit_price - fdetection_price) / funit_price > 0.2
        and (funit_price - fdetection_price) / funit_price <= 0.3 then "20%-30%"
        when (funit_price - fdetection_price) / funit_price > 0.3
        and (funit_price - fdetection_price) / funit_price <= 0.4 then "30%-40%"
        when (funit_price - fdetection_price) / funit_price > 0.4
        and (funit_price - fdetection_price) / funit_price <= 0.5 then "40%-50%"
        when (funit_price - fdetection_price) / funit_price > 0.5
        and (funit_price - fdetection_price) / funit_price <= 0.6 then "50%-60%"
        when (funit_price - fdetection_price) / funit_price > 0.6
        and (funit_price - fdetection_price) / funit_price <= 0.7 then "60%-70%"
        when (funit_price - fdetection_price) / funit_price > 0.7
        and (funit_price - fdetection_price) / funit_price <= 0.8 then "70%-80%"
        when (funit_price - fdetection_price) / funit_price > 0.8
        and (funit_price - fdetection_price) / funit_price <= 0.9 then "80%-90%"
        when (funit_price - fdetection_price) / funit_price > 0.9
        and (funit_price - fdetection_price) / funit_price <= 1 then "90%-100%"
        else null
    end as "压价率区间",
    left(a.fseries_number, 2) as "渠道",
    case
        when fdetect_time is not null then 1
        else null
    end as "检测数",
    case
        when fexempt_price >= 0
        and fdetection_price >= funit_price
        and foperation_price >= funit_price
        and (
            fwait_pay_time is not null
            or fpay_time is not null
        ) then 1
        when fexempt_price is null
        and fdetection_price >= funit_price
        and foperation_price >= funit_price
        and (
            fwait_pay_time is not null
            or fpay_time is not null
        ) then 1
        else null
    end as "检测无差价成交数",
    case
        when fexempt_price < 0
        and foperation_price >= funit_price
        and (
            fwait_pay_time is not null
            or fpay_time is not null
        ) then 1
        when fexempt_price is null
        and foperation_price >= funit_price
        and fdetection_price < funit_price
        and (
            fwait_pay_time is not null
            or fpay_time is not null
        ) then 1
        else null
    end as "豁免后成交数",
    case
        when fexempt_price >= 0
        and fdetection_price >= funit_price
        and (
            foperation_price >= funit_price
            or foperation_price is null
        )
        and forder_cancel_time is null
        and freturn_begin_time is not null
        and fwait_pay_time is null then 1
        when fexempt_price is null
        and fdetection_price >= funit_price
        and (
            foperation_price >= funit_price
            or foperation_price is null
        )
        and forder_cancel_time is null
        and freturn_begin_time is not null
        and fwait_pay_time is null then 1
        when fexempt_price >= 0
        and fdetection_price >= funit_price
        and (
            foperation_price >= funit_price
            or foperation_price is null
        )
        and freturn_begin_time is null
        and forder_cancel_time is not null
        and fwait_pay_time is null then 1
        when fexempt_price is null
        and fdetection_price >= funit_price
        and (
            foperation_price >= funit_price
            or foperation_price is null
        )
        and freturn_begin_time is null
        and forder_cancel_time is not null
        and fwait_pay_time is null then 1
        when fexempt_price >= 0
        and fdetection_price >= funit_price
        and (
            foperation_price >= funit_price
            or foperation_price is null
        )
        and forder_cancel_time is null
        and freturn_begin_time is null
        and fwait_pay_time is null
        and (
            fcancel_time is not null
            or frequire_back_time is not null
        ) then 1
        when fexempt_price is null
        and fdetection_price >= funit_price
        and (
            foperation_price >= funit_price
            or foperation_price is null
        )
        and forder_cancel_time is null
        and freturn_begin_time is null
        and fwait_pay_time is null
        and (
            fcancel_time is not null
            or frequire_back_time is not null
        ) then 1
        else null
    end as "检测无差价退货数",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is null
        and fdetect_push_time is not null
        and fwait_pay_time is not null
        and (
            unix_timestamp(fwait_pay_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(fdetect_push_time, 'yyyy-MM-dd HH:mm:ss')
        ) > 0 then (
            unix_timestamp(fwait_pay_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(fdetect_push_time, 'yyyy-MM-dd HH:mm:ss')
        ) / 3600
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is null
        and fdetect_push_time is not null
        and fwait_pay_time is not null
        and (
            unix_timestamp(fwait_pay_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(fdetect_push_time, 'yyyy-MM-dd HH:mm:ss')
        ) > 0 then (
            unix_timestamp(fwait_pay_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(fdetect_push_time, 'yyyy-MM-dd HH:mm:ss')
        ) / 3600
        when funit_price / 100 = 0.01
        and flast_bargain_time is null
        and fdetect_push_time is not null
        and fwait_pay_time is not null
        and (
            unix_timestamp(fwait_pay_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(fdetect_push_time, 'yyyy-MM-dd HH:mm:ss')
        ) > 0 then (
            unix_timestamp(fwait_pay_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(fdetect_push_time, 'yyyy-MM-dd HH:mm:ss')
        ) / 3600
        else 0
    end as "自行确认平均时效/小时",
    case
        when fexempt_price < 0
        and foperation_price >= funit_price
        and forder_cancel_time is null
        and freturn_begin_time is not null
        and fwait_pay_time is null then 1
        when fexempt_price < 0
        and foperation_price >= funit_price
        and freturn_begin_time is null
        and forder_cancel_time is not null
        and fwait_pay_time is null then 1
        when fexempt_price is null
        and foperation_price >= funit_price
        and fdetection_price < funit_price
        and forder_cancel_time is null
        and freturn_begin_time is not null
        and fwait_pay_time is null then 1
        when fexempt_price is null
        and foperation_price >= funit_price
        and fdetection_price < funit_price
        and freturn_begin_time is null
        and forder_cancel_time is not null
        and fwait_pay_time is null then 1
        when fexempt_price < 0
        and foperation_price >= funit_price
        and freturn_begin_time is null
        and forder_cancel_time is null
        and (
            fcancel_time is not null
            or frequire_back_time is not null
        )
        and fwait_pay_time is null then 1
        when fexempt_price is null
        and foperation_price >= funit_price
        and fdetection_price < funit_price
        and forder_cancel_time is null
        and freturn_begin_time is null
        and (
            fcancel_time is not null
            or frequire_back_time is not null
        )
        and fwait_pay_time is null then 1
        else null
    end as "豁免后退货数",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and HOUR(fdetect_time) < 16 then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and HOUR(fdetect_time) < 16 then 1
        when funit_price / 100 = 0.01
        and HOUR(fdetect_time) < 16 then 1
        else null
    end as "需议价量-剔除",
    case
        when foperation_price is not null
        and foperation_price < funit_price then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price then 1
        when funit_price / 100 = 0.01 then 1
        else null
    end as "需议价量",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and (
            fwait_pay_time is not null
            or fpay_time is not null
        ) then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and (
            fwait_pay_time is not null
            or fpay_time is not null
        ) then 1
        when funit_price / 100 = 0.01
        and (
            fwait_pay_time is not null
            or fpay_time is not null
        ) then 1
        else null
    end as "需议价成交量",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is null
        and fpay_time is not null
        and fsend_back_time is null then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is null
        and fpay_time is not null
        and fsend_back_time is null then 1
        when funit_price / 100 = 0.01
        and flast_bargain_time is null
        and fpay_time is not null
        and fsend_back_time is null then 1
        else null
    end as "非人工议价成交量",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and fpay_time is null
        and fsend_back_time is not null
        and forder_status_name != "待付款" then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and fpay_time is null
        and fsend_back_time is not null
        and forder_status_name != "待付款" then 1
        when funit_price / 100 = 0.01
        and fpay_time is null
        and fsend_back_time is not null
        and forder_status_name != "待付款" then 1
        else null
    end as "需议价退货数",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is not null
        and fpay_time is null
        and fsend_back_time is null
        and forder_status_name != "已退款"
        and forder_status_name != "待退款" then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is not null
        and fpay_time is null
        and fsend_back_time is null
        and forder_status_name != "已退款"
        and forder_status_name != "待退款" then 1
        when funit_price / 100 = 0.01
        and flast_bargain_time is not null
        and fpay_time is null
        and fsend_back_time is null
        and forder_status_name != "已退款"
        and forder_status_name != "待退款" then 1
        else null
    end as "需议价犹豫数",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is null
        and forder_cancel_time is null
        and freturn_begin_time is not null
        and fwait_pay_time is null then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is null
        and forder_cancel_time is null
        and freturn_begin_time is not null
        and fwait_pay_time is null then 1
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is null
        and forder_cancel_time is null
        and freturn_begin_time is not null
        and fwait_pay_time is null then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is null
        and forder_cancel_time is null
        and freturn_begin_time is not null
        and fwait_pay_time is null then 1
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is null
        and forder_cancel_time is null
        and freturn_begin_time is null
        and (
            fcancel_time is not null
            or frequire_back_time is not null
        )
        and fwait_pay_time is null then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is null
        and forder_cancel_time is null
        and freturn_begin_time is null
        and (
            fcancel_time is not null
            or frequire_back_time is not null
        )
        and fwait_pay_time is null then 1
        else null
    end as "非人工议价退货数",
    case
        when (
            flast_bargain_time is not null
            and fbargin_name is not null
        )
        or freturn_retention_time is not null then 1
        else null
    end as "人工介入量",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is not null
        and forder_cancel_time is null
        and freturn_begin_time is not null
        and fwait_pay_time is null
        and freturn_begin_time > flast_bargain_time then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is not null
        and forder_cancel_time is null
        and freturn_begin_time is not null
        and fwait_pay_time is null
        and freturn_begin_time > flast_bargain_time then 1
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is not null
        and freturn_begin_time is null
        and forder_cancel_time is not null
        and fwait_pay_time is null
        and forder_cancel_time > flast_bargain_time then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is not null
        and freturn_begin_time is null
        and forder_cancel_time is not null
        and fwait_pay_time is null
        and forder_cancel_time > flast_bargain_time then 1
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is not null
        and freturn_begin_time is null
        and forder_cancel_time is null
        and (
            fcancel_time is not null
            or frequire_back_time is not null
        )
        and fwait_pay_time is null
        and forder_cancel_time > flast_bargain_time then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is not null
        and freturn_begin_time is null
        and forder_cancel_time is null
        and (
            fcancel_time is not null
            or frequire_back_time is not null
        )
        and fwait_pay_time is null
        and forder_cancel_time > flast_bargain_time then 1
        else null
    end as "人工议价退货量",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and (
            freturn_begin_time is not null
            or forder_cancel_time is not null
            or fcancel_time is not null
            or frequire_back_time is not null
        ) then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and (
            freturn_begin_time is not null
            or forder_cancel_time is not null
            or fcancel_time is not null
            or frequire_back_time is not null
        ) then 1
        else null
    end as "有差价用户申请退货量",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is not null
        and (
            freturn_begin_time is not null
            or forder_cancel_time is not null
            or fcancel_time is not null
            or frequire_back_time is not null
        ) then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is not null
        and (
            freturn_begin_time is not null
            or forder_cancel_time is not null
            or fcancel_time is not null
            or frequire_back_time is not null
        ) then 1
        else null
    end as "有差价用户申请退货人工介入量",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is not null
        and forder_cancel_time is null
        and freturn_begin_time is not null
        and (
            fwait_pay_time is null
            or fpay_out_price is null
        )
        and freturn_begin_time < flast_bargain_time then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is not null
        and forder_cancel_time is null
        and freturn_begin_time is not null
        and (
            fwait_pay_time is null
            or fpay_out_price is null
        )
        and freturn_begin_time < flast_bargain_time then 1
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is not null
        and freturn_begin_time is null
        and forder_cancel_time is not null
        and (
            fwait_pay_time is null
            or fpay_out_price is null
        )
        and forder_cancel_time < flast_bargain_time then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is not null
        and freturn_begin_time is null
        and forder_cancel_time is not null
        and (
            fwait_pay_time is null
            or fpay_out_price is null
        )
        and forder_cancel_time < flast_bargain_time then 1
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is not null
        and freturn_begin_time is null
        and forder_cancel_time is null
        and (
            fcancel_time is not null
            or frequire_back_time is not null
        )
        and (
            fwait_pay_time is null
            or fpay_out_price is null
        )
        and (
            fcancel_time < flast_bargain_time
            or frequire_back_time < flast_bargain_time
        ) then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is not null
        and freturn_begin_time is null
        and forder_cancel_time is null
        and (
            fcancel_time is not null
            or frequire_back_time is not null
        )
        and (
            fwait_pay_time is null
            or fpay_out_price is null
        )
        and (
            fcancel_time < flast_bargain_time
            or frequire_back_time < flast_bargain_time
        ) then 1
        else null
    end as "有差价退货换回失败量",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is not null
        and (
            fwait_pay_time is not null
            or fpay_time is not null
        )
        and freturn_begin_time is null
        and forder_cancel_time is null
        and fcancel_time is null
        and frequire_back_time is null then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is not null
        and (
            fwait_pay_time is not null
            or fpay_time is not null
        )
        and freturn_begin_time is null
        and forder_cancel_time is null
        and fcancel_time is null
        and frequire_back_time is null then 1
        else null
    end as "人工议价成交量",
    case
        when fbargin_name is not null
        and flast_bargain_time is not null
        and fpay_time is not null then 1
        else null
    end as "人工介入成交量(整体)",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is not null
        and (
            fwait_pay_time is not null
            or fpay_time is not null
        )
        and (
            freturn_begin_time is not null
            or forder_cancel_time is not null
            or fcancel_time is not null
            or frequire_back_time is not null
        )
        and fpay_out_price is not null then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is not null
        and (
            fwait_pay_time is not null
            or fpay_time is not null
        )
        and (
            freturn_begin_time is not null
            or forder_cancel_time is not null
            or fcancel_time is not null
            or frequire_back_time is not null
        )
        and fpay_out_price is not null then 1
        else null
    end as "有差价退货挽回量",
    case
        when flast_bargain_time is not null
        and (
            freturn_begin_time is not null
            or forder_cancel_time is not null
            or fcancel_time is not null
            or frequire_back_time is not null
        ) then 1
        when flast_bargain_time is not null
        and (
            freturn_begin_time is not null
            or forder_cancel_time is not null
            or fcancel_time is not null
            or frequire_back_time is not null
        ) then 1
        else null
    end as "用户申请退货人工介入数",
    case
        when flast_bargain_time is not null
        and (
            fwait_pay_time is not null
            or fpay_time is not null
        )
        and (
            freturn_begin_time is not null
            or forder_cancel_time is not null
            or fcancel_time is not null
            or frequire_back_time is not null
        )
        and fpay_out_price is not null then 1
        when flast_bargain_time is not null
        and (
            fwait_pay_time is not null
            or fpay_time is not null
        )
        and (
            freturn_begin_time is not null
            or forder_cancel_time is not null
            or fcancel_time is not null
            or frequire_back_time is not null
        )
        and fpay_out_price is not null then 1
        else null
    end as "退货挽回量",
    fbargain_price / 100 as "总人工加价金额",
    (
        case
            when foperation_price is not null
            and foperation_price < funit_price
            and flast_bargain_time is not null then fbargain_price
            else null
        end / 100
    ) /(
        case
            when fbargain_price is not null then a.forder_id
            else null
        end
    ) as "单台人工加价金额",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is not null
        and fwait_pay_time is null
        and fpay_time is null
        and freturn_begin_time is null
        and forder_cancel_time is null
        and fcancel_time is null
        and frequire_back_time is null then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is not null
        and fwait_pay_time is null
        and fpay_time is null
        and freturn_begin_time is null
        and forder_cancel_time is null
        and fcancel_time is null
        and frequire_back_time is null then 1
        else null
    end as "有差价犹豫中",
    case
        when foperation_price is not null
        and foperation_price < funit_price
        and flast_bargain_time is null
        and fwait_pay_time is null
        and fpay_time is null
        and freturn_begin_time is null
        and forder_cancel_time is null
        and fcancel_time is null
        and frequire_back_time is null then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price < funit_price
        and flast_bargain_time is null
        and fwait_pay_time is null
        and fpay_time is null
        and freturn_begin_time is null
        and forder_cancel_time is null
        and fcancel_time is null
        and frequire_back_time is null then 1
        else null
    end as "有差价未联系",
    case
        when foperation_price is not null
        and foperation_price >= funit_price
        and fwait_pay_time is null
        and fpay_time is null
        and freturn_begin_time is null
        and forder_cancel_time is null
        and fpay_time is null
        and fcancel_time is null
        and frequire_back_time is null then 1
        when foperation_price is null
        and fdetection_price is not null
        and fdetection_price >= funit_price
        and fwait_pay_time is null
        and fpay_time is null
        and freturn_begin_time is null
        and forder_cancel_time is null
        and fpay_time is null
        and fcancel_time is null
        and frequire_back_time is null then 1
        else null
    end as "无差价用户未操作",
    fdetect_push_time
from
    huishouyijia as a
    left join sales as b on a.fseries_number = b.fseries_number
    left join gongdan_buchang as c on a.fseries_number = c.fbarcode_sn
    left join lanjie as d on a.forder_id = d.forder_id
    left join pjt as e on a.fseries_number = e.fseries_number
    left join chongjian_gongdan as f on a.fseries_number = f.fbarcode_sn
    left join detect_level as g on a.fseries_number = g.fserial_number
where
    to_date(fdetect_time) >= to_date(date_sub(from_unixtime(unix_timestamp()), 600))
order by
    detect_time desc
