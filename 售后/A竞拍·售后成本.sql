with jp_th_sale_all as (
    select
        -- 拍场信息
        fdetail_id,
        Fmerchant_jp,
        to_date(fstart_time) as fstart_time,
        to_date(fpay_time) as fpay_time,
        case 
            when forder_platform in(5,7) then '鱼市订单'
            when forder_platform in(6) then '采货侠订单'
            when forder_platform = 1 then '自有订单'
        else '其他' end as fbig_order_platform,
        case 
            when forder_platform = 1 then '自有订单'
            when forder_platform = 5 then 'B2B鱼市订单'
            when forder_platform = 6 then '采货侠订单'
            when Forder_platform = 7 then '鱼市同售'
        else '其他' end as forder_platform,
        Fsales_order_num,
        case 
            when forder_platform = 6 and Fchx_hsb_offer_price >=Fchx_base_price then ((foffer_price -(foffer_price - Fchx_hsb_offer_price)*0.4) - Fchx_service_fee)/100
            when forder_platform = 6 and Fchx_hsb_offer_price <Fchx_base_price then ((foffer_price -(foffer_price - Fchx_base_price)*0.4) - Fchx_service_fee)/100
        else foffer_price/100 end as foffer_price,
        
        -- 商品信息
        fseries_number,
        fclass_name,
        Fgoods_level_name,
        fpid,
        fimei,
        IF(Fout_aftersales_option=1,"合伙人","C2B") Fout_aftersales_option, -- 采货侠逆向货源分类
        get_json_object(c.Freport, '$.extra.merchantStore') as merchantStore, -- 鱼市逆向货源服务商
        -- 售后信息
        cast(Fafter_sales_order_id as string) Fafter_sales_order_id,
        Ftotal_real_refund_amount/100 Ftotal_real_refund_amount,
        
        -- 买家/卖家信息
        fuser_id as Fbuy_merchant_id,
        Fmerchant_id_new as Fseller_merchant_id,
        Fmerchant_name_new as Fseller_merchant_name,
        Fshop_id,
        Fshop_name,
        row_number() over(partition by fseries_number order by fdetail_id asc) as frn
    from dws.dws_jp_order_detail a
    left join drt.drt_my33306_hsb_sales_t_yushi_sync_sales_product c on a.fseries_number = c.Fserial_no
    where forder_status in(2,3,4,6)
    and Ftest_show <>1
    and to_date(fstart_time) >='2024-06-01'
    union all
    select
        -- 拍场信息
        0 fdetail_id,
        0 Fmerchant_jp,
        to_date(fstart_time) as fstart_time,
        to_date(fpay_time) as fpay_time,
        '统货订单' as fbig_order_platform,
        '统货订单' as forder_platform,
        Fsales_order_num,
        foffer_price/100 as foffer_price,
        
        -- 商品信息
        fseries_number,
        fclass_name,
        '' Fgoods_level_name,
        fpid,
        '' fimei,
        '' Fout_aftersales_option, -- 逆向货源分类
        '' merchantStore,
        -- 售后信息
        '' Fafter_sales_order_id,
        0 Ftotal_real_refund_amount,
        
        -- 买家/卖家信息
        0 as Fbuy_merchant_id,
        0 as Fseller_merchant_id,
        '' as Fseller_merchant_name,
        0 Fshop_id,
        '' Fshop_name,
        0 frn
    from dws.dws_th_order_detail
    where to_date(fstart_time) >='2024-06-01'
  	and Forderoffer_status is not null
    and Fstatus_pay = 2
),
-- 1、自有订单平台
zy_order_detail as(
    select
        a.*,
        case 
            when b.Frefund_no is not null               then 'V1.0售后'
            when c.Faftersales_order_id is not null     then 'V2.0售后'
        else '无售后' end as fafter_source,
        ifnull(b.Freason,rtrim(ltrim(split_part(c.buy_first_reason_fremark,'#',1),'{'),'}')) as Freason,
        -- ifnull(b.fremark,rtrim(ltrim(split_part(c.buy_first_reason_fremark,'#',2),'{'),'}')) as fremark,
        ifnull(b.Forder_status_name,c.Flast_status) as Forder_status_name,
        c.Finitializer,
        c.Fblame_flag,
        case 
            when a.Ftotal_real_refund_amount > 0 and a.foffer_price/100<=a.Ftotal_real_refund_amount/100 then '退货退款' 
            when a.Ftotal_real_refund_amount > 0 and a.foffer_price/100>a.Ftotal_real_refund_amount/100 then '补差'
            when a.Fafter_sales_order_id is not null and a.Ftotal_real_refund_amount = 0 then '无支付金额'
        else '无售后' end as Fafte_type
    from jp_th_sale_all a
    left join dws.dws_jp_after_sales_detail b on  cast(a.Fafter_sales_order_id as STRING) = b.Frefund_no                                
    left join dws.dws_jp_after_sales_detail2 c on cast(a.Fafter_sales_order_id as int) = c.Faftersales_order_id 
    where a.forder_platform = '自有订单'
),
zy_order_detail_second_times as( -- 自有订单售后二次销售数据
    select
        *
    from(
       select
            a.*,
            b.fseries_number as fnew_series_number,
            c.foffer_price as fnew_offer_price,
            c.forder_platform as fnew_order_platform,
            row_number() over(partition by a.fdetail_id order by c.fdetail_id desc) as frow_num
        from zy_order_detail a
        left join drt.drt_my33310_recycle_t_order b on cast(a.Fafter_sales_order_id as int) = b.forder_id
        left join jp_th_sale_all c on b.fseries_number = c.fseries_number
    ) t where frow_num = 1
),
-- 2、采货侠订单 / 鱼市B2B订单
caihuoxia_yushiB2B_order_detail as (
    select
        a.fdetail_id,
        a.Fmerchant_jp,
        a.fstart_time,
        a.fpay_time,
        a.fbig_order_platform,
        a.forder_platform,
        a.Fsales_order_num,
        a.foffer_price,
        
        -- 商品信息
        a.fseries_number,
        a.fclass_name,
        a.Fgoods_level_name,
        a.fpid,
        a.fimei,
        '' Fout_aftersales_option, -- 逆向货源分类
        '' merchantStore,
        -- 售后信息
        b.fafter_sale_no as fafter_sales_order_id,
        b.frefund_total/100 as Ftotal_real_refund_amount,
        
        -- 买家/卖家信息
        a.Fbuy_merchant_id,
        a.Fseller_merchant_id,
        a.Fseller_merchant_name,
        a.Fshop_id,
        a.Fshop_name,
        a.frn,

        case 
            when b.Finner_order_no is not null and a.forder_platform = "采货侠订单" then '采货侠售后' 
            when b.Finner_order_no is not null and a.forder_platform = "B2B鱼市订单" then '鱼市售后' 
  			when b.Finner_order_no is not null and a.forder_platform = "鱼市同售" then '鱼市售后' 
            else '无售后' end as fafter_source,
        b.Fjudge_reason as Freason,
        b.Fafter_sales_status as Forder_status_name,
        null Finitializer,
        null Fblame_flag,
        case 
            when b.frefund_total/100 = 0 then '无支付金额'
            when b.faftersales_type = 2 and b.frefund_total/100 < b.forder_deal_price/100 then '补差'
            when b.faftersales_type = 2 then '退货退款'
            when b.faftersales_type = 1 then '补差'
        else '无售后' end as Fafte_type,
        b.fnew_serial_no as fnew_series_number,
        c.foffer_price as fnew_offer_price,
        c.forder_platform as fnew_order_platform,
        1 frow_num
    from jp_th_sale_all a
    left join drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales b on a.Fsales_order_num = b.Finner_order_no
    left join jp_th_sale_all c on b.fnew_serial_no = c.fseries_number
    where a.forder_platform in( 'B2B鱼市订单','采货侠订单','鱼市同售')
),
-- 3、鱼市B2C订单
yushiB2C_order_detail as (
    select
        *
    from(
        select
            -- 拍场信息
            a.forder_id as fdetail_id,
            0 Fmerchant_jp,
            to_date(c.fauto_create_time) as fstart_time,
            to_date(c.fauto_create_time) as fpay_time,
            '鱼市订单' as fbig_order_platform,
            'B2C鱼市订单' as forder_platform,
            a.Fsales_order_num,
            a.Fsales_amount/100 as foffer_price,
    
    
            -- 商品信息
            b.fseries_number,
            b.fpid,
            b.fimei,
            
            -- 售后信息
            d.fafter_sale_no as fafter_sales_order_id,
            d.frefund_total/100 as Ftotal_real_refund_amount,
            
            -- 买家/卖家信息
            0 as Fbuy_merchant_id,
            0 as Fseller_merchant_id,
            '' as Fseller_merchant_name,
            0 Fshop_id,
            '' Fshop_name,
    
            
            row_number( ) over(partition by a.forder_id order by c.ftxn_id desc) as frn,
            
            case 
                when d.Finner_order_no is not null then '鱼市售后'
                else '无售后' end as fafter_source,
            d.Fjudge_reason as Freason,
            d.Fafter_sales_status as Forder_status_name,
            null Finitializer,
            null Fblame_flag,
            case 
                when d.frefund_total/100 = 0 then '无支付金额'
                when d.faftersales_type = 2 and d.frefund_total/100 < d.forder_deal_price/100 then '补差'
                when d.faftersales_type = 2 then '退货退款'
                when d.faftersales_type = 1 then '补差'
            else '无售后' end as Fafte_type,
            
            d.fnew_serial_no as fnew_series_number,
            e.foffer_price as fnew_offer_price,
            e.forder_platform as fnew_order_platform,
            1 frow_num
            
        from drt.drt_my33310_recycle_t_xy_jimai_plus_order a
        inner join drt.drt_my33310_recycle_t_order b on a.forder_id = b.forder_id
        inner join drt.drt_my33310_recycle_t_order_txn c on a.forder_id = c.forder_id and c.Forder_status in (714,814) and c.fremark in( '','售出分佣完成','售出分佣失败，未获取到规则，不分佣','获取售出分佣金额为0，不分佣')
        left join drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales d on a.Fsales_order_num = d.fout_order_id
        left join jp_th_sale_all e on d.fnew_serial_no = e.fseries_number
        where a.Fsales_type in(1,4)                         -- 销售类型  1:买家购买 2:平台回收 3:寄卖回收 4:买家购买(一口价)'
        and to_date(c.fauto_create_time) >= '2024-06-01'
    ) t where frn = 1
),
t_detect_record as (
    select
        fserial_number,
        fproduct_name,
        fclass_name, 
        fbrand_name
    from(
        select
            upper(fserial_number) fserial_number, 
            fproduct_name, 
            fclass_name, 
            fbrand_name,
            fcreate_time, 
            row_number() over(partition by fserial_number order by fcreate_time desc) as frow_number
        from drt.drt_my33310_detection_t_detect_record 
        where fdet_tpl = 6 -- 寄卖plus检测模板
    ) t where frow_number = 1
),
yushiB2C_order_detail_temp as(
    select
        -- 拍场信息
        a.fdetail_id,
        a.Fmerchant_jp,
        a.fstart_time,
        a.fpay_time,
        a.fbig_order_platform,
        a.forder_platform,
        a.Fsales_order_num,
        a.foffer_price,


        -- 商品信息
        a.fseries_number,
        b.fclass_name,
        '' fgood_level_name,
        a.fpid,
        a.fimei,
        '' Fout_aftersales_option, -- 逆向货源分类
        '' merchantStore,
        -- 售后信息
        a.fafter_sales_order_id,
        a.Ftotal_real_refund_amount,
        
        -- 买家/卖家信息
        a.Fbuy_merchant_id,
        a.Fseller_merchant_id,
        a.Fseller_merchant_name,
        a.Fshop_id,
        a.Fshop_name,

        a.frn,
        a.fafter_source,
        a.Freason,
        a.Forder_status_name,
        a.Finitializer,
        a.Fblame_flag,
        a.Fafte_type,
        a.fnew_series_number,
        a.fnew_offer_price,
        a.fnew_order_platform,
        1 frow_num
    from yushiB2C_order_detail a
    left join t_detect_record b on a.fseries_number = b.fserial_number
)
select * from zy_order_detail_second_times
union all 
select * from caihuoxia_yushiB2B_order_detail
union all 
select * from yushiB2C_order_detail_temp

