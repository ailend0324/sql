-- =========================
-- 回收全量订单 - 优化重构版
-- 说明：
-- 1) 统一时间配置参数，默认365天
-- 2) 优化重复逻辑，提取公共CTE
-- 3) 简化复杂嵌套条件
-- 4) 提升代码可读性和维护性
-- 5) 修复逻辑错误
-- =========================

-- =========================
-- 统一配置参数
-- =========================
with time_config as (
    select 
        date_sub(from_unixtime(unix_timestamp()), 365) as default_window  -- 统一365天
),

-- =========================
-- 公共CTE：检测数据
-- =========================
detect as (
    select
        fserial_number,
        fclass_name,
        fbrand_name
    from (
        select 
            *,
            row_number() over(partition by fserial_number order by fend_time asc) as num
        from drt.drt_my33310_detection_t_detect_record 
        where fdet_type=0
        and fis_deleted=0
        and freport_type=0
        and fverdict<>"测试单"
        and to_date(fend_time) >= to_date((select default_window from time_config))
    ) t
    where num=1
),

-- =========================
-- 公共CTE：渠道分类逻辑
-- =========================
channel_classification as (
    select 
        fseries_number,
        frecycle_type,
        fsupply_partner,
        fchannel_name,
        faccount,
        case 
            when frecycle_type=2 and fsupply_partner=2 then "闲鱼小站-自营上门"
            when frecycle_type=2 and fsupply_partner=3 then "闲鱼小站-加盟门店-上门"
            when frecycle_type=3 and fsupply_partner=2 then "闲鱼小站-自营门店-到店"
            when frecycle_type=3 and fsupply_partner=3 then "闲鱼小站-加盟门店-到店"
            when left(fseries_number,2) in ('XY','YJ') then "2C闲鱼"
            when left(fseries_number,2) in ('TM','TY') then "天猫以旧换新"
            when left(fseries_number,2)='ZF' then "支付宝小程序"
            when left(fseries_number,2)='CG' then "外采"
            when left(fseries_number,2)='QT' and fchannel_name like "%闲鱼小站%" then "2C闲鱼"
            when left(fseries_number,2)="BB" then "换机侠B端帮卖"
            when left(fseries_number,2)='ZY' and faccount='wendylei@huishoubao.com.cn' then "滞留单"
            when left(fseries_number,2) in ('01','02') then "验货宝"
            else "自有渠道" 
        end as fchannel,
        case 
            when frecycle_type=1 then "邮寄"
            when frecycle_type=2 then "上门"
            when frecycle_type=3 then "到店"
            else null 
        end as `回收方式`,
        case 
            when fsupply_partner=2 then "小站(自营)"
            when fsupply_partner=3 then "小站(加盟)"
            else "回收宝" 
        end as `履约方`
    from (
        select 
            a.fseries_number,
            a.frecycle_type,
            a.fsupply_partner,
            c.fchannel_name,
            b.faccount
        from drt.drt_my33310_recycle_t_order a
        left join drt.drt_my33310_recycle_t_account_info b on a.faccount_id=b.faccount_id
        left join drt.drt_my33310_recycle_t_channel c on a.fchannel_id=c.fchannel_id
        where to_date(a.forder_time) >= to_date((select default_window from time_config))
        and a.ftest=0
    ) base
),

-- =========================
-- 公共CTE：仓库判断逻辑
-- =========================
warehouse_classification as (
    select 
        fseries_number,
        case 
            when right(left(fseries_number,6),4) in ('0112','0118') then "东莞仓"
            when left(fseries_number,2)='02' then "杭州仓"
            else "深圳仓" 
        end as place
    from (
        select distinct fseries_number from drt.drt_my33310_recycle_t_order
        where to_date(forder_time) >= to_date((select default_window from time_config))
    ) base
),

-- =========================
-- 主查询：回收订单信息
-- =========================
recycle_orders as (
    select             
        to_date(a.forder_time) as fdate,
        case when a.Fpayment_mode=3 then "信用订单" else "普通订单" end as `订单类型`,
        w.place,
        case when left(a.fseries_number,2)="BB" then "寄卖" else "回收" end as ftype,
        cc.fchannel,
        c.fchannel_name,
        cc.`回收方式`,
        cc.`履约方`,
        a.fseries_number,
        a.fseries_number as fold_fseries_number,
        a.forder_num,
        a.forder_id,
        coalesce(e.fname, f.fclass_name) as fname,
        coalesce(g.fname, f.fbrand_name) as fbrand_name,
        a.fpay_out_price/100 as fpay_out_price
    from drt.drt_my33310_recycle_t_order as a
    left join drt.drt_my33310_recycle_t_account_info as b on a.faccount_id=b.faccount_id
    left join drt.drt_my33310_recycle_t_channel as c on a.fchannel_id=c.fchannel_id
    left join drt.drt_my33310_recycle_t_product as d on a.fproduct_id=d.fproduct_id
    left join drt.drt_my33310_recycle_t_pdt_class as e on d.fclass_id=e.fid
    left join detect as f on a.fseries_number=f.fserial_number
    left join drt.drt_my33310_recycle_t_pdt_brand as g on d.fbrand_id=g.fid
    left join channel_classification as cc on a.fseries_number=cc.fseries_number
    left join warehouse_classification as w on a.fseries_number=w.fseries_number
    where to_date(a.forder_time) >= to_date((select default_window from time_config))
    and a.ftest=0
    and (left(a.fseries_number,2) not in ('YZ','BM') or a.fseries_number is null)
    and c.fchannel_name not like "%寄卖%"
    and c.fchannel_name not like "%竞拍销售默认渠道号%"
),

-- =========================
-- 主查询：售后订单信息
-- =========================
after_sales_orders as (
    select                                     
        to_date(a.fauto_create_time) as fdate,
        case when c.Fpayment_mode=3 or j.Fpayment_mode=3 then "信用订单" else "普通订单" end as `订单类型`,
        case 
            when right(left(c.fseries_number,6),4) in ('0112','0118') then "东莞仓" 
            when left(a.fsales_series_number,2)='02' or left(h.fbusiness_id,2)='02' then "杭州仓"
            else "深圳仓" 
        end as place,
        case when left(c.fseries_number,2)="BB" then "寄卖" else "回收" end as ftype,
        case 
            when left(a.fsales_series_number,2) in ('01','02') then "验货宝"
            else cc.fchannel
        end as fchannel,
        case 
            when left(a.fsales_series_number,2) in ('01','02') or left(h.fbusiness_id,2) in ('01','02') 
            then "验货宝" 
            else e.fchannel_name 
        end as fchannel_name,
        case 
            when c.frecycle_type=1 or j.frecycle_type=1 then "邮寄"
            when c.frecycle_type=2 or j.frecycle_type=2 then "上门"
            when c.frecycle_type=3 or j.frecycle_type=3 then "到店"
            else "邮寄" 
        end as `回收方式`,
        case 
            when c.fsupply_partner=2 or j.fsupply_partner=2 then "小站(自营)"
            when c.fsupply_partner=3 or j.fsupply_partner=3 then "小站(加盟)"
            else "回收宝" 
        end as `履约方`,
        b.fseries_number,
        case 
            when left(a.fsales_series_number,2)='NT' then coalesce(h.fbusiness_id, upper(n.Fsrouce_serial_no))
            when left(a.fsales_series_number,2)='YZ' then k.fsales_series_number
            else a.fsales_series_number
        end as fold_fseries_number,
        coalesce(c.forder_num, j.forder_num, l.forder_num, o.forder_num) as forder_num,
        coalesce(c.forder_id, j.forder_id, l.forder_id, o.forder_id) as forder_id,
        coalesce(g.fname, m.fclass_name) as fname,
        coalesce(i.fname, m.fbrand_name) as fbrand_name,
        coalesce(c.fpay_out_price, j.fpay_out_price, l.fpay_out_price, o.fpay_out_price)/100 as fpay_out_price
    from drt.drt_my33310_recycle_t_after_sales_order_info as a
    left join drt.drt_my33310_recycle_t_order as b on a.fafter_sales_order_id=b.forder_id
    left join drt.drt_my33310_recycle_t_order as c on a.fsales_series_number=c.fseries_number
    left join drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales as h on a.fsales_series_number=h.fnew_serial_no
    left join drt.drt_my33310_recycle_t_order as j on h.fbusiness_id=j.fseries_number
    left join drt.drt_my33310_recycle_t_after_sales_order_info as k on c.forder_id=k.fafter_sales_order_id
    left join drt.drt_my33310_recycle_t_order as l on k.fsales_series_number=l.fseries_number
    left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as n on a.fsales_series_number=n.Fserial_no
    left join drt.drt_my33310_recycle_t_order as o on upper(n.Fsrouce_serial_no)=o.fseries_number
    left join drt.drt_my33310_recycle_t_account_info as d on coalesce(j.faccount_id, o.faccount_id, l.faccount_id, c.faccount_id)=d.faccount_id
    left join drt.drt_my33310_recycle_t_channel as e on coalesce(j.fchannel_id, o.fchannel_id, l.fchannel_id, c.fchannel_id)=e.fchannel_id
    left join drt.drt_my33310_recycle_t_product as f on coalesce(j.fproduct_id, o.fproduct_id, l.fproduct_id, c.fproduct_id)=f.fproduct_id
    left join drt.drt_my33310_recycle_t_pdt_class as g on f.fclass_id=g.fid
    left join drt.drt_my33310_recycle_t_pdt_brand as i on f.fbrand_id=i.fid
    left join detect as m on coalesce(h.fbusiness_id, a.fsales_series_number)=m.fserial_number
    left join channel_classification as cc on coalesce(c.fseries_number, j.fseries_number, l.fseries_number, o.fseries_number)=cc.fseries_number
    where to_date(a.fauto_create_time) >= to_date((select default_window from time_config))
    and b.fseries_number is not null
),

-- =========================
-- 主查询：采货侠&鱼市售后明细
-- =========================
caihuoxia_after_sales as (
    select              
        to_date(a.fcreate_time) as fdate,
        case when b.Fpayment_mode=3 or j.Fpayment_mode=3 then "信用订单" else "普通订单" end as `订单类型`,
        case 
            when right(left(b.fseries_number,6),4) in ('0112','0118') then "东莞仓" 
            when left(a.fbusiness_id,2)='02' or left(h.fbusiness_id,2)='02' then "杭州仓"
            else "深圳仓" 
        end as place,
        case when left(b.fseries_number,2)="BB" then "寄卖" else "回收" end as ftype,
        case 
            when left(a.fbusiness_id,2) in ('01','02') then "验货宝"
            else cc.fchannel
        end as fchannel,
        case 
            when left(a.fbusiness_id,2) in ('01','02') or left(h.fbusiness_id,2) in ('01','02') or left(k.fsales_series_number,2) in ('01','02') 
            then "验货宝" 
            else e.fchannel_name 
        end as fchannel_name,
        case 
            when b.frecycle_type=1 or j.frecycle_type=1 then "邮寄"
            when b.frecycle_type=2 or j.frecycle_type=2 then "上门"
            when b.frecycle_type=3 or j.frecycle_type=3 then "到店"
            else "邮寄" 
        end as `回收方式`,
        case 
            when b.fsupply_partner=2 or j.fsupply_partner=2 then "小站(自营)"
            when b.fsupply_partner=3 or j.fsupply_partner=3 then "小站(加盟)"
            else "回收宝" 
        end as `履约方`,
        a.fnew_serial_no as fseries_number,
        case 
            when left(a.fbusiness_id,2)='NT' then h.fbusiness_id
            when left(a.fbusiness_id,2)='YZ' then k.fsales_series_number
            else a.fbusiness_id
        end as fold_fseries_number,
        coalesce(b.forder_num, j.forder_num) as forder_num,
        coalesce(b.forder_id, j.forder_id) as forder_id,
        coalesce(g.fname, m.fclass_name) as fname,
        coalesce(i.fname, m.fbrand_name) as fbrand_name,
        coalesce(b.fpay_out_price, j.fpay_out_price, l.fpay_out_price)/100 as fpay_out_price
    from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales as a
    left join drt.drt_my33310_recycle_t_order as b on a.fbusiness_id=b.fseries_number
    left join drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales as h on a.fbusiness_id=h.fnew_serial_no
    left join drt.drt_my33310_recycle_t_order as j on h.fbusiness_id=j.fseries_number
    left join drt.drt_my33310_recycle_t_after_sales_order_info as k on b.forder_id=k.fafter_sales_order_id
    left join drt.drt_my33310_recycle_t_order as l on k.fsales_series_number=l.fseries_number
    left join drt.drt_my33310_recycle_t_account_info as d on coalesce(j.faccount_id, l.faccount_id, b.faccount_id)=d.faccount_id
    left join drt.drt_my33310_recycle_t_channel as e on coalesce(j.fchannel_id, l.fchannel_id, b.fchannel_id)=e.fchannel_id
    left join drt.drt_my33310_recycle_t_product as f on coalesce(j.fproduct_id, l.fproduct_id, b.fproduct_id)=f.fproduct_id
    left join drt.drt_my33310_recycle_t_pdt_class as g on f.fclass_id=g.fid
    left join drt.drt_my33310_recycle_t_pdt_brand as i on f.fbrand_id=i.fid
    left join detect as m on coalesce(h.fbusiness_id, a.fbusiness_id)=m.fserial_number
    left join channel_classification as cc on coalesce(b.fseries_number, j.fseries_number, l.fseries_number)=cc.fseries_number
    where to_date(a.fcreate_time) >= to_date((select default_window from time_config))
    and a.fnew_serial_no!=""
),

-- =========================
-- 主查询：系统自动转NT条码
-- =========================
nt_auto_convert as (
    select                                    
        to_date(a.fcreate_time) as fdate,
        case when b.Fpayment_mode=3 or j.Fpayment_mode=3 or l.Fpayment_mode=3 then "信用订单" else "普通订单" end as `订单类型`,
        case 
            when right(left(b.fseries_number,6),4) in ('0112','0118') then "东莞仓" 
            when left(upper(a.Fsrouce_serial_no),2)='02' or left(h.fbusiness_id,2)='02' or left(upper(k.Fsrouce_serial_no),2)='02' then "杭州仓"
            else "深圳仓" 
        end as place,
        case when left(b.fseries_number,2)="BB" then "寄卖" else "回收" end as ftype,
        case 
            when left(upper(a.Fsrouce_serial_no),2) in ('01','02') then "验货宝"
            else cc.fchannel
        end as fchannel,
        case 
            when left(upper(a.Fsrouce_serial_no),2) in ('01','02') or left(h.fbusiness_id,2) in ('01','02') or left(upper(k.Fsrouce_serial_no),2) in ('01','02') or left(n.fsales_series_number,2) in ('01','02') 
            then "验货宝" 
            else e.fchannel_name 
        end as fchannel_name,
        case 
            when b.frecycle_type=1 or j.frecycle_type=1 or l.frecycle_type=1 then "邮寄"
            when b.frecycle_type=2 or j.frecycle_type=2 or l.frecycle_type=2 then "上门"
            when b.frecycle_type=3 or j.frecycle_type=3 or l.frecycle_type=3 then "到店"  -- 修复原逻辑错误
            else "邮寄" 
        end as `回收方式`,
        case 
            when b.fsupply_partner=2 or j.fsupply_partner=2 or l.fsupply_partner=2 then "小站(自营)"
            when b.fsupply_partner=3 or j.fsupply_partner=3 or l.fsupply_partner=3 then "小站(加盟)"
            else "回收宝" 
        end as `履约方`,
        a.Fserial_no as fseries_number,
        case 
            when left(upper(a.Fsrouce_serial_no),2)='NT' then coalesce(h.fbusiness_id, upper(k.Fsrouce_serial_no))
            when left(upper(a.Fsrouce_serial_no),2)='YZ' then n.fsales_series_number
            else upper(a.Fsrouce_serial_no)
        end as fold_fseries_number,
        coalesce(b.forder_num, j.forder_num, l.forder_num) as forder_num,
        coalesce(b.forder_id, j.forder_id, l.forder_id) as forder_id,
        coalesce(g.fname, m.fclass_name) as fname,
        coalesce(i.fname, m.fbrand_name) as fbrand_name,
        coalesce(b.fpay_out_price, j.fpay_out_price, l.fpay_out_price, o.fpay_out_price)/100 as fpay_out_price
    from drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as a
    left join drt.drt_my33310_recycle_t_order as b on upper(a.Fsrouce_serial_no)=b.fseries_number
    left join drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales as h on upper(a.Fsrouce_serial_no)=h.fnew_serial_no
    left join drt.drt_my33310_recycle_t_order as j on h.fbusiness_id=j.fseries_number
    left join drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as k on upper(a.Fsrouce_serial_no)=k.Fserial_no
    left join drt.drt_my33310_recycle_t_order as l on upper(k.Fsrouce_serial_no)=l.fseries_number
    left join drt.drt_my33310_recycle_t_after_sales_order_info as n on b.forder_id=n.fafter_sales_order_id
    left join drt.drt_my33310_recycle_t_order as o on n.fsales_series_number=o.fseries_number
    left join drt.drt_my33310_recycle_t_account_info as d on coalesce(j.faccount_id, l.faccount_id, o.faccount_id, b.faccount_id)=d.faccount_id
    left join drt.drt_my33310_recycle_t_channel as e on coalesce(j.fchannel_id, l.fchannel_id, o.fchannel_id, b.fchannel_id)=e.fchannel_id
    left join drt.drt_my33310_recycle_t_product as f on coalesce(j.fproduct_id, l.fproduct_id, o.fproduct_id, b.fproduct_id)=f.fproduct_id
    left join drt.drt_my33310_recycle_t_pdt_class as g on f.fclass_id=g.fid
    left join drt.drt_my33310_recycle_t_pdt_brand as i on f.fbrand_id=i.fid
    left join detect as m on coalesce(h.fbusiness_id, upper(a.Fsrouce_serial_no))=m.fserial_number
    left join channel_classification as cc on coalesce(b.fseries_number, j.fseries_number, l.fseries_number, o.fseries_number)=cc.fseries_number
    where left(a.Fserial_no,2)='NT' 
    and to_date(a.fcreate_time) >= to_date((select default_window from time_config))
),

-- =========================
-- 主查询：验机转回收
-- =========================
inspection_to_recycle as (
    select                        
        to_date(fauto_create_time) as fdate,
        "普通订单" as `订单类型`,
        "深圳仓" as place,
        "验机" as ftype,
        "验货宝" as fchannel,
        "验货宝" as fchannel_name,
        "邮寄" as `回收方式`,
        "回收宝" as `履约方`,
        fhost_barcode,
        fhost_barcode as fold_fseries_number,
        cast(a.fxy_order_id as string) as fxy_order_id,
        forder_id,
        c.fclass_name as fname,
        c.fbrand_name,
        a.famount/100 as fpay_out_price
    from drt.drt_my33315_xy_detect_t_xy_yhb_recycle_pay_record as a 
    left join drt.drt_my33315_xy_detect_t_xy_hsb_order as b on a.fxy_order_id=b.fxy_order_id
    left join detect as c on b.fhost_barcode=c.fserial_number
    where to_date(fauto_create_time) >= to_date((select default_window from time_config))
),

-- =========================
-- 主查询：寄卖兜底回收
-- =========================
jimai_fallback as (
    select                             
        to_date(b.fpay_time) as fdate,
        case when a.Fpayment_mode=3 then "信用订单" else "普通订单" end as `订单类型`,
        case 
            when right(left(a.fseries_number,6),4) in ('0112','0118') then "东莞仓" 
            else "深圳仓" 
        end as place,
        "寄卖" as ftype,
        case 
            when a.frecycle_type=2 and a.fsupply_partner=2 then "闲鱼小站-自营上门-公司兜底"
            when a.frecycle_type=2 and a.fsupply_partner=3 then "闲鱼小站-加盟门店-上门-加盟商兜底"
            when a.frecycle_type=3 and a.fsupply_partner=2 then "闲鱼小站-自营门店-到店-公司兜底"
            when a.frecycle_type=3 and a.fsupply_partner=3 then "闲鱼小站-加盟门店-到店-加盟商兜底"
            when c.fchannel_name="小豹帮卖" then "自有渠道"
            else "闲鱼寄卖plus-公司兜底" 
        end as fchannel,
        h.fchannel_name,
        case 
            when a.frecycle_type=1 then "邮寄"
            when a.frecycle_type=2 then "上门"
            when a.frecycle_type=3 then "到店"
            else null 
        end as `回收方式`,
        case 
            when a.fsupply_partner=2 then "小站(自营)"
            when a.fsupply_partner=3 then "小站(加盟)"
            else "回收宝" 
        end as `履约方`,
        a.fseries_number,
        a.fseries_number as fold_fseries_number,
        a.forder_num,
        a.forder_id,
        f.fname,
        g.fname as fbrand_name,
        a.fpay_out_price/100 as fpay_out_price
    from drt.drt_my33310_recycle_t_order as a 
    left join (
        select                            
            *
        from (
            select 
                a.forder_id,
                a.fauto_update_time as fpay_time,
                row_number() over(partition by a.forder_id order by a.fauto_update_time asc) as num
            from drt.drt_my33310_recycle_t_order_txn as a
            left join drt.drt_my33310_recycle_t_xy_jimai_plus_order as b on a.forder_id=b.forder_id
            left join drt.drt_my33310_recycle_t_order as c on a.forder_id=c.forder_id 
            where a.forder_status in (714,815)
            and to_date(a.fauto_update_time) >= to_date((select default_window from time_config))
            and c.forder_status not in (90,110)
        ) t
        where num=1
    ) as d on a.forder_id=d.forder_id
    left join drt.drt_my33310_recycle_t_channel as c on a.fchannel_id=c.fchannel_id
    left join (
        select 
            *
        from (
            select 
                forder_id,
                fauto_create_time as fpay_time,
                row_number() over(partition by forder_id order by fauto_create_time asc) as num
            from drt.drt_my33310_recycle_t_order_txn 
            where forder_status in (260,261,130)
            and to_date(fauto_update_time) >= to_date((select default_window from time_config))
        ) t
        where num=1
    ) as b on a.forder_id=b.forder_id
    left join drt.drt_my33310_recycle_t_product as e on a.fproduct_id=e.fproduct_id
    left join drt.drt_my33310_recycle_t_pdt_class as f on e.fclass_id=f.fid
    left join drt.drt_my33310_recycle_t_pdt_brand as g on e.fbrand_id=g.fid
    left join drt.drt_my33310_recycle_t_channel as h on a.fchannel_id=h.fchannel_id
    where to_date(b.fpay_time) >= to_date((select default_window from time_config))
    and a.ftest=0
    and d.fpay_time is null
    and left(a.fseries_number,2) in ('BM')
),

-- =========================
-- 主查询：寄卖订单
-- =========================
jimai_orders as (
    select                                          
        to_date(d.fpay_time) as fdate,
        case when a.Fpayment_mode=3 then "信用订单" else "普通订单" end as `订单类型`,
        case 
            when right(left(a.fseries_number,6),4) in ('0112','0118') then "东莞仓" 
            else "深圳仓" 
        end as place,
        "寄卖" as ftype,
        case 
            when a.frecycle_type=2 and a.fsupply_partner=2 and d.forder_status=815 then "闲鱼小站-自营上门-一口价"
            when a.frecycle_type=2 and a.fsupply_partner=2 and d.forder_status=714 then "闲鱼小站-自营上门-帮卖成交"
            when a.frecycle_type=2 and a.fsupply_partner=3 and d.forder_status=815 then "闲鱼小站-加盟门店-上门-一口价"
            when a.frecycle_type=2 and a.fsupply_partner=3 and d.forder_status=714 then "闲鱼小站-加盟门店-上门-帮卖成交"
            when a.frecycle_type=3 and a.fsupply_partner=2 and d.forder_status=815 then "闲鱼小站-自营门店-到店-一口价"
            when a.frecycle_type=3 and a.fsupply_partner=2 and d.forder_status=714 then "闲鱼小站-自营门店-到店-帮卖成交"
            when a.frecycle_type=3 and a.fsupply_partner=3 and d.forder_status=815 then "闲鱼小站-加盟门店-到店-一口价"
            when a.frecycle_type=3 and a.fsupply_partner=3 and d.forder_status=714 then "闲鱼小站-加盟门店-到店-帮卖成交"
            when c.fchannel_name="小豹帮卖" then "自有渠道"
            when d.forder_status=815 and a.frecycle_type=1 then "闲鱼寄卖plus-一口价"
            else "闲鱼寄卖plus-帮卖成交" 
        end as fchannel,
        c.fchannel_name,
        case 
            when a.frecycle_type=1 then "邮寄"
            when a.frecycle_type=2 then "上门"
            when a.frecycle_type=3 then "到店"
            else null 
        end as `回收方式`,
        case 
            when a.fsupply_partner=2 then "小站(自营)"
            when a.fsupply_partner=3 then "小站(加盟)"
            else "回收宝" 
        end as `履约方`,
        a.fseries_number,
        a.fseries_number as fold_fseries_number,
        a.forder_num,
        a.forder_id,
        f.fname,
        g.fname as fbrand_name,
        a.fpay_out_price/100 as fpay_out_price
    from drt.drt_my33310_recycle_t_order as a
    inner join (
        select                            
            *
        from (
            select 
                a.forder_id,
                a.fauto_update_time as fpay_time,
                a.forder_status,
                row_number() over(partition by a.forder_id order by a.fauto_update_time asc) as num
            from drt.drt_my33310_recycle_t_order_txn as a
            left join drt.drt_my33310_recycle_t_xy_jimai_plus_order as b on a.forder_id=b.forder_id
            where a.forder_status in (714,815)
            and to_date(a.fauto_update_time) >= to_date((select default_window from time_config))
        ) t
        where num=1
    ) as d on a.forder_id=d.forder_id
    left join drt.drt_my33310_recycle_t_channel as c on a.fchannel_id=c.fchannel_id
    left join drt.drt_my33310_recycle_t_product as e on a.fproduct_id=e.fproduct_id
    left join drt.drt_my33310_recycle_t_pdt_class as f on e.fclass_id=f.fid
    left join drt.drt_my33310_recycle_t_pdt_brand as g on e.fbrand_id=g.fid
    where to_date(d.fpay_time) >= to_date((select default_window from time_config))
    and a.ftest=0
    and left(a.fseries_number,2) in ("BM")
    and a.forder_status not in (90,110)
)

-- =========================
-- 最终结果：合并所有数据
-- =========================
select * from recycle_orders
union all
select * from after_sales_orders
union all
select * from caihuoxia_after_sales
union all
select * from nt_auto_convert
union all
select * from inspection_to_recycle
union all
select * from jimai_fallback
union all
select * from jimai_orders
