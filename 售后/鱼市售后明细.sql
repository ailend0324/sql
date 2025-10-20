with jp_second_sale as (  --二次销售
        select 
        *
    from (
        select 
            a.fseries_number,
            a.foffer_price
        from dws.dws_jp_order_detail as a
        where ftest_show <> 1
        and left(a.fseries_number,2)='NT'
        and a.forder_status in (2,3,4,6)
        union all
        select 
            a.fseries_number,
            a.foffer_price
        from dws.dws_th_order_detail as a
        where a.Fbd_status <>2
        and left(a.fseries_number,2)='NT'
        UNION 
  		select
        b.Fstock_no as fseries_number,
        b.Fretail_price*100 as foffer_price
    from drt.drt_my33312_hsb_sales_product_t_stock_order_saleout a
    inner join drt.drt_my33312_hsb_sales_product_t_stock_order_saleout_detail b on a.Fstock_order_sn = b.Fstock_order_sn
    where a.Fsource = 5 -- 手工单
    and b.Fretail_price>0
    and left(b.Fstock_no,2)='NT') t
)
select
    case Fsource
        when 1 then '采货侠'
        when 2 then '鱼市-B2B'
        when 3 then '鱼市-寄卖'
        when 4 then '手动创建'
    end as 售后来源,
    fafter_sale_no 售后订单ID,
    fout_order_id 原销售订单ID,
    finner_order_no 原销售订单号,
    Frefund_operator 退款人,
    forder_deal_price / 100 订单成交金额,
    case faftersales_status
        when 1 then "申请售后"
        when 2 then '已发货'
        when 3 then '已收货'
        when 4 then '已检测'
        when 5 then '已入库'
        when 6 then '退货中'
        when 7 then '已退货'
    end as 售后状态,
    fapply_reason,
    fapply_time,
    fapply_time as "销售时间",
    fgoods_info,
    Fmerchant_id,
    fnew_serial_no,
    Fmerchant_sn,
    fbusiness_id,
    case fjudge_status
        when 0 then '未定责'
        when 1 then '已定责'
    end as 定责状态,
    fjudge_reason 判责原因,
    case fjudge_type
        when 0 then '非商家责任'
        when 1 then '商家责任'
    end as 判责类型,
    -- fjudge_type `判责类型 1-商家责任 0-非商家责任`,
    -- faftersales_type `售后类型：1 仅退款，2 退货退款`,
    case faftersales_type
        when 1 then '仅退款'
        when 2 then '退货退款'
    end as 售后类型,
    fjudge_result,
    fjudge_time,
    fjudge_remark,
    frefund_total/100 frefud_total,
    frefund_goods/100 frefund_goods,
    b.foffer_price/100 as fsecond_sales_price,
    case when faftersales_type=1 and frefund_total<frefund_goods then frefund_total/100
         when faftersales_type=2 and frefund_total>=frefund_goods and b.foffer_price is not null then (frefund_goods-b.foffer_price)/100
    else 0 end as "售后金额"
from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales as a
left join jp_second_sale as b on a.fnew_serial_no=b.fseries_number
where Fsource in (2,3)
order by fid desc 
