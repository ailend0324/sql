with nt as (               --NT售后条码
select 
    a.fserial_no,
    a.fsrouce_serial_no,
    b.frecycle_type,
    b.forder_num,
    b.forder_id,
    b.fchannel_name,
    b.fproject_name,
    b.fsupply_partner,
    b.fshop_name,
    b.fclass_name
from drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as a
left join dws.dws_hs_order_detail as b on upper(a.fsrouce_serial_no)=b.fseries_number
),
detect_record as (
select 
    t.fserial_number,
    t.fclass_name
    from (
        select 
            *,
            row_number() over(partition by fserial_number order by fend_time desc) as num
        from drt.drt_my33310_detection_t_detect_record 
        where fis_deleted=0
      	and freport_type=0
    	and fverdict<>"测试单"
        and to_date(fend_time)>='2021-01-01'
        )t
where num=1
),
sales as (             --竞拍销售数据
select 
    *
from (
    select 
        *,
        row_number()over(partition by fseries_number order by fstart_time desc)as num
    from(
        select 
            fstart_time,
            fseries_number,
            fcost_price/100 as fcost_price,
            Foffer_price/100 as Foffer_price,
            forder_platform,
            fsales_order_num,
            fclass_name
        from dws.dws_jp_order_detail 
        where Forder_status in (2,3,4,6)
        and Fpay_time IS NOT NULL
        and to_date(Fpay_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
        union all
        select 
            fstart_time,
            fseries_number,
            fcost_price/100 as fcost_price,
            a.Foffer_price/100 as Foffer_price,
            10 as forder_platform,
            a.fsales_order_num,
            fclass_name
        from dws.dws_th_order_detail as a
        where a.Forderoffer_status = 10
        and a.Fpay_time IS NOT NULL
        and to_date(a.fpay_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180)))a)t where num=1
),
t_stock_order_saleout as (            -- 手工单
    select
        b.fid, 
        a.Fout_type,
        to_date(a.fcreate_time),
        a.faccount,
        a.fmessage, 
        b.Fstock_no,
        b.Fretail_price ,
        d.fpay_out_price/100 
    from drt.drt_my33312_hsb_sales_product_t_stock_order_saleout a
    inner join drt.drt_my33312_hsb_sales_product_t_stock_order_saleout_detail b on a.Fstock_order_sn = b.Fstock_order_sn
    left join drt.drt_my33310_recycle_t_order d on b.Fstock_no = d.fseries_number 
    where a.Fsource = 5 -- 手工单
)
select                                         --销售&退货出库对应物流信息
    t.fcreate_time,
    t.fstock_order_sn,
    t.fserial_number,
    t.ftype,
    upper(t.fexpress_reality_sn) as fexpress_reality_sn,
    case when left(t.fserial_number,3) like "%020%" or left(t.fserial_number,3) like "%010%" or left(t.fserial_number,3) like "%050%" then "验机" 
         when left(t.fserial_number,2) like "%BM%" then "寄卖"
         when left(t.fserial_number,2) like "%CG%" then "采购回收"
         when left(t.fserial_number,2) like "%YZ%" or left(t.fserial_number,2) like "%NT%" then "售后回收"
         when left(t.fserial_number,2) like "%BB%" then "B端帮卖"
    else "回收" end as "业务类型",
    case 
         when if(h.frecycle_type is not null,h.frecycle_type,if(g.frecycle_type is not null,g.frecycle_type,if(f.frecycle_type is not null,f.frecycle_type,if(e.frecycle_type is not null,e.frecycle_type,if(d.frecycle_type is not null,d.frecycle_type,if(c.frecycle_type is not null,c.frecycle_type,b.frecycle_type))))))=3
              and if(h.fchannel_name is not null,h.fchannel_name,if(g.fchannel_name is not null,g.fchannel_name,if(f.fchannel_name is not null,f.fchannel_name,if(e.fchannel_name is not null,e.fchannel_name,if(d.fchannel_name is not null,d.fchannel_name,if(c.fchannel_name is not null,c.fchannel_name,b.fchannel_name)))))) not in ("闲鱼到店","回收宝APP-2C","H5回收宝","微信公众号2","闲鱼小站闲鱼小程序","支付宝小程序") then "小站"
         when (b.fproject_name='合作项目' and b.fchannel_name!='荣耀商城' and b.fchannel_name!='微博官网' and b.fchannel_name!='联想商城') or (c.fproject_name='合作项目' and c.fchannel_name!='荣耀商城' and c.fchannel_name!='微博官网' and c.fchannel_name!='联想商城') or (d.fproject_name='合作项目' and d.fchannel_name!='荣耀商城' and d.fchannel_name!='微博官网' and d.fchannel_name!='联想商城')
     or (e.fproject_name='合作项目' and e.fchannel_name!='荣耀商城' and e.fchannel_name!='微博官网' and e.fchannel_name!='联想商城') or (f.fproject_name='合作项目' and f.fchannel_name!='荣耀商城' and f.fchannel_name!='微博官网' and f.fchannel_name!='联想商城') or (g.fproject_name='合作项目' and g.fchannel_name!='荣耀商城' and g.fchannel_name!='微博官网' and g.fchannel_name!='联想商城') or (h.fproject_name='合作项目' and h.fchannel_name!='荣耀商城' and h.fchannel_name!='微博官网' and h.fchannel_name!='联想商城') then "合作"
         when b.fchannel_name='支付宝小程序' or b.fchannel_name='闲鱼同城小程序' or c.fchannel_name='支付宝小程序' or c.fchannel_name='闲鱼同城小程序' or d.fchannel_name='支付宝小程序' or d.fchannel_name='闲鱼同城小程序'
          or e.fchannel_name='支付宝小程序' or e.fchannel_name='闲鱼同城小程序'  or f.fchannel_name='支付宝小程序' or f.fchannel_name='闲鱼同城小程序'  or g.fchannel_name='支付宝小程序' or g.fchannel_name='闲鱼同城小程序' or h.fchannel_name='支付宝小程序' or h.fchannel_name='闲鱼同城小程序' then "合作"
         when left(t.fserial_number,3) like "%020%" or left(t.fserial_number,3) like "%010%" or left(t.fserial_number,3) like "%050%" then "验机"
         when b.fchannel_name='闲鱼寄卖plus' or c.fchannel_name='闲鱼寄卖plus' or d.fchannel_name='闲鱼寄卖plus' or e.fchannel_name='闲鱼寄卖plus' or f.fchannel_name='闲鱼寄卖plus' or g.fchannel_name='闲鱼寄卖plus' or h.fchannel_name='闲鱼寄卖plus'then "合作"
         when left(t.fserial_number,2) like "%XY%" or left(t.fserial_number,2) like "%YJ%" or left(t.fserial_number,2) like "%TM%" or left(t.fserial_number,2) like "%TY%" then "合作"
         when left(t.fserial_number,2) like "%CG%" then "采购回收"
         when left(t.fserial_number,2) like "%YZ%" or left(t.fserial_number,2) like "%NT%" then "售后回收"
         when left(t.fserial_number,2) like "%BB%" then "B端帮卖"
    else '自有' end as "流量来源",
    if(h.fsrouce_serial_no is not null,h.fsrouce_serial_no,if(g.fseries_number is not null,g.fseries_number,if(f.fseries_number is not null,f.fseries_number,if(e.fseries_number is not null,e.fseries_number,if(d.fseries_number is not null,d.fseries_number,if(c.fseries_number is not null,c.fseries_number,b.fseries_number)))))) as fold_series_number,
    if(h.fchannel_name is not null,h.fchannel_name,if(g.fchannel_name is not null,g.fchannel_name,if(f.fchannel_name is not null,f.fchannel_name,if(e.fchannel_name is not null,e.fchannel_name,if(d.fchannel_name is not null,d.fchannel_name,if(c.fchannel_name is not null,c.fchannel_name,b.fchannel_name)))))) as fchannel_name,
    if(h.forder_num is not null,h.forder_num,if(g.forder_num is not null,g.forder_num,if(f.forder_num is not null,f.forder_num,if(e.forder_num is not null,e.forder_num,if(d.forder_num is not null,d.forder_num,if(c.forder_num is not null,c.forder_num,b.forder_num)))))) as forder_num,
    if(h.forder_id is not null,h.forder_id,if(g.forder_id is not null,g.forder_id,if(f.forder_id is not null,f.forder_id,if(e.forder_id is not null,e.forder_id,if(d.forder_id is not null,d.forder_id,if(c.forder_id is not null,c.forder_id,b.forder_id)))))) as forder_id,
    if(k.fclass_name is not null,k.fclass_name,if(b.fclass_name is not null,b.fclass_name,i.fclass_name)) as fclass_name,
    case when i.forder_platform=1 then "自有竞拍"
         when i.forder_platform=5 then "B端鱼市"
         when i.forder_platform=6 then "采货侠"
         when i.forder_platform=7 then "双向鱼市"
         when i.forder_platform=10 then "统货"
         when left(t.fserial_number,2)='BM' and ftype="销售出库" then "C端鱼市"
         when j.fmessage like "%优品%" then "优品"
         when j.fstock_no is not null then "线下"
    else null end as forder_platform,
    if(ftype="销售出库",if(i.fsales_order_num is not null,i.fsales_order_num,z.fsales_order_num),null) as fsales_order_num,
    case when h.fsupply_partner=2 or g.fsupply_partner=2 or f.fsupply_partner=2 or e.fsupply_partner=2 or d.fsupply_partner=2 or c.fsupply_partner=2 or b.fsupply_partner=2 then "小站(自营)"
         when h.fsupply_partner=3 or g.fsupply_partner=3 or f.fsupply_partner=3 or e.fsupply_partner=3 or d.fsupply_partner=3 or c.fsupply_partner=3 or b.fsupply_partner=3 then "小站(加盟)" 
         when h.fsupply_partner=5 or g.fsupply_partner=5 or f.fsupply_partner=5 or e.fsupply_partner=5 or d.fsupply_partner=5 or c.fsupply_partner=5 or b.fsupply_partner=5 then "小豹哥" 
         when h.fsupply_partner=6 or g.fsupply_partner=6 or f.fsupply_partner=6 or e.fsupply_partner=6 or d.fsupply_partner=6 or c.fsupply_partner=6 or b.fsupply_partner=6 then "顺丰" 
         when h.fsupply_partner=0 or g.fsupply_partner=0 or f.fsupply_partner=0 or e.fsupply_partner=0 or d.fsupply_partner=0 or c.fsupply_partner=0 or b.fsupply_partner=0 then "回收宝" 
    else null end as "履约方",
    case when left(Fexpress_reality_code,2) like "%JD%" then "京东"
         when left(Fexpress_reality_code,2) like "%SF%" then "顺丰"
         when left(Fexpress_reality_code,3) like "%ZTO%" then "中通"
    else "其它" end as "快递类型"
from (
select  
    a.fstock_order_sn,
    a.fcreate_time,
    b.fserial_no as fserial_number,
    case when c.fcmd='JYCK' then "销售出库"
         when c.fcmd is null and left(b.fserial_no,2) like "%BM%" then "销售出库(小站寄卖)"
         when c.fcmd='CGTH' and (left(b.fserial_no,2) like "%YZ%" or left(b.fserial_no,2) like "%NT%") then "售后退货出库"
         when c.fcmd='CGTH' and left(b.fserial_no,2) not like "%YZ%" and left(b.fserial_no,2) not like "%NT%" then "回收退货出库"
    else null end as ftype,
    a.fexpress_reality_sn,
    a.Fexpress_reality_code,
    row_number()over(partition by b.fserial_no order by a.fcreate_time desc)as num
from  drt.drt_my33312_hsb_sales_product_t_stock_order_express as a
left join drt.drt_my33312_hsb_sales_product_t_stock_order_out_detail as b on a.fstock_order_sn=b.fstock_order_sn
left join drt.drt_my33312_hsb_sales_product_t_pm_wms_stock_notify as c on b.fserial_no=c.fserial_no
where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and ((c.fcmd='JYCK' and a.fstock_owner!=3) OR (c.fcmd='CGTH' and a.fstock_owner!=3) or (c.fcmd is null and a.fstock_owner=3))
)t
left join dws.dws_hs_order_detail as b on t.fserial_number=b.fseries_number
left join drt.drt_my33310_recycle_t_xy_jimai_plus_order as z on b.forder_id=z.forder_id
left join dws.dws_hs_order_detail as c on b.fold_fseries_number=c.fseries_number
left join dws.dws_hs_order_detail as d on c.fold_fseries_number=d.fseries_number
left join dws.dws_hs_order_detail as e on d.fold_fseries_number=e.fseries_number
left join dws.dws_hs_order_detail as f on e.fold_fseries_number=f.fseries_number
left join dws.dws_hs_order_detail as g on f.fold_fseries_number=g.fseries_number
left join nt as h on t.fserial_number=h.fserial_no
left join sales as i on t.fserial_number=i.fseries_number
left join t_stock_order_saleout as j on t.fserial_number=j.Fstock_no
left join detect_record as k on t.fserial_number=k.fserial_number
where t.num=1
and t.fexpress_reality_sn is not null
union
select                                           --异地销售出库对应物流信息
    t.fcreate_time,
    t.fstock_order_sn,
    t.fserial_number,
    "销售出库" as ftype,
    upper(t.fexpress_reality_sn) as fexpress_reality_sn,
    case when left(t.fserial_number,3) like "%020%" or left(t.fserial_number,3) like "%010%" or left(t.fserial_number,3) like "%050%" then "验机" 
         when left(t.fserial_number,2) like "%BM%" then "寄卖"
         when left(t.fserial_number,2) like "%CG%" then "采购回收"
         when left(t.fserial_number,2) like "%YZ%" or left(t.fserial_number,2) like "%NT%" then "售后回收"
         when left(t.fserial_number,2) like "%BB%" then "B端帮卖"
         when left(t.fserial_number,2) like "%BG%" then "商家自检"
    else "回收" end as "业务类型",
    case 
         when b.frecycle_type=3
              and b.fchannel_name not in ("闲鱼到店","回收宝APP-2C","H5回收宝","微信公众号2","闲鱼小站闲鱼小程序","支付宝小程序") then "小站"
         when (b.fproject_name='合作项目' and b.fchannel_name!='荣耀商城' and b.fchannel_name!='微博官网' and b.fchannel_name!='联想商城')  then "合作"
         when b.fchannel_name='支付宝小程序' or b.fchannel_name='闲鱼同城小程序' then "合作"
         when left(t.fserial_number,3) like "%020%" or left(t.fserial_number,3) like "%010%" or left(t.fserial_number,3) like "%050%" then "验机"
         when b.fchannel_name='闲鱼寄卖plus' then "合作"
         when left(t.fserial_number,2) like "%XY%" or left(t.fserial_number,2) like "%YJ%" or left(t.fserial_number,2) like "%TM%" or left(t.fserial_number,2) like "%TY%" then "合作"
         when left(t.fserial_number,2) like "%CG%" then "采购回收"
         when left(t.fserial_number,2) like "%YZ%" or left(t.fserial_number,2) like "%NT%" then "售后回收"
         when left(t.fserial_number,2) like "%BB%" then "B端帮卖"
         when left(t.fserial_number,2) like "%BG%" then "商家自检"
    else '自有' end as "流量来源",
    if(h.fsrouce_serial_no is not null,h.fsrouce_serial_no,if(g.fseries_number is not null,g.fseries_number,if(f.fseries_number is not null,f.fseries_number,if(e.fseries_number is not null,e.fseries_number,if(d.fseries_number is not null,d.fseries_number,if(c.fseries_number is not null,c.fseries_number,b.fseries_number)))))) as fold_series_number,
    if(h.fchannel_name is not null,h.fchannel_name,if(g.fchannel_name is not null,g.fchannel_name,if(f.fchannel_name is not null,f.fchannel_name,if(e.fchannel_name is not null,e.fchannel_name,if(d.fchannel_name is not null,d.fchannel_name,if(c.fchannel_name is not null,c.fchannel_name,b.fchannel_name)))))) as fchannel_name,
    if(h.forder_num is not null,h.forder_num,if(g.forder_num is not null,g.forder_num,if(f.forder_num is not null,f.forder_num,if(e.forder_num is not null,e.forder_num,if(d.forder_num is not null,d.forder_num,if(c.forder_num is not null,c.forder_num,b.forder_num)))))) as forder_num,
    if(h.forder_id is not null,h.forder_id,if(g.forder_id is not null,g.forder_id,if(f.forder_id is not null,f.forder_id,if(e.forder_id is not null,e.forder_id,if(d.forder_id is not null,d.forder_id,if(c.forder_id is not null,c.forder_id,b.forder_id)))))) as forder_id,
    if(k.fclass_name is not null,k.fclass_name,if(b.fclass_name is not null,b.fclass_name,i.fclass_name)) as fclass_name,
    case when i.forder_platform=1 then "自有竞拍"
         when i.forder_platform=5 then "B端鱼市"
         when i.forder_platform=6 then "采货侠"
         when i.forder_platform=7 then "双向鱼市"
         when i.forder_platform=10 then "统货"
         when left(t.fserial_number,2)='BM' then "C端鱼市"
         when j.fmessage like "%优品%" then "优品"
         when j.fstock_no is not null then "线下"
    else null end as forder_platform,
    if(i.fsales_order_num is not null,i.fsales_order_num,z.fsales_order_num) as fsales_order_num,
    case when h.fsupply_partner=2 or g.fsupply_partner=2 or f.fsupply_partner=2 or e.fsupply_partner=2 or d.fsupply_partner=2 or c.fsupply_partner=2 or b.fsupply_partner=2 then "小站(自营)"
         when h.fsupply_partner=3 or g.fsupply_partner=3 or f.fsupply_partner=3 or e.fsupply_partner=3 or d.fsupply_partner=3 or c.fsupply_partner=3 or b.fsupply_partner=3 then "小站(加盟)" 
         when h.fsupply_partner=5 or g.fsupply_partner=5 or f.fsupply_partner=5 or e.fsupply_partner=5 or d.fsupply_partner=5 or c.fsupply_partner=5 or b.fsupply_partner=5 then "小豹哥" 
         when h.fsupply_partner=6 or g.fsupply_partner=6 or f.fsupply_partner=6 or e.fsupply_partner=6 or d.fsupply_partner=6 or c.fsupply_partner=6 or b.fsupply_partner=6 then "顺丰" 
         when h.fsupply_partner=0 or g.fsupply_partner=0 or f.fsupply_partner=0 or e.fsupply_partner=0 or d.fsupply_partner=0 or c.fsupply_partner=0 or b.fsupply_partner=0 then "回收宝" 
    else null end as "履约方",
    case when left(Fexpress_reality_code,2) like "%JD%" then "京东"
         when left(Fexpress_reality_code,2) like "%SF%" then "顺丰"
         when left(Fexpress_reality_code,3) like "%ZTO%" then "中通"
    else "其它" end as "快递类型"
from (
select
    a.fstock_order_sn,
    a.fcreate_time,
    upper(c.Fstock_no) as fserial_number,
    a.fexpress_reality_sn,
    a.Fexpress_reality_code,
    row_number() over(partition by c.Fstock_no order by a.fcreate_time desc)as num
from  drt.drt_my33312_hsb_sales_product_t_stock_order_express as a
left join drt.drt_my33312_hsb_sales_product_t_stock_order_saleout_detail as c on a.Fstock_order_sn = c.Fstock_order_sn
where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and a.fstock_owner!=3)t
left join dws.dws_hs_order_detail as b on t.fserial_number=b.fseries_number
left join drt.drt_my33310_recycle_t_xy_jimai_plus_order as z on b.forder_id=z.forder_id
left join dws.dws_hs_order_detail as c on b.fold_fseries_number=c.fseries_number
left join dws.dws_hs_order_detail as d on c.fold_fseries_number=d.fseries_number
left join dws.dws_hs_order_detail as e on d.fold_fseries_number=e.fseries_number
left join dws.dws_hs_order_detail as f on e.fold_fseries_number=f.fseries_number
left join dws.dws_hs_order_detail as g on f.fold_fseries_number=g.fseries_number
left join nt as h on t.fserial_number=h.fserial_no
left join sales as i on t.fserial_number=i.fseries_number
left join t_stock_order_saleout as j on t.fserial_number=j.Fstock_no
left join detect_record as k on t.fserial_number=k.fserial_number
where t.num=1
and t.fexpress_reality_sn is not null
union all
select                                      --回收&寄卖用户寄出对应物流信息
    a.fupdate_time as fcreate_time,
    null as fstock_order_sn,
    b.fseries_number as fserial_number,
    case when c.fchannel_name like "%帮卖%" and b.frecycle_type!=1 then "销售出库" else "入库" end as fype,
    upper(a.fchannel_id) as fexpress_reality_sn,
    case when left(b.fseries_number,3) like "%020%" or left(b.fseries_number,3) like "%010%" or left(b.fseries_number,3) like "%050%" then "验机" 
         when left(b.fseries_number,2) like "%BM%" then "寄卖"
         when left(b.fseries_number,2) like "%CG%" then "采购回收"
         when left(b.fseries_number,2) like "%YZ%" or left(b.fseries_number,2) like "%NT%" then "售后回收"
         when left(b.fseries_number,2) like "%BB%" then "B端帮卖"
    else "回收" end as "业务类型",
    case 
         when b.Frecycle_type=3
              and c.fchannel_name not in ("闲鱼到店","回收宝APP-2C","H5回收宝","微信公众号2","闲鱼小站闲鱼小程序","支付宝小程序") then "小站"
         when c.fproject_name='合作项目' and c.fchannel_name!='荣耀商城' and c.fchannel_name!='微博官网' and c.fchannel_name!='联想商城' then "合作"
         when c.fchannel_name='支付宝小程序' or c.fchannel_name='闲鱼同城小程序' then "合作"
         when left(b.fseries_number,3) like "%020%" or left(b.fseries_number,3) like "%010%" or left(b.fseries_number,3) like "%050%" then "验机"
         when c.fchannel_name='闲鱼寄卖plus'then "合作"
         when left(b.fseries_number,2) like "%XY%" or left(b.fseries_number,2) like "%YJ%" or left(b.fseries_number,2) like "%TM%" or left(b.fseries_number,2) like "%TY%" then "合作"
         when left(b.fseries_number,2) like "%CG%" then "采购回收"
         when left(b.fseries_number,2) like "%YZ%" or left(b.fseries_number,2) like "%NT%" then "售后回收"
         when left(b.fseries_number,2) like "%BB%" then "B端帮卖"
    else '自有' end as "流量来源",
    null as fold_series_number,
    c.fchannel_name,
    b.forder_num,
    b.forder_id,
    f.fname as fclass_name,
    null as forder_platform,
    null as fsales_order_num,
    case when b.fsupply_partner=2 then "小站(自营)"
         when b.fsupply_partner=3 then "小站(加盟)" 
         when b.fsupply_partner=5 then "小豹哥" 
         when b.fsupply_partner=6 then "顺丰" 
         when b.fsupply_partner=0 then "回收宝" 
    else null end as "履约方",
    case when d.flogistics_name like "%顺丰%" then "顺丰"
         when d.flogistics_name like "%中通%" then "中通"
         when d.flogistics_name like "%申通%" then "申通"
         when d.flogistics_name like "%圆通%" then "圆通"
    else d.flogistics_name end as "快递类型"
from drt.drt_my33310_recycle_t_logistics as a
left join drt.drt_my33310_recycle_t_order as b on a.flogistics_id=b.flogistics_id
left join drt.drt_my33310_recycle_t_product as e on b.fproduct_id=e.fproduct_id
left join drt.drt_my33310_recycle_t_pdt_class as f on e.fclass_id=f.fid
left join drt.drt_my33310_pub_server_channel_center_db_t_pid_info as c on b.fpid=c.fpid
left join drt.drt_my33310_recycle_t_logistics_channel as d on a.fchannel_type=d.flogistics_type
where to_date(a.fupdate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and b.ftest=0
and b.fseries_number is not null
and left(b.fseries_number,2) not like "%YZ%" and left(b.fseries_number,2) not like "%NT%"
union
select                                      --寄卖上门发货给买家的物流信息
    a.Fcreate_time as fcreate_time,
    null as fstock_order_sn,
    b.fseries_number as fserial_number,
    case when c.fchannel_name like "%帮卖%" and b.frecycle_type!=1 then "销售出库(小站寄卖)" else "入库" end as fype,
    upper(a.Fbuyer_receive_trackingno) as fexpress_reality_sn,
    case when left(b.fseries_number,3) like "%020%" or left(b.fseries_number,3) like "%010%" or left(b.fseries_number,3) like "%050%" then "验机" 
         when left(b.fseries_number,2) like "%BM%" then "寄卖"
         when left(b.fseries_number,2) like "%CG%" then "采购回收"
         when left(b.fseries_number,2) like "%YZ%" or left(b.fseries_number,2) like "%NT%" then "售后回收"
         when left(b.fseries_number,2) like "%BB%" then "B端帮卖"
    else "回收" end as "业务类型",
    case 
         when b.Frecycle_type=3
              and c.fchannel_name not in ("闲鱼到店","回收宝APP-2C","H5回收宝","微信公众号2","闲鱼小站闲鱼小程序","支付宝小程序") then "小站"
         when c.fproject_name='合作项目' and c.fchannel_name!='荣耀商城' and c.fchannel_name!='微博官网' and c.fchannel_name!='联想商城' then "合作"
         when c.fchannel_name='支付宝小程序' or c.fchannel_name='闲鱼同城小程序' then "合作"
         when left(b.fseries_number,3) like "%020%" or left(b.fseries_number,3) like "%010%" or left(b.fseries_number,3) like "%050%" then "验机"
         when c.fchannel_name='闲鱼寄卖plus'then "合作"
         when left(b.fseries_number,2) like "%XY%" or left(b.fseries_number,2) like "%YJ%" or left(b.fseries_number,2) like "%TM%" or left(b.fseries_number,2) like "%TY%" then "合作"
         when left(b.fseries_number,2) like "%CG%" then "采购回收"
         when left(b.fseries_number,2) like "%YZ%" or left(b.fseries_number,2) like "%NT%" then "售后回收"
         when left(b.fseries_number,2) like "%BB%" then "B端帮卖"
    else '自有' end as "流量来源",
    null as fold_series_number,
    c.fchannel_name,
    b.forder_num,
    b.forder_id,
    f.fname as fclass_name,
    null as forder_platform,
    null as fsales_order_num,
    case when b.fsupply_partner=2 then "小站(自营)"
         when b.fsupply_partner=3 then "小站(加盟)" 
         when b.fsupply_partner=5 then "小豹哥" 
         when b.fsupply_partner=6 then "顺丰" 
         when b.fsupply_partner=0 then "回收宝" 
    else null end as "履约方",
    "顺丰" as "快递类型"
from drt.drt_my33310_recycle_t_xy_jimai_plus_order as a
left join drt.drt_my33310_recycle_t_order as b on a.forder_id=b.forder_id
left join drt.drt_my33310_recycle_t_product as e on b.fproduct_id=e.fproduct_id
left join drt.drt_my33310_recycle_t_pdt_class as f on e.fclass_id=f.fid
left join drt.drt_my33310_pub_server_channel_center_db_t_pid_info as c on b.fpid=c.fpid
where to_date(a.Fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and b.frecycle_type in (2,3)
and b.ftest=0
and a.Fbuyer_receive_trackingno is not null 
and a.Fbuyer_receive_trackingno!=""
and b.fseries_number is not null
union all
select                       --验机入库对应物流信息
    a.fadd_time as fcreate_time,
    null as fstock_order_sn,
    a.Fbar_code as fserial_number,
    "验机入库" as ftype,
    upper(a.Flogistics_num) as fexpress_reality_sn,
    "验机" as "业务类型",
    "验机" as "流量来源",
    null as fold_series_number,
    "闲鱼验机" as fchannel_name,
    a.forder_no as forder_num,
    a.forder_id,
    b.fclass_name,
    null as forder_platform,
    null as fsales_order_num,
    "回收宝" as "履约方",
    case when left(a.Flogistics_num,2) like "%SF%" then "顺丰"
         when left(a.Flogistics_num,2) like "%JD%" then "京东"
    else "其它" end as "快递类型"
from drt.drt_my33310_xywms_t_parcel as a 
left join detect_record as b on a.Fbar_code=b.fserial_number
where to_date(fadd_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and Flogistics_num is not null
union all
select                       --验机出库对应物流信息
    a.fadd_time as fcreate_time,
    null as fstock_order_sn,
    a.Fbar_code as fserial_number,
    "验机出库" as ftype,
    upper(a.Flogistics_num) as fexpress_reality_sn,
    "验机" as "业务类型",
    "验机" as "流量来源",
    null as fold_series_number,
    "闲鱼验机" as fchannel_name,
    a.forder_no as forder_num,
    a.forder_id,
    b.fclass_name,
    null as forder_platform,
    null as fsales_order_num,
    "回收宝" as "履约方",
    case when a.Flogistics_name like "%中通%" then "中通"
         when a.Flogistics_name like "%其他%" then "其它"
    else a.Flogistics_name end as "快递类型"
from drt.drt_my33310_xywms_t_product_put as a 
left join detect_record as b on a.Fbar_code=b.fserial_number
where to_date(fadd_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and Flogistics_num is not null
union all 
select                            --售后退回对应物流信息
    from_unixtime(t.Fadd_time) as fcreate_time,
    null as fstock_order_sn,
    t.fproduct_code as fserial_number,
    "售后退回" as ftype,
    upper(t.flogistics_number) as fexpress_reality_sn,
    "售后回收" as "业务类型",
    case 
         when if(h.frecycle_type is not null,h.frecycle_type,if(g.frecycle_type is not null,g.frecycle_type,if(f.frecycle_type is not null,f.frecycle_type,if(e.frecycle_type is not null,e.frecycle_type,if(d.frecycle_type is not null,d.frecycle_type,if(c.frecycle_type is not null,c.frecycle_type,b.frecycle_type))))))=3
              and if(h.fchannel_name is not null,h.fchannel_name,if(g.fchannel_name is not null,g.fchannel_name,if(f.fchannel_name is not null,f.fchannel_name,if(e.fchannel_name is not null,e.fchannel_name,if(d.fchannel_name is not null,d.fchannel_name,if(c.fchannel_name is not null,c.fchannel_name,b.fchannel_name)))))) not in ("闲鱼到店","回收宝APP-2C","H5回收宝","微信公众号2","闲鱼小站闲鱼小程序","支付宝小程序") then "小站"
        when (b.fproject_name='合作项目' and b.fchannel_name!='荣耀商城' and b.fchannel_name!='微博官网' and b.fchannel_name!='联想商城') or (c.fproject_name='合作项目' and c.fchannel_name!='荣耀商城' and c.fchannel_name!='微博官网' and c.fchannel_name!='联想商城') or (d.fproject_name='合作项目' and d.fchannel_name!='荣耀商城' and d.fchannel_name!='微博官网' and d.fchannel_name!='联想商城')
     or (e.fproject_name='合作项目' and e.fchannel_name!='荣耀商城' and e.fchannel_name!='微博官网' and e.fchannel_name!='联想商城') or (f.fproject_name='合作项目' and f.fchannel_name!='荣耀商城' and f.fchannel_name!='微博官网' and f.fchannel_name!='联想商城') or (g.fproject_name='合作项目' and g.fchannel_name!='荣耀商城' and g.fchannel_name!='微博官网' and g.fchannel_name!='联想商城') or (h.fproject_name='合作项目' and h.fchannel_name!='荣耀商城' and h.fchannel_name!='微博官网' and h.fchannel_name!='联想商城') then "合作"
         when b.fchannel_name='支付宝小程序' or b.fchannel_name='闲鱼同城小程序' or c.fchannel_name='支付宝小程序' or c.fchannel_name='闲鱼同城小程序' or d.fchannel_name='支付宝小程序' or d.fchannel_name='闲鱼同城小程序'
          or e.fchannel_name='支付宝小程序' or e.fchannel_name='闲鱼同城小程序'  or f.fchannel_name='支付宝小程序' or f.fchannel_name='闲鱼同城小程序'  or g.fchannel_name='支付宝小程序' or g.fchannel_name='闲鱼同城小程序' or h.fchannel_name='支付宝小程序' or h.fchannel_name='闲鱼同城小程序' then "合作"
         when b.fchannel_name='闲鱼寄卖plus' or c.fchannel_name='闲鱼寄卖plus' or d.fchannel_name='闲鱼寄卖plus' or e.fchannel_name='闲鱼寄卖plus' or f.fchannel_name='闲鱼寄卖plus' or g.fchannel_name='闲鱼寄卖plus' or h.fchannel_name='闲鱼寄卖plus'then "合作"
         when left(t.fproduct_code,2) like "%XY%" or left(t.fproduct_code,2) like "%YJ%" or left(t.fproduct_code,2) like "%TM%" or left(t.fproduct_code,2) like "%TY%" then "合作"
         when left(t.fproduct_code,2) like "%CG%" then "采购回收"
         when left(t.fproduct_code,2) like "%YZ%" or left(t.fproduct_code,2) like "%NT%" then "售后回收"
         when left(t.fproduct_code,2) like "%BB%" then "B端帮卖"
    else '自有' end as "流量来源",
    if(h.fsrouce_serial_no is not null,h.fsrouce_serial_no,if(g.fseries_number is not null,g.fseries_number,if(f.fseries_number is not null,f.fseries_number,if(e.fseries_number is not null,e.fseries_number,if(d.fseries_number is not null,d.fseries_number,if(c.fseries_number is not null,c.fseries_number,b.fseries_number)))))) as fold_series_number,
    if(h.fchannel_name is not null,h.fchannel_name,if(g.fchannel_name is not null,g.fchannel_name,if(f.fchannel_name is not null,f.fchannel_name,if(e.fchannel_name is not null,e.fchannel_name,if(d.fchannel_name is not null,d.fchannel_name,if(c.fchannel_name is not null,c.fchannel_name,b.fchannel_name)))))) as fchannel_name,
    if(h.forder_num is not null,h.forder_num,if(g.forder_num is not null,g.forder_num,if(f.forder_num is not null,f.forder_num,if(e.forder_num is not null,e.forder_num,if(d.forder_num is not null,d.forder_num,if(c.forder_num is not null,c.forder_num,b.forder_num)))))) as forder_num,
    if(h.forder_id is not null,h.forder_id,if(g.forder_id is not null,g.forder_id,if(f.forder_id is not null,f.forder_id,if(e.forder_id is not null,e.forder_id,if(d.forder_id is not null,d.forder_id,if(c.forder_id is not null,c.forder_id,b.forder_id)))))) as forder_id,
    if(k.fclass_name is not null,k.fclass_name,if(b.fclass_name is not null,b.fclass_name,i.fclass_name)) as fclass_name,
    case when i.forder_platform=1 then "自有竞拍"
         when i.forder_platform=5 then "B端鱼市"
         when i.forder_platform=6 then "采货侠"
         when i.forder_platform=7 then "双向鱼市"
         when i.forder_platform=10 then "统货"
         when left(if(h.fsrouce_serial_no is not null,h.fsrouce_serial_no,if(g.fold_fseries_number is not null,g.fold_fseries_number,if(f.fold_fseries_number is not null,f.fold_fseries_number,if(e.fold_fseries_number is not null,e.fold_fseries_number,if(d.fold_fseries_number is not null,d.fold_fseries_number,if(c.fold_fseries_number is not null,c.fold_fseries_number,b.fold_fseries_number)))))),2)='BM' then "C端鱼市"
         when j.fmessage like "%优品%" then "优品"
         when j.fstock_no is not null then "线下"
    else null end as forder_platform,
    null as fsales_order_num,
    case when h.fsupply_partner=2 or g.fsupply_partner=2 or f.fsupply_partner=2 or e.fsupply_partner=2 or d.fsupply_partner=2 or c.fsupply_partner=2 or b.fsupply_partner=2 then "小站(自营)"
         when h.fsupply_partner=3 or g.fsupply_partner=3 or f.fsupply_partner=3 or e.fsupply_partner=3 or d.fsupply_partner=3 or c.fsupply_partner=3 or b.fsupply_partner=3 then "小站(加盟)" 
         when h.fsupply_partner=5 or g.fsupply_partner=5 or f.fsupply_partner=5 or e.fsupply_partner=5 or d.fsupply_partner=5 or c.fsupply_partner=5 or b.fsupply_partner=5 then "小豹哥" 
         when h.fsupply_partner=6 or g.fsupply_partner=6 or f.fsupply_partner=6 or e.fsupply_partner=6 or d.fsupply_partner=6 or c.fsupply_partner=6 or b.fsupply_partner=6 then "顺丰" 
         when h.fsupply_partner=0 or g.fsupply_partner=0 or f.fsupply_partner=0 or e.fsupply_partner=0 or d.fsupply_partner=0 or c.fsupply_partner=0 or b.fsupply_partner=0 then "回收宝" 
    else null end as "履约方",
    case when t.Flogistics_name like "%顺丰%" then "顺丰"
         when t.Flogistics_name like "%圆通%" then "圆通"
         when t.Flogistics_name like "%中通%" then "中通"
         when t.Flogistics_name like "%申通%" then "申通"
         when t.Flogistics_name like "%其他%" then "其它"
    else t.Flogistics_name end as "快递类型"
from drt.drt_my33310_hsb_wms_t_sh_receive_order as t
left join dws.dws_hs_order_detail as b on t.fproduct_code=b.fseries_number
left join dws.dws_hs_order_detail as c on b.fold_fseries_number=c.fseries_number
left join dws.dws_hs_order_detail as d on c.fold_fseries_number=d.fseries_number
left join dws.dws_hs_order_detail as e on d.fold_fseries_number=e.fseries_number
left join dws.dws_hs_order_detail as f on e.fold_fseries_number=f.fseries_number
left join dws.dws_hs_order_detail as g on f.fold_fseries_number=g.fseries_number
left join nt as h on t.fproduct_code=h.fserial_no
left join detect_record as k on t.fproduct_code=k.fserial_number
left join sales as i on if(h.fsrouce_serial_no is not null,h.fsrouce_serial_no,if(g.fold_fseries_number is not null,g.fold_fseries_number,if(f.fold_fseries_number is not null,f.fold_fseries_number,if(e.fold_fseries_number is not null,e.fold_fseries_number,if(d.fold_fseries_number is not null,d.fold_fseries_number,if(c.fold_fseries_number is not null,c.fold_fseries_number,b.fold_fseries_number))))))=i.fseries_number
left join t_stock_order_saleout as j on if(h.fsrouce_serial_no is not null,h.fsrouce_serial_no,if(g.fold_fseries_number is not null,g.fold_fseries_number,if(f.fold_fseries_number is not null,f.fold_fseries_number,if(e.fold_fseries_number is not null,e.fold_fseries_number,if(d.fold_fseries_number is not null,d.fold_fseries_number,if(c.fold_fseries_number is not null,c.fold_fseries_number,b.fold_fseries_number))))))=j.Fstock_no
where to_date(from_unixtime(Fadd_time))>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and forder_system_id=4
union all 
select                            --采货侠和新售后条码退回对应物流信息
    t.fcreate_time,
    null as fstock_order_sn,
    t.fproduct_code as fserial_number,
    "售后退回" as ftype,
    upper(t.flogistics_number) as fexpress_reality_sn,
    "售后回收" as "业务类型",
    case 
         when if(h.frecycle_type is not null,h.frecycle_type,if(g.frecycle_type is not null,g.frecycle_type,if(f.frecycle_type is not null,f.frecycle_type,if(e.frecycle_type is not null,e.frecycle_type,if(d.frecycle_type is not null,d.frecycle_type,if(c.frecycle_type is not null,c.frecycle_type,b.frecycle_type))))))=3
              and if(h.fchannel_name is not null,h.fchannel_name,if(g.fchannel_name is not null,g.fchannel_name,if(f.fchannel_name is not null,f.fchannel_name,if(e.fchannel_name is not null,e.fchannel_name,if(d.fchannel_name is not null,d.fchannel_name,if(c.fchannel_name is not null,c.fchannel_name,b.fchannel_name)))))) not in ("闲鱼到店","回收宝APP-2C","H5回收宝","微信公众号2","闲鱼小站闲鱼小程序","支付宝小程序") then "小站"
        when (b.fproject_name='合作项目' and b.fchannel_name!='荣耀商城' and b.fchannel_name!='微博官网' and b.fchannel_name!='联想商城') or (c.fproject_name='合作项目' and c.fchannel_name!='荣耀商城' and c.fchannel_name!='微博官网' and c.fchannel_name!='联想商城') or (d.fproject_name='合作项目' and d.fchannel_name!='荣耀商城' and d.fchannel_name!='微博官网' and d.fchannel_name!='联想商城')
     or (e.fproject_name='合作项目' and e.fchannel_name!='荣耀商城' and e.fchannel_name!='微博官网' and e.fchannel_name!='联想商城') or (f.fproject_name='合作项目' and f.fchannel_name!='荣耀商城' and f.fchannel_name!='微博官网' and f.fchannel_name!='联想商城') or (g.fproject_name='合作项目' and g.fchannel_name!='荣耀商城' and g.fchannel_name!='微博官网' and g.fchannel_name!='联想商城') or (h.fproject_name='合作项目' and h.fchannel_name!='荣耀商城' and h.fchannel_name!='微博官网' and h.fchannel_name!='联想商城') then "合作"
         when b.fchannel_name='支付宝小程序' or b.fchannel_name='闲鱼同城小程序' or c.fchannel_name='支付宝小程序' or c.fchannel_name='闲鱼同城小程序' or d.fchannel_name='支付宝小程序' or d.fchannel_name='闲鱼同城小程序'
          or e.fchannel_name='支付宝小程序' or e.fchannel_name='闲鱼同城小程序'  or f.fchannel_name='支付宝小程序' or f.fchannel_name='闲鱼同城小程序'  or g.fchannel_name='支付宝小程序' or g.fchannel_name='闲鱼同城小程序' or h.fchannel_name='支付宝小程序' or h.fchannel_name='闲鱼同城小程序' then "合作"
         when b.fchannel_name='闲鱼寄卖plus' or c.fchannel_name='闲鱼寄卖plus' or d.fchannel_name='闲鱼寄卖plus' or e.fchannel_name='闲鱼寄卖plus' or f.fchannel_name='闲鱼寄卖plus' or g.fchannel_name='闲鱼寄卖plus' or h.fchannel_name='闲鱼寄卖plus'then "合作"
         when left(t.fproduct_code,2) like "%XY%" or left(t.fproduct_code,2) like "%YJ%" or left(t.fproduct_code,2) like "%TM%" or left(t.fproduct_code,2) like "%TY%" then "合作"
         when left(t.fproduct_code,2) like "%CG%" then "采购回收"
         when left(t.fproduct_code,2) like "%YZ%" or left(t.fproduct_code,2) like "%NT%" then "售后回收"
         when left(t.fproduct_code,2) like "%BB%" then "B端帮卖"
    else '自有' end as "流量来源",
    case when t.fold_series_number is not null then t.fold_series_number else 
    if(h.fsrouce_serial_no is not null,h.fsrouce_serial_no,if(g.fseries_number is not null,g.fseries_number,if(f.fseries_number is not null,f.fseries_number,if(e.fseries_number is not null,e.fseries_number,if(d.fseries_number is not null,d.fseries_number,if(c.fseries_number is not null,c.fseries_number,b.fseries_number)))))) end as fold_series_number,
    case when left(t.fold_series_number,2) in ('BG') then "商家自检" 
         when left(t.fold_series_number,2) in ('TS') then "采货侠"else  
    if(h.fchannel_name is not null,h.fchannel_name,if(g.fchannel_name is not null,g.fchannel_name,if(f.fchannel_name is not null,f.fchannel_name,if(e.fchannel_name is not null,e.fchannel_name,if(d.fchannel_name is not null,d.fchannel_name,if(c.fchannel_name is not null,c.fchannel_name,b.fchannel_name)))))) end as fchannel_name,
    if(h.forder_num is not null,h.forder_num,if(g.forder_num is not null,g.forder_num,if(f.forder_num is not null,f.forder_num,if(e.forder_num is not null,e.forder_num,if(d.forder_num is not null,d.forder_num,if(c.forder_num is not null,c.forder_num,b.forder_num)))))) as forder_num,
    if(h.forder_id is not null,h.forder_id,if(g.forder_id is not null,g.forder_id,if(f.forder_id is not null,f.forder_id,if(e.forder_id is not null,e.forder_id,if(d.forder_id is not null,d.forder_id,if(c.forder_id is not null,c.forder_id,b.forder_id)))))) as forder_id,
    if(k.fclass_name is not null,k.fclass_name,b.fclass_name) as fclass_name,
    case when i.forder_platform=1 then "自有竞拍"
         when i.forder_platform=5 then "B端鱼市"
         when i.forder_platform=6 then "采货侠"
         when i.forder_platform=7 then "双向鱼市"
         when i.forder_platform=10 then "统货"
         when left(if(h.fsrouce_serial_no is not null,h.fsrouce_serial_no,if(g.fold_fseries_number is not null,g.fold_fseries_number,if(f.fold_fseries_number is not null,f.fold_fseries_number,if(e.fold_fseries_number is not null,e.fold_fseries_number,if(d.fold_fseries_number is not null,d.fold_fseries_number,if(c.fold_fseries_number is not null,c.fold_fseries_number,b.fold_fseries_number)))))),2)='BM' then "C端鱼市"
         when j.fmessage like "%优品%" then "优品"
         when j.fstock_no is not null then "线下"
    else null end as forder_platform,
    null as fsales_order_num,
    case when h.fsupply_partner=2 or g.fsupply_partner=2 or f.fsupply_partner=2 or e.fsupply_partner=2 or d.fsupply_partner=2 or c.fsupply_partner=2 or b.fsupply_partner=2 then "小站(自营)"
         when h.fsupply_partner=3 or g.fsupply_partner=3 or f.fsupply_partner=3 or e.fsupply_partner=3 or d.fsupply_partner=3 or c.fsupply_partner=3 or b.fsupply_partner=3 then "小站(加盟)" 
         when h.fsupply_partner=5 or g.fsupply_partner=5 or f.fsupply_partner=5 or e.fsupply_partner=5 or d.fsupply_partner=5 or c.fsupply_partner=5 or b.fsupply_partner=5 then "小豹哥" 
         when h.fsupply_partner=6 or g.fsupply_partner=6 or f.fsupply_partner=6 or e.fsupply_partner=6 or d.fsupply_partner=6 or c.fsupply_partner=6 or b.fsupply_partner=6 then "顺丰" 
         when h.fsupply_partner=0 or g.fsupply_partner=0 or f.fsupply_partner=0 or e.fsupply_partner=0 or d.fsupply_partner=0 or c.fsupply_partner=0 or b.fsupply_partner=0 then "回收宝" 
    else null end as "履约方",
    case when t.flogistics_number like "%SF%" then "顺丰"
         when t.flogistics_number like "%JD%" then "京东"
    else "其它" end as "快递类型"
from (
select 
    fcreate_time,
    Fserial_no as fproduct_code,
    Fexpress_sn as flogistics_number,
    null as fold_series_number
from drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn
where Fexpress_sn is not null and Fexpress_sn!=""
and Fserial_no is not null and Fserial_no!=""
union all
SELECT 
r.fauto_create_time as fcreate_time,
o.Fseries_number as fproduct_code,
r.ftracking_number	as flogistics_number,
ifnull(a.Fsales_series_number,a1.Fsales_series_number) as fold_series_number
FROM drt.drt_my33310_recycle_t_order_logistics_record r							
left join drt.drt_my33310_recycle_t_order o on o.Forder_id = r.Forder_id					
left join drt.drt_my33310_recycle_t_after_sales_order_info a1 on a1.Fafter_sales_order_id = r.Forder_id
left join drt.drt_my33310_recycle_t_aftersales_v2_order_info a on a.Faftersales_order_id = r.Forder_id 
where left(ifnull(a.Fsales_series_number,a1.Fsales_series_number),2) in ('BG','TS','JM')
) as t  
left join dws.dws_hs_order_detail as b on t.fproduct_code=b.fseries_number
left join dws.dws_hs_order_detail as c on b.fold_fseries_number=c.fseries_number
left join dws.dws_hs_order_detail as d on c.fold_fseries_number=d.fseries_number
left join dws.dws_hs_order_detail as e on d.fold_fseries_number=e.fseries_number
left join dws.dws_hs_order_detail as f on e.fold_fseries_number=f.fseries_number
left join dws.dws_hs_order_detail as g on f.fold_fseries_number=g.fseries_number
left join nt as h on t.fproduct_code=h.fserial_no
left join detect_record as k on t.fproduct_code=k.fserial_number
left join sales as i on if(h.fsrouce_serial_no is not null,h.fsrouce_serial_no,if(g.fold_fseries_number is not null,g.fold_fseries_number,if(f.fold_fseries_number is not null,f.fold_fseries_number,if(e.fold_fseries_number is not null,e.fold_fseries_number,if(d.fold_fseries_number is not null,d.fold_fseries_number,if(c.fold_fseries_number is not null,c.fold_fseries_number,if(b.fold_fseries_number is not null,b.fold_fseries_number,t.fold_series_number)))))))=i.fseries_number
left join t_stock_order_saleout as j on if(h.fsrouce_serial_no is not null,h.fsrouce_serial_no,if(g.fold_fseries_number is not null,g.fold_fseries_number,if(f.fold_fseries_number is not null,f.fold_fseries_number,if(e.fold_fseries_number is not null,e.fold_fseries_number,if(d.fold_fseries_number is not null,d.fold_fseries_number,if(c.fold_fseries_number is not null,c.fold_fseries_number,if(b.fold_fseries_number is not null,b.fold_fseries_number,t.fold_series_number)))))))=j.Fstock_no
where to_date(t.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))



