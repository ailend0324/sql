with nt as (
select 
    a.fserial_no,
    a.fsrouce_serial_no,
    b.frecycle_type,
    b.fchannel_name,
    b.fproject_name,
    b.fsupply_partner,
    b.fshop_name
from drt.drt_my33312_hsb_sales_product_t_pm_local_create_sn as a
left join dws.dws_hs_order_detail as b on upper(a.fsrouce_serial_no)=b.fseries_number
)
select 
    t.fend_time as fdetect_time,
    fserial_number,
    if(h.fsrouce_serial_no is not null,h.fsrouce_serial_no,if(g.fold_fseries_number is not null,g.fold_fseries_number,if(f.fold_fseries_number is not null,f.fold_fseries_number,if(e.fold_fseries_number is not null,e.fold_fseries_number,if(d.fold_fseries_number is not null,d.fold_fseries_number,if(c.fold_fseries_number is not null,c.fold_fseries_number,b.fold_fseries_number)))))) as fold_fseries_number,
    case 
        when t.freal_name="段武" and to_date(fend_time) BETWEEN  '2022-10-01' and '2022-10-31' then "东莞仓"       
    	when t.freal_name="刘维贝" then "深圳仓"
    	when (t.Fwarehouse_code="12" or left(fserial_number,3) like "%050%") and to_date(fend_time) not BETWEEN  '2022-10-01' and '2022-10-31' then "东莞仓"
    	when left(fserial_number,3) like "%020%" or right(left(fserial_number,6),2)="16" then "杭州仓" 	
    else "深圳仓" end as "所在地",
    case when left(fserial_number,3) like "%020%" or left(fserial_number,3) like "%010%" or left(fserial_number,3) like "%050%" then "验机" 
         when left(fserial_number,2) like "%BM%" then "寄卖"
         when left(fserial_number,2) like "%CG%" then "采购回收"
         when left(fserial_number,2) like "%YZ%" or left(fserial_number,2) like "%NT%" then "售后回收"
         when left(fserial_number,2) like "%BB%" then "B端帮卖"
    else "回收" end as "业务类型",
    case 
         when if(h.frecycle_type is not null,h.frecycle_type,if(g.frecycle_type is not null,g.frecycle_type,if(f.frecycle_type is not null,f.frecycle_type,if(e.frecycle_type is not null,e.frecycle_type,if(d.frecycle_type is not null,d.frecycle_type,if(c.frecycle_type is not null,c.frecycle_type,b.frecycle_type))))))=3
              and if(h.fchannel_name is not null,h.fchannel_name,if(g.fchannel_name is not null,g.fchannel_name,if(f.fchannel_name is not null,f.fchannel_name,if(e.fchannel_name is not null,e.fchannel_name,if(d.fchannel_name is not null,d.fchannel_name,if(c.fchannel_name is not null,c.fchannel_name,b.fchannel_name)))))) not in ("闲鱼到店","回收宝APP-2C","H5回收宝","微信公众号2","闲鱼小站闲鱼小程序","支付宝小程序") then "小站"
         when (b.fproject_name='合作项目' and b.fchannel_name!='荣耀商城' and b.fchannel_name!='微博官网' and b.fchannel_name!='联想商城') or (c.fproject_name='合作项目' and c.fchannel_name!='荣耀商城' and c.fchannel_name!='微博官网' and c.fchannel_name!='联想商城') or (d.fproject_name='合作项目' and d.fchannel_name!='荣耀商城' and d.fchannel_name!='微博官网' and d.fchannel_name!='联想商城')
     or (e.fproject_name='合作项目' and e.fchannel_name!='荣耀商城' and e.fchannel_name!='微博官网' and e.fchannel_name!='联想商城') or (f.fproject_name='合作项目' and f.fchannel_name!='荣耀商城' and f.fchannel_name!='微博官网' and f.fchannel_name!='联想商城') or (g.fproject_name='合作项目' and g.fchannel_name!='荣耀商城' and g.fchannel_name!='微博官网' and g.fchannel_name!='联想商城') or (h.fproject_name='合作项目' and h.fchannel_name!='荣耀商城' and h.fchannel_name!='微博官网' and h.fchannel_name!='联想商城') then "合作"
         when b.fchannel_name='支付宝小程序' or b.fchannel_name='闲鱼同城小程序' or c.fchannel_name='支付宝小程序' or c.fchannel_name='闲鱼同城小程序' or d.fchannel_name='支付宝小程序' or d.fchannel_name='闲鱼同城小程序'
          or e.fchannel_name='支付宝小程序' or e.fchannel_name='闲鱼同城小程序'  or f.fchannel_name='支付宝小程序' or f.fchannel_name='闲鱼同城小程序'  or g.fchannel_name='支付宝小程序' or g.fchannel_name='闲鱼同城小程序' or h.fchannel_name='支付宝小程序' or h.fchannel_name='闲鱼同城小程序' then "合作"
         when left(fserial_number,3) like "%020%" or left(fserial_number,3) like "%010%" or left(fserial_number,3) like "%050%" then "验机"
         when b.fchannel_name='闲鱼寄卖plus' or c.fchannel_name='闲鱼寄卖plus' or d.fchannel_name='闲鱼寄卖plus' or e.fchannel_name='闲鱼寄卖plus' or f.fchannel_name='闲鱼寄卖plus' or g.fchannel_name='闲鱼寄卖plus' or h.fchannel_name='闲鱼寄卖plus'then "合作"
         when left(fserial_number,2) like "%XY%" or left(fserial_number,2) like "%YJ%" or left(fserial_number,2) like "%TM%" or left(fserial_number,2) like "%TY%" then "合作"
         when left(fserial_number,2) like "%CG%" then "采购回收"
         when left(fserial_number,2) like "%YZ%" or left(fserial_number,2) like "%NT%" then "售后回收"
         when left(fserial_number,2) like "%BB%" then "B端帮卖"
    else '自有' end as "流量来源",
    case when b.frecycle_type=1 or c.frecycle_type=1 or d.frecycle_type=1 or e.frecycle_type=1 or f.frecycle_type=1 or g.frecycle_type=1 or h.frecycle_type=1 then "邮寄"
    when left(fserial_number,3) like "%020%" or left(fserial_number,3) like "%010%" or left(fserial_number,3) like "%050%" then "邮寄"
    when left(fserial_number,2) like "%BB%" then '邮寄'
         when (b.frecycle_type=2 and b.fsupply_partner=2) or (c.frecycle_type=2 and c.fsupply_partner=2)  or (d.frecycle_type=2 and d.fsupply_partner=2)
          or (e.frecycle_type=2 and e.fsupply_partner=2)  or (f.frecycle_type=2 and f.fsupply_partner=2)  or (g.frecycle_type=2 and g.fsupply_partner=2) or (h.frecycle_type=2 and h.fsupply_partner=2) then '上门+自营' 
         when (b.frecycle_type=2 and b.fsupply_partner=3) or (c.frecycle_type=2 and c.fsupply_partner=3) or (d.frecycle_type=2 and d.fsupply_partner=3)
         or (e.frecycle_type=2 and e.fsupply_partner=3) or (f.frecycle_type=2 and f.fsupply_partner=3) or (g.frecycle_type=2 and g.fsupply_partner=3) or (h.frecycle_type=2 and h.fsupply_partner=3) then '上门+加盟'
         when (b.frecycle_type=3 and b.fsupply_partner=2) or (c.frecycle_type=3 and c.fsupply_partner=2) or (d.frecycle_type=3 and d.fsupply_partner=2)
         or (e.frecycle_type=3 and e.fsupply_partner=2) or (f.frecycle_type=3 and f.fsupply_partner=2) or (g.frecycle_type=3 and g.fsupply_partner=2) or (h.frecycle_type=3 and h.fsupply_partner=2) then '到店+自营'
         when (b.frecycle_type=3 and b.fsupply_partner=3) or (c.frecycle_type=3 and c.fsupply_partner=3) or (d.frecycle_type=3 and d.fsupply_partner=3)
         or (e.frecycle_type=3 and e.fsupply_partner=3) or (f.frecycle_type=3 and f.fsupply_partner=3) or (g.frecycle_type=3 and g.fsupply_partner=3) or (h.frecycle_type=3 and h.fsupply_partner=3) then '到店+加盟'
    else "邮寄" end as "履约方式",
    t.fclass_name, 
    t.fproduct_name, 
    t.freal_name as "检测工程师",
    if(h.fchannel_name is not null,h.fchannel_name,if(g.fchannel_name is not null,g.fchannel_name,if(f.fchannel_name is not null,f.fchannel_name,if(e.fchannel_name is not null,e.fchannel_name,if(d.fchannel_name is not null,d.fchannel_name,if(c.fchannel_name is not null,c.fchannel_name,b.fchannel_name)))))) as "渠道名称",
    if(h.fshop_name is not null,h.fshop_name,if(g.fshop_name is not null,g.fshop_name,if(f.fshop_name is not null,f.fshop_name,if(e.fshop_name is not null,e.fshop_name,if(d.fshop_name is not null,d.fshop_name,if(c.fshop_name is not null,c.fshop_name,b.fshop_name)))))) as "门店名称",
    case 
      when t.Fdet_tpl = 0 then '竞拍检测'
      when t.Fdet_tpl = 1 then '大检测'
      when t.Fdet_tpl = 2 then '竞拍检测'
      when t.Fdet_tpl = 6 then '闲鱼寄卖plus'
      when t.Fdet_tpl = 7 then '竞拍检测'
      when t.Fdet_tpl = 4 then '销售检测'
    else '其他' end as "检测模板",
    "一检" as "检测类型",
    case when t.freal_name="郑庆刚" and (LEFT(fserial_number,2)="XZ" or LEFT(fserial_number,2)="JM") then 0 
    	when t.fproduct_name not like "%公益%" then 1 else 0 end as "检测台数(不含公益机)"
from (  
        select 
            *
        from (
        select 
            a.*,
            b.fposition_id,
            row_number()over(partition by fserial_number order by fend_time desc) as num
        from drt.drt_my33310_detection_t_detect_record as a
        left join drt.drt_my33310_amcdb_t_user as b on a.freal_name=b.freal_name
        where fis_deleted=0
      	and freport_type=0
    	and fverdict<>"测试单"
        and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),900))
        and fdet_type=0
        and b.fposition_id not in (129,305,64,151)
        and b.fdepartment_id not in (52,55,56)
        and a.freal_name not in ('张晓梦','李栋','徐晶','于炉烨','黄宇','丁雪兵','陈冬凡','李振文'))a 
        where num=1) t
left join dws.dws_hs_order_detail as b on t.fserial_number=b.fseries_number
left join dws.dws_hs_order_detail as c on b.fold_fseries_number=c.fseries_number
left join dws.dws_hs_order_detail as d on c.fold_fseries_number=d.fseries_number
left join dws.dws_hs_order_detail as e on d.fold_fseries_number=e.fseries_number
left join dws.dws_hs_order_detail as f on e.fold_fseries_number=f.fseries_number
left join dws.dws_hs_order_detail as g on f.fold_fseries_number=g.fseries_number
left join nt as h on t.fserial_number=h.fserial_no
where to_date(fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),900))
union all 
select 
    t.fend_time as fdetect_time,
    fserial_number,
    if(h.fsrouce_serial_no is not null,h.fsrouce_serial_no,if(g.fold_fseries_number is not null,g.fold_fseries_number,if(f.fold_fseries_number is not null,f.fold_fseries_number,if(e.fold_fseries_number is not null,e.fold_fseries_number,if(d.fold_fseries_number is not null,d.fold_fseries_number,if(c.fold_fseries_number is not null,c.fold_fseries_number,b.fold_fseries_number)))))) as fold_fseries_number,
    case 
    	when t.Fwarehouse_code="12" or left(fserial_number,3) like "%050%" then "东莞仓"
    	when left(fserial_number,3) like "%020%" or right(left(fserial_number,6),2)="16" then "杭州仓" 	
    else "深圳仓" end as "所在地",
    case when left(fserial_number,3) like "%020%" or left(fserial_number,3) like "%010%" or left(fserial_number,3) like "%050%" then "验机" 
         when left(fserial_number,2) like "%BM%" then "寄卖"
         when left(fserial_number,2) like "%CG%" then "采购回收"
         when left(fserial_number,2) like "%YZ%" or left(fserial_number,2) like "%NT%" then "售后回收"
         when left(fserial_number,2) like "%BB%" then "B端帮卖"
    else "回收" end as "业务类型",
    case    
         when if(h.frecycle_type is not null,h.frecycle_type,if(g.frecycle_type is not null,g.frecycle_type,if(f.frecycle_type is not null,f.frecycle_type,if(e.frecycle_type is not null,e.frecycle_type,if(d.frecycle_type is not null,d.frecycle_type,if(c.frecycle_type is not null,c.frecycle_type,b.frecycle_type))))))=3
              and if(h.fchannel_name is not null,h.fchannel_name,if(g.fchannel_name is not null,g.fchannel_name,if(f.fchannel_name is not null,f.fchannel_name,if(e.fchannel_name is not null,e.fchannel_name,if(d.fchannel_name is not null,d.fchannel_name,if(c.fchannel_name is not null,c.fchannel_name,b.fchannel_name)))))) not in ("闲鱼到店","回收宝APP-2C","H5回收宝","微信公众号2","闲鱼小站闲鱼小程序","支付宝小程序") then "小站"
        when (b.fproject_name='合作项目' and b.fchannel_name!='荣耀商城' and b.fchannel_name!='微博官网' and b.fchannel_name!='联想商城') or (c.fproject_name='合作项目' and c.fchannel_name!='荣耀商城' and c.fchannel_name!='微博官网' and c.fchannel_name!='联想商城') or (d.fproject_name='合作项目' and d.fchannel_name!='荣耀商城' and d.fchannel_name!='微博官网' and d.fchannel_name!='联想商城')
     or (e.fproject_name='合作项目' and e.fchannel_name!='荣耀商城' and e.fchannel_name!='微博官网' and e.fchannel_name!='联想商城') or (f.fproject_name='合作项目' and f.fchannel_name!='荣耀商城' and f.fchannel_name!='微博官网' and f.fchannel_name!='联想商城') or (g.fproject_name='合作项目' and g.fchannel_name!='荣耀商城' and g.fchannel_name!='微博官网' and g.fchannel_name!='联想商城') or (h.fproject_name='合作项目' and h.fchannel_name!='荣耀商城' and h.fchannel_name!='微博官网' and h.fchannel_name!='联想商城') then "合作"
         when b.fchannel_name='支付宝小程序' or b.fchannel_name='闲鱼同城小程序' or c.fchannel_name='支付宝小程序' or c.fchannel_name='闲鱼同城小程序' or d.fchannel_name='支付宝小程序' or d.fchannel_name='闲鱼同城小程序'
          or e.fchannel_name='支付宝小程序' or e.fchannel_name='闲鱼同城小程序'  or f.fchannel_name='支付宝小程序' or f.fchannel_name='闲鱼同城小程序'  or g.fchannel_name='支付宝小程序' or g.fchannel_name='闲鱼同城小程序' or h.fchannel_name='支付宝小程序' or h.fchannel_name='闲鱼同城小程序' then "合作"
         when left(fserial_number,3) like "%020%" or left(fserial_number,3) like "%010%" or left(fserial_number,3) like "%050%" then "验机"
         when b.fchannel_name='闲鱼寄卖plus' or c.fchannel_name='闲鱼寄卖plus' or d.fchannel_name='闲鱼寄卖plus' or e.fchannel_name='闲鱼寄卖plus' or f.fchannel_name='闲鱼寄卖plus' or g.fchannel_name='闲鱼寄卖plus' or h.fchannel_name='闲鱼寄卖plus'then "合作"
         when left(fserial_number,2) like "%XY%" or left(fserial_number,2) like "%YJ%" or left(fserial_number,2) like "%TM%" or left(fserial_number,2) like "%TY%" then "合作"
         when left(fserial_number,2) like "%CG%" then "采购回收"
         when left(fserial_number,2) like "%YZ%" or left(fserial_number,2) like "%NT%" then "售后回收"
         when left(fserial_number,2) like "%BB%" then "B端帮卖"
    else '自有' end as "流量来源",
    case when b.frecycle_type=1 or c.frecycle_type=1 or d.frecycle_type=1 or e.frecycle_type=1 or f.frecycle_type=1 or g.frecycle_type=1 or h.frecycle_type=1 then "邮寄"
    when left(fserial_number,3) like "%020%" or left(fserial_number,3) like "%010%" or left(fserial_number,3) like "%050%" then "邮寄"
    when left(fserial_number,2) like "%BB%" then '邮寄'
         when (b.frecycle_type=2 and b.fsupply_partner=2) or (c.frecycle_type=2 and c.fsupply_partner=2)  or (d.frecycle_type=2 and d.fsupply_partner=2)
          or (e.frecycle_type=2 and e.fsupply_partner=2)  or (f.frecycle_type=2 and f.fsupply_partner=2)  or (g.frecycle_type=2 and g.fsupply_partner=2) or (h.frecycle_type=2 and h.fsupply_partner=2) then '上门+自营' 
         when (b.frecycle_type=2 and b.fsupply_partner=3) or (c.frecycle_type=2 and c.fsupply_partner=3) or (d.frecycle_type=2 and d.fsupply_partner=3)
         or (e.frecycle_type=2 and e.fsupply_partner=3) or (f.frecycle_type=2 and f.fsupply_partner=3) or (g.frecycle_type=2 and g.fsupply_partner=3) or (h.frecycle_type=2 and h.fsupply_partner=3) then '上门+加盟'
         when (b.frecycle_type=3 and b.fsupply_partner=2) or (c.frecycle_type=3 and c.fsupply_partner=2) or (d.frecycle_type=3 and d.fsupply_partner=2)
         or (e.frecycle_type=3 and e.fsupply_partner=2) or (f.frecycle_type=3 and f.fsupply_partner=2) or (g.frecycle_type=3 and g.fsupply_partner=2) or (h.frecycle_type=3 and h.fsupply_partner=2) then '到店+自营'
         when (b.frecycle_type=3 and b.fsupply_partner=3) or (c.frecycle_type=3 and c.fsupply_partner=3) or (d.frecycle_type=3 and d.fsupply_partner=3)
         or (e.frecycle_type=3 and e.fsupply_partner=3) or (f.frecycle_type=3 and f.fsupply_partner=3) or (g.frecycle_type=3 and g.fsupply_partner=3) or (h.frecycle_type=3 and h.fsupply_partner=3) then '到店+加盟'
    else "邮寄" end as "履约方式",
    t.fclass_name, 
    t.fproduct_name, 
    t.freal_name as "检测工程师",
    if(h.fchannel_name is not null,h.fchannel_name,if(g.fchannel_name is not null,g.fchannel_name,if(f.fchannel_name is not null,f.fchannel_name,if(e.fchannel_name is not null,e.fchannel_name,if(d.fchannel_name is not null,d.fchannel_name,if(c.fchannel_name is not null,c.fchannel_name,b.fchannel_name)))))) as "渠道名称",
    if(h.fshop_name is not null,h.fshop_name,if(g.fshop_name is not null,g.fshop_name,if(f.fshop_name is not null,f.fshop_name,if(e.fshop_name is not null,e.fshop_name,if(d.fshop_name is not null,d.fshop_name,if(c.fshop_name is not null,c.fshop_name,b.fshop_name)))))) as "门店名称",
    case 
      when t.Fdet_tpl = 0 then '竞拍检测'
      when t.Fdet_tpl = 1 then '大检测'
      when t.Fdet_tpl = 2 then '竞拍检测'
      when t.Fdet_tpl = 6 then '闲鱼寄卖plus'
      when t.Fdet_tpl = 7 then '竞拍检测'
      when t.Fdet_tpl = 4 then '销售检测'
    else '其他' end as "检测模板",
    "抽检" as "检测类型",
    case when t.freal_name="郑庆刚" and (LEFT(fserial_number,2)="XZ" or LEFT(fserial_number,2)="JM") then 0 
    	when t.fproduct_name not like "%公益%" then 1 else 0 end as "检测台数(不含公益机)"
from (  
        select 
            *
        from (
        select 
            *,
            row_number() over(partition by fserial_number order by fend_time desc) as num
        from drt.drt_my33310_detection_t_detect_record 
        where fis_deleted=0
        and fdet_type=1
        and (freal_name like "%杨泽文%" or freal_name like "%陈贻和%" or freal_name like "%陈先庭%")
        and to_date(fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),900)))a 
        where num=1) t
left join dws.dws_hs_order_detail as b on t.fserial_number=b.fseries_number
left join dws.dws_hs_order_detail as c on b.fold_fseries_number=c.fseries_number
left join dws.dws_hs_order_detail as d on c.fold_fseries_number=d.fseries_number
left join dws.dws_hs_order_detail as e on d.fold_fseries_number=e.fseries_number
left join dws.dws_hs_order_detail as f on e.fold_fseries_number=f.fseries_number
left join dws.dws_hs_order_detail as g on f.fold_fseries_number=g.fseries_number
left join nt as h on t.fserial_number=h.fserial_no
where to_date(fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),900))

