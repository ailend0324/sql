with seal_bag as (
select 
    *
from (
    select 
        a.fseries_number,
        a.fwarehouse_name,
        a.fadd_time,
        b.freal_name,
        row_number()over(partition by fseries_number order by fadd_time desc) as num
    from drt.drt_my33310_hsb_wms_t_seal_bag_log as a
    left join drt.drt_my33310_amcdb_t_user as b on a.fadd_user=b.fusername
)t where num=1
),
allot as (
select 
    *
from (
    select 
        *,
        row_number()over(partition by fbar_code order by fadd_time desc) as num
    from drt.drt_my33310_xywms_t_product_allot
)t where num=1
)
select 
    funpack_time as ftimeby,
    LEFT(fseries_number,2) as channel,
    case when left(fseries_number,2) in ('02') or right(left(fseries_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    case when Funpack_user="张雄均" then "张小凤" else funpack_user end as operator,
    case when ftype="回收" then if(concat(ftype,"拆包",Fcategory_name) is null,concat(ftype,"拆包"),concat(ftype,"拆包",Fcategory_name)) else concat(ftype,"拆包") end as ftype,
    count(if(ftype="验机" and to_date(funpack_time)>='2023-09-20',null,if(ftype="回收" and to_date(funpack_time)>='2023-10-21',null,fseries_number))) as num,
    count(if(hour(funpack_time)<18,fseries_number,null)) as "加班前数量",
    count(if(hour(funpack_time)>=18,fseries_number,null)) as "加班后数量"
from dws.dws_instock_details
where fseries_number is not null
and funpack_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and Funpack_user is not null
group by 1,2,3,4,5
union all
select
    t.freceive_time as ftimeby,
    left(a.fparts_bar_code,2) as channel,
    case when left(a.fparts_bar_code,2) in ('02') or right(left(a.fparts_bar_code,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    case when t.freceive_user="张雄均" then "张小凤" else t.freceive_user end as operator,
    "验机配件收货" as ftype,
    count(distinct a.fparts_bar_code) as num,
    count(distinct if(hour(t.freceive_time)<18,a.fparts_bar_code,null)) as "加班前数量",
    count(distinct if(hour(t.freceive_time)>=18,a.fparts_bar_code,null)) as "加班后数量"
from drt.drt_my33310_xywms_t_parcel as a
left join (
    select
        fparcel_id,
        fadd_time as freceive_time,
        fadd_user as freceive_user,
        row_number() over(partition by fparcel_id  order by fadd_time desc) as num
    from drt.drt_my33310_xywms_t_parcel_log
    where ftype=3
) t on a.fid=t.fparcel_id
where t.num=1
and t.freceive_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and a.fparts_bar_code is not null
and a.fparts_bar_code!=""
group by 1,2,3,4,5
union all
select 
    freceive_time as ftimeby,
    LEFT(fseries_number,2) as channel,
    case when left(fseries_number,2) in ('02') or right(left(fseries_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    case when to_date(freceive_time)='2024-05-29' and freceive_user="朱小洋" and LEFT(fseries_number,2)="01" then "李伟豪"
         when freceive_user="张雄均" then "张小凤"
    else freceive_user end as operator,
    case when fseries_number like "%\_%" then concat(ftype,"配件收货")
    	 when LEFT(fseries_number,2)="TL" then "太力收货"
        when ftype="回收" and fcategory_name in ('CPU','内存条','显卡','智能手写笔','游戏机','电子书','电脑主板','固态硬盘','显示器') then concat(ftype,"收货","电脑配件")
         when ftype="回收" and fcategory_name not in ("手机","平板",'平板电脑','笔记本','笔记本电脑','耳机','配件','充电套装','智能手表','智能手环') then concat(ftype,"收货","相机及其它")
        when ftype="回收" then if(concat(ftype,"收货",Fcategory_name) is null,concat(ftype,"收货"),concat(ftype,"收货",Fcategory_name)) else concat(ftype,"收货") end as ftype,
    count(fseries_number) as num,
    count(if(hour(freceive_time)<18,fseries_number,null)) as "加班前数量",
    count(if(hour(freceive_time)>=18,fseries_number,null)) as "加班后数量"
from dws.dws_instock_details
where fseries_number is not null
and freceive_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and freceive_user is not null
group by 1,2,3,4,5
union all 
select 
    fmain_photo_time as ftimeby,
    LEFT(fseries_number,2) as channel,
    case when left(fseries_number,2) in ('02') or right(left(fseries_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    case when to_date(fmain_photo_time)='2024-02-03' and fphoto_name="杨泽文" then "朱小洋" 
         when (to_date(fmain_photo_time) between '2024-02-26' and '2024-02-27') and fphoto_name="陈乐娟" then "高华铎"
         when to_date(fmain_photo_time)='2024-03-04' and fphoto_name="陈冬凡" then "周远鸿" 
         when to_date(fmain_photo_time)='2024-03-04' and fphoto_name="胡家华" then "黄成水"
         when fphoto_name="张雄均" then "张小凤"
         when to_date(fmain_photo_time)='2024-11-05' and hour(fmain_photo_time)<16 and fphoto_name="汤珂" then null
    else fphoto_name end as operator,
    case when ftype="验机" then concat(ftype,"拍照")
         when fcategory_name in ('CPU','内存条','显卡','智能手写笔','游戏机','电子书','电脑主板','固态硬盘','显示器') then concat("拍照","电脑配件")
         when fcategory_name not in ("手机","平板",'平板电脑','笔记本','笔记本电脑','耳机','配件','充电套装','智能手表','智能手环') then concat("拍照","相机及其它")
    else if(concat("拍照",Fcategory_name) is null,"拍照",concat("拍照",Fcategory_name))  end as ftype,
    count(fseries_number) as num,
    count(if(hour(fmain_photo_time)<18,fseries_number,null)) as "加班前数量",
    count(if(hour(fmain_photo_time)>=18,fseries_number,null)) as "加班后数量"
from dws.dws_instock_details
where fseries_number is not null
and fmain_photo_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and fphoto_name is not null
group by 1,2,3,4,5
union all 
select 
	fdefect_photo_time as ftimeby,
    LEFT(Fseries_number,2) as channel,
    case when left(fseries_number,2) in ('02') or right(left(fseries_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    fdefect_photo_name as operator,
    "回收转寄卖拍照" as ftype,
    count(Fseries_number) as num,
    count(if(hour(fdefect_photo_time)<18,fseries_number,null)) as "加班前数量",
    count(if(hour(fdefect_photo_time)>=18,fseries_number,null)) as "加班后数量"
from dws.dws_instock_details
where fseries_number is not null
and fdefect_photo_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and fdefect_photo_name is not null
group by 1,2,3,4,5
union all
select 
    Ftamper_time as ftimeby,
    LEFT(fseries_number,2) as channel,
    case when left(fseries_number,2) in ('02') or right(left(fseries_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    Foperator as operator,
    "防拆标" as ftype,
    count(fseries_number) as num,
    count(if(hour(Ftamper_time)<18,fseries_number,null)) as "加班前数量",
    count(if(hour(Ftamper_time)>=18,fseries_number,null)) as "加班后数量"
from dws.dws_instock_details
where fseries_number is not null
and Ftamper_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and Foperator is not null
group by 1,2,3,4,5
union all
select 
    fplus_print_time as ftimeby,
    LEFT(fseries_number,2) as channel,
    case when left(fseries_number,2) in ('02') or right(left(fseries_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    case when to_date(fplus_print_time)="2024-06-05" and fplus_printer="吴依卓" then null else fplus_printer end as operator,
    "寄卖打印" as ftype,
    count(fseries_number) as num,
    count(if(hour(fplus_print_time)<18,fseries_number,null)) as "加班前数量",
    count(if(hour(fplus_print_time)>=18,fseries_number,null)) as "加班后数量"
from dws.dws_instock_details
where fseries_number is not null
and fplus_print_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and fplus_printer is not null
group by 1,2,3,4,5
union all
select 
    from_unixtime(fadd_time) as ftimeby,
    left(fseries_number,2) as channel,
    case when left(fseries_number,2) in ('02') or right(left(fseries_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    case when to_date(from_unixtime(fadd_time))="2024-06-05" and freal_name="吴依卓" then null else freal_name end as operator,
    "装密封袋" as ftype,
    count(fseries_number) as num,
    count(if(hour(from_unixtime(fadd_time))<18,fseries_number,null)) as "加班前数量",
    count(if(hour(from_unixtime(fadd_time))>=18,fseries_number,null)) as "加班后数量"
from seal_bag
where fseries_number is not null
and from_unixtime(fadd_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and freal_name is not null
group by 1,2,3,4,5
union all
select 
    fput_time as ftimeby,
    left(fhost_barcode,2) as channel,
    case when left(fhost_barcode,2) in ('02') or right(left(fhost_barcode,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    fput_user,
    "验机出库" as ftype,
    count(fhost_barcode) as num,
    count(if(hour(fput_time)<18,fhost_barcode,null)) as "加班前数量",
    count(if(hour(fput_time)>=18,fhost_barcode,null)) as "加班后数量"
from dws.dws_xy_yhb_detail 
where fhost_barcode is not null
and fput_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and fput_user is not null
group by 1,2,3,4,5
union all
select 
    fadd_time as ftimeby,
    left(fbar_code,2) as channel,
    case when left(fbar_code,2) in ('02') or right(left(fbar_code,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    fadd_user, 
    "验机调拨" as ftype,
    count(fbar_code) as num,
    count(if(hour(fadd_time)<18,fbar_code,null)) as "加班前数量",
    count(if(hour(fadd_time)>=18,fbar_code,null)) as "加班后数量"
from allot
where fadd_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and fbar_code is not null
and fadd_user is not null
group by 1,2,3,4,5
union all
select 
    fupdate_time as ftimeby,
    left(fbar_code,2) as channel,
    case when left(fbar_code,2) in ('02') or right(left(fbar_code,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    fupdate_user, 
    "验机上架" as ftype,
    count(fbar_code) as num,
    count(if(hour(fupdate_time)<18,fbar_code,null)) as "加班前数量",
    count(if(hour(fupdate_time)>=18,fbar_code,null)) as "加班后数量"
from allot
where fupdate_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and fbar_code is not null
and fupdate_user is not null
group by 1,2,3,4,5
union all
select 
    fcreate_time,
    left(fserial_number,2) as fchannel,
    case when left(fserial_number,2) in ('02') or right(left(fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    case when to_date(fcreate_time)='2023-10-16' and Fbind_real_name="刘俊" then "周利" 
    	 when (to_date(fcreate_time) BETWEEN '2023-11-01' AND '2023-11-21') and Fbind_real_name="郑佩文" then null 
         when to_date(fcreate_time)='2024-01-29' and Fbind_real_name="林嘉成" then null
         when Fbind_real_name="张雄均" then "张小凤"
         when to_date(fcreate_time)='2025-04-14' and Fbind_real_name="严俊" then "林广泽"
         else Fbind_real_name end as freal_name,
    case when left(fserial_number,2) in ('01','02') then "验机-自动化检测" 
         when left(fserial_number,2) not in ('01','02') and fbrand_name="苹果" then "回收-模块一-苹果" else "回收-模块一-安卓" end as ftype,
    count(fserial_number) as num,
    count(if(hour(fcreate_time)<18,fserial_number,null)) as "加班前数量",
    count(if(hour(fcreate_time)>=18,fserial_number,null)) as "加班后数量"
from (
select 
    a.fcreate_time,
    a.fserial_number,
    a.Fbind_real_name,
    a.fbrand_name,
    row_number()over(partition by a.fserial_number order by a.fcreate_time desc) as num
from drt.drt_my33312_detection_t_automation_det_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_id=b.fuser_id
where a.fserial_number is not null and a.fserial_number!=""
and a.Fbind_real_name is not null
and to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
)t
where num=1
group by 1,2,3,4,5
union all
select 
    fcreate_time,
    left(fserial_number,2) as fchannel,
    case when left(fserial_number,2) in ('02') or right(left(fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    case when freal_name="张雄均" then "张小凤" else freal_name end as freal_name,
    case when left(fserial_number,2) in ('01','02') then "验机-模块二" 
         when left(fserial_number,2) not in ('01','02') and fbrand_name='Apple' then "回收-模块二-苹果"
    else "回收-模块二-安卓" end as ftype,
    count(fserial_number) as num,
    count(if(hour(fcreate_time)<18,fserial_number,null)) as "加班前数量",
    count(if(hour(fcreate_time)>=18,fserial_number,null)) as "加班后数量"
from (
select 
    a.fcreate_time,
    a.fserial_number,
    b.freal_name,
    a.fbrand_name,
    row_number()over(partition by a.fserial_number order by a.fcreate_time desc) as num
from drt.drt_my33312_detection_t_det_app_record as a 
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_id=b.fuser_id
where to_date(a.fcreate_time)>='2024-04-01'
and b.freal_name is not null
and a.fserial_number is not null and a.fserial_number!=""
and b.freal_name not in ('黄成水','张圳强','吴琼','冯铭焕','胡涛','李俊锋','黄雅如','朱惠萍','林红','陈映熹','张世梅')
)t
where num=1
group by 1,2,3,4,5
























