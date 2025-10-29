-- =========================
-- 收货组各环节效能 - 优化重构版
-- 说明：
-- 1) 统一时间配置参数，默认365天
-- 2) 优化代码结构，提高可读性和维护性
-- 3) 统一所有环节的时间周期
-- 4) 简化重复的CASE WHEN逻辑
-- =========================

-- =========================
-- 统一配置参数
-- =========================
with time_config as (
    select 
        date_sub(from_unixtime(unix_timestamp()), 365) as default_window  -- 统一365天
),

-- =========================
-- 公共CTE：密封袋数据
-- =========================
seal_bag as (
    select 
        *
    from (
        select 
            a.fseries_number,
            a.fwarehouse_name,
            a.fadd_time,
            b.freal_name,
            row_number() over(partition by fseries_number order by fadd_time desc) as num
        from drt.drt_my33310_hsb_wms_t_seal_bag_log as a
        left join drt.drt_my33310_amcdb_t_user as b on a.fadd_user=b.fusername
    ) t 
    where num=1
),

-- =========================
-- 公共CTE：调拨数据
-- =========================
allot as (
    select 
        *
    from (
        select 
            *,
            row_number() over(partition by fbar_code order by fadd_time desc) as num
        from drt.drt_my33310_xywms_t_product_allot
    ) t 
    where num=1
),

-- =========================
-- 公共CTE：仓库判断逻辑
-- =========================
warehouse_classification as (
    select 
        fseries_number,
        case 
            when left(fseries_number,2) in ('02') or right(left(fseries_number,6),2)="16" then "杭州" 
            else "深圳" 
        end as fwarehouse
    from (
        select distinct fseries_number from dws.dws_instock_details
        where funpack_time >= to_date((select default_window from time_config))
    ) base
),

-- =========================
-- 主查询：拆包环节
-- =========================
unpack_data as (
    select 
        d.funpack_time as ftimeby,
        LEFT(d.fseries_number,2) as channel,
        w.fwarehouse,
        case when d.Funpack_user="张雄均" then "张小凤" else d.funpack_user end as operator,
        case 
            when d.ftype="回收" then 
                if(concat(d.ftype,"拆包",d.Fcategory_name) is null,concat(d.ftype,"拆包"),concat(d.ftype,"拆包",d.Fcategory_name)) 
            else concat(d.ftype,"拆包") 
        end as ftype,
        count(if(d.ftype="验机" and to_date(d.funpack_time)>='2023-09-20',null,
                if(d.ftype="回收" and to_date(d.funpack_time)>='2023-10-21',null,d.fseries_number))) as num,
        count(if(hour(d.funpack_time)<18,d.fseries_number,null)) as "加班前数量",
        count(if(hour(d.funpack_time)>=18,d.fseries_number,null)) as "加班后数量"
    from dws.dws_instock_details d
    left join warehouse_classification w on d.fseries_number=w.fseries_number
    where d.fseries_number is not null
    and d.funpack_time >= to_date((select default_window from time_config))
    and d.Funpack_user is not null
    group by 1,2,3,4,5
),

-- =========================
-- 主查询：验机配件收货
-- =========================
inspection_parts_receive as (
    select
        t.freceive_time as ftimeby,
        left(a.fparts_bar_code,2) as channel,
        case 
            when left(a.fparts_bar_code,2) in ('02') or right(left(a.fparts_bar_code,6),2)="16" then "杭州" 
            else "深圳" 
        end as fwarehouse,
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
            row_number() over(partition by fparcel_id order by fadd_time desc) as num
        from drt.drt_my33310_xywms_t_parcel_log
        where ftype=3
    ) t on a.fid=t.fparcel_id
    where t.num=1
    and t.freceive_time >= to_date((select default_window from time_config))
    and a.fparts_bar_code is not null
    and a.fparts_bar_code!=""
    group by 1,2,3,4,5
),

-- =========================
-- 主查询：收货环节
-- =========================
receive_data as (
    select 
        d.freceive_time as ftimeby,
        LEFT(d.fseries_number,2) as channel,
        w.fwarehouse,
        case 
            when to_date(d.freceive_time)='2024-05-29' and d.freceive_user="朱小洋" and LEFT(d.fseries_number,2)="01" then "李伟豪"
            when d.freceive_user="张雄均" then "张小凤"
            else d.freceive_user 
        end as operator,
        case 
            when d.fseries_number like "%\_%" then concat(d.ftype,"配件收货")
            when LEFT(d.fseries_number,2)="TL" then "太力收货"
            when d.ftype="回收" and d.fcategory_name in ('CPU','内存条','显卡','智能手写笔','游戏机','电子书','电脑主板','固态硬盘','显示器') then concat(d.ftype,"收货","电脑配件")
            when d.ftype="回收" and d.fcategory_name not in ("手机","平板",'平板电脑','笔记本','笔记本电脑','耳机','配件','充电套装','智能手表','智能手环') then concat(d.ftype,"收货","相机及其它")
            when d.ftype="回收" then 
                if(concat(d.ftype,"收货",d.Fcategory_name) is null,concat(d.ftype,"收货"),concat(d.ftype,"收货",d.Fcategory_name)) 
            else concat(d.ftype,"收货") 
        end as ftype,
        count(d.fseries_number) as num,
        count(if(hour(d.freceive_time)<18,d.fseries_number,null)) as "加班前数量",
        count(if(hour(d.freceive_time)>=18,d.fseries_number,null)) as "加班后数量"
    from dws.dws_instock_details d
    left join warehouse_classification w on d.fseries_number=w.fseries_number
    where d.fseries_number is not null
    and d.freceive_time >= to_date((select default_window from time_config))
    and d.freceive_user is not null
    group by 1,2,3,4,5
),

-- =========================
-- 主查询：拍照环节
-- =========================
photo_data as (
    select 
        d.fmain_photo_time as ftimeby,
        LEFT(d.fseries_number,2) as channel,
        w.fwarehouse,
        case 
            when to_date(d.fmain_photo_time)='2024-02-03' and d.fphoto_name="杨泽文" then "朱小洋" 
            when (to_date(d.fmain_photo_time) between '2024-02-26' and '2024-02-27') and d.fphoto_name="陈乐娟" then "高华铎"
            when to_date(d.fmain_photo_time)='2024-03-04' and d.fphoto_name="陈冬凡" then "周远鸿" 
            when to_date(d.fmain_photo_time)='2024-03-04' and d.fphoto_name="胡家华" then "黄成水"
            when d.fphoto_name="张雄均" then "张小凤"
            when to_date(d.fmain_photo_time)='2024-11-05' and hour(d.fmain_photo_time)<16 and d.fphoto_name="汤珂" then null
            else d.fphoto_name 
        end as operator,
        case 
            when d.ftype="验机" then concat(d.ftype,"拍照")
            when d.fcategory_name in ('CPU','内存条','显卡','智能手写笔','游戏机','电子书','电脑主板','固态硬盘','显示器') then concat("拍照","电脑配件")
            when d.fcategory_name not in ("手机","平板",'平板电脑','笔记本','笔记本电脑','耳机','配件','充电套装','智能手表','智能手环') then concat("拍照","相机及其它")
            else if(concat("拍照",d.Fcategory_name) is null,"拍照",concat("拍照",d.Fcategory_name))  
        end as ftype,
        count(d.fseries_number) as num,
        count(if(hour(d.fmain_photo_time)<18,d.fseries_number,null)) as "加班前数量",
        count(if(hour(d.fmain_photo_time)>=18,d.fseries_number,null)) as "加班后数量"
    from dws.dws_instock_details d
    left join warehouse_classification w on d.fseries_number=w.fseries_number
    where d.fseries_number is not null
    and d.fmain_photo_time >= to_date((select default_window from time_config))
    and d.fphoto_name is not null
    group by 1,2,3,4,5
),

-- =========================
-- 主查询：缺陷拍照
-- =========================
defect_photo_data as (
    select 
        d.fdefect_photo_time as ftimeby,
        LEFT(d.Fseries_number,2) as channel,
        w.fwarehouse,
        d.fdefect_photo_name as operator,
        "回收转寄卖拍照" as ftype,
        count(d.Fseries_number) as num,
        count(if(hour(d.fdefect_photo_time)<18,d.fseries_number,null)) as "加班前数量",
        count(if(hour(d.fdefect_photo_time)>=18,d.fseries_number,null)) as "加班后数量"
    from dws.dws_instock_details d
    left join warehouse_classification w on d.fseries_number=w.fseries_number
    where d.fseries_number is not null
    and d.fdefect_photo_time >= to_date((select default_window from time_config))
    and d.fdefect_photo_name is not null
    group by 1,2,3,4,5
),

-- =========================
-- 主查询：防拆标
-- =========================
tamper_data as (
    select 
        d.Ftamper_time as ftimeby,
        LEFT(d.fseries_number,2) as channel,
        w.fwarehouse,
        d.Foperator as operator,
        "防拆标" as ftype,
        count(d.fseries_number) as num,
        count(if(hour(d.Ftamper_time)<18,d.fseries_number,null)) as "加班前数量",
        count(if(hour(d.Ftamper_time)>=18,d.fseries_number,null)) as "加班后数量"
    from dws.dws_instock_details d
    left join warehouse_classification w on d.fseries_number=w.fseries_number
    where d.fseries_number is not null
    and d.Ftamper_time >= to_date((select default_window from time_config))
    and d.Foperator is not null
    group by 1,2,3,4,5
),

-- =========================
-- 主查询：寄卖打印
-- =========================
print_data as (
    select 
        d.fplus_print_time as ftimeby,
        LEFT(d.fseries_number,2) as channel,
        w.fwarehouse,
        case when to_date(d.fplus_print_time)="2024-06-05" and d.fplus_printer="吴依卓" then null else d.fplus_printer end as operator,
        "寄卖打印" as ftype,
        count(d.fseries_number) as num,
        count(if(hour(d.fplus_print_time)<18,d.fseries_number,null)) as "加班前数量",
        count(if(hour(d.fplus_print_time)>=18,d.fseries_number,null)) as "加班后数量"
    from dws.dws_instock_details d
    left join warehouse_classification w on d.fseries_number=w.fseries_number
    where d.fseries_number is not null
    and d.fplus_print_time >= to_date((select default_window from time_config))
    and d.fplus_printer is not null
    group by 1,2,3,4,5
),

-- =========================
-- 主查询：装密封袋
-- =========================
seal_bag_data as (
    select 
        from_unixtime(s.fadd_time) as ftimeby,
        left(s.fseries_number,2) as channel,
        w.fwarehouse,
        case when to_date(from_unixtime(s.fadd_time))="2024-06-05" and s.freal_name="吴依卓" then null else s.freal_name end as operator,
        "装密封袋" as ftype,
        count(s.fseries_number) as num,
        count(if(hour(from_unixtime(s.fadd_time))<18,s.fseries_number,null)) as "加班前数量",
        count(if(hour(from_unixtime(s.fadd_time))>=18,s.fseries_number,null)) as "加班后数量"
    from seal_bag s
    left join warehouse_classification w on s.fseries_number=w.fseries_number
    where s.fseries_number is not null
    and from_unixtime(s.fadd_time) >= to_date((select default_window from time_config))
    and s.freal_name is not null
    group by 1,2,3,4,5
),

-- =========================
-- 主查询：验机出库
-- =========================
inspection_outbound_data as (
    select 
        o.fput_time as ftimeby,
        left(o.fhost_barcode,2) as channel,
        case 
            when left(o.fhost_barcode,2) in ('02') or right(left(o.fhost_barcode,6),2)="16" then "杭州" 
            else "深圳" 
        end as fwarehouse,
        o.fput_user,
        "验机出库" as ftype,
        count(o.fhost_barcode) as num,
        count(if(hour(o.fput_time)<18,o.fhost_barcode,null)) as "加班前数量",
        count(if(hour(o.fput_time)>=18,o.fhost_barcode,null)) as "加班后数量"
    from dws.dws_xy_yhb_detail o
    where o.fhost_barcode is not null
    and o.fput_time >= to_date((select default_window from time_config))
    and o.fput_user is not null
    group by 1,2,3,4,5
),

-- =========================
-- 主查询：验机调拨
-- =========================
allot_data as (
    select 
        a.fadd_time as ftimeby,
        left(a.fbar_code,2) as channel,
        case 
            when left(a.fbar_code,2) in ('02') or right(left(a.fbar_code,6),2)="16" then "杭州" 
            else "深圳" 
        end as fwarehouse,
        a.fadd_user, 
        "验机调拨" as ftype,
        count(a.fbar_code) as num,
        count(if(hour(a.fadd_time)<18,a.fbar_code,null)) as "加班前数量",
        count(if(hour(a.fadd_time)>=18,a.fbar_code,null)) as "加班后数量"
    from allot a
    where a.fadd_time >= to_date((select default_window from time_config))
    and a.fbar_code is not null
    and a.fadd_user is not null
    group by 1,2,3,4,5
),

-- =========================
-- 主查询：验机上架
-- =========================
shelf_data as (
    select 
        a.fupdate_time as ftimeby,
        left(a.fbar_code,2) as channel,
        case 
            when left(a.fbar_code,2) in ('02') or right(left(a.fbar_code,6),2)="16" then "杭州" 
            else "深圳" 
        end as fwarehouse,
        a.fupdate_user, 
        "验机上架" as ftype,
        count(a.fbar_code) as num,
        count(if(hour(a.fupdate_time)<18,a.fbar_code,null)) as "加班前数量",
        count(if(hour(a.fupdate_time)>=18,a.fbar_code,null)) as "加班后数量"
    from allot a
    where a.fupdate_time >= to_date((select default_window from time_config))
    and a.fbar_code is not null
    and a.fupdate_user is not null
    group by 1,2,3,4,5
),

-- =========================
-- 主查询：自动化检测
-- =========================
automation_detection_data as (
    select 
        t.fcreate_time,
        left(t.fserial_number,2) as fchannel,
        case 
            when left(t.fserial_number,2) in ('02') or right(left(t.fserial_number,6),2)="16" then "杭州" 
            else "深圳" 
        end as fwarehouse,
        case 
            when to_date(t.fcreate_time)='2023-10-16' and t.Fbind_real_name="刘俊" then "周利" 
            when (to_date(t.fcreate_time) BETWEEN '2023-11-01' AND '2023-11-21') and t.Fbind_real_name="郑佩文" then null 
            when to_date(t.fcreate_time)='2024-01-29' and t.Fbind_real_name="林嘉成" then null
            when t.Fbind_real_name="张雄均" then "张小凤"
            when to_date(t.fcreate_time)='2025-04-14' and t.Fbind_real_name="严俊" then "林广泽"
            else t.Fbind_real_name 
        end as freal_name,
        case 
            when left(t.fserial_number,2) in ('01','02') then "验机-自动化检测" 
            when left(t.fserial_number,2) not in ('01','02') and t.fbrand_name="苹果" then "回收-模块一-苹果" 
            else "回收-模块一-安卓" 
        end as ftype,
        count(t.fserial_number) as num,
        count(if(hour(t.fcreate_time)<18,t.fserial_number,null)) as "加班前数量",
        count(if(hour(t.fcreate_time)>=18,t.fserial_number,null)) as "加班后数量"
    from (
        select 
            a.fcreate_time,
            a.fserial_number,
            a.Fbind_real_name,
            a.fbrand_name,
            row_number() over(partition by a.fserial_number order by a.fcreate_time desc) as num
        from drt.drt_my33312_detection_t_automation_det_record as a
        left join drt.drt_my33310_amcdb_t_user as b on a.fuser_id=b.fuser_id
        where a.fserial_number is not null and a.fserial_number!=""
        and a.Fbind_real_name is not null
        and to_date(a.fcreate_time) >= to_date((select default_window from time_config))
    ) t
    where num=1
    group by 1,2,3,4,5
),

-- =========================
-- 主查询：模块二检测
-- =========================
module2_detection_data as (
    select 
        t.fcreate_time,
        left(t.fserial_number,2) as fchannel,
        case 
            when left(t.fserial_number,2) in ('02') or right(left(t.fserial_number,6),2)="16" then "杭州" 
            else "深圳" 
        end as fwarehouse,
        case when t.freal_name="张雄均" then "张小凤" else t.freal_name end as freal_name,
        case 
            when left(t.fserial_number,2) in ('01','02') then "验机-模块二" 
            when left(t.fserial_number,2) not in ('01','02') and t.fbrand_name='Apple' then "回收-模块二-苹果"
            else "回收-模块二-安卓" 
        end as ftype,
        count(t.fserial_number) as num,
        count(if(hour(t.fcreate_time)<18,t.fserial_number,null)) as "加班前数量",
        count(if(hour(t.fcreate_time)>=18,t.fserial_number,null)) as "加班后数量"
    from (
        select 
            a.fcreate_time,
            a.fserial_number,
            b.freal_name,
            a.fbrand_name,
            row_number() over(partition by a.fserial_number order by a.fcreate_time desc) as num
        from drt.drt_my33312_detection_t_det_app_record as a 
        left join drt.drt_my33310_amcdb_t_user as b on a.fuser_id=b.fuser_id
        where to_date(a.fcreate_time) >= to_date((select default_window from time_config))
        and b.freal_name is not null
        and a.fserial_number is not null and a.fserial_number!=""
        and b.freal_name not in ('黄成水','张圳强','吴琼','冯铭焕','胡涛','李俊锋','黄雅如','朱惠萍','林红','陈映熹','张世梅')
    ) t
    where num=1
    group by 1,2,3,4,5
)

-- =========================
-- 最终结果：合并所有数据
-- =========================
select * from unpack_data
union all
select * from inspection_parts_receive
union all
select * from receive_data
union all
select * from photo_data
union all
select * from defect_photo_data
union all
select * from tamper_data
union all
select * from print_data
union all
select * from seal_bag_data
union all
select * from inspection_outbound_data
union all
select * from allot_data
union all
select * from shelf_data
union all
select * from automation_detection_data
union all
select * from module2_detection_data;
