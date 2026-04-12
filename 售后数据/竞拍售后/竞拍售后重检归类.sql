with jp_th_sale_all as(
    select
        a.fdetail_id,
        fseries_number,
        a.fclass_name,
        Fafter_sales_order_id,
        case 
            when forder_platform = 1 then '自有订单'
            when forder_platform = 5 then 'B2B鱼市订单'
            when forder_platform = 6 then '采货侠订单'
        else '其他' end as forder_platform,
        a.fstart_time,
        a.Fsales_order_num
    from dws.dws_jp_order_detail a
    where forder_status in(2,3,4,6)
    and Ftest_show <>1
    and to_date(fstart_time) >='2024-01-01'
    and a.fclass_name in( "手机",'平板','笔记本')
    -- and left(fseries_number,2) not in ("TS") -- TS的售前报告检测系统没有，只能在竞拍系统取，暂时剔除
    -- and fseries_number in(
    -- 'XY0101240229001630'
    
    -- )
    -- and a.fclass_name = "手机"
    -- and forder_platform = 6
), 
-- 1、自有订单新条码
zy_order_detail as(
    select
        a.*,
        b.fseries_number as fnew_series_number
    from jp_th_sale_all a
    inner join drt.drt_my33310_recycle_t_order b on cast(a.Fafter_sales_order_id as int) = b.forder_id
    where a.forder_platform = '自有订单'
    and b.fseries_number is not null
),
-- 1、采货侠订单 / 鱼市B2B订单新条码
caihuoxia_yushiB2B_order_detail as(
    select
        a.*,
        b.fnew_serial_no as fnew_series_number
    from jp_th_sale_all a
    inner join drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales b on a.Fsales_order_num = b.Finner_order_no
    where a.forder_platform in( 'B2B鱼市订单','采货侠订单')
    and b.fnew_serial_no <> ''
),
t_after_new_series_number as(
    select
        *
    from zy_order_detail
    union all
    select
        *
    from caihuoxia_yushiB2B_order_detail
),
t_sale_back as( -- 售后:新条码检测记录ID
    select
        fdetail_id,
        fstart_time,
        fclass_name,
        fseries_number,
        fnew_series_number,
        fdetect_record_id,
        fproduct_id as fproduct_id_back,
        fdetect_price,
        forder_platform
    from(
        select
            a.fdetail_id,
            a.fstart_time,
            a.fclass_name,
            a.fseries_number,
            a.fnew_series_number,
            b.fdetect_record_id,
            b.fproduct_id,
            a.forder_platform,
            b.fdetect_price/100 fdetect_price, 
            row_number() over(partition by a.fnew_series_number order by b.fdetect_record_id desc) as frn
        from t_after_new_series_number a
        inner join dwd.dwd_detect_back_detect_detail b on a.fnew_series_number = b.fserial_number
    ) t where frn = 1
),
t_sale_front as( -- 售前:旧条码检测记录ID
    select
        fdetail_id,
        fstart_time,
        fclass_name,
        fseries_number,
        fnew_series_number,
        fdetect_record_id,
        fproduct_id as fproduct_id_front,
        fdetect_price,
        forder_platform
    from(
        select
            a.fdetail_id,
            a.fstart_time,
            a.fclass_name,
            a.fseries_number,
            a.fnew_series_number,
            b.fdetect_record_id,
            b.fproduct_id,
            a.forder_platform,
            b.fdetect_price/100 fdetect_price,
            row_number() over(partition by a.fseries_number order by b.fdetect_record_id desc) as frn
        from t_sale_back a
        inner join dwd.dwd_detect_back_detect_detail b on a.fseries_number = b.fserial_number
    ) t where frn = 1
),
t_sale_back_detect as ( -- 售后:新条码检测记录详情（标准检）
    select
        a.*,
        b.fissue_id,
        b.fissue_name, 
        b.fanswer_name
    from t_sale_back a
    inner join dwd.dwd_detect_back_detection_issue_and_answer_v2 b on a.fdetect_record_id = b.fdetect_record_id and b.field_source='fdet_norm_snapshot'
),
t_sale_front_detect as ( -- 售前:新条码检测记录详情（标准检）
    select
        a.*,
        b.fissue_id,
        b.fissue_name, 
        b.fanswer_name
    from t_sale_front a
    inner join dwd.dwd_detect_back_detection_issue_and_answer_v2 b on a.fdetect_record_id = b.fdetect_record_id and b.field_source='fdet_norm_snapshot'
),
fproduct_not_eq as ( -- 售后/售后检测机型不一致
    select
        a.fdetail_id,
        a.fseries_number,
        a.fnew_series_number,
        a.fproduct_id_back,
        b.fproduct_id_front,
        '检测机型不一致' as fremake
    from t_sale_back a
    inner join t_sale_front b on a.fdetail_id = b.fdetail_id
    where a.fproduct_id_back <> b.fproduct_id_front
),
fanswer_name_not_eq as( -- 售后/售后检测答案项不一致
    select
        a.fdetail_id,
        a.fseries_number,
        a.fnew_series_number,
        a.fissue_id,
        a.fissue_name,
        case 
            when a.fissue_id in(8996, 7421, 8997, 12125, 12913, 13455, 13484, 13544, 13587, 13597, 15248, 11, 16, 32, 39, 122, 504, 506, 582, 671, 674, 679, 680, 743, 918, 1930, 1966, 2232, 2270, 5334, 785, 2143, 5337, 12732, 19766, 19767) then 'sku'
            when a.fissue_id in(8998, 7480, 9000, 9001, 9061, 9066, 9002, 9003, 7551, 9004, 7569, 13061, 7576, 5179, 7583, 12249, 9013, 9014, 9005, 10256, 7582, 13005, 7584, 13065, 10827, 10834, 12111, 12119, 12088, 12994, 13000, 13013, 12143, 12128, 13017, 13025, 13077, 12137, 12146, 12149, 12155, 12166, 12169, 12173, 12176, 12179, 12182, 12185, 12189, 12193, 12132, 12140, 9368, 12397, 12419, 12416, 12423, 12426, 12429, 12434, 12437, 12440, 12443, 12447, 12451, 12455, 13084, 12463, 12466, 12470, 12473, 12990, 12459, 12908, 13009, 13043, 13057, 13049, 13021, 13053, 13029, 13036, 13081, 7573, 13069, 13073, 12987, 13452, 13862, 10719, 11832, 11844, 11845, 11856, 10757, 10764, 7585, 10770, 7875, 7550, 7549, 10775, 11838, 9295, 9298, 10789, 7484, 10797, 7485, 10235, 9307, 10813, 10823, 10877, 7794, 10914, 13582, 13590, 13593, 13640, 13664, 13667, 13670, 13673, 13677, 13680, 13684, 13688, 13691, 13694, 13697, 13701, 13704, 13707, 13711, 13733, 15251, 15260, 15264, 15329, 15334, 15339, 15343, 15348, 15353, 15357, 15361, 15365, 15369, 15373, 15377, 15381, 15385, 15389, 15394, 15399, 15403, 15407, 15411, 15415, 15419, 19040, 15468, 15473, 12727, 13474, 13934, 13944, 13807, 13799, 12800, 12803, 12807, 12812, 12816, 12820, 12824, 12827, 12830, 12833, 12836, 12842, 12848, 12854, 13980, 13985, 12858, 12862, 12866, 12870, 12874, 12878, 12882, 12885, 12888, 13408, 19768, 19773, 19774, 19775, 19776, 19777, 19778, 19779, 19780, 19781, 19783, 19784, 19785, 19786, 19801, 19787, 19788, 19799, 19800, 19782, 19790, 19789, 19802, 19806) then '功能'
            when a.fissue_id in(13089, 13737, 15255, 12893) then '配件'
            when a.fissue_id in(8999, 7859, 9038, 12057, 12097, 12066, 12160, 12163, 12357, 12360, 12367, 12373, 12379, 12385, 12400, 12403, 12916, 12923, 12930, 12934, 12938, 13842, 13481, 13495, 13500, 13507, 13513, 10563, 13518, 9225, 9230, 9235, 10579, 10585, 10590, 10596, 10600, 10609, 10617, 10621, 10628, 10640, 9260, 10653, 13602, 13608, 13614, 13620, 13626, 13632, 13636, 13644, 13652, 15269, 15276, 15280, 15287, 15423, 15430, 15437, 19043, 12735, 12741, 13474, 12747, 12751, 12755, 13934, 13944, 12762, 19769, 19770, 19771, 19958, 19803, 19804) then '外观'
            when a.fissue_id in(9006, 9007, 9008, 9009, 9010, 12333, 9011, 12100, 12106, 12115, 12406, 12409, 12413, 12959, 12963, 12966, 12971, 12976, 12980, 12983, 13865, 10846, 10842, 10850, 10859, 10869, 10873, 10883, 10894, 10898, 13715, 13718, 13721, 13725, 13729, 15300, 15305, 15308, 15313, 15318, 15321, 15325, 15458, 15463, 12774, 12778, 12781, 12785, 12789, 12793, 12796, 13968, 13973, 19791, 19792, 19793, 19794, 19795, 19796, 19797, 19798, 19807) then '维修'
            when a.fissue_id in(9046, 12077, 12391, 12951, 13852, 10661, 5076, 9810, 10680, 10685, 10690, 10695, 10712, 13656, 15292, 15442, 15450, 12766, 13807, 13799, 19772, 19805) then '显示'
        else '其他' end as fremake,
        a.fanswer_name as 售后答案项,
        b.fanswer_name as 售前答案项
    from t_sale_back_detect a
    inner join t_sale_front_detect b on a.fseries_number = b.fseries_number and a.fissue_id = b.fissue_id
    left join (select fseries_number from fproduct_not_eq group by fseries_number) c on a.fseries_number = c.fseries_number
    where a.fanswer_name != b.fanswer_name
    and c.fseries_number is null -- 检测机型不一致的部分不再判断答案项
)
-- 先汇总得到A1~A6
select
    b.fdetail_id,
    b.A1,
    b.A2,
    b.A3,
    b.A4,
    b.A5,
    b.A6,
    -- 构造前置加号，再去掉首个加号，避免任何函数兼容问题
    regexp_replace(
        concat(
            case when b.A1 is not null then concat('+', b.A1) else '' end,
            case when b.A2 is not null then concat('+', b.A2) else '' end,
            case when b.A3 is not null then concat('+', b.A3) else '' end,
            case when b.A4 is not null then concat('+', b.A4) else '' end,
            case when b.A5 is not null then concat('+', b.A5) else '' end,
            case when b.A6 is not null then concat('+', b.A6) else '' end
        ),
        '^\\+',
        ''
    ) as 合并,
    regexp_replace(
        concat(
            case when b.A1 is not null then concat('+', b.A1) else '' end,
            case when b.A2 is not null then concat('+', b.A2) else '' end,
            case when b.A3 is not null then concat('+', b.A3) else '' end,
            case when b.A4 is not null then concat('+', b.A4) else '' end,
            case when b.A5 is not null then concat('+', b.A5) else '' end,
            case when b.A6 is not null then concat('+', b.A6) else '' end
        ),
        '^\\+',
        ''
    ) as 重检归类
from(
    select
        fdetail_id,
        min(case when fremake='检测机型不一致' then '检测机型不一致' end) as A1,
        min(case when fremake='sku' then 'sku' end) as A2,
        min(case when fremake='外观' then '外观' end) as A3,
        min(case when fremake='显示' then '显示' end) as A4,
        min(case when fremake='功能' then '功能' end) as A5,
        min(case when fremake='维修' then '维修' end) as A6
    from(
        select
            fdetail_id,
            fremake
        from fproduct_not_eq
        union all
        select
            fdetail_id,
            fremake
        from fanswer_name_not_eq
        group by fdetail_id,fremake
    ) t
    group by fdetail_id
) b
