-- 分批查询策略 - 解决大数据量查询问题
-- 将180天数据分成6个30天的批次，分别查询后合并

-- 批次1：最近30天
with gongdan_1 as (
select 
    to_date(from_unixtime(a.fadd_time)) as fadd_time,
    a.fbarcode_sn,
    case when c.freal_name in ("黄奕锋","周晓薇","郑春玲","梁椿灏","何嫣红","林宁","徐小利","朱小露",'苍雅婷','叶志','林晓雪') then "议价组" else "前端咨询" end as fgroup
from drt.drt_my33310_csrdb_t_works as a
left join drt.drt_my33310_csrdb_t_works_config_appeal as d on a.fappeal_type2=d.fid
left join drt.drt_my33310_amcdb_t_user as c on a.fadd_user=c.fusername
where d.fcontent in ('找机重检','提供照片')
and to_date(from_unixtime(a.fadd_time))>=to_date(date_sub(from_unixtime(unix_timestamp()),30))  -- 最近30天
and to_date(from_unixtime(a.fadd_time))<to_date(date_sub(from_unixtime(unix_timestamp()),0))
and a.forder_system=2
and a.fduty_content!="无效工单"
and a.fbarcode_sn is not null
),

chongjian_1 as (
select 
    a.fserial_number,
    a.fissue_name,
    a.fanswer_name,
    b.fanswer_name as fchongjian_answer_name,
    b.fbrand_name,
    case when a.fissue_name in ('保修情况','网络类型','机身内存','存储容量','颜色','型号') then "模块一"
         when a.fissue_name in ('HOME键','扬声器','听筒','触摸','Face ID','蓝牙功能','NFC功能','WIFI功能') then "模块二"
         when a.fissue_name in ('外壳印渍','外壳划痕','屏幕显示','屏幕外观','外壳破损') then "模块三"
         when a.fissue_name in ('主板拆修情况','后置摄像头','无线充电','电池更换情况','进水/受潮','前置摄像头','电池健康度') then "模块四"
         else null end as type,
    a.fdetect_price,
    b.fchongjian_price
from (
select 
    a.fissue_name,
    a.fanswer_name,
    b.fserial_number,
    b.fdetect_price/100 as fdetect_price,
    row_number()over(partition by b.fserial_number,a.fissue_name order by a.fdetect_record_id desc) as num
from dwd.dwd_detect_back_detection_issue_and_answer_v2 as a
left join dwd.dwd_detect_back_detect_detail as b on a.fdetect_record_id=b.fdetect_record_id
where left(b.fserial_number,2) not in ('BB','NT','YZ','JM','BG','XZ')
and a.field_source='fdet_norm_snapshot'
and a.ds>=to_date(date_sub(from_unixtime(unix_timestamp()),30))  -- 最近30天
and a.ds<to_date(date_sub(from_unixtime(unix_timestamp()),0))
and b.fclass_name="手机"
and b.freal_name not in ('林杰俊','杨泽文','姜宜良','蒋宜良')
and b.fdet_type=0
and b.fserial_number is not null
) as a
left join (
select 
    a.fissue_name,
    b.fserial_number,
    b.fbrand_name,
    b.fdetect_price/100 as fchongjian_price,
    row_number()over (partition by b.fserial_number,a.fissue_name order by a.fdetect_record_id desc) as num
from dwd.dwd_detect_back_detection_issue_and_answer_v2 as a
left join dwd.dwd_detect_back_detect_detail as b on a.fdetect_record_id=b.fdetect_record_id
where left(b.fserial_number,2) not in ('BB','NT','YZ','JM','BG','XZ')
and a.field_source='fdet_norm_snapshot'
and a.ds>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
and a.ds<to_date(date_sub(from_unixtime(unix_timestamp()),0))
and b.fclass_name="手机"
and b.fdet_type=2
and b.fserial_number is not null
) as b on a.fserial_number=b.fserial_number and a.fissue_name=b.fissue_name
where a.num=1 
and b.num=1
and b.fserial_number is not null
and a.fanswer_name<>b.fanswer_name
)

select 
    a.fadd_time,
    a.fbarcode_sn,
    a.fgroup,
    case when right(left(a.fbarcode_sn,6),2)='16' then "杭州"
         when right(left(a.fbarcode_sn,6),2)='01' then "深圳"
    else null end as fwarehouse,
    b.fserial_number,
    b.fissue_name,
    b.fanswer_name,
    b.fchongjian_answer_name,
    b.fbrand_name,
    b.type,
    b.fdetect_price,
    b.fchongjian_price
from gongdan_1 as a
left join chongjian_1 as b on a.fbarcode_sn=b.fserial_number
where b.fserial_number is not null

-- 批次2：31-60天前
-- 批次3：61-90天前
-- 批次4：91-120天前
-- 批次5：121-150天前
-- 批次6：151-180天前
-- 每个批次查询完成后，使用UNION ALL合并结果

