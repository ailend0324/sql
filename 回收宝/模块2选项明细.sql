with detect_two as (
select 
    fserial_number,
    case when fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" 
         when to_date(fcreate_time)='2024-03-04' and freal_name="陈冬凡" then "周远鸿" 
         when to_date(fcreate_time)='2024-03-04' and freal_name="胡家华" then "黄成水"
    else freal_name end as fdetect_two_name,
    fcreate_time as fdetect_two_time
from (
select 
    a.fcreate_time,
    a.fuser_name,
    a.fserial_number,
    b.freal_name,
    row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
from drt.drt_my33312_detection_t_det_app_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),90))
and fserial_number!=""
and fserial_number is not null)t
where num=1
)
select
    a.*,
    t.fclass_name,
    case when left(a.fserial_number,2)="BM" then "寄卖" 
         when left(a.fserial_number,1)="0" then "验机" else "回收" end as ftype,
    case when t.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
    case when left(a.fserial_number,2) in ('02') or right(left(a.fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    b.fissue_name,
    b.fanswer_name
from detect_two as a 
left join (select 
            *,
            row_number() over(partition by fserial_number order by fend_time desc) as num
            from drt.drt_my33310_detection_t_detect_record 
            where fis_deleted=0
            and fdet_type=0
            and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),90))) t on a.fserial_number=t.fserial_number
left join dwd.dwd_detect_back_detection_issue_and_answer_v2 as b on t.frecord_id=b.fdetect_record_id
where t.num=1 
and (b.fissue_name like "%听筒%" or b.fissue_name like "%扬声器%" or b.fissue_name like '%Y3声音%' or b.fissue_name like "%触屏%" or b.fissue_name like "%通话%" or b.fissue_name like "%触摸%" or b.fissue_name like "%通信%" or b.fissue_name like "%Face ID%" or b.fissue_name like "%面容%" or b.fissue_name like "%NFC%" or b.fissue_name like "%面部识别%" or b.fissue_name like "%麦克风%" or b.fissue_name like "%SIM%")
and b.ds >=to_date(date_sub(from_unixtime(unix_timestamp()),90))
and b.field_source='fdet_norm_snapshot'
and t.fclass_name="手机"
--and left(a.fserial_number,1)!="0"
