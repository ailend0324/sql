-- 抽检差异项明细
with first_detect as (
    select 
        a.fend_time,
        a.fdetect_record_id, 
        a.fserial_number,
        a.fclass_name,
        a.fwarehouse_code,
        b.fissue_id,
        b.fissue_name,
        b.fanswer_id as fresult_id,
        b.fanswer_name as fresult_name
    from ( 
        select 
            fend_time,
            fdetect_record_id, 
            fserial_number,
            fclass_name,
            fwarehouse_code,
            row_number() over(partition by fserial_number order by fend_time desc) as num
        from dwd.dwd_detect_back_detect_detail
        where fdet_type = 0
            and fis_deleted = 0
            and freport_type = 0
            -- and to_date(fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
            and ds >= to_date(date_sub(from_unixtime(unix_timestamp()), 180))
    ) a
    left join (
        select 
            fdetect_record_id,
            fissue_id,
            fissue_name,
            fanswer_id,
            fanswer_name,
            field_source
        from dwd.dwd_detect_back_detection_issue_and_answer_v2
        where ds >= to_date(date_sub(from_unixtime(unix_timestamp()), 180))
    ) b on a.fdetect_record_id = b.fdetect_record_id
    where a.num = 1
        and b.field_source = 'fdet_norm_snapshot'
),
qc_detect as (
    select 
        a.fend_time as fqc_time,
        a.fdetect_record_id,
        a.fserial_number,
        b.fissue_id,
        b.fissue_name,
        b.fanswer_id as fresult_id,
        b.fanswer_name as fresult_name
    from (
        select 
            fend_time,
            fdetect_record_id,
            fserial_number,
            row_number() over(partition by fserial_number order by fend_time desc) as num
        from dwd.dwd_detect_back_detect_detail
        where fis_deleted = 0
            and (fdet_type = 1 or fdet_type = 2)
            -- and (freal_name like "%赖嘉琪%")
            and (freal_name like "%赖嘉琪%" 
                or freal_name like "%李伟雪%" 
                or freal_name like "%王封敏%" 
                or freal_name like "%王若桂%" 
                or freal_name like "%范嘉庆%" 
                or freal_name like "%陈斌%" 
                or freal_name like "%陈梓琦%" 
                or freal_name like "%周雨%" 
                or freal_name like "%郑君豪%"  
                or freal_name like "%叶思宁%")
            -- and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),180))
            and ds >= to_date(date_sub(from_unixtime(unix_timestamp()), 180))
    ) a
    left join (
        select 
            fdetect_record_id,
            fissue_id,
            fissue_name,
            fanswer_id,
            fanswer_name,
            field_source
        from dwd.dwd_detect_back_detection_issue_and_answer_v2
        where ds >= to_date(date_sub(from_unixtime(unix_timestamp()), 180))
    ) b on a.fdetect_record_id = b.fdetect_record_id
    where a.num = 1
        and b.field_source = 'fdet_norm_snapshot'
),
detect_three as (
    select 
        upper(fserial_number) as fserial_number,
        case when freal_name = "李俊峰" then "李俊锋" else freal_name end as fdetect_three_name,
        fcreate_time as fdetect_three_time
    from (
        select 
            a.fcreate_time,
            a.fserial_number,
            b.freal_name,
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
        from drt.drt_my33312_detection_t_det_task as a
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id = b.ftask_id
        where to_date(a.fend_time) >= to_date(date_sub(from_unixtime(unix_timestamp()), 180))
            and b.fdet_sop_task_name like "%外观%"
    ) t
    where num = 1
),
detect_three_pingmu as (
    select 
        upper(fserial_number) as fserial_number,
        case when freal_name = "李俊峰" then "李俊锋" else freal_name end as fdetect_three_name_pingmu,
        fcreate_time as fdetect_three_time_pingmu
    from (
        select 
            a.fcreate_time,
            a.fserial_number,
            b.freal_name,
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
        from drt.drt_my33312_detection_t_det_task as a
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id = b.ftask_id
        where to_date(a.fend_time) >= to_date(date_sub(from_unixtime(unix_timestamp()), 180))
            and b.fdet_sop_task_name like "%屏幕%"
            and b.fdet_sop_task_name != "外观屏幕"
    ) t
    where num = 1
)
select 
    a.fend_time,
    b.fqc_time,
    a.fserial_number,
    left(a.fserial_number, 2) as "渠道",
    a.fclass_name,
    case 
        when left(a.fserial_number, 3) like "%010%" 
            or left(a.fserial_number, 3) like "%020%" 
            or left(a.fserial_number, 3) like "%050%" then "验机"
        when left(a.fserial_number, 2) like "%BM%" then "帮卖"
        else "竞拍" 
    end as ftype,
    c.fdetect_three_name_pingmu as "屏幕检测人",
    d.fdetect_three_name as "外观检测人",
    a.fissue_name,
    a.fresult_id,
    a.fresult_name,
    b.fissue_name as fqc_fissue_name,
    b.fresult_id as fqc_fresult_id,
    b.fresult_name as fqc_fresult_name,
    case 
        when a.fissue_name like "%屏幕显示%" or a.fissue_name like "%副屏显示%" then "显示" 
        else "外观" 
    end as "模块所属",
    case when b.fresult_id < a.fresult_id then 1 else 0 end as "负差数",
    case when b.fresult_id > a.fresult_id then 1 else 0 end as "正差数",
    case 
        when a.fwarehouse_code = '12' then "东莞仓" 
        when right(left(a.fserial_number, 6), 2) = "16" or left(a.fserial_number, 3) = "020" then "杭州仓"  
        else "深圳仓" 
    end as fwarehouse_code
from first_detect as a
left join qc_detect as b on a.fserial_number = b.fserial_number and a.fissue_id = b.fissue_id
left join detect_three_pingmu as c on a.fserial_number = c.fserial_number
left join detect_three as d on a.fserial_number = d.fserial_number
where b.fserial_number is not null
    and (a.fissue_name like "%外观%" 
        or a.fissue_name like "%屏幕%" 
        or a.fissue_name like "%边框%" 
        or a.fissue_name like "%显示%" 
        or a.fissue_name like "%机身弯曲%" 
        or a.fissue_name like "%折叠屏转轴%" 
        or a.fissue_name like "%折叠屏保护膜%")
    and a.fissue_name not like "%维修%"
    and a.fissue_name not like "%光线%"
    and to_date(a.fend_time) >= to_date(date_sub(from_unixtime(unix_timestamp()), 180))
