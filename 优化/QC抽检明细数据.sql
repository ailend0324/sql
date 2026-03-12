with 
-- 【配置部分】在这里修改天数，一处修改，全文生效
config as (
    select 
        --在此处将 365 修改为你想要的天数，例如 180 或 90
        to_date(date_sub(from_unixtime(unix_timestamp()), 365)) as start_date 
),

first_detect as (
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
            t.fend_time,
            t.fdetect_record_id, 
            t.fserial_number,
            t.fclass_name,
            t.fwarehouse_code,
            row_number() over(partition by t.fserial_number order by t.fend_time desc) as num
        from dwd.dwd_detect_back_detect_detail t
        cross join config -- 引入配置
        where t.fdet_type = 0
            and t.fis_deleted = 0
            and t.freport_type = 0
            and t.ds >= config.start_date -- 使用配置时间
    ) a
    left join (
        select 
            t.fdetect_record_id,
            t.fissue_id,
            t.fissue_name,
            t.fanswer_id,
            t.fanswer_name,
            t.field_source
        from dwd.dwd_detect_back_detection_issue_and_answer_v2 t
        cross join config -- 引入配置
        where t.ds >= config.start_date -- 使用配置时间
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
            t.fend_time,
            t.fdetect_record_id, 
            t.fserial_number,
            row_number() over(partition by t.fserial_number order by t.fend_time desc) as num
        from dwd.dwd_detect_back_detect_detail t
        cross join config -- 引入配置
        where t.fis_deleted = 0
            and (t.fdet_type = 1 or t.fdet_type = 2)
            and (t.freal_name like "%赖嘉琪%" 
                or t.freal_name like "%李伟雪%" 
                or t.freal_name like "%王封敏%" 
                or t.freal_name like "%王若桂%" 
                or t.freal_name like "%范嘉庆%" 
                or t.freal_name like "%陈斌%" 
                or t.freal_name like "%陈梓琦%" 
                or t.freal_name like "%周雨%" 
                or t.freal_name like "%郑君豪%"  
                or t.freal_name like "%叶思宁%"
                or t.freal_name like "%刘凤瑊%"
                or t.freal_name like "%马显岩%"
                or t.freal_name like "%赵语放%"
                )
            and t.ds >= config.start_date -- 使用配置时间
    ) a
    left join (
        select 
            t.fdetect_record_id,
            t.fissue_id,
            t.fissue_name,
            t.fanswer_id,
            t.fanswer_name,
            t.field_source
        from dwd.dwd_detect_back_detection_issue_and_answer_v2 t
        cross join config -- 引入配置
        where t.ds >= config.start_date -- 使用配置时间
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
        cross join config -- 引入配置
        where to_date(a.fend_time) >= config.start_date -- 使用配置时间
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
        cross join config -- 引入配置
        where to_date(a.fend_time) >= config.start_date -- 使用配置时间
            and b.fdet_sop_task_name like "%屏幕%"
            and b.fdet_sop_task_name != "外观屏幕"
    ) t
    where num = 1
)

-- 主查询
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
cross join config -- 主查询也引入配置
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
    and to_date(a.fend_time) >= config.start_date -- 最后这里也使用配置时间