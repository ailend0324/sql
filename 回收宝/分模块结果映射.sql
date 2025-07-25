select 
    to_date(fauto_detect_time),
    fserial_number,
    case when fsource_table like "%t_det_app_record%" then "模块二" else "模块一" end as "模块类型",
    fissue_name,
    fanswer_name
from dwd.dwd_detect_auto_detection_issue_and_answer
where to_date(fauto_detect_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),70))
and fanswer_name is not null
and (fautomation_det_record_id>0 or fautomation_det_record_id is null) 
