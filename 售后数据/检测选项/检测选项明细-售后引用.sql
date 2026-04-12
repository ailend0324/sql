select 
	a.ds,
    a.fserial_number,
    a.fissue_name,
    a.fanswer_name
from (
select 
    a.*,
    b.fserial_number,
    row_number()over(partition by b.fserial_number,a.fissue_name order by a.fdetect_record_id desc) as num
from dwd.dwd_detect_back_detection_issue_and_answer_v2 as a
left join dwd.dwd_detect_back_detect_detail as b on a.fdetect_record_id=b.fdetect_record_id
where left(b.fserial_number,2) not in ('NT','YZ','JM','BG','XZ')
and a.field_source='fdet_norm_snapshot'
and a.ds>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and b.fend_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
and b.fclass_name="手机"
) as a
where a.num=1
