select 
    month(a.fend_time) as "月份",
    to_date(a.fend_time) as fdetect_time,
    a.fserial_number,
    count(case when b.fissue_name='屏幕外观划痕' and b.fanswer_name="无" then a.fserial_number else null end) as '屏幕外观划痕-无',
    count(case when b.fissue_name='外壳划痕' and b.fanswer_name="无" then a.fserial_number else null end) as '外壳划痕-无',
    count(case when b.fissue_name='外壳磕碰' and b.fanswer_name="无" then a.fserial_number else null end) as '外壳磕碰-无',
    count(case when b.fissue_name='外壳印渍' and b.fanswer_name="无" then a.fserial_number else null end) as '外壳印渍-无'
from (select 
            *,
            row_number() over(partition by fserial_number order by fend_time desc) as num
        from drt.drt_my33310_detection_t_detect_record 
        where fdet_type=0
        and fis_deleted=0
    	and fverdict<>"测试单"
        and to_date(fend_time) >='2023-09-01'
        and left(fserial_number,2) in ('01','02')) as a
left join dwd.dwd_detect_back_detection_issue_and_answer_v2 as b on a.frecord_id=b.fdetect_record_id
where left(a.fserial_number,2) in ('01','02')
and a.num=1
and b.field_source='fdet_norm_snapshot'
and b.ds>='2023-09-01'
and b.fissue_name in ('外壳划痕','外壳磕碰','屏幕外观划痕','外壳印渍')
and b.fanswer_name='无'
group by 1,2,3
