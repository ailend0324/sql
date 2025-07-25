--抽检差异项明细
with first_detect as(
select 
        a.fend_time,
        a.fdetect_record_id, 
        a.fserial_number,
  		a.fclass_name
from( 
    select 
        fend_time,
        fdetect_record_id, 
        fserial_number,
  		fclass_name,
        row_number()over(partition by fserial_number order by fend_time desc) as num
    from  dwd.dwd_detect_back_detect_detail
    where fdet_type=0
    and fis_deleted=0
    and freport_type=0
    and to_date(fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180)))a
where a.num=1
),
qc_detect as(
        select 
            a.fend_time as fqc_time,
            a.fdetect_record_id,
            a.fserial_number
        from (select 
                fend_time,
                fdetect_record_id,
                fserial_number,
                row_number()over(partition by fserial_number order by fend_time desc)as num
            from dwd.dwd_detect_back_detect_detail
            where fis_deleted=0
            and (fdet_type=1 or fdet_type=2)
            -- and (freal_name like "%赖嘉琪%")
             and (freal_name like "%赖嘉琪%" or freal_name like "%李伟雪%" or freal_name like "%王封敏%" or freal_name like "%王若桂%" or freal_name like "%陈斌%" or freal_name like "%范嘉庆%" or freal_name like "%陈梓琦%" or freal_name like "%周雨%" or freal_name like "%郑君豪%")

            and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),180)))a
        where a.num=1
)
select 
    to_date(a.fend_time),
    left(a.fserial_number,2) as "渠道",
    a.fclass_name,
    case when left(a.fserial_number,3) like "%010%" or left(a.fserial_number,3) like "%020%" or left(a.fserial_number,3) like "%050%" then "验机"
         when left(a.fserial_number,2) like "%BM%" then "帮卖"
    else "竞拍" end as ftype,
    count(*) as "检测数",
    count(case when b.fserial_number is not null then a.fserial_number else null end) as "抽检数"
from first_detect as a
left join qc_detect as b on a.fserial_number=b.fserial_number
where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
group by 1,2,3,4
