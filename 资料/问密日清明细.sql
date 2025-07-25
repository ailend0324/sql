with wenmi as (
    select 
    *
from(
    select 
    from_unixtime(a.Fupdate_time) as Fupdate_time,
    from_unixtime(a.flast_time) as Flast_time,
    from_unixtime(a.Fadd_time) as Fadd_time,
    from_unixtime(a.fcompletion_time) as fcompletion_time,
    a.fbarcode_sn,
    a.forder_id,
    a.fupdate_user,
    c.fsender_phone,
    a.fappeal_content,
    c.fsend_back_time, 
    c.fclass_name,
    c.fdetect_time, 
    c.fproject_name,
    c.fchannel_name,
    d.fwarehouse_number,
    a.fwork_status,
    from_unixtime(b.ffeedback_time) as ffeedback_time, 
    row_number() over(partition by a.fbarcode_sn order by a.Fadd_time asc) as num
from drt.drt_my33310_csrdb_t_works as a
left join drt.drt_my33310_csrdb_t_work_device_pwd_consulting as b on a.fid=b.fwork_id
left join dws.dws_hs_order_detail as c on a.fbarcode_sn=c.fseries_number
left join dws.dws_instock_details as d on a.fbarcode_sn=d.fseries_number
where a.fwork_type=4
and from_unixtime(a.Fadd_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),500))
and a.fwork_source<>3
and a.fappeal_type1<>0
and left(a.fbarcode_sn,2) not in ('01','02')
and a.fduty_content not like "%无效工单%"
and a.fappeal_content!='问密类型:无法开机/logo页/充不进电'
and a.fappeal_content!='问密类型:系统无型号'
)t where num=1
),
waihu as (
select 
    --fdate,
    fcustomer_number,
    min(fdate) as ffirst_call_time,
    count(*) as fcall_num
from ods.ods_kf_tianrun_describe_cdr_ob 
where fdate>=to_date(date_sub(from_unixtime(unix_timestamp()),500))
and fclient_name in ("杨香英","钟小慧","黄丽萍","田新月","赵婷婷","龚娟","严俊","郑静敏")
-- and fstatus='双方接听'
group by 1
),
remark as (
select 
    forder_id,
    foperator_name,
    fcreate_time
from (
    select 
        *,
        row_number()over(partition by forder_id order by fcreate_time asc) as num
    from drt.drt_my33310_recycle_t_order_remark
    where to_date(fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),500))
    and foperator_name in ('黄丽萍','郑静敏','杨香英','钟小慧','龚娟','田新月','赵婷婷')
)t
where num=1
)
select 
    a.fadd_time,
    a.fcompletion_time,
    a.ffeedback_time,
    a.Fupdate_time,
    a.fsend_back_time,
    a.fdetect_time,
    a.fclass_name,
    a.fwarehouse_number,
    a.fproject_name,
    a.fchannel_name,
    left(a.fbarcode_sn,2) as type,
    a.fbarcode_sn,
    a.fupdate_user,
    a.fwork_status,
    a.fsender_phone,
    b.ffirst_call_time,
    b.fcall_num,
    case when a.fsend_back_time is not null then null 
         when to_date(a.fadd_time)=to_date(a.fdetect_time) then null 
         when to_date(a.fadd_time)=to_date(c.fcreate_time) then null
         when to_date(a.fadd_time)=to_date(a.fcompletion_time) then null
         when to_date(a.fadd_time)=to_date(a.Fupdate_time) and a.fwork_status=40 then null
         when to_date(a.fadd_time)=to_date(a.ffeedback_time) and hour(a.ffeedback_time)>=17 then null 
         when to_date(a.fadd_time)=to_date(a.ffeedback_time) and hour(a.ffeedback_time)<17 and (b.ffirst_call_time is not null and  to_date(a.fadd_time)>=to_date(b.ffirst_call_time) or (b.ffirst_call_time is null and to_date(a.fadd_time)=to_date(a.fdetect_time))) then null
         when to_date(a.fadd_time)<to_date(a.ffeedback_time) and b.ffirst_call_time is not null and to_date(a.fadd_time)>=to_date(b.ffirst_call_time) then null
         when b.ffirst_call_time is not null and to_date(a.fadd_time)>=to_date(b.ffirst_call_time) then null 
         when (hour(a.fadd_time)>=18 or (hour(a.fadd_time)=17 and minute(a.fadd_time)>=30)) then null 
    else 1 end as "未完成日清数"
from wenmi as a
left join waihu as b on a.fsender_phone=b.fcustomer_number
left join remark as c on a.forder_id=c.forder_id


