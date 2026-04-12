-- 重检差异明细 - 最终版（单一权威版本）
-- 目标：保持180天范围不变，但显著降低扫描量
-- 策略：两阶段
--   1) diff_sn: 仅计算“存在重检答案差异”的序列号小集合（含必要维度）
--   2) 用小集合回查工单与检测人员信息，避免对原始大表重复宽扫描

-- 阶段1：差异序列号集合（体量最小、放最前，便于后续广播/小表驱动）
with diff_sn as (
  select 
      a1.fserial_number,
      a1.fissue_name,
      a1.fanswer_name as fanswer_name_det,
      a2.fanswer_name as fanswer_name_rechk,
      d.fbrand_name,
      d.fproduct_name,
      d.fend_time,
      d.fengineer_name,
      d.fdetect_price/100 as fdetect_price,
      d2.fdetect_price/100 as fchongjian_price
  from (
    select 
        i.fissue_name,
        i.fanswer_name,
        det.fserial_number,
        row_number()over(partition by det.fserial_number,i.fissue_name order by i.fdetect_record_id desc) as rn
    from dwd.dwd_detect_back_detection_issue_and_answer_v2 i
    left join dwd.dwd_detect_back_detect_detail det on i.fdetect_record_id=det.fdetect_record_id
    where i.field_source='fdet_norm_snapshot'
      and i.ds>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
      and left(det.fserial_number,2) not in ('BB','NT','YZ','JM','BG','XZ')
      and det.fclass_name = '手机'
      and det.fdet_type = 0
      and det.fserial_number is not null
  ) a1
  inner join (
    select 
        i.fissue_name,
        i.fanswer_name,
        det.fserial_number,
        row_number()over(partition by det.fserial_number,i.fissue_name order by i.fdetect_record_id desc) as rn
    from dwd.dwd_detect_back_detection_issue_and_answer_v2 i
    left join dwd.dwd_detect_back_detect_detail det on i.fdetect_record_id=det.fdetect_record_id
    where i.field_source='fdet_norm_snapshot'
      and i.ds>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
      and left(det.fserial_number,2) not in ('BB','NT','YZ','JM','BG','XZ')
      and det.fclass_name = '手机'
      and det.fdet_type = 2
      and det.fserial_number is not null
  ) a2
    on a1.fserial_number = a2.fserial_number and a1.fissue_name = a2.fissue_name
  left join dwd.dwd_detect_back_detect_detail d
    on a1.fserial_number = d.fserial_number and d.fdet_type=0
  left join dwd.dwd_detect_back_detect_detail d2
    on a2.fserial_number = d2.fserial_number and d2.fdet_type=2
  where a1.rn = 1
    and a2.rn = 1
    and a1.fanswer_name <> a2.fanswer_name
),

-- 阶段2：工单与检测人员只回查差异集合中的序列号
gongdan as (
  select 
      to_date(from_unixtime(w.fadd_time)) as fadd_time,
      w.fbarcode_sn,
      case when u.freal_name in ("黄奕锋","周晓薇","郑春玲","梁椿灏","何嫣红","林宁","徐小利","朱小露",'苍雅婷','叶志','林晓雪') then "议价组" else "前端咨询" end as fgroup
  from drt.drt_my33310_csrdb_t_works w
  left join drt.drt_my33310_csrdb_t_works_config_appeal ap on w.fappeal_type2=ap.fid
  left join drt.drt_my33310_amcdb_t_user u on w.fadd_user=u.fusername
  where ap.fcontent in ('找机重检','提供照片')
    and to_date(from_unixtime(w.fadd_time))>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
    and w.forder_system=2
    and w.fduty_content!="无效工单"
    and w.fbarcode_sn in (select fserial_number from diff_sn)
),

det_one as (
  select fserial_number, fdetect_one_name
  from (
    select 
      a.fserial_number,
      b.freal_name as fdetect_one_name,
      row_number() over(partition by upper(a.fserial_number) order by a.fend_det_time desc) as rn
    from drt.drt_my33312_detection_t_automation_det_record a
    left join drt.drt_my33310_amcdb_t_user b on a.fuser_name=b.fusername
    where to_date(from_unixtime(a.fend_det_time))>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
      and a.fserial_number in (select fserial_number from diff_sn)
  ) t
  where rn=1
),

det_two as (
  select fserial_number, fdetect_two_name
  from (
    select 
      a.fserial_number,
      b.freal_name as fdetect_two_name,
      row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as rn
    from drt.drt_my33312_detection_t_det_app_record a
    left join drt.drt_my33310_amcdb_t_user b on a.fuser_name=b.fusername
    where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
      and a.fserial_number in (select fserial_number from diff_sn)
  ) t
  where rn=1
),

det_three as (
  select fserial_number, fdetect_three_name
  from (
    select 
      a.fserial_number,
      r.freal_name as fdetect_three_name,
      row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as rn
    from drt.drt_my33312_detection_t_det_task a
    left join drt.drt_my33312_detection_t_det_task_record r on a.ftask_id=r.ftask_id
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
      and r.fdet_sop_task_name like "%外观%"
      and a.fserial_number in (select fserial_number from diff_sn)
  ) t
  where rn=1
),

det_four as (
  select fserial_number, fdetect_four_name
  from (
    select 
      a.fserial_number,
      r.freal_name as fdetect_four_name,
      row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as rn
    from drt.drt_my33312_detection_t_det_task a
    left join drt.drt_my33312_detection_t_det_task_record r on a.ftask_id=r.ftask_id
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
      and r.fdet_sop_task_name like "%拆修%"
      and a.fserial_number in (select fserial_number from diff_sn)
  ) t
  where rn=1
)

select 
  g.fadd_time,
  g.fbarcode_sn,
  case when right(left(g.fbarcode_sn,6),2)='16' then "杭州"
       when right(left(g.fbarcode_sn,6),2)='01' then "深圳"
       else null end as fwarehouse,
  d.fproduct_name,
  d.fbrand_name,
  d.fend_time,
  d.fengineer_name,
  ds.fissue_name,
  ds.fanswer_name_det,
  ds.fanswer_name_rechk,
  ds.fdetect_price,
  ds.fchongjian_price,
  det1.fdetect_one_name,
  det2.fdetect_two_name,
  det3.fdetect_three_name,
  det4.fdetect_four_name
from diff_sn ds
left join dwd.dwd_detect_back_detect_detail d on ds.fserial_number=d.fserial_number and d.fdet_type=0
left join gongdan g on ds.fserial_number=g.fbarcode_sn
left join det_one det1 on ds.fserial_number=det1.fserial_number
left join det_two det2 on ds.fserial_number=det2.fserial_number
left join det_three det3 on ds.fserial_number=det3.fserial_number
left join det_four det4 on ds.fserial_number=det4.fserial_number
-- 只输出真正有差异的记录，输出行数由diff_sn控制

