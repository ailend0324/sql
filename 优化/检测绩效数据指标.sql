
-- 目的：产出“检测绩效数据指标”明细，用于绩效核算与过程分析
-- 使用方式：在 Hive/Impala/SparkSQL 等兼容环境直接执行；如需拉长/缩短时间范围，修改下方 params 的天数
-- 时间窗：统一采用“最近 N 天”，默认 35 天（可调），确保所有子查询口径一致
-- 主要来源表：
--   - drt.drt_my33310_detection_t_detect_record         主体检测记录
--   - drt.drt_my33312_detection_t_automation_det_record 自动化记录（分模块-自动化）
--   - drt.drt_my33312_detection_t_det_app_record        App 记录（分模块-App）
--   - drt.drt_my33312_detection_t_det_task              任务记录（外观/屏幕/拆修）
--   - drt.drt_my33312_detection_t_det_task_record       任务记录明细（取执行人）
--   - drt.drt_my33312_detection_t_detailed_photo        验机细节拍照（规则 id=47）
--   - drt.drt_my33310_detection_t_def_phone_task        缺陷照任务（含“有斑”识别）
-- 关键字段解释（选取部分）：
--   fdetect_time          检测日期（按主检测记录的 fend_time）
--   绩效年月              绩效归属月份（25号含以后算下月）
--   fdetect_one/two/three/four_*   分模块最近一次的人名与时间（自动化/App/外观/拆修）
--   fdetect_three_*_pingmu         屏幕模块的人名与时间
--   fbrand / 品牌(原始)    标准化品牌与原始品牌
--   所在地 / 业务类型 / 业务   按规则由单号与仓库映射
--   检测模板              由 Fdet_tpl 映射
--   邮寄回收检测量        是否计入“邮寄回收”口径（0/1）
--   是否纳入绩效          非公益则纳入
--   是否拍缺陷照          关联缺陷照任务是否存在
--   缺陷照-拍照有斑       2025-04-09 起“有斑”规则命中则为 1
--   验机细节拍照          是否存在 frules_id=47 的细节拍照
--   是否拆机              FisDisassembly=1 则为是
--   是否分模块            存在自动化一检记录则为是

-- 统一时间窗（可调）
with params as (
  select date_sub(from_unixtime(unix_timestamp()), 35) as dt_from
),

-- 一检（自动化）最近一次记录 + 人名修正
detect_one as (
select 
    upper(fserial_number) as fserial_number,
    case 
         when to_date(from_unixtime(fend_det_time))='2024-03-04' and freal_name='陈冬凡' then '周远鸿'
         when to_date(from_unixtime(fend_det_time))='2024-03-04' and freal_name='胡家华' then '黄成水'
         when to_date(from_unixtime(fend_det_time))='2024-03-02' and freal_name='陈冬凡' then '李浩宇'
         when to_date(from_unixtime(fend_det_time))='2024-03-05' and freal_name='陈冬凡' then '周远鸿'
         when to_date(from_unixtime(fend_det_time))='2025-01-26' and freal_name='黄雅如' then '兼职'
         when to_date(from_unixtime(fend_det_time))='2024-03-14' and freal_name='严俊' then '林广泽'
         when to_date(from_unixtime(fend_det_time))='2024-04-14' and freal_name='严俊' then '林广泽'
         when fbind_real_name is not null and fbind_real_name<>'' then fbind_real_name
         else freal_name 
    end as fdetect_one_name,
    from_unixtime(fend_det_time) as fdetect_one_time
from (
  select 
      a.fserial_number,
      a.fend_det_time,
      case when a.fuser_name='lijunfeng@huishoubao.com.cn' then '李俊锋' else b.freal_name end as freal_name,
      a.fbind_real_name,
      row_number()over(partition by upper(a.fserial_number) order by a.fend_det_time desc) as num
  from drt.drt_my33312_detection_t_automation_det_record a
  left join drt.drt_my33310_amcdb_t_user b on a.fuser_name=b.fusername
  cross join params p
  where a.fserial_number<>'' 
    and a.fserial_number is not null
    and to_date(from_unixtime(a.fend_det_time))>=to_date(p.dt_from)
) t
where num=1
),
-- 二检（App）最近一次记录 + 人名修正
detect_two as (
select 
    upper(fserial_number) as fserial_number,
    case 
         when fuser_name='lijunfeng@huishoubao.com.cn' then '李俊锋'
         when to_date(fcreate_time)='2024-03-04' and freal_name='陈冬凡' then '周远鸿'
         when to_date(fcreate_time)='2024-03-04' and freal_name='胡家华' then '黄成水'
         when to_date(fcreate_time) between '2025-01-24' and '2025-01-26' and freal_name='黄雅如' then '兼职'
         when freal_name='陈冬凡' and to_date(fcreate_time)='2024-05-13' and fbrand_name<>'Apple' then '李俊锋'
         when freal_name='周远鸿' and to_date(fcreate_time)='2024-05-15' then null
         else freal_name 
    end as fdetect_two_name,
    fcreate_time as fdetect_two_time
from (
  select 
      a.fcreate_time,
      a.fuser_name,
      a.fbrand_name,
      a.fserial_number,
      b.freal_name,
      row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
  from drt.drt_my33312_detection_t_det_app_record a
  left join drt.drt_my33310_amcdb_t_user b on a.fuser_name=b.fusername
  cross join params p
  where to_date(a.fcreate_time)>=to_date(p.dt_from)
    and a.fserial_number<>'' and a.fserial_number is not null
) t
where num=1
),
-- 外观模块 最近一次执行人
detect_three as (
select 
    upper(fserial_number) as fserial_number,
    case when freal_name='李俊峰' then '李俊锋' else freal_name end as fdetect_three_name,
    fcreate_time as fdetect_three_time
from (
    select 
        a.fcreate_time,
        a.fserial_number,
        b.freal_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task a
    left join drt.drt_my33312_detection_t_det_task_record b on a.ftask_id=b.ftask_id
    cross join params p
    where to_date(a.fend_time)>=to_date(p.dt_from)
      and b.fdet_sop_task_name like '%外观%'
) t
where num=1
),
-- 屏幕模块 最近一次执行人
detect_three_pingmu as (
select 
    upper(fserial_number) as fserial_number,
    case when freal_name='李俊峰' then '李俊锋' else freal_name end as fdetect_three_name_pingmu,
    fcreate_time as fdetect_three_time_pingmu
from (
    select 
        a.fcreate_time,
        a.fserial_number,
        b.freal_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task a
    left join drt.drt_my33312_detection_t_det_task_record b on a.ftask_id=b.ftask_id
    cross join params p
    where to_date(a.fend_time)>=to_date(p.dt_from)
      and b.fdet_sop_task_name like '%屏幕%'
      and b.fdet_sop_task_name<>'外观屏幕'
) t
where num=1
),
-- 拆修模块 最近一次执行人
detect_four as (
select 
    upper(fserial_number) as fserial_number,
    freal_name as fdetect_four_name,
    fcreate_time as fdetect_four_time
from (
    select 
        a.fcreate_time,
        a.fserial_number,
        b.freal_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task a
    left join drt.drt_my33312_detection_t_det_task_record b on a.ftask_id=b.ftask_id
    cross join params p
    where to_date(a.fend_time)>=to_date(p.dt_from)
      and b.fdet_sop_task_name like '%拆修%'
) t
where num=1
),
-- 验机细节拍照（规则 id=47）最近一次
yanji_paizhao as (
select 
    fcreate_time,
    fserial_number,
    freal_name
from (
    select 
        fcreate_time,
        fserial_number,
        freal_name,
        row_number()over(partition by fserial_number order by fcreate_time desc) as num
    from drt.drt_my33312_detection_t_detailed_photo 
    cross join params p
    where fcreate_time>=to_date(p.dt_from)
      and frules_id=47
) t
where num=1
)

select 
    -- 核心时间与绩效归属
    to_date(t.fend_time) as fdetect_time,
    t.fend_time,
    case when day(t.fend_time)>=25 
         then concat(cast(year(t.fend_time) as string), cast(month(t.fend_time)+1 as string)) 
         else concat(cast(year(t.fend_time) as string), cast(month(t.fend_time) as string)) end as '绩效年月',
    -- 分模块近一次执行人与时间
    d.fdetect_one_time,
    if((to_date(d.fdetect_one_time) between '2024-02-26' and '2024-02-27' or to_date(d.fdetect_one_time)='2024-02-29') and d.fdetect_one_name='陈冬凡','高华铎',
       if(to_date(d.fdetect_one_time) between '2024-02-26' and '2024-02-27' and d.fdetect_one_name='赖云龙','刘海其',d.fdetect_one_name)) as fdetect_one_name,
    e.fdetect_two_time,
    e.fdetect_two_name,
    f.fdetect_three_time,
    f.fdetect_three_name,
    g.fdetect_four_time,
    g.fdetect_four_name,
    h.fdetect_three_time_pingmu,
    h.fdetect_three_name_pingmu,
    -- 品牌与地理/业务映射
    case when t.fbrand_name like '%苹果%' then '苹果' else '安卓' end as fbrand,
    t.fbrand_name as '品牌(原始)',
    upper(t.fserial_number) as fserial_number,
    case 
        when t.Fwarehouse_code='12' or left(t.fserial_number,3) like '%050%' then '东莞'
        when left(t.fserial_number,3) like '%020%' or right(left(t.fserial_number,6),2)='16' then '杭州'
        else '深圳' end as '所在地',
    case when left(t.fserial_number,3) like '%020%' or left(t.fserial_number,3) like '%010%' or left(t.fserial_number,3) like '%050%' then '验机' else '回收' end as '业务类型',
    case when left(t.fserial_number,3) like '%020%' or left(t.fserial_number,3) like '%010%' or left(t.fserial_number,3) like '%050%' then '验机'
         when left(t.fserial_number,2)='BM' then '寄卖'
         when (left(t.fserial_number,2)='TL' or (left(t.fserial_number,2)='CG' and to_date(t.fend_time)>='2024-12-01')) then '太力'
         else '回收' end as '业务',
    -- 产品与价格
    t.fclass_name, 
    t.fdetect_price/100 as fdetect_price,
    case when t.fproduct_name in (
        '三星 Galaxy Fold',
        '三星 Galaxy Fold（5G）',
        '三星 W20（5G）',
        '华为 Mate Xs（5G）',
        '三星 Galaxy Z  Fold2（5G）',
        '三星 W21（5G）',
        '小米 MIX FOLD（5G）',
        '三星 Galaxy Z Fold3（5G）',
        '三星 W22（5G）',
        '荣耀 Magic V（5G）',
        'vivo X Fold（5G）',
        '小米 MIX FOLD2（5G）',
        '三星 Galaxy Z Fold4（5G）',
        'vivo X Fold+（5G）',
        '三星 W23（5G）',
        '荣耀 Magic Vs（5G）',
        '荣耀 Magic Vs 至臻版',
        '华为 Mate X3',
        '华为 Mate X2 典藏版',
        '华为 Mate X3 典藏版',
        'vivo X Fold 2（5G）',
        '谷歌 Pixel Fold',
        '荣耀 Magic V2（5G）',
        '荣耀 Magic V2 至臻版',
        '三星 Galaxy Z Fold5（5G）',
        '小米 MIX FOLD3（5G）',
        '华为 Mate X5',
        '华为 Mate X5 典藏版',
        '三星 W24（5G）',
        '荣耀 Magic Vs 2（5G）',
        'OPPO Find N3（5G）',
        '荣耀 Magic V2 RSR 保时捷设计',
        'vivo X Fold 3',
        'vivo X Fold3 Pro',
        '荣耀 Magic V3',
        '三星 Galaxy Z Fold6（5G）',
        '小米 MIX FOLD 4',
        '荣耀 Magic Vs 3',
        '三星 Galaxy Z Fold6（5G）',
        '小米 MIX FOLD 4',
        '荣耀 Magic Vs 3',
        '华为 Mate X2（5G）',
        '华为 Mate X2（4G）',
        '一加 Open',
        '华为 Mate XT 非凡大师',
        '三星 W25',
        '华为 Mate X6',
        '华为 Mate X6 典藏版',
        'OPPO Find N5',
        'vivo X Fold 5'
    ) then '是' else '否' end as '是否折叠屏',
    t.fproduct_name, 
    t.freal_name, 
    -- 渠道与项目修正：闲鱼验机与支付宝小程序
    case when b.fproject_name='自有项目' and b.fchannel_name='支付宝小程序' then '合作项目' 
         when left(t.fserial_number,3) like '%020%' or left(t.fserial_number,3) like '%010%' or left(t.fserial_number,3) like '%050%' then '合作项目'
         else b.fproject_name end as fproject_name,
    case when left(t.fserial_number,3) like '%020%' or left(t.fserial_number,3) like '%010%' or left(t.fserial_number,3) like '%050%' then '闲鱼验机' else b.fchannel_name end as fchannel_name,
    -- 模板/纳入与各类拍照、拆机标记
    case 
      when t.Fdet_tpl = 0 then '竞拍检测'
      when t.Fdet_tpl = 1 then '大检测'
      when t.Fdet_tpl = 2 then '竞拍检测'
      when t.Fdet_tpl = 6 then '闲鱼寄卖plus'
      when t.Fdet_tpl = 7 then '竞拍检测'
      when t.Fdet_tpl = 4 then '销售检测'
      else '其他' end as '检测模板',
    case when t.fproduct_name like '%公益%' then 0
         when left(t.fserial_number,2) <>'BM' and left(t.fserial_number,2) <>'JM' and left(t.fserial_number,2) <>'XZ' and t.Fdet_tpl <> 4 
              and left(t.fserial_number,3) not like '%020%' and left(t.fserial_number,3) not like '%010%' and left(t.fserial_number,3) not like '%050%' then 1 else 0 end as '邮寄回收检测量',
    case when t.fproduct_name like '%公益%' then '否' else '是' end as '是否纳入绩效',
    case when c.fshooting_time is not null and left(c.fseries_number,1)<>'0' and left(c.fseries_number,2)<>'BM' then 1 else 0 end as '是否拍缺陷照',
    case when c.fshooting_time>='2025-04-09' and c.fyouban=1 and left(c.fseries_number,1)<>'0' then 1 else 0 end as '缺陷照-拍照有斑',
    case when i.fcreate_time is not null then 1 else 0 end as '验机细节拍照',
    case when t.FisDisassembly=1 then '是' else '否' end as '是否拆机',
    case when d.fdetect_one_time is not null then '是' else '否' end as '是否分模块'
from (
  -- 与原始写法一致，但统一使用 35 天窗口并拆成双向取首尾
  select *, row_number() over(partition by upper(fserial_number) order by fend_time asc)  as num
  from drt.drt_my33310_detection_t_detect_record 
  cross join params p
  where fdet_type=0 and fis_deleted=0 and freport_type=0 and fverdict<>'测试单'
    and to_date(fend_time) >= to_date(p.dt_from)
    and to_date(fend_time) <= to_date(from_unixtime(unix_timestamp()))
    and left(fserial_number,3) not like '%020%' and left(fserial_number,2)<> 'CG'
  union all
  select *, row_number() over(partition by upper(fserial_number) order by fend_time desc) as num
  from drt.drt_my33310_detection_t_detect_record 
  cross join params p
  where fdet_type=0 and fis_deleted=0 and freport_type=0 and fverdict<>'测试单'
    and to_date(fend_time) >= to_date(p.dt_from)
    and to_date(fend_time) <= to_date(from_unixtime(unix_timestamp()))
    and (left(fserial_number,3) like '%020%' or left(fserial_number,2)='CG')
) t
left join dws.dws_hs_order_detail b on upper(t.fserial_number)=b.fseries_number
left join (
    select 
        upper(a.fserial_number) as fseries_number,
        a.fshooting_time,
        b.freal_name,
        case when a.frules_option like '%19777%' then 1 else null end as fyouban,
        row_number()over(partition by upper(a.fserial_number) order by a.fshooting_time desc) as num
    from drt.drt_my33310_detection_t_def_phone_task a
    left join drt.drt_my33310_amcdb_t_user b on a.fshooting_user_name=b.fusername
    cross join params p
    where (a.fstatus=3 or a.fstatus=2)
      and to_date(a.fshooting_time) >= to_date(p.dt_from)
) c on upper(t.fserial_number)=c.fseries_number and t.freal_name=c.freal_name and c.num=1
left join detect_one d on upper(t.fserial_number)=d.fserial_number
left join detect_two e on upper(t.fserial_number)=e.fserial_number
left join detect_three f on upper(t.fserial_number)=f.fserial_number
left join detect_four g on upper(t.fserial_number)=g.fserial_number
left join detect_three_pingmu h on upper(t.fserial_number)=h.fserial_number
left join yanji_paizhao i on upper(t.fserial_number)=i.fserial_number
where t.num=1
order by t.fend_time desc