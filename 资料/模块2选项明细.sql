/*
📱 模块二检测选项明细
用途：查看模块二检测的详细结果，特别关注功能性检测项目
就像查看"手机功能检测的详细体检报告"
*/

with detect_two as (                     -- 第一步：获取模块二的检测人员和时间信息
select 
    fserial_number,                      -- 设备序列号
    case when fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" 
         when to_date(fcreate_time)='2024-03-04' and freal_name="陈冬凡" then "周远鸿" 
         when to_date(fcreate_time)='2024-03-04' and freal_name="胡家华" then "黄成水"
    else freal_name end as fdetect_two_name,  -- 模块二检测人员（含特殊日期调整）
    fcreate_time as fdetect_two_time     -- 模块二检测时间
from (
select 
    a.fcreate_time,                      -- 检测创建时间
    a.fuser_name,                        -- 用户名
    a.fserial_number,                    -- 设备序列号
    b.freal_name,                        -- 真实姓名
    row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    -- ↑ 为每个设备的模块二检测记录编号，按时间倒序，取最新一次
    -- upper()确保序列号大小写统一
    
from drt.drt_my33312_detection_t_det_app_record as a  -- 从"检测APP记录表"（模块二）
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername  -- 关联用户表
where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),90))  -- 最近90天
and fserial_number!=""                   -- 序列号不为空
and fserial_number is not null
)t
where num=1                              -- 只保留每个设备最新的模块二检测记录
)

-- 主查询：获取完整的检测信息和检测结果
select
    a.*,                                 -- 模块二检测的基本信息
    t.fclass_name,                       -- 设备类别（如手机、平板等）
    case when left(a.fserial_number,2)="BM" then "寄卖" 
         when left(a.fserial_number,1)="0" then "验机" 
    else "回收" end as ftype,            -- 业务类型分类
    case when t.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,  -- 品牌分类
    case when left(a.fserial_number,2) in ('02') or right(left(a.fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    -- ↑ 仓库位置判断
    b.fissue_name,                       -- 检测项目名称
    b.fanswer_name                       -- 检测结果
from detect_two as a                     -- 模块二检测信息
left join (select 
            *,
            row_number() over(partition by fserial_number order by fend_time desc) as num
            from drt.drt_my33310_detection_t_detect_record  -- 检测记录主表
            where fis_deleted=0          -- 记录未删除
            and fdet_type=0              -- 检测类型为0
            and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),90))  -- 最近90天
           ) t on a.fserial_number=t.fserial_number  -- 关联检测记录
left join dwd.dwd_detect_back_detection_issue_and_answer_v2 as b on t.frecord_id=b.fdetect_record_id
-- ↑ 关联检测问题答案表，获取具体的检测项目和结果

where t.num=1                            -- 只要每个设备最新的检测记录
and (b.fissue_name like "%听筒%" or b.fissue_name like "%扬声器%" or b.fissue_name like '%Y3声音%' 
     or b.fissue_name like "%触屏%" or b.fissue_name like "%通话%" or b.fissue_name like "%触摸%" 
     or b.fissue_name like "%通信%" or b.fissue_name like "%Face ID%" or b.fissue_name like "%面容%" 
     or b.fissue_name like "%NFC%" or b.fissue_name like "%面部识别%" or b.fissue_name like "%麦克风%" 
     or b.fissue_name like "%SIM%")
-- ↑ 只关注这些功能性检测项目：
-- 听筒、扬声器、声音、触屏、通话、触摸、通信、Face ID、面容识别、NFC、面部识别、麦克风、SIM卡
-- 这些都是手机的核心功能，检测结果对定价很重要

and b.ds >=to_date(date_sub(from_unixtime(unix_timestamp()),90))  -- 答案数据也要在90天内
and b.field_source='fdet_norm_snapshot'  -- 只要标准检测快照数据
and t.fclass_name="手机"                 -- 只看手机设备

/*
💡 简单解释：
这个查询就像手机功能检测的详细报告：
"找出最近3个月模块二检测的所有手机，
看看每台手机的核心功能（听筒、触屏、Face ID等）检测结果如何"

🔍 关注的功能检测项目：
- 📞 通话功能：听筒、麦克风、通话
- 👆 触控功能：触屏、触摸  
- 🔊 声音功能：扬声器、声音
- 🔐 安全功能：Face ID、面容识别、面部识别
- 📶 通信功能：通信、NFC、SIM卡

📊 为什么重要：
这些功能的好坏直接影响手机的回收价格和销售价格
就像体检中的心跳、血压等关键指标

🎯 业务价值：
- 了解不同功能的故障率
- 为定价提供依据
- 识别检测质量问题
- 优化检测流程

💡 技术细节：
- 通过模块二APP检测人员信息
- 关联检测记录获取完整设备信息  
- 筛选出核心功能检测项目
- 确保数据的时效性（90天内）
*/
