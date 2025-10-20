/*
QC质量控制数据统计
用途：统计质量检查的抽检比例和数量
就像统计"产品质检中抽查了多少产品，抽检比例是多少"
*/

-- 抽检差异项明细
with first_detect as(                   -- 第一步：获取首次检测数据
select 
        a.fend_time,                     -- 检测结束时间
        a.fdetect_record_id,             -- 检测记录ID
        a.fserial_number,                -- 设备序列号
  		a.fclass_name                    -- 设备类别
from( 
    select 
        fend_time,
        fdetect_record_id, 
        fserial_number,
  		fclass_name,
        row_number()over(partition by fserial_number order by fend_time desc) as num
        -- ↑ 为每个设备的检测记录编号，按结束时间倒序，取最新一次
        -- 确保每个设备只统计一次最新的检测
        
    from  dwd.dwd_detect_back_detect_detail  -- 从"检测详情表"
    where fdet_type=0                    -- 检测类型为0（正常检测）
    and fis_deleted=0                    -- 记录未删除
    and freport_type=0                   -- 报告类型为0（正常报告）
    and to_date(fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))  -- 最近180天
)a
where a.num=1                            -- 只保留每个设备最新的检测记录
),

qc_detect as(                           -- 第二步：获取QC抽检数据
        select 
            a.fend_time as fqc_time,     -- QC检测时间
            a.fdetect_record_id,         -- 检测记录ID
            a.fserial_number,            -- 设备序列号
            a.freal_name as fqc_detector  -- QC检测员姓名（用于明细输出）
        from (select 
                fend_time,
                fdetect_record_id,
                fserial_number,
                freal_name,
                row_number()over(partition by fserial_number order by fend_time desc)as num
                -- ↑ 为每个设备的QC检测记录编号，取最新一次

            from dwd.dwd_detect_back_detect_detail
            where fis_deleted=0          -- 记录未删除
            and (fdet_type=1 or fdet_type=2)  -- 检测类型为1或2（QC检测）
            and (freal_name like "%赖嘉琪%" or freal_name like "%李伟雪%" or freal_name like "%王封敏%" 
                 or freal_name like "%王若桂%" or freal_name like "%陈斌%" or freal_name like "%范嘉庆%" 
                 or freal_name like "%陈梓琦%" or freal_name like "%周雨%" or freal_name like "%郑君豪%" 
                 or freal_name like "%叶思宁%")
            -- ↑ 只统计这些指定的QC检测员的检测记录
            -- 就像只统计质检部门特定人员的抽检工作
            
            and to_date(fend_time) >=to_date(date_sub(from_unixtime(unix_timestamp()),180))  -- 最近180天
        )a
        where a.num=1                    -- 只保留每个设备最新的QC检测
)

-- 主查询：统计检测数量和抽检数量
select 
    to_date(a.fend_time),                -- 检测日期
    left(a.fserial_number,2) as "渠道",   -- 从序列号前2位看出渠道
    a.fclass_name,                       -- 设备类别
    case when left(a.fserial_number,3) like "%010%" or left(a.fserial_number,3) like "%020%" or left(a.fserial_number,3) like "%050%" then "验机"
         when left(a.fserial_number,2) like "%BM%" then "帮卖"
    else "竞拍" end as ftype,            -- 业务类型分类
    count(*) as "检测数",                 -- 总检测数量
    count(case when b.fserial_number is not null then a.fserial_number else null end) as "抽检数",
    case when max(case when b.fserial_number is not null then 1 else 0 end)=1 then "是" else "否" end as "是否被QC抽检",
    group_concat(distinct case when b.fserial_number is not null then b.fqc_detector else null end, ",") as "QC检测员"
    -- ↑ 抽检数量：如果在QC检测表中有记录，就算作被抽检了
    -- 就像统计"总共检测了100台，其中被质检抽查了10台"
    
from first_detect as a                   -- 首次检测数据
left join qc_detect as b on a.fserial_number=b.fserial_number  -- 关联QC抽检数据
where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))  -- 最近180天
group by 1,2,3,4                        -- 按日期、渠道、设备类别、业务类型分组统计

/*
简单解释：
这个查询就像工厂的质检统计表：
"统计最近6个月，每天各种类型的产品总共检测了多少个，
其中被质检员抽查了多少个，计算抽检比例"

QC是什么：
QC = Quality Control（质量控制）
就像工厂生产线上的质检员，会随机抽取产品进行二次检查

统计维度：
- 时间：每天
- 渠道：从序列号前缀识别
- 设备类别：手机、平板等
- 业务类型：验机、帮卖、竞拍

关键指标：
- 检测数：当天总共检测了多少台设备
- 抽检数：其中被QC人员抽检了多少台
-- 是否被QC抽检：文本标记“是/否”，便于报表直观看懂
-- QC检测员：同一分组内参与抽检的检测员名单（去重后以逗号拼接）

QC检测员：
只统计指定的10个QC检测员的抽检工作：
赖嘉琪、李伟雪、王封敏、王若桂、陈斌、范嘉庆、陈梓琦、周雨、郑君豪、叶思宁

业务价值：
- 监控抽检覆盖率
- 确保质量控制标准
- 评估QC工作量
- 发现抽检比例异常的情况

典型结果示例：
日期        | 渠道 | 设备类别 | 业务类型 | 检测数 | 抽检数 | 是否被QC抽检 | QC检测员
2024-01-01  | 01   | 手机     | 验机     | 100    | 15     | 是           | 叶思宁,李伟雪
(表示这天验机渠道的手机总共检测100台，QC抽检了15台，抽检率15%)
（“QC检测员”列会把该分组下所有参与抽检的检测员去重后列出来）
（表示这天验机渠道的手机总共检测100台，QC抽检了15台，抽检率15%）
*/
