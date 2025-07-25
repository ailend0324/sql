/*
🔍 验货宝检测差异分析
用途：统计验货宝设备外观检测中显示"无问题"的各项指标
就像统计"体检中哪些项目显示正常的设备有多少台"
*/

select 
    month(a.fend_time) as "月份",          -- 检测完成的月份
    to_date(a.fend_time) as fdetect_time, -- 检测完成的具体日期
    a.fserial_number,                     -- 设备序列号
    count(case when b.fissue_name='屏幕外观划痕' and b.fanswer_name="无" then a.fserial_number else null end) as '屏幕外观划痕-无',
    -- ↑ 统计屏幕外观划痕检测结果为"无"的设备数量
    count(case when b.fissue_name='外壳划痕' and b.fanswer_name="无" then a.fserial_number else null end) as '外壳划痕-无',
    -- ↑ 统计外壳划痕检测结果为"无"的设备数量
    count(case when b.fissue_name='外壳磕碰' and b.fanswer_name="无" then a.fserial_number else null end) as '外壳磕碰-无',
    -- ↑ 统计外壳磕碰检测结果为"无"的设备数量
    count(case when b.fissue_name='外壳印渍' and b.fanswer_name="无" then a.fserial_number else null end) as '外壳印渍-无'
    -- ↑ 统计外壳印渍检测结果为"无"的设备数量
    -- 这4个统计就像体检时统计"血压正常"、"心跳正常"等项目的人数
    
from (select 
            *,
            row_number() over(partition by fserial_number order by fend_time desc) as num
            -- ↑ 为每个设备的检测记录编号，按结束时间倒序，确保取最新的一次检测
            
        from drt.drt_my33310_detection_t_detect_record  -- 从"检测记录表"
        where fdet_type=0                 -- 检测类型为0（特定的检测类型）
        and fis_deleted=0                 -- 记录未被删除
    	and fverdict<>"测试单"            -- 不是测试单
        and to_date(fend_time) >='2023-09-01'  -- 2023年9月1日以后的记录
        and left(fserial_number,2) in ('01','02')  -- 只看验货宝设备（01、02开头）
        ) as a
left join dwd.dwd_detect_back_detection_issue_and_answer_v2 as b on a.frecord_id=b.fdetect_record_id
-- ↑ 关联检测问题答案表，获取具体的检测项目和结果
-- 就像把体检报告和具体的检查项目结果连接起来

where left(a.fserial_number,2) in ('01','02')  -- 再次确认只看验货宝设备
and a.num=1                           -- 只要每个设备最新的一次检测
and b.field_source='fdet_norm_snapshot'  -- 只要标准检测快照数据
and b.ds>='2023-09-01'                -- 检测数据也要在指定日期范围内
and b.fissue_name in ('外壳划痕','外壳磕碰','屏幕外观划痕','外壳印渍')  -- 只关注这4个检测项目
and b.fanswer_name='无'               -- 只统计结果为"无"（即无问题）的情况
group by 1,2,3                       -- 按月份、日期、序列号分组统计

/*
💡 简单解释：
这个查询就像医院统计体检报告：
"给我统计一下2023年9月以后，验货宝设备检测中
各项外观检查显示'无问题'的设备分别有多少台，按月份和日期分组"

🔍 关键词解释：
- fdet_type=0 = 特定的检测类型
- fis_deleted=0 = 记录未被删除（有效记录）
- fverdict<>"测试单" = 不是测试数据
- case when...then...else null end = 条件统计（满足条件就统计，否则不计入）

📊 结果示例：
月份 | 检测日期   | 序列号  | 屏幕划痕-无 | 外壳划痕-无 | 外壳磕碰-无 | 外壳印渍-无
1    | 2024-01-01 | ABC123  | 1          | 1          | 1          | 1
1    | 2024-01-02 | DEF456  | 1          | 0          | 1          | 1

每行代表一个设备在某个日期的各项外观检测"无问题"的统计结果
*/
