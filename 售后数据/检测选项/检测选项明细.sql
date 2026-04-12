/*
📱 手机检测选项明细
用途：查看手机检测的详细结果，每个设备每个检测项目只保留最新的一次结果
就像查看"每部手机最新的体检报告"
*/

select 
	a.ds,                                 -- 数据日期
    a.fserial_number,                     -- 序列号（手机的身份证号）
    a.fissue_name,                        -- 检测项目名称（检查什么）
    a.fanswer_name                        -- 检测结果（检查出什么）
from (
select 
    a.*,                                  -- 选择检测记录的所有信息
    b.fserial_number,                     -- 关联设备的序列号
    row_number()over(partition by b.fserial_number,a.fissue_name order by a.fdetect_record_id desc) as num
    -- ↑ 为每个设备的每个检测项目编号：
    -- 按设备序列号和检测项目分组，按检测记录ID倒序排列
    -- 这样每组的第1号就是最新的那次检测
    -- 就像给每个学生的每门课程的考试成绩按时间排序，取最新的那次
    
from dwd.dwd_detect_back_detection_issue_and_answer_v2 as a  -- 从"检测问题和答案表"
left join dwd.dwd_detect_back_detect_detail as b on a.fdetect_record_id=b.fdetect_record_id
-- ↑ 关联检测详情表，通过检测记录ID连接
-- 就像通过考试编号把考试成绩和考生信息连接起来

where left(b.fserial_number,2) not in ('NT','YZ','JM','BG','XZ')  -- 排除特定类型的设备
and a.field_source='fdet_norm_snapshot'   -- 只要标准检测快照的数据
and a.ds>=to_date(date_sub(from_unixtime(unix_timestamp()),365))  -- 只看最近365天（1年）的数据
and b.fend_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))  -- 检测结束时间也要在1年内
and b.fclass_name="手机"                  -- 只看手机类设备（不包括平板、笔记本等）
) as a
where a.num=1                             -- 只保留每组的第1号（即最新的检测结果）

/*
💡 简单解释：
这个查询就像医院查病历：
"给我看看最近1年所有手机的最新检测结果，
每部手机的每个检测项目只要最新的那次结果"

🔍 关键词解释：
- row_number() = 编号（给每行数据编号）
- partition by = 分组（按什么分组）
- order by desc = 倒序排列（从大到小排序）
- left join = 左连接（把两张表的相关信息连接起来）
- field_source = 字段来源（数据的类型）

📊 数据流程：
1. 先从检测表和详情表获取所有手机检测数据
2. 为每部手机的每个检测项目按时间排序编号
3. 只保留编号为1的记录（即最新的检测结果）
4. 排除特殊类型的设备，只保留普通手机
*/
