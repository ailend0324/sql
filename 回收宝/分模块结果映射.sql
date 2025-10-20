/*
🔍 分模块检测结果映射
用途：查看自动检测的结果，区分是模块一还是模块二的检测
就像查看"哪些检测是机器自动完成的，哪些是人工完成的"
*/

select 
    to_date(fauto_detect_time),           -- 自动检测的日期
    fserial_number,                       -- 序列号（设备的身份证号）
    case when fsource_table like "%t_det_app_record%" then "模块二" else "模块一" end as "模块类型",
    -- ↑ 根据数据来源判断是哪个模块：
    -- 如果数据来源表名包含"t_det_app_record"，就是"模块二"
    -- 否则就是"模块一"
    -- 就像根据考试卷子的标题判断是期中考试还是期末考试
    
    fissue_name,                          -- 检测项目名称（检查什么问题）
    fanswer_name                          -- 检测结果（检查出什么结果）
from dwd.dwd_detect_auto_detection_issue_and_answer  -- 从"自动检测问题和答案表"查找数据
where to_date(fauto_detect_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),70))  -- 只看最近70天的记录
and fanswer_name is not null              -- 并且检测结果不为空（确实有检测结果）
and (fautomation_det_record_id>0 or fautomation_det_record_id is null)  -- 并且记录ID有效

/*
💡 简单解释：
这个查询就像问质检员：
"给我看看最近70天所有自动检测的记录，
告诉我每个设备通过了哪种检测方式，检查了什么项目，结果如何"

🔍 关键词解释：
- fauto_detect_time = 自动检测时间
- fsource_table = 数据来源表（就像数据的出处）
- fissue_name = 问题名称（检测的项目）
- fanswer_name = 答案名称（检测的结果）
- is not null = 不为空（确实有数据）

📊 结果示例：
日期        | 序列号    | 模块类型 | 检测项目 | 检测结果
2024-01-01  | ABC123   | 模块一   | 屏幕检测 | 正常
2024-01-01  | DEF456   | 模块二   | 电池检测 | 异常
*/ 
