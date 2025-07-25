/*
🤖 自动检测记录查询
用途：查看设备自动检测的记录，包括检测人员、检测类型等信息
就像查看"哪些设备通过了自动化检测，是谁操作的，在哪里检测的"
*/

select 
    fcreate_time,                         -- 检测时间
    left(fserial_number,2) as fchannel,   -- 从序列号前2位看出渠道（就像从车牌看省份）
    case when left(fserial_number,2) in ('01','02') then "验机"
    	when left(fserial_number,2)='BM' then "寄卖"
        when (left(fserial_number,2)="CG" and fcreate_time>='2024-12-01') or left(fserial_number,2)="TL" then "太力"
    else "回收" end as "业务类型",
    -- ↑ 根据序列号开头判断业务类型：
    -- 01或02开头 = 验机业务
    -- BM开头 = 寄卖业务  
    -- CG开头且2024年12月后，或TL开头 = 太力业务
    -- 其他 = 回收业务
    -- 就像根据工号前缀判断员工属于哪个部门
    
    case when left(fserial_number,2) in ('02') or right(left(fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    -- ↑ 判断在哪个仓库检测：
    -- 02开头或序列号包含16 = 杭州仓库
    -- 其他 = 深圳仓库
    
    case when fdet_type=1 then "苹果" else "安卓" end as fbrand,  -- 检测类型：1=苹果，其他=安卓
    case when to_date(fcreate_time)='2023-10-16' and freal_name="刘俊" then "周利" 
         when to_date(fcreate_time)='2024-03-04' and freal_name="陈冬凡" then "周远鸿" 
         when to_date(fcreate_time)='2024-03-04' and freal_name="胡家华" then "黄成水"
         when to_date(fcreate_time)='2024-03-02' and freal_name="陈冬凡" then "李浩宇"
         when to_date(fcreate_time)='2024-03-05' and freal_name="陈冬凡" then "周远鸿"
         when to_date(fcreate_time)='2024-03-14' and freal_name="严俊" then "林广泽"
         when fbind_real_name is not null or fbind_real_name="" then fbind_real_name
    else freal_name end as freal_name,
    -- ↑ 检测人员姓名（包含一些特殊日期的人员调整）
    -- 某些特定日期有人员变动，需要用实际操作人员的名字
    -- 就像临时代班时要记录真实的操作员
    
    case when fdet_type=1 then "插线检测" else "imel检测" end as fdet_type,  -- 检测方式
    fserial_number                        -- 设备序列号
from (
select 
    a.fcreate_time,                       -- 检测创建时间
    a.fserial_number,                     -- 设备序列号
    a.fbind_real_name,                    -- 绑定的真实姓名 
    case when a.fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" else b.freal_name end as freal_name,
    -- ↑ 处理特殊用户名的显示
    a.fdet_type,                          -- 检测类型
    row_number()over(partition by a.fserial_number order by a.fcreate_time desc) as num
    -- ↑ 为每个设备的检测记录编号，按时间倒序，取最新的一次
    
from drt.drt_my33312_detection_t_automation_det_record as a  -- 从"自动检测记录表"
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_id=b.fuser_id  -- 关联用户表获取真实姓名
where a.fserial_number is not null and a.fserial_number!=""  -- 序列号不为空
and to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),300))  -- 最近300天的记录
)t
where num=1                               -- 只保留每个设备最新的一次检测记录

/*
💡 简单解释：
这个查询就像查看检测车间的工作日志：
"给我看看最近300天每个设备最新的自动检测记录，
告诉我是谁检测的、在哪个仓库、什么业务类型、用什么方式检测的"

🔍 关键词解释：
- fdet_type = 检测类型（1=苹果设备插线检测，其他=安卓设备IMEI检测）
- fbind_real_name = 绑定真实姓名（临时代班人员）
- row_number() = 编号排序（确保每个设备只保留最新记录）

📊 结果会显示：
检测时间 | 渠道 | 业务类型 | 仓库 | 品牌 | 检测员 | 检测方式 | 序列号
每行代表一个设备的最新检测情况
*/
