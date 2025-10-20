/*
🐆 检测豹操作记录查询
用途：查看检测豹系统的操作记录，包括操作时间、人员、设备信息等
就像查看"检测员工使用检测豹系统的工作日志"
*/

select
    t.*,                                 -- 所有基本信息
    hour(fcreate_time) as fhour,         -- 操作小时（几点钟操作的）
    MINUTE(fcreate_time) as fminute,     -- 操作分钟（几分钟操作的）
    LEFT(t.fserial_number,2) as fchannel -- 从序列号前2位看出渠道
from (
select 
a.fserial_number,                        -- 设备序列号
a.fcreate_time,                          -- 操作时间
 case when left(a.fserial_number,2) in ('01','02') then "验机"
  	  when left(a.fserial_number,2)="BM" then "寄卖"
  	   when (left(a.fserial_number,2)="CG" and a.fcreate_time>='2024-12-01') or left(a.fserial_number,2)="TL" then "太力"
else "回收" end as "业务类型",
-- ↑ 根据序列号开头判断业务类型：
-- 01、02开头 = 验机业务
-- BM开头 = 寄卖业务
-- CG开头且2024年12月后，或TL开头 = 太力业务  
-- 其他 = 回收业务

  case when left(a.fserial_number,2) in ('02') or right(left(a.fserial_number,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
-- ↑ 判断在哪个仓库操作：02开头或序列号包含16 = 杭州，其他 = 深圳

case when to_date(a.fcreate_time)="2023-10-18" and b.freal_name="成露露" then "江珊" 
     when a.fuser_name='lijunfeng@huishoubao.com.cn' then "李俊锋" 
  	 when to_date(a.fcreate_time)='2024-03-04' and b.freal_name="陈冬凡" then "周远鸿" 
         when to_date(a.fcreate_time)='2024-03-04' and b.freal_name="胡家华" then "黄成水"
  	when b.freal_name="陈冬凡" and to_date(a.fcreate_time)='2024-05-13' and a.fbrand_name!="Apple" then "李俊锋"
  	when b.freal_name="周远鸿" and to_date(a.fcreate_time)='2024-05-15' then null
  else b.freal_name end as freal_name,
-- ↑ 操作人员姓名（包含多个特殊日期的人员调整）
-- 处理临时代班、请假、特殊用户名等情况
-- 就像考勤系统中记录实际操作人员

a.fproduct_name,                         -- 产品名称
case when a.fbrand_name='Apple' or e.fname='苹果' then "苹果" else "安卓" end as fname,
-- ↑ 品牌分类：Apple或苹果 = 苹果，其他 = 安卓

a.foriginal_data,                        -- 原始数据
a.ftransform_options,                    -- 转换选项
row_number()over(partition by a.fserial_number order by a.fcreate_time desc) as num
-- ↑ 为每个设备的操作记录编号，按时间倒序，只保留最新的一次操作

from drt.drt_my33312_detection_t_det_app_record as a  -- 从"检测APP记录表"
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername  -- 关联用户表获取真实姓名
left join drt.drt_my33310_recycle_t_order as c on a.fserial_number=c.fseries_number  -- 关联订单表
left join drt.drt_my33310_recycle_t_product as d on c.fproduct_id=d.fproduct_id  -- 关联产品表
left join drt.drt_my33310_recycle_t_pdt_brand as e on d.fclass_id=e.fid  -- 关联品牌表
-- ↑ 通过多表关联获取完整的设备信息

where a.fserial_number!=""               -- 序列号不为空
and a.fserial_number is not null
and to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),366))  -- 最近366天（约1年）
)t
where num=1                              -- 只保留每个设备最新的一次操作记录

/*
💡 简单解释：
这个查询就像检测豹系统的使用日志：
"查看最近1年每台设备在检测豹系统上的最新操作记录，
记录谁在什么时候、什么地方、对什么设备进行了操作"

🔍 检测豹是什么：
检测豹是一个检测系统的名称，用于手机等设备的检测
就像医院的体检设备，有专门的操作软件

📊 记录的信息：
- ⏰ 时间：具体到小时和分钟
- 👤 人员：实际操作的检测员（处理代班情况）
- 📍 地点：杭州仓库还是深圳仓库
- 📱 设备：什么品牌、什么型号
- 💼 业务：验机、寄卖、回收等不同业务类型

🎯 业务价值：
- 追踪设备检测流程
- 考核检测员工作效率
- 分析操作时间分布
- 故障排查和质量控制

💡 特殊处理：
- 多个特定日期的人员调整（临时代班）
- 特殊用户名的真实姓名映射
- 品牌名称的统一化处理
- 避免重复记录（只取最新一次）

🕐 时间维度：
添加小时和分钟字段，可以分析：
- 哪个时间段操作最频繁
- 是否有加班操作
- 不同时间段的工作效率
*/
