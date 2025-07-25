/*
📦 验货宝到货预测分析
用途：预测验货宝包裹的到货时间，基于历史物流数据
就像根据以往快递经验预测"包裹大概什么时候能到"
*/

with t_parcel as(                        -- 第一步：整理验机包裹的基本信息
-- 验机条码明细 - 验机包裹物流时效、预计到达时间整理
select 
    fid,                                 -- 包裹ID
    Forder_id,                          -- 订单ID
    fseries_number,                     -- 序列号（就像包裹的身份证号）
    fexpress_number                     -- 快递单号
from(
    select 
        fid, 
        Forder_id,
        upper(Fbar_code) as fseries_number,     -- 将条码转为大写（统一格式）
        flogistics_num as fexpress_number,      -- 物流单号
        row_number() over(partition by fbar_code order by fupdate_time desc) as num
        -- ↑ 为每个条码的记录编号，按更新时间倒序，取最新的一条
        -- 就像每个包裹可能有多次记录，我们只要最新的那次
    from drt.drt_my33310_xywms_t_parcel     -- 从"验机包裹表"
) t where num=1                          -- 只保留每个包裹最新的记录
),

t_parcel_receive as(                     -- 第二步：整理包裹签收信息
-- 验机签收记录
select 
    fparcel_id,                         -- 包裹ID
    fsign_time,                         -- 签收时间
    freceive_user                       -- 收货人员
from(
    select 
        fparcel_id,
        fadd_time as fsign_time,        -- 添加时间就是签收时间
        fadd_user as freceive_user,     -- 添加用户就是收货人员
        row_number() over(partition by fparcel_id  order by fadd_time desc) as num
        -- ↑ 为每个包裹的签收记录编号，取最新的一次签收
    from drt.drt_my33310_xywms_t_parcel_log   -- 从"包裹日志表"
    where ftype=1                       -- 类型1表示签收操作
) t where num=1                         -- 只保留最新的签收记录
),

wuliu_shixiao as(                       -- 第三步：计算各城市的平均物流时效
-- 物流时效统计：计算从下单到签收的平均天数
select 
    d.fseller_city_name,                -- 发货城市
    ceil(avg(case when c.fsign_time>a.forder_time then 
        (unix_timestamp(c.fsign_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(a.forder_time,'yyyy-MM-dd HH:mm:ss'))/(3600*24) 
        else null end)) as avg_day
    -- ↑ 计算平均到货天数：
    -- 1. 如果签收时间晚于下单时间，才计算时差
    -- 2. 将时间差转换为天数（除以3600*24）
    -- 3. 向上取整（ceil）得到平均天数
    -- 就像统计"从北京到深圳的快递平均需要几天"
    
from drt.drt_my33315_xy_detect_t_xy_hsb_order as a         -- 验机订单表
left join t_parcel as b on a.flogistics_number=b.fexpress_number    -- 关联包裹信息
left join t_parcel_receive as c on b.fid=c.fparcel_id              -- 关联签收信息
left join drt.drt_my33315_xy_detect_t_xy_yhb_detect_order as d on a.fxy_order_id=d.fxy_order_id  -- 关联检测订单
where c.fsign_time >=to_date(date_sub(from_unixtime(unix_timestamp()),31))  -- 只看最近31天的数据
and d.fseller_city_name <> ""           -- 发货城市不为空
group by 1                              -- 按发货城市分组统计
)

-- 主查询：查找还未签收的包裹，预测其到货时间
select 
    a.fxy_order_id,                     -- 闲鱼订单ID
    a.flogistics_number,                -- 物流单号
    a.fhost_barcode,                    -- 主条码
    d.fseller_city_name,                -- 发货城市
    case 
        when d.Fseller_province_name like "%北京%" then "杭州仓"
        when d.Fseller_province_name like "%山东%" then "杭州仓"
        when d.Fseller_province_name like "%上海%" then "杭州仓" 
        when d.Fseller_province_name like "%江苏%" then "杭州仓"
        when d.Fseller_province_name like "%浙江%" then "杭州仓" 
    else "深圳仓" end as "仓库",
    -- ↑ 根据发货省份决定包裹应该送到哪个仓库
    -- 华东地区（北京、山东、上海、江苏、浙江）送杭州仓
    -- 其他地区送深圳仓
    
    d.forder_dtime,                     -- 订单时间
    a.forder_time,                      -- 下单时间
    c.fsign_time,                       -- 签收时间（这里应该为空，因为还未签收）
    e.avg_day                           -- 该城市的平均到货天数（用于预测）
from drt.drt_my33315_xy_detect_t_xy_hsb_order as a         -- 验机订单表
left join t_parcel as b on a.flogistics_number=b.fexpress_number    -- 关联包裹
left join t_parcel_receive as c on b.fid=c.fparcel_id              -- 关联签收记录
left join drt.drt_my33315_xy_detect_t_xy_yhb_detect_order as d on a.fxy_order_id=d.fxy_order_id  -- 关联检测订单
left join wuliu_shixiao as e on d.fseller_city_name=e.fseller_city_name  -- 关联城市平均时效
where a.forder_time>=to_date(date_sub(from_unixtime(unix_timestamp()),31))  -- 最近31天的订单
and d.fseller_city_name != ""           -- 发货城市不为空
and c.fsign_time is null                -- 重点：还未签收的包裹（签收时间为空）

/*
💡 简单解释：
这个查询就像快递预测系统：
"找出最近一个月还没有签收的验机包裹，
根据发货城市的历史平均时效，预测它们大概什么时候能到货"

🔍 关键逻辑：
1. 先统计各个城市过去一个月的平均到货时间
2. 再找出还没签收的包裹
3. 用对应城市的平均时效来预测到货时间

📊 结果用途：
- 帮助仓库提前安排人手
- 提醒客户预计到货时间
- 识别可能延误的包裹

🎯 业务价值：
就像天气预报一样，虽然不是100%准确，但能帮助做好准备工作
*/


