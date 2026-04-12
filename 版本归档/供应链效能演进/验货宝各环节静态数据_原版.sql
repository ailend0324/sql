-- 验货宝各流程数据统计（时间周期优化版）
-- 用途：统计验货宝各个流程环节的数据，按日期、仓库、时段、品类进行分组
-- 优化：大幅优化时间周期查询，减少重复计算和不必要的数据加载

with 
-- 预计算时间参数，避免重复计算
time_params as (
    select 
        to_date(date_sub(from_unixtime(unix_timestamp()),1200)) as recent_date,  -- 50天前
        to_date(date_sub(from_unixtime(unix_timestamp()),600)) as short_recent_date,  -- 25天前
        '2024-01-01' as start_date
),

put as (
select                  --验货宝 取 出库请求时间节点,跟 dws_xy_yhb_detail 用forder_id 进行左连接匹配
    a.forder_id,
    b.fauto_create_time as frequest_put_time  --出库请求时间
from drt.drt_my33315_xy_detect_t_xy_hsb_order as a
left join (
    select 
        *
    from(
    select 
        *,
        row_number()over(partition by forder_id order by fauto_create_time desc) as num
    from drt.drt_my33315_xy_detect_t_xy_yhb_order_txn 
    where forder_status_name in ("待平台发货","待平台退货")) t where num=1
) as b on a.forder_id=b.forder_id
),

-- 优化的品类信息子查询：一次性计算好品类分类
category_info as (
    select 
        fserial_number,
        fclass_name,
        fbrand_name,
        -- 预计算品类分类，避免重复计算
        case when fclass_name in ('平板','平板电脑') then '平板'
             when fclass_name in ('笔记本','笔记本电脑') then '笔记本'
             when fclass_name in ('手机','') or fclass_name is null then '手机'  -- 处理NULL值
             when fclass_name in ('单反闪光灯','单反转接环','移动电源','移动硬盘','云台','拍照配件/云台','增距镜') then '3C数码配件'
             when fclass_name in ('彩色激光多功能一体机','复印打印多功能一体机','激光打印机','墨盒','收款机','投影机','投影仪','硒鼓粉盒','针式打印机') then '办公设备耗材'
             when fclass_name in ('CPU','电脑服务器','电脑固态硬盘','固态硬盘','电脑内存','内存条','电脑显卡','显卡','电脑硬件套装','电脑主板','键盘','品牌台机','无线鼠标','显示器','一体机','组装台机','品牌台式机') then '电脑硬件及周边'
             when fclass_name in ('路由器') then '网络设备'
             when fclass_name in ('PS游戏光盘/软件','其他游戏配件','游戏机','游戏卡','游戏手柄','PS4游戏','PS5游戏','Switch游戏') then '电玩'
             when fclass_name in ('单反套机','单反相机','拍立得','摄像机','摄影机','数码相机','微单相机','相机镜头','运动相机','单反/微单套机','单反/微单相机') then '相机/摄像机'
             when fclass_name in ('耳机','MP3/MP4','黑胶唱片机','蓝牙耳机','蓝牙音响/音箱','麦克风/话筒','影音播放器','智能音响/音箱') then '影音数码/电器'
             when fclass_name in ('VR眼镜头盔','VR虚拟现实','按摩器','吹风机','磁吸式键盘','电子书','翻译器','风扇','加湿器','录音笔','美发器','手写笔','智能手写笔','无人机','吸尘器','学习机','智能办公本','智能配饰','智能摄像','智能手表','智能手环') then '智能设备'
             when fclass_name in ('黄金') then '黄金'
        else '手机' end as fclass  -- 所有其他情况都归类为手机
    from (
        select 
            *,
            row_number() over(partition by fserial_number order by fend_time asc) as num
        from drt.drt_my33310_detection_t_detect_record 
        where fdet_type=0
        and fis_deleted=0
        and freport_type=0
        and fverdict<>"测试单"
        and to_date(fend_time) >= (select recent_date from time_params)  -- 使用预计算的时间
    ) t
    where num=1
),

-- 核心优化：分时段获取数据，避免加载过多历史数据
-- 下单数据（历史数据，需要较长时间范围）
order_data as (
    select 
        a.forder_id,
        a.fhost_barcode,
        a.forder_time,
        case 
            when left(a.fhost_barcode,3) like "%010%" then "深圳仓"
            when left(a.fhost_barcode,3) like "%020%" then "杭州仓"
            when left(a.fhost_barcode,3) like "%050%" then "东莞仓"
        else "" end as stock_name,
        c.fclass
    from dws.dws_xy_yhb_detail as a
    left join category_info as c on a.fhost_barcode=c.fserial_number
    where a.forder_time>='2024-01-01'  -- 下单数据需要历史数据
),

-- 近期数据（最近50天的各种流程数据）
recent_data as (
    select 
        a.forder_id,
        a.fhost_barcode,
        a.freceive_time,
        a.fdetect_time,
        a.fsale_put_time,
        a.frefund_put_time,
        a.fput_time,
        a.fzhibao_create_time,
        a.fzhibao_update_time,
        a.fzhibao_status,
        a.fcomplain_add_time,
        a.fcomplain_duty,
        a.Fis_disassembly,
        case 
            when left(a.fhost_barcode,3) like "%010%" then "深圳仓"
            when left(a.fhost_barcode,3) like "%020%" then "杭州仓"
            when left(a.fhost_barcode,3) like "%050%" then "东莞仓"
        else "" end as stock_name,
        c.fclass,
        p.frequest_put_time
    from dws.dws_xy_yhb_detail as a
    left join category_info as c on a.fhost_barcode=c.fserial_number
    left join put as p on a.forder_id=p.forder_id
    cross join time_params as tp
    where (
        a.freceive_time >= tp.recent_date or
        a.fdetect_time >= tp.recent_date or
        a.fsale_put_time >= tp.recent_date or
        a.frefund_put_time >= tp.recent_date or
        a.fput_time >= tp.recent_date or
        a.fzhibao_create_time >= tp.recent_date or
        (a.fzhibao_update_time >= tp.recent_date and (a.fzhibao_status=50 or a.fzhibao_status=60)) or
        a.fcomplain_add_time >= tp.recent_date or
        p.frequest_put_time >= tp.recent_date
    )
)

-- 下单数据
select 
    to_date(forder_time) as ftime_by,
    stock_name,
    case when HOUR(forder_time)>0 and HOUR(forder_time)<=20 then "0-20" else ">20" end as "时段",
    coalesce(fclass, '手机') as fclass,
    count(forder_id) as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from order_data
group by 1,2,3,4

union all

-- 收货数据
select 
    to_date(freceive_time) as ftime_by,
    stock_name,
    case when HOUR(freceive_time)>0 and HOUR(freceive_time)<=20 then "0-20" else ">20" end as "时段",
    coalesce(fclass, '手机') as fclass,
    null as "下单数",
    count(forder_id) as "收货数",
    count(IF(to_date(fdetect_time)=to_date(freceive_time),forder_id,null)) as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from recent_data
cross join time_params as tp
where freceive_time >= tp.recent_date
group by 1,2,3,4

union all

-- 检测数据
select 
    to_date(fdetect_time) as ftime_by,
    stock_name,
    case when HOUR(fdetect_time)>0 and HOUR(fdetect_time)<=20 then "0-20" else ">20" end as "时段",
    coalesce(fclass, '手机') as fclass,
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    count(forder_id) as "检测数",
    count(if(Fis_disassembly=1,forder_id,null)) as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from recent_data
cross join time_params as tp
where fdetect_time >= tp.recent_date
group by 1,2,3,4

union all

-- 销售出库数据
select 
    to_date(fsale_put_time) as ftime_by,
    stock_name,
    case when HOUR(fsale_put_time)>0 and HOUR(fsale_put_time)<=20 then "0-20" else ">20" end as "时段",
    coalesce(fclass, '手机') as fclass,
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    count(fsale_put_time) as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from recent_data
cross join time_params as tp
where fsale_put_time >= tp.recent_date
group by 1,2,3,4

union all

-- 退货出库数据
select 
    to_date(frefund_put_time) as ftime_by,
    stock_name,
    case when HOUR(frefund_put_time)>0 and HOUR(frefund_put_time)<=20 then "0-20" else ">20" end as "时段",
    coalesce(fclass, '手机') as fclass,
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    count(forder_id) as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from recent_data
cross join time_params as tp
where frefund_put_time >= tp.recent_date
group by 1,2,3,4

union all

-- 出库数据
select 
    to_date(fput_time) as ftime_by,
    stock_name,
    case when HOUR(fput_time)>0 and HOUR(fput_time)<=20 then "0-20" else ">20" end as "时段",
    coalesce(fclass, '手机') as fclass,
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    count(forder_id) as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from recent_data
cross join time_params as tp
where fput_time >= tp.recent_date
group by 1,2,3,4

union all

-- 质保申请数据
select 
    to_date(fzhibao_create_time) as ftime_by,
    stock_name,
    case when HOUR(fzhibao_create_time)>0 and HOUR(fzhibao_create_time)<=20 then "0-20" else ">20" end as "时段",
    coalesce(fclass, '手机') as fclass,
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    count(forder_id) as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from recent_data
cross join time_params as tp
where fzhibao_create_time >= tp.recent_date
group by 1,2,3,4

union all

-- 质保通过数据
select 
    to_date(fzhibao_update_time) as ftime_by,
    stock_name,
    case when HOUR(fzhibao_update_time)>0 and HOUR(fzhibao_update_time)<=20 then "0-20" else ">20" end as "时段",
    coalesce(fclass, '手机') as fclass,
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    count(forder_id) as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from recent_data
cross join time_params as tp
where fzhibao_update_time >= tp.recent_date
and fzhibao_status=50 or fzhibao_status=60
group by 1,2,3,4

union all

-- 客诉数据
select 
    to_date(fcomplain_add_time) as ftime_by,
    stock_name,
    case when HOUR(fcomplain_add_time)>0 and HOUR(fcomplain_add_time)<=20 then "0-20" else ">20" end as "时段",
    coalesce(fclass, '手机') as fclass,
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    count(forder_id) as "客诉数",
    count(case when fcomplain_add_time is not null and fcomplain_duty=2 then forder_id else null end) as "客诉有责数",
    null as "出库请求数",
    null as "当日请求当日出库数"
from recent_data
cross join time_params as tp
where fcomplain_add_time >= tp.recent_date
group by 1,2,3,4

union all

-- 出库请求数据
select 
    to_date(frequest_put_time) as ftime_by,
    stock_name,
    case when HOUR(frequest_put_time)>0 and HOUR(frequest_put_time)<=20 then "0-20" else ">20" end as "时段",
    coalesce(fclass, '手机') as fclass,
    null as "下单数",
    null as "收货数",
    null as "收货当日检测数",
    null as "检测数",
    null as "拆机数",
    null as "销售出库数",
    null as "退货出库数",
    null as "出库数",
    null as "质保申请数",
    null as "质保通过数",
    null as "客诉数",
    null as "客诉有责数",
    count(forder_id) as "出库请求数",
    count(IF(to_date(fput_time)=to_date(frequest_put_time),forder_id,null)) as "当日请求当日出库数"
from recent_data
cross join time_params as tp
where frequest_put_time >= tp.recent_date
group by 1,2,3,4

