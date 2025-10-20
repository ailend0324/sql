-- 这个SQL查询的目的是：计算从到达深圳前第一个节点到最终送达深圳签收点的时间间隔
-- 简单来说，就是计算快递从进入深圳地区到最终签收需要多少天

-- 第一个临时表：route_1 - 获取每个快递单号的最新记录（最终签收记录）
with route_1 as (
select 
    *  -- 选择所有字段
from (
    select
        *,
        -- 为每个快递单号(fmailno)按时间倒序排列，给每行分配序号
        -- 这样最新的记录序号就是1
        row_number() over(partition by fmailno order by faccept_time desc) as num
    from drt.drt_my33310_recycle_t_route_info_record  -- 从路由信息记录表中获取数据
    --where faccept_addr='深圳市'  -- 这行被注释掉了，原本是想只查询深圳的记录
    ) t
where num=1  -- 只取序号为1的记录，也就是每个快递单号的最新记录
),

-- 第二个临时表：route_2 - 获取每个快递单号的最早记录（进入深圳前的第一个节点）
route_2 as (
select 
    *  -- 选择所有字段
from (
    select
        *,
        -- 为每个快递单号按时间正序排列，给每行分配序号
        -- 这样最早的记录序号就是1
        row_number() over(partition by fmailno order by faccept_time asc) as num
    from drt.drt_my33310_recycle_t_route_info_record
    -- 排除签收操作，只保留运输过程中的记录
    where fop_code!='80' and fop_code!='8000'  -- 80和8000是签收操作代码
    ) t
where num=1  -- 只取序号为1的记录，也就是每个快递单号的最早记录
)

-- 主查询：计算平均送达时间
select
    b.faccept_addr,  -- 显示地址信息
    -- 计算平均送达时间（天数）
    -- 使用ceil函数向上取整，确保时间不会出现小数
    ceil(
        avg(
            case 
                when a.faccept_time>b.faccept_time then 
                    -- 如果签收时间晚于起始时间，计算时间差
                    -- unix_timestamp将时间转换为秒数，然后除以(3600*24)转换为天数
                    (unix_timestamp(a.faccept_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(b.faccept_time,'yyyy-MM-dd HH:mm:ss'))/(3600*24) 
                else null 
            end
        )
    ) as "平均送达时间"
from route_1 as a  -- 使用第一个临时表作为主表（签收记录）
left join route_2 as b on a.fmailno=b.fmailno  -- 关联第二个临时表（起始记录），通过快递单号关联
where (a.fop_code='80' or a.fop_code='8000')  -- 只查询签收操作的记录
and (a.faccept_addr='深圳市' or a.faccept_addr='东莞市')  -- 只查询深圳和东莞的地址
and a.faccept_time>=to_date(date_sub(from_unixtime(unix_timestamp()),61))  -- 只查询最近61天的数据
group by 1  -- 按地址分组，计算每个地址的平均送达时间
