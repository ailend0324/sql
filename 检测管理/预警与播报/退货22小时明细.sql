-- 退货预警机器人SQL（明细版）
-- 变更：将汇总统计改为明细表形式展示，方便查看每条记录详情


-- 第一个临时表：入库出库记录表
-- 用来记录每个商品的入库时间、退货出库时间、销售出库时间
with t_instock_inout as (                     
    select
        -- 把序列号转换成大写，确保格式统一
        upper(fserial_no) as fseries_number,
        -- 找出最早的入库时间（CGRK表示采购入库）
        -- 就像记录商品第一次进入仓库的时间
        min(case when fcmd = 'CGRK' then fchange_time else null end) as fstock_in_time,
        -- 找出最晚的退货出库时间（CGTH表示采购退货）
        -- 就像记录商品最后一次退货离开仓库的时间
        max(case when fcmd = 'CGTH' then fchange_time else null end) as freturn_out_time,
        -- 找出最晚的销售出库时间（JYCK表示交易出库）
        -- 就像记录商品最后一次卖出去的时间
        max(case when fcmd = 'JYCK' then fchange_time else null end) as fsale_out_time
    -- 从库存变动通知表查询数据
    from drt.drt_my33312_hsb_sales_product_t_pm_wms_stock_notify
    -- 只查询最近30天的数据
    where fchange_time>=to_date(date_sub(from_unixtime(unix_timestamp()),30))
    -- 只查询采购退货和采购入库的记录
    and fcmd in ( 'CGTH','CGRK')
    -- 按序列号分组，每个商品只保留一条记录
    group by fserial_no
), 

-- 第二个临时表：出库请求表
-- 用来找出需要退货或已取消的订单
stock_out_request as( -- 出库
select
    -- 订单ID
    forder_id,
    -- 序列号（商品的唯一编号）
    fseries_number,
    -- 自动创建时间，作为请求结束时间
    fauto_create_time as frequest_endtime
from (
    -- 子查询：找出每个订单的最新状态记录
    select 
        a.forder_id,                    -- 订单ID
        c.fseries_number,               -- 序列号
        a.fauto_create_time,            -- 自动创建时间
        -- 给每个订单的记录按时间排序，最新的排第一
        row_number()over(partition by a.forder_id order by a.fauto_create_time desc) as num
    -- 从订单交易表开始查询
    from drt.drt_my33310_recycle_t_order_txn as a
    -- 关联订单状态表，了解订单当前状态
    left join drt.drt_my33310_recycle_t_order_status as b on a.forder_status=b.forder_status_id
    -- 关联订单主表，获取订单基本信息
    left join drt.drt_my33310_recycle_t_order as c on a.forder_id=c.forder_id
    -- 只查询状态是"待退货"或"已取消"的订单
    where b.forder_status_name in ("待退货","已取消")
    -- 只查询最近14天的数据
    and a.fauto_create_time>=to_date(date_sub(from_unixtime(unix_timestamp()),14))
)t where num=1  -- 只保留每个订单的最新记录
),

-- 第三个临时表：备注信息表
-- 用来过滤掉有特殊备注的订单（这些订单不需要预警）
remark as (
select 
    -- 订单ID
    forder_id,
    -- 备注内容
    fremark
from (
    select 
        *,
        -- 给每个订单的备注按创建时间排序，最新的排第一
        row_number()over(partition by forder_id order by fcreate_time desc) as num
    -- 从订单备注表查询
    from drt.drt_my33310_recycle_t_order_remark
    -- 只查询最近50天的数据
    where fcreate_time>=to_date(date_sub(from_unixtime(unix_timestamp()),50))
    -- 备注不能为空
    and fremark is not null
    -- 过滤掉包含以下关键词的备注（这些订单有特殊处理方式，不需要预警）
    and (fremark like "%拆坏%"           -- 商品被拆坏了
    or fremark like "%清库存%"          -- 清库存处理
    or fremark like "%自行处理%"        -- 客户自己处理
    or fremark like "%环保%"            -- 环保处理
    or fremark like "%虚拟出库%"        -- 虚拟出库
    or fremark like "%退回物流单号%"    -- 有物流单号
    or fremark like "%无实物%"          -- 没有实物
    or fremark like "%预付款已操作入库%" -- 预付款已处理
    or fremark like "%刷单%"            -- 刷单订单
    or fremark like "%测试%"            -- 测试订单
    or fremark like "%批量取消%"        -- 批量取消
    or fremark like "%超额运费未扣%")   -- 运费问题
    )t
where num=1  -- 只保留每个订单的最新备注
)

-- 直接查询退货预警明细
-- 不再使用临时表和汇总统计，直接展示每条记录
select 
    -- 序号（行号）
    row_number() over(order by b.frequest_endtime asc) as "序号",
    -- 退货时间（转换为日期格式）
    to_date(b.frequest_endtime) as "退货日期",
    -- 序列号
    b.fseries_number as "序列号",
    -- 订单ID
    b.forder_id as "订单ID",
    -- 判断业务类型：如果序列号以'BM'开头就是寄卖，否则是回收
    case when left(b.fseries_number,2)='BM' then "寄卖" else "回收" end as "业务类型",
    -- 判断仓库类型：根据序列号判断是哪个仓库
    case when right(left(b.fseries_number,6),4)="0112" then "东莞仓" 
         when right(left(b.fseries_number,6),2)="16" then "杭州仓"
    else "深圳仓" end as "仓库类型",
    -- 订单状态
    e.forder_status_name as "订单状态",
    -- 计算超时小时数：当前时间减去请求结束时间，除以3600秒得到小时数，保留1位小数
    -- 就像计算这个退货订单已经等待了多长时间
    round((unix_timestamp(from_timestamp(now(),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(b.frequest_endtime,'yyyy-MM-dd HH:mm:ss'))/3600,1) as "超时小时数",
    -- 请求结束时间（完整时间格式）
    b.frequest_endtime as "请求结束时间"
-- 从出库请求表开始查询
from stock_out_request as b
-- 关联入库出库记录表，了解商品库存变动情况
left join t_instock_inout as a on a.fseries_number=b.fseries_number
-- 关联订单主表，获取订单详细信息
left join drt.drt_my33310_recycle_t_order as c on b.fseries_number=c.fseries_number
-- 关联备注表，获取备注信息
left join remark as d on c.forder_id=d.forder_id
-- 关联信用订单数据表
left join drt.drt_my33310_recycle_t_xy_order_data as f on c.Forder_id=f.forder_id
-- 关联订单状态表，获取当前订单状态
left join drt.drt_my33310_recycle_t_order_status as e on c.forder_status=e.forder_status_id
-- 只查询最近7天的数据
where b.frequest_endtime>=to_date(date_sub(from_unixtime(unix_timestamp()),7))
-- 订单状态必须包含"退货"或"已取消"
and (e.forder_status_name like "%退货%" or e.forder_status_name like "%已取消%")
-- 但不能是"已退货"状态（已经处理完的不需要预警）
and e.forder_status_name!="已退货"
-- 不是测试订单
and c.ftest=0
-- 没有退货出库记录（说明还在等待处理）
and a.freturn_out_time is null
-- 没有特殊备注（有备注的订单有特殊处理方式）
and d.fremark is null
-- 排除以下特殊业务类型：
and left(b.fseries_number,2)!='YZ'  -- 不是YZ开头的订单
and left(b.fseries_number,2)!='AS'  -- 不是AS开头的订单
and left(b.fseries_number,2)!='NT'  -- 不是NT开头的订单
and left(b.fseries_number,2)!='BB'  -- 不是BB开头的订单
and left(b.fseries_number,2)!='CG'  -- 不是CG开头的订单
and left(b.fseries_number,2)!='TL'  -- 不是TL开头的订单

-- 超时时间必须大于等于20小时（超过20小时才需要预警）
and (unix_timestamp(from_timestamp(now(),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(b.frequest_endtime,'yyyy-MM-dd HH:mm:ss'))/3600>=20
-- 排除特定订单号
and b.fseries_number!='BM0101191103001998'
-- 调整：允许统计寄卖（BM）或 回收类型=1 的订单
and (left(b.fseries_number,2)='BM' or c.frecycle_type=1)
-- 新增：剔除2025年前下单的寄卖订单
and not (
    left(b.fseries_number,2)='BM' 
    and year(c.forder_time)<2025
)
-- 新增：序列号必须非空（防止出现空字符串导致问题）
and b.fseries_number is not null
and length(trim(b.fseries_number))>0
-- 按请求结束时间升序排列（最早的排在前面）
order by b.frequest_endtime asc
