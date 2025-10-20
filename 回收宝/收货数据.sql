/*
📦 收货数据查询
用途：查看仓库收到货物的记录
就像查看"今天仓库收到了哪些快递包裹"
*/

select 
    *,                                    -- 选择所有原始字段（把表格的所有列都显示出来）
    case when right(left(fserial_no,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    -- ↑ 这句话的意思是：
    -- 1. 先取序列号的前6位：left(fserial_no,6)
    -- 2. 再取这6位的最后2位：right(...,2)  
    -- 3. 如果最后2位是"16"，就标记为"杭州"仓库，否则就是"深圳"仓库
    -- 就像通过邮政编码判断是哪个城市
    
    left(fserial_no,2) as fchannel       -- 从序列号前2位判断渠道（就像从车牌号看出是哪个省）
from dwd.dwd_t_pm_wms_stock_notify       -- 从"仓库库存通知表"里找数据
where to_date(fcreate_time)>='2024-01-01'  -- 只看2024年1月1日以后的记录
and fcmd='CGRK'                          -- 并且操作类型是"CGRK"（采购入库的意思）

/*
💡 简单解释：
这个查询就像问快递员：
"给我看看2024年以后所有收到的包裹，
告诉我每个包裹是哪个渠道寄来的，存放在哪个仓库"

🔍 关键词解释：
- CGRK = 采购入库（Cai Gou Ru Ku的拼音首字母）
- fserial_no = 序列号（就像每个包裹的快递单号）
- fcreate_time = 创建时间（就像包裹到达的时间）
*/

