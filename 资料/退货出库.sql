/*
↩️ 退货出库数据查询
用途：查看客户退货的记录
就像查看"哪些商品被客户退回来了"
*/

select 
    *,                                    -- 选择所有原始字段（显示退货的完整信息）
    case when right(left(fserial_no,6),2)="16" then "杭州" else "深圳" end as fwarehouse,
    -- ↑ 判断退货是退到哪个仓库：
    -- 序列号包含"16"的退到杭州仓库
    -- 其他的退到深圳仓库
    -- 就像快递退货时要知道退到哪个收货点
    
    left(fserial_no,2) as fchannel       -- 从序列号看出是哪个渠道的退货
from dwd.dwd_t_pm_wms_stock_notify       -- 从"仓库库存通知表"查找数据  
where to_date(fcreate_time)>='2024-01-01'  -- 只看2024年以后的退货记录
and fcmd='CGTH'                          -- 操作类型是"CGTH"（采购退货）

/*
💡 简单解释：
这个查询就像问客服：
"给我看看2024年以后所有的退货记录，
告诉我每个退货是通过哪个渠道退的，退到了哪个仓库"

🔍 关键词解释：
- CGTH = 采购退货（Cai Gou Tui Huo的拼音首字母）
- 就像在淘宝上买了东西不满意，要退回给卖家
*/
