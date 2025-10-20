/*
🏭 供应链作业明细查询
用途：查看仓库作业的详细记录，按仓库和业务类型分类
就像查看"各个仓库每天干了什么活，处理了什么货物"
*/

select *,
    case when right(left(fseries_number,6),4)="0112" then "东莞仓" 
    	 when right(left(fseries_number,6),2)="16" then "杭州仓"
    else "深圳仓" end as fwms_type,
    -- ↑ 根据序列号判断货物在哪个仓库：
    -- 如果序列号前6位的后4位是"0112"，就是东莞仓
    -- 如果序列号前6位的后2位是"16"，就是杭州仓
    -- 其他情况都是深圳仓
    -- 就像通过工号判断员工在哪个部门工作
    
    case when left(fseries_number,2)='TL' or (left(fseries_number,2)='CG' and funpack_time>='2024-12-01') then "太力" 
    else ftype end as "业务",
    -- ↑ 判断是什么业务类型：
    -- 如果序列号以"TL"开头，或者以"CG"开头且时间在2024年12月1日以后，就是"太力"业务
    -- 否则就用原来的业务类型
    -- 就像根据不同的标识判断是哪个项目的工作
    
    case when fseries_number like "%\_%" then "配件" else "成品" end as fproduct_type
    -- ↑ 判断是配件还是成品：
    -- 如果序列号包含下划线"_"，就是配件
    -- 否则就是成品
    -- 就像根据编号格式区分零件和整机
    
from dws.dws_instock_details              -- 从"入库明细表"查找数据
where funpack_time>=to_date(date_sub(from_unixtime(unix_timestamp()),30))  -- 只看最近30天的记录
and funpack_user not in("于炉烨","张晓梦","徐晶")  -- 排除这3个操作员的记录
and left(fseries_number,2) not like "%YZ%"  -- 排除"YZ"开头的序列号
and left(fseries_number,2) not like "%NT%"  -- 排除"NT"开头的序列号

/*
💡 简单解释：
这个查询就像问仓库主管：
"给我看看最近30天各个仓库的作业情况，
告诉我每批货物是在哪个仓库处理的，属于什么业务，是配件还是成品，
但是不要包括某些特定操作员的记录和特殊类型的货物"

🔍 关键词解释：
- right(left(...)) = 先取左边几位，再取右边几位（就像先取前几个字，再取后几个字）
- like "%\_%" = 包含下划线（%是通配符，\_是下划线的转义写法）
- not in = 不在列表中（排除指定的值）
*/
