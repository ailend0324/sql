-- 这是一个分析鱼市B端售后明细数据的SQL查询
-- 就像我们要统计鱼市平台上商家对商家交易的所有售后情况，了解每个商品的检测、销售和售后全过程

-- 第一步：创建一个检测记录查询，获取每个商品的最新检测信息
-- 就像给每个商品找它的"体检报告"
with detect as (       
    select 
        *   -- 选择所有字段
    from (
        select 
            a.fcreate_time,  -- 检测开始时间（什么时候开始检测的）
            upper(a.fserial_number) as fserial_number,  -- 商品序列号转大写（就像身份证号统一格式）
            a.Fdet_tpl,  -- 检测模板类型（用哪种检测方式，比如标准检、大质检等）
            a.Freal_name,  -- 检测师傅的真实姓名（谁给商品做的检测）
            a.Fend_time,  -- 检测结束时间（检测什么时候完成的）
      		a.fbrand_name,  -- 商品品牌名称（比如苹果、华为等）
            a.Fdetection_object,  -- 检测对象（检测的具体内容是什么）
            a.fgoods_level,  -- 商品等级（比如9成新、8成新等）
      		a.fwarehouse_code,  -- 仓库代码（在哪个仓库做的检测）
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time asc) as num  -- 给每个商品的多次检测按时间排序编号（1,2,3...）
        from drt.drt_my33310_detection_t_detect_record as a  -- 从检测记录主表获取数据
        -- 关联订单表，获取订单创建时间
        left join (select 
                        fseries_number,  -- 商品序列号
                        forder_create_time  -- 订单创建时间
                   from (
                        select 
                            fseries_number,  -- 商品序列号
                            forder_create_time,  -- 订单创建时间
                        row_number() over(partition by fseries_number order by  forder_create_time desc) as num  -- 每个商品按订单时间倒序排号（找最新的订单）
                    from dws.dws_jp_order_detail  -- 从竞拍订单明细表获取数据
                    where ftest_show <> 1  -- 排除测试数据（不要假的测试订单）
                    and (fmerchant_jp=0 or fmerchant_jp is null)  -- 排除商家竞拍（只要个人用户的订单）
                    and forder_status in (2,3,4,6)  -- 只要特定状态的订单（已成交、已发货等）
                    and forder_create_time>=to_date(date_sub(from_unixtime(unix_timestamp()),365))  -- 只要近365天的数据（最近一年的数据）
                    ) t where t.num=1) as b on upper(a.fserial_number)=b.fseries_number  -- 关联条件：序列号匹配
        -- 关联用户表，获取检测师傅的岗位信息
        left join (
                    select 
                        freal_name,  -- 真实姓名
                        Fposition_id  -- 岗位ID（工作职位编号）
                    from (select 
                                *,  -- 所有字段
                                row_number() over(partition by freal_name order by fcreate_time desc) as num  -- 每个人按创建时间倒序排号（找最新的员工信息）
                          from drt.drt_my33310_amcdb_t_user  -- 从用户信息表获取数据
                          )t
                    where num=1) as c on a.freal_name=c.freal_name  -- 关联条件：姓名匹配
        where a.fis_deleted=0  -- 排除已删除的记录（不要已经删除的检测记录）
        and to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))  -- 只要近365天的检测数据
        and a.fend_time<b.forder_create_time  -- 检测时间要早于订单创建时间（先检测再卖，不能先卖再检测）
        and c.Fposition_id <>129            -- 排除入库组缺陷拍照的人员（岗位ID为129的人，这些人的检测不算正式检测）
        --and fdetection_object<>3  -- 注释掉的条件：排除某种检测对象
            ) c 
    where c.num=1  -- 只取每个商品的第一条检测记录（只要第一次检测，不要重复检测）
),

-- 第二步：创建竞拍销售记录查询，获取鱼市B端的销售数据
jp_sale as(
    select 
        *
    from (
        select 
            *,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num  -- 每个商品按订单时间倒序排号（找最新的销售记录）
        from dws.dws_jp_order_detail  -- 从竞拍订单明细表获取数据
        where ftest_show <> 1  -- 排除测试数据
        and forder_platform=5  -- 只要鱼市B端订单（平台类型为5）
        and forder_status in (2,3,4,6)) t where num=1  -- 只要特定状态的订单，并且只要每个商品的最新记录
),

-- 第三步：创建第二次检测记录查询（分模块检测）
detect_two as (
select 
    upper(fserial_number) as fserial_number,  -- 商品序列号转大写
    freal_name as fdetect_two_name,  -- 第二次检测师傅姓名
    fcreate_time as fdetect_two_time  -- 第二次检测时间
from (
select 
    a.fcreate_time,  -- 检测创建时间
    a.fserial_number,  -- 商品序列号
    b.freal_name,  -- 检测师傅真实姓名
    row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num  -- 按商品序列号分组，选择最新的检测记录
from drt.drt_my33312_detection_t_det_app_record as a  -- 从检测应用记录表获取数据
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername  -- 关联用户表获取真实姓名
where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))  -- 只要近365天的数据
and fserial_number!=""  -- 序列号不为空
and fserial_number is not null)t  -- 序列号不为空值
where num=1  -- 只选择每组的第一条记录
),

-- 第四步：创建第三次检测记录查询（外观检测）
detect_three as (
select 
    upper(fserial_number) as fserial_number,  -- 商品序列号转大写
    freal_name as fdetect_three_name,  -- 第三次检测师傅姓名
    fcreate_time as fdetect_three_time  -- 第三次检测时间
from (
    select 
        a.fcreate_time,  -- 检测创建时间
        a.fserial_number,  -- 商品序列号
        b.freal_name,  -- 检测师傅真实姓名
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num  -- 按商品序列号分组，选择最新的检测记录
    from drt.drt_my33312_detection_t_det_task as a  -- 从检测任务表获取数据
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id  -- 关联检测任务记录表
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))  -- 只要近365天的数据
    and b.fdet_sop_task_name like "%外观%"  -- 只要包含"外观"的检测任务
    )t
where num=1  -- 只选择每组的第一条记录
),

-- 第五步：创建屏幕检测记录查询（专门检测屏幕）
detect_three_pingmu as (
select 
    upper(fserial_number) as fserial_number,  -- 商品序列号转大写
    case when freal_name="李俊峰" then "李俊锋" else freal_name end as fdetect_three_name_pingmu,  -- 屏幕检测师傅姓名（修正名字错误）
    fcreate_time as fdetect_three_time_pingmu  -- 屏幕检测时间
from (
    select 
        a.fcreate_time,  -- 检测创建时间
        a.fserial_number,  -- 商品序列号
        b.freal_name,  -- 检测师傅真实姓名
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num  -- 按商品序列号分组，选择最新的检测记录
    from drt.drt_my33312_detection_t_det_task as a  -- 从检测任务表获取数据
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id  -- 关联检测任务记录表
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))  -- 只要近365天的数据
    and b.fdet_sop_task_name like "%屏幕%"  -- 只要包含"屏幕"的检测任务
    and b.fdet_sop_task_name!="外观屏幕"  -- 排除"外观屏幕"（避免重复）
    )t
where num=1  -- 只选择每组的第一条记录
)

-- 最后：选择所有需要的数据字段，就像把所有信息整理成一个完整的表格
select 
    a.fstart_time,  -- 拍卖开始时间
    a.fpay_time,  -- 付款时间
    a.fseries_number,  -- 商品序列号
    a.fclass_name,  -- 商品分类名称
    a.fchannel_name,  -- 渠道名称
    a.fproduct_name,  -- 产品名称
    a.fproject_name,  -- 项目名称
    a.fshop_name,  -- 店铺名称
    a.fbuyer_merchant_id, -- 买家商家id
    
    -- 判断业务渠道类型
    case when d.fseries_number is not null then "阿里回流" else "其它渠道" end as "业务渠道",
    
    -- 判断回收方式
    case when c.frecycle_type=1 then "邮寄"
         when c.frecycle_type=2 then "上门"
         when c.frecycle_type=3 then "到店"
    else null end as "回收类型",
    
    -- 判断是否异地竞拍
    case when Fmerchant_jp=0 then "否" else "是" end as "是否异地上拍",
    
    left(a.fseries_number,2) as "渠道",  -- 取序列号前两位作为渠道标识
    
    a.fcost_price/100 as "成本价",  -- 成本价，分转元
    a.foffer_price/100 as "当前出价",  -- 当前出价，分转元
    
    -- 检测相关信息
    b.Fdet_tpl,  -- 检测模板
    b.Freal_name,  -- 检测师傅姓名
    b.Fend_time,  -- 检测结束时间
    b.Fdetection_object,  -- 检测对象
    get_json_object(b.Fgoods_level,'$.levelName') as Fgoods_level,  -- 从JSON中提取商品等级名称
    
    -- 判断检测渠道类型
    case 
        when b.Fdet_tpl=1 then "大检测"
        when (b.Fdet_tpl=0 or b.Fdet_tpl=2 or b.Fdet_tpl=6 or b.Fdet_tpl=7) then "竞拍检测"
    else '其他' end as "检测渠道",
    
    -- 判断检测模板类型
    case 
      when b.Fdet_tpl = 0 then '标准检'
      when b.Fdet_tpl = 1 then '大质检'
      when b.Fdet_tpl = 2 then '新标准检测'
  	  when b.Fdet_tpl = 3 then '产线检'
      when b.Fdet_tpl = 4 then '34项检测'
      when b.Fdet_tpl = 5 then '无忧购'
      when b.Fdet_tpl = 6 then '寄卖plus'
      when b.Fdet_tpl = 7 then '价格3.0的检测'
    else '其他' end as "检测模板",
    
    -- 检测师傅信息（优先显示分模块检测师傅）
    if(g.fdetect_two_name is null,b.freal_name,g.fdetect_two_name) as fdetect_two_name,  -- 第二次检测师傅姓名
    if(h.fdetect_three_name is null,b.freal_name,h.fdetect_three_name) as fdetect_three_name,  -- 第三次检测师傅姓名
    j.fdetect_three_name_pingmu,  -- 屏幕检测师傅姓名
    
    -- 判断是否进行分模块检测
    if(g.fdetect_two_time is not null,"是","否") as "是否分模块",
    
    -- 判断仓库位置
    case when b.fwarehouse_code='12' then "东莞仓" 
    	 when right(left(a.fseries_number,6),2)="16" or left(a.fseries_number,3)="020" then "杭州仓"
    else "深圳仓" end as fwarehouse_code,
    
    -- 判断品牌类型
    case when b.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
    
    -- 售后相关信息
    case when cc.frefund_total>0 then 1 else 0 end as "售后数",  -- 是否有售后（1有，0无）
    case when cc.frefund_total>0 and a.foffer_price>cc.frefund_total then cc.frefund_total/100 else 0 end as "赔付金额",  -- 实际赔付金额，分转元
    cc.fjudge_reason as fapply_desc,  -- 售后申请原因描述
    
    -- 判断售后类型
    case cc.faftersales_type
        when 1 then '仅退款'
        when 2 then '退货退款'
    end as faftersales_type
    
-- 从各个数据表获取数据
from jp_sale as a  -- 主表：鱼市B端销售数据
left join detect as b on a.fseries_number=b.fserial_number  -- 关联第一次检测记录
left join dws.dws_hs_order_detail as c on a.fseries_number=c.fseries_number  -- 关联回收订单详情
left join dws.dws_hs_order_detail_al as d on a.fseries_number=d.fseries_number  -- 关联阿里回收订单详情
left join detect_two as g on a.fseries_number=g.fserial_number  -- 关联第二次检测记录
left join detect_three as h on a.fseries_number=h.fserial_number  -- 关联第三次检测记录
left join detect_three_pingmu as j on a.fseries_number=j.fserial_number  -- 关联屏幕检测记录
left join drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales as cc on a.fseries_number=cc.fbusiness_id  -- 关联售后数据

-- 筛选条件：只要近720天的数据（最近两年的数据）
where a.fstart_time>=to_date(date_sub(from_unixtime(unix_timestamp()),720))





