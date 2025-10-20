-- 竞拍售后明细数据分析查询 - 全平台版本
-- 这个查询整合了所有平台的检测、销售和售后情况
-- 包括：自有平台、B端鱼市、采货侠、C端鱼市等

-- 第一步：创建一个detect子查询，用来找出每个手机的第一次检测记录
with detect as (       
    select
        *   -- 选择所有字段
    from (
        select
            a.fcreate_time,  -- 检测什么时候开始的
            upper(a.fserial_number) as fserial_number,  -- 手机序列号（转成大写便于匹配）
            a.Fdet_tpl,  -- 用的哪种检测模板
            a.Freal_name,  -- 检测师傅的名字
            a.Fend_time,  -- 检测什么时候结束的
      		a.fbrand_name,  -- 手机品牌
            a.Fdetection_object,  -- 检测的是什么对象
            a.fgoods_level,  -- 手机成色等级
      		a.fwarehouse_code,  -- 在哪个仓库检测的
            row_number() over(partition by upper(a.fserial_number) order by a.fcreate_time asc) as num  -- 给同一个手机的多次检测按时间排序编号
        from drt.drt_my33310_detection_t_detect_record as a  -- 主检测记录表
        -- 关联订单表获取订单创建时间
        left join (select
                        fseries_number,  -- 序列号
                        forder_create_time  -- 订单创建时间
                   from (
                        select
                            fseries_number,  -- 序列号
                            forder_create_time,  -- 订单创建时间
                        row_number() over(partition by fseries_number order by  forder_create_time desc) as num  -- 每个序列号按订单时间倒序排号
                    from dws.dws_jp_order_detail  -- 竞拍订单明细表
                    where ftest_show <> 1  -- 排除测试数据
                    and (fmerchant_jp=0 or fmerchant_jp is null)  -- 排除商家竞拍
                    and forder_status in (2,3,4,6)  -- 只要特定状态的订单
                    and forder_create_time>=to_date(date_sub(from_unixtime(unix_timestamp()),400))  -- 只要近400天的数据
                    ) t where t.num=1) as b on upper(a.fserial_number)=b.fseries_number  -- 关联条件：序列号匹配
        -- 关联用户表获取人员岗位信息
        left join (
                    select
                        freal_name,  -- 真实姓名
                        Fposition_id  -- 岗位ID
                    from (select
                                *,  -- 所有字段
                                row_number() over(partition by freal_name order by fcreate_time desc) as num  -- 每个人按创建时间倒序排号
                          from drt.drt_my33310_amcdb_t_user  -- 用户信息表
                          )t
                    where num=1) as c on a.freal_name=c.freal_name  -- 关联条件：姓名匹配
        where a.fis_deleted=0  -- 排除已删除的记录
        and to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),400))  -- 只要近400天的检测数据
        and a.fend_time<b.forder_create_time  -- 检测时间要早于订单创建时间
        and c.Fposition_id <>129            -- 排除入库组缺陷拍照的人员
            ) c
    where c.num=1  -- 只取每个序列号的第一条检测记录
),

-- 第二步：售后检测记录子查询（售后时重新检测的记录）
after_sale_detect as (
select
        *
    from (
        select
            *,
            row_number() over(partition by fserial_number order by fend_time asc) as num  -- 给每个手机的售后检测按时间排序编号
        from drt.drt_my33310_detection_t_detect_record  -- 检测记录表
        where fdet_type=0  -- 检测类型为0（正式检测）
        and fis_deleted=0  -- 排除已删除的记录
      	and freport_type=0  -- 报告类型为0（正式报告）
    	and fverdict<>"测试单"  -- 排除测试单
        and to_date(fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),400))  -- 只要近400天的数据
        and left(fserial_number,2) in ('YZ','NT')  -- 只要特定渠道的手机
        )t
    where num=1  -- 只取每个序列号的第一条售后检测记录
),

-- 第三步：自有平台销售记录（平台1）
self_platform_sale as(
    select
        *,
        "自有平台" as platform_type  -- 添加平台标识
    from (
        select
            *,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num  -- 每个序列号按订单时间倒序排号
        from dws.dws_jp_order_detail  -- 竞拍订单明细表
        where ftest_show <> 1  -- 排除测试数据
        and forder_platform=1  -- 自有平台
        and to_date(forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))  -- 只要近365天的数据
        and (fmerchant_jp=0 or fmerchant_jp is null)  -- 排除商家竞拍
        and forder_status in (2,3,4,6)  -- 只要特定状态的订单
        ) t where num=1  -- 只取每个序列号的最新销售记录
    union all  -- 合并历史数据
    select
        *,
        "自有平台" as platform_type
    from (
        select
            *,
            "" as Fys_b2b_series_number,
           , 0  as Fys_b2b_order_status,
           , 0  as Fys_b2b_order_platform,
           , 0  as Fys_b2b_foffer_price,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num
        from dws.dws_jp_order_detail_history2023  -- 2023年历史订单表
        where ftest_show <> 1
        and forder_platform=1
        and to_date(forder_create_time) between '2023-01-01' and to_date(date_sub(from_unixtime(unix_timestamp()),366))
        and (fmerchant_jp=0 or fmerchant_jp is null)
        and forder_status in (2,3,4,6)) t where num=1
),

-- 第四步：B端鱼市销售记录（平台5）
b2b_yushi_sale as(
    select
        *,
        "B端鱼市" as platform_type  -- 添加平台标识
    from (
        select
            *,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num
        from dws.dws_jp_order_detail 
        where ftest_show <> 1
        and forder_platform=5  -- B端鱼市
        and to_date(forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
        and forder_status in (2,3,4,6)) t where num=1
    union all
    select
        *,
        "B端鱼市" as platform_type
    from (
        select
            *,
            "" as Fys_b2b_series_number,
           , 0  as Fys_b2b_order_status,
           , 0  as Fys_b2b_order_platform,
           , 0  as Fys_b2b_foffer_price,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num
        from dws.dws_jp_order_detail_history2023
        where ftest_show <> 1
        and forder_platform=5
        and to_date(forder_create_time) between '2023-01-01' and to_date(date_sub(from_unixtime(unix_timestamp()),366))
        and forder_status in (2,3,4,6)) t where num=1
),

-- 第五步：采货侠销售记录（平台6）
caihuoxia_sale as (
    select
        *,
        "采货侠" as platform_type  -- 添加平台标识
    from (
        select
            *,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num
        from dws.dws_jp_order_detail
        where ftest_show <> 1
        and forder_platform=6  -- 采货侠
        and fmerchant_jp=0
        and to_date(forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
        and forder_status in (2,3,4,6)) t where num=1
    union all
    select
        *,
        "采货侠" as platform_type
    from (
        select
            *,
            "" as Fys_b2b_series_number,
           , 0  as Fys_b2b_order_status,
           , 0  as Fys_b2b_order_platform,
           , 0  as Fys_b2b_foffer_price,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num
        from dws.dws_jp_order_detail_history2023
        where ftest_show <> 1
        and forder_platform=6
        and fmerchant_jp=0
        and to_date(forder_create_time) between '2023-01-01' and to_date(date_sub(from_unixtime(unix_timestamp()),366))
        and forder_status in (2,3,4,6)) t where num=1
),

-- 第六步：C端鱼市销售记录（BM开头序列号）
c_end_yushi_sale as(
    select
        *,
        "C端鱼市" as platform_type  -- 添加平台标识
    from (
        select
            *,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num
        from dws.dws_jp_order_detail 
        where ftest_show <> 1
        and left(fseries_number,2)='BM'  -- C端鱼市通过序列号前缀识别
        and to_date(forder_create_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),365))
        and forder_status in (2,3,4,6)) t where num=1
    union all
    select
        *,
        "C端鱼市" as platform_type
    from (
        select
            *,
            "" as Fys_b2b_series_number,
           , 0  as Fys_b2b_order_status,
           , 0  as Fys_b2b_order_platform,
           , 0  as Fys_b2b_foffer_price,
            row_number() over(partition by fseries_number order by  forder_create_time desc) as num
        from dws.dws_jp_order_detail_history2023
        where ftest_show <> 1
        and left(fseries_number,2)='BM'
        and to_date(forder_create_time) between '2023-01-01' and to_date(date_sub(from_unixtime(unix_timestamp()),366))
        and forder_status in (2,3,4,6)) t where num=1
),

-- 第七步：自有平台售后记录
self_platform_after_sale as (
    select
        *
    from (
        select
            a.*,  -- 售后表的所有信息
            b.fseries_number,  -- 关联到的竞拍销售序列号
            row_number() over(partition by fsales_series_number order by a.fauto_create_time desc ) as num  -- 同一销售序列号按创建时间倒序
        from drt.drt_my33310_recycle_t_after_sales_order_info as a  -- 自有平台售后信息表
        left join drt.drt_my33310_recycle_t_order as b on a.fafter_sales_order_id=b.forder_id  -- 关联原订单
        where a.fvalid=1  -- 只要有效售后
        ) t where num=1
),

-- 第八步：鱼市售后记录（B端鱼市、采货侠、C端鱼市共用）
yushi_after_sale as (
    select
        *
    from (
        select
            *,
            row_number() over(partition by fbusiness_id order by fcreate_time desc)as num  -- 同一业务单取最新
        from drt.drt_my33306_hsb_sales_t_caihuoxia_after_sales  -- 鱼市售后表
        where Fsource in (1,2,3)  -- 1=采货侠, 2=鱼市-B2B, 3=鱼市-寄卖
        )t where num=1
),

-- 第九步：检测相关子查询（保持原有逻辑）
detect_two as (
    select
        upper(fserial_number) as fserial_number,
        freal_name as fdetect_two_name,
        fcreate_time as fdetect_two_time
    from (
        select
            a.fcreate_time,
            a.fserial_number,
            b.freal_name,
            row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
        from drt.drt_my33312_detection_t_det_app_record as a
        left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
        where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),800))
        and fserial_number!=""
        and fserial_number is not null)t
    where num=1
),

detect_three as (
    select
        upper(fserial_number) as fserial_number,
        freal_name as fdetect_three_name,
        fcreate_time as fdetect_three_time
    from (
        select
            a.fcreate_time,
            a.fserial_number,
            b.freal_name,
            row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
        from drt.drt_my33312_detection_t_det_task as a
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
        where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),800))
        and b.fdet_sop_task_name like "%外观%")t
    where num=1
),

detect_three_pingmu as (
    select
        upper(fserial_number) as fserial_number,
        case when freal_name="李俊峰" then "李俊锋" else freal_name end as fdetect_three_name_pingmu,
        fcreate_time as fdetect_three_time_pingmu
    from (
        select
            a.fcreate_time,
            a.fserial_number,
            b.freal_name,
            row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
        from drt.drt_my33312_detection_t_det_task as a
        left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
        where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),800))
        and b.fdet_sop_task_name like "%屏幕%"
        and b.fdet_sop_task_name!="外观屏幕")t
    where num=1
)

-- 最终输出：整合所有平台的数据
select
    a.fstart_time,
    a.fseries_number,  -- 序列号
    a.fclass_name,  -- 分类/品类
    case when b.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,  -- 品牌归类
    a.platform_type,  -- 平台类型
    a.fchannel_name,  -- 销售渠道名称
    a.fproduct_name,  -- 商品名称
    a.fproject_name,  -- 项目名称
    a.Fcity_name,  -- 收货城市
    a.Forder_address,  -- 收货地址
    a.Freceiver_id,  -- 收货人ID
    a.Freceiver_name,  -- 收货人姓名
    a.Freceiver_phone,  -- 收货人电话
    b.Fdet_tpl,  -- 检测模板代码
    b.Freal_name,  -- 检测人员
    b.Fend_time,  -- 检测结束时间
    b.Fdetection_object,  -- 检测对象
    case when b.fwarehouse_code='12' then "东莞仓"
    	 when right(left(a.fseries_number,6),2)="16" or left(a.fseries_number,3)="020" then "杭州仓"
    else "深圳仓" end as fwarehouse_code,  -- 检测仓库
    get_json_object(b.Fgoods_level,'$.levelName') as Fgoods_level,  -- 成色等级名称
    case
        when b.Fdet_tpl=1 then "大检测"
        when (b.Fdet_tpl=0 or b.Fdet_tpl=2 or b.Fdet_tpl=6 or b.Fdet_tpl=7) then "竞拍检测"
    else '其他' end as "检测渠道",
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
    a.fcost_price/100 as "成本价",  -- 成本（分转元）
    a.foffer_price/100 as "销售价格",  -- 销售价格（分转元）
    if(g.fdetect_two_name is null,b.freal_name,g.fdetect_two_name) as fdetect_two_name,  -- 分模块检测人员
    if(h.fdetect_three_name is null,b.freal_name,h.fdetect_three_name) as fdetect_three_name,  -- 外观检测人员
    j.fdetect_three_name_pingmu,  -- 屏幕检测人员
    if(g.fdetect_two_time is not null,"是","否") as "是否分模块",  -- 是否做过分模块检测
    a.fanchor_level  -- 锚点等级

-- 自有平台数据
from self_platform_sale as a
left join detect as b on a.fseries_number=b.fserial_number
left join self_platform_after_sale as c on a.fseries_number=c.Fsales_series_number
left join after_sale_detect as f on c.fseries_number=f.fserial_number
left join detect_two as g on a.fseries_number=g.fserial_number
left join detect_three as h on a.fseries_number=h.fserial_number
left join detect_three_pingmu as j on a.fseries_number=j.fserial_number
where a.fstart_time>=to_date(date_sub(from_unixtime(unix_timestamp()),400))

union all

-- B端鱼市数据
select
    a.fstart_time,
    a.fseries_number,
    a.fclass_name,
    case when b.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
    a.platform_type,
    a.fchannel_name,
    a.fproduct_name,
    a.fproject_name,
    a.Fcity_name,
    a.Forder_address,
    a.Freceiver_id,
    a.Freceiver_name,
    a.Freceiver_phone,
    b.Fdet_tpl,
    b.Freal_name,
    b.Fend_time,
    b.Fdetection_object,
    case when b.fwarehouse_code='12' then "东莞仓"
    	 when right(left(a.fseries_number,6),2)="16" or left(a.fseries_number,3)="020" then "杭州仓"
    else "深圳仓" end as fwarehouse_code,
    get_json_object(b.Fgoods_level,'$.levelName') as Fgoods_level,
    case
        when b.Fdet_tpl=1 then "大检测"
        when (b.Fdet_tpl=0 or b.Fdet_tpl=2 or b.Fdet_tpl=6 or b.Fdet_tpl=7) then "竞拍检测"
    else '其他' end as "检测渠道",
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
    a.fcost_price/100 as "成本价",
    a.foffer_price/100 as "销售价格",
    if(g.fdetect_two_name is null,b.freal_name,g.fdetect_two_name) as fdetect_two_name,
    if(h.fdetect_three_name is null,b.freal_name,h.fdetect_three_name) as fdetect_three_name,
    j.fdetect_three_name_pingmu,
    if(g.fdetect_two_time is not null,"是","否") as "是否分模块",
    a.fanchor_level
from b2b_yushi_sale as a
left join detect as b on a.fseries_number=b.fserial_number
left join yushi_after_sale as c on a.fseries_number=c.fbusiness_id and c.Fsource=2  -- B端鱼市售后
left join detect_two as g on a.fseries_number=g.fserial_number
left join detect_three as h on a.fseries_number=h.fserial_number
left join detect_three_pingmu as j on a.fseries_number=j.fserial_number
where a.fstart_time>=to_date(date_sub(from_unixtime(unix_timestamp()),400))

union all

-- 采货侠数据
select
    a.fstart_time,
    a.fseries_number,
    a.fclass_name,
    case when b.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
    a.platform_type,
    a.fchannel_name,
    a.fproduct_name,
    a.fproject_name,
    a.Fcity_name,
    a.Forder_address,
    a.Freceiver_id,
    a.Freceiver_name,
    a.Freceiver_phone,
    b.Fdet_tpl,
    b.Freal_name,
    b.Fend_time,
    b.Fdetection_object,
    case when b.fwarehouse_code='12' then "东莞仓"
    	 when right(left(a.fseries_number,6),2)="16" or left(a.fseries_number,3)="020" then "杭州仓"
    else "深圳仓" end as fwarehouse_code,
    get_json_object(b.Fgoods_level,'$.levelName') as Fgoods_level,
    case
        when b.Fdet_tpl=1 then "大检测"
        when (b.Fdet_tpl=0 or b.Fdet_tpl=2 or b.Fdet_tpl=6 or b.Fdet_tpl=7) then "竞拍检测"
    else '其他' end as "检测渠道",
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
    a.fcost_price/100 as "成本价",
    a.foffer_price/100 as "销售价格",
    if(g.fdetect_two_name is null,b.freal_name,g.fdetect_two_name) as fdetect_two_name,
    if(h.fdetect_three_name is null,b.freal_name,h.fdetect_three_name) as fdetect_three_name,
    j.fdetect_three_name_pingmu,
    if(g.fdetect_two_time is not null,"是","否") as "是否分模块",
    a.fanchor_level
from caihuoxia_sale as a
left join detect as b on a.fseries_number=b.fserial_number
left join yushi_after_sale as c on a.fseries_number=c.fbusiness_id and c.Fsource=1  -- 采货侠售后
left join detect_two as g on a.fseries_number=g.fserial_number
left join detect_three as h on a.fseries_number=h.fserial_number
left join detect_three_pingmu as j on a.fseries_number=j.fserial_number
where a.fstart_time>=to_date(date_sub(from_unixtime(unix_timestamp()),400))

union all

-- C端鱼市数据
select
    a.fstart_time,
    a.fseries_number,
    a.fclass_name,
    case when b.fbrand_name="苹果" then "苹果" else "安卓" end as fbrand_name,
    a.platform_type,
    a.fchannel_name,
    a.fproduct_name,
    a.fproject_name,
    a.Fcity_name,
    a.Forder_address,
    a.Freceiver_id,
    a.Freceiver_name,
    a.Freceiver_phone,
    b.Fdet_tpl,
    b.Freal_name,
    b.Fend_time,
    b.Fdetection_object,
    case when b.fwarehouse_code='12' then "东莞仓"
    	 when right(left(a.fseries_number,6),2)="16" or left(a.fseries_number,3)="020" then "杭州仓"
    else "深圳仓" end as fwarehouse_code,
    get_json_object(b.Fgoods_level,'$.levelName') as Fgoods_level,
    case
        when b.Fdet_tpl=1 then "大检测"
        when (b.Fdet_tpl=0 or b.Fdet_tpl=2 or b.Fdet_tpl=6 or b.Fdet_tpl=7) then "竞拍检测"
    else '其他' end as "检测渠道",
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
    a.fcost_price/100 as "成本价",
    a.foffer_price/100 as "销售价格",
    if(g.fdetect_two_name is null,b.freal_name,g.fdetect_two_name) as fdetect_two_name,
    if(h.fdetect_three_name is null,b.freal_name,h.fdetect_three_name) as fdetect_three_name,
    j.fdetect_three_name_pingmu,
    if(g.fdetect_two_time is not null,"是","否") as "是否分模块",
    a.fanchor_level
from c_end_yushi_sale as a
left join detect as b on a.fseries_number=b.fserial_number
left join yushi_after_sale as c on a.fseries_number=c.fbusiness_id and c.Fsource=3  -- C端鱼市售后
left join detect_two as g on a.fseries_number=g.fserial_number
left join detect_three as h on a.fseries_number=h.fserial_number
left join detect_three_pingmu as j on a.fseries_number=j.fserial_number
where a.fstart_time>=to_date(date_sub(from_unixtime(unix_timestamp()),400))
