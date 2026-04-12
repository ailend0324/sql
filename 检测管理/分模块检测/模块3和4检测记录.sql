with t_xianyu_order_activity_add_record_order as (
    /*
        闲鱼自定义订单活动加价记录表:下单环节
    */
    select
        Forder_id,
        Fadd_price,
        Factivity_type
    from(
        SELECT
          Forder_id,
          Fadd_price,
          Fdelete_flag,
          Fcreate_time,
          Factivity_id,
          Factivity_type,
          row_number() over(partition by forder_id order by Fauto_create_time desc) as frn 
        FROM drt.drt_my33310_recycle_t_xianyu_order_activity_add_record 
        WHERE 1=1
        and Forder_step = 1 -- '订单加价所在的步骤,1.下单,2.检测3.议价',
        and to_date(Fauto_create_time) >='2024-11-25'
    ) t 
    where frn = 1
)
select
    case 
        when to_date(e.forder_time) >= '2024-11-26' and to_date(e.forder_time) <= '2024-11-29' then '241126-241129华为mate70闲鱼加价券'
        when to_date(e.forder_time) >= '2025-01-14' and to_date(e.forder_time) <= '2025-01-22' then '250114-250122闲鱼上门回收(暖冬行动)'
        when to_date(e.forder_time) >= '2025-02-18' and to_date(e.forder_time) <= '2025-02-24' then '250218-250224闲鱼复苏季活动'
        when to_date(e.forder_time) >= '2025-03-07' and to_date(e.forder_time) <= '2025-03-16' then '250307-250316荣耀国补哪吒联名'
        when to_date(e.forder_time) >= '2025-05-26' and to_date(e.forder_time) <= '2025-06-03' then '250526-250603(618回收第一波)'
        when to_date(e.forder_time) >= '2025-06-23' and to_date(e.forder_time) < '2025-07-01' then '250623-250701(618回收第二波)'
        when to_date(e.forder_time) >= '2025-07-07' and to_date(e.forder_time) < '2025-07-14' then '250707-250714(闲鱼7月活动)'
    else '未定义' end as 活动自定义名称,
    a.forder_id 订单ID,
    e.fseries_number 条码,
    to_date(a.forder_time) 下单时间,
    to_date(e.fsend_time) 发货时间,
    to_date(e.fgetin_time) 收货时间,
    e.fpay_out_time 成交时间,
    e.fpay_out_price/100 成交金额,
    a.fhsb_channelid 渠道Id,
    a.fquote_price/100 预估价格,
    a.Fdetect_price/100 最终检测价,
    d.fchannel_name 下单渠道,
    c.fname 下单类目,
	e.fproduct_id 机型ID,
    e.fproduct_name 机型名称,
    if(e.ftest=0,'非测试','测试') 是否测试单,
    CASE e.Frecycle_type
    WHEN 0 THEN
      '邮寄回收'
    WHEN 1 THEN
      '邮寄回收' 
    WHEN 2 THEN
      '上门回收'
    WHEN 3 THEN
      '到店回收'
    WHEN 4 THEN
      'ATM回收'
    ELSE cast(e.Frecycle_type as string)
    END '回收方式',
    ifnull(h.Fadd_price/100,0) 下单加价券,
    i.forder_status_name 订单状态,
    case 
        when e.fchannel_id in(10000135, 10000141, 10000143, 10000170, 10000188, 10000190, 10000193, 10000203, 10000216, 10000266, 10000295, 10000323, 10000324, 10000335, 10000429, 10000935, 10000943, 10001102, 10001104, 10001131, 10000325,10000151,10000267) then '闲鱼价格包' 
        when e.fchannel_id = 10000342 then '闲鱼换新'
        when e.fchannel_id = 10001191 then '闲鱼保卖'
    else '天猫价格包' end as '价格包'
from drt.drt_my33310_recycle_t_xy_order_data a
inner join drt.drt_my33310_recycle_t_order e on a.forder_Id = e.forder_id
left join drt.drt_my33310_recycle_t_channel d on e.fchannel_id = d.fchannel_id
left join t_xianyu_order_activity_add_record_order h on a.forder_id = h.forder_id
left join drt.drt_my33310_recycle_t_order_status i on e.forder_status = i.forder_status_id
left join drt.drt_my33310_recycle_t_pdt_class c on e.fproduct_class_id = c.fid
where (e.ftest = 0 and to_date(e.forder_time) >= '2024-11-26' and to_date(e.forder_time) <= '2024-11-29')
or (e.ftest = 0 and to_date(e.forder_time) >= '2025-01-14' and to_date(e.forder_time) <= '2025-01-22')
or (e.ftest = 0 and to_date(e.forder_time) >= '2025-02-18' and to_date(e.forder_time) <= '2025-02-24')
or (e.ftest = 0 and to_date(e.forder_time) >= '2025-03-07' and to_date(e.forder_time) <= '2025-03-16')
or (e.ftest = 0 and to_date(e.forder_time) >= '2025-05-26' and to_date(e.forder_time) <= '2025-06-03')
or (e.ftest = 0 and to_date(e.forder_time) >= '2025-06-23' and to_date(e.forder_time) < '2025-07-01')
or (e.ftest = 0 and to_date(e.forder_time) >= '2025-07-07' and to_date(e.forder_time) < '2025-07-14')

