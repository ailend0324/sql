-- 财务毛利数据查询SQL
-- 这个SQL的作用是：把三个不同来源的财务数据合并在一起，方便查看整体的财务情况
-- 就像把三个不同的账本合并成一个总账本一样

-- 第一部分：从竞拍维度表查询数据
select
    -- 用户组ID（用来区分不同的用户群体）
    fuser_group_id,
    -- 用户组名称（比如：VIP用户、普通用户等）
    fuser_group_name,
    -- 项目ID（用来区分不同的业务项目）
    fproject_id,
    -- 项目名称（如果项目是'可乐优品'且供应商不是'卢会苹'，就显示为'华为采购'，否则显示原项目名）
    -- 这就像给某些特殊情况起个别名一样
    if((fproject_name='可乐优品' and Fsupplier_name!='卢会苹'), '华为采购', fproject_name) as fproject_name,
    -- 渠道ID（销售渠道的编号）
    fchannel_id,
    -- 渠道名称（比如：线上商城、实体店、代理商等）
    fchannel_name,
    -- 产品ID（产品的唯一编号）
    fpid,
    -- 产品名称（比如：iPhone 13、华为P40等）
    fpid_name,
    -- 回收类型（比如：手机回收、电脑回收等）
    frecycle_type,
    -- 接入业务（业务接入的方式）
    faccess_business,
    -- 配送方式（比如：快递、自提、上门等）
    fdelivery_mode,
    -- 支付方式（比如：现金、银行卡、支付宝等）
    fpayment_mode,
    -- 业务模式（比如：B2B、B2C等）
    fbusiness_mode,
    -- 产品名称（产品的具体名称）
    fproduct_name,
    -- 分类名称（产品的大类，比如：手机、电脑、配件等）
    -- cast是转换的意思，把分类名转换成字符串格式
    cast(fclass_name as string) as fclass_name,
    -- 品牌名称（比如：苹果、华为、小米等）
    fbrand_name,
    -- 颜色名称（比如：黑色、白色、金色等）
    fcolor_name,
    -- 内存名称（比如：128GB、256GB等）
    fmemory_name,
    -- 检测模板（用来检测产品质量的标准模板）
    fdet_tpl,
    -- 按天统计的时间
    ftime_byday,
    -- 回收全类型（回收业务的完整分类）
    frecycle_alltype,
    -- 销售类型（销售的方式分类）
    fsales_type,

    -- 销售数量（卖出了多少个产品）
    fsales_qty,
    -- 退货金额（客户退货时退回的钱）
    freturn_amount,
    -- 退货商品成本金额（退货商品的成本价）
    freturn_goods_cost_amount,
    -- 报价金额A（第一个来源的报价）
    foffer_amount               as foffer_amounta,
    -- 报价金额B（第二个来源的报价，这里设为0）
    0                           as foffer_amountb,
    -- 报价金额C（第三个来源的报价，这里设为0）
    0                           as foffer_amountc,
    -- 成本金额A（第一个来源的成本）
    fcost_amount                as fcost_amounta,
    -- 成本金额B（第二个来源的成本，这里设为0）
    0                           as fcost_amountb,
    -- 成本金额C（第三个来源的成本，这里设为0）
    0                           as fcost_amountc,

    -- 退货数量（退货的商品个数）
    freturn_qty,
    -- 检测金额（产品检测的费用）
    fdetection_amount           as fdetection_amount,
    -- 供应合作伙伴（提供产品的合作方）
    fsupply_partner

-- 从竞拍维度表查询数据
from dm.dm_jp_dimension

-- 用UNION ALL把三个查询结果合并在一起
-- UNION ALL就像把三张表格上下拼接在一起，不会去重
union all

-- 第二部分：从天润维度表查询数据
select
    -- 用户组ID（用来区分不同的用户群体）
    fuser_group_id,
    -- 用户组名称（比如：VIP用户、普通用户等）
    fuser_group_name,
    -- 项目ID（用来区分不同的业务项目）
    fproject_id,
    -- 项目名称（如果项目是'可乐优品'且供应商不是'卢会苹'，就显示为'华为采购'，否则显示原项目名）
    if((fproject_name='可乐优品' and Fsupplier_name!='卢会苹'), '华为采购', fproject_name) as fproject_name,
    -- 渠道ID（销售渠道的编号）
    fchannel_id,
    -- 渠道名称（比如：线上商城、实体店、代理商等）
    fchannel_name,
    -- 产品ID（产品的唯一编号）
    fpid,
    -- 产品名称（比如：iPhone 13、华为P40等）
    fpid_name,
    -- 回收类型（比如：手机回收、电脑回收等）
    frecycle_type,
    -- 接入业务（业务接入的方式）
    faccess_business,
    -- 配送方式（比如：快递、自提、上门等）
    fdelivery_mode,
    -- 支付方式（比如：现金、银行卡、支付宝等）
    fpayment_mode,
    -- 业务模式（比如：B2B、B2C等）
    fbusiness_mode,
    -- 产品名称（产品的具体名称）
    fproduct_name,
    -- 分类名称（产品的大类，比如：手机、电脑、配件等）
    cast(fclass_name as string) as fclass_name,
    -- 品牌名称（比如：苹果、华为、小米等）
    fbrand_name,
    -- 颜色名称（比如：黑色、白色、金色等）
    fcolor_name,
    -- 内存名称（比如：128GB、256GB等）
    fmemory_name,
    -- 检测模板（用来检测产品质量的标准模板）
    fdet_tpl,
    -- 按天统计的时间
    ftime_byday,
    -- 回收全类型（回收业务的完整分类）
    frecycle_alltype,
    -- 销售类型（销售的方式分类）
    fsales_type,

    -- 销售数量（卖出了多少个产品）
    fsales_qty,
    -- 退货金额（这里设为0，因为天润表没有退货数据）
    0                           as freturn_amount,
    -- 退货商品成本金额（这里设为0，因为天润表没有退货数据）
    0                           as freturn_goods_cost_amount,
    -- 报价金额A（这里设为0，因为天润表的数据放在B列）
    0                           as foffer_amounta,
    -- 报价金额B（天润表的报价数据）
    foffer_amount               as foffer_amountb,
    -- 报价金额C（这里设为0，因为天润表没有C列数据）
    0                           as foffer_amountc,
    -- 成本金额A（这里设为0，因为天润表的数据放在B列）
    0                           as fcost_amounta,
    -- 成本金额B（天润表的成本数据）
    fcost_amount                as fcost_amountb,
    -- 成本金额C（这里设为0，因为天润表没有C列数据）
    0                           as fcost_amountc,

    -- 退货数量（这里设为0，因为天润表没有退货数据）
    0                           as freturn_qty,
    -- 检测金额（产品检测的费用）
    fdetection_amount           as fdetection_amount,
    -- 供应合作伙伴（提供产品的合作方）
    fsupply_partner

-- 从天润维度表查询数据
from dm.dm_th_dimension

-- 继续用UNION ALL合并第三个查询结果
union all

-- 第三部分：从自采维度表查询数据
select
    -- 用户组ID（用来区分不同的用户群体）
    fuser_group_id,
    -- 用户组名称（比如：VIP用户、普通用户等）
    fuser_group_name,
    -- 项目ID（用来区分不同的业务项目）
    fproject_id,
    -- 项目名称（如果项目是'可乐优品'且供应商不是'卢会苹'，就显示为'华为采购'，否则显示原项目名）
    if((fproject_name='可乐优品' and Fsupplier_name!='卢会苹'), '华为采购', fproject_name) as fproject_name,
    -- 渠道ID（销售渠道的编号）
    fchannel_id,
    -- 渠道名称（比如：线上商城、实体店、代理商等）
    fchannel_name,
    -- 产品ID（产品的唯一编号）
    fpid,
    -- 产品名称（比如：iPhone 13、华为P40等）
    fpid_name,
    -- 回收类型（比如：手机回收、电脑回收等）
    frecycle_type,
    -- 接入业务（业务接入的方式）
    faccess_business,
    -- 配送方式（比如：快递、自提、上门等）
    fdelivery_mode,
    -- 支付方式（比如：现金、银行卡、支付宝等）
    fpayment_mode,
    -- 业务模式（比如：B2B、B2C等）
    fbusiness_mode,
    -- 产品名称（产品的具体名称）
    fproduct_name,
    -- 分类名称（产品的大类，比如：手机、电脑、配件等）
    cast(fclass_name as string) as fclass_name,
    -- 品牌名称（比如：苹果、华为、小米等）
    fbrand_name,
    -- 颜色名称（比如：黑色、白色、金色等）
    fcolor_name,
    -- 内存名称（比如：128GB、256GB等）
    fmemory_name,
    -- 检测模板（用来检测产品质量的标准模板）
    fdet_tpl,
    -- 按天统计的时间
    ftime_byday,
    -- 回收全类型（回收业务的完整分类）
    frecycle_alltype,
    -- 销售类型（销售的方式分类）
    fsales_type,

    -- 销售数量（卖出了多少个产品）
    fsales_qty,
    -- 退货金额（这里设为0，因为自采表没有退货数据）
    0                           as freturn_amount,
    -- 退货商品成本金额（这里设为0，因为自采表没有退货数据）
    0                           as freturn_goods_cost_amount,
    -- 报价金额A（这里设为0，因为自采表的数据放在C列）
    0                           as foffer_amounta,
    -- 报价金额B（这里设为0，因为自采表没有B列数据）
    0                           as foffer_amountb,
    -- 报价金额C（自采表的报价数据）
    foffer_amount               as foffer_amountc,
    -- 成本金额A（这里设为0，因为自采表的数据放在C列）
    0                           as fcost_amounta,
    -- 成本金额B（这里设为0，因为自采表没有B列数据）
    0                           as fcost_amountb,
    -- 成本金额C（自采表的成本数据）
    fcost_amount                as fcost_amountc,

    -- 退货数量（这里设为0，因为自采表没有退货数据）
    0                           as freturn_qty,
    -- 检测金额（产品检测的费用）
    fdetection_amount           as fdetection_amount,
    -- 供应合作伙伴（提供产品的合作方）
    fsupply_partner

-- 从自采维度表查询数据
from dm.dm_zc_dimension

-- 总结：这个SQL就像把三个不同的账本（竞拍、天润、自采）合并成一个总账本
-- 每个账本都有相同的基础信息（用户、项目、产品等），但财务数据（报价、成本等）放在不同的列
-- 这样合并后，就能在一个表格里看到所有业务的完整财务情况了
