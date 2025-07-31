# 回收宝SQL规则完整分析文档

> **文档说明**: 基于54个SQL文件的深度分析，完整提取回收宝数据库设计规则、业务逻辑和最佳实践

---

## 📋 目录

1. [表结构设计规则](#1-表结构设计规则)
2. [字段命名规范](#2-字段命名规范) 
3. [业务编码规则](#3-业务编码规则)
4. [SQL函数使用模式](#4-sql函数使用模式)
5. [业务逻辑映射规则](#5-业务逻辑映射规则)
6. [查询结构模式](#6-查询结构模式)
7. [最佳实践总结](#7-最佳实践总结)

---

## 1. 表结构设计规则

### 1.1 表命名层级结构

```
数据库.数据层级_项目编号_业务模块_表类型_具体功能
```

| 层级 | 前缀 | 说明 | 示例 |
|------|------|------|------|
| 数据仓库层 | `drt.drt_my33310_` | 业务主表 | `drt.drt_my33310_recycle_t_order` |
| 数据服务层 | `dws.dws_` | 汇总处理表 | `dws.dws_hs_order_detail` |
| 数据明细层 | `dwd.dwd_` | 明细数据表 | `dwd_detect_back_detection_issue_and_answer_v2` |
| 原始数据层 | `ods.ods_` | 原始导入表 | `ods.ods_kf_tianrun_describe_cdr_ib` |

### 1.2 业务模块表分类

#### 订单相关表
```sql
drt.drt_my33310_recycle_t_order                    -- 回收订单主表
drt.drt_my33310_recycle_t_xy_order_data           -- 闲鱼订单数据
drt.drt_my33310_recycle_t_after_sales_order_info  -- 售后订单信息
drt.drt_my33310_recycle_t_order_txn               -- 订单交易记录
```

#### 检测相关表  
```sql
drt.drt_my33310_detection_t_detect_record         -- 检测记录主表
drt.drt_my33315_xy_detect_t_record_info           -- 验机检测记录
drt.drt_my33310_detection_t_detect_issue          -- 检测问题表
```

#### 产品相关表
```sql
drt.drt_my33310_recycle_t_product                 -- 产品信息表
drt.drt_my33310_recycle_t_pdt_class               -- 产品类别表
drt.drt_my33310_recycle_t_pdt_brand               -- 产品品牌表
```

#### 渠道相关表
```sql
drt.drt_my33310_recycle_t_channel                 -- 渠道信息表
drt.drt_my33310_pub_server_channel_center_db_t_pid_info  -- 渠道中心表
```

### 1.3 表关系结构树

```
回收订单主表 (drt_my33310_recycle_t_order)
├── 产品信息 (t_product)
│   ├── 产品类别 (t_pdt_class)
│   └── 产品品牌 (t_pdt_brand)
├── 渠道信息 (t_channel)
├── 账户信息 (t_account_info)
├── 检测记录 (detection_t_detect_record)
│   ├── 检测问题 (t_detect_issue)
│   └── 检测答案 (detection_issue_and_answer)
├── 售后订单 (t_after_sales_order_info)
└── 闲鱼订单 (t_xy_order_data)
    ├── 闲鱼交易 (t_xianyu_order_txn)
    └── 闲鱼评估 (t_xy_eva_data)
```

---

## 2. 字段命名规范

### 2.1 字段前缀规则表

| 前缀 | 用途 | 示例 | 说明 |
|------|------|------|------|
| `f` | 标准业务字段 | `forder_id`, `fseries_number` | 所有核心业务字段 |
| `F` | 外部系统字段 | `Fxy_order_id`, `Fcreate_dtime` | 来自外部系统的字段 |
| 无前缀 | 计算字段 | `dt`, `num`, `place` | 查询中的临时字段 |

### 2.2 字段类型命名模式

#### 时间字段
| 字段模式 | 示例 | 含义 |
|----------|------|------|
| `f*_time` | `forder_time`, `fend_time` | 业务时间点 |
| `f*_dtime` | `fcreate_dtime`, `fupdate_dtime` | 系统时间戳 |
| `fauto_*_time` | `fauto_create_time` | 自动生成时间 |

#### 标识字段
| 字段模式 | 示例 | 含义 |
|----------|------|------|
| `f*_id` | `forder_id`, `fproduct_id` | 主键/外键ID |
| `f*_number` | `fseries_number`, `fserial_number` | 业务编号 |
| `f*_name` | `fproduct_name`, `freal_name` | 名称字段 |

#### 状态字段
| 字段模式 | 示例 | 含义 |
|----------|------|------|
| `f*_status` | `forder_status` | 状态码 |
| `fis_*` | `fis_deleted` | 布尔标识 |
| `f*_type` | `frecycle_type`, `fdet_type` | 类型码 |

#### 金额字段
| 字段模式 | 示例 | 存储规则 |
|----------|------|----------|
| `f*_price` | `fpay_out_price`, `fquote_price` | 以分为单位存储 |
| `f*_fee` | `fconfirm_fee`, `fservice_fee` | 以分为单位存储 |

### 2.3 特殊字段命名规律

#### 地理位置字段
```sql
-- 省市相关
fprovince_name, fcity_name, fcity_id
Fdeliver_province, Fdeliver_city, Fdeliver_address

-- 物流相关  
flogistics_id, flogistics_number, fexpress_reality_sn
```

#### 用户相关字段
```sql
-- 用户标识
fuser_id, faccount_id, freal_name, fuser_name

-- 联系方式
fmobile, fphone, femail
```

---

## 3. 业务编码规则

### 3.1 订单号编码规则表

| 前缀 | 业务类型 | 说明 | 示例 |
|------|----------|------|------|
| `XY` | 2C闲鱼 | 闲鱼平台C端订单 | `XY202401010001` |
| `YJ` | 2C闲鱼 | 闲鱼平台C端订单 | `YJ202401010002` |
| `TM` | 天猫以旧换新 | 天猫平台以旧换新 | `TM202401010003` |
| `TY` | 天猫以旧换新 | 天猫平台以旧换新 | `TY202401010004` |
| `ZF` | 支付宝小程序 | 支付宝渠道订单 | `ZF202401010005` |
| `CG` | 外采 | 外部采购订单 | `CG202401010006` |
| `BB` | 换机侠B端帮卖 | B端帮卖订单 | `BB202401010007` |
| `ZY` | 滞留单 | 滞留处理订单 | `ZY202401010008` |
| `QT` | 其他 | 其他类型订单 | `QT202401010009` |
| `YZ` | 售后回收 | 售后回收订单 | `YZ202401010010` |
| `NT` | 售后回收 | 售后回收订单 | `NT202401010011` |
| `01*` | 验货宝 | 验货宝业务订单 | `010202401010001` |
| `02*` | 验货宝 | 验货宝业务订单 | `020202401010002` |
| `05*` | 验货宝 | 验货宝业务订单 | `050202401010003` |

### 3.2 仓库编码规则

#### 通过订单号识别仓库
```sql
-- 仓库识别逻辑
case when right(left(fseries_number,6),4)='0112' then "东莞仓"
     when right(left(fseries_number,6),4)='0118' then "东莞仓"  
     when right(left(fseries_number,6),2)="16" then "杭州仓"
     when left(fseries_number,3) like "%020%" then "杭州仓"
     when left(fseries_number,3) like "%050%" then "东莞仓"
     else "深圳仓" end
```

#### 仓库编码表
| 编码位置 | 编码值 | 仓库名称 | 说明 |
|----------|--------|----------|------|
| 订单号第3-6位 | `0112` | 东莞仓 | 东莞仓库代码 |
| 订单号第3-6位 | `0118` | 东莞仓 | 东莞仓库代码 |
| 订单号第5-6位 | `16` | 杭州仓 | 杭州仓库代码 |
| 订单号前3位 | `020` | 杭州仓 | 杭州验机业务 |
| 订单号前3位 | `050` | 东莞仓 | 东莞验机业务 |
| 订单号前3位 | `010` | 深圳仓 | 深圳验机业务 |

### 3.3 业务状态编码

#### 订单状态码表
| 状态码 | 状态说明 | 业务含义 |
|--------|----------|----------|
| `714` | 买家付款 | 用户已完成支付 |
| `815` | 买家付款 | 用户已完成支付 |
| `80` | 取消状态 | 订单已取消 |
| `88` | 特殊状态 | 需要排除的状态 |
| `90` | 未完结状态 | 订单处理中 |
| `110` | 未完结状态 | 订单处理中 |
| `351` | 退款状态 | 订单已退款 |

#### 回收方式编码
| 编码 | 回收方式 | 说明 |
|------|----------|------|
| `1` | 邮寄 | 用户邮寄设备 |
| `2` | 上门 | 工作人员上门回收 |
| `3` | 到店 | 用户到店回收 |

#### 供应合作伙伴编码
| 编码 | 合作伙伴类型 | 说明 |
|------|--------------|------|
| `2` | 小站(自营) | 自营小站 |
| `3` | 小站(加盟) | 加盟小站 |
| 其他 | 回收宝 | 回收宝自营 |

#### 检测模板编码
| 模板ID | 模板名称 | 说明 |
|--------|----------|------|
| `0` | 竞拍检测 | 竞拍业务检测 |
| `1` | 大检测 | 全面检测 |
| `2` | 竞拍检测 | 竞拍业务检测 |
| `4` | 销售检测 | 销售前检测 |
| `6` | 闲鱼寄卖plus | 闲鱼寄卖检测 |
| `7` | 竞拍检测 | 竞拍业务检测 |

---

## 4. SQL函数使用模式

### 4.1 时间处理函数模式

#### 常用时间函数组合
| 函数组合 | 示例 | 用途 |
|----------|------|------|
| `to_date()` | `to_date(forder_time)` | 提取日期部分 |
| `date_sub()` | `date_sub(from_unixtime(unix_timestamp()),720)` | 向前推算天数 |
| `substr()` | `substr(forder_time,1,10)` | 截取日期字符串 |
| `unix_timestamp()` | `unix_timestamp(time1)-unix_timestamp(time2)` | 计算时间差 |

#### 时间处理标准模式
```sql
-- 日期范围过滤标准写法
where to_date(fend_time) >= to_date(date_sub(from_unixtime(unix_timestamp()),720))

-- 当天数据筛选
where feva_time between to_date(now()) and now()

-- 时间段筛选  
where to_date(fend_time) between '2023-01-01' and '2023-12-31'
```

### 4.2 字符串处理函数

#### 字符串截取模式
| 函数 | 示例 | 用途 |
|------|------|------|
| `left()` | `left(fseries_number,2)` | 左侧截取N位 |
| `right()` | `right(left(fseries_number,6),4)` | 组合截取 |
| `substr()` | `substr(forder_time,1,10)` | 指定位置截取 |
| `upper()` | `upper(fserial_number)` | 转大写 |

#### 字符串匹配模式
```sql
-- 模糊匹配
where fchannel_name like "%闲鱼小站%"
where fproduct_name not like "%公益%"

-- 精确匹配
where left(fseries_number,2) in ('XY','YJ')
where left(fseries_number,3) like "%020%"
```

### 4.3 窗口函数使用模式

#### 标准排序去重模式
```sql
-- 基础模式
row_number() over(partition by fserial_number order by fend_time desc) as num

-- 常用分区字段
partition by fserial_number    -- 按序列号分区
partition by forder_id         -- 按订单ID分区  
partition by fseries_number    -- 按订单号分区

-- 常用排序字段
order by fend_time desc        -- 按结束时间倒序
order by forder_time asc       -- 按订单时间正序
order by fcreate_dtime desc    -- 按创建时间倒序
```

#### 窗口函数应用场景
```sql
-- 取最新检测记录
select * from (
    select *, row_number() over(partition by fserial_number order by fend_time desc) as num
    from detection_table
) t where num = 1

-- 取最早订单记录
select * from (
    select *, row_number() over(partition by forder_id order by forder_time asc) as num  
    from order_table
) t where num = 1
```

### 4.4 条件函数组合

#### IF函数嵌套模式
```sql
-- 多层IF嵌套处理空值
if(h.fsrouce_serial_no is not null, h.fsrouce_serial_no,
   if(g.fold_fseries_number is not null, g.fold_fseries_number,
      if(f.fold_fseries_number is not null, f.fold_fseries_number,
         default_value)))
```

#### CASE WHEN标准结构
```sql
case when condition1 then result1
     when condition2 then result2  
     when condition3 then result3
     else default_result end as alias_name
```

---

## 5. 业务逻辑映射规则

### 5.1 产品类目标准化映射

#### 主要产品类目映射表
| 原始类目 | 标准类目 | 包含子类目 |
|----------|----------|------------|
| 手机类 | `手机` | `手机`, `""` |
| 平板类 | `平板` | `平板`, `平板电脑` |
| 笔记本类 | `笔记本` | `笔记本`, `笔记本电脑` |
| 3C数码配件 | `3C数码配件` | `单反闪光灯`, `移动电源`, `移动硬盘`, `云台` |
| 办公设备 | `办公设备耗材` | `激光打印机`, `投影仪`, `收款机`, `硒鼓粉盒` |
| 电脑硬件 | `电脑硬件及周边` | `CPU`, `显卡`, `内存条`, `固态硬盘`, `显示器` |
| 相机摄像 | `相机/摄像机` | `单反相机`, `数码相机`, `微单相机`, `摄像机` |
| 影音数码 | `影音数码/电器` | `耳机`, `蓝牙音响`, `智能音响`, `麦克风` |

#### 产品类目映射SQL模式
```sql
case when fcategory in ('平板','平板电脑') then '平板'
     when fcategory in ('笔记本','笔记本电脑') then '笔记本'  
     when fcategory in ('手机','') then '手机'
     when fcategory in ('单反闪光灯','单反转接环','移动电源','移动硬盘','云台') then '3C数码配件'
     when fcategory in ('激光打印机','打印机','投影仪','收款机','硒鼓粉盒') then '办公设备耗材'
     when fcategory in ('CPU','显卡','内存条','固态硬盘','显示器','键盘') then '电脑硬件及周边'
     when fcategory in ('单反相机','数码相机','微单相机','摄像机','相机镜头') then '相机/摄像机'
     when fcategory in ('耳机','蓝牙音响','智能音响','麦克风','蓝牙耳机') then '影音数码/电器'
     else '其他' end as standard_category
```

### 5.2 渠道分类映射规则

#### 渠道分类层级结构
```
一级分类: 自有 / 合作 / 小站 / 其他
├── 自有渠道
│   ├── APP投放 (APP_android, APP_ios)
│   ├── 微信小程序及其他 (PC, H5, 微信小程序)
│   ├── 投放H5 (投放H5-抖音, 投放H5-搜索引擎)
│   └── 品牌商城 (vivo商城, 华为商城, 荣耀商城)
├── 合作渠道  
│   ├── 闲鱼 (2C闲鱼)
│   ├── 天猫 (天猫以旧换新)
│   └── 支付宝 (支付宝小程序, 支付宝比价回收)
├── 小站渠道
│   ├── 闲鱼小站-自营 (上门/到店)
│   └── 闲鱼小站-加盟 (上门/到店)  
└── 其他渠道
    ├── CPS中小渠道
    └── 第三方合作
```

#### 渠道识别SQL逻辑
```sql
case when a.frecycle_type=2 and fsupply_partner=2 then "闲鱼小站-自营上门"
     when a.frecycle_type=2 and fsupply_partner=3 then "闲鱼小站-加盟门店-上门"  
     when a.frecycle_type=3 and fsupply_partner=2 then "闲鱼小站-自营门店-到店"
     when a.frecycle_type=3 and fsupply_partner=3 then "闲鱼小站-加盟门店-到店"
     when left(fseries_number,2) in ('XY','YJ') then "2C闲鱼"
     when left(fseries_number,2) in ('TM','TY') then "天猫以旧换新"
     when left(fseries_number,2)='ZF' then "支付宝小程序"
     when left(fseries_number,2)='CG' then "外采"
     when left(fseries_number,2)="BB" then "换机侠B端帮卖"
     else "自有渠道" end as channel_type
```

### 5.3 履约方式映射

#### 履约方式分类表
| 履约方式编码 | 履约方式名称 | 业务场景 |
|--------------|--------------|----------|
| `邮寄` | 用户邮寄 | frecycle_type=1 |
| `上门+自营` | 自营小站上门 | frecycle_type=2 and fsupply_partner=2 |
| `上门+加盟` | 加盟小站上门 | frecycle_type=2 and fsupply_partner=3 |
| `到店+自营` | 自营小站到店 | frecycle_type=3 and fsupply_partner=2 |
| `到店+加盟` | 加盟小站到店 | frecycle_type=3 and fsupply_partner=3 |

### 5.4 业务类型识别规则

#### 基于订单号的业务类型识别
```sql
case when left(fserial_number,3) like "%020%" or left(fserial_number,3) like "%010%" or left(fserial_number,3) like "%050%" then "验机"
     when left(fserial_number,2) like "%BM%" then "寄卖"
     when left(fserial_number,2) like "%CG%" then "采购回收"  
     when left(fserial_number,2) like "%YZ%" or left(fserial_number,2) like "%NT%" then "售后回收"
     when left(fserial_number,2) like "%BB%" then "B端帮卖"
     else "回收" end as business_type
```

---

## 6. 查询结构模式

### 6.1 WITH CTE标准结构

#### 多层CTE查询模式
```sql
-- 标准WITH CTE结构
with layer1_data as (
    -- 第一层数据准备
    select 基础字段筛选
    from 核心业务表
    where 基础过滤条件
),
layer2_process as (
    -- 第二层数据处理  
    select 业务逻辑处理
    from layer1_data a
    left join 维度表 b on 关联条件
    where 进一步过滤
),
final_result as (
    -- 最终结果层
    select 最终输出字段
    from layer2_process
    where 最终过滤条件
)
select * from final_result
```

#### CTE命名规范
| CTE名称模式 | 用途 | 示例 |
|-------------|------|------|
| `detect` | 检测相关数据 | `with detect as (...)` |
| `order_info` | 订单信息处理 | `with order_info as (...)` |
| `deal` | 成交数据 | `with deal as (...)` |
| `final` | 最终结果集 | `with final as (...)` |
| `*_tod` | 当天数据 | `with gujia_tod as (...)` |
| `*_yes` | 昨天数据 | `with gujia_yes as (...)` |

### 6.2 多层关联查询模式

#### 订单追溯关联模式
```sql
-- 标准的7层订单追溯关联
from core_table t
left join dws.dws_hs_order_detail as b on t.fserial_number=b.fseries_number
left join dws.dws_hs_order_detail as c on b.fold_fseries_number=c.fseries_number  
left join dws.dws_hs_order_detail as d on c.fold_fseries_number=d.fseries_number
left join dws.dws_hs_order_detail as e on d.fold_fseries_number=e.fseries_number
left join dws.dws_hs_order_detail as f on e.fold_fseries_number=f.fseries_number
left join dws.dws_hs_order_detail as g on f.fold_fseries_number=g.fseries_number
left join other_table as h on t.fserial_number=h.fserial_no
```

#### 维度表关联模式
```sql
-- 标准维度表关联
from 主表 as a
left join 账户表 as b on a.faccount_id=b.faccount_id
left join 渠道表 as c on a.fchannel_id=c.fchannel_id  
left join 产品表 as d on a.fproduct_id=d.fproduct_id
left join 类别表 as e on d.fclass_id=e.fid
left join 品牌表 as f on d.fbrand_id=f.fid
```

### 6.3 去重和排序模式

#### 窗口函数去重标准模式
```sql
-- 检测记录去重（取最新）
select * from (
    select *, 
           row_number() over(partition by fserial_number order by fend_time desc) as num
    from detection_table
    where fis_deleted=0 and freport_type=0 and fverdict<>"测试单"
) t where num=1

-- 订单记录去重（取最早）  
select * from (
    select *,
           row_number() over(partition by forder_id order by forder_time asc) as num
    from order_table  
    where ftest=0
) t where num=1
```

### 6.4 UNION ALL组合模式

#### 多数据源合并查询
```sql
-- 标准UNION ALL结构
select 标准化字段列表
from 数据源1
where 过滤条件1

union all

select 相同字段列表  
from 数据源2
where 过滤条件2

union all

select 相同字段列表
from 数据源3
where 过滤条件3
```

---

## 7. 最佳实践总结

### 7.1 数据质量控制规则

#### 标准过滤条件组合
```sql
-- 检测数据质量控制
where fis_deleted=0              -- 未删除数据
  and freport_type=0             -- 正常报告类型
  and fverdict<>"测试单"         -- 排除测试数据
  and fdet_type=0                -- 一检数据
  
-- 订单数据质量控制  
where ftest=0                    -- 非测试订单
  and forder_status not in (88)  -- 排除异常状态
  and left(fseries_number,2) not in ('YZ','BM')  -- 排除特定类型
```

#### 时间范围控制
```sql
-- 相对时间范围（推荐）
where to_date(fend_time) >= to_date(date_sub(from_unixtime(unix_timestamp()),720))

-- 绝对时间范围
where to_date(forder_time) between '2023-01-01' and '2023-12-31'

-- 当天数据
where feva_time between to_date(now()) and now()
```

### 7.2 性能优化模式

#### 索引友好的查询方式
```sql
-- 推荐：使用函数索引字段
where to_date(forder_time) = '2023-01-01'

-- 推荐：范围查询
where forder_time >= '2023-01-01 00:00:00' 
  and forder_time < '2023-01-02 00:00:00'
  
-- 推荐：IN查询代替OR
where left(fseries_number,2) in ('XY','YJ','TM')
```

#### 分区查询优化
```sql
-- 按时间分区的查询优化
where to_date(forder_time) >= '2023-01-01'  -- 利用分区剪枝
  and other_conditions
```

### 7.3 代码可维护性规范

#### 字段别名规范
```sql
-- 推荐：有意义的别名
select forder_time as order_date,
       fpay_out_price/100 as pay_amount,
       case when ftest=0 then '正式' else '测试' end as order_type

-- 推荐：中文别名用于报表
select forder_time as "订单时间",
       case when frecycle_type=1 then "邮寄" 
            when frecycle_type=2 then "上门"
            else "到店" end as "回收方式"
```

#### 注释规范
```sql
-- 业务逻辑注释
select             -- 回收订单信息匹配
    to_date(a.forder_time) as fdate,
    case when Fpayment_mode=3 then "信用订单" else "普通订单" end as "订单类型",
    -- 仓库识别逻辑
    case when right(left(fseries_number,6),4)='0112' then "东莞仓" 
         else "深圳仓" end as place
```

### 7.4 错误处理和边界条件

#### 空值处理模式
```sql
-- IF函数处理空值链
if(e.fname is null, f.fclass_name, e.fname) as product_class

-- COALESCE函数处理空值（如果支持）
coalesce(e.fname, f.fclass_name, 'Unknown') as product_class

-- CASE WHEN处理复杂空值逻辑
case when e.fname is not null then e.fname
     when f.fclass_name is not null then f.fclass_name  
     else 'Unknown' end as product_class
```

#### 除零处理
```sql
-- 金额字段除零保护
case when fpay_out_price > 0 then fpay_out_price/100 else 0 end as pay_amount
```

### 7.5 业务规则标准化

#### 订单号解析标准函数
```sql
-- 标准化的业务类型识别函数
case when left(fseries_number,2) in ('XY','YJ') then "2C闲鱼"
     when left(fseries_number,2) in ('TM','TY') then "天猫以旧换新"
     when left(fseries_number,2)='ZF' then "支付宝小程序"
     when left(fseries_number,2)='CG' then "外采"
     when left(fseries_number,2)="BB" then "换机侠B端帮卖"
     when left(fseries_number,2) in ('YZ','NT') then "售后回收"
     else "自有渠道" end as channel_type
```

#### 仓库识别标准函数
```sql
-- 标准化的仓库识别函数
case when right(left(fseries_number,6),4) in ('0112','0118') then "东莞仓"
     when right(left(fseries_number,6),2)="16" then "杭州仓"
     when left(fseries_number,3) like "%020%" then "杭州仓"
     when left(fseries_number,3) like "%050%" then "东莞仓"
     else "深圳仓" end as warehouse
```

---

## 📊 总结

### 核心设计原则

1. **统一性**: 字段命名、表结构、业务逻辑保持高度一致
2. **可追溯性**: 通过fold_fseries_number建立完整的订单关系链
3. **可扩展性**: 模块化的表设计支持业务扩展
4. **规范化**: 标准化的编码规则和映射逻辑

### 关键业务特点

1. **复杂业务链路**: 从估价→下单→检测→销售的完整闭环
2. **多渠道整合**: 自有、合作、小站等多种渠道统一管理  
3. **精细化运营**: 基于订单号的精确业务识别和分类
4. **数据治理**: 严格的数据质量控制和标准化处理

### 技术实现亮点

1. **窗口函数**: 大量使用row_number()实现业务去重
2. **CTE结构**: 复杂查询的分层处理提高可读性
3. **字符串解析**: 基于订单号的智能业务识别
4. **多层关联**: 支持7层订单追溯的复杂关联查询

这套规则体系为回收宝的数据分析和业务运营提供了坚实的技术基础，也为类似电商回收业务的数据库设计提供了完整的参考模板。

---

**文档版本**: v1.0  
**分析文件数**: 54个SQL文件  
**最后更新**: 2024年12月  
**维护团队**: 数据分析团队