select fwarehouse
,sum(case when ftype="回收" then getnum else 0 end ) as huishou
,sum(case when ftype="太力" then getnum else 0 end ) as taili
,sum(case when ftype="验货宝" then getnum else 0 end ) as yanji
,sum(case when ftype="寄卖" then getnum else 0 end ) as jimai
,sum(getnum) as 总数
 from (
SELECT
		case 
        when left(a.fhost_barcode,3) like "%010%" then "深圳仓"
        when left(a.fhost_barcode,3) like "%020%" then "杭州仓"
        when left(a.fhost_barcode,3) like "%050%" then "东莞"
        else "" end as fwarehouse, 
		"验货宝" as ftype,
		COUNT(a.fhost_barcode) as getnum
        from drt.drt_my33315_xy_detect_t_xy_hsb_order as a
        left join drt.drt_my33315_xy_detect_t_xy_detect_receive_record as b on a.forder_id=b.forder_id
        where b.Freceive_time>=to_date(date_sub(from_unixtime(unix_timestamp()),0))
        GROUP BY 1,2
union all
select 
		case when right(left(a.fseries_number,6),4)="0112" then "东莞" 
             when right(left(a.fseries_number,6),2)="16" then "杭州仓"   
        else "深圳仓" end as fwarehouse,
		case when b.fchannel_name like "%闲鱼寄卖%" then "寄卖" 
		     when left(a.fseries_number,2)="CG" or left(a.fseries_number,2)="TL" then "太力"
		else "回收" end as ftype,
		COUNT(a.fseries_number) as getnum
        from drt.drt_my33310_recycle_t_order as a
        left join drt.drt_my33310_recycle_t_channel as b on a.fchannel_id=b.fchannel_id
        where a.Ftest=0
        and a.Frecycle_type=1
        and a.Fgetin_time>=to_date(date_sub(from_unixtime(unix_timestamp()),0))
        and left(a.fseries_number,2) <>"BB" 
        GROUP BY 1,2
)A
group by fwarehouse