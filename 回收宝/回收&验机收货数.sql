select 
 		to_date(fgetin_time) as time_by,
 		case when left(fseries_number,2)='BM' then "寄卖"
        else "回收" end as ftype,
 		case when right(left(fseries_number,6),4)="0112" then "东莞" 
        	 when right(left(fseries_number,6),2)="16" then "杭州仓"
    else "深圳" end as fwarehouse,
 		count(fseries_number) as num 
from drt.drt_my33310_recycle_t_order 
where to_date(fgetin_time)>='2022-01-01'
and ftest=0
group by 1,2,3
union all
select 
 		to_date(freceive_time) as time_by,
 		"验机" as ftype,
 		case when left(fhost_barcode,3)="020" then "杭州"
        	 when left(fhost_barcode,3)="010" then "深圳"
             when left(fhost_barcode,3)="050" then "东莞"
    	else "" end as fwarehouse,
 		count(fhost_barcode) as num 
 from dws.dws_xy_yhb_detail 
where to_date(freceive_time)>='2022-01-01'
group by 1,2,3
