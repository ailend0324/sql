select *,case when right(left(fseries_number,6),4)="0112" then "东莞仓" 
    	 when right(left(fseries_number,6),2)="16" then "杭州仓"
    else "深圳仓" end as fwms_type,
    case when left(fseries_number,2)='TL' or (left(fseries_number,2)='CG' and funpack_time>='2024-12-01') then "太力" else ftype end as "业务",
    case when fseries_number like "%\_%" then "配件" else "成品" end as fproduct_type
    from dws.dws_instock_details
where funpack_time>=to_date(date_sub(from_unixtime(unix_timestamp()),200))
and funpack_user not in("于炉烨","张晓梦","徐晶")
and left(fseries_number,2) not like "%YZ%"
and left(fseries_number,2) not like "%NT%"
