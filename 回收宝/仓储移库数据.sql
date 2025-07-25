select 
    *,
    left(fserial_no,2) as fchannel
from dwd.dwd_t_pm_wms_stock_notify 
where to_date(fcreate_time)>='2024-01-01'
and fcmd='move'
