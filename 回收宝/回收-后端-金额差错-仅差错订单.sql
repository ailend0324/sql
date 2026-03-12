SELECT 
fdetect_date,
fpid,
fpid_name,
fchannel_id,
fchannel_name,
fuser_group_id,
fuser_group_name,
fproject_id,
fproject_name,
fclass_id,
fclass_name,
fwarehouse_code,
fdet_tpl,
Freal_name,
Fengineer_name,
sum(fdif_price) as fdif_price,
sum(fdif_absolute_price) as  fdif_absolute_price,
sum(fdif_increase_price) as fdif_increase_price,
sum(fdif_decrease_price) as fdif_decrease_price,
sum(Fdetect_qc_price) as Fdetect_qc_price,
sum(Fdetect_price) as Fdetect_price
FROM 
dws_detect_record_qc_dif
WHERE 
fdetect_qc_dif_qty>0
and fdetect_date>=to_date(date_sub(from_unixtime(unix_timestamp()),720))
GROUP BY fdetect_date,
fpid,
fpid_name,
fchannel_id,
fchannel_name,
fuser_group_id,
fuser_group_name,
fproject_id,
fproject_name,
fclass_id,
fclass_name,
fwarehouse_code,
fdet_tpl,
Freal_name,
Fengineer_name
