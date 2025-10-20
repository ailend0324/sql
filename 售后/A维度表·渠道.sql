select
    *,
    case 
        when qudao in('vivo商城','华为商城回收','联想商城','荣耀商城','联想管家','官方微博','APP投放','投放H5','微信小程序及其他','CPS中小渠道','闲鱼同城小程序','万科住这儿','美宜佳','支付宝比价回收','OPPO商城','支付宝回收频道') then '自有'
        when qudao in('闲鱼','天猫') then '合作'
        when qudao in('闲鱼寄卖plus(邮寄)','闲鱼帮卖(上门)','闲鱼帮卖(到店)') then '闲鱼寄卖plus'
        when qudao ='闲鱼寄卖V1' then '闲鱼寄卖V1'
        else qudao end qudao_1
from(
    select
        *,
        case
            when channel in ('APP_android','APP_ios','APP投放-抖音APP') then 'APP投放'
            when channel in ('PC','H5','微信小程序','可乐优品商城','估价未下单召回') then '微信小程序及其他'
            when channel in ('CPS中小渠道','分期乐','万科住这儿','美宜佳','捂碳星球数码回收','爱博绿数码回收') then 'CPS中小渠道'
            when channel in ('华为商城回收') then '华为商城回收'
            when channel in ('vivo商城') then 'vivo商城'
            when channel in ('荣耀商城') then '荣耀商城'
            when channel in ('投放H5-抖音','投放H5-搜索引擎','H5投放-召回页面') then '投放H5'
        else channel end  as qudao
    from(
        select
            a.fpid,
            a.fpid_name,
            a.fchannel_id,
            a.fchannel_name,
            CASE
                WHEN a.fchannel_id = 10000943
                    AND fpid IN ( 11030, 11031, 11032, 11033, 11034, 11035 ) THEN
                '投放H5-抖音'
                WHEN a.fchannel_id = 10000943 THEN
                '投放H5-搜索引擎'
                WHEN fpid IN ( 1653, 1663 ) THEN
                'H5投放-召回页面'
                WHEN fpid IN ( 1479 ) THEN
                'APP投放-抖音APP'
                WHEN fpid IN ( 1588 ) THEN
                '估价未下单召回'
                WHEN ( a.fpid IN ( 1494, 1493, 1492, 1475, 1473, 1472, 1382, 1380, 1379, 1368, 1356, 1334, 1331, 1273, 1272, 1182, 1180, 1177, 1105, 1104, 1053, 1012, 1004, 3255 )
                    OR a.fchannel_id IN ( 30000001, 10000246, 10000306) ) THEN
                'H5'
                WHEN ( a.fpid IN ( 11007, 1367, 1355, 1330, 1183, 1181, 1178, 1147, 1146, 1117, 1042, 1001 )
                    OR a.fchannel_id IN ( 40000001 ) ) THEN
                'PC'
                WHEN a.fpid IN ( 1587, 1588, 1439, 1481, 1482 ) THEN
                '可乐优品商城'
                WHEN a.fchannel_id IN ( 10000211, 10000060, 10000333 )
                    AND a.fpid NOT IN ( 1260, 1176 ) THEN
                'APP_android'
                WHEN a.fpid IN ( 1260, 1176 ) THEN
                'APP_ios'
                WHEN a.fchannel_id IN ( 10000012, 10000001 ) THEN
                '微信小程序'
                WHEN a.fchannel_id IN ( 10001039, 10001041, 10000016, 10000021, 10000035, 10000036, 10000063, 10000099, 10000100, 10000102, 10000130, 10000144, 10000171, 10000174, 10000212, 10000245, 10000248, 10000249, 10000201, 10000250, 10000265, 10000302, 10000303, 10000332, 10000334, 10000336, 10000134, 10000426, 10000349, 10000340, 10000311, 10000313, 10000350, 10000352, 10000380, 10000366, 10000374, 10000375, 10000377, 10000378, 10000381, 10000382, 10000383, 10000384, 10000385, 10000388, 10000387, 10000389, 10000390, 10000368, 10000369, 10000370, 10000371, 10000372, 10000373, 10000391, 10000376, 10000392, 10000393, 10000379, 10000380, 10000395, 10000396, 10000397, 10000398, 10000399, 10000400, 10000401, 10000402, 10000403, 10000404, 10000405, 10000406, 10000407, 10000408, 10000409, 10000412, 10000413, 10000414, 10000415, 10000416, 10000417, 10000418, 10000419, 10000420, 10000421, 10000422, 10000423, 10000424, 10000425, 10000338, 10000113 ) THEN
                'CPS中小渠道'
                WHEN a.fchannel_id IN ( 10000121 ) THEN
                'vivo商城'
                WHEN a.fchannel_id IN ( 10000137 ) THEN
                '分期乐'
                WHEN a.fchannel_id IN (10000056, 10001054 ) THEN
                '官方微博'
                WHEN a.fchannel_id IN (10001040 ) THEN
                '联想商城'
                WHEN a.fchannel_id IN (10001070 ) THEN
                '联想管家'
                WHEN a.fchannel_id IN (10000266) THEN
                '闲鱼同城小程序'
				WHEN a.fchannel_id IN (10001186) THEN
				'万科住这儿'
				WHEN a.fchannel_id IN (10001188) THEN
				'美宜佳'
                WHEN a.fchannel_id IN (10001206) THEN
                '捂碳星球数码回收'
				WHEN a.fchannel_id IN (10001196) THEN
				'太力优服'
				WHEN a.fchannel_id IN (10001272) THEN
				'支付宝回收频道'
				WHEN a.fchannel_id IN (10001260) THEN
				'爱博绿数码回收'
				
                
				when a.fchannel_name = '支付宝比价回收' then '支付宝比价回收'
				when a.fchannel_name = 'OPPO商城' then 'OPPO商城'
                when a.fchannel_name = '华为商城回收' then '华为商城回收'
                when a.fchannel_name = '荣耀商城' then '荣耀商城'
                when a.fchannel_name = '支付宝小程序' then '支付宝小程序'
                
                when a.fchannel_name = '闲鱼寄卖（闲鱼回收入口）' then '闲鱼寄卖V1'
                when a.fchannel_name = '闲鱼寄卖plus' then '闲鱼寄卖plus(邮寄)'
                when a.fchannel_name = '闲鱼帮卖（上门）' then '闲鱼帮卖(上门)'
                when a.fchannel_name = '闲鱼帮卖（到店）' then '闲鱼帮卖(到店)'
                when a.fchannel_name = '竞拍销售默认渠道号' then '竞拍售后'
                when a.fchannel_name = '小豹帮卖' then '小豹帮卖'
                when a.fchannel_name = '默认采购渠道' then '采购'
                when a.fchannel_name = 'B端寄卖平台渠道' then 'B端帮卖'
                when a.fchannel_name = '淘宝店铺回收' then '闲鱼'
                when a.fproject_name = '合作项目' and a.fchannel_name like '%闲鱼%' then '闲鱼'
                when a.fproject_name = '合作项目' and a.fchannel_name like '%天猫%' then '天猫'
            else '小站' end as channel
        from drt.drt_my33310_pub_server_channel_center_db_t_pid_info a
    ) t
) t
