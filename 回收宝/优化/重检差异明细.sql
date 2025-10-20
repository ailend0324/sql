with gongdan as (
select 
    t.*,
    b.fclass_name
from (
select 
    to_date(from_unixtime(a.fadd_time)) as fadd_time,
    d.fcontent,
    a.fduty_content,
    a.fbarcode_sn,
    case when c.freal_name in ('黄奕锋','周晓薇','郑春玲','梁椿灏','何嫣红','林宁','徐小利','朱小露','苍雅婷','叶志','林晓雪') then '议价组' else '前端咨询' end as fgroup,
    row_number()over(partition by a.fbarcode_sn order by a.fadd_time desc) as num
from drt.drt_my33310_csrdb_t_works as a
left join drt.drt_my33310_csrdb_t_works_config_appeal as d on a.fappeal_type2=d.fid
left join drt.drt_my33310_amcdb_t_user as c on a.fadd_user=c.fusername
where d.fcontent in ('找机重检','提供照片')
and to_date(from_unixtime(a.fadd_time))>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and a.forder_system=2
and a.fduty_content!="无效工单")t
left join dws.dws_hs_order_detail as b on t.fbarcode_sn=b.fseries_number
where num=1
--and fbarcode_sn='XY0101240307001056'
),
chongjian as (
select 
    a.fend_time,
    a.fserial_number,
    a.fproduct_name,
    a.fissue_name,
    a.fanswer_name,
    b.fanswer_name as fchongjian_answer_name,
    b.fbrand_name,
    case when a.fissue_name in ('保修情况',
                            '网络类型',
                            '机身内存',
                            'iCloud账号',
                            'iCloud账号:',
                            '存储容量',
                            '购买渠道',
                            '颜色',
                            '是否全新',
                            '制式',
                            '拆封情况',
                            '开机情况',
                            '开机情况:',
                            '型号',
                            '保修期') then '模块一'
           when a.fissue_name in ('有线充电','有线充电:') and b.fbrand_name='苹果' then '模块一'
           when a.fissue_name in ('Y3声音',
                                'HOME键',
                                'Y3光线感应',
                                '无线功能（WIFI/蓝牙）',
                                '无线功能:',
                                '扬声器',
                                'Y3麦克风',
                                'Y3振动',
                                '振动',
                                '振动:',
                                'SIM 卡2',
                                '声音功能（麦克风/扬声器/听筒）',
                                '声音功能（麦克风/扬声器/听筒）:',
                                'Y3蓝牙',
                                '听筒',
                                '距离感应',
                                '重力感应',
                                '通话功能',
                                '通话功能:',
                                'Y3通信功能',
                                '静音键',
                                'Y3NFC',
                                'Y3Wi-Fi',
                                '侧键功能',
                                '侧键功能:',
                                'Y3原彩功能',
                                'Y3陀螺仪',
                                '屏幕传感器功能（光线/距离感应）',
                                '屏幕传感器功能（光线/距离感应）:',
                                '触摸',
                                'Face ID',
                                'Face ID:',
                                '闪光灯功能',
                                '蓝牙功能',
                                'Y3按键功能',
                                'Y3触摸功能',
                                '面部识别',
                                'Y3充电功能',
                                '触屏功能',
                                '触屏功能:',
                                'SIM 卡1',
                                '音量增键',
                                'NFC功能',
                                'NFC功能:',
                                '音量减键',
                                '指南针',
                                '指南针:',
                                'WIFI功能',
                                '底部麦克风',
                                'Y3距离感应',
                                '电源键',
                                '副屏-触摸功能',
                                '副屏-触摸功能:') then '模块二'
                  when a.fissue_name in ('有线充电','有线充电:') and b.fbrand_name!='苹果' then '模块二'
                  when a.fissue_name in ('面容识别','面容识别:','Y3面容功能') and b.fbrand_name='苹果' then '模块二'
                  when a.fissue_name in ('指纹识别','指纹解锁','指纹解锁:','Y3指纹功能') and b.fbrand_name='苹果' then '模块二'
                  when a.fissue_name in ('外壳印渍',
                                '外壳划痕',
                                '其他显示问题',
                                '显示气泡',
                                '壳内掉漆',
                                '外壳缝隙',
                                '显示漏液',
                                'Y3屏幕显示',
                                '外壳磕碰',
                                '后摄像头外观',
                                '闪光灯外观',
                                '内屏掉漆',
                                '正面麦克风',
                                '前摄像头外观',
                                '屏幕显示',
                                '屏幕显示:',
                                '显示图像/文字印痕',
                                '光线感应',
                                '其他按键',
                                '屏幕外观',
                                '屏幕外观:',
                                '屏幕外观碎裂',
                                '指纹按键外观',
                                '屏幕外观/其他',
                                '边框背板',
                                '边框背板:',
                                '外壳破损',
                                '显示进灰',
                                '显示老化/色差',
                                'Y3屏下异物',
                                '显示色斑/压伤',
                                'Y3弯曲情况',
                                '机身弯曲',
                                '机身弯曲:',
                                '屏幕外观划痕',
                                '显示亮点/坏点',
                                '外壳弯曲变形',
                                'Y3机身外观',
                                '其他外壳外观',
                                '外壳脱胶',
                                '外壳掉漆',
                                'Y3外屏损伤',
                                '副屏-屏幕外观',
                                '副屏-屏幕外观:',
                                '折叠屏保护膜情况',
                                '折叠屏保护膜情况:',
                                'Y3折叠屏保护膜',
                                '转轴状况',
                                '转轴状况:',
                                '副屏-屏幕显示',
                                '副屏-屏幕显示:') then '模块三'
                       when a.fissue_name in ('主板拆修情况',
                                '后置摄像头',
                                '后置摄像头:',
                                '无线充电',
                                '后壳维修',
                                '后壳维修:',
                                '账号',
                                '账号:',
                                '电池更换情况',
                                '电池维修情况:',
                                '数据接口',
                                '进水/受潮',
                                '进水/受潮:',
                                '软件检测',
                                'Y3账号',
                                '后摄像头维修情况',
                                '后摄像头维修情况:',
                                'Y3电池健康度',
                                '屏幕拆修情况',
                                '电池信息情况',
                                '拆修痕迹',
                                'Y3无线充电功能',
                                '无线充电功能:',
                                'Y3前置摄像头维修',
                                '耳机接口',
                                '售后案例情况',
                                '售后案例情况:',
                                '前置摄像头',
                                '前置摄像头:',
                                'Y3基带功能',
                                'Y3屏幕维修',
                                'Y3配件情况',
                                'Y3前置摄像头',
                                'ID 锁',
                                'Y3后置摄像头',
                                'Y3尾插',
                                '浸液痕迹',
                                'Y3售后案例',
                                'Y3系统使用',
                                '摄像头维修情况',
                                'Y3后置摄像头维修',
                                '机身拆修情况',
                                '尾插螺丝',
                                '音频网罩',
                                'USB联机',
                                'USB联机:',
                                '前置摄像头功能',
                                '屏幕维修情况',
                                '屏幕维修情况:',
                                '后置摄像头功能',
                                '前摄像头维修情况',
                                '前摄像头维修情况:',
                                'Y3零件维修情况',
                                'Y3进水情况',
                                'Y3主板维修',
                                'Y3定位功能',
                                '卡托',
                                '组件拆修情况',
                                '系统情况',
                                'Y3机身维修',
                                '主板维修情况',
                                '主板维修情况:',
                                '电池健康度',
                                '电池健康度:',
                                '是否可恢复出厂设置',
                                '是否可恢复出厂设置:',
                                '其他零部件情况',
                                '其他零部件情况:',
                                '副屏-维修情况',
                                '副屏-维修情况:',
                                '虹膜识别',
                                'Y3其他功能') then '模块四' 
                    when a.fissue_name in ('面容识别','面容识别:','Y3面容功能') and b.fbrand_name!='苹果' then '模块四' 
                    when a.fissue_name in ('指纹识别','指纹解锁','指纹解锁:','Y3指纹功能') and b.fbrand_name!='苹果' then '模块四' else null end as module_type,
        a.fdetect_price,
        b.fchongjian_price,
  		a.fengineer_name
from (
select 
    a.*,
    b.fserial_number,
    b.fproduct_name,
    b.fend_time,
  	b.fengineer_name,
    b.fdetect_price/100 as fdetect_price,
    row_number()over(partition by b.fserial_number,a.fissue_name order by a.fdetect_record_id desc) as num
from dwd.dwd_detect_back_detection_issue_and_answer_v2 as a
left join dwd.dwd_detect_back_detect_detail as b on a.fdetect_record_id=b.fdetect_record_id
where left(b.fserial_number,2) not in ('BB','NT','YZ','JM','BG','XZ')
and a.field_source='fdet_norm_snapshot'
and a.ds>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
--and b.fclass_name="手机"
and b.freal_name not in ('林杰俊','杨泽文','姜宜良','蒋宜良')
and b.fdet_type=0
) as a
left join (
select 
    a.*,
    b.fserial_number,
    b.fbrand_name,
    b.fdetect_price/100 as fchongjian_price,
    row_number()over (partition by b.fserial_number,a.fissue_name order by a.fdetect_record_id desc) as num
from dwd.dwd_detect_back_detection_issue_and_answer_v2 as a
left join dwd.dwd_detect_back_detect_detail as b on a.fdetect_record_id=b.fdetect_record_id
where left(b.fserial_number,2) not in ('BB','NT','YZ','JM','BG','XZ')
and a.field_source='fdet_norm_snapshot'
and a.ds>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
--and b.fclass_name="手机"
and b.fdet_type=2
) as b on a.fserial_number=b.fserial_number and a.fissue_name=b.fissue_name
where a.num=1 
and b.num=1
and b.fserial_number is not null
and a.fanswer_name<>b.fanswer_name
--and b.fserial_number='XY0101240227001645'
),
detect_one as (
select 
    fserial_number,
    freal_name as fdetect_one_name,
    from_unixtime(fend_det_time) as fdetect_one_time
from (
select 
    a.fserial_number,
  	a.fend_det_time,
    b.freal_name,
    row_number()over(partition by upper(a.fserial_number) order by a.fend_det_time desc) as num
from drt.drt_my33312_detection_t_automation_det_record as a
left join drt.drt_my33310_amcdb_t_user as b on a.fuser_name=b.fusername
where fserial_number!=""
and fserial_number is not null
and to_date(from_unixtime(a.fend_det_time))>=to_date(date_sub(from_unixtime(unix_timestamp()),180)))t
where num=1
),
detect_two as (
select 
    fserial_number,
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
where to_date(a.fcreate_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
and fserial_number!=""
and fserial_number is not null)t
where num=1
),
detect_three as (
select 
    fserial_number,
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
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
    and b.fdet_sop_task_name like '%外观%')t
where num=1
),
detect_four as (
select 
    fserial_number,
    freal_name as fdetect_four_name,
    fcreate_time as fdetect_four_time
from (
    select 
        a.fcreate_time,
        a.fserial_number,
        b.freal_name,
        row_number()over(partition by upper(a.fserial_number) order by a.fcreate_time desc) as num
    from drt.drt_my33312_detection_t_det_task as a
    left join drt.drt_my33312_detection_t_det_task_record as b on a.ftask_id=b.ftask_id
    where to_date(a.fend_time)>=to_date(date_sub(from_unixtime(unix_timestamp()),180))
    and b.fdet_sop_task_name like '%拆修%')t
where num=1
)
select 
    a.*,
    case when right(left(a.fbarcode_sn,6),2)='16' then '杭州'
    	 when right(left(a.fbarcode_sn,6),2)='01' then '深圳'
    else null end as fwarehouse,
    b.*,
    c.fdetect_one_name,
    d.fdetect_two_name,
    e.fdetect_three_name,
    f.fdetect_four_name
from gongdan as a
left join chongjian as b on a.fbarcode_sn=b.fserial_number
left join detect_one as c on a.fbarcode_sn=c.fserial_number
left join detect_two as d on a.fbarcode_sn=d.fserial_number
left join detect_three as e on a.fbarcode_sn=e.fserial_number
left join detect_four as f on a.fbarcode_sn=f.fserial_number
