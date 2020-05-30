> Azkaban是一个批量工作流任务调度器，用于在一个工作流内以一个特定的顺序运行一组工作和流程。
## job
> 本次项目的任务调度流包括6个job
* import.job
```
type=command
do_date=${dt}
command=/home/zkpk/run/sqoop_import.sh all ${do_date}
```
* ods.job
```
type=command
do_date=${dt}
dependencies=import
command=/home/zkpk/run/ods_db.sh ${do_date}
```
* dwd.job
```
type=command
do_date=${dt}
dependencies=ods
command=/home/zkpk/run/dwd_db.sh ${do_date}
```
* dws.job
```
type=command
do_date=${dt}
dependencies=dwd 
command=/home/zkpk/run/dws_db_wide.sh ${do_date}
```
* ads.job
```
type=command
do_date=${dt}
dependencies=dws
command=/home/zkpk/run/ads_db_gmv.sh ${do_date}
```
* export.job
```
type=command
dependencies=ads
command=/home/zkpk/run/sqoop_export.sh ads_gmv_sum_day
```
## 执行脚本
* sqoop_import.sh
```
#!/bin/bash

db_date=$2
echo $db_date
db_name=gmall

import_data() {
/home/zkpk/app/srv/sqoop/bin/sqoop import \
--connect jdbc:mysql://192.168.233.1:3306/$db_name?useSSL=false \
--username root \
--password 123123 \
--target-dir /origin_data/$db_name/db/$1/$db_date \
--delete-target-dir \
--num-mappers 1 \
--fields-terminated-by "\t" \
--query "$2"' and $CONDITIONS;'
}

import_sku_info(){
  import_data "sku_info" "select 
id, spu_id, price, sku_name, sku_desc, weight, tm_id,
category3_id, create_time
  from sku_info where 1=1"
}

import_user_info(){
  import_data "user_info" "select 
id, name, birthday, gender, email, user_level, 
create_time 
from user_info where 1=1"
}

import_base_category1(){
  import_data "base_category1" "select 
id, name from base_category1 where 1=1"
}

import_base_category2(){
  import_data "base_category2" "select 
id, name, category1_id from base_category2 where 1=1"
}

import_base_category3(){
  import_data "base_category3" "select id, name, category2_id from base_category3 where 1=1"
}

import_order_detail(){
  import_data   "order_detail"   "select 
    od.id, 
    order_id, 
    user_id, 
    sku_id, 
    sku_name, 
    order_price, 
    sku_num, 
    o.create_time  
  from order_info o, order_detail od
  where o.id=od.order_id
  and DATE_FORMAT(create_time,'%Y-%m-%d')='$db_date'"
}

import_payment_info(){
  import_data "payment_info"   "select 
    id,  
    out_trade_no, 
    order_id, 
    user_id, 
    alipay_trade_no, 
    total_amount,  
    subject, 
    payment_type, 
    payment_time 
  from payment_info 
  where DATE_FORMAT(payment_time,'%Y-%m-%d')='$db_date'"
}

import_order_info(){
  import_data   "order_info"   "select 
    id, 
    total_amount, 
    order_status, 
    user_id, 
    payment_way, 
    out_trade_no, 
    create_time, 
    operate_time  
  from order_info 
  where (DATE_FORMAT(create_time,'%Y-%m-%d')='$db_date' or DATE_FORMAT(operate_time,'%Y-%m-%d')='$db_date')"
}

case $1 in
  "base_category1")
     import_base_category1
;;
  "base_category2")
     import_base_category2
;;
  "base_category3")
     import_base_category3
;;
  "order_info")
     import_order_info
;;
  "order_detail")
     import_order_detail
;;
  "sku_info")
     import_sku_info
;;
  "user_info")
     import_user_info
;;
  "payment_info")
     import_payment_info
;;
   "all")
   import_base_category1
   import_base_category2
   import_base_category3
   import_order_info
   import_order_detail
   import_sku_info
   import_user_info
   import_payment_info
;;
esac
```
* ods_db.sh
```
#!/bin/bash

   APP=gmall
   hive=/home/zkpk/app/srv/hive/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
	do_date=$1
else 
	do_date=`date -d "-1 day" +%F`
fi

sql=" 
load data inpath '/origin_data/$APP/db/order_info/$do_date' OVERWRITE into table "$APP".ods_order_info partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/order_detail/$do_date' OVERWRITE into table "$APP".ods_order_detail partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/sku_info/$do_date' OVERWRITE into table "$APP".ods_sku_info partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/user_info/$do_date' OVERWRITE into table "$APP".ods_user_info partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/payment_info/$do_date' OVERWRITE into table "$APP".ods_payment_info partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/base_category1/$do_date' OVERWRITE into table "$APP".ods_base_category1 partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/base_category2/$do_date' OVERWRITE into table "$APP".ods_base_category2 partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/base_category3/$do_date' OVERWRITE into table "$APP".ods_base_category3 partition(dt='$do_date'); 
"
$hive -e "$sql"
```
* dwd_db.sh
```
#!/bin/bash

# 定义变量方便修改
APP=gmall
hive=/opt/bigdata/hive/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
	do_date=$1
else 
	do_date=`date -d "-1 day" +%F`  
fi 

sql="

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.mode.local.auto=true;
insert overwrite table gmall.dwd_order_info partition(dt)
select * from gmall.ods_order_info 
where dt='2020-05-10' and id is not null;
 
insert overwrite table "$APP".dwd_order_detail partition(dt)
select * from "$APP".ods_order_detail 
where dt='$do_date'   and id is not null;

insert overwrite table "$APP".dwd_user_info partition(dt)
select * from "$APP".ods_user_info
where dt='$do_date' and id is not null;
 
insert overwrite table gmall.dwd_payment_info partition(dt)
select * from gmall.ods_payment_info
where dt='2020-05-10' and id is not null;

insert overwrite table "$APP".dwd_sku_info partition(dt)
select  
    sku.id,
    sku.spu_id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.tm_id,
    sku.category3_id,
    c2.id category2_id,
    c1.id category1_id,
    c3.name category3_name,
    c2.name category2_name,
    c1.name category1_name,
    sku.create_time,
    sku.dt
from
    "$APP".ods_sku_info sku
join "$APP".ods_base_category3 c3 on sku.category3_id=c3.id 
    join "$APP".ods_base_category2 c2 on c3.category2_id=c2.id 
    join "$APP".ods_base_category1 c1 on c2.category1_id=c1.id 
where sku.dt='$do_date'  and c2.dt='$do_date'
and c3.dt='$do_date' and c1.dt='$do_date'
and sku.id is not null;
"

$hive -e "$sql"

```
* dws_db_wide.sh
```
#!/bin/bash

# 定义变量方便修改
APP=gmall
hive=/home/zkpk/app/srv/hive/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
	do_date=$1
else 
	do_date=`date -d "-1 day" +%F`  
fi 

sql="
set hive.exec.mode.local.auto=true;
with  
tmp_order as
(
    select 
        user_id, 
        sum(oi.total_amount) order_amount, 
        count(*)  order_count
    from "$APP".dwd_order_info  oi
    where date_format(oi.create_time,'yyyy-MM-dd')='$do_date'
    group by user_id
)  ,
tmp_payment as
(
    select 
        user_id, 
        sum(pi.total_amount) payment_amount, 
        count(*) payment_count 
    from "$APP".dwd_payment_info pi 
    where date_format(pi.payment_time,'yyyy-MM-dd')='$do_date'
    group by user_id
),
tmp_comment as
(  
    select  
        user_id, 
        count(*) comment_count
    from "$APP".dwd_comment_log c
    where date_format(c.dt,'yyyy-MM-dd')='$do_date'
    group by user_id 
)

Insert overwrite table "$APP".dws_user_action partition(dt='$do_date')
select 
    user_actions.user_id, 
    sum(user_actions.order_count), 
    sum(user_actions.order_amount),
    sum(user_actions.payment_count), 
    sum(user_actions.payment_amount),
    sum(user_actions.comment_count) 
from
(
    select
        user_id,
        order_count,
        order_amount,
        0 payment_count,
        0 payment_amount,
        0 comment_count
    from tmp_order

    union all
    select
        user_id,
        0 order_count,
        0 order_amount,
        payment_count,
        payment_amount,
        0 comment_count
    from tmp_payment

    union all
    select
        user_id,
        0 order_count,
        0 order_amount,
        0 payment_count,
        0 payment_amount,
        comment_count 
    from tmp_comment
 ) user_actions
group by user_id;
"

$hive -e "$sql"
```
* ads_db_gmv.sh
```
#!/bin/bash

# 定义变量方便修改
APP=gmall
hive=/home/zkpk/app/srv/hive/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
	do_date=$1
else 
	do_date=`date -d "-1 day" +%F`
fi 

sql="
insert into table "$APP".ads_gmv_sum_day 
select 
    '$do_date' dt,
    sum(order_count)  gmv_count,
    sum(order_amount) gmv_amount,
    sum(payment_amount) payment_amount 
from "$APP".dws_user_action 
where dt ='$do_date'
group by dt;
"

$hive -e "$sql"
```
* sqoop_export.sh
```
#!/bin/bash

db_name=gmall
export_data(){
/home/zkpk/app/srv/sqoop/bin/sqoop export \
--connect "jdbc:mysql://192.168.233.1:3306/${db_name}?useUnicode=true&characterEncoding=utf-8&useSSL=false" \
--username root \
--password 123123 \
--table $1 \
--m 1 \
--export-dir /warehouse/$db_name/ads/$1 \
--input-fields-terminated-by "\t" \
--update-mode allowinsert \
--update-key  "dt" \
--input-null-string  '\\N' \
--input-null-non-string  '\\N' 
}

case $1 in 
  "ads_uv_count")
   export_data "ads_uv_count"
;;
 "ads_user_action_convert_day")
   export_data "ads_user_action_convert_day"
;;
 "ads_gmv_sum_day")
   export_data "ads_gmv_sum_day"
;;
 "all")
   export_data "ads_uv_count" 
   export_data "ads_user_action_convert_day"
   export_data "ads_gmv_sum_day"
;;
esac
```