# ODS层 #
## 创建订单表 ##
		hive (gmall)>
		drop table if exists gmall.ods_order_info;
		create external table gmall.ods_order_info (
 		`id` string COMMENT '订单编号',
 		`total_amount` decimal(10,2) COMMENT '订单金额',
 		`order_status` string COMMENT '订单状态',
 		`user_id` string COMMENT '用户id',
 		`payment_way` string COMMENT '支付方式',
 		`out_trade_no` string COMMENT '支付流水号',
 		`create_time` string COMMENT '创建时间',
 		`operate_time` string COMMENT '操作时间'
		) COMMENT '订单表'
		PARTITIONED BY (`dt` string)
		row format delimited fields terminated by '\t'
		location '/warehouse/gmall/ods/ods_order_info/'
		;
## 创建支付流水表 ##
		hive (gmall)>
		drop table if exists gmall.ods_payment_info;
		create external table gmall.ods_payment_info(
		 `id` bigint COMMENT '编号',
		 `out_trade_no` string COMMENT '对外业务编号',
		 `order_id` string COMMENT '订单编号',
		 `user_id` string COMMENT '用户编号',
		 `alipay_trade_no` string COMMENT '支付宝交易流水编号',
		 `total_amount` decimal(16,2) COMMENT '支付金额',
		 `subject` string COMMENT '交易内容',
		 `payment_type` string COMMENT '支付类型',
		 `payment_time` string COMMENT '支付时间'
		)COMMENT '支付流水表' 
		PARTITIONED BY (`dt` string)
		row format delimited fields terminated by '\t'
		location '/warehouse/gmall/ods/ods_payment_info/'
		;

##  ODS层数据导入脚本 ##
### （1）在/home/hadoop/bin目录下创建脚本ods_db.sh ###
		[hadoop@node1 bin]$ vim ods_db.sh
脚本中填写如下内容
	
	#!/bin/bash
	APP=gmall
   	hive=/opt/bigdata/hive/bin/hive

	# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
	if [ -n "$1" ] ;then
		do_date=$1
	else 
		do_date=`date -d "-1 day" +%F`
	fi

	sql=" 
	load data inpath '/origin_data/$APP/db/order_info/$do_date' OVERWRITE into table "$APP".ods_order_info partition(dt='$do_date');

	load data inpath '/origin_data/$APP/db/payment_info/$do_date' OVERWRITE into table "$APP".ods_payment_info partition(dt='$do_date');
	
	"
	$hive -e "$sql"

### （2）增加脚本执行权限 ###
		[hadoop@node1 bin]$ chmod 777 ods_db.sh
###  （3）执行脚本导入数据###
		[hadoop@node1 bin]$ ./ods_db.sh 2020-04-28
###  （4）执行脚本导入数据查询导入数据###
		hive (gmall)> 
		select * from gmall.ods_order_info where dt='2020-04-28' limit 1;

# DWD层 #
## 创建订单表 ##
		hive (gmall)>
		drop table if exists gmall.dwd_order_info;
		create external table gmall.dwd_order_info (
		 `id` string COMMENT '',
		 `total_amount` decimal(10,2) COMMENT '',
		 `order_status` string COMMENT ' 1 2 3 4 5',
		 `user_id` string COMMENT 'id',
		 `payment_way` string COMMENT '',
		 `out_trade_no` string COMMENT '',
		 `create_time` string COMMENT '',
		 `operate_time` string COMMENT ''
		) 
		PARTITIONED BY (`dt` string)
		stored as parquet 
		location '/warehouse/gmall/dwd/dwd_order_info/' 
		tblproperties ("parquet.compression"="snappy") 
		;
## 创建支付流水表 ##
		hive (gmall)>
		drop table if exists gmall.dwd_payment_info;
		create external table gmall.dwd_payment_info(
		 `id` bigint COMMENT '',
		 `out_trade_no` string COMMENT '',
		 `order_id` string COMMENT '',
		 `user_id` string COMMENT '',
		 `alipay_trade_no` string COMMENT '',
		 `total_amount` decimal(16,2) COMMENT '',
		 `subject` string COMMENT '',
		 `payment_type` string COMMENT '',
		 `payment_time` string COMMENT ''
		)  
		PARTITIONED BY (`dt` string)
		stored as parquet
		location '/warehouse/gmall/dwd/dwd_payment_info/'
		tblproperties ("parquet.compression"="snappy")
		;
## DWD层数据导入脚本 ##

### （1）在/home/hadoop/bin目录下创建脚本dwd_db.sh ###
		[hadoop@node1 bin]$ vim dwd_db.sh
在脚本中填写如下内容

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
		insert overwrite table "$APP".dwd_order_info partition(dt)
		select * from "$APP".ods_order_info 
		where dt='$do_date' and id is not null;
 
		insert overwrite table "$APP".dwd_payment_info partition(dt)
		select * from "$APP".ods_payment_info
		where dt='$do_date' and id is not null;

		"

		$hive -e "$sql"

### （2）增加脚本执行权限 ###
		[hadoop@node1 bin]$ chmod 777 dwd_db.sh
### （3）执行脚本导入数据 ###
		[hadoop@node1 bin]$ ./dwd_db.sh 2020-04-28
### （4）查看导入数据 ###
		hive (gmall)>
		select * from gmall.dwd_sku_info where dt='2020-04-28' limit 2;

# DWS层 #
## 创建用户行为宽表 ##
		hive (gmall)>
		drop table if exists gmall.dws_user_action;
		create external table gmall.dws_user_action 
		(   
		    user_id          string      comment '用户 id',
		    order_count     bigint      comment '下单次数 ',
		    order_amount    decimal(16,2)  comment '下单金额 ',
		    payment_count   bigint      comment '支付次数',
		    payment_amount  decimal(16,2) comment '支付金额 ',
		    comment_count   bigint      comment '评论次数'
		) COMMENT '每日用户行为宽表'
		PARTITIONED BY (`dt` string)
		stored as parquet
		location '/warehouse/gmall/dws/dws_user_action/'
		tblproperties ("parquet.compression"="snappy");
## 向用户行为宽表导入数据 ##
		hive (gmall)>
		with  
		tmp_order as
		(
    			select 
		        user_id, 
				count(*)  order_count,
        			sum(oi.total_amount) order_amount
    			from gmall.dwd_order_info oi
    		where date_format(oi.create_time,'yyyy-MM-dd')='2019-02-10'
    		group by user_id
		) ,
		tmp_payment as
		(
		    select
		        user_id, 
		        sum(pi.total_amount) payment_amount, 
		        count(*) payment_count 
		    from gmall.dwd_payment_info pi 
		    where date_format(pi.payment_time,'yyyy-MM-dd')='2019-02-10'
		    group by user_id
		),
		tmp_comment as
		(
		    select
		        user_id,
		        count(*) comment_count
		    from gmall.dwd_comment_log c
		    where date_format(c.dt,'yyyy-MM-dd')='2019-02-10'
		    group by user_id 
		) 
		
		insert overwrite table gmall.dws_user_action partition(dt='2019-02-10') 
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

## 用户行为数据宽表导入脚本 ##
### （1）在/home/hadoop/bin目录下创建脚本dws_db_wide.sh ###
		[hadoop@node1 bin]$ vim dws_db_wide.sh
在脚本中填写如下内容

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
    			sele	ct 
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

		Insert o	verwrite table "$APP".dws_user_action partition(dt='$do_date')
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

### （2）增加脚本执行权限 ###
		[hadoop@node1 bin]$ chmod 777 dws_db_wide.sh
### （3）执行脚本导入数据 ###
		[hadoop@node1 bin]$ ./dws_db_wide.sh 2020-04-28
### （4）查看导入数据 ###
		hive (gmall)> 
		select * from gmall.dws_user_action where dt='2020-04-28' limit 2;


# ADS层 #
# GMV成交总额 #
## 建表语句 ##
		hive (gmall)>
		drop table if exists gmall.ads_gmv_sum_day;
		create external table gmall.ads_gmv_sum_day(
		 `dt` string COMMENT '统计日期',
		 `gmv_count` bigint COMMENT '当日gmv订单个数',
		 `gmv_amount` decimal(16,2) COMMENT '当日gmv订单总金额',
		 `gmv_payment` decimal(16,2) COMMENT '当日支付金额'
		) COMMENT 'GMV'
		row format delimited fields terminated by '\t'
		location '/warehouse/gmall/ads/ads_gmv_sum_day/'
		;

## 数据导入 ##
		hive (gmall)>
		insert into table gmall.ads_gmv_sum_day
		select 
		'2020-04-28' dt,
    			sum(order_count) gmv_count,
    			sum(order_amount) gmv_amount,
    			sum(payment_amount) payment_amount 
		from gmall.dws_user_action	
		where dt ='2020-04-28'
		group by dt
		;

## 数据导入脚本 ##
### （1)在/home/hadoop/bin目录下创建脚本ads_db_gmv.sh ###
		[hadoop@node1 bin]$ vim ads_db_gmv.sh
在脚本中填写如下内容

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
### （2）增加脚本执行权限 ###
		[hadoop@node1 bin]$ chmod 777 ads_db_gmv.sh
### （3）执行脚本导入数据 ###
		[hadoop@node1 bin]$ ./ads_db_gmv.sh 2020-04-28
###  (4) 查看导入数据 ###
		hive (gmall)> 
		select * from gmall.ads_gmv_sum_day;
