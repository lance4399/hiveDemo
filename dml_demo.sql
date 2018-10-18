
--################################################ Hive or Impala   #######################################################################################
1.每日base收益预---   CONCAT(ROUND(a/b * 100, 2),'','%')   
1) 流量占比
select * from 
	(select 
		user_name,
		sum(amount) as base_amount,
		sum(pc_pv) as pc_pv,
		sum(wap_pv) as wap_pv,
		sum(app_pv) as app_pv,
		sum(pc_pv)/sum(pv) as pc_pv_ratio,
		sum(wap_pv)/sum(pv) as wap_pv_ratio ,
		sum(app_pv)/sum(pv) as app_pv_ratio,
		dt,
		level,
		mp_channel_id_desc,
		passport,
		home_page 
		from ads_pass_d where dt=${dt} group by user_name,dt,passport,level,mp_channel_id_desc,home_page 
	) tmp 
where base_amount>=300 order by base_amount desc ;

-------------------------------------------------------------------

2)差值报警：
select * from (
select a.user_name,a.base_amount as t_amount,b.base_amount as y_amount,abs(a.base_amount-b.base_amount) as amount_sub,abs(a.pv-b.pv) as pv_sub,
abs(a.pc_pv-b.pc_pv) as pv_pc_sub,abs(a.wap_pv-b.wap_pv) as pv_wap_pv,abs(a.app_pv-b.app_pv) as pv_app_sub,a.dt, a.level,a.mp_channel_id_desc,a.passport,a.home_page 
from 
	(select user_name,passport,level,mp_channel_id_desc,home_page,sum(amount) as base_amount,sum(pv) as pv,sum(pc_pv) as pc_pv, sum(wap_pv) as wap_pv,sum(app_pv) as app_pv,dt
		from ads_pass_d where dt=${dt} group by dt,user_name,passport,level,mp_channel_id_desc,home_page 
	)a 
	join 
	(select user_name,passport,level,mp_channel_id_desc,home_page,sum(amount) as base_amount,sum(pv) as pv,sum(pc_pv) as pc_pv, sum(wap_pv) as wap_pv,sum(app_pv) as app_pv
		from ads_pass_d where dt=${dt} group by user_name,passport,level,mp_channel_id_desc,home_page
	)b 
	on a.passport = b.passport 
) u where amount_sub>=250 ;



--#######################################################################################################################################
2.广告计划日报 ：
1)七日大盘数据： 

select t.dt,total_amount, a_amount, b_amount, c_amount, d_amount, top10_amount, a_amount_ratio, b_amount_ratio, c_amount_ratio,
	d_amount_ratio, (top10_amount/total_amount) as top10_amount_ratio from 
(
	select dt ,sum(amount) as total_amount, sum(if(fc_level='a',amount,0)) as a_amount, sum(if(fc_level='b',amount,0)) as b_amount,
		sum(if(fc_level='c',amount,0)) as c_amount, sum(if(fc_level='d',amount,0)) as d_amount, 
		(sum(if(fc_level='a',amount,0))/sum(amount)) as a_amount_ratio,
		(sum(if(fc_level='b',amount,0))/sum(amount)) as b_amount_ratio,
		(sum(if(fc_level='c',amount,0))/sum(amount)) as c_amount_ratio, 
		(sum(if(fc_level='d',amount,0))/sum(amount)) as d_amount_ratio
	from ads_pass_d where dt>=dateOpts(${dt},-7) and dt<=${dt} group by dt
) t
join 
(
	select dt,sum(amount) as top10_amount from 
		(select row_number() over (partition by dt order by amount desc) as rank,amount,dt from 
			(select user_id,sum(amount) as amount,dt from 
				ads_pass_d where dt>=dateOpts(${dt},-7) and dt<=${dt} group by user_id,dt
			) tmp 
			where amount>0
		) a 
		where rank<=10 group by dt order by dt desc ;
) b 
on t.dt=b.dt;

-------------------------------------------------------------------
2) 今日审核情况：
select d_apply_num,d_to_check,d_apply_pass,d_quit,d_ilegal,total_d_profit,d_profit_num,penalty_num,total_d_penalty,
 total_d_profit-total_d_penalty from 
 (SELECT SUM(if(if_apply=1,1,0)) as d_apply_num, 
  	SUM(IF(if_noaudit=1,1,0) ) as d_to_check,
  	SUM(IF(if_access=1,1,0) ) as d_apply_pass,
  	SUM(IF(if_quit=1,1,0) ) as d_quit,  
 	SUM(IF(if_punish=1,1,0) ) as d_ilegal,'a' as key
   FROM ads_user_stat_d where dt=${dt}
 ) as t1
 join
 (select sum(amount) as total_d_profit, count(distinct user_id) as d_profit_num,'a' as key 
 	from ads_pass_d where dt={dt} and amount>0
 ) as t2 
 on t1.key=t2.key 
 left outer join 
 (select if(count(distinct user_id) is null,0,count(distinct user_id) ) as penalty_num,if(sum(amount) is null,0,sum(amount)) as total_d_penalty,'a' as key 
 	from ads_punish_d where dt={dt}
 ) as t3 
 on t1.key=t3.key;

------------------------------------------------------------------------------------------------------------------------------------------------------
3)用户覆盖范围  

---------------------------DDL-----------------------------
insert overwrite table rpt_user_coverage_d partition(dt=${dt})
select 
if(audit_status is null,'头部',audit_status) as audit_status,
count(distinct user_id) as total_user_num,
count( if(user_active_state='yes',user_id,null) )/( cast(${dt} as bigint)-cast(concat(substring(${dt},1,6),'01') as bigint)+1 )  as ad_active_num, 
sum(pv)/( cast(${dt} as bigint)-cast(concat(substring(${dt},1,6),'01') as bigint) +1 ) as average_pv,
sum(a_pv)/( cast(${dt} as bigint)-cast(concat(substring(${dt},1,6),'01') as bigint) +1 ) as average_a_pv,
sum(b_pv)/( cast(${dt} as bigint)-cast(concat(substring(${dt},1,6),'01') as bigint) +1 ) as average_b_pv,
sum(c_pv)/( cast(${dt} as bigint)-cast(concat(substring(${dt},1,6),'01') as bigint) +1 ) as average_c_pv,
sum(d_pv)/( cast(${dt} as bigint)-cast(concat(substring(${dt},1,6),'01') as bigint) +1 ) as average_d_pv,
sum(published_paper_num)/( cast(${dt} as bigint)-cast(concat(substring(${dt},1,6),'01') as bigint) +1 ) as average_paper_num, 
sum(a_published_paper_num)/( cast(${dt} as bigint)-cast(concat(substring(${dt},1,6),'01') as bigint)+1 ) as average_a_paper_num,
sum(b_published_paper_num)/( cast(${dt} as bigint)-cast(concat(substring(${dt},1,6),'01') as bigint) +1 ) as average_b_paper_num,
sum(c_published_paper_num)/( cast(${dt} as bigint)-cast(concat(substring(${dt},1,6),'01') as bigint) +1 ) as average_c_paper_num,
sum(d_published_paper_num)/( cast(${dt} as bigint)-cast(concat(substring(${dt},1,6),'01') as bigint) +1 ) as average_d_paper_num
from 
(
select user_id,user_active_state,audit_status,level,dt,
sum(pv) as pv,
sum(if(fc_level='a',pv,0)) as a_pv,
sum(if(fc_level='b',pv,0)) as b_pv,
sum(if(fc_level='c',pv,0)) as c_pv,
sum(if(fc_level='d',pv,0)) as d_pv,
sum(published_paper_num) as published_paper_num,
sum(if(fc_level='a',published_paper_num,0)) as a_published_paper_num,
sum(if(fc_level='b',published_paper_num,0)) as b_published_paper_num,
sum(if(fc_level='c',published_paper_num,0)) as c_published_paper_num,
sum(if(fc_level='d',published_paper_num,0)) as d_published_paper_num 
from ads_shmm_fc_base_global_d where dt>=concat(substring(${dt},1,6),'01') and dt<=${dt}
and (level>=1 and level<=2 and audit_status is null or audit_status is not null) 
group by user_id,user_active_state,audit_status,level,dt
) tmp group by audit_status;

-------------- DML -----------------------
select case audit_status 
	when '1' then '本月未申请用户'
	when '2' then '本月待审核用户'
	when '3' then '本月通过用户'
	when '4' then '本月驳回用户'
	when '5' then '本月退出用户'
	when '7' then '本月违规退出用户'
	when '头部' then '本月无资格用户（1/2级)'
	end as audit_status,
	total_user_num, ad_active_num,average_pv,average_a_pv,average_b_pv,average_c_pv,average_d_pv,
	average_paper_num,average_a_paper_num,average_b_paper_num,average_c_paper_num,average_d_paper_num 
 from rpt_user_coverage_d where dt='20181014' 
 group by audit_status,total_user_num, ad_active_num,average_pv,average_a_pv,average_b_pv,average_c_pv,average_d_pv,
	average_paper_num,average_a_paper_num,average_b_paper_num,average_c_paper_num,average_d_paper_num;

------------------------------------------------------------------------------------------------------------------------------------------------------
4)【周报】累计通过用户的活跃占比单日趋势:
select * from(
	select a.dt,active_number,pass_number,(active_number/pass_number) as active_ratio from 
		(select dt,count(distinct user_id) as active_number   
			from ads_global_d where dt>='20181011' and dt <='20181015' and  audit_status ='3' and user_active_state ='yes' group by dt 
		) a	
		join  
		(select dt,count(distinct user_id) as pass_number   
			from ads_global_d where dt>='20181011' and dt <='20181015' and  audit_status ='3' group by dt 
		) b 
		on a.dt=b.dt
) tmp order by dt desc ;



4.	【周报】大盘流量  ----udf
select level,
audit_status,
count(user_id) as media_num, 
sum(if(published_paper_num>0,1,0)) as active_media_num,
sum(published_paper_num) as content_num,
sum(if(fc_level='a',published_paper_num,0)) as a_published_num,
sum(if(fc_level='b',published_paper_num,0)) as b_published_num,
sum(if(fc_level='c',published_paper_num,0)) as c_published_num,
sum(if(fc_level='d',published_paper_num,0)) as d_published_num,
sum(if(WorkDayFun(dt)=1,published_paper_num,0)) as workday_paper_total_num, 
sum(if(WorkDayFun(dt)=1 and fc_level='a',published_paper_num,0)) as workday_paper_a_num,
sum(if(WorkDayFun(dt)=1 and fc_level='b',published_paper_num,0)) as workday_paper_b_num,
sum(if(WorkDayFun(dt)=1 and fc_level='c',published_paper_num,0)) as workday_paper_c_num,
sum(if(WorkDayFun(dt)=1 and fc_level='d',published_paper_num,0)) as workday_paper_d_num,
sum(if(WorkDayFun(dt)=0,published_paper_num,0)) as week_flagend_paper_total_num,
sum(if(WorkDayFun(dt)=0 and fc_level='a',published_paper_num,0)) as week_flagend_paper_a_num,
sum(if(WorkDayFun(dt)=0 and fc_level='b',published_paper_num,0)) as week_flagend_paper_b_num,
sum(if(WorkDayFun(dt)=0 and fc_level='c',published_paper_num,0)) as week_flagend_paper_c_num,
sum(if(WorkDayFun(dt)=0 and fc_level='d',published_paper_num,0)) as week_flagend_paper_d_num,
sum(if(fc_level='a',pv,0)) as a_pv, 
sum(if(fc_level='b',pv,0)) as b_pv, 
sum(if(fc_level='c',pv,0)) as c_pv, 
sum(if(fc_level='d',pv,0)) as d_pv
from ads_qualifier_d where dt>=dateOpts(${dt},-7) and dt<=${dt} group by level,audit_status;




-----------------###########################################################################################################################################
5.	分成通过用户日活变动   --dt>=dateOpts(${dt},-7) and dt <=${dt}

select * from(
	select a.dt,active_number,(active_number/pass_number) as active_ratio from 
		(select dt,count(distinct user_id) as active_number   
			from ads_global_d where dt>='20181011' and dt <='20181015' and  audit_status ='3' and user_active_state ='yes' group by dt 
		) a	
		join  
		(select dt,count(distinct user_id) as pass_number   
			from ads_global_d where dt>='20181011' and dt <='20181015' and  audit_status ='3' group by dt 
		) b 
		on a.dt=b.dt
) tmp order by dt desc ;



-----------------###########################################################################################################################################
6.	上周大盘发文对比

-----------------------------new version  DDL for case a and b -----------------------------
create external table rpt_publishnum_comparison_d(
dapan_paper_num bigint comment '大盘用户发文数',
self_dapan_paper_num bigint comment '自主用户发文数',
self_head_paper_num bigint comment '自主头部发文数',
average_dapan_paper_num float comment '大盘用户人均发文数',
average_self_dapan_paper_num float comment '自主人均发文数',
average_self_head_paper_num float comment '自主头部人均发文数',
average_qualified_paper_num float comment '资格用户人均发文数',
average_pass_paper_num float comment '通过用户人均发文数',
average_ilegal_drop_paper_num float comment '违规退出用户人均发文数',
average_non_qualified_paper_num float comment '非资格用户人均发文数'
)
partitioned by (
  dt string  comment '时间分区'
)
stored as parquet
LOCATION 'hdfs://dc1/hive/warehouse/tables/rpt_publishnum_comparison_d';


insert overwrite table rpt_publishnum_comparison_d partition(dt=${dt})
select dapan_paper_num,self_dapan_paper_num,self_head_paper_num,average_dapan_paper_num,average_self_dapan_paper_num,average_self_head_paper_num,
average_qualified_paper_num,average_pass_paper_num,average_ilegal_drop_paper_num,average_non_qualified_paper_num from 
(
select sum(published_paper_num) as dapan_paper_num,
sum(if(length(mobile)>5,published_paper_num,0)) as self_dapan_paper_num,
sum(if(length(mobile)>5 and level >=1 and level<=2,published_paper_num,0)) as self_head_paper_num ,
sum(published_paper_num)/count(distinct user_id) as average_dapan_paper_num,
sum(if(length(mobile)>5,published_paper_num,0))/count(distinct if(length(mobile)>5,user_id,null)) as average_self_dapan_paper_num,'a' as key,
sum(if(length(mobile)>5 and level >=1 and level<=2,published_paper_num,0))/count(distinct if(length(mobile)>5 and level >=1 and level<=2,user_id,null)) as average_self_head_paper_num,
sum(if(audit_status is null,published_paper_num,0))/count(distinct if(audit_status is null,user_id,null)) as average_non_qualified_paper_num 
from ads_global_d where dt>=dateOpts(${dt},-7) and dt<=${dt}
) t1
join 
(
select sum(published_paper_num)/count(distinct user_id) as average_qualified_paper_num,
sum(if(audit_status=3,published_paper_num,0))/count(distinct if(audit_status=3,user_id,null)) as average_pass_paper_num,'a' as key,
if(count(distinct if(audit_status=7,user_id,null))=0,0,sum(if(audit_status=7,published_paper_num,0))/count(distinct if(audit_status=7,user_id,null)) ) as average_ilegal_drop_paper_num 
from ads_qualifier_d where dt>=dateOpts(${dt},-7) and dt<=${dt}
) t2
on t1.key=t2.key;

a)	大盘发文情况:
-----------------------------new version  DML -----------------------------
select '本周' as week_flag,
	dapan_paper_num,
	self_dapan_paper_num,
	self_head_paper_num 
	from rpt_publishnum_comparison_d
	where dt=${dt}
union all
select '上周' as week_flag,
	dapan_paper_num,
	self_dapan_paper_num,
	self_head_paper_num 
	from rpt_publishnum_comparison_d
	where dt=dateOpts(${dt},-7);

###########################################################################################################################################
b)	人均发文情况:
-------------new DML --------------
select '本周' as week_flag,
	average_dapan_paper_num ,
	average_self_dapan_paper_num,
	average_self_head_paper_num,
	average_qualified_paper_num,
	average_pass_paper_num,
	average_ilegal_drop_paper_num,
	average_non_qualified_paper_num
from rpt_publishnum_comparison_d where dt=${dt}
union all
select '上周' as week_flag,
	average_dapan_paper_num ,
	average_self_dapan_paper_num,
	average_self_head_paper_num,
	average_qualified_paper_num,
	average_pass_paper_num,
	average_ilegal_drop_paper_num,
	average_non_qualified_paper_num
from rpt_publishnum_comparison_d where dt=dateOpts(${dt},-7);


