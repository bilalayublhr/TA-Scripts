
###################### Callgroup Table ##############################

select * from `sky_retention_platform_balerion.callgroups` where iscurrent = 1;
;
set sql_safe_updates = 0;


Select sum(MaxPercentile - MinPercentile) from `sky_retention_platform_balerion.callgroups`;
Select Max(Percentile) , Min(Percentile) , count(1) from `sky_retention_platform_balerion.agentperformance`;

drop temporary table if exists tempdb.x;
create temporary table tempdb.x 
select * from `sky_retention_platform_balerion.callgroups` 
 ;

drop temporary table if exists tempdb.y;
create temporary table tempdb.y 
select * from `sky_retention_platform_balerion.callgroups` 
;

select * from  tempdb.x ;
set @diamondcount = 5;
set @nondiamondcount = 30;
# set @hmcount = 7; // this was for the HM trial

set sql_Safe_updates = 0;
update tempdb.x 
set 
callgroup = concat(callgroup, '|VDNType:Diamond'),
predicate = concat('(VDN like ''*VIP*'' and VDN like ''*Diamond*'') and ', predicate),
FrontNCount = @diamondcount;


update `sky_retention_platform_balerion.callgroups`
set 
callgroup = concat(callgroup, '|VDNType:NonDiamond'),
predicate = concat('(!(VDN like ''*VIP*'' and VDN like ''*Diamond*'')) and ', predicate),
FrontNCount = @nondiamondcount,
predicaterank = PredicateRank + 500
where iscurrent = 1;


drop temporary table if exists tempdb.frontncgs;
create temporary table tempdb.frontncgs
# select * from tempdb.y
# Union
select * from tempdb.x
union
select * from `sky_retention_platform_balerion.callgroups` where iscurrent
;

select substring_index(callgroup, 'VDNType:', -1) vdntype, 
	   substring_index(predicate, ') and', 1) frontnpredicateheader,
       count(1) cgcount,
	   avg(frontncount) frontncount_avg,
       max(length(callgroup)) max_cg_length, predicate, callgroup, avg(PredicateRank)
from tempdb.frontncgs
group by 1, 2;




# update `sky_retention_platform_balerion.callgroups`  set iscurrent = 0;
insert into `sky_retention_platform_balerion.callgroups`  select * from tempdb.x ;
# union select * from tempdb.y;



update `sky_retention_platform_balerion.callgroups`
set predicate = replace(predicate, "VDN like 'VQ_SCT_SALPTF_2_Xfer_PBX_RSIP_0*'",
"VDN like 'VQ_SCT_SALPTF_2_Xfer_PBX_RSIP_0*' or VDN like 'VQ_SCT_SALTRN_HM2_SkyQ_Xfer_PBX_RSIP_0*'")  
where iscurrent = 1 
and CallGroup like '%balerion%' and predicate like '%VQ_SCT_SALPTF_2_Xfer%';





###################### AP Table Table ##############################

select * from `sky_retention_platform_balerion.agentperformance` where iscurrent = 1 ;


####################################################################

set sql_safe_updates = 0;


update `sky_retention_platform_balerion.agentperformance` 
set generatedon = now();
update `sky_retention_platform_balerion.callgroups`
set generatedon = now();

##############################################################################
