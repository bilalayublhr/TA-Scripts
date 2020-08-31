date_vector <- seq(as.Date("2020-06-14"), as.Date("2020-07-31"), by="days")

eval <- "Select '%s' as date, a.callguid, a.ewt/1000 as ewt
                    from  `bskybinternal.vevaluatorlog` a
                    where calltime between '%s' and '%s'
                    and Area =  'Platform Classic'"


ecl <- run_query("Select CallId, callguid, calltime,
                  (case when (32767-substring(QueuedSkillsList, locate(':',QueuedSkillsList)+1, 5)) > 200 then '200+'
				          when (32767-substring(QueuedSkillsList, locate(':',QueuedSkillsList)+1, 5)) between 150 and 200 then '150-200'
                  when (32767-substring(QueuedSkillsList, locate(':',QueuedSkillsList)+1, 5))  between 100 and 150 then '100-150'
                  when (32767-substring(QueuedSkillsList, locate(':',QueuedSkillsList)+1, 5))  between 50 and 100 then '50-100'
					         when (32767-substring(QueuedSkillsList, locate(':',QueuedSkillsList)+1, 5))  between 0 and 50 then '0-50' end) as Priority_Buckets, 
                  reason, successful, evalalgoused, on_off,  timestampdiff(second,calltime,answtime) as asa, 
                  (case when evalalgoused like '%L1%' then 'L1' when evalalgoused like '%L2%' then 'L2' else 'NA' end) as EvalALgo, 
                  (case when reason like '%Call SLA Blown%' then 1 else 0 end) as Call_SLA
                  from  `bskybsatmap`.`bskybinternal.vecl`
                  where calltime >= '2020-06-14'
                  and Area =  'Platform Classic'
                  and successful = 1;", "arch")


loaddata_parr <- function( startdate, query){
  enddate <- startdate + 1 
  query <- sprintf(query, startdate, startdate, enddate)
  return(run_query(query, "arch"))
}


dat <- mclapply(date_vector, loaddata_parr, eval, mc.cores = 20L)
df <- rbindlist(dat)

setDT(ecl)
setDT(df)

ecl_eval <- df[ecl, on = "callguid", nomatch = 0]
ecl_eval_L2_Off <- ecl_eval[on_off == 0 & evalalgo %like% 'L2']
ecl_eval_L2_Off <- ecl_eval_L2_Off %>% select(priority_buckets, ewt, asa)
data <- melt(data = ecl_eval_L2_Off[priority_buckets != "NA"], id.vars = "priority_buckets", measure.vars = c("ewt", "asa"))


## SLA Blown
setDT(ecl)
df2 <- ecl[on_off == 0 & evalalgo %like% "L2", .(SLA_Blown = 100*mean(call_sla)), by = .(date(calltime))]


df1 <- ecl_eval_L2_Off[priority_buckets != "NA", .(Calls = .N), by = .(date,priority_buckets)]
write.xlsx(df2, "df2.xlsx")

ggplot(df1, aes(x = date, y = Calls, fill = priority_buckets))+ geom_bar(stat = "identity", position = "fill")+
ggplot()+ geom_line(data = df2, aes(x = date, y = SLA_Blown))

ggplot(data,aes(x=value, color = as.factor(variable)))+ geom_density()+ facet_grid(.~factor(priority_buckets,levels=c("0-50","50-100","100-150", "150-200", "200+")))


