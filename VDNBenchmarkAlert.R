library(easy.db.query)
library(data.table)
library(stats)
library(Hmisc)
library(parallel)
library(ranger)


#function to create dateframe of t-test values for each vdn 
t.test1 <- function(df, mu, alternative)
{
  
  on_off = df[,on_off]
  vdn = df[,vdn[1]]
  beta <- t.test(on_off, mu = mu, alternative = alternative)
  p_value <- beta$p.value
  lower_boundary = beta$conf.int[1]
  upper_boundary = beta$conf.int[2]
  estimate = beta$estimate[[1]]
  t_value = beta$statistic[[1]]
  confidence_level = attributes(beta$conf.int)
  df_final <- data.frame(vdn, p_value, t_value, lower_boundary, upper_boundary, confidence_level,  estimate)
  
  return(df_final)
  
}

loaddata_parr <- function( startdate, query){
  enddate <- startdate + 1 
  query <- sprintf(query, startdate, startdate, enddate)
  return(run_query(query, "arch"))
}

start.date  = Sys.Date()  - 60 #60Days from Current Date 
end.date    = Sys.Date() #current date
date_vector <- seq(start.date, end.date, by="days")

#Fetching vaca Data
if(1==1){
  vaca_query <- "select '%s' as date, vq_sct as vdn , on_off, isrelevant, successful
  from `bskybinternal.vaca`  a
  where callstarttime between '%s' and '%s'
  and customgroup = 'Platform Classic'"

  dat <- mclapply(date_vector, loaddata_parr, vaca_query, mc.cores = 20L)
  vaca <- rbindlist(dat)
}

setDT(vaca)


#Fetching SME Data
#sme <- setDT(run_query(paste0("select substr(`vdn_bskyb`,1,(locate('_PBX',`vdn_bskyb`) -1)) as vdn, on_off, v_calltime_dt as date, isrelevant
#                         from  `smexplorerdata`.`bskybinternal.smetable_ptf_xfer_newdg2` 
#                         where calltime >= '", start.date, "' and calltime =<'", end.date,"'
#                         and successful = 1" ,sep=""), "ai"))

#Fetching Engine.VDN 
engine.vdn <- run_query(paste0("select substr(`VDN`,1,(locate('_PBX',`VDN`) -1)) vdn, benchmarkid
                         from bskybsatmap.`engine.vdn`
                               group by 1,2" , sep=""), "arch") 


#Creating a dummy isrelevant flag for applying dcast 
vaca[isrelevant == 1, flag_isrelevant := "Reported_Calls"]
vaca[isrelevant == 0, flag_isrelevant := "Non-Reported_Calls"]



relevant_vdns <- vaca[,.N, by = .(vdn)] # This will include all vdns which are present in sme in last 60 days
benchmark_check <- merge(relevant_vdns , engine.vdn , by= "vdn" ) # Extracting vdn level benchmarkid for each vdn 
benchmark_check <- benchmark_check[,!c("N")] #Removing a column "N" from final data

vaca_bm <- vaca[successful ==1] # Only successful calls should be part of the analysis for checking Reported/Non-Repoerted Calls. 
vaca_success <- vaca[,.(success_calls_perc = round(100*(sum(successful)/.N),2)), by = vdn]
#Below steps are fetching total number of Reported and Non-Reporting Calls for each vdn 
vaca_vdn_isrelevant <- vaca_bm[,.(calls = .N), by = .(vdn,flag_isrelevant)]
vaca_vdn_isrelevant_dcast <- dcast(vaca_vdn_isrelevant, vdn ~ flag_isrelevant, value.var = "calls", fill = 0)
final_benchmark_check <- merge(vaca_vdn_isrelevant_dcast ,  benchmark_check, by= "vdn" ) #Merging the Reported/Non-Reported Calls Table with Benchmarkid table
final_final_benchmark_check <- merge(final_benchmark_check, vaca_success, by = "vdn")
View(final_final_benchmark_check[order(benchmarkid,-(Reported_Calls+`Non-Reported_Calls`))]) #Final Output


#Significence Test
vaca_isrelevant <- vaca[isrelevant==1]
vaca_L7D = vaca_isrelevant[date >= Sys.Date()-7 & date <= Sys.Date()]
# 
# 
# sig_test <- binconf(x=sme_L7D$oncalls,n=sme_L7D$total_calls)
# sig_test <- as.data.table(x1)
# sig_test_final <- cbind.data.frame(x[,c("vdn","total_calls", "bm")],sig_test)
# View(sig_test_final)


#T-Test
vaca_ttest <- vaca_L7D[,.(vdn,on_off = as.numeric(on_off))]
rel_vdns <- vaca_ttest[,.(calls = .N), by = .(vdn)][calls > 10]
vaca_ttest <- vaca_ttest[vdn %in% rel_vdns$vdn]
a = split(vaca_ttest,by = "vdn")
b = vaca_L7D[, .(calls = .N), by = "vdn"]

df_final = lapply(a, t.test1, mu = 0.8, alternative = "two.sided")
t_test <- rbindlist(df_final)
t_test_final <- t_test[order(p_value)] %>% mutate_if(is.numeric, ~round(., 4)) 
t_test_final[is.na(t_test_final)] <- "NA"
t_test_final_calls <- merge(b,t_test_final, by= "vdn" )


conn <- dbConnect(MySQL(), user = "satmap_ai_bskyb", password = "$bskyb#123", 
                  dbname = "etl_dev", host = "10.105.2.57", 
                  port = 3307)


dbWriteTable(conn, name = "t_test_alert", value = t_test_final_calls[order(-calls, p_value)] , overwrite = T, append = F, row.names = F)
dbWriteTable(conn, name = "vdn_bmcheck_alert", value = final_final_benchmark_check[order(benchmarkid,-(Reported_Calls+`Non-Reported_Calls`))] , overwrite = T, append = F, row.names = F)


